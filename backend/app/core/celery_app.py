"""
Celery Task Queue Configuration

Handles asynchronous background tasks:
- Long-running query execution
- Schema metadata refresh
- Query result caching
- Scheduled report generation
- Email/notification delivery
"""

import logging
from celery import Celery
from celery.signals import task_prerun, task_postrun, task_failure
from opentelemetry import trace, propagate, context
from opentelemetry.trace import SpanKind
from kombu import serializer

from app.core.config import settings

logger = logging.getLogger(__name__)

# Initialize Celery app
celery_app = Celery(
    "amil_bi_agent",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.query_tasks",
        "app.tasks.schema_tasks",
        "app.tasks.report_tasks",
        "app.tasks.alert_tasks",
        "app.tasks.webhook_tasks",
    ],
)

# Celery Configuration
celery_app.conf.update(
    # Task execution settings
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    
    # Task result settings
    result_expires=3600,  # Results expire after 1 hour
    result_backend_transport_options={
        "master_name": "mymaster",
        "retry_on_timeout": True,
    },
    
    # Task routing
    task_routes={
        "app.tasks.query_tasks.*": {"queue": "queries"},
        "app.tasks.schema_tasks.*": {"queue": "schema"},
        "app.tasks.report_tasks.*": {"queue": "reports"},
        "app.tasks.webhook_tasks.*": {"queue": "webhooks"},
    },
    
    # Worker settings
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks
    worker_disable_rate_limits=False,
    
    # Task execution limits
    task_soft_time_limit=300,  # 5 minutes soft limit
    task_time_limit=600,  # 10 minutes hard limit
    task_acks_late=True,  # Acknowledge after task completes
    task_reject_on_worker_lost=True,
    
    # Retry settings
    task_default_retry_delay=60,  # Retry after 1 minute
    task_max_retries=3,
    
    # Beat schedule (periodic tasks)
    beat_schedule={
        "refresh-schema-cache-every-hour": {
            "task": "app.tasks.schema_tasks.refresh_schema_cache",
            "schedule": 3600.0,  # Every hour
        },
        "cleanup-old-query-results-daily": {
            "task": "app.tasks.query_tasks.cleanup_old_results",
            "schedule": 86400.0,  # Every 24 hours
            "kwargs": {"days_old": 7},
        },
        # ADDITIONAL ISSUE: Checkpoint Growth - Schedule cleanup task
        "cleanup-langgraph-checkpoints-daily": {
            "task": "app.tasks.checkpoint_cleanup.cleanup_old_checkpoints_task",
            "schedule": 86400.0,  # Every 24 hours
            "kwargs": {"retention_days": 7, "max_per_thread": 10, "dry_run": False},
        },
        "run-due-report-schedules": {
            "task": "app.tasks.report_tasks.run_due_schedules",
            "schedule": 60.0,
        },
        "process-alert-escalations": {
            "task": "app.tasks.alert_tasks.process_alert_escalations",
            "schedule": 60.0,
        },
    },
)


# ==================== SIGNAL HANDLERS ====================

@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **extra):
    """Log task start"""
    logger.info(f"Task started: {task.name} (ID: {task_id})")
    try:
        tracer = trace.get_tracer("celery")
        headers = getattr(task.request, "headers", {}) or {}
        ctx = propagate.extract(headers)
        token = context.attach(ctx)
        span = tracer.start_span(task.name, kind=SpanKind.CONSUMER)
        task.request._otel_span = span
        task.request._otel_token = token
    except Exception:
        pass


@task_postrun.connect
def task_postrun_handler(
    sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, **extra
):
    """Log task completion"""
    logger.info(f"Task completed: {task.name} (ID: {task_id})")
    try:
        span = getattr(task.request, "_otel_span", None)
        token = getattr(task.request, "_otel_token", None)
        if span:
            span.end()
        if token:
            context.detach(token)
    except Exception:
        pass


@task_failure.connect
def task_failure_handler(
    sender=None, task_id=None, exception=None, args=None, kwargs=None, traceback=None, **extra
):
    """Log task failure"""
    logger.error(f"Task failed: {sender.name} (ID: {task_id}) - {exception}")
    try:
        span = getattr(sender.request, "_otel_span", None)
        token = getattr(sender.request, "_otel_token", None)
        if span:
            span.record_exception(exception)
            span.end()
        if token:
            context.detach(token)
    except Exception:
        pass


# ==================== HELPER FUNCTIONS ====================

def get_task_status(task_id: str) -> dict:
    """
    Get task execution status
    
    Args:
        task_id: Celery task ID
        
    Returns:
        Task status dict
    """
    from celery.result import AsyncResult
    
    result = AsyncResult(task_id, app=celery_app)
    
    return {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
        "traceback": result.traceback if result.failed() else None,
    }


def revoke_task(task_id: str, terminate: bool = False) -> bool:
    """
    Cancel a running task
    
    Args:
        task_id: Celery task ID
        terminate: Force terminate (SIGKILL) if True
        
    Returns:
        True if successful
    """
    try:
        celery_app.control.revoke(task_id, terminate=terminate)
        logger.info(f"Task revoked: {task_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to revoke task {task_id}: {e}")
        return False


def get_active_tasks() -> list:
    """Get list of currently running tasks"""
    inspect = celery_app.control.inspect()
    active = inspect.active()
    
    if not active:
        return []
    
    tasks = []
    for worker, task_list in active.items():
        for task in task_list:
            tasks.append({
                "worker": worker,
                "task_id": task["id"],
                "task_name": task["name"],
                "args": task["args"],
                "kwargs": task["kwargs"],
            })
    
    return tasks


def purge_queue(queue_name: str = None) -> int:
    """
    Remove all tasks from queue
    
    Args:
        queue_name: Specific queue to purge, or all queues if None
        
    Returns:
        Number of tasks purged
    """
    try:
        if queue_name:
            count = celery_app.control.purge([queue_name])
        else:
            count = celery_app.control.purge()
        
        logger.info(f"Purged {count} tasks from queue")
        return count
    except Exception as e:
        logger.error(f"Failed to purge queue: {e}")
        return 0
