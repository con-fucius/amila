"""
Celery Fallback Handler

Provides graceful degradation when Celery workers are unavailable.
Executes tasks synchronously or queues them in memory for later processing.

Features:
- Synchronous task execution fallback
- In-memory task queue for non-critical tasks
- Task priority management
- Automatic retry when Celery recovers
"""

import logging
import asyncio
from typing import Any, Callable, Optional, Dict, List
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
import uuid
from collections import deque

logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """Task priority levels"""
    CRITICAL = "critical"  # Execute immediately even in degraded mode
    HIGH = "high"  # Execute synchronously if Celery unavailable
    NORMAL = "normal"  # Queue for later if Celery unavailable
    LOW = "low"  # Skip if Celery unavailable


class TaskStatus(Enum):
    """Task execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class FallbackTask:
    """Represents a task in the fallback queue"""
    task_id: str
    task_name: str
    func: Callable
    args: tuple
    kwargs: dict
    priority: TaskPriority
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Any] = None
    error: Optional[str] = None
    attempts: int = 0
    max_attempts: int = 3


class CeleryFallbackHandler:
    """
    Handles task execution when Celery is unavailable.
    
    Strategies:
    1. CRITICAL/HIGH priority: Execute synchronously
    2. NORMAL priority: Queue in memory for later
    3. LOW priority: Skip with warning
    """
    
    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self.task_queue: deque[FallbackTask] = deque(maxlen=max_queue_size)
        self.completed_tasks: Dict[str, FallbackTask] = {}
        self.max_completed_history = 100
        self.processing_lock = asyncio.Lock()
        self.is_processing = False
        
        logger.info("Celery Fallback Handler initialized")
    
    def is_celery_available(self) -> bool:
        """Check if Celery workers are available"""
        try:
            from app.core.celery_app import celery_app
            
            inspect = celery_app.control.inspect()
            stats = inspect.stats()
            
            return bool(stats)
        
        except Exception as e:
            logger.debug(f"Celery availability check failed: {e}")
            return False
    
    async def execute_task(
        self,
        task_name: str,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        celery_task: Optional[Any] = None
    ) -> Optional[str]:
        """
        Execute a task with fallback handling.
        
        Args:
            task_name: Name of the task
            func: Function to execute
            args: Positional arguments
            kwargs: Keyword arguments
            priority: Task priority
            celery_task: Celery task object (if available)
            
        Returns:
            Task ID if queued/executed, None if skipped
        """
        kwargs = kwargs or {}
        
        # Try Celery first
        if celery_task and self.is_celery_available():
            try:
                result = celery_task.apply_async(args=args, kwargs=kwargs)
                logger.debug(f"Task {task_name} submitted to Celery: {result.id}")
                return result.id
            except Exception as e:
                logger.warning(f"Celery task submission failed: {e}. Using fallback.")
        
        # Fallback handling based on priority
        task_id = str(uuid.uuid4())
        task = FallbackTask(
            task_id=task_id,
            task_name=task_name,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority
        )
        
        if priority in (TaskPriority.CRITICAL, TaskPriority.HIGH):
            # Execute synchronously
            logger.info(f"Executing {priority.value} priority task synchronously: {task_name}")
            await self._execute_task_sync(task)
            return task_id
        
        elif priority == TaskPriority.NORMAL:
            # Queue for later
            if len(self.task_queue) >= self.max_queue_size:
                logger.warning(f"Task queue full, dropping oldest task")
                dropped = self.task_queue.popleft()
                dropped.status = TaskStatus.SKIPPED
                dropped.error = "Queue overflow"
            
            self.task_queue.append(task)
            logger.info(f"Task {task_name} queued for later execution (queue size: {len(self.task_queue)})")
            return task_id
        
        else:  # LOW priority
            # Skip task
            task.status = TaskStatus.SKIPPED
            task.error = "Celery unavailable, low priority task skipped"
            logger.info(f"Skipping low priority task: {task_name}")
            self._add_to_history(task)
            return None
    
    async def _execute_task_sync(self, task: FallbackTask):
        """Execute a task synchronously"""
        task.status = TaskStatus.RUNNING
        task.attempts += 1
        
        try:
            # Execute function
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func(*task.args, **task.kwargs)
            else:
                result = task.func(*task.args, **task.kwargs)
            
            task.status = TaskStatus.COMPLETED
            task.result = result
            logger.info(f"Task {task.task_name} completed successfully")
        
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            logger.error(f"Task {task.task_name} failed: {e}")
        
        finally:
            self._add_to_history(task)
    
    async def process_queued_tasks(self, max_tasks: int = 10):
        """
        Process queued tasks (called when Celery recovers or periodically).
        
        Args:
            max_tasks: Maximum number of tasks to process in one batch
        """
        if self.is_processing:
            logger.debug("Task processing already in progress")
            return
        
        async with self.processing_lock:
            self.is_processing = True
            
            try:
                processed = 0
                
                while self.task_queue and processed < max_tasks:
                    task = self.task_queue.popleft()
                    
                    # Check if Celery is now available
                    if self.is_celery_available():
                        # Try to submit to Celery
                        try:
                            from app.core.celery_app import celery_app
                            
                            # Find the Celery task by name
                            celery_task = celery_app.tasks.get(task.task_name)
                            if celery_task:
                                result = celery_task.apply_async(
                                    args=task.args,
                                    kwargs=task.kwargs
                                )
                                task.status = TaskStatus.COMPLETED
                                task.result = result.id
                                logger.info(f"Queued task {task.task_name} submitted to Celery: {result.id}")
                            else:
                                # Execute synchronously if Celery task not found
                                await self._execute_task_sync(task)
                        
                        except Exception as e:
                            logger.warning(f"Failed to submit queued task to Celery: {e}")
                            await self._execute_task_sync(task)
                    
                    else:
                        # Celery still unavailable, execute synchronously
                        await self._execute_task_sync(task)
                    
                    processed += 1
                
                if processed > 0:
                    logger.info(f"Processed {processed} queued tasks")
            
            finally:
                self.is_processing = False
    
    def _add_to_history(self, task: FallbackTask):
        """Add completed task to history"""
        self.completed_tasks[task.task_id] = task
        
        # Limit history size
        if len(self.completed_tasks) > self.max_completed_history:
            # Remove oldest tasks
            oldest_keys = sorted(
                self.completed_tasks.keys(),
                key=lambda k: self.completed_tasks[k].created_at
            )[:len(self.completed_tasks) - self.max_completed_history]
            
            for key in oldest_keys:
                del self.completed_tasks[key]
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a task"""
        # Check completed history
        if task_id in self.completed_tasks:
            task = self.completed_tasks[task_id]
            return {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "status": task.status.value,
                "result": task.result,
                "error": task.error,
                "created_at": task.created_at.isoformat(),
                "attempts": task.attempts,
            }
        
        # Check queue
        for task in self.task_queue:
            if task.task_id == task_id:
                return {
                    "task_id": task.task_id,
                    "task_name": task.task_name,
                    "status": task.status.value,
                    "created_at": task.created_at.isoformat(),
                    "queue_position": list(self.task_queue).index(task),
                }
        
        return None
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            "queue_size": len(self.task_queue),
            "max_queue_size": self.max_queue_size,
            "completed_tasks": len(self.completed_tasks),
            "celery_available": self.is_celery_available(),
            "is_processing": self.is_processing,
            "tasks_by_priority": {
                priority.value: sum(1 for t in self.task_queue if t.priority == priority)
                for priority in TaskPriority
            },
            "tasks_by_status": {
                status.value: sum(
                    1 for t in self.completed_tasks.values()
                    if t.status == status
                )
                for status in TaskStatus
            },
        }
    
    def clear_queue(self):
        """Clear the task queue (admin operation)"""
        cleared = len(self.task_queue)
        self.task_queue.clear()
        logger.warning(f"Task queue cleared: {cleared} tasks removed")
        return cleared


# Global instance
celery_fallback_handler = CeleryFallbackHandler()


# Convenience functions for common tasks

async def execute_with_fallback(
    task_name: str,
    func: Callable,
    args: tuple = (),
    kwargs: dict = None,
    priority: TaskPriority = TaskPriority.NORMAL,
    celery_task: Optional[Any] = None
) -> Optional[str]:
    """
    Execute a task with automatic Celery fallback.
    
    Usage:
        from app.core.celery_fallback import execute_with_fallback, TaskPriority
        
        task_id = await execute_with_fallback(
            "generate_report",
            generate_report_func,
            args=(query_id,),
            kwargs={"format": "pdf"},
            priority=TaskPriority.HIGH
        )
    """
    return await celery_fallback_handler.execute_task(
        task_name, func, args, kwargs, priority, celery_task
    )
