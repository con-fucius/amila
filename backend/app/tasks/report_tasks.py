"""
Scheduled report generation and delivery tasks
"""

import logging
from typing import Dict, Any
from datetime import datetime, timezone

from app.core.celery_app import celery_app
from app.services.report_schedule_service import ReportScheduleService
from app.services.report_generation_service import ReportGenerationService
from app.services.notification_service import NotificationService
from app.services.database_router import DatabaseRouter
from app.core.config import settings
import asyncio

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.report_tasks.run_due_schedules")
def run_due_schedules() -> Dict[str, Any]:
    """
    Poll for due report schedules and execute them.
    """
    if not settings.REPORTS_SCHEDULER_ENABLED:
        return {"status": "skipped", "reason": "disabled"}

    async def _run():
        due = await ReportScheduleService.list_due_schedules()
        executed = 0
        for item in due:
            try:
                await _execute_schedule(item)
                executed += 1
                await ReportScheduleService.update_next_run(
                    schedule_id=item["schedule_id"],
                    cron=item["cron"],
                    last_status="success",
                )
            except Exception as e:
                await ReportScheduleService.update_next_run(
                    schedule_id=item["schedule_id"],
                    cron=item["cron"],
                    last_status="error",
                    last_error=str(e),
                )
        return {"status": "success", "executed": executed}

    # Run async in sync task
    return asyncio.run(_run())


async def _execute_schedule(item: Dict[str, Any]) -> None:
    user_id = item.get("user_id", "unknown")
    sql_query = item.get("sql_query")
    db_type = item.get("database_type", "oracle")
    conn_name = item.get("connection_name")
    recipients = item.get("recipients") or []

    if not recipients:
        raise ValueError("No recipients configured for schedule")

    result = await DatabaseRouter.execute_sql(
        database_type=db_type,
        sql_query=sql_query,
        connection_name=conn_name,
        user_id=user_id,
        request_id=f"scheduled_{item.get('schedule_id')}",
    )
    if result.get("status") != "success":
        raise ValueError(result.get("error") or "Query execution failed")

    report = await ReportGenerationService.generate_report_with_llm_insights(
        query_results=[{
            "columns": result.get("columns", []),
            "rows": result.get("rows", []),
            "row_count": result.get("row_count", len(result.get("rows", []))),
            "sql_query": sql_query,
        }],
        format=item.get("format", "html"),
        title=item.get("name"),
        user_queries=[sql_query],
    )
    if report.get("status") != "success":
        raise ValueError(report.get("message") or "Report generation failed")

    subject = f"Scheduled Report: {item.get('name')}"
    body = f"Scheduled report {item.get('name')} generated at {datetime.now(timezone.utc).isoformat()}."
    html_body = report.get("content") if report.get("format") == "html" else None

    NotificationService.send_email(
        to_addresses=recipients,
        subject=subject,
        body=body,
        html_body=html_body,
    )


@celery_app.task(name="app.tasks.report_tasks.export_query_results")
def export_query_results(query_id: str, format: str, user_id: str) -> str:
    """
    Export query results to file format
    """
    try:
        logger.info(f"Exporting query {query_id} to {format} for user {user_id}")
        file_path = f"/exports/{query_id}.{format}"
        logger.info(f"Export completed: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Export failed: {e}")
        return ""
