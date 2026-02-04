"""
Report Scheduling Endpoints
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import logging

from app.core.rbac import rbac_manager
from app.services.report_schedule_service import ReportScheduleService

router = APIRouter()
logger = logging.getLogger(__name__)


class ReportScheduleRequest(BaseModel):
    name: str = Field(..., description="Schedule name")
    cron: str = Field(..., description="Cron expression (5 fields)")
    sql_query: str = Field(..., description="SQL to execute")
    database_type: str = Field(default="oracle")
    connection_name: Optional[str] = Field(default=None)
    format: str = Field(default="html")
    recipients: List[str] = Field(default_factory=list)


@router.post("/schedule")
async def create_report_schedule(
    request: ReportScheduleRequest,
    user: dict = Depends(rbac_manager.get_current_user),
):
    try:
        if request.format.lower() not in {"html", "pdf", "docx"}:
            raise HTTPException(status_code=400, detail="format must be html, pdf, or docx")
        schedule = await ReportScheduleService.create_schedule(
            user_id=user.get("username") or user.get("user_id") or "unknown",
            name=request.name,
            cron=request.cron,
            sql_query=request.sql_query,
            database_type=request.database_type,
            connection_name=request.connection_name,
            format=request.format,
            recipients=request.recipients,
        )
        return {"status": "success", "schedule": schedule}
    except Exception as e:
        logger.error(f"Failed to create schedule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schedules")
async def list_report_schedules(
    limit: int = 50,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    schedules = await ReportScheduleService.list_schedules(
        user_id=user.get("username") or user.get("user_id") or "unknown",
        limit=limit
    )
    return {"status": "success", "schedules": schedules}


@router.delete("/schedule/{schedule_id}")
async def delete_report_schedule(
    schedule_id: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    ok = await ReportScheduleService.delete_schedule(schedule_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"status": "success", "deleted": schedule_id}

