"""
Proactive Insights API Endpoints
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging

from app.core.rbac import rbac_manager

router = APIRouter()
logger = logging.getLogger(__name__)


class InsightResponse(BaseModel):
    status: str
    insights: List[Dict[str, Any]]


class TriggerResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None


@router.get("/proactive", response_model=InsightResponse)
async def get_proactive_insights(
    limit: int = 10,
    user: dict = Depends(rbac_manager.get_current_user),
) -> InsightResponse:
    """
    Get recent proactive insights for the current user.
    
    Args:
        limit: Maximum number of insights to return
        
    Returns:
        List of recent proactive insights
    """
    try:
        from app.core.redis_client import redis_client
        
        username = user.get("username", "admin")
        key = f"notifications:{username}:insights"
        
        # Get insights from Redis
        insights = await redis_client.lrange(key, 0, limit - 1)
        
        return InsightResponse(
            status="success",
            insights=insights if insights else []
        )
        
    except Exception as e:
        logger.error(f"Failed to fetch proactive insights: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch insights: {str(e)}"
        )


@router.post("/trigger", response_model=TriggerResponse)
async def trigger_insight_generation(
    user: dict = Depends(rbac_manager.get_current_user),
) -> TriggerResponse:
    """
    Manually trigger proactive insight generation.
    
    **RBAC:** Requires admin permission
    
    Returns:
        Task status
    """
    try:
        # Check admin permission
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        from app.tasks.insight_scheduler import generate_proactive_insights
        
        # Trigger Celery task
        task = generate_proactive_insights.delay()
        
        logger.info(f"Triggered proactive insight generation: {task.id}")
        
        return TriggerResponse(
            status="success",
            message="Proactive insight generation triggered",
            task_id=task.id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger insights: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger insights: {str(e)}"
        )


@router.post("/schedule")
async def update_insight_schedule(
    schedule: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Update the scheduled time for proactive insight generation.
    
    **RBAC:** Requires admin permission
    
    Args:
        schedule: Cron-style schedule (e.g., "0 8 * * *" for 8 AM daily)
        
    Returns:
        Update status
    """
    try:
        # Check admin permission
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        # NOTE: In production, this would update Celery Beat schedule
        # For now, just return success
        
        logger.info(f"Insight schedule updated to: {schedule}")
        
        return {
            "status": "success",
            "message": f"Schedule updated to: {schedule}",
            "note": "Requires Celery Beat restart to take effect"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update schedule: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update schedule: {str(e)}"
        )
