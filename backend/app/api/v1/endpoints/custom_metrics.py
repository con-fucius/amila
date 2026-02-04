"""
Custom Metrics Endpoints
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import logging

from app.core.rbac import require_permission, Permission, rbac_manager
from app.services.custom_metrics_service import CustomMetricsService

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateCustomMetricRequest(BaseModel):
    metric_id: str = Field(..., description="Unique metric ID")
    name: str = Field(..., description="Metric name")
    description: str = Field(default="Custom metric")
    type: str = Field(default="gauge")
    labels: List[str] = Field(default_factory=list)


class RecordMetricValueRequest(BaseModel):
    value: float
    labels: Optional[Dict[str, str]] = None


@router.post("/custom")
@require_permission(Permission.SYSTEM_METRICS)
async def create_custom_metric(
    request: CreateCustomMetricRequest,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    try:
        metric = await CustomMetricsService.create_metric(
            metric_id=request.metric_id,
            name=request.name,
            description=request.description,
            metric_type=request.type,
            labels=request.labels,
        )
        return {"status": "success", "metric": metric}
    except Exception as e:
        logger.error(f"Failed to create metric: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/custom")
@require_permission(Permission.SYSTEM_METRICS)
async def list_custom_metrics(
    limit: int = 100,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    metrics = await CustomMetricsService.list_metrics(limit=limit)
    return {"status": "success", "metrics": metrics}


@router.post("/custom/{metric_id}/value")
@require_permission(Permission.SYSTEM_METRICS)
async def record_custom_metric_value(
    metric_id: str,
    request: RecordMetricValueRequest,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    try:
        return await CustomMetricsService.record_value(metric_id, request.value, request.labels)
    except Exception as e:
        logger.error(f"Failed to record metric value: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/custom/{metric_id}")
@require_permission(Permission.SYSTEM_METRICS)
async def delete_custom_metric(
    metric_id: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    await CustomMetricsService.delete_metric(metric_id)
    return {"status": "success", "deleted": metric_id}

