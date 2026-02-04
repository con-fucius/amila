"""
SLO/SLI Endpoints
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import logging

from app.core.rbac import require_permission, Permission, rbac_manager
from app.services.slo_service import SLOService

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateSLORequest(BaseModel):
    name: str
    sli_type: str = Field(..., description="availability | latency_p95 | error_rate")
    target: float
    window_minutes: int = Field(default=60)
    description: Optional[str] = None


@router.post("/")
@require_permission(Permission.SYSTEM_METRICS)
async def create_slo(
    request: CreateSLORequest,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    try:
        slo = await SLOService.create_slo(
            name=request.name,
            sli_type=request.sli_type,
            target=request.target,
            window_minutes=request.window_minutes,
            description=request.description,
        )
        return {"status": "success", "slo": slo}
    except Exception as e:
        logger.error(f"Failed to create SLO: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
@require_permission(Permission.SYSTEM_METRICS)
async def list_slos(
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    slos = await SLOService.list_slos()
    return {"status": "success", "slos": slos}


@router.get("/{slo_id}/status")
@require_permission(Permission.SYSTEM_METRICS)
async def get_slo_status(
    slo_id: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    slo = await SLOService.list_slos()
    target = None
    for item in slo:
        if item.get("slo_id") == slo_id:
            target = item
            break
    if not target:
        raise HTTPException(status_code=404, detail="SLO not found")
    sli = await SLOService.compute_sli(target)
    return {"status": "success", "slo": target, "sli": sli}


@router.delete("/{slo_id}")
@require_permission(Permission.SYSTEM_METRICS)
async def delete_slo(
    slo_id: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    await SLOService.delete_slo(slo_id)
    return {"status": "success", "deleted": slo_id}

