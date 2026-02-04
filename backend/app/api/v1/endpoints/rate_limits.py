from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from app.core.rbac import rbac_manager, Role
from app.core.rate_limiter import rate_limiter, RateLimitTier

router = APIRouter(prefix="/ratelimits", tags=["rate-limits"])


@router.get("/status")
async def get_rate_limit_status(
    endpoints: Optional[List[str]] = Query(None, description="Endpoints to check"),
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    username = user.get("username", "anonymous")
    role = user.get("role")
    tier = RateLimitTier.VIEWER
    try:
        if isinstance(role, Role):
            tier = RateLimitTier(role.value)
        elif isinstance(role, str):
            tier = RateLimitTier(role)
    except Exception:
        tier = RateLimitTier.VIEWER

    # Default endpoints of interest
    default_endpoints = [
        "/api/v1/queries/process",
        "/api/v1/queries/submit",
        "/api/v1/queries/connections",
        "/api/v1/queries/stream",
        "/api/v1/diagnostics/status",
    ]

    target_endpoints = endpoints or default_endpoints
    results: Dict[str, Any] = {}
    for ep in target_endpoints:
        try:
            results[ep] = await rate_limiter.get_rate_limit_status(
                user=username,
                endpoint=ep,
                tier=tier,
            )
        except Exception as e:
            results[ep] = {"error": str(e), "tier": tier.value}

    return {
        "status": "success",
        "user": username,
        "tier": tier.value,
        "endpoints": results,
    }
