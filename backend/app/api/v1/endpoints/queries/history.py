
import logging
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request, Depends

from app.services.query_state_manager import get_query_state_manager
from app.core.rbac import rbac_manager, Role
from app.core.rate_limiter import apply_rate_limit, RateLimitTier

router = APIRouter()
logger = logging.getLogger(__name__)

# Helper
def validate_query_id(query_id: str):
    import re
    if not re.match(r"^[a-zA-Z0-9_\-]+$", query_id):
        raise HTTPException(
            status_code=400, 
            detail="Invalid query ID format. Only alphanumeric characters, dashes, and underscores allowed."
        )

@router.get("/{query_id}/status")
async def get_query_status(
    query_id: str,
    user: Optional[dict] = Depends(rbac_manager.get_current_user_optional),
) -> Dict[str, Any]:
    """
    Get query processing status from state manager.
    """
    validate_query_id(query_id)
    
    try:
        state_manager = await get_query_state_manager()
        query_metadata = await state_manager.get_query_metadata(query_id)
        current_state = await state_manager.get_state(query_id)
        
        if query_metadata:
            is_owner = user and (
                user.get("username") == query_metadata.get("username") or
                user.get("username") == query_metadata.get("user_id")
            )
            is_admin = user and user.get("role") == Role.ADMIN
            
            if is_owner or is_admin:
                return {
                    "query_id": query_id,
                    "status": current_state.value if current_state else query_metadata.get("status", "unknown"),
                    "message": "Query status retrieved",
                    "metadata": query_metadata,
                }
            else:
                return {
                    "query_id": query_id,
                    "status": current_state.value if current_state else query_metadata.get("status", "unknown"),
                    "message": "Query status retrieved",
                }
        
        if current_state:
            return {
                "query_id": query_id,
                "status": current_state.value,
                "message": "Query status retrieved (no metadata)",
            }
        
        return {
            "query_id": query_id,
            "status": "not_found",
            "message": "Query not found or expired",
        }
    except Exception as e:
        logger.warning(f"Failed to get query status for {query_id}: {e}")
        return {
            "query_id": query_id,
            "status": "unknown",
            "message": "Unable to retrieve query status",
        }
