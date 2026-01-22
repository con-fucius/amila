
import logging
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from app.core.rbac import rbac_manager, Role
from app.core.rate_limiter import apply_rate_limit, RateLimitTier
from app.core.audit import audit_logger, AuditAction
from app.core.session_manager import session_manager
from app.services.query_state_manager import get_query_state_manager

router = APIRouter()
logger = logging.getLogger(__name__)


class CancelQueryRequest(BaseModel):
    """Request model for query cancellation"""
    query_id: str


class CancelQueryResponse(BaseModel):
    """Response model for query cancellation"""
    query_id: str
    status: str
    message: str
    cancelled: bool


@router.post("/{query_id}/cancel", response_model=CancelQueryResponse)
async def cancel_query(
    query_id: str,
    request: Request,
    user: dict = Depends(rbac_manager.get_current_user)
) -> CancelQueryResponse:
    """
    Cancel a running query
    
    **RBAC:** User must own the query or be an admin
    **Rate Limit:** 30 req/min for analysts, 100 req/min for admins
    **Security:** Ownership validation, audit trail
    
    This endpoint triggers the full kill-chain:
    1. Database-level kill (Oracle: ALTER SYSTEM KILL SESSION, Doris: kill_query)
    2. Process termination (fallback for Oracle)
    3. Resource cleanup
    """
    # Apply rate limiting
    await apply_rate_limit(request, user, RateLimitTier(user["role"].value))
    
    # Validate query ID format
    import re
    if not re.match(r"^[a-zA-Z0-9_\-]+$", query_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid query ID format"
        )
    
    # Check query ownership
    state_manager = await get_query_state_manager()
    query_metadata = await state_manager.get_query_metadata(query_id)
    
    if not query_metadata:
        raise HTTPException(
            status_code=404,
            detail="Query not found or not registered"
        )
    
    # Verify ownership or admin role
    query_owner = query_metadata.get("user_id") or query_metadata.get("username")
    if query_owner != user.get("username") and user.get("role") != Role.ADMIN:
        raise HTTPException(
            status_code=403,
            detail="Permission denied: You can only cancel your own queries"
        )
    
    logger.info(f"User {user['username']} requesting cancellation of query {query_id[:8]}...")
    
    try:
        # Trigger cancellation via session manager
        cancelled = await session_manager.cancel_query(query_id)
        
        if cancelled:
            # Audit successful cancellation
            await audit_logger.log(
                action=AuditAction.QUERY_CANCEL,
                user=user["username"],
                user_role=user["role"].value,
                success=True,
                resource="query",
                resource_id=query_id,
                details={
                    "query_owner": query_owner,
                    "cancelled_by": user["username"],
                },
                ip_address=request.client.host if request.client else None,
            )
            
            return CancelQueryResponse(
                query_id=query_id,
                status="cancelled",
                message="Query cancellation initiated successfully",
                cancelled=True
            )
        else:
            # No cancellation handler found
            logger.warning(f"No cancellation handler found for query {query_id[:8]}...")
            
            # Audit failed cancellation
            await audit_logger.log(
                action=AuditAction.QUERY_CANCEL,
                user=user["username"],
                user_role=user["role"].value,
                success=False,
                resource="query",
                resource_id=query_id,
                details={
                    "query_owner": query_owner,
                    "cancelled_by": user["username"],
                    "reason": "No cancellation handler found"
                },
                ip_address=request.client.host if request.client else None,
            )
            
            return CancelQueryResponse(
                query_id=query_id,
                status="not_found",
                message="Query not found or already completed",
                cancelled=False
            )
            
    except Exception as e:
        logger.error(f"Failed to cancel query {query_id[:8]}...: {e}", exc_info=True)
        
        # Audit error
        await audit_logger.log(
            action=AuditAction.QUERY_CANCEL,
            user=user["username"],
            user_role=user["role"].value,
            success=False,
            resource="query",
            resource_id=query_id,
            details={
                "query_owner": query_owner,
                "cancelled_by": user["username"],
                "error": str(e)
            },
            ip_address=request.client.host if request.client else None,
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel query: {str(e)}"
        )
