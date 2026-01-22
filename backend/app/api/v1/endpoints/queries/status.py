
import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse

from app.services.query_state_manager import get_query_state_manager
from app.core.rbac import rbac_manager, Role
from app.core.rate_limiter import rate_limiter, RateLimitTier
from app.core.config import settings
from app.core.audit import audit_sse_access

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

@router.get("/{query_id}/stream")
async def stream_query_state(
    query_id: str,
    request: Request,
    token: Optional[str] = None,
    auth_user: Optional[dict] = Depends(rbac_manager.get_current_user_optional),
) -> StreamingResponse:
    """
    Stream real-time query state changes via Server-Sent Events (SSE)
    """
    validate_query_id(query_id)
    
    user = auth_user
    if not user and token:
        try:
            from app.core.auth import AuthenticationManager
            auth_manager = AuthenticationManager()
            payload = auth_manager.decode_token(token, token_type="access")
            if payload:
                user = {"username": payload.get("sub"), "role": Role(payload.get("role", "viewer"))}
        except Exception:
            pass
    
    if not user:
        if settings.is_development:
            user = {"username": "anonymous", "role": Role.VIEWER}
        else:
            raise HTTPException(status_code=401, detail="Authentication required for SSE")
    
    try:
        user_tier = RateLimitTier(user["role"].value) if hasattr(user["role"], "value") else RateLimitTier.VIEWER
        await rate_limiter.check_rate_limit(user=user["username"], endpoint="/api/v1/queries/stream", tier=user_tier)
    except HTTPException: raise
    except Exception: pass
    
    try:
        await audit_sse_access(
            user=user["username"],
            user_role=user["role"].value if hasattr(user["role"], "value") else str(user["role"]),
            query_id=query_id,
            ip_address=request.client.host if request.client else None,
        )
    except Exception: pass
    
    logger.info(f"Starting SSE stream for query {query_id[:8]}... (user: {user['username']})")
    
    state_manager = await get_query_state_manager()
    query_metadata = await state_manager.get_query_metadata(query_id)
    
    if not query_metadata:
        if not settings.is_development:
            raise HTTPException(status_code=404, detail="Query not found or not registered")
    else:
        query_owner = query_metadata.get("user_id") or query_metadata.get("username")
        if query_owner and query_owner != user.get("username") and user.get("role") != Role.ADMIN:
            raise HTTPException(status_code=403, detail="Permission denied")
    
    async def event_generator():
        """
        Generate SSE events for query state changes.
        Automatically cancels the query if client disconnects.
        """
        try:
            async for message in state_manager.subscribe(query_id):
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.warning(f"Client disconnected from SSE stream for query {query_id[:8]}...")
                    
                    # Trigger query cancellation to prevent zombie queries
                    try:
                        from app.core.session_manager import session_manager
                        cancelled = await session_manager.cancel_query(query_id)
                        if cancelled:
                            logger.info(f"Successfully triggered cancellation for disconnected query {query_id[:8]}...")
                        else:
                            logger.warning(f"No cancellation handler found for query {query_id[:8]}...")
                    except Exception as cancel_err:
                        logger.error(f"Failed to cancel query {query_id[:8]}... on disconnect: {cancel_err}")
                    
                    break
                    
                yield message
                
        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for query {query_id[:8]}...")
            
            # Also trigger cancellation on asyncio cancellation
            try:
                from app.core.session_manager import session_manager
                await session_manager.cancel_query(query_id)
            except Exception:
                pass
                
        except Exception as e:
            logger.error(f"Error in SSE stream for query {query_id[:8]}...: {e}")
            yield f"event: error\ndata: {{\"message\": \"{str(e)}\"}}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}
    )
