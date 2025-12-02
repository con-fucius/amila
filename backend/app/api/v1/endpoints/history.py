"""
Query History API Endpoints
Undo/redo functionality for queries
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from app.services.query_history_service import QueryHistoryService
from app.core.rbac import rbac_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/history", tags=["history"])


class UndoRedoRequest(BaseModel):
    """Request for undo/redo operations"""
    session_id: str = Field(..., description="Session identifier")


class HistoryRequest(BaseModel):
    """Request to get query history"""
    session_id: str = Field(..., description="Session identifier")
    limit: int = Field(default=10, ge=1, le=50, description="Max entries to retrieve")


@router.post("/undo")
async def undo_query(
    request: UndoRedoRequest,
    current_user: dict = Depends(rbac_manager.get_current_user)
):
    """
    Undo last query - restore previous query state
    
    Moves current query from history stack to redo stack
    """
    try:
        previous_state = await QueryHistoryService.undo(request.session_id)
        
        if not previous_state:
            return {
                "status": "info",
                "message": "Nothing to undo",
                "state": None
            }
        
        return {
            "status": "success",
            "message": "Undo successful",
            "state": previous_state,
            "can_undo": await QueryHistoryService.can_undo(request.session_id),
            "can_redo": await QueryHistoryService.can_redo(request.session_id)
        }
        
    except Exception as e:
        logger.error(f"Undo failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/redo")
async def redo_query(
    request: UndoRedoRequest,
    current_user: dict = Depends(rbac_manager.get_current_user)
):
    """
    Redo last undone query - restore next query state
    
    Moves query from redo stack back to history stack
    """
    try:
        next_state = await QueryHistoryService.redo(request.session_id)
        
        if not next_state:
            return {
                "status": "info",
                "message": "Nothing to redo",
                "state": None
            }
        
        return {
            "status": "success",
            "message": "Redo successful",
            "state": next_state,
            "can_undo": await QueryHistoryService.can_undo(request.session_id),
            "can_redo": await QueryHistoryService.can_redo(request.session_id)
        }
        
    except Exception as e:
        logger.error(f"Redo failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/get")
async def get_history(
    request: HistoryRequest,
    current_user: dict = Depends(rbac_manager.get_current_user)
):
    """
    Get query history for session
    
    Returns list of previous queries (most recent first)
    """
    try:
        history = await QueryHistoryService.get_session_history(
            session_id=request.session_id,
            limit=request.limit
        )
        
        return {
            "status": "success",
            "history": history,
            "count": len(history),
            "can_undo": await QueryHistoryService.can_undo(request.session_id),
            "can_redo": await QueryHistoryService.can_redo(request.session_id)
        }
        
    except Exception as e:
        logger.error(f"Failed to get history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear")
async def clear_history(
    request: UndoRedoRequest,
    current_user: dict = Depends(rbac_manager.get_current_user)
):
    """
    Clear query history for session
    
    Removes all history and redo stack entries
    """
    try:
        await QueryHistoryService.clear_history(request.session_id)
        
        return {
            "status": "success",
            "message": "History cleared"
        }
        
    except Exception as e:
        logger.error(f"Failed to clear history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{session_id}")
async def get_history_status(
    session_id: str,
    current_user: dict = Depends(rbac_manager.get_current_user)
):
    """
    Get undo/redo availability status
    
    Returns whether undo and redo operations are available
    """
    try:
        return {
            "status": "success",
            "can_undo": await QueryHistoryService.can_undo(session_id),
            "can_redo": await QueryHistoryService.can_redo(session_id)
        }
        
    except Exception as e:
        logger.error(f"Failed to get history status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
