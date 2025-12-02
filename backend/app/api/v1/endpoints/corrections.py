"""
Query Corrections API Endpoints
Allows storing and retrieving SQL corrections for learning
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from app.services.query_corrections_service import QueryCorrectionsService
from app.core.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/corrections", tags=["corrections"])


class StoreCorrectionRequest(BaseModel):
    """Request to store a SQL correction"""
    original_query: str = Field(..., description="Original natural language query")
    generated_sql: str = Field(..., description="AI-generated SQL")
    corrected_sql: str = Field(..., description="User-corrected SQL")
    correction_type: str = Field(default="user_edit", description="Type of correction")
    intent: Optional[str] = Field(None, description="Query intent")
    success_after_correction: bool = Field(default=True, description="Whether corrected SQL succeeded")
    session_id: Optional[str] = Field(None, description="Session identifier")


class GetCorrectionsRequest(BaseModel):
    """Request to retrieve relevant corrections"""
    original_query: Optional[str] = Field(None, description="Query to match")
    intent: Optional[str] = Field(None, description="Intent to match")
    limit: int = Field(default=5, ge=1, le=50, description="Max corrections to retrieve")


@router.post("/store")
async def store_correction(
    request: StoreCorrectionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Store a SQL correction for learning
    
    When users edit generated SQL, store the correction to learn from it
    """
    try:
        user_id = current_user.get("user_id", "default_user")
        session_id = request.session_id or current_user.get("session_id", "default_session")
        
        correction_id = await QueryCorrectionsService.store_correction(
            user_id=user_id,
            session_id=session_id,
            original_query=request.original_query,
            generated_sql=request.generated_sql,
            corrected_sql=request.corrected_sql,
            correction_type=request.correction_type,
            intent=request.intent,
            success_after_correction=request.success_after_correction,
        )
        
        return {
            "status": "success",
            "correction_id": correction_id,
            "message": "Correction stored successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to store correction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/retrieve")
async def get_relevant_corrections(
    request: GetCorrectionsRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Retrieve relevant corrections for similar queries
    
    Used to show users how similar queries were corrected in the past
    """
    try:
        corrections = await QueryCorrectionsService.get_relevant_corrections(
            original_query=request.original_query,
            intent=request.intent,
            limit=request.limit,
        )
        
        return {
            "status": "success",
            "corrections": corrections,
            "count": len(corrections)
        }
        
    except Exception as e:
        logger.error(f"Failed to retrieve corrections: {e}")
        raise HTTPException(status_code=500, detail=str(e))
