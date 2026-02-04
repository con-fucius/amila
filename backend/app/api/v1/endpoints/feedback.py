"""
User Feedback API Endpoints

Captures explicit user feedback (thumbs up/down) to improve sentiment tracking.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status
import logging

from app.core.auth import require_permissions
from app.services.sentiment_tracker import SentimentTracker

logger = logging.getLogger(__name__)

router = APIRouter()


class FeedbackRequest(BaseModel):
    """User feedback request"""
    rating: int = Field(..., ge=-1, le=1, description="Feedback rating: +1 (positive) or -1 (negative)")
    comment: Optional[str] = Field(None, description="Optional feedback comment")
    query_id: Optional[str] = Field(None, description="Optional query ID to associate feedback")


class FeedbackResponse(BaseModel):
    """Feedback response"""
    success: bool
    recorded_at: str
    indicators: list


@router.post("/", response_model=FeedbackResponse)
async def submit_feedback(
    request: FeedbackRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "analyst", "viewer"]))
) -> FeedbackResponse:
    """
    Submit explicit feedback for a query or interaction.
    """
    try:
        if request.rating == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Rating must be +1 or -1"
            )
        result = await SentimentTracker.record_feedback(
            user_id=current_user.get("id", "anonymous"),
            rating=request.rating,
            comment=request.comment,
            query_id=request.query_id
        )
        return FeedbackResponse(
            success=True,
            recorded_at=result.get("recorded_at", ""),
            indicators=result.get("indicators", [])
        )
    except Exception as e:
        logger.error(f"Failed to record feedback: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record feedback"
        )
