"""
Error Reporting Endpoints
Provides endpoints for frontend error reporting and error monitoring
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from app.core.rbac import rbac_manager, Role
from app.services.error_reporter_service import (
    ErrorReporterService,
    ErrorLayer,
    ErrorSeverity,
)

router = APIRouter()
logger = logging.getLogger(__name__)


class FrontendErrorReport(BaseModel):
    """Request model for frontend error reports"""
    message: str
    stack: Optional[str] = None
    url: Optional[str] = None
    component: Optional[str] = None
    user_agent: Optional[str] = None
    additional_context: Optional[Dict[str, Any]] = None
    
    class Config:
        str_strip_whitespace = True
        max_anystr_length = 5000


class ErrorReportResponse(BaseModel):
    """Response model for error reports"""
    status: str
    error_id: str
    message: str
    timestamp: str


@router.post("/report", response_model=ErrorReportResponse)
async def report_frontend_error(
    request: Request,
    error_report: FrontendErrorReport,
    user: Optional[dict] = Depends(rbac_manager.get_current_user_optional),
) -> ErrorReportResponse:
    """
    Report frontend errors to backend
    
    This endpoint allows the frontend to report JavaScript errors,
    React component errors, and other client-side issues.
    
    Args:
        error_report: Error details from frontend
        user: Authenticated user (optional)
        
    Returns:
        ErrorReportResponse with error ID for tracking
    """
    try:
        # Get user ID if authenticated
        user_id = user.get("username") if user else None
        
        # Get client IP for context
        client_ip = request.client.host if request.client else None
        
        # Add request context to additional_context
        context = error_report.additional_context or {}
        context["client_ip"] = client_ip
        context["user_agent"] = error_report.user_agent or request.headers.get("User-Agent")
        
        # Report the error
        error = ErrorReporterService.report_frontend_error(
            message=error_report.message,
            stack=error_report.stack,
            url=error_report.url,
            user_id=user_id,
            component=error_report.component,
            additional_context=context,
        )
        
        logger.info(f"Frontend error reported: {error.error_id}")
        
        return ErrorReportResponse(
            status="reported",
            error_id=error.error_id,
            message="Error has been logged",
            timestamp=error.timestamp,
        )
        
    except Exception as e:
        logger.error(f"Failed to report frontend error: {e}")
        raise HTTPException(status_code=500, detail="Failed to report error")


@router.get("/recent")
async def get_recent_errors(
    limit: int = 20,
    layer: Optional[str] = None,
    severity: Optional[str] = None,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get recent errors for debugging (admin only)
    
    Args:
        limit: Maximum number of errors to return (default 20)
        layer: Filter by error layer (optional)
        severity: Filter by severity (optional)
        user: Authenticated user
        
    Returns:
        Dict with recent errors
    """
    # Only admins can view error logs
    if user.get("role") != Role.ADMIN:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Parse filters
        layer_filter = ErrorLayer(layer) if layer else None
        severity_filter = ErrorSeverity(severity) if severity else None
        
        errors = ErrorReporterService.get_recent_errors(
            limit=min(limit, 100),  # Cap at 100
            layer=layer_filter,
            severity=severity_filter,
        )
        
        return {
            "status": "success",
            "count": len(errors),
            "errors": errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid filter value: {e}")
    except Exception as e:
        logger.error(f"Failed to get recent errors: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve errors")
