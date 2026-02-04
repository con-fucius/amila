"""
Dashboard API Endpoints
Automated dashboard generation without replacing Plotly
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging

from app.services.dashboard_generator import DashboardGenerator
from app.core.rbac import rbac_manager

router = APIRouter()
logger = logging.getLogger(__name__)


class DashboardGenerateRequest(BaseModel):
    sql_query: str
    query_results: Dict[str, Any]
    title: Optional[str] = None
    description: Optional[str] = None


class DashboardPatternRequest(BaseModel):
    pattern_name: str
    parameters: Optional[Dict[str, Any]] = None


@router.post("/generate")
async def generate_dashboard(
    request: DashboardGenerateRequest,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Generate dashboard specification from query results.
    
    **Does NOT replace Plotly**: Returns spec that can be rendered by frontend.
    
    Args:
        request: Dashboard generation request with query and results
        
    Returns:
        Dashboard specification with chart recommendations
    """
    try:
        result = await DashboardGenerator.generate_from_query(
            sql_query=request.sql_query,
            query_results=request.query_results,
            title=request.title,
            description=request.description
        )
        
        if result.get("status") == "success":
            # Store dashboard scoped to owner
            dashboard_id = await DashboardGenerator.store_dashboard(
                result["dashboard"],
                owner_id=user.get("username") or user.get("user_id")
            )
            result["dashboard"]["id"] = dashboard_id
        
        return result
        
    except Exception as e:
        logger.error(f"Dashboard generation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Dashboard generation failed: {str(e)}"
        )


@router.post("/generate/pattern")
async def generate_from_pattern(
    request: DashboardPatternRequest,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Generate dashboard from predefined pattern.
    
    Args:
        request: Pattern name and parameters
        
    Returns:
        Dashboard specification
    """
    try:
        result = await DashboardGenerator.generate_from_pattern(
            pattern_name=request.pattern_name,
            parameters=request.parameters
        )
        
        if result.get("status") == "success":
            # Store dashboard scoped to owner
            dashboard_id = await DashboardGenerator.store_dashboard(
                result["dashboard"],
                owner_id=user.get("username") or user.get("user_id")
            )
            result["dashboard"]["id"] = dashboard_id
        
        return result
        
    except Exception as e:
        logger.error(f"Pattern dashboard generation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Pattern generation failed: {str(e)}"
        )


@router.get("/")
async def list_dashboards(
    limit: int = 20,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    List all stored dashboards.
    
    Args:
        limit: Maximum number to return
        
    Returns:
        List of dashboard metadata
    """
    try:
        dashboards = await DashboardGenerator.list_dashboards(
            limit=limit,
            user_id=user.get("username") or user.get("user_id")
        )
        
        return {
            "status": "success",
            "dashboards": dashboards,
            "count": len(dashboards)
        }
        
    except Exception as e:
        logger.error(f"Dashboard listing failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Dashboard listing failed: {str(e)}"
        )


@router.get("/{dashboard_id}")
async def get_dashboard(
    dashboard_id: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get dashboard specification by ID.
    
    Args:
        dashboard_id: Dashboard ID
        
    Returns:
        Full dashboard specification
    """
    try:
        dashboard = await DashboardGenerator.get_dashboard(dashboard_id)
        
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")
        
        return {
            "status": "success",
            "dashboard": dashboard
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard retrieval failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Dashboard retrieval failed: {str(e)}"
        )


@router.delete("/{dashboard_id}")
async def delete_dashboard(
    dashboard_id: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Delete dashboard.
    
    **RBAC**: Requires admin permission or dashboard owner
    
    Args:
        dashboard_id: Dashboard ID
        
    Returns:
        Deletion status
    """
    try:
        # Admin check (simplified - in production, check ownership)
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        success = await DashboardGenerator.delete_dashboard(dashboard_id)
        
        return {
            "status": "success" if success else "error",
            "message": f"Dashboard {dashboard_id} deleted"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard deletion failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Dashboard deletion failed: {str(e)}"
        )
