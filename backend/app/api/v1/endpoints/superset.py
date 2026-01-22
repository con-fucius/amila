"""
Apache Superset API Endpoints
Provides dashboard management and auto-generation capabilities
"""

import logging
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Body
from pydantic import BaseModel, Field

from app.core.auth import get_current_user
from app.core.exceptions import ExternalServiceException
from app.services.superset_service import create_superset_client
from app.services.superset_dashboard_service import create_superset_dashboard_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/superset", tags=["superset"])


class SupersetHealthResponse(BaseModel):
    status: str
    error: Optional[str] = None


class SupersetDashboardListResponse(BaseModel):
    status: str
    dashboards: list
    count: int


class SupersetDashboardResponse(BaseModel):
    status: str
    dashboard: dict


class SupersetChartCreateRequest(BaseModel):
    chart_config: Dict[str, Any] = Field(..., description="Chart configuration")


class SupersetChartResponse(BaseModel):
    status: str
    chart: dict


class SupersetDashboardGenerateRequest(BaseModel):
    query_result: Dict[str, Any] = Field(..., description="Query execution result")
    dashboard_title: str = Field(..., description="Title for the dashboard")


class SupersetDashboardGenerateResponse(BaseModel):
    status: str
    message: str
    visualization_recommendation: Dict[str, Any]
    note: str


@router.get("/health", response_model=SupersetHealthResponse)
async def check_superset_health(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Check Apache Superset API health
    
    Requires authentication.
    """
    try:
        client = create_superset_client()
        result = await client.health_check()
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Apache Superset not configured: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Superset health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Apache Superset unavailable: {str(e)}"
        )


@router.get("/dashboards", response_model=SupersetDashboardListResponse)
async def list_superset_dashboards(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    List Apache Superset dashboards (read-only)
    
    Returns:
        List of Superset dashboards
    """
    try:
        client = create_superset_client()
        result = await client.list_dashboards(user_id=current_user.get("username"))
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Apache Superset not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to list Superset dashboards: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Superset dashboards"
        )


@router.get("/dashboards/{dashboard_id}", response_model=SupersetDashboardResponse)
async def get_superset_dashboard(
    dashboard_id: int,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get Apache Superset dashboard details
    
    Args:
        dashboard_id: Dashboard ID
        
    Returns:
        Dashboard details
    """
    try:
        client = create_superset_client()
        result = await client.get_dashboard(
            dashboard_id=dashboard_id,
            user_id=current_user.get("username")
        )
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Apache Superset not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to get Superset dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Superset dashboard"
        )


@router.post("/charts", response_model=SupersetChartResponse)
async def create_superset_chart(
    request: SupersetChartCreateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Create a new chart in Apache Superset
    
    Args:
        request: Chart configuration
        
    Returns:
        Created chart details
    """
    try:
        client = create_superset_client()
        result = await client.create_chart(
            chart_config=request.chart_config,
            user_id=current_user.get("username")
        )
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Apache Superset not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to create Superset chart: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create Superset chart"
        )


@router.post("/dashboards/generate", response_model=SupersetDashboardGenerateResponse)
async def generate_superset_dashboard(
    request: SupersetDashboardGenerateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Auto-generate Superset dashboard from query results
    
    Args:
        request: Query result and dashboard title
        
    Returns:
        Dashboard generation result with visualization recommendations
    """
    try:
        client = create_superset_client()
        dashboard_service = create_superset_dashboard_service()
        
        result = await dashboard_service.generate_dashboard_from_query(
            superset_client=client,
            query_result=request.query_result,
            dashboard_title=request.dashboard_title,
            user_id=current_user.get("username")
        )
        
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Apache Superset not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to generate Superset dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate Superset dashboard"
        )

