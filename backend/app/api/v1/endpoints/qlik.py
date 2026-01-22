"""
Qlik Sense API Endpoints
Provides read-only access to Qlik Sense dashboards and apps
"""

import logging
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.core.auth import get_current_user
from app.core.exceptions import ExternalServiceException
from app.services.qlik_service import create_qlik_client, QlikSenseClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/qlik", tags=["qlik"])


class QlikHealthResponse(BaseModel):
    status: str
    version: Optional[str] = None
    product: Optional[str] = None
    error: Optional[str] = None


class QlikAppListResponse(BaseModel):
    status: str
    apps: list
    count: int


class QlikAppResponse(BaseModel):
    status: str
    app: dict


class QlikSheetListResponse(BaseModel):
    status: str
    sheets: list
    count: int


@router.get("/health", response_model=QlikHealthResponse)
async def check_qlik_health(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Check Qlik Sense API health
    
    Requires authentication.
    """
    try:
        client = create_qlik_client()
        result = await client.health_check()
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Qlik Sense not configured: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Qlik health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Qlik Sense unavailable: {str(e)}"
        )


@router.get("/apps", response_model=QlikAppListResponse)
async def list_qlik_apps(
    filter_query: Optional[str] = None,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    List Qlik Sense apps (read-only)
    
    Args:
        filter_query: Optional filter (e.g., "name eq 'Sales Dashboard'")
        
    Returns:
        List of Qlik apps with metadata
    """
    try:
        client = create_qlik_client()
        result = await client.list_apps(
            filter_query=filter_query,
            user_id=current_user.get("username")
        )
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Qlik Sense not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to list Qlik apps: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Qlik apps"
        )


@router.get("/apps/{app_id}", response_model=QlikAppResponse)
async def get_qlik_app(
    app_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get Qlik Sense app details (read-only)
    
    Args:
        app_id: Qlik app GUID
        
    Returns:
        App details
    """
    try:
        client = create_qlik_client()
        result = await client.get_app(
            app_id=app_id,
            user_id=current_user.get("username")
        )
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Qlik Sense not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to get Qlik app: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Qlik app"
        )


@router.get("/apps/{app_id}/sheets", response_model=QlikSheetListResponse)
async def list_qlik_sheets(
    app_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    List sheets in a Qlik Sense app (read-only)
    
    Args:
        app_id: Qlik app GUID
        
    Returns:
        List of sheets
    """
    try:
        client = create_qlik_client()
        result = await client.list_sheets(
            app_id=app_id,
            user_id=current_user.get("username")
        )
        await client.close()
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Qlik Sense not configured: {str(e)}"
        )
    except ExternalServiceException as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to list Qlik sheets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Qlik sheets"
        )

