"""
LDAP Integration API Endpoints

Provides endpoints for LDAP/Active Directory integration testing and management.
Allows administrators to test LDAP connections, verify user groups, and manage
AD integration settings.

Features:
- Test LDAP connectivity
- Verify user group membership
- Resolve user permissions from AD
- Manage LDAP cache
- LDAP configuration status

Security:
- Admin-only access for sensitive operations
- Correlation ID tracking
- Structured audit logging
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status, Query
import logging

from app.core.auth import get_current_user, require_permissions
from app.services.ldap_integration_service import (
    ldap_service,
    get_user_ad_groups,
    resolve_user_ldap_permissions,
    invalidate_ldap_cache,
    get_ldap_status
)

logger = logging.getLogger(__name__)

router = APIRouter()


# Pydantic Models
class LDAPTestConnectionRequest(BaseModel):
    """Request to test LDAP connection with custom settings."""
    server_url: Optional[str] = Field(None, description="LDAP server URL (optional, uses config if not provided)")
    bind_dn: Optional[str] = Field(None, description="Bind DN (optional)")
    bind_password: Optional[str] = Field(None, description="Bind password (optional)")
    base_dn: Optional[str] = Field(None, description="Base DN (optional)")
    use_ssl: Optional[bool] = Field(None, description="Use SSL/TLS")


class LDAPTestConnectionResponse(BaseModel):
    """LDAP connection test result."""
    success: bool
    message: str
    server: Optional[str] = None
    response_time_ms: Optional[float] = None
    error: Optional[str] = None


class LDAPUserGroupsRequest(BaseModel):
    """Request to get user AD groups."""
    user_id: str = Field(..., description="User ID or email to lookup")
    use_cache: bool = Field(True, description="Use cached results if available")


class LDAPUserGroupsResponse(BaseModel):
    """User AD groups response."""
    user_id: str
    groups: List[str]
    group_count: int
    source: str = Field(..., description="ldap or static")
    resolved_at: str
    ldap_status: str


class LDAPUserPermissionsResponse(BaseModel):
    """Comprehensive LDAP permissions response."""
    user_id: str
    groups: List[str]
    group_count: int
    source: str
    resolved_at: str
    ldap_status: str
    ldap_enabled: bool
    server_url: Optional[str] = None


class LDAPStatusResponse(BaseModel):
    """LDAP service status."""
    status: str = Field(..., description="connected, disconnected, error, or disabled")
    enabled: bool
    server_url: Optional[str] = None
    base_dn: Optional[str] = None
    cache_ttl_seconds: int
    timestamp: str


class LDAPCacheInvalidateRequest(BaseModel):
    """Request to invalidate LDAP cache."""
    user_id: Optional[str] = Field(None, description="Specific user to invalidate, or all if not provided")


class LDAPCacheInvalidateResponse(BaseModel):
    """LDAP cache invalidation result."""
    success: bool
    message: str
    user_id: Optional[str] = None


# API Endpoints

@router.get("/status", response_model=LDAPStatusResponse)
async def get_ldap_service_status(
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "view_ldap_status"]))
) -> LDAPStatusResponse:
    """
    Get LDAP service status and configuration.
    
    Returns current LDAP connection status, configuration details,
    and caching settings. Requires admin or view_ldap_status permission.
    """
    try:
        status_info = await get_ldap_status()
        from datetime import datetime, timezone
        
        return LDAPStatusResponse(
            status=status_info.get("status", "unknown"),
            enabled=status_info.get("enabled", False),
            server_url=status_info.get("server_url"),
            base_dn=status_info.get("base_dn"),
            cache_ttl_seconds=status_info.get("cache_ttl", 3600),
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    except Exception as e:
        logger.error(f"Failed to get LDAP status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get LDAP status: {str(e)}"
        )


@router.post("/test-connection", response_model=LDAPTestConnectionResponse)
async def test_ldap_connection(
    request: LDAPTestConnectionRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> LDAPTestConnectionResponse:
    """
    Test LDAP connection with current or custom configuration.
    
    Tests connectivity to the LDAP server using either the current
    configured settings or custom settings provided in the request.
    Admin permission required.
    """
    from datetime import datetime, timezone
    import time
    
    start_time = time.time()
    
    try:
        # If custom settings provided, temporarily use them
        if request.server_url:
            # Create temporary config for testing
            original_config = ldap_service.config
            
            # Create new config with provided values
            from app.services.ldap_integration_service import LDAPConfig
            test_config = LDAPConfig(
                enabled=True,
                server_url=request.server_url,
                bind_dn=request.bind_dn or original_config.bind_dn,
                bind_password=request.bind_password or original_config.bind_password,
                base_dn=request.base_dn or original_config.base_dn,
                user_search_filter=original_config.user_search_filter,
                group_search_filter=original_config.group_search_filter,
                use_ssl=request.use_ssl if request.use_ssl is not None else original_config.use_ssl,
                timeout_seconds=original_config.timeout_seconds,
                cache_ttl_seconds=original_config.cache_ttl_seconds
            )
            
            # Temporarily replace config
            ldap_service.config = test_config
        
        # Test the connection
        result = await ldap_service.test_connection()
        
        response_time = (time.time() - start_time) * 1000
        
        return LDAPTestConnectionResponse(
            success=result.get("success", False),
            message=result.get("message") or result.get("error", "Unknown result"),
            server=request.server_url or ldap_service.config.server_url,
            response_time_ms=round(response_time, 2),
            error=result.get("error") if not result.get("success") else None
        )
        
    except Exception as e:
        logger.error(f"LDAP connection test failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"LDAP connection test failed: {str(e)}"
        )


@router.get("/user-groups/{user_id}", response_model=LDAPUserGroupsResponse)
async def get_user_groups(
    user_id: str,
    use_cache: bool = Query(True, description="Use cached results if available"),
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "view_user_groups"]))
) -> LDAPUserGroupsResponse:
    """
    Get AD groups for a specific user.
    
    Resolves Active Directory group membership for the specified user.
    Supports caching for performance. Requires admin or view_user_groups permission.
    """
    try:
        from datetime import datetime, timezone
        
        # Check if user can only view their own groups (unless admin)
        is_admin = "admin" in current_user.get("permissions", []) or current_user.get("role") == "admin"
        if not is_admin and user_id != current_user.get("id"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Can only view your own group memberships"
            )
        
        # Get groups from LDAP
        groups = await get_user_ad_groups(user_id, use_cache=use_cache)
        
        # Get status
        status_info = await get_ldap_status()
        
        return LDAPUserGroupsResponse(
            user_id=user_id,
            groups=groups,
            group_count=len(groups),
            source="ldap" if status_info.get("enabled") else "static",
            resolved_at=datetime.now(timezone.utc).isoformat(),
            ldap_status=status_info.get("status", "unknown")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get user groups for {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user groups: {str(e)}"
        )


@router.get("/user-permissions/{user_id}", response_model=LDAPUserPermissionsResponse)
async def get_user_permissions(
    user_id: str,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> LDAPUserPermissionsResponse:
    """
    Get comprehensive LDAP permissions for a user.
    
    Returns detailed permission information including group memberships,
    LDAP status, and resolution metadata. Admin permission required.
    """
    try:
        # Resolve permissions
        permissions = await resolve_user_ldap_permissions(user_id)
        
        # Get status for additional info
        status_info = await get_ldap_status()
        
        return LDAPUserPermissionsResponse(
            user_id=permissions.get("user_id", user_id),
            groups=permissions.get("groups", []),
            group_count=permissions.get("group_count", 0),
            source=permissions.get("source", "static"),
            resolved_at=permissions.get("resolved_at"),
            ldap_status=permissions.get("ldap_status", "unknown"),
            ldap_enabled=status_info.get("enabled", False),
            server_url=status_info.get("server_url") if status_info.get("enabled") else None
        )
        
    except Exception as e:
        logger.error(f"Failed to get user permissions for {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user permissions: {str(e)}"
        )


@router.post("/cache/invalidate", response_model=LDAPCacheInvalidateResponse)
async def invalidate_cache(
    request: LDAPCacheInvalidateRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> LDAPCacheInvalidateResponse:
    """
    Invalidate LDAP cache for a user or all users.
    
    Clears cached LDAP data to force fresh lookups on next request.
    Admin permission required.
    """
    try:
        if request.user_id:
            # Invalidate specific user
            await invalidate_ldap_cache(request.user_id)
            return LDAPCacheInvalidateResponse(
                success=True,
                message=f"LDAP cache invalidated for user {request.user_id}",
                user_id=request.user_id
            )
        else:
            # Invalidate all users (would need to scan keys in production)
            # For now, just return success with message about limitation
            return LDAPCacheInvalidateResponse(
                success=True,
                message="LDAP cache invalidation for all users requires Redis SCAN operation (not implemented)",
                user_id=None
            )
        
    except Exception as e:
        logger.error(f"Failed to invalidate LDAP cache: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to invalidate cache: {str(e)}"
        )


@router.post("/cache/invalidate/{user_id}", response_model=LDAPCacheInvalidateResponse)
async def invalidate_user_cache(
    user_id: str,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> LDAPCacheInvalidateResponse:
    """
    Invalidate LDAP cache for a specific user.
    
    Clears cached LDAP data for the specified user.
    Admin permission required.
    """
    try:
        await invalidate_ldap_cache(user_id)
        return LDAPCacheInvalidateResponse(
            success=True,
            message=f"LDAP cache invalidated for user {user_id}",
            user_id=user_id
        )
        
    except Exception as e:
        logger.error(f"Failed to invalidate LDAP cache for {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to invalidate cache: {str(e)}"
        )


@router.get("/my-groups", response_model=LDAPUserGroupsResponse)
async def get_my_groups(
    use_cache: bool = Query(True, description="Use cached results if available"),
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> LDAPUserGroupsResponse:
    """
    Get current user's AD groups.
    
    Returns the Active Directory group membership for the currently
    authenticated user. Available to all authenticated users.
    """
    try:
        from datetime import datetime, timezone
        
        user_id = current_user.get("id", "unknown")
        
        # Get groups from LDAP
        groups = await get_user_ad_groups(user_id, use_cache=use_cache)
        
        # Get status
        status_info = await get_ldap_status()
        
        return LDAPUserGroupsResponse(
            user_id=user_id,
            groups=groups,
            group_count=len(groups),
            source="ldap" if status_info.get("enabled") else "static",
            resolved_at=datetime.now(timezone.utc).isoformat(),
            ldap_status=status_info.get("status", "unknown")
        )
        
    except Exception as e:
        logger.error(f"Failed to get my groups for {current_user.get('id')}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get groups: {str(e)}"
        )
