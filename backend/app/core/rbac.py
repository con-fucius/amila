"""
Role-Based Access Control (RBAC) Module
Implements fine-grained permission system with role hierarchy
"""

import logging
from typing import List, Optional, Callable, Set
from functools import wraps
from enum import Enum

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.auth import AuthenticationManager
from app.core.config import settings

logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer()


class Role(str, Enum):
    """User roles with hierarchical permissions"""
    ADMIN = "admin"          # Full system access
    DEVELOPER = "developer"  # Development and debugging access
    ANALYST = "analyst"      # Query execution + schema read
    VIEWER = "viewer"        # Read-only query results
    GUEST = "guest"          # Limited demo access


class Permission(str, Enum):
    """Granular permissions"""
    # Query permissions
    QUERY_EXECUTE = "query:execute"
    QUERY_APPROVE = "query:approve"
    QUERY_VIEW = "query:view"
    QUERY_HISTORY = "query:history"
    
    # Schema permissions
    SCHEMA_READ = "schema:read"
    SCHEMA_MODIFY = "schema:modify"
    
    # System permissions
    SYSTEM_HEALTH = "system:health"
    SYSTEM_METRICS = "system:metrics"
    SYSTEM_CONFIG = "system:config"
    
    # Admin permissions
    ADMIN_USERS = "admin:users"
    ADMIN_ROLES = "admin:roles"
    ADMIN_AUDIT = "admin:audit"


# Role to permissions mapping
ROLE_PERMISSIONS: dict[Role, Set[Permission]] = {
    Role.GUEST: {
        Permission.QUERY_VIEW,
        Permission.SYSTEM_HEALTH,
    },
    Role.VIEWER: {
        Permission.QUERY_VIEW,
        Permission.QUERY_HISTORY,
        Permission.SYSTEM_HEALTH,
        Permission.SYSTEM_METRICS,
    },
    Role.ANALYST: {
        Permission.QUERY_EXECUTE,
        Permission.QUERY_VIEW,
        Permission.QUERY_HISTORY,
        Permission.QUERY_APPROVE,
        Permission.SCHEMA_READ,
        Permission.SYSTEM_HEALTH,
        Permission.SYSTEM_METRICS,
    },
    Role.DEVELOPER: {
        # Developers have analyst permissions plus system config and diagnostics
        Permission.QUERY_EXECUTE,
        Permission.QUERY_VIEW,
        Permission.QUERY_HISTORY,
        Permission.QUERY_APPROVE,
        Permission.SCHEMA_READ,
        Permission.SYSTEM_HEALTH,
        Permission.SYSTEM_METRICS,
        Permission.SYSTEM_CONFIG,
    },
    Role.ADMIN: {
        # Admins have all permissions
        *[p for p in Permission]
    }
}


class RBACManager:
    """Role-Based Access Control manager"""
    
    def __init__(self):
        self.auth_manager = AuthenticationManager()
    
    def get_role_permissions(self, role: Role) -> Set[Permission]:
        """Get all permissions for a role"""
        return ROLE_PERMISSIONS.get(role, set())
    
    def has_permission(self, role: Role, permission: Permission) -> bool:
        """Check if role has specific permission"""
        return permission in self.get_role_permissions(role)
    
    def has_any_permission(self, role: Role, permissions: List[Permission]) -> bool:
        """Check if role has any of the specified permissions"""
        role_perms = self.get_role_permissions(role)
        return any(p in role_perms for p in permissions)
    
    def has_all_permissions(self, role: Role, permissions: List[Permission]) -> bool:
        """Check if role has all specified permissions"""
        role_perms = self.get_role_permissions(role)
        return all(p in role_perms for p in permissions)
    
    def verify_permission(self, role: Role, permission: Permission) -> None:
        """
        Verify role has permission, raise exception if not
        
        Raises:
            HTTPException: If permission denied
        """
        if not self.has_permission(role, permission):
            logger.warning(f"Permission denied: role={role.value}, permission={permission.value}")
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient permissions. Required: {permission.value}"
            )
    
    async def get_current_user(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
        """
        Extract and validate user from JWT token
        
        Args:
            credentials: HTTP Bearer token
            
        Returns:
            User data with role
            
        Raises:
            HTTPException: If token invalid or expired
        """
        if not credentials:
            raise HTTPException(
                status_code=401,
                detail="Missing authentication token"
            )
        
        token = credentials.credentials
        
        # DEV ONLY: Bypass for temp token - RESTRICTED to development environment only
        # SECURITY: In production, this bypass is completely disabled
        if token == "temp-dev-token":
            from app.core.config import settings
            if settings.is_development:
                logger.warning("DEV ONLY: temp-dev-token used - returns VIEWER role (not admin)")
                return {
                    "username": "dev_user",
                    "role": Role.VIEWER,  # SECURITY: Restricted to VIEWER, not ADMIN
                    "token_data": {"sub": "dev_user", "role": "viewer"}
                }
            else:
                logger.error("SECURITY: temp-dev-token rejected in non-development environment")
                raise HTTPException(
                    status_code=401,
                    detail="Development tokens not allowed in this environment"
                )
        
        # Decode token
        payload = self.auth_manager.decode_token(token, token_type="access")
        
        if not payload:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )
        
        username = payload.get("sub")
        role = payload.get("role", "viewer")
        
        if not username:
            raise HTTPException(
                status_code=401,
                detail="Invalid token payload"
            )
        
        logger.info(f"Authenticated user: {username}, role: {role}")
        
        return {
            "username": username,
            "role": Role(role),
            "token_data": payload
        }

    async def get_current_user_optional(self, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[dict]:
        """
        Optional authentication - returns user if authenticated, None otherwise.
        This mirrors the module-level helper but delegates to the instance method.
        """
        if not credentials:
            return None
        try:
            return await self.get_current_user(credentials)
        except HTTPException:
            return None


# Global RBAC manager instance
rbac_manager = RBACManager()


def require_permission(permission: Permission):
    """
    Decorator to require specific permission for endpoint access
    
    Usage:
        @router.get("/admin/config")
        @require_permission(Permission.SYSTEM_CONFIG)
        async def get_config(user: dict = Depends(rbac_manager.get_current_user)):
            return {"config": "data"}
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract user from kwargs (injected by FastAPI)
            user = kwargs.get("user")
            
            if not user:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )
            
            user_role = user.get("role")
            
            if not user_role:
                raise HTTPException(
                    status_code=403,
                    detail="User role not found"
                )
            
            # Verify permission
            rbac_manager.verify_permission(user_role, permission)
            
            # Execute endpoint
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def require_role(required_role: Role):
    """
    Decorator to require specific role (or higher in hierarchy)
    
    Role hierarchy: ADMIN > ANALYST > VIEWER > GUEST
    
    Usage:
        @router.post("/queries/execute")
        @require_role(Role.ANALYST)
        async def execute_query(user: dict = Depends(rbac_manager.get_current_user)):
            return {"status": "executed"}
    """
    role_hierarchy = {
        Role.GUEST: 0,
        Role.VIEWER: 1,
        Role.ANALYST: 2,
        Role.DEVELOPER: 3,
        Role.ADMIN: 4,
    }
    
    required_level = role_hierarchy.get(required_role, 0)
    
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract user from kwargs
            user = kwargs.get("user")
            
            if not user:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )
            
            user_role = user.get("role")
            
            if not user_role:
                raise HTTPException(
                    status_code=403,
                    detail="User role not found"
                )
            
            user_level = role_hierarchy.get(user_role, 0)
            
            # Check if user role meets requirement
            if user_level < required_level:
                logger.warning(
                    f"Role requirement not met: user={user.get('username')}, "
                    f"role={user_role.value}, required={required_role.value}"
                )
                raise HTTPException(
                    status_code=403,
                    detail=f"Insufficient role. Required: {required_role.value} or higher"
                )
            
            # Execute endpoint
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def require_any_role(roles: List[Role]):
    """
    Decorator to require any of the specified roles
    
    Usage:
        @router.get("/data")
        @require_any_role([Role.ANALYST, Role.ADMIN])
        async def get_data(user: dict = Depends(rbac_manager.get_current_user)):
            return {"data": "..."}
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user = kwargs.get("user")
            
            if not user:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )
            
            user_role = user.get("role")
            
            if not user_role or user_role not in roles:
                logger.warning(
                    f"Role requirement not met: user={user.get('username')}, "
                    f"role={user_role.value if user_role else 'none'}, "
                    f"required_any={[r.value for r in roles]}"
                )
                raise HTTPException(
                    status_code=403,
                    detail=f"Insufficient role. Required one of: {[r.value for r in roles]}"
                )
            
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def get_current_user_optional(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[dict]:
    """
    Backwards-compatible module-level helper that forwards to the global RBAC manager.
    """
    return rbac_manager.get_current_user_optional(credentials)


# Convenience dependency for common use cases
async def require_authentication(user: dict = Depends(rbac_manager.get_current_user)) -> dict:
    """
    Simple authentication requirement without role checks
    Returns authenticated user
    """
    return user


async def require_analyst_role(user: dict = Depends(rbac_manager.get_current_user)) -> dict:
    """
    Require analyst role or higher
    Convenience dependency for query execution endpoints
    """
    rbac_manager.verify_permission(user["role"], Permission.QUERY_EXECUTE)
    return user


async def require_admin_role(user: dict = Depends(rbac_manager.get_current_user)) -> dict:
    """
    Require admin role
    Convenience dependency for admin endpoints
    """
    if user["role"] != Role.ADMIN:
        raise HTTPException(
            status_code=403,
            detail="Admin role required"
        )
    return user


async def require_developer_role(user: dict = Depends(rbac_manager.get_current_user)) -> dict:
    """
    Require developer role or higher
    Convenience dependency for developer/diagnostic endpoints
    """
    role_hierarchy = {
        Role.GUEST: 0,
        Role.VIEWER: 1,
        Role.ANALYST: 2,
        Role.DEVELOPER: 3,
        Role.ADMIN: 4,
    }
    
    user_level = role_hierarchy.get(user["role"], 0)
    required_level = role_hierarchy.get(Role.DEVELOPER, 3)
    
    if user_level < required_level:
        raise HTTPException(
            status_code=403,
            detail="Developer role or higher required"
        )
    return user