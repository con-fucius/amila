"""
Role-Based Query Limits Service

Enforces query result limits and quotas based on user roles.
Implements tiered access controls as specified in security requirements.

Tiers:
- Viewer: 100 rows max, no write operations
- Analyst: 10,000 rows max, read-only
- Developer: 100,000 rows max, limited writes
- Admin: Unlimited rows, full access
"""

import logging
from typing import Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone, timedelta

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class UserRole(str, Enum):
    """User role tiers"""
    GUEST = "guest"
    VIEWER = "viewer"
    ANALYST = "analyst"
    DEVELOPER = "developer"
    ADMIN = "admin"


@dataclass
class RoleLimits:
    """Query limits for a specific role"""
    max_rows: int
    daily_query_quota: int
    daily_cost_quota: float  # in USD
    allowed_operations: list
    can_export: bool
    can_schedule: bool
    max_tables: int
    max_joins: int


class RoleBasedLimitsService:
    """
    Service for enforcing role-based query limits.
    
    Features:
    - Row limits per role tier
    - Daily query quotas
    - Daily cost quotas
    - Operation restrictions
    """
    
    # Default limits per role
    ROLE_LIMITS = {
        UserRole.GUEST: RoleLimits(
            max_rows=50,
            daily_query_quota=10,
            daily_cost_quota=0.50,
            allowed_operations=["SELECT"],
            can_export=False,
            can_schedule=False,
            max_tables=2,
            max_joins=1
        ),
        UserRole.VIEWER: RoleLimits(
            max_rows=100,
            daily_query_quota=50,
            daily_cost_quota=5.00,
            allowed_operations=["SELECT"],
            can_export=False,
            can_schedule=False,
            max_tables=3,
            max_joins=2
        ),
        UserRole.ANALYST: RoleLimits(
            max_rows=10000,
            daily_query_quota=200,
            daily_cost_quota=50.00,
            allowed_operations=["SELECT"],
            can_export=True,
            can_schedule=False,
            max_tables=6,
            max_joins=5
        ),
        UserRole.DEVELOPER: RoleLimits(
            max_rows=100000,
            daily_query_quota=500,
            daily_cost_quota=200.00,
            allowed_operations=["SELECT", "INSERT", "UPDATE", "DELETE"],
            can_export=True,
            can_schedule=True,
            max_tables=10,
            max_joins=8
        ),
        UserRole.ADMIN: RoleLimits(
            max_rows=0,  # 0 means unlimited
            daily_query_quota=0,  # 0 means unlimited
            daily_cost_quota=0.0,  # 0.0 means unlimited
            allowed_operations=["SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"],
            can_export=True,
            can_schedule=True,
            max_tables=0,
            max_joins=0
        ),
    }
    
    # Redis key prefixes
    # Sensitive items for HITL
    SENSITIVE_TABLES = ["SALARIES", "BONUSES", "CLIENT_PII", "USER_CREDENTIALS", "AUDIT_LOGS_SENSITIVE"]
    SENSITIVE_COLUMNS = ["SSN", "PASSWORD_HASH", "CREDIT_CARD", "SECRET_KEY", "SALARY_AMOUNT"]

    DAILY_QUOTA_PREFIX = "quota:daily:"
    COST_TRACKING_PREFIX = "quota:cost:"
    
    @classmethod
    def get_role_limits(cls, role: Union[str, UserRole]) -> RoleLimits:
        """Get limits for a specific role"""
        if isinstance(role, str):
            role = role.lower()
            try:
                role_enum = UserRole(role)
            except ValueError:
                logger.warning(f"Unknown role: {role}, defaulting to VIEWER")
                role_enum = UserRole.VIEWER
        else:
            role_enum = role
        
        return cls.ROLE_LIMITS.get(role_enum, cls.ROLE_LIMITS[UserRole.VIEWER])
    
    @classmethod
    def get_max_rows_for_role(cls, role: Union[str, UserRole]) -> int:
        """Get maximum rows allowed for a role"""
        limits = cls.get_role_limits(role)
        return limits.max_rows

    @classmethod
    def get_max_tables_for_role(cls, role: Union[str, UserRole]) -> int:
        """Get maximum tables allowed for a role (0 means unlimited)."""
        limits = cls.get_role_limits(role)
        return limits.max_tables

    @classmethod
    def get_max_joins_for_role(cls, role: Union[str, UserRole]) -> int:
        """Get maximum joins allowed for a role (0 means unlimited)."""
        limits = cls.get_role_limits(role)
        return limits.max_joins
    
    @classmethod
    def can_execute_operation(cls, role: Union[str, UserRole], operation: str) -> bool:
        """Check if a role can execute a specific SQL operation"""
        limits = cls.get_role_limits(role)
        return operation.upper() in [op.upper() for op in limits.allowed_operations]
    
    @classmethod
    def can_export(cls, role: Union[str, UserRole]) -> bool:
        """Check if a role can export data"""
        limits = cls.get_role_limits(role)
        return limits.can_export
    
    @classmethod
    async def check_and_increment_query_quota(
        cls,
        user_id: str,
        role: Union[str, UserRole]
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if user has remaining daily query quota and increment.
        
        Returns:
            Tuple of (allowed, quota_info)
        """
        limits = cls.get_role_limits(role)
        
        # Admin has unlimited quota
        if limits.daily_query_quota == 0:
            return True, {
                "allowed": True,
                "quota_type": "unlimited",
                "remaining": None
            }
        
        # Build quota key
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        quota_key = f"{cls.DAILY_QUOTA_PREFIX}{user_id}:{today}"
        
        try:
            # Get current count
            current = await redis_client._client.get(quota_key)
            current_count = int(current) if current else 0
            
            if current_count >= limits.daily_query_quota:
                return False, {
                    "allowed": False,
                    "quota_type": "daily_query",
                    "limit": limits.daily_query_quota,
                    "used": current_count,
                    "remaining": 0,
                    "resets_at": (datetime.now(timezone.utc) + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).isoformat()
                }
            
            # Increment count
            new_count = await redis_client._client.incr(quota_key)
            
            # Set expiry if new key
            if new_count == 1:
                await redis_client._client.expire(quota_key, 86400)  # 24 hours
            
            return True, {
                "allowed": True,
                "quota_type": "daily_query",
                "limit": limits.daily_query_quota,
                "used": new_count,
                "remaining": limits.daily_query_quota - new_count
            }
            
        except Exception as e:
            logger.error(f"Failed to check query quota: {e}")
            # Fail open (allow) but log error
            return True, {
                "allowed": True,
                "quota_type": "daily_query",
                "error": str(e),
                "warning": "Quota check failed, allowing query"
            }
    
    @classmethod
    async def check_cost_quota(
        cls,
        user_id: str,
        role: Union[str, UserRole],
        estimated_cost: float = 0.0
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if user has remaining daily cost quota.
        
        Returns:
            Tuple of (allowed, quota_info)
        """
        limits = cls.get_role_limits(role)
        
        # Admin has unlimited quota
        if limits.daily_cost_quota == 0.0:
            return True, {
                "allowed": True,
                "quota_type": "unlimited",
                "remaining": None
            }
        
        # Build quota key
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        quota_key = f"{cls.COST_TRACKING_PREFIX}{user_id}:{today}"
        
        try:
            # Get current cost
            current = await redis_client._client.get(quota_key)
            current_cost = float(current) if current else 0.0
            
            # Check if adding estimated cost would exceed limit
            if current_cost + estimated_cost > limits.daily_cost_quota:
                return False, {
                    "allowed": False,
                    "quota_type": "daily_cost",
                    "limit": limits.daily_cost_quota,
                    "used": round(current_cost, 2),
                    "estimated_additional": round(estimated_cost, 2),
                    "remaining": round(limits.daily_cost_quota - current_cost, 2),
                    "resets_at": (datetime.now(timezone.utc) + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).isoformat()
                }
            
            return True, {
                "allowed": True,
                "quota_type": "daily_cost",
                "limit": limits.daily_cost_quota,
                "used": round(current_cost, 2),
                "estimated_additional": round(estimated_cost, 2),
                "remaining": round(limits.daily_cost_quota - current_cost - estimated_cost, 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to check cost quota: {e}")
            # Fail open (allow) but log error
            return True, {
                "allowed": True,
                "quota_type": "daily_cost",
                "error": str(e),
                "warning": "Cost quota check failed, allowing query"
            }
    
    @classmethod
    async def track_query_cost(
        cls,
        user_id: str,
        cost: float
    ):
        """Track actual query cost against user's daily quota"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        quota_key = f"{cls.COST_TRACKING_PREFIX}{user_id}:{today}"
        
        try:
            # Add cost to total
            await redis_client._client.incrbyfloat(quota_key, cost)
            
            # Set expiry if new key
            ttl = await redis_client._client.ttl(quota_key)
            if ttl < 0:
                await redis_client._client.expire(quota_key, 86400)
                
        except Exception as e:
            logger.warning(f"Failed to track query cost: {e}")
    
    @classmethod
    async def get_quota_status(
        cls,
        user_id: str,
        role: Union[str, UserRole]
    ) -> Dict[str, Any]:
        """Get current quota status for a user"""
        limits = cls.get_role_limits(role)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        result = {
            "user_id": user_id,
            "role": role.value if isinstance(role, UserRole) else role,
            "limits": {
                "max_rows": limits.max_rows if limits.max_rows > 0 else "unlimited",
                "max_tables": limits.max_tables if limits.max_tables > 0 else "unlimited",
                "max_joins": limits.max_joins if limits.max_joins > 0 else "unlimited",
                "daily_query_quota": limits.daily_query_quota if limits.daily_query_quota > 0 else "unlimited",
                "daily_cost_quota": limits.daily_cost_quota if limits.daily_cost_quota > 0 else "unlimited",
                "allowed_operations": limits.allowed_operations,
                "can_export": limits.can_export,
                "can_schedule": limits.can_schedule
            },
            "usage": {}
        }
        
        try:
            # Get query quota usage
            if limits.daily_query_quota > 0:
                query_key = f"{cls.DAILY_QUOTA_PREFIX}{user_id}:{today}"
                query_count = await redis_client._client.get(query_key)
                result["usage"]["daily_queries"] = {
                    "used": int(query_count) if query_count else 0,
                    "limit": limits.daily_query_quota,
                    "remaining": limits.daily_query_quota - (int(query_count) if query_count else 0)
                }
            
            # Get cost quota usage
            if limits.daily_cost_quota > 0:
                cost_key = f"{cls.COST_TRACKING_PREFIX}{user_id}:{today}"
                cost_total = await redis_client._client.get(cost_key)
                result["usage"]["daily_cost"] = {
                    "used": round(float(cost_total), 2) if cost_total else 0.0,
                    "limit": limits.daily_cost_quota,
                    "remaining": round(limits.daily_cost_quota - (float(cost_total) if cost_total else 0.0), 2)
                }
            
            # Reset time
            tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            result["resets_at"] = tomorrow.isoformat()
            
        except Exception as e:
            logger.error(f"Failed to get quota status: {e}")
            result["error"] = str(e)
        
        return result
    
    @classmethod
    def apply_row_limit(cls, sql: str, role: Union[str, UserRole], dialect: str = "oracle") -> str:
        """
        Apply row limit to SQL query based on role.
        
        Args:
            sql: SQL query
            role: User role
            dialect: Database dialect (oracle, postgres, doris)
            
        Returns:
            Modified SQL with row limit
        """
        limits = cls.get_role_limits(role)
        max_rows = limits.max_rows
        
        # Admin has no limit
        if max_rows == 0:
            return sql
        
        # Check if query already has a limit
        sql_upper = sql.upper()
        has_oracle_limit = "FETCH FIRST" in sql_upper or "ROWNUM" in sql_upper
        has_postgres_limit = "LIMIT" in sql_upper
        
        if has_oracle_limit or has_postgres_limit:
            # Already has a limit, ensure it's within bounds
            # This is a simplified check - production would need proper SQL parsing
            return sql
        
        # Apply limit based on dialect
        dialect = dialect.lower()
        if dialect in ["postgres", "postgresql", "doris"]:
            # Add LIMIT clause
            return f"{sql.rstrip(';')} LIMIT {max_rows}"
        else:
            # Oracle - add FETCH FIRST
            return f"{sql.rstrip(';')} FETCH FIRST {max_rows} ROWS ONLY"


# Global instance
role_based_limits_service = RoleBasedLimitsService()


# Convenience functions

def get_max_rows(role: str) -> int:
    """Get maximum rows for a role"""
    return RoleBasedLimitsService.get_max_rows_for_role(role)


def can_export_data(role: str) -> bool:
    """Check if role can export data"""
    return RoleBasedLimitsService.can_export(role)


async def check_quota(user_id: str, role: str) -> Tuple[bool, Dict[str, Any]]:
    """Check if user has available quota"""
    return await RoleBasedLimitsService.check_and_increment_query_quota(user_id, role)


def apply_limit(sql: str, role: str, dialect: str = "oracle") -> str:
    """Apply row limit to SQL"""
    return RoleBasedLimitsService.apply_row_limit(sql, role, dialect)
