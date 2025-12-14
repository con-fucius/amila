"""
Audit Trail System
Tracks all user actions for security, compliance, and debugging
"""

import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from enum import Enum
from dataclasses import dataclass, asdict
import json

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class AuditAction(str, Enum):
    """Audit action types"""
    # Authentication
    LOGIN = "auth.login"
    LOGOUT = "auth.logout"
    TOKEN_REFRESH = "auth.token_refresh"
    AUTH_FAILED = "auth.failed"
    
    # Query operations
    QUERY_SUBMIT = "query.submit"
    QUERY_EXECUTE = "query.execute"
    QUERY_APPROVE = "query.approve"
    QUERY_REJECT = "query.reject"
    QUERY_VIEW = "query.view"
    
    # Schema operations
    SCHEMA_VIEW = "schema.view"
    SCHEMA_MODIFY = "schema.modify"
    
    # System operations
    HEALTH_CHECK = "system.health_check"
    CONFIG_VIEW = "system.config_view"
    CONFIG_UPDATE = "system.config_update"
    
    # Admin operations
    USER_CREATE = "admin.user_create"
    USER_UPDATE = "admin.user_update"
    USER_DELETE = "admin.user_delete"
    ROLE_ASSIGN = "admin.role_assign"


class AuditSeverity(str, Enum):
    """Audit event severity"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEntry:
    """Audit trail entry"""
    timestamp: str
    action: str
    user: str
    user_role: Optional[str]
    severity: str
    success: bool
    resource: Optional[str]
    resource_id: Optional[str]
    details: Dict[str, Any]
    ip_address: Optional[str]
    user_agent: Optional[str]
    session_id: Optional[str]
    correlation_id: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())


class AuditLogger:
    """Audit trail logging system"""
    
    def __init__(self):
        self.logger = logging.getLogger("audit")
        self.redis_prefix = "audit:"
        self.retention_days = 90  # Keep audit logs for 90 days
    
    async def log(
        self,
        action: AuditAction,
        user: str,
        user_role: Optional[str] = None,
        success: bool = True,
        severity: AuditSeverity = AuditSeverity.INFO,
        resource: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ):
        """
        Log an audit event
        
        Args:
            action: Action being audited
            user: Username performing action
            user_role: User's role
            success: Whether action succeeded
            severity: Event severity
            resource: Resource type being accessed
            resource_id: Specific resource identifier
            details: Additional context
            ip_address: Client IP address
            user_agent: Client user agent
            session_id: Session identifier
            correlation_id: Request correlation ID
        """
        timestamp = datetime.now(timezone.utc)
        
        # Create audit entry
        entry = AuditEntry(
            timestamp=timestamp.isoformat(),
            action=action.value,
            user=user,
            user_role=user_role,
            severity=severity.value,
            success=success,
            resource=resource,
            resource_id=resource_id,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=session_id,
            correlation_id=correlation_id,
        )
        
        # Log to structured logger
        log_message = (
            f"AUDIT: action={action.value}, user={user}, success={success}, "
            f"resource={resource}, resource_id={resource_id}"
        )
        
        if severity == AuditSeverity.CRITICAL:
            self.logger.critical(log_message, extra={"audit_entry": entry.to_dict()})
        elif severity == AuditSeverity.ERROR:
            self.logger.error(log_message, extra={"audit_entry": entry.to_dict()})
        elif severity == AuditSeverity.WARNING:
            self.logger.warning(log_message, extra={"audit_entry": entry.to_dict()})
        else:
            self.logger.info(log_message, extra={"audit_entry": entry.to_dict()})
        
        # Store in Redis for querying
        try:
            await self._store_in_redis(entry)
        except Exception as e:
            logger.error(f"Failed to store audit entry in Redis: {e}")
    
    async def _store_in_redis(self, entry: AuditEntry):
        """Store audit entry in Redis"""
        # Generate unique key
        entry_key = f"{self.redis_prefix}{entry.timestamp}:{entry.user}:{entry.action}"
        
        # Store entry
        await redis_client.setex(
            entry_key,
            self.retention_days * 24 * 60 * 60,  # TTL in seconds
            entry.to_json()
        )
        
        # Add to user's audit trail (sorted set)
        user_audit_key = f"{self.redis_prefix}user:{entry.user}"
        await redis_client.zadd(
            user_audit_key,
            {entry_key: datetime.fromisoformat(entry.timestamp).timestamp()}
        )
        await redis_client.expire(user_audit_key, self.retention_days * 24 * 60 * 60)
        
        # Add to action index (sorted set)
        action_audit_key = f"{self.redis_prefix}action:{entry.action}"
        await redis_client.zadd(
            action_audit_key,
            {entry_key: datetime.fromisoformat(entry.timestamp).timestamp()}
        )
        await redis_client.expire(action_audit_key, self.retention_days * 24 * 60 * 60)
    
    async def get_user_audit_trail(
        self,
        user: str,
        limit: int = 100,
        offset: int = 0
    ) -> list[AuditEntry]:
        """
        Get audit trail for a specific user
        
        Args:
            user: Username
            limit: Maximum number of entries
            offset: Pagination offset
            
        Returns:
            List of audit entries
        """
        user_audit_key = f"{self.redis_prefix}user:{user}"
        
        # Get entry keys from sorted set (newest first)
        entry_keys = await redis_client.zrevrange(
            user_audit_key,
            offset,
            offset + limit - 1
        )
        
        # Fetch entries
        entries = []
        for key in entry_keys:
            try:
                entry_json = await redis_client.get(key)
                if entry_json:
                    entry_dict = json.loads(entry_json)
                    entries.append(AuditEntry(**entry_dict))
            except Exception as e:
                logger.error(f"Failed to parse audit entry {key}: {e}")
                continue
        
        return entries
    
    async def get_action_audit_trail(
        self,
        action: AuditAction,
        limit: int = 100,
        offset: int = 0
    ) -> list[AuditEntry]:
        """
        Get audit trail for a specific action type
        
        Args:
            action: Action type
            limit: Maximum number of entries
            offset: Pagination offset
            
        Returns:
            List of audit entries
        """
        action_audit_key = f"{self.redis_prefix}action:{action.value}"
        
        # Get entry keys from sorted set (newest first)
        entry_keys = await redis_client.zrevrange(
            action_audit_key,
            offset,
            offset + limit - 1
        )
        
        # Fetch entries
        entries = []
        for key in entry_keys:
            try:
                entry_json = await redis_client.get(key)
                if entry_json:
                    entry_dict = json.loads(entry_json)
                    entries.append(AuditEntry(**entry_dict))
            except Exception as e:
                logger.error(f"Failed to parse audit entry {key}: {e}")
                continue
        
        return entries
    
    async def get_recent_audit_trail(
        self,
        limit: int = 100
    ) -> list[AuditEntry]:
        """
        Get most recent audit entries across all users/actions
        
        Args:
            limit: Maximum number of entries
            
        Returns:
            List of audit entries
        """
        # Get all audit keys
        pattern = f"{self.redis_prefix}*"
        keys = await redis_client.keys(pattern)
        
        # Filter to only entry keys (not index keys)
        entry_keys = [k for k in keys if not k.endswith(":user:*") and not k.endswith(":action:*")]
        
        # Sort by timestamp (from key)
        entry_keys.sort(reverse=True)
        
        # Fetch entries
        entries = []
        for key in entry_keys[:limit]:
            try:
                entry_json = await redis_client.get(key)
                if entry_json:
                    entry_dict = json.loads(entry_json)
                    entries.append(AuditEntry(**entry_dict))
            except Exception as e:
                logger.error(f"Failed to parse audit entry {key}: {e}")
                continue
        
        return entries
    
    def generate_query_fingerprint(self, sql_query: str) -> str:
        """
        Generate fingerprint for SQL query (for tracking similar queries)
        
        Args:
            sql_query: SQL query string
            
        Returns:
            Query fingerprint hash
        """
        # Normalize query for fingerprinting
        normalized = sql_query.lower().strip()
        # Remove whitespace variations
        normalized = " ".join(normalized.split())
        
        # Generate SHA256 hash
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]


# Global audit logger instance
audit_logger = AuditLogger()


# Convenience functions for common audit actions

async def audit_login(user: str, success: bool, ip_address: str = None, details: dict = None):
    """Audit login attempt"""
    await audit_logger.log(
        action=AuditAction.LOGIN if success else AuditAction.AUTH_FAILED,
        user=user,
        success=success,
        severity=AuditSeverity.INFO if success else AuditSeverity.WARNING,
        ip_address=ip_address,
        details=details or {}
    )


async def audit_query_execution(
    user: str,
    user_role: str,
    sql_query: str,
    success: bool,
    execution_time_ms: int = None,
    row_count: int = None,
    error: str = None,
    ip_address: str = None,
    session_id: str = None,
    correlation_id: str = None,
):
    """Audit query execution"""
    query_fingerprint = audit_logger.generate_query_fingerprint(sql_query)
    
    details = {
        "sql_query": sql_query[:500],  # Truncate for storage
        "query_fingerprint": query_fingerprint,
        "execution_time_ms": execution_time_ms,
        "row_count": row_count,
    }
    
    if error:
        details["error"] = str(error)
    
    await audit_logger.log(
        action=AuditAction.QUERY_EXECUTE,
        user=user,
        user_role=user_role,
        success=success,
        severity=AuditSeverity.INFO if success else AuditSeverity.ERROR,
        resource="sql_query",
        resource_id=query_fingerprint,
        details=details,
        ip_address=ip_address,
        session_id=session_id,
        correlation_id=correlation_id,
    )


async def audit_query_approval(
    user: str,
    user_role: str,
    query_id: str,
    approved: bool,
    reason: str = None,
    ip_address: str = None,
):
    """Audit query approval/rejection"""
    action = AuditAction.QUERY_APPROVE if approved else AuditAction.QUERY_REJECT
    
    await audit_logger.log(
        action=action,
        user=user,
        user_role=user_role,
        success=True,
        resource="query",
        resource_id=query_id,
        details={"reason": reason} if reason else {},
        ip_address=ip_address,
    )


async def audit_config_change(
    user: str,
    user_role: str,
    config_key: str,
    old_value: Any,
    new_value: Any,
    ip_address: str = None,
):
    """Audit configuration changes"""
    await audit_logger.log(
        action=AuditAction.CONFIG_UPDATE,
        user=user,
        user_role=user_role,
        success=True,
        severity=AuditSeverity.WARNING,  # Config changes are important
        resource="config",
        resource_id=config_key,
        details={
            "old_value": str(old_value),
            "new_value": str(new_value),
        },
        ip_address=ip_address,
    )


async def audit_sse_access(
    user: str,
    user_role: str,
    query_id: str,
    ip_address: str = None,
):
    """Audit SSE stream access for security monitoring"""
    await audit_logger.log(
        action=AuditAction.QUERY_VIEW,
        user=user,
        user_role=user_role,
        success=True,
        severity=AuditSeverity.INFO,
        resource="sse_stream",
        resource_id=query_id,
        details={"stream_type": "query_state"},
        ip_address=ip_address,
    )