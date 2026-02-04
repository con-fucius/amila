"""
Audit Trail System
Tracks all user actions for security, compliance, and debugging

Security Features:
- Application-level encryption for sensitive fields
- Immutable audit logs with 90-day retention
- Structured logging with correlation IDs
- Field-level encryption for PII and sensitive data
- Cryptographic integrity verification with blockchain-inspired chaining
"""

import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from enum import Enum
from dataclasses import dataclass, asdict
import json

from app.utils.json_encoder import CustomJSONEncoder
from app.core.redis_client import redis_client
from app.core.config import settings
from app.core.encryption import get_encryption_service
from app.models.internal_models import AuditEntryData, safe_parse_json

# Import audit immutability service for cryptographic verification
from app.services.audit_immutability_service import (
    AuditImmutabilityService,
    AuditEntry as ImmutabilityAuditEntry,
    VerificationStatus
)
from app.services.native_audit_service import native_audit_service

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
    QUERY_CANCEL = "query.cancel"
    
    # Schema operations
    SCHEMA_VIEW = "schema.view"
    SCHEMA_MODIFY = "schema.modify"
    
    # System operations
    HEALTH_CHECK = "system.health_check"
    CONFIG_VIEW = "system.config_view"
    CONFIG_UPDATE = "system.config_update"
    
    # Agent operations
    AGENT_INTERACTION = "agent.interaction"
    AGENT_DECISION = "agent.decision"
    
    # Admin operations
    USER_CREATE = "admin.user_create"
    USER_UPDATE = "admin.user_update"
    USER_DELETE = "admin.user_delete"
    ROLE_ASSIGN = "admin.role_assign"
    
    # Security operations
    SECURITY_VIOLATION = "security.violation"
    RLS_ENFORCEMENT = "security.rls_enforcement"


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
        return json.dumps(self.to_dict(), cls=CustomJSONEncoder)


class AuditLogger:
    """Audit trail logging system with cryptographic immutability verification"""
    
    def __init__(self):
        self.logger = logging.getLogger("audit")
        self.redis_prefix = "audit:"
        self.retention_days = 90  # Keep audit logs for 90 days
        
        # Initialize immutability service for cryptographic verification
        self.immutability_service = AuditImmutabilityService()
        self._last_entry_hash = None  # Track last entry for chain integrity
        self._chain_initialized = False
    
    async def _initialize_chain(self):
        """Initialize the audit chain with a genesis entry if needed"""
        if self._chain_initialized:
            return
        
        try:
            # Check if we have existing chain head in Redis
            chain_head = await redis_client.get(f"{self.redis_prefix}chain_head")
            if chain_head:
                self._last_entry_hash = chain_head
                logger.debug("Audit chain initialized from existing head")
            else:
                # Create genesis entry for new chain
                genesis_entry = self.immutability_service.create_genesis_entry(
                    system_id="amila_audit",
                    description="Audit trail genesis - cryptographic chain start"
                )
                self._last_entry_hash = genesis_entry.hash
                
                # Store genesis entry
                await redis_client.set(
                    f"{self.redis_prefix}genesis",
                    json.dumps({
                        "entry_id": genesis_entry.entry_id,
                        "timestamp": genesis_entry.timestamp,
                        "hash": genesis_entry.hash,
                        "signature": genesis_entry.signature
                    })
                )
                await redis_client.set(f"{self.redis_prefix}chain_head", genesis_entry.hash)
                logger.info("Audit chain genesis entry created")
            
            self._chain_initialized = True
        except Exception as e:
            logger.error(f"Failed to initialize audit chain: {e}")
            # Continue without chain integrity - audit logging should not fail
            self._chain_initialized = True  # Mark as initialized to prevent retry loops
    
    async def _decrypt_and_parse_entry(self, key: str, entry_json: Any) -> Optional[AuditEntry]:
        """
        Helper method to decrypt and parse an audit entry
        
        Args:
            key: Redis key for the entry
            entry_json: Raw entry data from Redis
            
        Returns:
            Decrypted AuditEntry or None if parsing fails
        """
        encryption_service = get_encryption_service()
        
        try:
            # Validate with Pydantic model
            if isinstance(entry_json, str):
                entry_data = safe_parse_json(entry_json, AuditEntryData, default=None, log_errors=True)
                if entry_data:
                    # Decrypt sensitive fields
                    decrypted_dict = encryption_service.decrypt_audit_entry(entry_data.model_dump())
                    return AuditEntry(**decrypted_dict)
                else:
                    # Fallback to direct parsing if validation fails
                    logger.warning(f"Audit entry validation failed for {key}, attempting direct parse")
                    from app.models.internal_models import safe_parse_json_dict
                    entry_dict = safe_parse_json_dict(entry_json, default={}, log_errors=False)
                    if entry_dict:
                        decrypted_dict = encryption_service.decrypt_audit_entry(entry_dict)
                        return AuditEntry(**decrypted_dict)
                    return None
            else:
                # Already a dict from Redis
                decrypted_dict = encryption_service.decrypt_audit_entry(entry_json)
                return AuditEntry(**decrypted_dict)
        except Exception as e:
            logger.error(f"Failed to parse audit entry {key}: {e}")
            return None
    
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
            
        # Write to native database if enabled
        if getattr(settings, 'NATIVE_AUDIT_ENABLED', False):
            try:
                # We don't pass a db_session here, the service will get one if needed
                # or we can pass None and let it handle connection management
                await native_audit_service.write_audit_entry(
                    action=action.value,
                    user_id=user,
                    user_role=user_role,
                    success=success,
                    severity=severity.value,
                    resource_type=resource,
                    resource_id=resource_id,
                    details=details,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    session_id=session_id,
                    correlation_id=correlation_id
                )
            except Exception as e:
                logger.error(f"Failed to write audit entry to native database: {e}")
    
    async def _store_in_redis(self, entry: AuditEntry):
        """Store audit entry in Redis with encryption and cryptographic signing"""
        # Initialize chain if needed
        if not self._chain_initialized:
            await self._initialize_chain()
        
        # Generate unique key
        entry_key = f"{self.redis_prefix}{entry.timestamp}:{entry.user}:{entry.action}"
        
        # Convert to dict and encrypt sensitive fields
        entry_dict = entry.to_dict()
        encryption_service = get_encryption_service()
        encrypted_entry = encryption_service.encrypt_audit_entry(entry_dict)
        
        # Create cryptographically signed audit entry for immutability
        try:
            # Create immutability entry linking to previous entry in chain
            immutability_entry = self.immutability_service.create_entry(
                event_type=entry.action,
                user_id=entry.user,
                session_id=entry.session_id or "unknown",
                correlation_id=entry.correlation_id or entry_key,
                action=entry.action,
                resource=entry.resource or "system",
                status="success" if entry.success else "failure",
                details={
                    "severity": entry.severity,
                    "user_role": entry.user_role,
                    "resource_id": entry.resource_id,
                    "audit_entry_key": entry_key,
                    **encrypted_entry  # Include encrypted audit data
                },
                previous_entry=self._get_previous_entry()
            )
            
            # Store the signed entry with immutability metadata
            signed_entry_data = {
                **encrypted_entry,
                "immutability": {
                    "entry_id": immutability_entry.entry_id,
                    "hash": immutability_entry.hash,
                    "previous_hash": immutability_entry.previous_hash,
                    "signature": immutability_entry.signature,
                    "timestamp": immutability_entry.timestamp
                }
            }
            
            # Update chain head
            self._last_entry_hash = immutability_entry.hash
            await redis_client.set(f"{self.redis_prefix}chain_head", immutability_entry.hash)
            
        except Exception as e:
            logger.warning(f"Failed to create signed audit entry: {e}")
            # Continue with unsigned entry - don't fail audit logging
            signed_entry_data = encrypted_entry
        
        # Store encrypted entry
        await redis_client.setex(
            entry_key,
            self.retention_days * 24 * 60 * 60,  # TTL in seconds
            json.dumps(signed_entry_data, cls=CustomJSONEncoder)
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
    
    def _get_previous_entry(self):
        """Get the previous audit entry for chain linking"""
        if not self._last_entry_hash:
            return None
        
        # Create a minimal entry object with just the hash
        class MinimalEntry:
            def __init__(self, hash_value):
                self.hash = hash_value
        
        return MinimalEntry(self._last_entry_hash)
    
    async def verify_audit_chain_integrity(self) -> Dict[str, Any]:
        """
        Verify the integrity of the audit trail chain.
        
        Returns:
            Verification report with chain integrity status
        """
        try:
            # Get all audit entries from Redis
            pattern = f"{self.redis_prefix}*"
            keys = await redis_client.keys(pattern)
            
            # Filter to entry keys only (exclude index and metadata keys)
            entry_keys = [
                k for k in keys 
                if not k.endswith(":chain_head") 
                and not k.endswith(":genesis")
                and not k.startswith(f"{self.redis_prefix}user:")
                and not k.startswith(f"{self.redis_prefix}action:")
            ]
            
            # Fetch and parse entries with immutability data
            entries = []
            for key in entry_keys[:1000]:  # Limit to prevent memory issues
                entry_json = await redis_client.get(key)
                if entry_json:
                    try:
                        if isinstance(entry_json, str):
                            entry_data = json.loads(entry_json)
                        else:
                            entry_data = entry_json
                        
                        # Check if entry has immutability metadata
                        if "immutability" in entry_data:
                            entries.append(entry_data["immutability"])
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            if not entries:
                return {
                    "status": "empty",
                    "message": "No signed audit entries found",
                    "verified": True
                }
            
            # Use immutability service to verify chain
            from app.services.audit_immutability_service import AuditEntry
            
            audit_entries = []
            for imm_data in entries:
                audit_entries.append(AuditEntry(
                    entry_id=imm_data.get("entry_id", ""),
                    timestamp=imm_data.get("timestamp", ""),
                    event_type="audit",
                    user_id="system",
                    session_id="system",
                    correlation_id=imm_data.get("entry_id", ""),
                    action="verify",
                    resource="audit_chain",
                    status="pending",
                    details={},
                    hash=imm_data.get("hash"),
                    previous_hash=imm_data.get("previous_hash"),
                    signature=imm_data.get("signature")
                ))
            
            # Verify chain integrity
            all_valid, results = self.immutability_service.verify_entry_chain(audit_entries)
            
            verified_count = sum(1 for r in results if r.status == VerificationStatus.VERIFIED)
            tampered_count = sum(1 for r in results if r.status == VerificationStatus.TAMPERED)
            
            return {
                "status": "healthy" if all_valid else "compromised",
                "verified": all_valid,
                "total_entries": len(entries),
                "verified_count": verified_count,
                "tampered_count": tampered_count,
                "chain_head": self._last_entry_hash,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Audit chain verification failed: {e}")
            return {
                "status": "error",
                "verified": False,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
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
        
        # Fetch and decrypt entries
        entries = []
        for key in entry_keys:
            entry_json = await redis_client.get(key)
            if entry_json:
                entry = await self._decrypt_and_parse_entry(key, entry_json)
                if entry:
                    entries.append(entry)
        
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
        
        # Fetch and decrypt entries
        entries = []
        for key in entry_keys:
            entry_json = await redis_client.get(key)
            if entry_json:
                entry = await self._decrypt_and_parse_entry(key, entry_json)
                if entry:
                    entries.append(entry)
        
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
        
        # Fetch and decrypt entries
        entries = []
        for key in entry_keys[:limit]:
            entry_json = await redis_client.get(key)
            if entry_json:
                entry = await self._decrypt_and_parse_entry(key, entry_json)
                if entry:
                    entries.append(entry)
        
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


async def log_audit_event(
    event_type: str,
    user_id: str,
    details: Optional[Dict[str, Any]] = None,
    severity: str = "info",
    resource: Optional[str] = None,
    resource_id: Optional[str] = None,
    correlation_id: Optional[str] = None
):
    """
    General purpose audit logging function (convenience wrapper).
    Used across multiple services.
    """
    # Map string severity to Enum
    sev_enum = AuditSeverity.INFO
    if severity.lower() == "warning":
        sev_enum = AuditSeverity.WARNING
    elif severity.lower() == "error":
        sev_enum = AuditSeverity.ERROR
    elif severity.lower() == "critical":
        sev_enum = AuditSeverity.CRITICAL
        
    await audit_logger.log(
        action=AuditAction.AGENT_INTERACTION, # Default action type for generic events
        user=user_id,
        severity=sev_enum,
        resource=resource or "generic",
        resource_id=resource_id,
        details={
            "original_event_type": event_type,
            **(details or {})
        },
        correlation_id=correlation_id
    )