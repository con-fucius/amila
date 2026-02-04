"""
Session Binding Service

Binds query approvals to the original session context to prevent token forwarding attacks.
Validates that approval requests come from the same session, IP, and user agent as the query initiator.

Security Features:
- Session fingerprinting (session_id + IP + user_agent hash)
- Token binding to prevent forwarding
- Context validation on approval
- Audit trail for security events
"""

import logging
import hashlib
import hmac
import secrets
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class SessionContext:
    """Context for a query session"""
    session_id: str
    user_id: str
    ip_address: str
    user_agent: str
    correlation_id: str
    created_at: str
    fingerprint: str


@dataclass
class BindingResult:
    """Result of a binding validation"""
    valid: bool
    reason: str
    security_event: Optional[str] = None


class SessionBindingService:
    """
    Service for binding query approvals to session context.
    
    Prevents token forwarding attacks by validating that:
    1. The approval comes from the same session that initiated the query
    2. The IP address matches (or is in allowed range)
    3. The user agent matches (or is similar enough)
    """
    
    # Redis key prefix
    BINDING_PREFIX = "session:binding:"
    SECURITY_EVENTS_PREFIX = "security:event:"
    
    # Configuration
    BINDING_TTL = 3600  # 1 hour
    IP_TOLERANCE = "strict"  # "strict", "subnet", "none"
    
    @classmethod
    def _generate_fingerprint(
        cls,
        session_id: str,
        ip_address: str,
        user_agent: str
    ) -> str:
        """
        Generate a fingerprint for the session context.
        
        Args:
            session_id: Session identifier
            ip_address: Client IP address
            user_agent: Client user agent string
            
        Returns:
            HMAC fingerprint
        """
        # Get secret key from settings or generate one
        secret = getattr(settings, 'SESSION_BINDING_SECRET', None)
        if not secret:
            secret = 'default-secret-change-in-production'
            logger.warning("Using default session binding secret - configure SESSION_BINDING_SECRET in production!")
        
        # Create binding data
        binding_data = f"{session_id}:{ip_address}:{user_agent}"
        
        # Generate HMAC
        signature = hmac.new(
            secret.encode(),
            binding_data.encode(),
            hashlib.sha256
        ).hexdigest()[:32]
        
        return signature
    
    @classmethod
    def _normalize_ip(cls, ip_address: str) -> str:
        """Normalize IP address for comparison"""
        # Remove IPv6 prefix if present
        if ip_address.startswith("::ffff:"):
            ip_address = ip_address[7:]
        return ip_address.strip()
    
    @classmethod
    def _compare_ip(cls, ip1: str, ip2: str) -> bool:
        """
        Compare two IP addresses.
        
        Supports:
        - Exact match (strict mode)
        - Subnet match (subnet mode)
        - Any match (none mode - not recommended)
        """
        ip1 = cls._normalize_ip(ip1)
        ip2 = cls._normalize_ip(ip2)
        
        if cls.IP_TOLERANCE == "strict":
            return ip1 == ip2
        elif cls.IP_TOLERANCE == "subnet":
            # Compare first 3 octets for IPv4
            if "." in ip1 and "." in ip2:
                return ip1.rsplit(".", 1)[0] == ip2.rsplit(".", 1)[0]
            return ip1 == ip2
        else:
            return True
    
    @classmethod
    def _compare_user_agent(cls, ua1: str, ua2: str) -> bool:
        """
        Compare user agent strings.
        
        Allows for minor differences (e.g., version updates) but catches
        significant changes (e.g., different browser, device type change).
        """
        # Normalize
        ua1 = ua1.lower().strip() if ua1 else ""
        ua2 = ua2.lower().strip() if ua2 else ""
        
        # Exact match
        if ua1 == ua2:
            return True
        
        # Check for browser family match
        browsers = ["chrome", "firefox", "safari", "edge", "opera"]
        ua1_browser = next((b for b in browsers if b in ua1), None)
        ua2_browser = next((b for b in browsers if b in ua2), None)
        
        if ua1_browser and ua2_browser and ua1_browser == ua2_browser:
            # Same browser family - allow
            return True
        
        # Check for mobile vs desktop
        mobile_keywords = ["mobile", "android", "iphone", "ipad"]
        ua1_mobile = any(k in ua1 for k in mobile_keywords)
        ua2_mobile = any(k in ua2 for k in mobile_keywords)
        
        # If one is mobile and other is desktop, reject
        if ua1_mobile != ua2_mobile:
            return False
        
        # Allow if similarity is high enough
        # Simple similarity: same length within 20%
        if len(ua1) > 0 and len(ua2) > 0:
            len_diff = abs(len(ua1) - len(ua2)) / max(len(ua1), len(ua2))
            if len_diff < 0.2:
                return True
        
        return False
    
    @classmethod
    async def create_binding(
        cls,
        query_id: str,
        session_id: str,
        user_id: str,
        ip_address: str,
        user_agent: str,
        correlation_id: str
    ) -> SessionContext:
        """
        Create a session binding for a query.
        
        Args:
            query_id: Query identifier
            session_id: Session identifier
            user_id: User identifier
            ip_address: Client IP address
            user_agent: Client user agent
            correlation_id: Correlation ID
            
        Returns:
            SessionContext
        """
        fingerprint = cls._generate_fingerprint(session_id, ip_address, user_agent)
        
        context = SessionContext(
            session_id=session_id,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            correlation_id=correlation_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            fingerprint=fingerprint
        )
        
        # Store binding in Redis
        binding_key = f"{cls.BINDING_PREFIX}{query_id}"
        binding_data = {
            "session_id": session_id,
            "user_id": user_id,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "correlation_id": correlation_id,
            "fingerprint": fingerprint,
            "created_at": context.created_at
        }
        
        try:
            await redis_client.set(binding_key, binding_data, ttl=cls.BINDING_TTL)
            logger.info(f"Created session binding for query {query_id}")
        except Exception as e:
            logger.error(f"Failed to create session binding: {e}")
            raise
        
        return context
    
    @classmethod
    async def validate_approval(
        cls,
        query_id: str,
        session_id: str,
        user_id: str,
        ip_address: str,
        user_agent: str
    ) -> BindingResult:
        """
        Validate an approval request against the original session binding.
        
        Args:
            query_id: Query identifier
            session_id: Current session ID
            user_id: Current user ID
            ip_address: Current IP address
            user_agent: Current user agent
            
        Returns:
            BindingResult
        """
        binding_key = f"{cls.BINDING_PREFIX}{query_id}"
        
        try:
            # Get stored binding
            binding_data = await redis_client.get(binding_key)
            
            if not binding_data:
                return BindingResult(
                    valid=False,
                    reason="Session binding not found or expired",
                    security_event="binding_not_found"
                )
            
            # Check if binding has expired (Redis TTL should handle this, but double-check)
            created_at = datetime.fromisoformat(binding_data.get("created_at", "1970-01-01T00:00:00"))
            if datetime.now(timezone.utc) - created_at > timedelta(seconds=cls.BINDING_TTL):
                return BindingResult(
                    valid=False,
                    reason="Session binding has expired",
                    security_event="binding_expired"
                )
            
            # Validate user ID
            if binding_data.get("user_id") != user_id:
                await cls._log_security_event(
                    event_type="user_mismatch",
                    query_id=query_id,
                    expected=binding_data.get("user_id"),
                    actual=user_id,
                    details={"ip_address": ip_address}
                )
                return BindingResult(
                    valid=False,
                    reason="User ID mismatch - approval must come from query initiator",
                    security_event="user_mismatch"
                )
            
            # Validate session ID
            if binding_data.get("session_id") != session_id:
                await cls._log_security_event(
                    event_type="session_mismatch",
                    query_id=query_id,
                    expected=binding_data.get("session_id"),
                    actual=session_id,
                    details={"user_id": user_id, "ip_address": ip_address}
                )
                return BindingResult(
                    valid=False,
                    reason="Session mismatch - possible token forwarding attempt",
                    security_event="session_mismatch"
                )
            
            # Validate IP address
            if not cls._compare_ip(binding_data.get("ip_address", ""), ip_address):
                await cls._log_security_event(
                    event_type="ip_mismatch",
                    query_id=query_id,
                    expected=binding_data.get("ip_address"),
                    actual=ip_address,
                    details={"user_id": user_id, "session_id": session_id}
                )
                return BindingResult(
                    valid=False,
                    reason="IP address mismatch - possible token forwarding attempt",
                    security_event="ip_mismatch"
                )
            
            # Validate user agent
            if not cls._compare_user_agent(binding_data.get("user_agent", ""), user_agent):
                await cls._log_security_event(
                    event_type="user_agent_mismatch",
                    query_id=query_id,
                    expected=binding_data.get("user_agent"),
                    actual=user_agent,
                    details={"user_id": user_id, "ip_address": ip_address}
                )
                return BindingResult(
                    valid=False,
                    reason="User agent mismatch - possible token forwarding attempt",
                    security_event="user_agent_mismatch"
                )
            
            # Regenerate fingerprint and compare
            expected_fingerprint = binding_data.get("fingerprint")
            actual_fingerprint = cls._generate_fingerprint(session_id, ip_address, user_agent)
            
            if not hmac.compare_digest(expected_fingerprint, actual_fingerprint):
                await cls._log_security_event(
                    event_type="fingerprint_mismatch",
                    query_id=query_id,
                    expected=expected_fingerprint,
                    actual=actual_fingerprint,
                    details={"user_id": user_id}
                )
                return BindingResult(
                    valid=False,
                    reason="Session fingerprint mismatch - possible tampering",
                    security_event="fingerprint_mismatch"
                )
            
            # All checks passed
            return BindingResult(
                valid=True,
                reason="Session binding validated successfully"
            )
            
        except Exception as e:
            logger.error(f"Failed to validate session binding: {e}")
            return BindingResult(
                valid=False,
                reason=f"Validation error: {e}",
                security_event="validation_error"
            )
    
    @classmethod
    async def _log_security_event(
        cls,
        event_type: str,
        query_id: str,
        expected: str,
        actual: str,
        details: Dict[str, Any]
    ):
        """Log a security event for audit purposes"""
        try:
            event = {
                "event_type": event_type,
                "query_id": query_id,
                "expected": expected,
                "actual": actual,
                "details": details,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": "high"
            }
            
            event_key = f"{cls.SECURITY_EVENTS_PREFIX}{event_type}:{query_id}"
            await redis_client.set(event_key, event, ttl=86400 * 30)  # 30 days retention
            
            # Also add to security event list
            list_key = f"{cls.SECURITY_EVENTS_PREFIX}recent"
            await redis_client._client.lpush(list_key, event_key)
            await redis_client._client.ltrim(list_key, 0, 999)  # Keep last 1000
            
            logger.warning(
                f"Security event: {event_type} for query {query_id}. "
                f"Expected: {expected}, Actual: {actual}"
            )
            
        except Exception as e:
            logger.error(f"Failed to log security event: {e}")
    
    @classmethod
    async def get_binding_info(cls, query_id: str) -> Optional[Dict[str, Any]]:
        """Get binding information for a query"""
        binding_key = f"{cls.BINDING_PREFIX}{query_id}"
        
        try:
            return await redis_client.get(binding_key)
        except Exception as e:
            logger.error(f"Failed to get binding info: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to get binding info: {e}")
            return None
    
    @classmethod
    async def clear_binding(cls, query_id: str):
        """Clear the binding for a query (e.g., after completion)"""
        binding_key = f"{cls.BINDING_PREFIX}{query_id}"
        
        try:
            await redis_client._client.delete(binding_key)
            logger.debug(f"Cleared session binding for query {query_id}")
        except Exception as e:
            logger.error(f"Failed to clear binding: {e}")
    
    @classmethod
    async def get_recent_security_events(
        cls,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> list:
        """Get recent security events for monitoring"""
        try:
            if event_type:
                pattern = f"{cls.SECURITY_EVENTS_PREFIX}{event_type}:*"
            else:
                pattern = f"{cls.SECURITY_EVENTS_PREFIX}*"
            
            # Get keys (this is a simplified approach)
            list_key = f"{cls.SECURITY_EVENTS_PREFIX}recent"
            event_keys = await redis_client._client.lrange(list_key, 0, limit - 1)
            
            events = []
            for key in event_keys:
                event_data = await redis_client.get(key)
                if event_data:
                    events.append(event_data)
            
            return events
            
        except Exception as e:
            logger.error(f"Failed to get security events: {e}")
            return []


# Global instance
session_binding_service = SessionBindingService()


# Convenience functions

async def create_session_binding(
    query_id: str,
    session_id: str,
    user_id: str,
    ip_address: str,
    user_agent: str,
    correlation_id: str
) -> SessionContext:
    """Create a session binding"""
    return await SessionBindingService.create_binding(
        query_id, session_id, user_id, ip_address, user_agent, correlation_id
    )


async def validate_approval_binding(
    query_id: str,
    session_id: str,
    user_id: str,
    ip_address: str,
    user_agent: str
) -> BindingResult:
    """Validate an approval request"""
    return await SessionBindingService.validate_approval(
        query_id, session_id, user_id, ip_address, user_agent
    )