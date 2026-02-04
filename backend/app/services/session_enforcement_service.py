"""
Session Enforcement Service

Implements session timeout enforcement and concurrent session limits to prevent
security risks from abandoned sessions.

Features:
- Configurable session timeout based on activity
- Maximum concurrent sessions per user
- Automatic session cleanup
- Session activity tracking
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from app.core.config import settings

logger = logging.getLogger(__name__)


# Session enforcement configuration
@dataclass
class SessionEnforcementConfig:
    """Session enforcement configuration"""

    max_concurrent_sessions: int = 5  # Maximum sessions per user
    session_timeout_seconds: int = 3600  # 1 hour default
    session_absolute_max_seconds: int = 86400  # 24 hours absolute max
    cleanup_interval_seconds: int = 300  # 5 minutes


class SessionEnforcementService:
    """
    Service for enforcing session limits and timeouts.

    Features:
    - Activity-based timeout (extends on activity)
    - Absolute maximum session lifetime
    - Maximum concurrent sessions per user
    - Automatic cleanup of expired sessions
    """

    def __init__(self, config: Optional[SessionEnforcementConfig] = None):
        self.config = config or SessionEnforcementConfig()
        self._get_redis_client()

    def _get_redis_client(self):
        """Get Redis client for session enforcement."""
        try:
            from app.core.redis_client import redis_client

            self._redis_client = redis_client
        except Exception as e:
            logger.warning(f"Redis unavailable for session enforcement: {e}")
            self._redis_client = None

    async def enforce_session_limits(
        self, username: str, user_id: str, session_id: str, user_role: str = "user"
    ) -> Dict[str, Any]:
        """
        Enforce session limits before creating a new session.

        Args:
            username: Username
            user_id: User identifier
            session_id: New session ID
            user_role: User role (admin, developer, analyst, viewer)

        Returns:
            dict: Enforcement result with allowed flag and reasons
        """
        result = {
            "allowed": True,
            "reason": None,
            "terminated_sessions": [],
            "enforced_at": datetime.now(timezone.utc).isoformat(),
        }

        if not self._redis_client:
            logger.warning("Redis unavailable, skipping session enforcement")
            return result

        try:
            user_sessions_key = f"session:enforcement:{username}:sessions"

            # Get current active sessions for user
            active_sessions = await self._get_active_sessions(username)

            # Apply role-based concurrent limits
            max_sessions = self._get_role_max_sessions(user_role)

            if len(active_sessions) >= max_sessions:
                # Need to terminate oldest sessions
                sessions_to_terminate = active_sessions[: -(max_sessions - 1)]

                for session in sessions_to_terminate:
                    await self._terminate_session(session["session_id"])
                    result["terminated_sessions"].append(
                        {
                            "session_id": session["session_id"],
                            "reason": "concurrent_limit_exceeded",
                            "terminated_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )

                result["reason"] = (
                    f"Exceeded concurrent session limit ({max_sessions}). Terminated {len(sessions_to_terminate)} oldest session(s)."
                )

            # Clean up expired sessions
            await self._cleanup_expired_sessions(username, active_sessions)

            return result

        except Exception as e:
            logger.error(f"Error enforcing session limits for user {username}: {e}")
            result["allowed"] = True  # Allow on error for availability
            result["reason"] = f"Enforcement error: {str(e)}"
            return result

    async def validate_session_activity(self, username: str, session_id: str) -> bool:
        """
        Validate session is still active within timeout limits.

        Args:
            username: Username
            session_id: Session ID to validate

        Returns:
            bool: True if session is valid, False if expired
        """
        if not self._redis_client:
            return True  # Allow if Redis unavailable

        try:
            session_key = f"session:enforcement:{username}:session:{session_id}"
            session_data = await self._redis_client._client.get(session_key)

            if not session_data:
                logger.warning(f"Session {session_id} not found in enforcement records")
                return False

            import json

            session = json.loads(session_data)

            # Check absolute max lifetime
            created_at = datetime.fromisoformat(session["created_at"])
            absolute_max = timedelta(seconds=self.config.session_absolute_max_seconds)

            if datetime.now(timezone.utc) - created_at > absolute_max:
                logger.info(f"Session {session_id} exceeded absolute max lifetime")
                await self._terminate_session(session_id)
                return False

            # Update last activity
            await self._update_activity(username, session_id)

            return True

        except Exception as e:
            logger.error(f"Error validating session activity for {session_id}: {e}")
            return True  # Allow on error for availability

    async def _get_active_sessions(self, username: str) -> List[Dict[str, Any]]:
        """Get list of active sessions for user."""
        try:
            user_sessions_key = f"session:enforcement:{username}:sessions"

            if not self._redis_client._session_client:
                return []

            session_ids = await self._redis_client._session_client.smembers(
                user_sessions_key
            )

            active_sessions = []
            current_time = datetime.now(timezone.utc)

            for session_id in session_ids:
                session_key = f"session:enforcement:{username}:session:{session_id}"
                session_data = await self._redis_client._client.get(session_key)

                if session_data:
                    import json

                    session = json.loads(session_data)

                    # Check if session is expired
                    last_activity = datetime.fromisoformat(session["last_activity"])
                    timeout = timedelta(seconds=self.config.session_timeout_seconds)

                    if current_time - last_activity <= timeout:
                        active_sessions.append(session)
                    else:
                        # Remove expired session
                        await self._terminate_session(session_id)
                else:
                    # Session data missing, remove from set
                    await self._redis_client._session_client.srem(
                        user_sessions_key, session_id
                    )

            # Sort by last activity (oldest first)
            active_sessions.sort(key=lambda x: x["last_activity"])

            return active_sessions

        except Exception as e:
            logger.error(f"Error getting active sessions for {username}: {e}")
            return []

    async def _terminate_session(self, session_id: str) -> bool:
        """Terminate a session immediately."""
        try:
            from app.core.redis_client import redis_client

            await redis_client.delete_session(session_id)
            logger.info(f"Session {session_id} terminated")
            return True
        except Exception as e:
            logger.error(f"Error terminating session {session_id}: {e}")
            return False

    async def _cleanup_expired_sessions(
        self, username: str, active_sessions: List[Dict[str, Any]]
    ) -> None:
        """Clean up expired sessions for user."""
        try:
            current_time = datetime.now(timezone.utc)
            timeout = timedelta(seconds=self.config.session_timeout_seconds)

            for session in active_sessions:
                last_activity = datetime.fromisoformat(session["last_activity"])

                if current_time - last_activity > timeout:
                    await self._terminate_session(session["session_id"])
                    logger.info(
                        f"Expired session {session['session_id']} cleaned up for user {username}"
                    )

        except Exception as e:
            logger.error(f"Error cleaning up expired sessions for {username}: {e}")

    async def _update_activity(self, username: str, session_id: str) -> None:
        """Update session activity timestamp."""
        try:
            session_key = f"session:enforcement:{username}:session:{session_id}"

            session_data = await self._redis_client._client.get(session_key)
            if session_data:
                import json

                session = json.loads(session_data)
                session["last_activity"] = datetime.now(timezone.utc).isoformat()

                # Update with TTL
                ttl = self.config.session_timeout_seconds
                await self._redis_client._client.setex(
                    session_key, ttl, json.dumps(session)
                )

        except Exception as e:
            logger.error(f"Error updating activity for session {session_id}: {e}")

    def _get_role_max_sessions(self, role: str) -> int:
        """Get max concurrent sessions based on role."""
        role_limits = {"admin": 10, "developer": 8, "analyst": 5, "viewer": 3}
        return role_limits.get(role.lower(), self.config.max_concurrent_sessions)

    async def get_user_session_count(self, username: str) -> int:
        """Get current active session count for user."""
        active_sessions = await self._get_active_sessions(username)
        return len(active_sessions)

    async def terminate_all_user_sessions(
        self, username: str, except_session_id: Optional[str] = None
    ) -> int:
        """
        Terminate all sessions for a user (e.g., on password change).

        Args:
            username: Username
            except_session_id: Session ID to keep active (optional)

        Returns:
            int: Number of sessions terminated
        """
        try:
            active_sessions = await self._get_active_sessions(username)
            terminated_count = 0

            for session in active_sessions:
                if session["session_id"] != except_session_id:
                    await self._terminate_session(session["session_id"])
                    terminated_count += 1

            logger.info(f"Terminated {terminated_count} sessions for user {username}")
            return terminated_count

        except Exception as e:
            logger.error(f"Error terminating sessions for user {username}: {e}")
            return 0


# Singleton instance
_session_enforcement_service: Optional[SessionEnforcementService] = None


def get_session_enforcement_service() -> SessionEnforcementService:
    """Get or create session enforcement service singleton."""
    global _session_enforcement_service
    if _session_enforcement_service is None:
        _session_enforcement_service = SessionEnforcementService()
    return _session_enforcement_service
