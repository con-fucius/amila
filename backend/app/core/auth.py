"""
Enhanced Authentication Module
Implements JWT authentication with refresh tokens, session management, and rate limiting.
Uses Redis for distributed state (rate limiting, sessions, tokens).
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from jose import jwt, JWTError
import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import logging
import secrets
import time
import json
from dataclasses import dataclass

from app.core.config import settings
from app.core.exceptions import AuthenticationError, RateLimitError
from app.core.resilience import ResilienceManager, retry_async, retry_sync
from app.models.internal_models import SessionData, RefreshTokenData, safe_parse_json, safe_parse_json_list

# Constants
MAX_REFRESH_TOKENS_PER_USER = 5
RATE_LIMIT_KEY_PREFIX = "auth:rate_limit:"
BLOCKED_USER_KEY_PREFIX = "auth:blocked:"
SESSION_KEY_PREFIX = "auth:session:"
REFRESH_TOKEN_KEY_PREFIX = "auth:refresh:"

# Password hashing configuration (bcrypt manages its own context)
# pwd_context removed in favor of direct bcrypt usage

# HTTP Bearer token security scheme
security = HTTPBearer()

# Logger
logger = logging.getLogger(__name__)

# Resilience manager for auth operations
resilience_manager = ResilienceManager()


# Rate limiting configuration
@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    max_attempts: int = 5
    window_seconds: int = 300  # 5 minutes
    block_duration_seconds: int = 900  # 15 minutes


def _get_redis_client():
    """Get Redis client for auth state storage."""
    try:
        from app.core.redis_client import redis_client
        return redis_client
    except Exception as e:
        logger.warning(f"Redis unavailable for auth state, falling back to in-memory: {e}")
        return None


class AuthenticationManager:
    """Centralized authentication manager"""

    def __init__(self):
        self.rate_limit_config = RateLimitConfig(
            max_attempts=settings.auth_rate_limit_max_attempts,
            window_seconds=settings.auth_rate_limit_window_seconds,
            block_duration_seconds=settings.auth_rate_limit_block_duration
        )

    @retry_sync("auth_verify_password")
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """
        Verify a plain password against a hashed password using direct bcrypt

        Args:
            plain_password: Plain text password
            hashed_password: Hashed password

        Returns:
            bool: True if password matches, False otherwise
        """
        try:
            return bcrypt.checkpw(
                plain_password.encode('utf-8'),
                hashed_password.encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Password verification failed: {e}")
            return False

    def get_password_hash(self, password: str) -> str:
        """
        Hash a password using direct bcrypt

        Args:
            password: Plain text password

        Returns:
            str: Hashed password (utf-8 decoded string)
        """
        return bcrypt.hashpw(
            password.encode('utf-8'),
            bcrypt.gensalt()
        ).decode('utf-8')

    async def check_rate_limit(self, username: str) -> None:
        """
        Check if user is rate limited (uses Redis for distributed state).

        Args:
            username: Username to check

        Raises:
            RateLimitError: If user is rate limited
        """
        current_time = time.time()
        redis = _get_redis_client()
        
        if redis and redis._client:
            try:
                # Check if user is blocked in Redis
                blocked_key = f"{BLOCKED_USER_KEY_PREFIX}{username}"
                blocked_until = await redis._client.get(blocked_key)
                if blocked_until:
                    blocked_until_ts = float(blocked_until)
                    if current_time < blocked_until_ts:
                        remaining = int(blocked_until_ts - current_time)
                        raise RateLimitError(
                            message=f"Account temporarily blocked. Try again in {remaining} seconds.",
                            details={"remaining_seconds": remaining}
                        )
                    else:
                        await redis._client.delete(blocked_key)

                # Get attempts from Redis sorted set (score = timestamp)
                rate_key = f"{RATE_LIMIT_KEY_PREFIX}{username}"
                cutoff_time = current_time - self.rate_limit_config.window_seconds
                
                # Remove old attempts
                await redis._client.zremrangebyscore(rate_key, "-inf", cutoff_time)
                
                # Count current attempts
                attempt_count = await redis._client.zcard(rate_key)
                
                if attempt_count >= self.rate_limit_config.max_attempts:
                    block_until = current_time + self.rate_limit_config.block_duration_seconds
                    await redis._client.setex(blocked_key, self.rate_limit_config.block_duration_seconds, str(block_until))
                    logger.warning(f"User {username} rate limited and blocked")
                    raise RateLimitError(
                        message="Too many failed attempts. Account blocked for 15 minutes.",
                        details={"block_duration": self.rate_limit_config.block_duration_seconds}
                    )
                return
            except RateLimitError:
                raise
            except Exception as e:
                logger.warning(f"Redis rate limit check failed, skipping: {e}")

    async def record_failed_attempt(self, username: str) -> None:
        """
        Record a failed authentication attempt (uses Redis).

        Args:
            username: Username that failed authentication
        """
        current_time = time.time()
        redis = _get_redis_client()
        
        if redis and redis._client:
            try:
                rate_key = f"{RATE_LIMIT_KEY_PREFIX}{username}"
                # Add attempt to sorted set with timestamp as score
                await redis._client.zadd(rate_key, {str(current_time): current_time})
                # Set TTL on the key
                await redis._client.expire(rate_key, self.rate_limit_config.window_seconds + 60)
            except Exception as e:
                logger.warning(f"Redis record attempt failed: {e}")
        
        logger.warning(f"Failed authentication attempt for user {username}")

    async def clear_rate_limit(self, username: str) -> None:
        """
        Clear rate limiting for a user (on successful login).

        Args:
            username: Username to clear rate limiting for
        """
        redis = _get_redis_client()
        
        if redis and redis._client:
            try:
                rate_key = f"{RATE_LIMIT_KEY_PREFIX}{username}"
                blocked_key = f"{BLOCKED_USER_KEY_PREFIX}{username}"
                await redis._client.delete(rate_key, blocked_key)
            except Exception as e:
                logger.warning(f"Redis clear rate limit failed: {e}")

    @retry_sync("auth_create_access_token")
    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
        """
        Create a JWT access token

        Args:
            data: Data to encode in the token
            expires_delta: Token expiration time

        Returns:
            str: Encoded JWT token
        """
        to_encode = data.copy()

        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expire_hours)

        to_encode.update({"exp": expire, "iat": datetime.now(timezone.utc), "type": "access"})

        encoded_jwt = jwt.encode(
            to_encode,
            settings.jwt_secret_key,
            algorithm=settings.jwt_algorithm
        )

        return encoded_jwt

    def create_refresh_token(self, data: dict) -> str:
        """
        Create a JWT refresh token

        Args:
            data: Data to encode in the token

        Returns:
            str: Encoded refresh token
        """
        to_encode = data.copy()

        # Refresh tokens have longer expiration
        expire = datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days)
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "type": "refresh",
            "jti": secrets.token_urlsafe(32)  # Unique token ID for rotation
        })

        encoded_jwt = jwt.encode(
            to_encode,
            settings.jwt_refresh_secret_key,
            algorithm=settings.jwt_algorithm
        )

        return encoded_jwt

    @retry_sync("auth_decode_token")
    def decode_token(self, token: str, token_type: str = "access") -> Optional[Dict[str, Any]]:
        """
        Decode a JWT token

        Args:
            token: JWT token to decode
            token_type: Type of token (access or refresh)

        Returns:
            dict: Decoded token data or None if invalid
        """
        try:
            secret_key = settings.jwt_secret_key if token_type == "access" else settings.jwt_refresh_secret_key
            payload = jwt.decode(
                token,
                secret_key,
                algorithms=[settings.jwt_algorithm]
            )

            # Verify token type
            if payload.get("type") != token_type:
                logger.error(f"Invalid token type: expected {token_type}, got {payload.get('type')}")
                return None

            return payload
        except JWTError as e:
            logger.error(f"JWT decode error for {token_type} token: {e}")
            return None

    async def create_session(self, username: str, user_agent: Optional[str] = None, ip_address: Optional[str] = None) -> str:
        """
        Create a new user session (stored in Redis).

        Args:
            username: Username for the session
            user_agent: User agent string
            ip_address: IP address of the client

        Returns:
            str: Session ID
        """
        session_id = secrets.token_urlsafe(32)
        session_data = {
            "username": username,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_activity": datetime.now(timezone.utc).isoformat(),
            "user_agent": user_agent,
            "ip_address": ip_address,
            "is_active": True
        }
        
        redis = _get_redis_client()
        if redis and redis._client:
            try:
                session_key = f"{SESSION_KEY_PREFIX}{session_id}"
                # Store session with 24 hour TTL
                await redis._client.setex(session_key, 86400, json.dumps(session_data))
            except Exception as e:
                logger.warning(f"Redis session create failed: {e}")

        logger.info(f"Session created for user {username}: {session_id[:8]}...")
        return session_id

    async def validate_session(self, session_id: str) -> Optional[dict]:
        """
        Validate an active session (from Redis).

        Args:
            session_id: Session ID to validate

        Returns:
            dict: Session data if valid, None otherwise
        """
        redis = _get_redis_client()
        if redis and redis._client:
            try:
                session_key = f"{SESSION_KEY_PREFIX}{session_id}"
                session_json = await redis._client.get(session_key)
                if not session_json:
                    return None
                
                # Validate with Pydantic model
                session_data = safe_parse_json(session_json, SessionData, default=None, log_errors=True)
                if session_data:
                    session = session_data.model_dump()
                else:
                    # Fallback to raw parsing
                    logger.warning(f"Session validation failed for {session_id}, using raw data")
                    from app.models.internal_models import safe_parse_json_dict
                    session = safe_parse_json_dict(session_json, default=None, log_errors=False)
                    if not session:
                        return None
                
                if not session.get("is_active"):
                    return None
                
                # Update last activity
                session["last_activity"] = datetime.now(timezone.utc).isoformat()
                await redis._client.setex(session_key, 86400, json.dumps(session))
                return session
            except Exception as e:
                logger.warning(f"Redis session validate failed: {e}")
        
        return None

    async def invalidate_session(self, session_id: str) -> None:
        """
        Invalidate a session (in Redis).

        Args:
            session_id: Session ID to invalidate
        """
        redis = _get_redis_client()
        if redis and redis._client:
            try:
                session_key = f"{SESSION_KEY_PREFIX}{session_id}"
                session_json = await redis._client.get(session_key)
                if session_json:
                    # Validate with Pydantic model
                    session_data = safe_parse_json(session_json, SessionData, default=None, log_errors=False)
                    if session_data:
                        session = session_data.model_dump()
                    else:
                        # Fallback to raw parsing
                        from app.models.internal_models import safe_parse_json_dict
                        session = safe_parse_json_dict(session_json, default={}, log_errors=False)
                    
                    session["is_active"] = False
                    await redis._client.setex(session_key, 3600, json.dumps(session))  # Keep for 1 hour for audit
                    logger.info(f"Session invalidated for user {session.get('username')}: {session_id[:8]}...")
            except Exception as e:
                logger.warning(f"Redis session invalidate failed: {e}")

    async def store_refresh_token(self, username: str, token_jti: str, session_id: str) -> None:
        """
        Store a refresh token for rotation tracking (in Redis).

        Args:
            username: Username
            token_jti: Token unique identifier
            session_id: Associated session ID
        """
        token_data = {
            "jti": token_jti,
            "session_id": session_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "is_active": True
        }
        
        redis = _get_redis_client()
        if redis and redis._client:
            try:
                refresh_key = f"{REFRESH_TOKEN_KEY_PREFIX}{username}"
                # Get existing tokens with validation
                existing = await redis._client.get(refresh_key)
                if existing:
                    tokens = safe_parse_json_list(existing, default=[], log_errors=True)
                else:
                    tokens = []
                tokens.append(token_data)
                
                # Keep only last N tokens
                if len(tokens) > MAX_REFRESH_TOKENS_PER_USER:
                    tokens = tokens[-MAX_REFRESH_TOKENS_PER_USER:]
                
                # Store with 30 day TTL
                await redis._client.setex(refresh_key, 86400 * 30, json.dumps(tokens))
            except Exception as e:
                logger.warning(f"Redis store refresh token failed: {e}")

    async def revoke_refresh_token(self, username: str, token_jti: str) -> None:
        """
        Revoke a refresh token (for rotation, in Redis).

        Args:
            username: Username
            token_jti: Token unique identifier to revoke
        """
        redis = _get_redis_client()
        if redis and redis._client:
            try:
                refresh_key = f"{REFRESH_TOKEN_KEY_PREFIX}{username}"
                existing = await redis._client.get(refresh_key)
                if existing:
                    tokens = safe_parse_json_list(existing, default=[], log_errors=True)
                    for token in tokens:
                        if isinstance(token, dict) and token.get("jti") == token_jti:
                            token["is_active"] = False
                            logger.info(f"Refresh token revoked for user {username}: {token_jti[:8]}...")
                            break
                    await redis._client.setex(refresh_key, 86400 * 30, json.dumps(tokens))
            except Exception as e:
                logger.warning(f"Redis revoke refresh token failed: {e}")

    async def validate_refresh_token(self, username: str, token_jti: str) -> bool:
        """
        Validate if a refresh token is active and not revoked (from Redis).

        Args:
            username: Username
            token_jti: Token unique identifier

        Returns:
            bool: True if token is valid
        """
        redis = _get_redis_client()
        if redis and redis._client:
            try:
                refresh_key = f"{REFRESH_TOKEN_KEY_PREFIX}{username}"
                existing = await redis._client.get(refresh_key)
                if existing:
                    tokens = safe_parse_json_list(existing, default=[], log_errors=True)
                    for token in tokens:
                        if isinstance(token, dict) and token.get("jti") == token_jti and token.get("is_active", False):
                            return True
            except Exception as e:
                logger.warning(f"Redis validate refresh token failed: {e}")
        return False

    async def authenticate_user(self, username: str, password: str) -> Optional[dict]:
        """
        Authenticate a user with rate limiting.

        Args:
            username: Username to authenticate
            password: Password to verify

        Returns:
            dict: User data if authentication successful, None otherwise

        Raises:
            RateLimitError: If user is rate limited
        """
        # Check rate limiting first
        await self.check_rate_limit(username)

        # NOTE: In production, replace this with proper database user lookup
        # This demo user store should be removed before production deployment
        users_db = getattr(settings, 'users_db', None)
        if users_db is None:
            logger.warning("No users_db configured - authentication will fail for all users")
            await self.record_failed_attempt(username)
            return None

        user = users_db.get(username)
        if not user:
            await self.record_failed_attempt(username)
            return None

        if user.get("disabled"):
            await self.record_failed_attempt(username)
            return None

        if not self.verify_password(password, user["hashed_password"]):
            await self.record_failed_attempt(username)
            return None

        # Successful authentication
        await self.clear_rate_limit(username)
        logger.info(f"User {username} authenticated successfully")
        return user

    async def refresh_access_token(self, refresh_token: str, username: str) -> tuple[str, str]:
        """
        Refresh access and refresh tokens with rotation.

        Args:
            refresh_token: Refresh token
            username: Username for validation

        Returns:
            tuple[str, str]: (new_access_token, new_refresh_token)

        Raises:
            AuthenticationError: If refresh token is invalid
        """
        payload = self.decode_token(refresh_token, "refresh")
        if not payload:
            raise AuthenticationError("Invalid refresh token")

        token_username = payload.get("sub")
        token_jti = payload.get("jti")

        if token_username != username:
            raise AuthenticationError("Token username mismatch")

        if not await self.validate_refresh_token(username, token_jti):
            raise AuthenticationError("Refresh token has been revoked")

        # Revoke the old refresh token (rotation)
        await self.revoke_refresh_token(username, token_jti)

        # Create new tokens
        new_access_token = self.create_access_token({"sub": username})
        new_refresh_token = self.create_refresh_token({"sub": username})

        # Store the new refresh token
        new_payload = self.decode_token(new_refresh_token, "refresh")
        if new_payload:
            await self.store_refresh_token(username, new_payload["jti"], "current_session")

        logger.info(f"Tokens rotated for user {username}")
        return new_access_token, new_refresh_token


# Global authentication manager instance
auth_manager = AuthenticationManager()


# Convenience functions for backward compatibility
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Backward compatibility wrapper"""
    return auth_manager.verify_password(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Backward compatibility wrapper"""
    return auth_manager.get_password_hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Backward compatibility wrapper"""
    return auth_manager.create_access_token(data, expires_delta)


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    """Backward compatibility wrapper"""
    return auth_manager.decode_token(token, "access")


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    FastAPI dependency to get the current authenticated user from JWT token
    
    Args:
        credentials: HTTP Authorization credentials
        
    Returns:
        dict: User information from the token
        
    Raises:
        HTTPException: If token is invalid or expired
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        token = credentials.credentials
        payload = auth_manager.decode_token(token, "access")
        
        if payload is None:
            raise credentials_exception
            
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
            
        # For demo purposes, return basic user info
        # In production, you'd query the database for full user details
        return {
            "user_id": username,
            "username": username,
            "session_id": payload.get("session_id", "default_session")
        }
        
    except JWTError:
        raise credentials_exception
