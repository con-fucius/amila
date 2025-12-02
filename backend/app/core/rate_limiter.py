"""
Advanced Rate Limiting System
Per-user, per-endpoint rate limiting with Redis backend and sliding window algorithm
"""

import logging
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

from fastapi import HTTPException, Request
from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class RateLimitTier(str, Enum):
    """Rate limit tiers based on user role"""
    GUEST = "guest"      # 10 req/min
    VIEWER = "viewer"    # 30 req/min
    ANALYST = "analyst"  # 100 req/min
    ADMIN = "admin"      # 1000 req/min


@dataclass
class RateLimitConfig:
    """Rate limit configuration"""
    max_requests: int
    window_seconds: int
    tier: RateLimitTier


# Rate limit configurations by tier
RATE_LIMIT_CONFIGS = {
    RateLimitTier.GUEST: RateLimitConfig(
        max_requests=10,
        window_seconds=60,
        tier=RateLimitTier.GUEST
    ),
    RateLimitTier.VIEWER: RateLimitConfig(
        max_requests=30,
        window_seconds=60,
        tier=RateLimitTier.VIEWER
    ),
    RateLimitTier.ANALYST: RateLimitConfig(
        max_requests=100,
        window_seconds=60,
        tier=RateLimitTier.ANALYST
    ),
    RateLimitTier.ADMIN: RateLimitConfig(
        max_requests=1000,
        window_seconds=60,
        tier=RateLimitTier.ADMIN
    ),
}


# Special rate limits for specific endpoints
ENDPOINT_RATE_LIMITS = {
    "/api/v1/queries/submit": {
        RateLimitTier.GUEST: RateLimitConfig(5, 60, RateLimitTier.GUEST),
        RateLimitTier.VIEWER: RateLimitConfig(10, 60, RateLimitTier.VIEWER),
        RateLimitTier.ANALYST: RateLimitConfig(50, 60, RateLimitTier.ANALYST),
        RateLimitTier.ADMIN: RateLimitConfig(500, 60, RateLimitTier.ADMIN),
    },
    "/api/v1/queries/process": {
        RateLimitTier.GUEST: RateLimitConfig(5, 60, RateLimitTier.GUEST),
        RateLimitTier.VIEWER: RateLimitConfig(10, 60, RateLimitTier.VIEWER),
        RateLimitTier.ANALYST: RateLimitConfig(50, 60, RateLimitTier.ANALYST),
        RateLimitTier.ADMIN: RateLimitConfig(500, 60, RateLimitTier.ADMIN),
    },
}


class RateLimiter:
    """
    Advanced rate limiter with sliding window algorithm
    Uses Redis sorted sets for distributed rate limiting
    """
    
    def __init__(self):
        self.redis_prefix = "ratelimit:"
    
    async def check_rate_limit(
        self,
        user: str,
        endpoint: str,
        tier: RateLimitTier = RateLimitTier.VIEWER
    ) -> Dict[str, Any]:
        """
        Check if request is within rate limit
        
        Args:
            user: Username
            endpoint: API endpoint path
            tier: User's rate limit tier
            
        Returns:
            Dict with rate limit status
            
        Raises:
            HTTPException: If rate limit exceeded
        """
        # Get applicable rate limit config
        config = self._get_rate_limit_config(endpoint, tier)
        
        # Generate Redis key for this user+endpoint
        key = f"{self.redis_prefix}{user}:{endpoint}"
        
        current_time = time.time()
        window_start = current_time - config.window_seconds
        
        try:
            # Remove old entries outside the window
            await redis_client.zremrangebyscore(key, 0, window_start)
            
            # Count requests in current window
            request_count = await redis_client.zcard(key)
            
            if request_count >= config.max_requests:
                # Rate limit exceeded
                oldest_entry = await redis_client.zrange(key, 0, 0, withscores=True)
                
                if oldest_entry:
                    reset_time = oldest_entry[0][1] + config.window_seconds
                    retry_after = int(reset_time - current_time)
                else:
                    retry_after = config.window_seconds
                
                logger.warning(
                    f"Rate limit exceeded: user={user}, endpoint={endpoint}, "
                    f"tier={tier.value}, requests={request_count}/{config.max_requests}"
                )
                
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "Rate limit exceeded",
                        "tier": tier.value,
                        "limit": config.max_requests,
                        "window_seconds": config.window_seconds,
                        "retry_after_seconds": retry_after,
                    },
                    headers={"Retry-After": str(retry_after)}
                )
            
            # Add current request to window
            await redis_client.zadd(key, {str(current_time): current_time})
            
            # Set TTL to window + buffer
            await redis_client.expire(key, config.window_seconds + 60)
            
            # Calculate remaining requests
            remaining = config.max_requests - (request_count + 1)
            
            return {
                "allowed": True,
                "limit": config.max_requests,
                "remaining": remaining,
                "reset_seconds": config.window_seconds,
                "tier": tier.value,
            }
        
        except HTTPException:
            raise
        except Exception as e:
            # If Redis fails, allow request but log error
            logger.error(f"Rate limit check failed: {e}")
            return {
                "allowed": True,
                "limit": config.max_requests,
                "remaining": config.max_requests,
                "reset_seconds": config.window_seconds,
                "tier": tier.value,
                "error": "rate_limiter_unavailable"
            }
    
    def _get_rate_limit_config(self, endpoint: str, tier: RateLimitTier) -> RateLimitConfig:
        """
        Get rate limit configuration for endpoint and tier
        
        Args:
            endpoint: API endpoint path
            tier: User's rate limit tier
            
        Returns:
            RateLimitConfig
        """
        # Check for endpoint-specific config
        if endpoint in ENDPOINT_RATE_LIMITS:
            return ENDPOINT_RATE_LIMITS[endpoint].get(tier, RATE_LIMIT_CONFIGS[tier])
        
        # Fall back to tier default
        return RATE_LIMIT_CONFIGS.get(tier, RATE_LIMIT_CONFIGS[RateLimitTier.VIEWER])
    
    async def get_rate_limit_status(
        self,
        user: str,
        endpoint: str,
        tier: RateLimitTier = RateLimitTier.VIEWER
    ) -> Dict[str, Any]:
        """
        Get current rate limit status without incrementing counter
        
        Args:
            user: Username
            endpoint: API endpoint path
            tier: User's rate limit tier
            
        Returns:
            Rate limit status
        """
        config = self._get_rate_limit_config(endpoint, tier)
        key = f"{self.redis_prefix}{user}:{endpoint}"
        
        current_time = time.time()
        window_start = current_time - config.window_seconds
        
        try:
            # Remove old entries
            await redis_client.zremrangebyscore(key, 0, window_start)
            
            # Count current requests
            request_count = await redis_client.zcard(key)
            
            remaining = max(0, config.max_requests - request_count)
            
            return {
                "limit": config.max_requests,
                "remaining": remaining,
                "used": request_count,
                "window_seconds": config.window_seconds,
                "tier": tier.value,
            }
        
        except Exception as e:
            logger.error(f"Failed to get rate limit status: {e}")
            return {
                "limit": config.max_requests,
                "remaining": config.max_requests,
                "used": 0,
                "window_seconds": config.window_seconds,
                "tier": tier.value,
                "error": "unavailable"
            }
    
    async def reset_user_rate_limit(self, user: str, endpoint: Optional[str] = None):
        """
        Reset rate limit for a user (admin function)
        
        Args:
            user: Username
            endpoint: Optional specific endpoint (if None, resets all)
        """
        if endpoint:
            key = f"{self.redis_prefix}{user}:{endpoint}"
            await redis_client.delete(key)
            logger.info(f"Reset rate limit for user={user}, endpoint={endpoint}")
        else:
            # Reset all endpoints for user
            pattern = f"{self.redis_prefix}{user}:*"
            keys = await redis_client.keys(pattern)
            if keys:
                await redis_client.delete(*keys)
            logger.info(f"Reset all rate limits for user={user}")


# Global rate limiter instance
rate_limiter = RateLimiter()


async def apply_rate_limit(
    request: Request,
    user: Dict[str, Any],
    tier: Optional[RateLimitTier] = None
) -> Dict[str, Any]:
    """
    Apply rate limiting to a request
    
    Args:
        request: FastAPI request object
        user: Authenticated user dict
        tier: Optional override for rate limit tier
        
    Returns:
        Rate limit status
        
    Raises:
        HTTPException: If rate limit exceeded
    """
    username = user.get("username", "anonymous")
    endpoint = request.url.path
    
    # Determine tier from user role if not provided
    if not tier:
        user_role = user.get("role", "viewer")
        tier = RateLimitTier(user_role) if user_role in [t.value for t in RateLimitTier] else RateLimitTier.VIEWER
    
    # Check rate limit
    status = await rate_limiter.check_rate_limit(username, endpoint, tier)
    
    # Add rate limit headers to response (via middleware or manually)
    # Headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
    
    return status