"""
LLM Middleware Layer
Provides cross-cutting concerns for LLM operations:
- Tracing/Observability
- Caching
- Validation
- Cost Control

Feature-flagged for safe rollout
"""

import logging
import time
import hashlib
from typing import Dict, Any, Optional, Callable, TypeVar
from functools import wraps
from datetime import datetime, timezone

from app.utils.json_encoder import CustomJSONEncoder

logger = logging.getLogger(__name__)

# Feature flags - all disabled by default for safety
MIDDLEWARE_TRACING_ENABLED = False
MIDDLEWARE_CACHING_ENABLED = False
MIDDLEWARE_VALIDATION_ENABLED = False
MIDDLEWARE_COST_CONTROL_ENABLED = False

T = TypeVar('T')


class MiddlewareConfig:
    """Configuration for middleware features"""
    
    # Tracing
    tracing_enabled: bool = MIDDLEWARE_TRACING_ENABLED
    
    # Caching
    caching_enabled: bool = MIDDLEWARE_CACHING_ENABLED
    cache_ttl_seconds: int = 3600  # 1 hour
    
    # Validation
    validation_enabled: bool = MIDDLEWARE_VALIDATION_ENABLED
    max_retries: int = 2
    
    # Cost control
    cost_control_enabled: bool = MIDDLEWARE_COST_CONTROL_ENABLED
    max_tokens_per_request: int = 4000
    max_requests_per_minute: int = 60


config = MiddlewareConfig()


class TracingMiddleware:
    """
    Middleware for tracing LLM calls
    Integrates with Langfuse for observability
    """
    
    @staticmethod
    def wrap(func: Callable[..., T]) -> Callable[..., T]:
        """Wrap a function with tracing"""
        if not config.tracing_enabled:
            return func
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            trace_id = f"trace_{int(start_time * 1000)}"
            
            try:
                logger.debug(f"[Tracing] Starting {func.__name__} (trace_id={trace_id})")
                result = await func(*args, **kwargs)
                
                duration_ms = int((time.time() - start_time) * 1000)
                logger.debug(f"[Tracing] Completed {func.__name__} in {duration_ms}ms")
                
                # Log to Langfuse if available
                try:
                    from app.core.langfuse_client import log_generation
                    log_generation(
                        trace_id=trace_id,
                        name=func.__name__,
                        input_data={"args_count": len(args), "kwargs_keys": list(kwargs.keys())},
                        output_data={"success": True, "duration_ms": duration_ms},
                    )
                except Exception:
                    pass
                
                return result
                
            except Exception as e:
                duration_ms = int((time.time() - start_time) * 1000)
                logger.error(f"[Tracing] Failed {func.__name__} after {duration_ms}ms: {e}")
                raise
        
        return wrapper


class CachingMiddleware:
    """
    Middleware for caching LLM responses
    Uses Redis for distributed caching
    """
    
    _cache: Dict[str, Any] = {}  # In-memory fallback
    
    @classmethod
    def _get_cache_key(cls, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate cache key from function call"""
        key_data = f"{func_name}:{str(args)}:{str(sorted(kwargs.items()))}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    @classmethod
    async def get(cls, key: str) -> Optional[Any]:
        """Get cached value"""
        if not config.caching_enabled:
            return None
        
        try:
            from app.core.client_registry import registry
            redis = registry.get_redis_client()
            if redis:
                cached = await redis.get(f"llm_cache:{key}")
                if cached:
                    import json
                    return json.loads(cached)
        except Exception as e:
            logger.warning(f"[Cache] Redis get failed: {e}")
        
        # Fallback to in-memory
        return cls._cache.get(key)
    
    @classmethod
    async def set(cls, key: str, value: Any, ttl: int = None) -> None:
        """Set cached value"""
        if not config.caching_enabled:
            return
        
        ttl = ttl or config.cache_ttl_seconds
        
        try:
            from app.core.client_registry import registry
            redis = registry.get_redis_client()
            if redis:
                import json
                await redis.setex(f"llm_cache:{key}", ttl, json.dumps(value, cls=CustomJSONEncoder))
                return
        except Exception as e:
            logger.warning(f"[Cache] Redis set failed: {e}")
        
        # Fallback to in-memory
        cls._cache[key] = value
    
    @staticmethod
    def wrap(func: Callable[..., T]) -> Callable[..., T]:
        """Wrap a function with caching"""
        if not config.caching_enabled:
            return func
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Skip caching for non-cacheable calls
            if kwargs.get("skip_cache"):
                return await func(*args, **kwargs)
            
            cache_key = CachingMiddleware._get_cache_key(func.__name__, args, kwargs)
            
            # Check cache
            cached = await CachingMiddleware.get(cache_key)
            if cached is not None:
                logger.debug(f"[Cache] Hit for {func.__name__}")
                return cached
            
            # Execute and cache
            result = await func(*args, **kwargs)
            await CachingMiddleware.set(cache_key, result)
            logger.debug(f"[Cache] Miss for {func.__name__}, cached result")
            
            return result
        
        return wrapper


class ValidationMiddleware:
    """
    Middleware for validating LLM outputs
    Automatically retries with structured output constraints if validation fails
    """
    
    @staticmethod
    def wrap(
        func: Callable[..., T],
        validator: Optional[Callable[[Any], bool]] = None,
        max_retries: int = None
    ) -> Callable[..., T]:
        """Wrap a function with output validation"""
        if not config.validation_enabled:
            return func
        
        max_retries = max_retries or config.max_retries
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    
                    # Validate if validator provided
                    if validator and not validator(result):
                        raise ValueError("Output validation failed")
                    
                    return result
                    
                except Exception as e:
                    last_error = e
                    if attempt < max_retries:
                        logger.warning(f"[Validation] Attempt {attempt + 1} failed for {func.__name__}: {e}")
                        # Add retry hint to kwargs
                        kwargs["_retry_attempt"] = attempt + 1
                    else:
                        logger.error(f"[Validation] All {max_retries + 1} attempts failed for {func.__name__}")
            
            raise last_error
        
        return wrapper


class CostControlMiddleware:
    """
    Middleware for controlling LLM costs
    Tracks token usage and enforces limits
    """
    
    _usage: Dict[str, Dict[str, int]] = {}  # user_id -> {tokens, requests}
    
    @classmethod
    def track_usage(cls, user_id: str, tokens: int) -> None:
        """Track token usage for a user"""
        if not config.cost_control_enabled:
            return
        
        if user_id not in cls._usage:
            cls._usage[user_id] = {"tokens": 0, "requests": 0, "reset_at": time.time()}
        
        # Reset if window expired (1 minute)
        if time.time() - cls._usage[user_id].get("reset_at", 0) > 60:
            cls._usage[user_id] = {"tokens": 0, "requests": 0, "reset_at": time.time()}
        
        cls._usage[user_id]["tokens"] += tokens
        cls._usage[user_id]["requests"] += 1
    
    @classmethod
    def check_limits(cls, user_id: str) -> bool:
        """Check if user is within limits"""
        if not config.cost_control_enabled:
            return True
        
        usage = cls._usage.get(user_id, {})
        
        if usage.get("requests", 0) >= config.max_requests_per_minute:
            logger.warning(f"[CostControl] User {user_id} exceeded request limit")
            return False
        
        return True
    
    @staticmethod
    def wrap(func: Callable[..., T], user_id_arg: str = "user_id") -> Callable[..., T]:
        """Wrap a function with cost control"""
        if not config.cost_control_enabled:
            return func
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user_id = kwargs.get(user_id_arg, "anonymous")
            
            if not CostControlMiddleware.check_limits(user_id):
                raise ValueError(f"Rate limit exceeded for user {user_id}")
            
            result = await func(*args, **kwargs)
            
            # Track usage (estimate tokens from result if available)
            tokens = 0
            if isinstance(result, dict):
                tokens = result.get("usage", {}).get("total_tokens", 100)
            CostControlMiddleware.track_usage(user_id, tokens)
            
            return result
        
        return wrapper


def apply_middleware(
    func: Callable[..., T],
    tracing: bool = True,
    caching: bool = False,
    validation: bool = False,
    cost_control: bool = False,
    validator: Optional[Callable[[Any], bool]] = None,
) -> Callable[..., T]:
    """
    Apply multiple middleware layers to a function
    
    Args:
        func: Function to wrap
        tracing: Enable tracing middleware
        caching: Enable caching middleware
        validation: Enable validation middleware
        cost_control: Enable cost control middleware
        validator: Custom validator function for validation middleware
        
    Returns:
        Wrapped function with middleware applied
    """
    wrapped = func
    
    # Apply in order: cost_control -> validation -> caching -> tracing
    # (innermost first, outermost last)
    
    if cost_control and config.cost_control_enabled:
        wrapped = CostControlMiddleware.wrap(wrapped)
    
    if validation and config.validation_enabled:
        wrapped = ValidationMiddleware.wrap(wrapped, validator=validator)
    
    if caching and config.caching_enabled:
        wrapped = CachingMiddleware.wrap(wrapped)
    
    if tracing and config.tracing_enabled:
        wrapped = TracingMiddleware.wrap(wrapped)
    
    return wrapped


def enable_middleware(
    tracing: bool = False,
    caching: bool = False,
    validation: bool = False,
    cost_control: bool = False,
) -> None:
    """
    Enable middleware features at runtime
    
    Args:
        tracing: Enable tracing
        caching: Enable caching
        validation: Enable validation
        cost_control: Enable cost control
    """
    config.tracing_enabled = tracing
    config.caching_enabled = caching
    config.validation_enabled = validation
    config.cost_control_enabled = cost_control
    
    logger.info(f"Middleware enabled: tracing={tracing}, caching={caching}, validation={validation}, cost_control={cost_control}")


def disable_all_middleware() -> None:
    """Disable all middleware features"""
    config.tracing_enabled = False
    config.caching_enabled = False
    config.validation_enabled = False
    config.cost_control_enabled = False
    
    logger.info("All middleware disabled")
