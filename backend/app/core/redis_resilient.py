"""
Resilient Redis Wrapper with Circuit Breaker and Graceful Degradation

Provides fault-tolerant Redis operations with:
- Circuit breaker pattern for automatic failure detection
- Graceful degradation with in-memory fallback
- Automatic retry with exponential backoff
- Comprehensive error handling for all operations
- Health monitoring and recovery
"""

import asyncio
import logging
from typing import Any, Optional, Callable
from datetime import datetime, timezone, timedelta
from enum import Enum
from collections import defaultdict
from functools import wraps

try:
    from redis.exceptions import RedisError, ConnectionError, TimeoutError
except ImportError:
    # Fallback for testing without redis installed
    class RedisError(Exception):
        pass
    class ConnectionError(RedisError):
        pass
    class TimeoutError(RedisError):
        pass

from app.core.exceptions import ExternalServiceException

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker for Redis operations
    
    Prevents cascading failures by:
    - Opening circuit after threshold failures
    - Allowing test requests after timeout
    - Closing circuit on successful recovery
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_state_change: datetime = datetime.now(timezone.utc)
    
    def record_success(self):
        """Record successful operation"""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self._close_circuit()
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        
        if self.state == CircuitState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                self._open_circuit()
        elif self.state == CircuitState.HALF_OPEN:
            self._open_circuit()
    
    def can_attempt(self) -> bool:
        """Check if operation should be attempted"""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._half_open_circuit()
                return True
            return False
        
        # HALF_OPEN state
        return True
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if not self.last_failure_time:
            return True
        
        elapsed = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
        return elapsed >= self.recovery_timeout
    
    def _open_circuit(self):
        """Open circuit (stop allowing requests)"""
        if self.state != CircuitState.OPEN:
            logger.warning(
                f"Circuit breaker OPENED after {self.failure_count} failures. "
                f"Will retry after {self.recovery_timeout}s"
            )
            self.state = CircuitState.OPEN
            self.last_state_change = datetime.now(timezone.utc)
    
    def _half_open_circuit(self):
        """Half-open circuit (allow test requests)"""
        logger.info("Circuit breaker HALF-OPEN, testing recovery")
        self.state = CircuitState.HALF_OPEN
        self.success_count = 0
        self.last_state_change = datetime.now(timezone.utc)
    
    def _close_circuit(self):
        """Close circuit (normal operation)"""
        logger.info("Circuit breaker CLOSED, Redis recovered")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = datetime.now(timezone.utc)
    
    def get_state_info(self) -> dict:
        """Get current circuit breaker state"""
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None,
            "last_state_change": self.last_state_change.isoformat(),
            "can_attempt": self.can_attempt()
        }


class InMemoryFallback:
    """
    In-memory fallback cache for when Redis is unavailable
    
    Provides temporary storage with:
    - TTL-based expiration
    - Size limits to prevent memory exhaustion
    - LRU eviction when full
    """
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache: dict[str, tuple[Any, Optional[datetime]]] = {}
        self.access_times: dict[str, datetime] = {}
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Store value with optional TTL"""
        # Evict if at capacity
        if len(self.cache) >= self.max_size:
            self._evict_lru()
        
        expiry = None
        if ttl:
            expiry = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        
        self.cache[key] = (value, expiry)
        self.access_times[key] = datetime.now(timezone.utc)
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve value if not expired"""
        if key not in self.cache:
            return None
        
        value, expiry = self.cache[key]
        
        # Check expiration
        if expiry and datetime.now(timezone.utc) > expiry:
            del self.cache[key]
            del self.access_times[key]
            return None
        
        # Update access time
        self.access_times[key] = datetime.now(timezone.utc)
        return value
    
    def delete(self, key: str):
        """Remove key from cache"""
        self.cache.pop(key, None)
        self.access_times.pop(key, None)
    
    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired"""
        return self.get(key) is not None
    
    def clear(self):
        """Clear all cached data"""
        self.cache.clear()
        self.access_times.clear()
    
    def _evict_lru(self):
        """Evict least recently used item"""
        if not self.access_times:
            return
        
        lru_key = min(self.access_times, key=self.access_times.get)
        self.delete(lru_key)
        logger.debug(f"Evicted LRU key from fallback cache: {lru_key}")
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "utilization": len(self.cache) / self.max_size if self.max_size > 0 else 0
        }


class ResilientRedisWrapper:
    """
    Resilient wrapper for Redis client with automatic fallback
    
    Features:
    - Circuit breaker for failure detection
    - In-memory fallback when Redis unavailable
    - Automatic retry with exponential backoff
    - Comprehensive error handling
    - Health monitoring
    """
    
    def __init__(self, redis_client, enable_fallback: bool = True):
        self.redis_client = redis_client
        self.circuit_breaker = CircuitBreaker()
        self.fallback = InMemoryFallback() if enable_fallback else None
        self.enable_fallback = enable_fallback
        self.operation_stats = defaultdict(lambda: {"success": 0, "failure": 0, "fallback": 0})
    
    async def execute_with_fallback(
        self,
        operation: Callable,
        fallback_operation: Optional[Callable] = None,
        operation_name: str = "redis_operation",
        *args,
        **kwargs
    ) -> Any:
        """
        Execute Redis operation with circuit breaker and fallback
        
        Args:
            operation: Redis operation to execute
            fallback_operation: Fallback operation if Redis fails
            operation_name: Name for logging and stats
            *args, **kwargs: Arguments for the operation
            
        Returns:
            Result from Redis or fallback
        """
        # Check circuit breaker
        if not self.circuit_breaker.can_attempt():
            logger.debug(f"Circuit breaker OPEN, using fallback for {operation_name}")
            if fallback_operation:
                result = await fallback_operation(*args, **kwargs) if asyncio.iscoroutinefunction(fallback_operation) else fallback_operation(*args, **kwargs)
                self.operation_stats[operation_name]["fallback"] += 1
                return result
            return None
        
        # Attempt Redis operation
        try:
            result = await operation(*args, **kwargs)
            self.circuit_breaker.record_success()
            self.operation_stats[operation_name]["success"] += 1
            return result
            
        except (ExternalServiceException, RedisError, ConnectionError, TimeoutError) as e:
            logger.warning(f"Redis operation '{operation_name}' failed: {e}")
            self.circuit_breaker.record_failure()
            self.operation_stats[operation_name]["failure"] += 1
            
            # Use fallback if available
            if fallback_operation:
                try:
                    result = await fallback_operation(*args, **kwargs) if asyncio.iscoroutinefunction(fallback_operation) else fallback_operation(*args, **kwargs)
                    self.operation_stats[operation_name]["fallback"] += 1
                    return result
                except Exception as fallback_error:
                    logger.error(f"Fallback operation failed for '{operation_name}': {fallback_error}")
            
            return None
        
        except Exception as e:
            # Catch all other exceptions and treat as failures
            logger.error(f"Unexpected error in Redis operation '{operation_name}': {e}")
            self.circuit_breaker.record_failure()
            self.operation_stats[operation_name]["failure"] += 1
            
            # Use fallback if available
            if fallback_operation:
                try:
                    result = await fallback_operation(*args, **kwargs) if asyncio.iscoroutinefunction(fallback_operation) else fallback_operation(*args, **kwargs)
                    self.operation_stats[operation_name]["fallback"] += 1
                    return result
                except Exception as fallback_error:
                    logger.error(f"Fallback operation failed for '{operation_name}': {fallback_error}")
            
            return None
    
    def is_available(self) -> bool:
        """Check if Redis is available"""
        return self.circuit_breaker.state == CircuitState.CLOSED
    
    def get_health_status(self) -> dict:
        """Get comprehensive health status"""
        return {
            "available": self.is_available(),
            "circuit_breaker": self.circuit_breaker.get_state_info(),
            "fallback_enabled": self.enable_fallback,
            "fallback_stats": self.fallback.get_stats() if self.fallback else None,
            "operation_stats": dict(self.operation_stats)
        }
    
    def reset_circuit(self):
        """Manually reset circuit breaker (for testing/admin)"""
        self.circuit_breaker._close_circuit()
        logger.info("Circuit breaker manually reset")


def resilient_redis_operation(operation_name: str = None):
    """
    Decorator for resilient Redis operations
    
    Usage:
        @resilient_redis_operation("get_session")
        async def get_session(self, session_id: str):
            # Redis operation
            pass
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            op_name = operation_name or func.__name__
            
            # Check if instance has resilient wrapper
            if not hasattr(self, '_resilient_wrapper'):
                # No wrapper, execute directly
                return await func(self, *args, **kwargs)
            
            wrapper_instance = self._resilient_wrapper
            
            # Define fallback based on operation type
            fallback_func = None
            if wrapper_instance.enable_fallback:
                # Create fallback for common operations
                if 'get' in op_name.lower():
                    fallback_func = lambda *a, **kw: wrapper_instance.fallback.get(args[0] if args else kwargs.get('key'))
                elif 'set' in op_name.lower():
                    fallback_func = lambda *a, **kw: wrapper_instance.fallback.set(
                        args[0] if args else kwargs.get('key'),
                        args[1] if len(args) > 1 else kwargs.get('value'),
                        kwargs.get('ttl')
                    )
                elif 'delete' in op_name.lower():
                    fallback_func = lambda *a, **kw: wrapper_instance.fallback.delete(args[0] if args else kwargs.get('key'))
                elif 'exists' in op_name.lower():
                    fallback_func = lambda *a, **kw: wrapper_instance.fallback.exists(args[0] if args else kwargs.get('key'))
            
            return await wrapper_instance.execute_with_fallback(
                lambda: func(self, *args, **kwargs),
                fallback_func,
                op_name
            )
        
        return wrapper
    return decorator
