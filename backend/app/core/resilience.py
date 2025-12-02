"""
Resilience utilities for the Amila backend
Provides retry decorators and circuit breaker implementations for improved fault tolerance
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry strategies for exponential backoff"""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service is back


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_attempts: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    backoff_factor: float = 2.0
    jitter: bool = True
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retry_on: Tuple[Type[Exception], ...] = (Exception,)
    retry_condition: Optional[Callable[[Exception], bool]] = None


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    recovery_timeout: int = 60  # seconds
    success_threshold: int = 2  # successes needed to close circuit
    name: str = "default"


class CircuitBreaker:
    """
    Generic circuit breaker implementation
    Can be used for any service or operation
    """

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self._lock = asyncio.Lock()

    async def can_execute(self) -> bool:
        """Check if operation can be executed (thread-safe with lock)"""
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.config.recovery_timeout:
                    # Transition to half-open for testing
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info(f"Circuit breaker '{self.config.name}' transitioning to HALF_OPEN")
                    return True
                return False
            else:  # HALF_OPEN
                return True

    async def record_success(self):
        """Record successful operation"""
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0  # Reset success count
                    logger.info(f"Circuit breaker '{self.config.name}' CLOSED - service recovered")
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0

    async def record_failure(self):
        """Record failed operation"""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.warning(f"Circuit breaker '{self.config.name}' OPEN - {self.failure_count} failures")
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.config.name}' OPEN - failure during recovery")

    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status (sync snapshot - use can_execute for accurate state)"""
        return {
            "name": self.config.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time,
        }


class ResilienceManager:
    """
    Manager for circuit breakers and retry configurations
    Provides centralized management and monitoring
    """

    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_configs: Dict[str, RetryConfig] = {}

    def get_or_create_circuit_breaker(self, name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get existing circuit breaker or create new one"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(config or CircuitBreakerConfig(name=name))
        return self.circuit_breakers[name]

    def get_retry_config(self, name: str) -> RetryConfig:
        """Get retry configuration by name"""
        return self.retry_configs.get(name, RetryConfig())

    def set_retry_config(self, name: str, config: RetryConfig):
        """Set retry configuration"""
        self.retry_configs[name] = config

    def get_all_status(self) -> Dict[str, Any]:
        """Get status of all circuit breakers"""
        return {
            name: cb.get_status() for name, cb in self.circuit_breakers.items()
        }


# Global resilience manager instance
resilience_manager = ResilienceManager()


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate delay for retry attempt"""
    if config.strategy == RetryStrategy.FIXED:
        delay = config.base_delay
    elif config.strategy == RetryStrategy.LINEAR:
        delay = config.base_delay * attempt
    else:  # EXPONENTIAL
        delay = config.base_delay * (config.backoff_factor ** (attempt - 1))

    # Apply maximum delay limit
    delay = min(delay, config.max_delay)

    # Add jitter if enabled
    if config.jitter:
        delay *= (0.5 + random.random() * 0.5)  # 50% to 100% of calculated delay

    return delay


def should_retry(exception: Exception, config: RetryConfig) -> bool:
    """Determine if operation should be retried based on exception"""
    # Check exception type
    if not isinstance(exception, config.retry_on):
        return False

    # Check custom condition if provided
    if config.retry_condition and not config.retry_condition(exception):
        return False

    return True


def retry_async(config: Optional[Union[RetryConfig, str]] = None):
    """
    Decorator for async functions with retry logic and exponential backoff

    Args:
        config: RetryConfig instance or name of registered config
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get retry configuration
            if isinstance(config, str):
                retry_config = resilience_manager.get_retry_config(config)
            elif isinstance(config, RetryConfig):
                retry_config = config
            else:
                retry_config = RetryConfig()

            last_exception = None

            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt == retry_config.max_attempts:
                        logger.error(f"Operation failed after {attempt} attempts: {e}")
                        raise e

                    if not should_retry(e, retry_config):
                        logger.warning(f"Non-retryable exception: {e}")
                        raise e

                    delay = calculate_delay(attempt, retry_config)
                    logger.warning(f"Retrying in {delay:.2f}s (attempt {attempt}/{retry_config.max_attempts})")
                    await asyncio.sleep(delay)

            # This should never be reached, but just in case
            raise last_exception

        return wrapper
    return decorator


def retry_sync(config: Optional[Union[RetryConfig, str]] = None):
    """
    Decorator for sync functions with retry logic and exponential backoff

    Args:
        config: RetryConfig instance or name of registered config
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get retry configuration
            if isinstance(config, str):
                retry_config = resilience_manager.get_retry_config(config)
            elif isinstance(config, RetryConfig):
                retry_config = config
            else:
                retry_config = RetryConfig()

            last_exception = None

            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt == retry_config.max_attempts:
                        logger.error(f"Operation failed after {attempt} attempts: {e}")
                        raise e

                    if not should_retry(e, retry_config):
                        logger.warning(f"Non-retryable exception: {e}")
                        raise e

                    delay = calculate_delay(attempt, retry_config)
                    logger.warning(f"Retrying in {delay:.2f}s (attempt {attempt}/{retry_config.max_attempts})")
                    time.sleep(delay)

            # This should never be reached, but just in case
            raise last_exception

        return wrapper
    return decorator


@asynccontextmanager
async def circuit_breaker_context(name: str, config: Optional[CircuitBreakerConfig] = None):
    """
    Async context manager for circuit breaker protection

    Usage:
        async with circuit_breaker_context("database"):
            await database_operation()
    """
    cb = resilience_manager.get_or_create_circuit_breaker(name, config)

    if not await cb.can_execute():
        raise CircuitBreakerOpenError(f"Circuit breaker '{name}' is OPEN")

    try:
        yield
        await cb.record_success()
    except Exception as e:
        await cb.record_failure()
        raise e


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open"""
    pass


def with_circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None):
    """
    Decorator to apply circuit breaker protection to async functions

    Args:
        name: Circuit breaker name
        config: Circuit breaker configuration
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with circuit_breaker_context(name, config):
                return await func(*args, **kwargs)
        return wrapper
    return decorator


# Convenience functions for common configurations
def create_db_retry_config() -> RetryConfig:
    """Create retry config optimized for database operations"""
    return RetryConfig(
        max_attempts=3,
        base_delay=0.5,
        max_delay=10.0,
        backoff_factor=2.0,
        retry_on=(ConnectionError, TimeoutError, OSError),
        jitter=True
    )


def create_http_retry_config() -> RetryConfig:
    """Create retry config optimized for HTTP operations"""
    return RetryConfig(
        max_attempts=3,
        base_delay=1.0,
        max_delay=30.0,
        backoff_factor=2.0,
        retry_on=(ConnectionError, TimeoutError, OSError),
        jitter=True
    )


def create_auth_retry_config() -> RetryConfig:
    """Create retry config optimized for authentication operations"""
    return RetryConfig(
        max_attempts=2,
        base_delay=0.1,
        max_delay=1.0,
        backoff_factor=2.0,
        retry_on=(OSError, RuntimeError),  # Cryptographic or system-level errors
        jitter=True
    )


def create_default_circuit_breaker(name: str) -> CircuitBreaker:
    """Create circuit breaker with default configuration"""
    return resilience_manager.get_or_create_circuit_breaker(name)


# Initialize common configurations
resilience_manager.set_retry_config("database", create_db_retry_config())
resilience_manager.set_retry_config("http", create_http_retry_config())
resilience_manager.set_retry_config("auth_create_access_token", create_auth_retry_config())
resilience_manager.set_retry_config("auth_decode_token", create_auth_retry_config())
resilience_manager.set_retry_config("auth_verify_password", create_auth_retry_config())
resilience_manager.set_retry_config("default", RetryConfig())