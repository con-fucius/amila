"""
Database-Specific Timeout and Retry Policies
Tailored retry strategies for Oracle (analytical) and Doris (OLAP) workloads
"""

import logging
import asyncio
from typing import Callable, Optional, Any, Dict
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DatabaseType(str, Enum):
    """Database types with different timeout characteristics"""
    ORACLE = "oracle"
    DORIS = "doris"


@dataclass
class TimeoutPolicy:
    """
    Timeout configuration for database operations
    """
    # Connection timeout (establishing connection)
    connection_timeout: float
    
    # Query execution timeout (running queries)
    query_timeout: float
    
    # Pool acquisition timeout (getting client from pool)
    pool_acquisition_timeout: float
    
    # Network read timeout
    read_timeout: float
    
    def __str__(self):
        return (
            f"TimeoutPolicy(connection={self.connection_timeout}s, "
            f"query={self.query_timeout}s, "
            f"pool_acquisition={self.pool_acquisition_timeout}s, "
            f"read={self.read_timeout}s)"
        )


@dataclass
class RetryPolicy:
    """
    Retry configuration for database operations
    """
    # Maximum number of retry attempts
    max_attempts: int
    
    # Base delay for exponential backoff (seconds)
    backoff_base: float
    
    # Maximum backoff delay cap (seconds)
    backoff_cap: float
    
    # Jitter factor (0.0 to 1.0) to randomize backoff
    jitter: float = 0.1
    
    # Whether to retry on timeout errors
    retry_on_timeout: bool = True
    
    # Whether to retry on connection errors
    retry_on_connection_error: bool = True
    
    # Whether to retry on transient errors
    retry_on_transient: bool = True
    
    def calculate_backoff(self, attempt: int) -> float:
        """
        Calculate backoff delay for given attempt using exponential backoff with jitter
        
        Args:
            attempt: Attempt number (0-indexed)
            
        Returns:
            Delay in seconds
        """
        import random
        
        # Exponential backoff: base^attempt
        delay = min(self.backoff_base ** attempt, self.backoff_cap)
        
        # Add jitter to prevent thundering herd
        if self.jitter > 0:
            jitter_amount = delay * self.jitter
            delay += random.uniform(-jitter_amount, jitter_amount)
        
        return max(0, delay)
    
    def __str__(self):
        return (
            f"RetryPolicy(max_attempts={self.max_attempts}, "
            f"backoff={self.backoff_base}s, cap={self.backoff_cap}s)"
        )


class DatabaseTimeoutRetryConfig:
    """
    Database-specific timeout and retry configurations
    """
    
    # Oracle configuration - optimized for analytical queries
    # Longer timeouts for complex analytical workloads
    ORACLE_TIMEOUT_POLICY = TimeoutPolicy(
        connection_timeout=30.0,          # Oracle connection can be slow
        query_timeout=600.0,              # 10 minutes for complex analytical queries
        pool_acquisition_timeout=30.0,    # Wait up to 30s for pool client
        read_timeout=600.0                # Match query timeout
    )
    
    ORACLE_RETRY_POLICY = RetryPolicy(
        max_attempts=3,
        backoff_base=2.0,                 # 2s, 4s, 8s backoff
        backoff_cap=10.0,
        jitter=0.1,
        retry_on_timeout=True,            # Retry timeouts (may be transient)
        retry_on_connection_error=True,   # Retry connection errors
        retry_on_transient=True           # Retry transient errors (ORA-03113, etc.)
    )
    
    # Doris configuration - optimized for OLAP queries with HTTP transport
    # Faster timeouts for OLAP workloads, HTTP-level retries
    DORIS_TIMEOUT_POLICY = TimeoutPolicy(
        connection_timeout=10.0,          # HTTP connection is fast
        query_timeout=120.0,              # 2 minutes for OLAP queries (faster than Oracle)
        pool_acquisition_timeout=5.0,     # Doris uses single client, not pool
        read_timeout=120.0                # Match query timeout
    )
    
    DORIS_RETRY_POLICY = RetryPolicy(
        max_attempts=3,
        backoff_base=1.5,                 # 1.5s, 2.25s, 3.375s backoff (faster than Oracle)
        backoff_cap=5.0,                  # Shorter cap for OLAP
        jitter=0.2,                       # More jitter for HTTP
        retry_on_timeout=True,
        retry_on_connection_error=True,
        retry_on_transient=True
    )
    
    @staticmethod
    def get_timeout_policy(database_type: DatabaseType) -> TimeoutPolicy:
        """Get timeout policy for database type"""
        if database_type == DatabaseType.ORACLE:
            return DatabaseTimeoutRetryConfig.ORACLE_TIMEOUT_POLICY
        elif database_type == DatabaseType.DORIS:
            return DatabaseTimeoutRetryConfig.DORIS_TIMEOUT_POLICY
        else:
            logger.warning(f"Unknown database type: {database_type}, using Oracle defaults")
            return DatabaseTimeoutRetryConfig.ORACLE_TIMEOUT_POLICY
    
    @staticmethod
    def get_retry_policy(database_type: DatabaseType) -> RetryPolicy:
        """Get retry policy for database type"""
        if database_type == DatabaseType.ORACLE:
            return DatabaseTimeoutRetryConfig.ORACLE_RETRY_POLICY
        elif database_type == DatabaseType.DORIS:
            return DatabaseTimeoutRetryConfig.DORIS_RETRY_POLICY
        else:
            logger.warning(f"Unknown database type: {database_type}, using Oracle defaults")
            return DatabaseTimeoutRetryConfig.ORACLE_RETRY_POLICY


class RetryableExecutor:
    """
    Executor with automatic retry logic based on database-specific policies
    """
    
    @staticmethod
    async def execute_with_retry(
        func: Callable,
        database_type: DatabaseType,
        error_normalizer: Optional[Callable[[Dict[str, Any]], Any]] = None,
        operation_name: str = "database_operation",
        **kwargs
    ) -> Any:
        """
        Execute function with automatic retry based on database policy
        
        Args:
            func: Async function to execute
            database_type: Database type for policy selection
            error_normalizer: Optional error normalizer function
            operation_name: Operation name for logging
            **kwargs: Additional arguments to pass to func
            
        Returns:
            Result from func
            
        Raises:
            Last exception if all retries exhausted
        """
        retry_policy = DatabaseTimeoutRetryConfig.get_retry_policy(database_type)
        last_error = None
        
        for attempt in range(retry_policy.max_attempts):
            try:
                logger.debug(
                    f"{operation_name}: Attempt {attempt + 1}/{retry_policy.max_attempts}"
                )
                
                # Execute the function
                result = await func(**kwargs)
                
                # Check if result indicates error (for functions that return dicts)
                if isinstance(result, dict) and result.get("status") == "error":
                    # Normalize error if normalizer provided
                    if error_normalizer:
                        normalized_error = error_normalizer(result)
                        
                        # Check if we should retry
                        if not normalized_error.retry_strategy.should_retry:
                            logger.info(
                                f"{operation_name}: Non-retryable error on attempt {attempt + 1}: "
                                f"{normalized_error.message}"
                            )
                            return result  # Return error result without retry
                        
                        # Check retry conditions
                        should_retry = RetryableExecutor._should_retry_error(
                            normalized_error,
                            retry_policy
                        )
                        
                        if not should_retry:
                            logger.info(
                                f"{operation_name}: Error not eligible for retry: "
                                f"{normalized_error.category.value}"
                            )
                            return result
                    
                    # If we're here, it's retryable or no normalizer
                    if attempt < retry_policy.max_attempts - 1:
                        backoff = retry_policy.calculate_backoff(attempt)
                        logger.warning(
                            f"{operation_name}: Attempt {attempt + 1} failed with error, "
                            f"retrying in {backoff:.2f}s... Error: {result.get('message', 'Unknown')[:100]}"
                        )
                        await asyncio.sleep(backoff)
                        last_error = result
                        continue
                    else:
                        # Last attempt, return error
                        logger.error(
                            f"{operation_name}: All {retry_policy.max_attempts} attempts exhausted"
                        )
                        return result
                
                # Success
                if attempt > 0:
                    logger.info(
                        f"{operation_name}: Succeeded on attempt {attempt + 1}"
                    )
                return result
                
            except (asyncio.TimeoutError, TimeoutError) as e:
                last_error = e
                if not retry_policy.retry_on_timeout or attempt >= retry_policy.max_attempts - 1:
                    logger.error(
                        f"{operation_name}: Timeout after {attempt + 1} attempts"
                    )
                    raise
                
                backoff = retry_policy.calculate_backoff(attempt)
                logger.warning(
                    f"{operation_name}: Timeout on attempt {attempt + 1}, "
                    f"retrying in {backoff:.2f}s..."
                )
                await asyncio.sleep(backoff)
                
            except (ConnectionError, ConnectionRefusedError, OSError) as e:
                last_error = e
                if not retry_policy.retry_on_connection_error or attempt >= retry_policy.max_attempts - 1:
                    logger.error(
                        f"{operation_name}: Connection error after {attempt + 1} attempts: {e}"
                    )
                    raise
                
                backoff = retry_policy.calculate_backoff(attempt)
                logger.warning(
                    f"{operation_name}: Connection error on attempt {attempt + 1}, "
                    f"retrying in {backoff:.2f}s... Error: {str(e)[:100]}"
                )
                await asyncio.sleep(backoff)
                
            except Exception as e:
                last_error = e
                # For other exceptions, fail immediately unless explicitly retryable
                logger.error(
                    f"{operation_name}: Unexpected error on attempt {attempt + 1}: {type(e).__name__}: {str(e)[:100]}"
                )
                raise
        
        # Should not reach here, but just in case
        if last_error:
            raise last_error
        raise RuntimeError(f"{operation_name}: All retries exhausted with no result")
    
    @staticmethod
    def _should_retry_error(normalized_error: Any, retry_policy: RetryPolicy) -> bool:
        """
        Determine if normalized error should be retried based on policy
        
        Args:
            normalized_error: NormalizedError instance
            retry_policy: RetryPolicy instance
            
        Returns:
            True if should retry, False otherwise
        """
        # Import here to avoid circular dependency
        from app.core.error_normalizer import ErrorCategory
        
        category = normalized_error.category
        
        # Connection errors
        if category in {ErrorCategory.CONNECTION_ERROR, ErrorCategory.NETWORK_ERROR}:
            return retry_policy.retry_on_connection_error
        
        # Timeout errors
        if category == ErrorCategory.TIMEOUT:
            return retry_policy.retry_on_timeout
        
        # Transient errors (from retry strategy)
        if normalized_error.retry_strategy.is_transient:
            return retry_policy.retry_on_transient
        
        # Non-retryable errors (syntax, permission, etc.)
        return False


def get_database_policies(database_type: str) -> tuple[TimeoutPolicy, RetryPolicy]:
    """
    Convenience function to get both timeout and retry policies
    
    Args:
        database_type: "oracle" or "doris"
        
    Returns:
        Tuple of (TimeoutPolicy, RetryPolicy)
    """
    db_type = DatabaseType.ORACLE if database_type.lower() == "oracle" else DatabaseType.DORIS
    return (
        DatabaseTimeoutRetryConfig.get_timeout_policy(db_type),
        DatabaseTimeoutRetryConfig.get_retry_policy(db_type)
    )
