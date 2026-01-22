"""
Resilient MCP Client Wrapper with Error Handling and Retry Logic

Provides fault-tolerant MCP operations with:
- Comprehensive error handling for all MCP operations
- Automatic retry with exponential backoff
- Graceful degradation when MCP unavailable
- Clear error messages for debugging
"""

import asyncio
import logging
from typing import Any, Optional, Callable, Dict
from datetime import datetime, timezone
from functools import wraps

from app.core.exceptions import ExternalServiceException

logger = logging.getLogger(__name__)


class MCPError(Exception):
    """Base exception for MCP-related errors"""
    pass


class MCPConnectionError(MCPError):
    """MCP connection error"""
    pass


class MCPExecutionError(MCPError):
    """MCP execution error"""
    pass


class MCPTimeoutError(MCPError):
    """MCP timeout error"""
    pass


async def retry_with_backoff(
    operation: Callable,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    backoff_factor: float = 2.0,
    operation_name: str = "mcp_operation"
) -> Any:
    """
    Retry an async operation with exponential backoff
    
    Args:
        operation: Async function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        backoff_factor: Multiplier for delay on each retry
        operation_name: Name for logging
        
    Returns:
        Result from successful operation
        
    Raises:
        Last exception if all retries fail
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            result = await operation()
            if attempt > 0:
                logger.info(f"MCP operation '{operation_name}' succeeded on attempt {attempt + 1}")
            return result
            
        except (MCPConnectionError, MCPTimeoutError, asyncio.TimeoutError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                logger.warning(
                    f"MCP operation '{operation_name}' failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
            else:
                logger.error(
                    f"MCP operation '{operation_name}' failed after {max_retries} attempts: {e}"
                )
        
        except Exception as e:
            # Non-retryable errors
            logger.error(f"MCP operation '{operation_name}' failed with non-retryable error: {e}")
            raise
    
    # All retries exhausted
    raise last_exception


class ResilientMCPWrapper:
    """
    Resilient wrapper for MCP clients with automatic error handling
    
    Features:
    - Automatic retry with exponential backoff
    - Comprehensive error handling
    - Operation statistics tracking
    - Health monitoring
    """
    
    def __init__(self, mcp_client, max_retries: int = 3):
        self.mcp_client = mcp_client
        self.max_retries = max_retries
        self.operation_stats = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "retried_operations": 0
        }
    
    async def execute_with_retry(
        self,
        operation: Callable,
        operation_name: str = "mcp_operation",
        timeout: Optional[float] = None,
        retryable: bool = True
    ) -> Any:
        """
        Execute MCP operation with retry and error handling
        
        Args:
            operation: Async function to execute
            operation_name: Name for logging and stats
            timeout: Optional timeout in seconds
            retryable: Whether to retry on failure
            
        Returns:
            Result from operation
            
        Raises:
            MCPError: On operation failure
        """
        self.operation_stats["total_operations"] += 1
        
        try:
            if timeout:
                result = await asyncio.wait_for(
                    operation() if not retryable else retry_with_backoff(
                        operation,
                        max_retries=self.max_retries,
                        operation_name=operation_name
                    ),
                    timeout=timeout
                )
            else:
                result = await (operation() if not retryable else retry_with_backoff(
                    operation,
                    max_retries=self.max_retries,
                    operation_name=operation_name
                ))
            
            self.operation_stats["successful_operations"] += 1
            return result
            
        except asyncio.TimeoutError:
            self.operation_stats["failed_operations"] += 1
            logger.error(f"MCP operation '{operation_name}' timed out after {timeout}s")
            raise MCPTimeoutError(f"Operation '{operation_name}' timed out after {timeout}s")
        
        except (MCPError, MCPConnectionError, MCPExecutionError) as e:
            self.operation_stats["failed_operations"] += 1
            logger.error(f"MCP operation '{operation_name}' failed: {e}")
            raise
        
        except Exception as e:
            self.operation_stats["failed_operations"] += 1
            logger.error(f"Unexpected error in MCP operation '{operation_name}': {e}")
            raise MCPExecutionError(f"Unexpected error in '{operation_name}': {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get operation statistics"""
        total = self.operation_stats["total_operations"]
        success_rate = (
            (self.operation_stats["successful_operations"] / total * 100)
            if total > 0 else 0
        )
        
        return {
            **self.operation_stats,
            "success_rate": round(success_rate, 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def reset_stats(self):
        """Reset operation statistics"""
        self.operation_stats = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "retried_operations": 0
        }


def handle_mcp_errors(operation_name: str = None, timeout: Optional[float] = None):
    """
    Decorator for MCP operations with error handling
    
    Usage:
        @handle_mcp_errors("execute_sql", timeout=600)
        async def execute_sql(self, sql: str, connection: str):
            # MCP operation
            pass
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            op_name = operation_name or func.__name__
            
            try:
                if timeout:
                    result = await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
                else:
                    result = await func(*args, **kwargs)
                return result
                
            except asyncio.TimeoutError:
                logger.error(f"MCP operation '{op_name}' timed out after {timeout}s")
                return {
                    "status": "error",
                    "error": f"Operation timed out after {timeout}s",
                    "error_type": "timeout"
                }
            
            except (MCPConnectionError, MCPError) as e:
                logger.error(f"MCP operation '{op_name}' failed: {e}")
                return {
                    "status": "error",
                    "error": str(e),
                    "error_type": "mcp_error"
                }
            
            except Exception as e:
                logger.error(f"Unexpected error in MCP operation '{op_name}': {e}")
                return {
                    "status": "error",
                    "error": f"Unexpected error: {str(e)}",
                    "error_type": "unexpected"
                }
        
        return wrapper
    return decorator


async def safe_mcp_call(
    mcp_client,
    method_name: str,
    *args,
    timeout: Optional[float] = None,
    default_return: Any = None,
    **kwargs
) -> Any:
    """
    Safely call an MCP client method with error handling
    
    Args:
        mcp_client: MCP client instance
        method_name: Name of method to call
        *args: Positional arguments for method
        timeout: Optional timeout in seconds
        default_return: Value to return on error
        **kwargs: Keyword arguments for method
        
    Returns:
        Result from method or default_return on error
    """
    try:
        method = getattr(mcp_client, method_name)
        
        if timeout:
            result = await asyncio.wait_for(method(*args, **kwargs), timeout=timeout)
        else:
            result = await method(*args, **kwargs)
        
        return result
        
    except asyncio.TimeoutError:
        logger.error(f"MCP method '{method_name}' timed out after {timeout}s")
        return default_return
    
    except AttributeError:
        logger.error(f"MCP client does not have method '{method_name}'")
        return default_return
    
    except Exception as e:
        logger.error(f"Error calling MCP method '{method_name}': {e}")
        return default_return


def validate_mcp_response(response: Dict[str, Any], operation_name: str = "mcp_operation") -> bool:
    """
    Validate MCP response and log errors
    
    Args:
        response: Response dict from MCP operation
        operation_name: Name for logging
        
    Returns:
        True if response is valid and successful, False otherwise
    """
    if not isinstance(response, dict):
        logger.error(f"MCP operation '{operation_name}' returned non-dict response: {type(response)}")
        return False
    
    status = response.get("status")
    if status == "error":
        error_msg = response.get("error", "Unknown error")
        logger.error(f"MCP operation '{operation_name}' returned error: {error_msg}")
        return False
    
    if status != "success":
        logger.warning(f"MCP operation '{operation_name}' returned unexpected status: {status}")
        return False
    
    return True
