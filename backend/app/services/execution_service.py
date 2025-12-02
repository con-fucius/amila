import asyncio
import logging
import time
import uuid
from typing import Dict, Any, Optional, Callable, Awaitable

from app.core.config import settings
from app.core.langfuse_client import (
    create_trace,
    get_langfuse_client,
    trace_span,
    update_trace,
)
from app.core.resilience import circuit_breaker_context, CircuitBreakerConfig, CircuitBreakerOpenError

logger = logging.getLogger(__name__)

class ExecutionService:
    """
    Shared service for executing queries with resilience, observability, and standardized error handling.
    """

    @staticmethod
    async def execute_with_observability(
        query_id: str,
        query_text: str,
        execute_fn: Callable[[], Awaitable[Dict[str, Any]]],
        trace_metadata: Dict[str, Any],
        circuit_breaker_name: str,
        timeout: float = 600.0,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a query function with:
        - Langfuse Tracing
        - Circuit Breaker protection
        - Timeout enforcement
        - Standardized error handling/logging
        """
        start_time = time.time()
        
        # Initialize Langfuse trace
        langfuse_client = get_langfuse_client()
        trace_user = user_id or "anonymous_sql_user"
        
        # Ensure metadata has basic fields
        trace_metadata.update({
            "timeout_seconds": timeout,
            "app_version": getattr(settings, "app_version", "unknown"),
        })

        langfuse_trace_id = create_trace(
            query_id=query_id,
            user_id=trace_user,
            user_query=query_text,
            metadata=trace_metadata,
        )
        trace_identifier = langfuse_trace_id or query_id

        def record_trace(status_label: str, *, error_message: Optional[str] = None, result_dict: Optional[Dict[str, Any]] = None) -> None:
            if not langfuse_client:
                return
            try:
                row_count = 0
                if isinstance(result_dict, dict):
                    # Normalize result check - handle both nested 'results' dict and flat structure if needed
                    results_block = result_dict.get("results")
                    if isinstance(results_block, dict):
                        row_count = results_block.get("row_count", len(results_block.get("rows", [])))
                    elif result_dict.get("row_count") is not None:
                         row_count = result_dict.get("row_count", 0)

                update_trace(
                    trace_identifier,
                    output_data={
                        "status": status_label,
                        "query_id": query_id,
                        "row_count": row_count,
                        "execution_time_ms": int((time.time() - start_time) * 1000),
                    },
                    metadata={
                        **trace_metadata,
                        "error": error_message,
                    },
                    tags=[status_label, trace_metadata.get("database", "unknown")],
                )
                langfuse_client.flush()
            except Exception:
                pass

        # Circuit Breaker Configuration
        cb_config = CircuitBreakerConfig(
            name=circuit_breaker_name,
            failure_threshold=5,
            recovery_timeout=60,
            success_threshold=2,
        )

        try:
            async with circuit_breaker_context(circuit_breaker_name, cb_config):
                
                async with trace_span(
                    trace_identifier,
                    f"{circuit_breaker_name}.execute",
                    input_data={
                        "query_preview": query_text[:200],
                        **trace_metadata
                    },
                ) as span_data:
                    
                    try:
                        result = await asyncio.wait_for(
                            execute_fn(),
                            timeout=timeout,
                        )
                        
                        # Extract execution stats for span output
                        row_count = None
                        status = result.get("status")
                        
                        if isinstance(result.get("results"), dict):
                            row_count = result.get("results", {}).get("row_count")
                        
                        span_data["output"] = {
                            "status": status,
                            "row_count": row_count,
                        }
                        
                        # Ensure result has trace/query IDs
                        if isinstance(result, dict):
                            result.setdefault("trace_id", trace_identifier)
                            result.setdefault("query_id", query_id)

                        logger.info(f"Query executed successfully ({circuit_breaker_name})")
                        record_trace("success", result_dict=result)
                        return result

                    except asyncio.TimeoutError:
                        logger.error(f"Query execution timed out after {timeout}s ({circuit_breaker_name})")
                        error_msg = f"Query execution timed out after {timeout} seconds"
                        record_trace("error", error_message=error_msg)
                        
                        # Return standardized error structure (don't raise, let caller decide or return error dict)
                        # Actually, existing services perform specific returns or raises. 
                        # To be generic, we can raise specific exceptions or return an error dict.
                        # The services usually return an error dict.
                        return {
                            "status": "error",
                            "error": error_msg,
                            "query_id": query_id,
                            "trace_id": trace_identifier,
                            "database_type": trace_metadata.get("database", "unknown"),
                        }

        except CircuitBreakerOpenError as e:
            logger.error(f"Execution circuit open: {e}", exc_info=True)
            record_trace(
                "error",
                error_message="Execution temporarily disabled due to repeated failures"
            )
            raise # Re-raise for caller to handle specific HTTP mapping if needed
            
        except (ConnectionError, TimeoutError, OSError) as e:
            # Recoverable errors - may succeed on retry
            logger.warning(f"Recoverable execution error: {e}", exc_info=True)
            record_trace("error", error_message=f"Recoverable: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "recoverable": True,
                "query_id": query_id,
                "trace_id": trace_identifier,
                "database_type": trace_metadata.get("database", "unknown"),
            }
        except Exception as e:
            # Non-recoverable errors
            logger.error(f"Execution failed: {e}", exc_info=True)
            record_trace("error", error_message=str(e))
            return {
                "status": "error",
                "error": str(e),
                "recoverable": False,
                "query_id": query_id,
                "trace_id": trace_identifier,
                "database_type": trace_metadata.get("database", "unknown"),
            }
