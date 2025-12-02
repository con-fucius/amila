"""
Comprehensive Structured Logging Configuration

Implements:
- JSON-formatted logs with structlog
- Trace ID correlation across all requests
- Performance profiling
- Error categorization
- Integration with OpenTelemetry
"""

import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import structlog
    from structlog.types import EventDict, Processor
except Exception:  # pragma: no cover - graceful degradation path
    structlog = None  # type: ignore
    EventDict = Dict[str, Any]  # type: ignore
    Processor = Any  # type: ignore

# Context variable for trace ID (thread-safe, async-safe)
trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar("user_id", default=None)
session_id_var: ContextVar[Optional[str]] = ContextVar("session_id", default=None)


def get_trace_id() -> str:
    """Get current trace ID or generate new one"""
    trace_id = trace_id_var.get()
    if not trace_id:
        trace_id = str(uuid.uuid4())
        trace_id_var.set(trace_id)
    return trace_id


def set_trace_id(trace_id: str) -> None:
    """Set trace ID for current context"""
    trace_id_var.set(trace_id)


def set_user_context(user_id: Optional[str] = None, session_id: Optional[str] = None) -> None:
    """Set user context for logging"""
    if user_id:
        user_id_var.set(user_id)
    if session_id:
        session_id_var.set(session_id)


def clear_context() -> None:
    """Clear logging context (call at request end)"""
    trace_id_var.set(None)
    user_id_var.set(None)
    session_id_var.set(None)


def add_trace_context(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Add trace ID and user context to all log entries"""
    trace_id = trace_id_var.get()
    if trace_id:
        event_dict["trace_id"] = trace_id
    
    user_id = user_id_var.get()
    if user_id:
        event_dict["user_id"] = user_id
    
    session_id = session_id_var.get()
    if session_id:
        event_dict["session_id"] = session_id
    
    return event_dict


def add_timestamp(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Add ISO8601 timestamp"""
    event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def add_level(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Add log level"""
    if method_name == "warn":
        # Normalize to "warning"
        event_dict["level"] = "warning"
    else:
        event_dict["level"] = method_name
    return event_dict


def categorize_error(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Categorize errors for better filtering"""
    if "exc_info" in event_dict or "exception" in event_dict:
        exc_info = event_dict.get("exc_info") or event_dict.get("exception")
        if exc_info:
            exc_type = type(exc_info).__name__ if not isinstance(exc_info, tuple) else exc_info[0].__name__
            
            # Categorize based on exception type
            if "Timeout" in str(exc_type):
                event_dict["error_category"] = "timeout"
            elif "Connection" in str(exc_type) or "Network" in str(exc_type):
                event_dict["error_category"] = "network"
            elif "Permission" in str(exc_type) or "Auth" in str(exc_type):
                event_dict["error_category"] = "auth"
            elif "Validation" in str(exc_type):
                event_dict["error_category"] = "validation"
            elif "ORA-" in str(event_dict.get("event", "")):
                event_dict["error_category"] = "database"
            else:
                event_dict["error_category"] = "application"
    
    return event_dict


def configure_structured_logging(
    log_level: str = "INFO",
    json_format: bool = True,
    enable_colors: bool = False,
) -> None:
    """Configure structured logging for the application.

    If ``structlog`` (and thus ``rich``) cannot be imported, this falls back to
    plain stdlib logging so that observability never prevents startup.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Use JSON format (True for production, False for development)
        enable_colors: Enable colored output (ignored when structlog is absent)
    """

    # Fallback path when structlog / rich are not usable
    if structlog is None:
        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
            level=getattr(logging, log_level.upper(), logging.INFO),
        )
        logging.getLogger(__name__).warning(
            "Structlog unavailable; using standard logging only for structured_logging",
        )
        return

    # Processor chain
    processors: list[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        add_trace_context,
        add_timestamp,
        add_level,
        categorize_error,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if json_format:
        # Production: JSON output
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Development: Human-readable output (avoid structlog.dev.ConsoleRenderer to sidestep rich/box issues)
        processors.append(structlog.processors.KeyValueRenderer())
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    
    # Set uvicorn/fastapi loggers to use structlog format
    for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error", "fastapi"]:
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.propagate = True


def get_logger(name: str):
    """Get a structured logger instance.

    When ``structlog`` is not available, this returns a stdlib logger instead
    of crashing.
    """
    if structlog is None:
        return logging.getLogger(name)
    return structlog.get_logger(name)


class PerformanceTracker:
    """Context manager for tracking operation performance"""
    
    def __init__(self, operation_name: str, logger: Optional[Any] = None):
        self.operation_name = operation_name
        self.logger = logger or get_logger(__name__)
        self.start_time: Optional[float] = None
        self.trace_id = get_trace_id()
    
    def __enter__(self):
        self.start_time = datetime.now(timezone.utc).timestamp()
        self.logger.debug(
            "operation_started",
            operation=self.operation_name,
            trace_id=self.trace_id,
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration_ms = (datetime.now(timezone.utc).timestamp() - self.start_time) * 1000
            
            if exc_type:
                self.logger.error(
                    "operation_failed",
                    operation=self.operation_name,
                    trace_id=self.trace_id,
                    duration_ms=round(duration_ms, 2),
                    error_type=exc_type.__name__,
                    error_message=str(exc_val),
                )
            else:
                self.logger.info(
                    "operation_completed",
                    operation=self.operation_name,
                    trace_id=self.trace_id,
                    duration_ms=round(duration_ms, 2),
                )
        
        return False  # Don't suppress exceptions


def log_query_lifecycle(
    stage: str,
    query_id: str,
    user_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    logger: Optional[Any] = None,
) -> None:
    """
    Log query lifecycle events with consistent structure
    
    Args:
        stage: Query stage (submitted, planning, executing, completed, error)
        query_id: Unique query identifier
        user_id: User identifier
        metadata: Additional metadata
        logger: Logger instance (optional)
    """
    log = logger or get_logger("query_lifecycle")
    
    event_data = {
        "event_type": "query_lifecycle",
        "stage": stage,
        "query_id": query_id,
        "user_id": user_id,
        "trace_id": get_trace_id(),
    }
    
    if metadata:
        event_data.update(metadata)
    
    # Log at appropriate level
    if stage == "error":
        log.error("query_lifecycle_event", **event_data)
    elif stage in ["submitted", "completed"]:
        log.info("query_lifecycle_event", **event_data)
    else:
        log.debug("query_lifecycle_event", **event_data)
