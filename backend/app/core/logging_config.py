"""
Logging Configuration
Sets up structured logging with OpenTelemetry integration
"""

from __future__ import annotations

import logging
import logging.config
import sys
from typing import Dict, Any

try:
    import structlog
except Exception:
    structlog = None

from opentelemetry import trace

from app.core.config import settings


_EMOJI_REPLACEMENTS = {
    "\u2705": "[OK]",
    "\u274c": "[ERROR]",
    "\u26a0": "[WARN]",
    "\U0001f680": "[START]",
    "\U0001f916": "[BOT]",
    "\U0001f9e0": "[KNOWLEDGE]",
    "\U0001f517": "[LINK]",
    "\U0001f504": "[RETRY]",
    "\U0001f4da": "[DOCS]",
    "\U0001f6d1": "[STOP]",
    "\U0001f4a1": "[INFO]",
    "\U0001f6a8": "[CRITICAL]",
    "\U0001f9f9": "[CLEANUP]",
    "\U0001f4ca": "[METRIC]",
    "\U0001f4ac": "[CHAT]",
    "\U0001f4e6": "[PACKAGE]",
}


def _sanitize_string(value: str) -> str:
    """Replace emoji with ASCII tokens and drop unsupported characters."""
    for emoji, replacement in _EMOJI_REPLACEMENTS.items():
        value = value.replace(emoji, replacement)
    # Ensure final string is ASCII-only (strip remaining non-ASCII)
    return value.encode("ascii", "ignore").decode("ascii")


class AsciiSanitizingFilter(logging.Filter):
    """Ensure log records only emit ASCII-friendly text."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _sanitize_string(record.msg)
        if record.args:
            record.args = tuple(
                _sanitize_string(arg) if isinstance(arg, str) else arg
                for arg in record.args
            )
        if isinstance(getattr(record, "stack_info", None), str):
            record.stack_info = _sanitize_string(record.stack_info)
        return True


def setup_logging() -> None:
    """
    Configure structured logging with OpenTelemetry trace correlation
    """
    
    if structlog is None:
        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
            level=getattr(logging, settings.log_level.upper(), logging.INFO),
        )
        logger = logging.getLogger(__name__)
        logger.warning("Structlog unavailable; falling back to standard logging only")
        return

    # Choose renderer: avoid structlog.dev.ConsoleRenderer to prevent rich/box issues on some Windows setups
    if settings.is_development:
        renderer = structlog.processors.KeyValueRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    # Configure structlog processors
    processors = [
        # Add OpenTelemetry trace context to logs
        _add_trace_context,
        # Add timestamp
        structlog.processors.TimeStamper(fmt="ISO"),
        # Add log level
        structlog.stdlib.add_log_level,
        # Add logger name
        structlog.stdlib.add_logger_name,
        # Format stack info
        structlog.processors.format_exc_info,
        # JSON formatting for structured logs
        renderer,
    ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Standard library logging configuration
    logging_config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "ascii_sanitizer": {
                "()": "app.core.logging_config.AsciiSanitizingFilter",
            }
        },
        "formatters": {
            "json": {
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
                "class": "pythonjsonlogger.jsonlogger.JsonFormatter"
            },
            "console": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "console" if settings.is_development else "json",
                "stream": sys.stdout,
                "filters": ["ascii_sanitizer"],
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "json",
                "filename": "logs/bi-agent.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
            }
        },
        "loggers": {
            "": {  # Root logger - ALWAYS log to file for debugging and audit trail
                "level": settings.log_level,
                "handlers": ["console", "file"],  # File logging enabled in ALL environments
                "propagate": False
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console", "file"],  # Enable file logging for uvicorn
                "propagate": False
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["console", "file"],  # Enable file logging for access logs
                "propagate": False
            },
            "fastapi": {
                "level": "INFO",
                "handlers": ["console", "file"],  # Enable file logging for FastAPI
                "propagate": False
            },
            # Silence noisy filesystem watcher debug logs that slow startup
            "watchfiles": {
                "level": "WARNING",
                "handlers": ["console", "file"],
                "propagate": False
            },
            "watchfiles.main": {
                "level": "WARNING",
                "handlers": ["console", "file"],
                "propagate": False
            },
            # Reduce aiosqlite verbosity to avoid checkpoint duplication
            "aiosqlite": {
                "level": "WARNING",  # Changed from INFO to WARNING to reduce noise
                "handlers": ["console", "file"],
                "propagate": False
            },
            # Route Graphiti core logs (especially embedder details) to file-only
            "graphiti_core.embedder.gemini": {
                "level": "WARNING",  # Reduced from DEBUG to WARNING
                "handlers": ["file"],
                "propagate": False
            },
            "graphiti_core.driver.falkordb_driver": {
                "level": "WARNING",  # Reduced from INFO to WARNING
                "handlers": ["file"],
                "propagate": False
            },
            # Reduce httpx/httpcore verbosity (used by MCP clients)
            "httpx": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            },
            "httpcore": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            },
            # Reduce asyncio verbosity
            "asyncio": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            }
        }
    }
    
    # Create logs directory if it doesn't exist
    import os
    os.makedirs("logs", exist_ok=True)
    
    # Apply logging configuration
    logging.config.dictConfig(logging_config)
    
    logger = logging.getLogger(__name__)
    logger.info("[OK] Logging configured - Level: %s", settings.log_level)


def _add_trace_context(logger, method_name, event_dict):
    """
    Add OpenTelemetry trace context to log records
    """
    # Get current span context
    span = trace.get_current_span()
    if span.is_recording():
        span_context = span.get_span_context()
        event_dict["trace_id"] = f"{span_context.trace_id:032x}"
        event_dict["span_id"] = f"{span_context.span_id:016x}"
    
    return event_dict


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a configured structlog logger
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)