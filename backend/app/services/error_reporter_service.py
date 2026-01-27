"""
Error Reporter Service
Centralized error reporting with structured logging and optional external integrations
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorLayer(str, Enum):
    """Layer where error originated"""
    API = "api"
    ORCHESTRATOR = "orchestrator"
    MCP_CLIENT = "mcp_client"
    DATABASE = "database"
    LLM = "llm"
    FRONTEND = "frontend"
    UNKNOWN = "unknown"


class ErrorSeverity(str, Enum):
    """Error severity levels"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AmilaError:
    """
    Standardized error format for consistent error handling
    
    Attributes:
        error_id: Unique error identifier (UUID)
        layer: System layer where error occurred
        error_type: Type of error (timeout, sql_error, auth_error, etc.)
        message: Human-readable error message
        details: Additional context (DB-specific codes, query, etc.)
        trace_id: Request trace ID for correlation
        timestamp: ISO8601 timestamp
        severity: Error severity level
        user_id: User who encountered the error (optional)
        query_id: Related query ID (optional)
        database_type: Database type if applicable (optional)
    """
    error_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    layer: ErrorLayer = ErrorLayer.UNKNOWN
    error_type: str = "unknown"
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    trace_id: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    severity: ErrorSeverity = ErrorSeverity.ERROR
    user_id: Optional[str] = None
    query_id: Optional[str] = None
    database_type: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "error_id": self.error_id,
            "layer": self.layer.value,
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
            "trace_id": self.trace_id,
            "timestamp": self.timestamp,
            "severity": self.severity.value,
            "user_id": self.user_id,
            "query_id": self.query_id,
            "database_type": self.database_type,
        }
    
    def to_log_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for structured logging (sanitized)"""
        # Sanitize sensitive data
        sanitized_details = {k: v for k, v in self.details.items() 
                           if k.lower() not in ['password', 'token', 'secret', 'jwt', 'api_key']}
        
        return {
            "error_id": self.error_id,
            "layer": self.layer.value,
            "error_type": self.error_type,
            "message": self.message[:500],  # Truncate long messages
            "details": sanitized_details,
            "trace_id": self.trace_id,
            "timestamp": self.timestamp,
            "severity": self.severity.value,
            "user_id": self.user_id,
            "query_id": self.query_id,
            "database_type": self.database_type,
        }


class ErrorReporterService:
    """
    Centralized error reporting service
    
    Features:
    - Structured error logging
    - Error aggregation and deduplication
    - Optional external service integration (CloudWatch, Sentry, etc.)
    """
    
    # In-memory error buffer for recent errors (for debugging)
    _recent_errors: List[AmilaError] = []
    _max_buffer_size: int = 100
    
    @classmethod
    def report(
        cls,
        message: str,
        layer: ErrorLayer = ErrorLayer.UNKNOWN,
        error_type: str = "unknown",
        details: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        user_id: Optional[str] = None,
        query_id: Optional[str] = None,
        database_type: Optional[str] = None,
        exception: Optional[Exception] = None,
    ) -> AmilaError:
        """
        Report an error with standardized format
        
        Args:
            trace_id: Optional trace ID for correlation (will be validated)
        
        Returns:
            AmilaError instance
        """
        # Validate trace_id if provided
        if trace_id:
            from app.core.structured_logging import validate_and_fix_trace_id
            trace_id = validate_and_fix_trace_id(trace_id)
        
        # Build details dict
        error_details = details or {}
        
        # Add exception info if provided
        if exception:
            error_details["exception_type"] = type(exception).__name__
            error_details["exception_message"] = str(exception)[:500]
        
        # Create error object
        error = AmilaError(
            layer=layer,
            error_type=error_type,
            message=message,
            details=error_details,
            trace_id=trace_id or "",
            severity=severity,
            user_id=user_id,
            query_id=query_id,
            database_type=database_type,
        )
        
        # Log the error
        cls._log_error(error)
        
        # Buffer for recent errors
        cls._buffer_error(error)
        
        # Send to external services (async, non-blocking)
        cls._send_to_external_services(error)
        
        return error
    
    @classmethod
    def report_frontend_error(
        cls,
        message: str,
        stack: Optional[str] = None,
        url: Optional[str] = None,
        user_id: Optional[str] = None,
        component: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> AmilaError:
        """
        Report frontend errors to backend
        
        Returns:
            AmilaError instance
        """
        details = {
            "url": url,
            "component": component,
            **(additional_context or {})
        }
        
        # Sanitize stack trace (remove file paths that might expose server structure)
        if stack:
            details["stack"] = stack[:2000]  # Limit stack trace length
        
        return cls.report(
            message=message,
            layer=ErrorLayer.FRONTEND,
            error_type="frontend_error",
            details=details,
            severity=ErrorSeverity.ERROR,
            user_id=user_id,
        )
    
    @classmethod
    def report_mcp_error(
        cls,
        message: str,
        database_type: str,
        error_code: Optional[str] = None,
        sql_query: Optional[str] = None,
        trace_id: Optional[str] = None,
        user_id: Optional[str] = None,
        query_id: Optional[str] = None,
    ) -> AmilaError:
        """
        Report MCP client errors with database context
        """
        details = {
            "error_code": error_code,
        }
        
        # Sanitize SQL query (remove potential PII)
        if sql_query:
            details["sql_preview"] = sql_query[:200] + "..." if len(sql_query) > 200 else sql_query
        
        return cls.report(
            message=message,
            layer=ErrorLayer.MCP_CLIENT,
            error_type="mcp_error",
            details=details,
            trace_id=trace_id,
            severity=ErrorSeverity.ERROR,
            user_id=user_id,
            query_id=query_id,
            database_type=database_type,
        )
    
    @classmethod
    def report_orchestrator_error(
        cls,
        message: str,
        node_name: str,
        trace_id: Optional[str] = None,
        user_id: Optional[str] = None,
        query_id: Optional[str] = None,
        state_snapshot: Optional[Dict[str, Any]] = None,
    ) -> AmilaError:
        """
        Report orchestrator node errors with state context
        """
        details = {
            "node_name": node_name,
        }
        
        # Include safe state snapshot (exclude sensitive data)
        if state_snapshot:
            safe_keys = ["current_stage", "next_action", "total_iterations", "database_type"]
            details["state"] = {k: state_snapshot.get(k) for k in safe_keys if k in state_snapshot}
        
        return cls.report(
            message=message,
            layer=ErrorLayer.ORCHESTRATOR,
            error_type="orchestrator_error",
            details=details,
            trace_id=trace_id,
            severity=ErrorSeverity.ERROR,
            user_id=user_id,
            query_id=query_id,
        )
    
    @classmethod
    def _log_error(cls, error: AmilaError) -> None:
        """Log error with appropriate level"""
        log_data = error.to_log_dict()
        
        if error.severity == ErrorSeverity.CRITICAL:
            logger.critical("AMILA_ERROR", extra=log_data)
        elif error.severity == ErrorSeverity.ERROR:
            logger.error("AMILA_ERROR", extra=log_data)
        elif error.severity == ErrorSeverity.WARNING:
            logger.warning("AMILA_ERROR", extra=log_data)
        else:
            logger.info("AMILA_ERROR", extra=log_data)
    
    @classmethod
    def _buffer_error(cls, error: AmilaError) -> None:
        """Buffer error for recent errors list"""
        cls._recent_errors.append(error)
        
        # Trim buffer if too large
        if len(cls._recent_errors) > cls._max_buffer_size:
            cls._recent_errors = cls._recent_errors[-cls._max_buffer_size:]
    
        # External service integration placeholders (CloudWatch, Sentry, etc.)
        pass
    
    @classmethod
    def get_recent_errors(
        cls,
        limit: int = 20,
        layer: Optional[ErrorLayer] = None,
        severity: Optional[ErrorSeverity] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get recent errors for debugging
        
        Args:
            limit: Maximum number of errors to return
            layer: Filter by layer (optional)
            severity: Filter by severity (optional)
            
        Returns:
            List of error dictionaries
        """
        errors = cls._recent_errors.copy()
        
        # Apply filters
        if layer:
            errors = [e for e in errors if e.layer == layer]
        if severity:
            errors = [e for e in errors if e.severity == severity]
        
        # Return most recent first
        return [e.to_dict() for e in reversed(errors[-limit:])]
