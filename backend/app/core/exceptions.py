"""
Custom Exception Hierarchy
Standardized error handling with error codes and consistent messaging
Implements RFC 7807 Problem Details for HTTP APIs
"""

import uuid
from typing import Any, Dict, Optional
from fastapi import HTTPException


class BaseAppException(Exception):
    """Base exception class for application errors (RFC 7807 compliant)"""

    def __init__(
        self,
        message: str,
        error_code: str,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        self.correlation_id = correlation_id or str(uuid.uuid4())

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON response"""
        return {
            "error": {
                "code": self.error_code,
                "message": self.message,
                "details": self.details,
                "correlation_id": self.correlation_id
            }
        }
    
    def to_rfc7807(self) -> Dict[str, Any]:
        """
        Convert exception to RFC 7807 Problem Details format (Gap #32)
        https://datatracker.ietf.org/doc/html/rfc7807
        
        Returns:
            RFC 7807 compliant error response
        """
        problem = {
            "type": f"https://bi-agent.local/errors/{self.error_code.lower().replace('_', '-')}",
            "title": self.error_code.replace("_", " ").title(),
            "status": self.status_code,
            "detail": self.message,
            "instance": f"/errors/{self.correlation_id}",
        }
        
        # Add extension members (details)
        if self.details:
            problem.update(self.details)
        
        # Add correlation ID
        problem["correlationId"] = self.correlation_id
        
        return problem


class ValidationException(BaseAppException):
    """Exception for input validation errors"""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            status_code=400,
            details=details,
            correlation_id=correlation_id
        )


class AuthenticationException(BaseAppException):
    """Exception for authentication failures"""

    def __init__(
        self,
        message: str = "Authentication failed",
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="AUTHENTICATION_ERROR",
            status_code=401,
            details=details,
            correlation_id=correlation_id
        )


# Alias for backward compatibility
AuthenticationError = AuthenticationException


class AuthorizationException(BaseAppException):
    """Exception for authorization failures"""

    def __init__(
        self,
        message: str = "Insufficient permissions",
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="AUTHORIZATION_ERROR",
            status_code=403,
            details=details,
            correlation_id=correlation_id
        )


class RateLimitException(BaseAppException):
    """Exception for rate limiting"""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="RATE_LIMIT_ERROR",
            status_code=429,
            details=details,
            correlation_id=correlation_id
        )


# Alias for backward compatibility
RateLimitError = RateLimitException


class DatabaseException(BaseAppException):
    """Exception for database-related errors"""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="DATABASE_ERROR",
            status_code=503,
            details=details,
            correlation_id=correlation_id
        )


class MCPException(BaseAppException):
    """Exception for MCP client errors"""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="MCP_ERROR",
            status_code=503,
            details=details,
            correlation_id=correlation_id
        )


class ConfigurationException(BaseAppException):
    """Exception for configuration errors"""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="CONFIGURATION_ERROR",
            status_code=500,
            details=details,
            correlation_id=correlation_id
        )


class BusinessLogicException(BaseAppException):
    """Exception for business logic errors"""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            message=message,
            error_code="BUSINESS_LOGIC_ERROR",
            status_code=400,
            details=details,
            correlation_id=correlation_id
        )


class ExternalServiceException(BaseAppException):
    """Exception for external service failures"""

    def __init__(
        self,
        message: str,
        service_name: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        details = details or {}
        details["service"] = service_name
        super().__init__(
            message=message,
            error_code="EXTERNAL_SERVICE_ERROR",
            status_code=502,
            details=details,
            correlation_id=correlation_id
        )


# Convenience functions for common error scenarios
def create_validation_error(
    field: str,
    value: Any,
    reason: str,
    correlation_id: Optional[str] = None
) -> ValidationException:
    """Create a validation error for a specific field"""
    return ValidationException(
        message=f"Validation failed for field '{field}': {reason}",
        details={"field": field, "value": str(value), "reason": reason},
        correlation_id=correlation_id
    )


def create_database_connection_error(
    connection_name: str,
    correlation_id: Optional[str] = None
) -> DatabaseException:
    """Create a database connection error"""
    return DatabaseException(
        message=f"Failed to connect to database '{connection_name}'",
        details={"connection_name": connection_name},
        correlation_id=correlation_id
    )


def create_query_validation_error(
    query: str,
    reason: str,
    correlation_id: Optional[str] = None
) -> ValidationException:
    """Create a query validation error"""
    return ValidationException(
        message=f"Query validation failed: {reason}",
        details={"query": query[:100] + "..." if len(query) > 100 else query, "reason": reason},
        correlation_id=correlation_id
    )


def create_security_violation_error(
    violation_type: str,
    details: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None
) -> AuthorizationException:
    """Create a security violation error"""
    return AuthorizationException(
        message=f"Security violation: {violation_type}",
        details=details,
        correlation_id=correlation_id
    )


# Error code constants for easy reference
ERROR_CODES = {
    "VALIDATION_ERROR": "VALIDATION_ERROR",
    "AUTHENTICATION_ERROR": "AUTHENTICATION_ERROR",
    "AUTHORIZATION_ERROR": "AUTHORIZATION_ERROR",
    "RATE_LIMIT_ERROR": "RATE_LIMIT_ERROR",
    "DATABASE_ERROR": "DATABASE_ERROR",
    "MCP_ERROR": "MCP_ERROR",
    "CONFIGURATION_ERROR": "CONFIGURATION_ERROR",
    "BUSINESS_LOGIC_ERROR": "BUSINESS_LOGIC_ERROR",
    "EXTERNAL_SERVICE_ERROR": "EXTERNAL_SERVICE_ERROR",
}