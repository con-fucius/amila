"""
Global Error Handler Middleware
Provides standardized error responses, correlation IDs, and comprehensive logging
"""

import logging
import time
import uuid
from typing import Any, Dict, Callable, Optional
from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .exceptions import BaseAppException


logger = logging.getLogger(__name__)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Middleware to add correlation ID and request context to each request"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate correlation ID if not present
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())

        # Add correlation ID to request state
        request.state.correlation_id = correlation_id

        # Add request start time for performance tracking
        request.state.request_start_time = time.time()

        # Call the next middleware/route handler
        response = await call_next(request)

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id

        return response


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """Middleware to handle and standardize all application errors"""

    def __init__(self, app: Callable, include_traceback: bool = False):
        super().__init__(app)
        self.include_traceback = include_traceback

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            # Process the request
            response = await call_next(request)

            # Log successful requests with performance info
            if hasattr(request.state, "request_start_time"):
                duration = time.time() - request.state.request_start_time
                correlation_id = getattr(request.state, "correlation_id", "unknown")
                logger.info(
                    f"Request completed successfully",
                    extra={
                        "correlation_id": correlation_id,
                        "method": request.method,
                        "url": str(request.url),
                        "status_code": response.status_code,
                        "duration_ms": round(duration * 1000, 2)
                    }
                )

            return response

        except BaseAppException as e:
            # Handle custom application exceptions
            return await self._handle_app_exception(request, e)

        except HTTPException as e:
            # Handle FastAPI HTTP exceptions
            return await self._handle_http_exception(request, e)

        except Exception as e:
            # Handle unexpected exceptions
            return await self._handle_unexpected_exception(request, e)

    async def _handle_app_exception(self, request: Request, exc: BaseAppException) -> JSONResponse:
        """Handle custom application exceptions with RFC 7807 Problem Details format"""
        correlation_id = getattr(request.state, "correlation_id", "unknown")

        # Update exception with correlation ID if not set
        if not exc.correlation_id:
            exc.correlation_id = correlation_id

        # Log the error with context
        logger.error(
            f"Application error: {exc.error_code} - {exc.message}",
            extra={
                "correlation_id": correlation_id,
                "error_code": exc.error_code,
                "status_code": exc.status_code,
                "details": exc.details,
                "method": request.method,
                "url": str(request.url),
                "user_agent": request.headers.get("User-Agent", "unknown"),
                "client_ip": self._get_client_ip(request)
            },
            exc_info=True
        )

        # Return RFC 7807 Problem Details response (Gap #32)
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.to_rfc7807(),
            headers={"Content-Type": "application/problem+json"}
        )

    async def _handle_http_exception(self, request: Request, exc: HTTPException) -> JSONResponse:
        """Handle FastAPI HTTP exceptions with RFC 7807 Problem Details format"""
        correlation_id = getattr(request.state, "correlation_id", "unknown")

        # Map HTTP status codes to error codes
        error_code = self._map_http_status_to_error_code(exc.status_code)

        # Create RFC 7807 Problem Details response (Gap #32)
        error_response = {
            "type": f"https://amila.local/errors/{error_code.lower().replace('_', '-')}",
            "title": error_code.replace("_", " ").title(),
            "status": exc.status_code,
            "detail": str(exc.detail),
            "instance": f"/errors/{correlation_id}",
            "correlationId": correlation_id
        }

        # Log the error
        logger.warning(
            f"HTTP exception: {exc.status_code} - {exc.detail}",
            extra={
                "correlation_id": correlation_id,
                "error_code": error_code,
                "status_code": exc.status_code,
                "method": request.method,
                "url": str(request.url),
                "user_agent": request.headers.get("User-Agent", "unknown"),
                "client_ip": self._get_client_ip(request)
            }
        )

        return JSONResponse(
            status_code=exc.status_code,
            content=error_response,
            headers={"Content-Type": "application/problem+json"}
        )

    async def _handle_unexpected_exception(self, request: Request, exc: Exception) -> JSONResponse:
        """Handle unexpected exceptions with RFC 7807 Problem Details format"""
        correlation_id = getattr(request.state, "correlation_id", "unknown")

        # Log the critical error
        logger.critical(
            f"Unexpected error: {str(exc)}",
            extra={
                "correlation_id": correlation_id,
                "error_code": "INTERNAL_SERVER_ERROR",
                "method": request.method,
                "url": str(request.url),
                "user_agent": request.headers.get("User-Agent", "unknown"),
                "client_ip": self._get_client_ip(request)
            },
            exc_info=True
        )

        # Return RFC 7807 Problem Details response (Gap #32)
        return JSONResponse(
            status_code=500,
            content={
                "type": "https://amila.local/errors/internal-server-error",
                "title": "Internal Server Error",
                "status": 500,
                "detail": "An unexpected error occurred. Please try again later.",
                "instance": f"/errors/{correlation_id}",
                "correlationId": correlation_id
            },
            headers={"Content-Type": "application/problem+json"}
        )

    def _map_http_status_to_error_code(self, status_code: int) -> str:
        """Map HTTP status codes to standardized error codes"""
        mapping = {
            400: "BAD_REQUEST",
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            405: "METHOD_NOT_ALLOWED",
            409: "CONFLICT",
            422: "UNPROCESSABLE_ENTITY",
            429: "TOO_MANY_REQUESTS",
            500: "INTERNAL_SERVER_ERROR",
            502: "BAD_GATEWAY",
            503: "SERVICE_UNAVAILABLE",
            504: "GATEWAY_TIMEOUT"
        }
        return mapping.get(status_code, "HTTP_ERROR")

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address from request"""
        # Check for forwarded headers first
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        # Check for real IP header
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        # Fallback to client host
        client_host = getattr(request.client, "host", "unknown") if request.client else "unknown"
        return client_host


def setup_error_handling(app: Callable, include_traceback: bool = False) -> None:
    """
    Setup error handling middleware for the FastAPI application

    Args:
        app: FastAPI application instance
        include_traceback: Whether to include stack traces in error responses (development only)
    """
    # Add request context middleware
    app.add_middleware(RequestContextMiddleware)

    # Add error handler middleware
    app.add_middleware(ErrorHandlerMiddleware, include_traceback=include_traceback)

    logger.info("Error handling middleware configured")


# Utility functions for error handling
def log_error_with_context(
    message: str,
    error_code: str,
    correlation_id: str,
    extra: Optional[Dict[str, Any]] = None,
    exc_info: bool = True
) -> None:
    """Log an error with standardized context"""
    log_data = {
        "correlation_id": correlation_id,
        "error_code": error_code
    }
    if extra:
        log_data.update(extra)

    logger.error(message, extra=log_data, exc_info=exc_info)


def create_error_response(
    error_code: str,
    message: str,
    status_code: int,
    correlation_id: str,
    details: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a standardized error response dictionary"""
    return {
        "error": {
            "code": error_code,
            "message": message,
            "correlation_id": correlation_id,
            "details": details or {}
        }
    }