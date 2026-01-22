"""
CSRF Protection Middleware
Implements double-submit cookie pattern for CSRF protection
"""

import logging
import secrets
import hmac
import hashlib
from typing import Callable
from fastapi import Request, Response, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from app.core.config import settings

logger = logging.getLogger(__name__)

# CSRF token configuration
CSRF_TOKEN_LENGTH = 32
CSRF_COOKIE_NAME = "csrf_token"
CSRF_HEADER_NAME = "X-CSRF-Token"
CSRF_TOKEN_MAX_AGE = 3600 * 8  # 8 hours (same as JWT)

# Methods that require CSRF protection
CSRF_PROTECTED_METHODS = {"POST", "PUT", "DELETE", "PATCH"}

# Endpoints exempt from CSRF validation (e.g., login, public APIs)
CSRF_EXEMPT_PATHS = {
    "/api/v1/auth/login",
    "/api/v1/auth/refresh",
    "/health",
    "/",
    "/docs",
    "/openapi.json",
    "/redoc",
}


def generate_csrf_token() -> str:
    """
    Generate a cryptographically secure CSRF token
    
    Returns:
        Random token string
    """
    return secrets.token_urlsafe(CSRF_TOKEN_LENGTH)


def sign_csrf_token(token: str, secret: str) -> str:
    """
    Sign CSRF token with HMAC for additional security
    
    Args:
        token: CSRF token to sign
        secret: Secret key for signing
    
    Returns:
        Signed token (token:signature)
    """
    signature = hmac.new(
        secret.encode(),
        token.encode(),
        hashlib.sha256
    ).hexdigest()
    return f"{token}:{signature}"


def verify_csrf_token(signed_token: str, secret: str) -> bool:
    """
    Verify CSRF token signature
    
    Args:
        signed_token: Signed token (token:signature)
        secret: Secret key for verification
    
    Returns:
        True if valid, False otherwise
    """
    try:
        if ':' not in signed_token:
            return False
        
        token, signature = signed_token.rsplit(':', 1)
        expected_signature = hmac.new(
            secret.encode(),
            token.encode(),
            hashlib.sha256
        ).hexdigest()
        
        # Use constant-time comparison to prevent timing attacks
        return hmac.compare_digest(signature, expected_signature)
    except Exception as e:
        logger.warning(f"CSRF token verification failed: {e}")
        return False


class CSRFMiddleware(BaseHTTPMiddleware):
    """
    CSRF Protection Middleware using double-submit cookie pattern
    
    Flow:
    1. On first request, generate CSRF token and set as cookie
    2. Frontend reads cookie and includes token in X-CSRF-Token header
    3. Backend validates token matches cookie for state-changing requests
    """
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.secret = settings.jwt_secret_key  # Reuse JWT secret for CSRF signing
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request with CSRF validation
        
        Args:
            request: Incoming request
            call_next: Next middleware/handler
        
        Returns:
            Response with CSRF cookie if needed
        """
        # Skip CSRF for exempt paths
        if self._is_exempt_path(request.url.path):
            response = await call_next(request)
            return response
        
        # Skip CSRF for safe methods (GET, HEAD, OPTIONS)
        if request.method not in CSRF_PROTECTED_METHODS:
            response = await call_next(request)
            # Set CSRF cookie on safe requests if not present
            if CSRF_COOKIE_NAME not in request.cookies:
                self._set_csrf_cookie(response)
            return response
        
        # Validate CSRF token for state-changing requests
        if not self._validate_csrf(request):
            logger.warning(
                f"CSRF validation failed for {request.method} {request.url.path} "
                f"from {request.client.host if request.client else 'unknown'}"
            )
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    "detail": "CSRF validation failed. Please refresh the page and try again.",
                    "error": "csrf_validation_failed",
                },
            )
        
        # Process request
        response = await call_next(request)
        
        # Refresh CSRF cookie on successful state-changing request
        self._set_csrf_cookie(response)
        
        return response
    
    def _is_exempt_path(self, path: str) -> bool:
        """
        Check if path is exempt from CSRF validation
        
        Args:
            path: Request path
        
        Returns:
            True if exempt, False otherwise
        """
        # Exact match
        if path in CSRF_EXEMPT_PATHS:
            return True
        
        # Prefix match for dynamic paths
        for exempt_path in CSRF_EXEMPT_PATHS:
            if path.startswith(exempt_path):
                return True
        
        # Development mode: exempt SSE endpoints (EventSource can't send custom headers)
        if settings.environment == "development" and "/stream" in path:
            return True
        
        return False
    
    def _validate_csrf(self, request: Request) -> bool:
        """
        Validate CSRF token from header matches cookie
        
        Args:
            request: Incoming request
        
        Returns:
            True if valid, False otherwise
        """
        # Get token from cookie
        cookie_token = request.cookies.get(CSRF_COOKIE_NAME)
        if not cookie_token:
            logger.debug("CSRF validation failed: No cookie token")
            return False
        
        # Get token from header
        header_token = request.headers.get(CSRF_HEADER_NAME)
        if not header_token:
            logger.debug("CSRF validation failed: No header token")
            return False
        
        # Verify cookie token signature
        if not verify_csrf_token(cookie_token, self.secret):
            logger.debug("CSRF validation failed: Invalid cookie signature")
            return False
        
        # Verify header token signature
        if not verify_csrf_token(header_token, self.secret):
            logger.debug("CSRF validation failed: Invalid header signature")
            return False
        
        # Extract unsigned tokens for comparison
        cookie_unsigned = cookie_token.split(':')[0]
        header_unsigned = header_token.split(':')[0]
        
        # Compare tokens (constant-time)
        if not hmac.compare_digest(cookie_unsigned, header_unsigned):
            logger.debug("CSRF validation failed: Token mismatch")
            return False
        
        return True
    
    def _set_csrf_cookie(self, response: Response) -> None:
        """
        Set CSRF token cookie on response
        
        Args:
            response: Response to set cookie on
        """
        # Generate new token
        token = generate_csrf_token()
        signed_token = sign_csrf_token(token, self.secret)
        
        # Set cookie with security flags
        response.set_cookie(
            key=CSRF_COOKIE_NAME,
            value=signed_token,
            max_age=CSRF_TOKEN_MAX_AGE,
            httponly=False,  # Must be readable by JavaScript
            secure=settings.environment == "production",  # HTTPS only in production
            samesite="lax",  # Protect against CSRF while allowing normal navigation
            path="/",
        )


def add_csrf_exempt_path(path: str) -> None:
    """
    Add path to CSRF exemption list
    
    Args:
        path: Path to exempt
    """
    CSRF_EXEMPT_PATHS.add(path)


def remove_csrf_exempt_path(path: str) -> None:
    """
    Remove path from CSRF exemption list
    
    Args:
        path: Path to remove
    """
    CSRF_EXEMPT_PATHS.discard(path)
