"""
FastAPI Application Factory
Creates and configures the FastAPI application instance
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import Response, JSONResponse
import logging
from datetime import datetime, timezone
import time

from app.core.config import settings
from app.core.logging_config import setup_logging
from app.core.error_handler import setup_error_handling
from app.core.security_middleware import CSPMiddleware, CSRFMiddleware
from app.core.logging_middleware import RequestLoggingMiddleware
from app.api.v1.router import api_router
from app.core.lifespan import lifespan
from app.core.client_registry import registry

# Initialize logger
logger = logging.getLogger(__name__)

def create_application() -> FastAPI:
    """
    Create and configure FastAPI application
    
    Returns:
        FastAPI: Configured application instance
    """
    # Setup logging first
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Create FastAPI instance
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="Amila - Secure natural language database querying",
        docs_url="/docs" if settings.is_development else None,
        redoc_url="/redoc" if settings.is_development else None,
        openapi_url="/openapi.json" if settings.is_development else None,
        lifespan=lifespan
    )
    
    # Add security middleware (order matters - outermost first)
    
    # 0. Request logging & correlation IDs
    app.add_middleware(RequestLoggingMiddleware)

    # 1. Content Security Policy (CSP) & Security Headers
    app.add_middleware(CSPMiddleware)
    
    # 2. CSRF Protection (disabled in development)
    if not settings.is_development:
        app.add_middleware(CSRFMiddleware)
    
    # 3. Trusted Host Protection (production only)
    if not settings.is_development:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=["localhost", "127.0.0.1"]
        )
    
    # 4. CORS middleware
    # In development, merge common dev origins to avoid front-end origin mismatches
    dev_origins = [
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://localhost:3000",
    ]
    allowed_origins = settings.cors_origins
    if getattr(settings, "is_development", False):
        try:
            # settings.cors_origins may be tuple/list; dedupe while preserving type
            allowed_origins = list({*allowed_origins, *dev_origins})
        except Exception:
            allowed_origins = dev_origins

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=settings.cors_allow_methods,
        allow_headers=settings.cors_allow_headers,
    )

    # Setup standardized error handling
    setup_error_handling(app, include_traceback=settings.is_development)

    # Include API routes
    app.include_router(api_router, prefix="/api/v1")
    
    # Health check endpoint
    @app.get("/health")
    async def health_check():
        """Enhanced health check endpoint for load balancers"""
        
        # Basic health status
        health_status = {
            "status": "healthy",
            "service": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": int(time.time() - getattr(app.state, "start_time", time.time()))
        }
        
        # Add component status (dynamic where possible)
        doris_ready = getattr(app.state, "doris_initialized", False)
        sqlcl_ready = getattr(app.state, "pool_initialized", False)
        mcp_ready = getattr(app.state, "mcp_initialized", False)
        
        # Database is available if either Doris or SQLcl is ready
        db_available = doris_ready or sqlcl_ready or mcp_ready
        
        health_status["components"] = {
            "doris_mcp": "connected" if doris_ready else "inactive",
            "sqlcl_pool": "active" if sqlcl_ready else "inactive",
            "mcp_client": "connected" if mcp_ready else "fallback",
            "database": "available" if db_available else "mock",
            "redis": "disconnected",  # dynamic update below
            "graphiti": "connected" if getattr(app.state, "graphiti_initialized", False) else "inactive"
        }

        # Add pool status if available
        sqlcl_pool = registry.get_sqlcl_pool()
        if sqlcl_pool and getattr(app.state, "pool_initialized", False):
            health_status["pool_status"] = sqlcl_pool.get_status()

        # Dynamic Redis health check (does not fail the endpoint)
        try:
            from app.core.redis_client import redis_client
            redis_health = await redis_client.health_check()
            is_healthy = redis_health.get("status") == "healthy"
            health_status["components"]["redis"] = "connected" if is_healthy else "disconnected"
            if not is_healthy:
                health_status.setdefault("warnings", []).append(f"Redis unhealthy: {redis_health.get('error','unknown')}")
                health_status["status"] = "degraded"
        except Exception as _e:
            # Keep prior status; note as warning
            health_status.setdefault("warnings", []).append("Redis health check error")

        # Add startup warnings if any
        if hasattr(app.state, "startup_errors") and app.state.startup_errors:
            health_status["warnings"] = app.state.startup_errors
            health_status["status"] = "degraded"
        
        return health_status
    
    # Prometheus metrics endpoint
    @app.get("/metrics")
    async def metrics():
        """Prometheus metrics endpoint for Grafana dashboard"""
        try:
            from app.core.prometheus_metrics import get_metrics, get_content_type
            return Response(content=get_metrics(), media_type=get_content_type())
        except Exception as e:
            logger.error(f"Error generating metrics: {e}")
            return Response(
                content=f"# Error generating metrics: {e}\n",
                media_type="text/plain; version=0.0.4"
            )
    
    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint"""
        return {
            "message": f"Welcome to {settings.app_name}",
            "version": settings.app_version,
            "docs_url": "/docs" if settings.is_development else None
        }
    
    logger.info(f"{settings.app_name} application created successfully")
    return app
