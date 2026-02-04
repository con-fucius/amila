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
from app.core.security_middleware import CSPMiddleware, CSRFMiddleware, HMACMiddleware
from app.core.logging_middleware import RequestLoggingMiddleware
from app.api.v1.router import api_router
from app.api.docs import router as docs_router
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
    
    # 2. CSRF Protection (enabled with warnings in development, enforced in production)
    app.add_middleware(CSRFMiddleware)
    
    
    # 3. HMAC Request Signing (before CSRF/CORS to reject invalid requests early)
    app.add_middleware(HMACMiddleware)

    # 4. Trusted Host Protection (production only)
    if not settings.is_development:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=["localhost", "127.0.0.1"]
        )
    
    # 4. CORS middleware
    # Production: Use explicit origins only
    # Development: Merge common dev origins for convenience
    if settings.environment == "production":
        # Production: Strict CORS with explicit origins
        allowed_origins = settings.cors_origins
        logger.info(f"Production CORS: {len(allowed_origins)} explicit origins configured")
    else:
        # Development: Merge dev origins for convenience
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
                logger.info(f"Development CORS: {len(allowed_origins)} origins (including dev defaults)")
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

    app.include_router(docs_router)
    
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
        
        # Dynamic Health Checks
        # Execute checks in parallel for performance
        import asyncio
        from app.core.doris_client import doris_client
        from app.core.graphiti_client import get_graphiti_client
        from app.core.redis_client import redis_client

        async def check_component(name, coro):
            try:
                return name, await coro
            except Exception as e:
                return name, {"status": "error", "message": str(e)}

        # Redis Check
        redis_task = check_component("redis", redis_client.health_check())

        # Doris Check
        doris_task = check_component("doris_mcp", doris_client.health_check())

        # Graphiti Check
        async def check_graphiti():
            client = await get_graphiti_client()
            return await client.health_check()
        graph_task = check_component("graphiti", check_graphiti())

        # Run checks
        results = await asyncio.gather(redis_task, doris_task, graph_task)
        health_results = dict(results)

        # Process Results
        redis_status = health_results["redis"].get("status") == "healthy"
        doris_status = health_results["doris_mcp"].get("status") == "connected"
        graph_status = health_results["graphiti"].get("status") == "connected"

        # SQLcl Status (still static for now as it's a pool)
        sqlcl_ready = getattr(app.state, "pool_initialized", False)
        
        # Determine Aggregate Database Status
        # Database is available if either specific DB path is working
        db_available = doris_status or sqlcl_ready

        # Integrate with DegradedModeManager
        from app.core.degraded_mode_manager import degraded_mode_manager
        system_status = degraded_mode_manager.get_system_status()

        health_status["components"] = {
            "doris_mcp": health_results["doris_mcp"].get("status", "unknown"),
            "sqlcl_pool": "active" if sqlcl_ready else "inactive",
            "mcp_client": "connected" if getattr(app.state, "mcp_initialized", False) else "fallback", 
            "database": "available" if db_available else "mock",
            "redis": "connected" if redis_status else "disconnected",
            "graphiti": health_results["graphiti"].get("status", "unknown"),
            "langgraph_checkpointer": degraded_mode_manager.get_component_status("langgraph_checkpointer").status.value if degraded_mode_manager.get_component_status("langgraph_checkpointer") else "unknown"
        }
        
        health_status["system_detail"] = system_status

        # Add warnings
        if not redis_status:
            health_status.setdefault("warnings", []).append(f"Redis unhealthy: {health_results['redis'].get('error', 'unknown')}")
        if not doris_status and settings.DORIS_MCP_ENABLED:
             health_status.setdefault("warnings", []).append(f"Doris MCP unhealthy: {health_results['doris_mcp'].get('message', 'unknown')}")

        if health_status.get("warnings"):
            health_status["status"] = "degraded"

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
