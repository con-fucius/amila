"""
API v1 Router
Main router for all v1 API endpoints
"""

from fastapi import APIRouter
from app.api.v1.endpoints import auth, queries, health, schema, analytics, corrections, doris_proxy, errors

# Create main API router
api_router = APIRouter()

# Include endpoint routers
api_router.include_router(
    auth.router,
    prefix="/auth",
    tags=["authentication"]
)

api_router.include_router(
    queries.router,
    prefix="/queries",
    tags=["queries"]
)

api_router.include_router(
    schema.router,
    prefix="/schema",
    tags=["schema"]
)

api_router.include_router(
    health.router,
    prefix="/health",
    tags=["health"]
)

api_router.include_router(
    analytics.router,
    prefix="/analytics",
    tags=["analytics"]
)

api_router.include_router(
    corrections.router,
    prefix="/corrections",
    tags=["corrections"]
)

api_router.include_router(
    doris_proxy.router,
    prefix="/mcp/doris",
    tags=["doris-mcp"]
)

# Error reporting endpoint
api_router.include_router(
    errors.router,
    prefix="/errors",
    tags=["errors"]
)
