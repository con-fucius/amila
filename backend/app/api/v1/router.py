"""
API v1 Router
Main router for all v1 API endpoints
"""

from fastapi import APIRouter
from app.api.v1.endpoints import auth, health, schema, analytics, corrections, doris_proxy, errors, governance, diagnostics, qlik, superset, cost_tracking, ldap, skills, feedback, dashboards, reports, custom_metrics, slo, webhooks, rate_limits, graphql
from app.api.v1.endpoints.queries import router as queries_router

# Create main API router
api_router = APIRouter()

# Include endpoint routers
api_router.include_router(
    auth.router,
    prefix="/auth",
    tags=["authentication"]
)

api_router.include_router(
    queries_router,
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

api_router.include_router(
    errors.router,
    prefix="/errors",
    tags=["errors"]
)

api_router.include_router(
    governance.router,
    prefix="/governance",
    tags=["governance"]
)

api_router.include_router(
    diagnostics.router,
    tags=["diagnostics"]
)

api_router.include_router(
    qlik.router,
    tags=["qlik"]
)

api_router.include_router(
    superset.router,
    tags=["superset"]
)

api_router.include_router(
    dashboards.router,
    prefix="/dashboards",
    tags=["dashboards"]
)

api_router.include_router(
    reports.router,
    prefix="/reports",
    tags=["reports"]
)

api_router.include_router(
    custom_metrics.router,
    prefix="/metrics",
    tags=["metrics"]
)

api_router.include_router(
    slo.router,
    prefix="/slo",
    tags=["slo"]
)

api_router.include_router(
    cost_tracking.router,
    prefix="/cost",
    tags=["cost-tracking"]
)

api_router.include_router(
    ldap.router,
    prefix="/ldap",
    tags=["ldap"]
)

api_router.include_router(
    skills.router,
    prefix="/skills",
    tags=["skills"]
)

api_router.include_router(
    feedback.router,
    prefix="/feedback",
    tags=["feedback"]
)

api_router.include_router(
    webhooks.router,
    tags=["webhooks"]
)

api_router.include_router(
    rate_limits.router,
)

api_router.include_router(
    graphql.router,
    tags=["graphql"],
)
