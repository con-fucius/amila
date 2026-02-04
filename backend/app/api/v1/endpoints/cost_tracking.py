"""
API Endpoints: Cost Tracking & Quota Management
Fix 9: API endpoints for monitoring query costs and enforcing quotas
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field

from app.core.auth import get_current_user, require_permissions
from app.core.config import settings
from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)

router = APIRouter()


# Pydantic Models
class CostUsageResponse(BaseModel):
    """User's current cost usage and quota status."""
    user_id: str
    period: str = Field(description="Billing period (e.g., '2026-02')")
    quota_usd: float = Field(description="Monthly quota in USD")
    used_usd: float = Field(description="Amount used in current period")
    remaining_usd: float = Field(description="Remaining quota")
    percentage_used: float = Field(description="Percentage of quota used")
    queries_count: int = Field(description="Number of queries this period")
    tokens_consumed: int = Field(description="Total LLM tokens consumed")
    alerts: List[str] = Field(default=[], description="Active quota alerts")
    last_updated: str = Field(description="ISO timestamp of last update")


class CostEstimateRequest(BaseModel):
    """Request to estimate query cost before execution."""
    query_complexity: str = Field(default="medium", description="Simple/medium/complex")
    estimated_rows: int = Field(default=1000, description="Estimated rows to scan")
    database_type: str = Field(default="oracle", description="Target database")


class CostEstimateResponse(BaseModel):
    """Estimated cost for a query."""
    estimated_cost_usd: float
    estimated_tokens: int
    estimated_db_time_ms: int
    risk_level: str = Field(description="low/medium/high based on cost")
    breakdown: Dict[str, float] = Field(description="Cost breakdown by component")
    warning: Optional[str] = None


class OrgDashboardResponse(BaseModel):
    """Organization-wide cost dashboard."""
    period: str
    total_queries: int
    total_cost_usd: float
    total_tokens: int
    unique_users: int
    avg_cost_per_query: float
    top_users: List[Dict[str, Any]]
    daily_breakdown: List[Dict[str, Any]]
    cost_by_domain: List[Dict[str, Any]]


class QuotaUpdateRequest(BaseModel):
    """Request to update user quota (admin only)."""
    user_id: str
    new_quota_usd: float
    reason: str


class QuotaUpdateResponse(BaseModel):
    """Response after quota update."""
    success: bool
    user_id: str
    previous_quota: float
    new_quota: float
    effective_from: str


# Helper functions
async def _get_user_cost_key(user_id: str, period: str) -> str:
    """Generate Redis key for user cost tracking."""
    return f"cost:user:{user_id}:{period}"


async def _get_organization_cost_key(period: str) -> str:
    """Generate Redis key for organization cost tracking."""
    return f"cost:org:{settings.ORGANIZATION_ID or 'default'}:{period}"


async def _get_role_quota(user_role: str) -> float:
    """Get monthly quota based on user role."""
    quotas = settings.ROLE_BASED_QUOTAS or {
        "admin": float("inf"),
        "analyst": 50.0,
        "manager": 25.0,
        "viewer": 10.0,
        "default": 5.0
    }
    return quotas.get(user_role, quotas.get("default", 5.0))


# API Endpoints
@router.get("/usage", response_model=CostUsageResponse)
async def get_user_cost_usage(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> CostUsageResponse:
    """
    Get current user's cost usage and quota status.
    """
    user_id = current_user.get("id", "unknown")
    user_role = current_user.get("role", "viewer")
    
    # Get current billing period
    now = datetime.now(timezone.utc)
    period = now.strftime("%Y-%m")
    
    # Get quota for user
    quota_usd = await _get_role_quota(user_role)
    
    # Get usage from Redis
    cost_key = await _get_user_cost_key(user_id, period)
    usage_data = await redis_client.get(cost_key) or {}
    
    used_usd = float(usage_data.get("total_cost", 0))
    queries_count = int(usage_data.get("query_count", 0))
    tokens_consumed = int(usage_data.get("total_tokens", 0))
    
    remaining_usd = quota_usd - used_usd if quota_usd != float("inf") else float("inf")
    percentage_used = (used_usd / quota_usd * 100) if quota_usd > 0 and quota_usd != float("inf") else 0
    
    # Generate alerts
    alerts = []
    if percentage_used >= 100:
        alerts.append("Quota exceeded - queries may be blocked")
    elif percentage_used >= 90:
        alerts.append("Approaching quota limit - 90% used")
    elif percentage_used >= 75:
        alerts.append("75% of quota used")
    
    return CostUsageResponse(
        user_id=user_id,
        period=period,
        quota_usd=quota_usd if quota_usd != float("inf") else -1,
        used_usd=round(used_usd, 4),
        remaining_usd=round(remaining_usd, 4) if remaining_usd != float("inf") else -1,
        percentage_used=round(percentage_used, 2),
        queries_count=queries_count,
        tokens_consumed=tokens_consumed,
        alerts=alerts,
        last_updated=usage_data.get("last_updated", now.isoformat())
    )


@router.post("/estimate", response_model=CostEstimateResponse)
async def estimate_query_cost(
    request: CostEstimateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> CostEstimateResponse:
    """
    Estimate query cost before execution.
    """
    from app.services.cost_service import CostService
    
    # Calculate estimates based on complexity
    cost_estimate = CostService.estimate_query_cost(
        db_type=request.database_type,
        estimated_rows=request.estimated_rows
    )
    
    # Token estimates by complexity
    token_estimates = {
        "simple": 1500,
        "medium": 3500,
        "complex": 8000
    }
    estimated_tokens = token_estimates.get(request.query_complexity, 3500)
    
    # Add LLM cost estimate
    llm_cost = CostService.estimate_llm_cost_for_tokens(estimated_tokens)
    
    total_cost = cost_estimate.estimated_cost_usd + llm_cost
    
    # Determine risk level
    user_role = current_user.get("role", "viewer")
    quota = await _get_role_quota(user_role)
    
    risk_level = "low"
    warning = None
    
    if quota != float("inf"):
        usage = await get_user_cost_usage(current_user)
        projected_usage = (usage.used_usd + total_cost) / quota * 100
        
        if projected_usage > 100:
            risk_level = "high"
            warning = "This query would exceed your monthly quota"
        elif projected_usage > 90:
            risk_level = "medium"
            warning = "This query brings you close to quota limit"
        elif total_cost > quota * 0.1:  # > 10% of quota
            risk_level = "medium"
    
    return CostEstimateResponse(
        estimated_cost_usd=round(total_cost, 4),
        estimated_tokens=estimated_tokens,
        estimated_db_time_ms=cost_estimate.estimated_time_ms,
        risk_level=risk_level,
        breakdown={
            "llm_cost_usd": round(llm_cost, 4),
            "db_cost_usd": round(cost_estimate.estimated_cost_usd, 4),
            "db_time_ms": cost_estimate.estimated_time_ms
        },
        warning=warning
    )


@router.get("/dashboard", response_model=OrgDashboardResponse)
async def get_organization_dashboard(
    period: Optional[str] = Query(None, description="YYYY-MM format, defaults to current month"),
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "view_cost_dashboard"]))
) -> OrgDashboardResponse:
    """
    Get organization-wide cost dashboard (requires view_cost_dashboard permission).
    """
    now = datetime.now(timezone.utc)
    period = period or now.strftime("%Y-%m")
    
    # Get org data from Redis
    org_key = await _get_organization_cost_key(period)
    org_data = await redis_client.get(org_key) or {}
    
    # Get all user usage for the period
    user_pattern = f"cost:user:*:{period}"
    user_keys = await redis_client.keys(user_pattern)
    
    total_cost = 0.0
    total_queries = 0
    total_tokens = 0
    unique_users = set()
    top_users = []
    
    for key in user_keys[:100]:  # Limit for performance
        try:
            user_data = await redis_client.get(key) or {}
            user_id = key.split(":")[2]
            user_cost = float(user_data.get("total_cost", 0))
            
            if user_cost > 0:
                unique_users.add(user_id)
                total_cost += user_cost
                total_queries += int(user_data.get("query_count", 0))
                total_tokens += int(user_data.get("total_tokens", 0))
                
                top_users.append({
                    "user_id": user_id,
                    "cost_usd": round(user_cost, 4),
                    "queries": int(user_data.get("query_count", 0)),
                    "tokens": int(user_data.get("total_tokens", 0))
                })
        except Exception as e:
            logger.warning(f"Failed to process user cost data: {e}")
    
    # Sort and limit top users
    top_users.sort(key=lambda x: x["cost_usd"], reverse=True)
    top_users = top_users[:10]
    
    # Generate daily breakdown (simplified)
    daily_breakdown = []
    for i in range(30):
        day = now.replace(day=1) + timedelta(days=i)
        if day.month != now.month:
            break
        daily_breakdown.append({
            "date": day.strftime("%Y-%m-%d"),
            "cost_usd": round(total_cost / 30, 4),  # Simplified distribution
            "queries": total_queries // 30
        })
    
    # Cost by domain (from taxonomy data if available)
    cost_by_domain = org_data.get("by_domain", [
        {"domain": "sales", "cost_usd": round(total_cost * 0.4, 4), "percentage": 40},
        {"domain": "finance", "cost_usd": round(total_cost * 0.3, 4), "percentage": 30},
        {"domain": "operations", "cost_usd": round(total_cost * 0.2, 4), "percentage": 20},
        {"domain": "general", "cost_usd": round(total_cost * 0.1, 4), "percentage": 10}
    ])
    
    return OrgDashboardResponse(
        period=period,
        total_queries=total_queries,
        total_cost_usd=round(total_cost, 4),
        total_tokens=total_tokens,
        unique_users=len(unique_users),
        avg_cost_per_query=round(total_cost / max(total_queries, 1), 4),
        top_users=top_users,
        daily_breakdown=daily_breakdown,
        cost_by_domain=cost_by_domain
    )


@router.post("/quota", response_model=QuotaUpdateResponse)
async def update_user_quota(
    request: QuotaUpdateRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> QuotaUpdateResponse:
    """
    Update user quota (admin only).
    """
    # Get current quota
    user_key = f"cost:user:{request.user_id}:quota"
    current_quota_data = await redis_client.get(user_key) or {}
    previous_quota = float(current_quota_data.get("quota_usd", 5.0))
    
    # Update quota
    now = datetime.now(timezone.utc)
    await redis_client.set(user_key, {
        "quota_usd": request.new_quota_usd,
        "updated_by": current_user.get("id"),
        "updated_at": now.isoformat(),
        "reason": request.reason
    })
    
    logger.info(f"Quota updated for user {request.user_id}: {previous_quota} -> {request.new_quota_usd} by {current_user.get('id')}")
    
    return QuotaUpdateResponse(
        success=True,
        user_id=request.user_id,
        previous_quota=previous_quota,
        new_quota=request.new_quota_usd,
        effective_from=now.isoformat()
    )


# Budget Forecasting Endpoints
class BudgetForecastResponse(BaseModel):
    """Budget forecast response."""
    current_period: str
    current_usage: float
    forecasted_usage: float
    budget_limit: float
    projected_overrun: Optional[float]
    confidence_interval_low: float
    confidence_interval_high: float
    days_remaining: int
    trend_direction: str
    daily_average: float
    recommended_daily_budget: float


class CostAnomalyResponse(BaseModel):
    """Cost anomaly detection response."""
    date: str
    cost: float
    expected_cost: float
    deviation_percentage: float
    severity: str
    description: str


class BudgetAlertResponse(BaseModel):
    """Budget alert response."""
    alert_level: str
    message: str
    current_usage: float
    budget_limit: float
    percentage_used: float
    recommended_action: str
    triggered_at: str


class CostOptimizationRecommendation(BaseModel):
    """Cost optimization recommendation."""
    type: str
    priority: str
    message: str
    details: str
    potential_savings: str


@router.get("/forecast", response_model=BudgetForecastResponse)
async def get_budget_forecast(
    budget_limit: Optional[float] = Query(None, description="Optional budget limit override"),
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> BudgetForecastResponse:
    """
    Get budget forecast for current user.
    
    Provides cost forecasting based on historical usage patterns,
    including trend analysis and projected overrun detection.
    """
    user_id = current_user.get("id", "unknown")
    
    try:
        from app.services.cost_forecasting_service import cost_forecasting_service
        
        forecast = await cost_forecasting_service.generate_forecast(
            user_id=user_id,
            budget_limit=budget_limit
        )
        
        return BudgetForecastResponse(
            current_period=forecast.current_period,
            current_usage=forecast.current_usage,
            forecasted_usage=forecast.forecasted_usage,
            budget_limit=forecast.budget_limit,
            projected_overrun=forecast.projected_overrun,
            confidence_interval_low=forecast.confidence_interval[0],
            confidence_interval_high=forecast.confidence_interval[1],
            days_remaining=forecast.days_remaining,
            trend_direction=forecast.trend_direction.value,
            daily_average=forecast.daily_average,
            recommended_daily_budget=forecast.recommended_daily_budget
        )
    except Exception as e:
        logger.error(f"Failed to generate budget forecast: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate forecast: {str(e)}"
        )


@router.get("/anomalies", response_model=List[CostAnomalyResponse])
async def get_cost_anomalies(
    days: int = Query(30, ge=7, le=90, description="Number of days to analyze"),
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> List[CostAnomalyResponse]:
    """
    Detect cost anomalies for current user.
    
    Identifies unusual spending patterns using statistical analysis.
    """
    user_id = current_user.get("id", "unknown")
    
    try:
        from app.services.cost_forecasting_service import cost_forecasting_service
        
        anomalies = await cost_forecasting_service.detect_anomalies(
            user_id=user_id,
            days=days
        )
        
        return [
            CostAnomalyResponse(
                date=a.date,
                cost=a.cost,
                expected_cost=a.expected_cost,
                deviation_percentage=a.deviation_percentage,
                severity=a.severity.value,
                description=a.description
            )
            for a in anomalies
        ]
    except Exception as e:
        logger.error(f"Failed to detect cost anomalies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to detect anomalies: {str(e)}"
        )


@router.get("/budget-alerts", response_model=List[BudgetAlertResponse])
async def get_budget_alerts(
    budget_limit: Optional[float] = Query(None, description="Optional budget limit override"),
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> List[BudgetAlertResponse]:
    """
    Get budget threshold alerts for current user.
    
    Checks for warning (80%) and critical (95%) budget thresholds.
    """
    user_id = current_user.get("id", "unknown")
    
    try:
        from app.services.cost_forecasting_service import cost_forecasting_service
        
        alerts = await cost_forecasting_service.check_budget_alerts(
            user_id=user_id,
            budget_limit=budget_limit
        )
        
        return [
            BudgetAlertResponse(
                alert_level=a.alert_level.value,
                message=a.message,
                current_usage=a.current_usage,
                budget_limit=a.budget_limit,
                percentage_used=a.percentage_used,
                recommended_action=a.recommended_action,
                triggered_at=a.triggered_at
            )
            for a in alerts
        ]
    except Exception as e:
        logger.error(f"Failed to check budget alerts: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check alerts: {str(e)}"
        )


@router.get("/optimization", response_model=List[CostOptimizationRecommendation])
async def get_cost_optimization_recommendations(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> List[CostOptimizationRecommendation]:
    """
    Get cost optimization recommendations for current user.
    
    Provides actionable suggestions to reduce query costs based on
    usage patterns and historical data.
    """
    user_id = current_user.get("id", "unknown")
    
    try:
        from app.services.cost_forecasting_service import cost_forecasting_service
        
        recommendations = await cost_forecasting_service.get_optimization_recommendations(
            user_id=user_id
        )
        
        return [
            CostOptimizationRecommendation(
                type=r["type"],
                priority=r["priority"],
                message=r["message"],
                details=r["details"],
                potential_savings=r["potential_savings"]
            )
            for r in recommendations
        ]
    except Exception as e:
        logger.error(f"Failed to get optimization recommendations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get recommendations: {str(e)}"
        )


@router.get("/alerts")
async def get_cost_alerts(
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "view_cost_dashboard"]))
) -> Dict[str, Any]:
    """
    Get users approaching or exceeding quotas.
    """
    now = datetime.now(timezone.utc)
    period = now.strftime("%Y-%m")
    
    alerts = {
        "exceeded": [],
        "approaching": [],
        "high_cost_recently": []
    }
    
    user_pattern = f"cost:user:*:{period}"
    user_keys = await redis_client.keys(user_pattern)
    
    for key in user_keys[:100]:
        try:
            user_data = await redis_client.get(key) or {}
            user_id = key.split(":")[2]
            
            usage_data = await redis_client.get(f"cost:user:{user_id}:{period}") or {}
            used = float(usage_data.get("total_cost", 0))
            
            # Get user quota (default or custom)
            quota_key = f"cost:user:{user_id}:quota"
            quota_data = await redis_client.get(quota_key) or {}
            quota = quota_data.get("quota_usd", 5.0)
            
            if used > quota:
                alerts["exceeded"].append({
                    "user_id": user_id,
                    "used_usd": round(used, 4),
                    "quota_usd": quota,
                    "overage": round(used - quota, 4)
                })
            elif used > quota * 0.9:
                alerts["approaching"].append({
                    "user_id": user_id,
                    "used_usd": round(used, 4),
                    "quota_usd": quota,
                    "percentage": round(used / quota * 100, 2)
                })
            
            # Check recent high-cost queries (last 24h)
            recent_cost = float(user_data.get("recent_cost_24h", 0))
            if recent_cost > quota * 0.5:
                alerts["high_cost_recently"].append({
                    "user_id": user_id,
                    "recent_cost_24h": round(recent_cost, 4),
                    "alert_threshold": round(quota * 0.5, 4)
                })
        except Exception as e:
            logger.warning(f"Failed to process alert check: {e}")
    
    return alerts
