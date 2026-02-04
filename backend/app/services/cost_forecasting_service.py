"""
Query Cost Budget Forecasting Service

Provides budget forecasting and anomaly detection for query costs.
Helps prevent budget overruns and provides spending insights.

Features:
- Daily/weekly/monthly cost forecasting
- Budget anomaly detection
- Spending trend analysis
- Alert generation for budget thresholds
- Cost optimization recommendations
"""

import logging
import statistics
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class BudgetAlertLevel(Enum):
    """Budget alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class TrendDirection(Enum):
    """Cost trend direction"""
    INCREASING = "increasing"
    DECREASING = "decreasing"
    STABLE = "stable"


@dataclass
class BudgetForecast:
    """Budget forecast result"""
    current_period: str
    current_usage: float
    forecasted_usage: float
    budget_limit: float
    projected_overrun: Optional[float]
    confidence_interval: Tuple[float, float]
    days_remaining: int
    trend_direction: TrendDirection
    daily_average: float
    recommended_daily_budget: float


@dataclass
class CostAnomaly:
    """Detected cost anomaly"""
    date: str
    cost: float
    expected_cost: float
    deviation_percentage: float
    severity: BudgetAlertLevel
    description: str


@dataclass
class BudgetAlert:
    """Budget alert"""
    alert_level: BudgetAlertLevel
    message: str
    current_usage: float
    budget_limit: float
    percentage_used: float
    recommended_action: str
    triggered_at: str


class CostForecastingService:
    """
    Service for forecasting query costs and detecting anomalies.
    
    Provides:
    - Cost trend analysis
    - Budget forecasting
    - Anomaly detection
    - Optimization recommendations
    """
    
    # Forecasting settings
    FORECAST_DAYS = 30
    ANOMALY_THRESHOLD_STD = 2.0  # Standard deviations
    TREND_WINDOW_DAYS = 7
    
    # Alert thresholds
    WARNING_THRESHOLD = 0.80  # 80%
    CRITICAL_THRESHOLD = 0.95  # 95%
    
    def __init__(self):
        self.redis_prefix = "cost:forecast:"
    
    async def get_historical_costs(
        self,
        user_id: str,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get historical cost data for a user.
        
        Args:
            user_id: User identifier
            days: Number of days to retrieve
            
        Returns:
            List of daily cost records
        """
        costs = []
        today = datetime.now(timezone.utc)
        
        for i in range(days):
            date = today - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")
            
            key = f"cost:user:{user_id}:daily:{date_str}"
            try:
                cost = await redis_client._client.get(key)
                if cost:
                    costs.append({
                        "date": date_str,
                        "cost": float(cost),
                        "day_of_week": date.strftime("%A")
                    })
            except Exception as e:
                logger.debug(f"Failed to get cost for {date_str}: {e}")
        
        return sorted(costs, key=lambda x: x["date"])
    
    async def generate_forecast(
        self,
        user_id: str,
        budget_limit: Optional[float] = None
    ) -> BudgetForecast:
        """
        Generate cost forecast for a user.
        
        Args:
            user_id: User identifier
            budget_limit: Monthly budget limit (optional)
            
        Returns:
            BudgetForecast with predictions
        """
        # Get budget if not provided
        if budget_limit is None:
            from app.services.query_cost_tracker import QueryCostTracker
            budget = await QueryCostTracker.get_user_budget(user_id)
            budget_limit = budget.monthly_budget_usd
        
        # Get historical costs
        historical = await self.get_historical_costs(user_id, days=30)
        
        if not historical:
            # No historical data - use default forecast
            return self._create_default_forecast(budget_limit)
        
        # Calculate metrics
        costs = [h["cost"] for h in historical]
        daily_average = statistics.mean(costs)
        
        # Detect trend
        trend = self._calculate_trend(historical)
        
        # Calculate current month usage
        current_month_costs = await self._get_current_month_costs(user_id)
        current_usage = sum(current_month_costs)
        
        # Forecast remaining month
        today = datetime.now(timezone.utc)
        last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        days_remaining = (last_day - today).days + 1
        
        # Apply trend adjustment
        trend_multiplier = 1.0
        if trend == TrendDirection.INCREASING:
            trend_multiplier = 1.1
        elif trend == TrendDirection.DECREASING:
            trend_multiplier = 0.9
        
        projected_daily = daily_average * trend_multiplier
        forecasted_remaining = projected_daily * days_remaining
        forecasted_total = current_usage + forecasted_remaining
        
        # Calculate confidence interval
        if len(costs) >= 7:
            std_dev = statistics.stdev(costs[-7:])
            confidence_low = max(0, forecasted_total - (std_dev * days_remaining))
            confidence_high = forecasted_total + (std_dev * days_remaining)
        else:
            confidence_low = forecasted_total * 0.8
            confidence_high = forecasted_total * 1.2
        
        # Check for projected overrun
        projected_overrun = None
        if forecasted_total > budget_limit:
            projected_overrun = forecasted_total - budget_limit
        
        # Calculate recommended daily budget
        remaining_budget = budget_limit - current_usage
        recommended_daily = remaining_budget / max(1, days_remaining)
        
        return BudgetForecast(
            current_period=today.strftime("%Y-%m"),
            current_usage=round(current_usage, 4),
            forecasted_usage=round(forecasted_total, 4),
            budget_limit=round(budget_limit, 4),
            projected_overrun=round(projected_overrun, 4) if projected_overrun else None,
            confidence_interval=(round(confidence_low, 4), round(confidence_high, 4)),
            days_remaining=days_remaining,
            trend_direction=trend,
            daily_average=round(daily_average, 4),
            recommended_daily_budget=round(recommended_daily, 4)
        )
    
    async def detect_anomalies(
        self,
        user_id: str,
        days: int = 30
    ) -> List[CostAnomaly]:
        """
        Detect cost anomalies for a user.
        
        Args:
            user_id: User identifier
            days: Number of days to analyze
            
        Returns:
            List of detected anomalies
        """
        historical = await self.get_historical_costs(user_id, days)
        
        if len(historical) < 7:
            return []  # Not enough data
        
        costs = [h["cost"] for h in historical]
        mean_cost = statistics.mean(costs)
        
        if len(costs) >= 2:
            std_cost = statistics.stdev(costs)
        else:
            std_cost = mean_cost * 0.2  # Estimate 20% variance
        
        anomalies = []
        
        for record in historical:
            cost = record["cost"]
            deviation = abs(cost - mean_cost)
            
            if std_cost > 0:
                std_deviations = deviation / std_cost
            else:
                std_deviations = 0
            
            if std_deviations >= self.ANOMALY_THRESHOLD_STD:
                deviation_pct = ((cost - mean_cost) / mean_cost) * 100 if mean_cost > 0 else 0
                
                # Determine severity
                if std_deviations >= 3.0:
                    severity = BudgetAlertLevel.CRITICAL
                elif std_deviations >= 2.5:
                    severity = BudgetAlertLevel.WARNING
                else:
                    severity = BudgetAlertLevel.INFO
                
                anomaly = CostAnomaly(
                    date=record["date"],
                    cost=cost,
                    expected_cost=round(mean_cost, 4),
                    deviation_percentage=round(deviation_pct, 2),
                    severity=severity,
                    description=f"Cost was {deviation_pct:+.1f}% vs typical ${mean_cost:.2f}"
                )
                anomalies.append(anomaly)
        
        return sorted(anomalies, key=lambda x: x.date, reverse=True)
    
    async def check_budget_alerts(
        self,
        user_id: str,
        budget_limit: Optional[float] = None
    ) -> List[BudgetAlert]:
        """
        Check for budget threshold alerts.
        
        Args:
            user_id: User identifier
            budget_limit: Monthly budget limit (optional)
            
        Returns:
            List of budget alerts
        """
        # Get current usage
        from app.services.query_cost_tracker import QueryCostTracker
        usage = await QueryCostTracker.get_user_usage(user_id)
        
        if budget_limit is None:
            budget = await QueryCostTracker.get_user_budget(user_id)
            budget_limit = budget.monthly_budget_usd
        
        current_usage = usage["monthly_usage_usd"]
        percentage_used = (current_usage / budget_limit) * 100 if budget_limit > 0 else 0
        
        alerts = []
        now = datetime.now(timezone.utc).isoformat()
        
        # Check critical threshold
        if percentage_used >= self.CRITICAL_THRESHOLD * 100:
            remaining = budget_limit - current_usage
            alerts.append(BudgetAlert(
                alert_level=BudgetAlertLevel.CRITICAL,
                message=f"Monthly budget CRITICAL: {percentage_used:.1f}% used",
                current_usage=current_usage,
                budget_limit=budget_limit,
                percentage_used=percentage_used,
                recommended_action=f"Reduce query volume. Only ${remaining:.2f} remaining.",
                triggered_at=now
            ))
        
        # Check warning threshold
        elif percentage_used >= self.WARNING_THRESHOLD * 100:
            remaining = budget_limit - current_usage
            days_left = self._get_days_in_month()
            recommended_daily = remaining / max(1, days_left)
            
            alerts.append(BudgetAlert(
                alert_level=BudgetAlertLevel.WARNING,
                message=f"Monthly budget WARNING: {percentage_used:.1f}% used",
                current_usage=current_usage,
                budget_limit=budget_limit,
                percentage_used=percentage_used,
                recommended_action=f"Slow down to ${recommended_daily:.2f}/day to stay within budget",
                triggered_at=now
            ))
        
        # Check for trend-based alert
        forecast = await self.generate_forecast(user_id, budget_limit)
        if forecast.projected_overrun:
            alerts.append(BudgetAlert(
                alert_level=BudgetAlertLevel.WARNING,
                message=f"Projected budget overrun: ${forecast.projected_overrun:.2f}",
                current_usage=current_usage,
                budget_limit=budget_limit,
                percentage_used=percentage_used,
                recommended_action=f"Reduce daily spend to ${forecast.recommended_daily_budget:.2f}",
                triggered_at=now
            ))
        
        return alerts
    
    async def get_optimization_recommendations(
        self,
        user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get cost optimization recommendations.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Get cost history
        history = await self.get_historical_costs(user_id, days=14)
        
        if not history:
            return recommendations
        
        costs = [h["cost"] for h in history]
        avg_cost = statistics.mean(costs)
        
        # Check for high daily costs
        high_cost_days = [h for h in history if h["cost"] > avg_cost * 2]
        if len(high_cost_days) >= 2:
            recommendations.append({
                "type": "spike_reduction",
                "priority": "high",
                "message": f"Detected {len(high_cost_days)} high-cost days",
                "details": "Consider reviewing queries on high-cost days",
                "potential_savings": f"Up to ${avg_cost:.2f}/day"
            })
        
        # Check for weekend spending (potential automation/unnecessary queries)
        weekend_days = [h for h in history if h["day_of_week"] in ["Saturday", "Sunday"]]
        weekend_cost = sum(h["cost"] for h in weekend_days)
        if weekend_cost > avg_cost * 3:
            recommendations.append({
                "type": "schedule_optimization",
                "priority": "medium",
                "message": "High weekend query costs detected",
                "details": "Consider scheduling non-urgent queries for weekdays",
                "potential_savings": f"Up to ${weekend_cost * 0.5:.2f}/weekend"
            })
        
        # Check forecast for potential overrun
        forecast = await self.generate_forecast(user_id)
        if forecast.projected_overrun:
            recommendations.append({
                "type": "budget_protection",
                "priority": "critical",
                "message": f"Projected overrun of ${forecast.projected_overrun:.2f}",
                "details": f"Reduce daily spend to ${forecast.recommended_daily_budget:.2f}",
                "potential_savings": f"${forecast.projected_overrun:.2f}"
            })
        
        return recommendations
    
    def _calculate_trend(self, historical: List[Dict]) -> TrendDirection:
        """Calculate trend direction from historical data"""
        if len(historical) < 7:
            return TrendDirection.STABLE
        
        # Compare first half vs second half
        mid = len(historical) // 2
        first_half = historical[:mid]
        second_half = historical[mid:]
        
        first_avg = statistics.mean([h["cost"] for h in first_half])
        second_avg = statistics.mean([h["cost"] for h in second_half])
        
        if first_avg == 0:
            return TrendDirection.STABLE
        
        change_pct = ((second_avg - first_avg) / first_avg) * 100
        
        if change_pct > 20:
            return TrendDirection.INCREASING
        elif change_pct < -20:
            return TrendDirection.DECREASING
        else:
            return TrendDirection.STABLE
    
    async def _get_current_month_costs(self, user_id: str) -> List[float]:
        """Get daily costs for current month"""
        today = datetime.now(timezone.utc)
        days_in_month = today.day
        
        costs = []
        for i in range(days_in_month):
            date = today - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")
            
            key = f"cost:user:{user_id}:daily:{date_str}"
            try:
                cost = await redis_client._client.get(key)
                if cost:
                    costs.append(float(cost))
            except Exception:
                pass
        
        return costs
    
    def _create_default_forecast(self, budget_limit: float) -> BudgetForecast:
        """Create default forecast when no historical data exists"""
        today = datetime.now(timezone.utc)
        days_remaining = self._get_days_in_month()
        
        return BudgetForecast(
            current_period=today.strftime("%Y-%m"),
            current_usage=0.0,
            forecasted_usage=budget_limit * 0.5,  # Assume 50% usage
            budget_limit=budget_limit,
            projected_overrun=None,
            confidence_interval=(0.0, budget_limit),
            days_remaining=days_remaining,
            trend_direction=TrendDirection.STABLE,
            daily_average=0.0,
            recommended_daily_budget=budget_limit / max(1, days_remaining)
        )
    
    def _get_days_in_month(self) -> int:
        """Get number of days remaining in current month"""
        today = datetime.now(timezone.utc)
        last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        return (last_day - today).days + 1


# Global instance
cost_forecasting_service = CostForecastingService()


# Convenience functions

async def get_budget_forecast(user_id: str, budget_limit: Optional[float] = None) -> BudgetForecast:
    """Get budget forecast for a user"""
    return await cost_forecasting_service.generate_forecast(user_id, budget_limit)


async def detect_cost_anomalies(user_id: str, days: int = 30) -> List[CostAnomaly]:
    """Detect cost anomalies for a user"""
    return await cost_forecasting_service.detect_anomalies(user_id, days)


async def get_budget_alerts(user_id: str, budget_limit: Optional[float] = None) -> List[BudgetAlert]:
    """Get budget alerts for a user"""
    return await cost_forecasting_service.check_budget_alerts(user_id, budget_limit)


async def get_cost_optimization_recommendations(user_id: str) -> List[Dict[str, Any]]:
    """Get cost optimization recommendations"""
    return await cost_forecasting_service.get_optimization_recommendations(user_id)