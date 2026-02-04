"""
Query Cost Tracker Service

Tracks and limits query execution costs per user to prevent resource abuse.

Cost tracking dimensions:
- LLM token usage (input/output tokens)
- Database execution cost (estimated from query complexity)
- Query frequency (rate limiting)
- CPU time (if available)

Enforcement:
- Per-user daily cost budgets
- Per-query cost limits
- Alerts when approaching limits
- Graceful degradation (smaller LLM context when budget low)
"""

import logging
import json
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings
from app.services.alert_service import alert_service

logger = logging.getLogger(__name__)


class CostTier(Enum):
    """Cost tier for queries"""
    FREE = "free"  # No cost (cached, simple queries)
    LOW = "low"    # < $0.01
    MEDIUM = "medium"  # $0.01 - $0.10
    HIGH = "high"  # $0.10 - $1.00
    CRITICAL = "critical"  # > $1.00


@dataclass
class QueryCost:
    """Cost breakdown for a single query"""
    llm_input_tokens: int = 0
    llm_output_tokens: int = 0
    llm_cost_usd: float = 0.0
    db_execution_ms: int = 0
    db_cost_estimate: float = 0.0
    total_cost_usd: float = 0.0
    cost_tier: str = CostTier.FREE.value
    timestamp: str = ""
    query_id: str = ""


@dataclass
class UserCostBudget:
    """User's cost budget and current usage"""
    user_id: str
    daily_budget_usd: float
    daily_usage_usd: float
    monthly_budget_usd: float
    monthly_usage_usd: float
    reset_time_utc: str
    warning_sent: bool = False


class QueryCostTracker:
    """
    Service for tracking and enforcing query cost limits.
    
    Features:
    - Track LLM token costs per user
    - Estimate database execution costs
    - Enforce daily/monthly budgets
    - Alert when approaching limits
    - Cost-aware query optimization suggestions
    """
    
    # Cost constants (adjust based on actual pricing)
    LLM_COST_PER_1K_INPUT_TOKENS = 0.0005  # $0.50 per 1M tokens (example for GPT-3.5)
    LLM_COST_PER_1K_OUTPUT_TOKENS = 0.0015  # $1.50 per 1M tokens
    
    # Database cost factors (relative scale)
    DB_COST_PER_MS = 0.00001  # Arbitrary cost factor
    
    # Budget defaults by role
    DEFAULT_BUDGETS = {
        "viewer": {"daily": 1.0, "monthly": 10.0},      # $1/day, $10/month
        "analyst": {"daily": 10.0, "monthly": 100.0},   # $10/day, $100/month
        "developer": {"daily": 50.0, "monthly": 500.0}, # $50/day, $500/month
        "admin": {"daily": 1000.0, "monthly": 10000.0}, # Admins have high limits
    }
    
    # Redis key prefixes
    COST_TRACKER_PREFIX = "cost:user:"
    COST_HISTORY_PREFIX = "cost:history:"
    BUDGET_PREFIX = "budget:user:"
    
    # TTL settings
    DAILY_TTL = 86400 * 2  # 2 days
    MONTHLY_TTL = 86400 * 62  # 62 days
    HISTORY_TTL = 86400 * 90  # 90 days
    
    @classmethod
    def calculate_llm_cost(cls, input_tokens: int, output_tokens: int) -> float:
        """
        Calculate LLM cost based on token usage.
        
        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            
        Returns:
            Cost in USD
        """
        input_cost = (input_tokens / 1000) * cls.LLM_COST_PER_1K_INPUT_TOKENS
        output_cost = (output_tokens / 1000) * cls.LLM_COST_PER_1K_OUTPUT_TOKENS
        return round(input_cost + output_cost, 6)
    
    @classmethod
    def estimate_db_cost(cls, execution_time_ms: int, rows_scanned: int = 0) -> float:
        """
        Estimate database execution cost.
        
        Args:
            execution_time_ms: Query execution time in milliseconds
            rows_scanned: Estimated rows scanned (if available)
            
        Returns:
            Estimated cost in USD
        """
        base_cost = execution_time_ms * cls.DB_COST_PER_MS
        
        # Add cost for large scans
        if rows_scanned > 10000:
            base_cost += (rows_scanned / 10000) * 0.001
        
        return round(base_cost, 6)
    
    @classmethod
    def determine_cost_tier(cls, total_cost: float) -> CostTier:
        """Determine cost tier based on total cost"""
        if total_cost == 0:
            return CostTier.FREE
        elif total_cost < 0.01:
            return CostTier.LOW
        elif total_cost < 0.10:
            return CostTier.MEDIUM
        elif total_cost < 1.00:
            return CostTier.HIGH
        else:
            return CostTier.CRITICAL
    
    @classmethod
    async def record_query_cost(
        cls,
        user_id: str,
        query_id: str,
        llm_input_tokens: int = 0,
        llm_output_tokens: int = 0,
        db_execution_ms: int = 0,
        rows_scanned: int = 0
    ) -> QueryCost:
        """
        Record the cost of a query execution.
        
        Returns:
            QueryCost object with calculated costs
        """
        # Calculate costs
        llm_cost = cls.calculate_llm_cost(llm_input_tokens, llm_output_tokens)
        db_cost = cls.estimate_db_cost(db_execution_ms, rows_scanned)
        total_cost = llm_cost + db_cost
        
        cost_tier = cls.determine_cost_tier(total_cost)
        
        query_cost = QueryCost(
            llm_input_tokens=llm_input_tokens,
            llm_output_tokens=llm_output_tokens,
            llm_cost_usd=llm_cost,
            db_execution_ms=db_execution_ms,
            db_cost_estimate=db_cost,
            total_cost_usd=total_cost,
            cost_tier=cost_tier.value,
            timestamp=datetime.now(timezone.utc).isoformat(),
            query_id=query_id
        )
        
        # Update user's daily/monthly usage
        await cls._update_user_usage(user_id, total_cost)
        
        # Store query cost history
        await cls._store_query_cost(user_id, query_id, query_cost)
        
        # Trigger alert for high-cost queries (> $1.00)
        if total_cost >= 1.0:
            await alert_service.trigger_alert(
                title="High Cost Query Detected",
                message=f"Query {query_id} executed by {user_id} cost ${total_cost:.4f}",
                level="CRITICAL",
                component="cost_tracker",
                metadata={"user_id": user_id, "query_id": query_id, "total_cost": total_cost}
            )
        elif total_cost >= 0.10:
            await alert_service.trigger_alert(
                title="Elevated Query Cost",
                message=f"Query {query_id} by {user_id} cost ${total_cost:.4f}",
                level="WARNING",
                component="cost_tracker",
                metadata={"user_id": user_id, "query_id": query_id, "total_cost": total_cost}
            )
            
        return query_cost
    
    @classmethod
    async def _update_user_usage(cls, user_id: str, cost_usd: float):
        """Update user's daily and monthly usage counters"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        
        daily_key = f"{cls.COST_TRACKER_PREFIX}{user_id}:daily:{today}"
        monthly_key = f"{cls.COST_TRACKER_PREFIX}{user_id}:monthly:{month}"
        
        try:
            # Get current values and increment
            daily_usage = await redis_client._client.incrbyfloat(daily_key, cost_usd)
            monthly_usage = await redis_client._client.incrbyfloat(monthly_key, cost_usd)
            
            # Set TTL if new keys
            await redis_client._client.expire(daily_key, cls.DAILY_TTL)
            await redis_client._client.expire(monthly_key, cls.MONTHLY_TTL)
            
        except Exception as e:
            logger.error(f"Failed to update usage for {user_id}: {e}")
    
    @classmethod
    async def _store_query_cost(cls, user_id: str, query_id: str, cost: QueryCost):
        """Store query cost in history"""
        history_key = f"{cls.COST_HISTORY_PREFIX}{user_id}:{datetime.now(timezone.utc).strftime('%Y-%m')}"
        
        try:
            cost_dict = asdict(cost)
            await redis_client._client.lpush(history_key, json.dumps(cost_dict))
            await redis_client._client.ltrim(history_key, 0, 999)  # Keep last 1000 queries
            await redis_client._client.expire(history_key, cls.HISTORY_TTL)
        except Exception as e:
            logger.error(f"Failed to store query cost history: {e}")
    
    @classmethod
    async def get_user_usage(cls, user_id: str) -> Dict[str, Any]:
        """
        Get user's current cost usage.
        
        Returns:
            Dict with daily_usage, monthly_usage, budgets, and remaining
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        
        daily_key = f"{cls.COST_TRACKER_PREFIX}{user_id}:daily:{today}"
        monthly_key = f"{cls.COST_TRACKER_PREFIX}{user_id}:monthly:{month}"
        
        try:
            daily_usage = float(await redis_client._client.get(daily_key) or 0)
            monthly_usage = float(await redis_client._client.get(monthly_key) or 0)
        except Exception as e:
            logger.error(f"Failed to get usage for {user_id}: {e}")
            daily_usage = 0.0
            monthly_usage = 0.0
        
        # Get budget
        budget = await cls.get_user_budget(user_id)
        
        return {
            "daily_usage_usd": round(daily_usage, 4),
            "daily_budget_usd": budget.daily_budget_usd,
            "daily_remaining_usd": round(budget.daily_budget_usd - daily_usage, 4),
            "daily_percent_used": round((daily_usage / budget.daily_budget_usd) * 100, 2) if budget.daily_budget_usd > 0 else 0,
            "monthly_usage_usd": round(monthly_usage, 4),
            "monthly_budget_usd": budget.monthly_budget_usd,
            "monthly_remaining_usd": round(budget.monthly_budget_usd - monthly_usage, 4),
            "monthly_percent_used": round((monthly_usage / budget.monthly_budget_usd) * 100, 2) if budget.monthly_budget_usd > 0 else 0,
            "reset_time": budget.reset_time_utc,
        }
    
    @classmethod
    async def get_user_budget(cls, user_id: str) -> UserCostBudget:
        """Get or create user's cost budget"""
        budget_key = f"{cls.BUDGET_PREFIX}{user_id}"
        
        try:
            budget_data = await redis_client.get(budget_key)
            if budget_data:
                return UserCostBudget(**budget_data)
        except Exception as e:
            logger.error(f"Failed to get budget for {user_id}: {e}")
        
        # Create default budget
        return cls._create_default_budget(user_id)
    
    @classmethod
    def _create_default_budget(cls, user_id: str) -> UserCostBudget:
        """Create default budget based on user role"""
        # Infer role from user_id (could be enhanced to query actual role)
        # Default to analyst
        role = "analyst"
        
        default_budget = cls.DEFAULT_BUDGETS.get(role, cls.DEFAULT_BUDGETS["viewer"])
        
        # Set reset time to midnight UTC
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
        reset_time = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return UserCostBudget(
            user_id=user_id,
            daily_budget_usd=default_budget["daily"],
            monthly_budget_usd=default_budget["monthly"],
            daily_usage_usd=0.0,
            monthly_usage_usd=0.0,
            reset_time_utc=reset_time.isoformat()
        )
    
    @classmethod
    async def set_user_budget(
        cls,
        user_id: str,
        daily_budget: Optional[float] = None,
        monthly_budget: Optional[float] = None
    ):
        """Set custom budget for a user"""
        budget_key = f"{cls.BUDGET_PREFIX}{user_id}"
        
        current_budget = await cls.get_user_budget(user_id)
        
        if daily_budget is not None:
            current_budget.daily_budget_usd = daily_budget
        if monthly_budget is not None:
            current_budget.monthly_budget_usd = monthly_budget
        
        try:
            await redis_client.set(budget_key, asdict(current_budget), ttl=cls.MONTHLY_TTL)
        except Exception as e:
            logger.error(f"Failed to set budget for {user_id}: {e}")
            raise
    
    @classmethod
    async def check_budget_enforcement(
        cls,
        user_id: str,
        estimated_cost: float = 0.0
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Check if user has budget to execute a query.
        
        Returns:
            Tuple of (allowed, reason, budget_info)
        """
        usage = await cls.get_user_usage(user_id)
        
        daily_remaining = usage["daily_remaining_usd"]
        monthly_remaining = usage["monthly_remaining_usd"]
        
        # Check if already exceeded limits
        if daily_remaining <= 0:
            return (
                False,
                f"Daily budget exceeded. Used: ${usage['daily_usage_usd']:.2f} / ${usage['daily_budget_usd']:.2f}",
                usage
            )
        
        if monthly_remaining <= 0:
            return (
                False,
                f"Monthly budget exceeded. Used: ${usage['monthly_usage_usd']:.2f} / ${usage['monthly_budget_usd']:.2f}",
                usage
            )
        
        # Check if estimated cost would exceed budget
        if estimated_cost > 0:
            if estimated_cost > daily_remaining:
                return (
                    False,
                    f"Query estimated cost (${estimated_cost:.4f}) exceeds daily remaining budget (${daily_remaining:.4f})",
                    usage
                )
            if estimated_cost > monthly_remaining:
                return (
                    False,
                    f"Query estimated cost (${estimated_cost:.4f}) exceeds monthly remaining budget (${monthly_remaining:.4f})",
                    usage
                )
        
        # Check warning thresholds (80% and 95%)
        warning_reason = None
        if usage["daily_percent_used"] >= 95:
            warning_reason = "WARNING: Daily budget 95% consumed"
        elif usage["daily_percent_used"] >= 80:
            warning_reason = "WARNING: Daily budget 80% consumed"
        elif usage["monthly_percent_used"] >= 95:
            warning_reason = "WARNING: Monthly budget 95% consumed"
        elif usage["monthly_percent_used"] >= 80:
            warning_reason = "WARNING: Monthly budget 80% consumed"
        
        return True, warning_reason or "Budget check passed", usage
    
    @classmethod
    async def get_cost_history(
        cls,
        user_id: str,
        limit: int = 100
    ) -> list:
        """Get user's query cost history"""
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        history_key = f"{cls.COST_HISTORY_PREFIX}{user_id}:{month}"
        
        try:
            history = await redis_client._client.lrange(history_key, 0, limit - 1)
            return [json.loads(item) for item in history]
        except Exception as e:
            logger.error(f"Failed to get cost history for {user_id}: {e}")
            return []
    
    @classmethod
    async def get_high_cost_users(cls, threshold: float = 10.0) -> list:
        """Get users with high daily costs (for monitoring)"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        pattern = f"{cls.COST_TRACKER_PREFIX}*:daily:{today}"
        
        high_cost_users = []
        
        try:
            # Scan for all daily cost keys
            keys = []
            async for key in redis_client._client.scan_iter(match=pattern):
                keys.append(key)
            
            for key in keys:
                try:
                    cost = float(await redis_client._client.get(key) or 0)
                    if cost >= threshold:
                        # Extract user_id from key
                        user_id = key.decode().split(":")[2]
                        high_cost_users.append({
                            "user_id": user_id,
                            "daily_cost": round(cost, 4)
                        })
                except Exception:
                    continue
            
        except Exception as e:
            logger.error(f"Failed to get high cost users: {e}")
        
        return sorted(high_cost_users, key=lambda x: x["daily_cost"], reverse=True)
    
    @classmethod
    async def get_total_system_cost_24h(cls) -> float:
        """Aggregate total query cost across all users for today"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        pattern = f"{cls.COST_TRACKER_PREFIX}*:daily:{today}"
        
        total_cost = 0.0
        try:
            async for key in redis_client._client.scan_iter(match=pattern):
                cost = await redis_client._client.get(key)
                if cost:
                    total_cost += float(cost)
        except Exception as e:
            logger.error(f"Failed to aggregate total system cost: {e}")
            
        return round(total_cost, 4)


# Global instance
query_cost_tracker = QueryCostTracker()


# Convenience functions

async def record_query_cost(
    user_id: str,
    query_id: str,
    llm_input_tokens: int = 0,
    llm_output_tokens: int = 0,
    db_execution_ms: int = 0
) -> QueryCost:
    """Record query cost"""
    return await QueryCostTracker.record_query_cost(
        user_id, query_id, llm_input_tokens, llm_output_tokens, db_execution_ms
    )


async def check_user_budget(user_id: str, estimated_cost: float = 0.0) -> Tuple[bool, str, Dict]:
    """Check if user has budget"""
    return await QueryCostTracker.check_budget_enforcement(user_id, estimated_cost)


async def get_user_cost_usage(user_id: str) -> Dict[str, Any]:
    """Get user's cost usage"""
    return await QueryCostTracker.get_user_usage(user_id)