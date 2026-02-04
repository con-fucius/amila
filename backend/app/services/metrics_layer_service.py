"""
Metrics Layer Service
Defines canonical business metrics that map to SQL expressions.

Provides:
- Centralized metric definitions with versioning
- SQL template generation for consistent calculations
- Governance controls (approval workflows, ownership)
- Cross-domain semantic consistency
"""

import logging
import yaml
import json
import re
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class MetricStatus(str, Enum):
    """Metric definition status"""
    DRAFT = "draft"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class MetricType(str, Enum):
    """Types of metrics"""
    COUNT = "count"  # Simple counts
    SUM = "sum"  # Summations
    AVERAGE = "average"  # Averages
    RATIO = "ratio"  # Ratios/percentages
    DISTINCT_COUNT = "distinct_count"  # Unique counts
    TIME_SERIES = "time_series"  # Time-based aggregations
    COMPOSITE = "composite"  # Derived from other metrics


@dataclass
class MetricDimension:
    """A dimension along which a metric can be analyzed"""
    name: str
    description: str
    column_mapping: Dict[str, str]  # table -> column
    required: bool = False


@dataclass
class MetricDefinition:
    """Canonical definition of a business metric"""
    metric_id: str
    name: str
    display_name: str
    description: str
    metric_type: MetricType
    
    # SQL template with placeholders
    sql_template: str
    sql_template_doris: Optional[str] = None  # Doris-specific variant
    sql_template_oracle: Optional[str] = None  # Oracle-specific variant
    
    # Dimensions this metric supports
    dimensions: List[MetricDimension] = field(default_factory=list)
    
    # Governance
    owner: str = ""  # Team/person responsible
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    status: MetricStatus = MetricStatus.DRAFT
    version: str = "1.0"
    
    # Metadata
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tags: List[str] = field(default_factory=list)
    
    # Usage tracking
    usage_count: int = 0
    last_used_at: Optional[str] = None
    
    # Documentation
    business_definition: str = ""  # Business-friendly definition
    calculation_logic: str = ""  # How it's calculated
    example_queries: List[str] = field(default_factory=list)
    
    # Validation
    validation_rules: Dict[str, Any] = field(default_factory=dict)


class MetricsLayerService:
    """
    Service for managing canonical business metrics.
    
    Features:
    - Centralized metric definitions with versioning
    - SQL template generation for different dialects
    - Governance workflows (approval, ownership)
    - Semantic validation and consistency checks
    - Usage tracking for metric popularity
    """
    
    METRICS_KEY_PREFIX = "metrics:definition:"
    METRICS_INDEX_KEY = "metrics:index"
    
    def __init__(self):
        self._metrics_cache: Dict[str, MetricDefinition] = {}
        self._default_metrics_file = Path(__file__).parent.parent / "skills" / "metrics_layer.yaml"
    
    async def initialize_default_metrics(self):
        """Load default metrics from YAML file or create built-ins"""
        try:
            if self._default_metrics_file.exists():
                with open(self._default_metrics_file, 'r') as f:
                    data = yaml.safe_load(f)
                    for metric_data in data.get('metrics', []):
                        await self.register_metric(MetricDefinition(**metric_data))
            else:
                # Create built-in metrics
                await self._create_builtin_metrics()
        except Exception as e:
            logger.error(f"Failed to initialize default metrics: {e}")
            await self._create_builtin_metrics()
    
    async def _create_builtin_metrics(self):
        """Create built-in example metrics"""
        builtins = [
            MetricDefinition(
                metric_id="active_users",
                name="active_users",
                display_name="Active Users",
                description="Count of distinct users with activity in the last 30 days",
                metric_type=MetricType.DISTINCT_COUNT,
                sql_template="COUNT(DISTINCT user_id) FILTER (WHERE last_active >= CURRENT_DATE - INTERVAL '30 days')",
                sql_template_doris="COUNT(DISTINCT CASE WHEN last_active >= DATE_SUB(CURDATE(), 30) THEN user_id END)",
                sql_template_oracle="COUNT(DISTINCT CASE WHEN last_active >= SYSDATE - 30 THEN user_id END)",
                dimensions=[
                    MetricDimension("date", "Activity date", {"users": "activity_date"}),
                    MetricDimension("region", "User region", {"users": "region"}),
                ],
                owner="product_team",
                status=MetricStatus.APPROVED,
                business_definition="Users who have performed any action in the last 30 days",
                calculation_logic="Count unique user_ids where last_active >= today - 30 days",
                tags=["user", "engagement", "standard"]
            ),
            MetricDefinition(
                metric_id="monthly_recurring_revenue",
                name="monthly_recurring_revenue",
                display_name="Monthly Recurring Revenue (MRR)",
                description="Sum of all active subscription values",
                metric_type=MetricType.SUM,
                sql_template="SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END)",
                dimensions=[
                    MetricDimension("date", "Subscription date", {"subscriptions": "start_date"}),
                    MetricDimension("plan_type", "Subscription plan", {"subscriptions": "plan_type"}),
                    MetricDimension("region", "Customer region", {"subscriptions": "region"}),
                ],
                owner="finance_team",
                status=MetricStatus.APPROVED,
                business_definition="Predictable monthly revenue from active subscriptions",
                calculation_logic="Sum of monthly amounts for all subscriptions with status='active'",
                tags=["revenue", "finance", "saas", "standard"]
            ),
            MetricDefinition(
                metric_id="average_revenue_per_user",
                name="average_revenue_per_user",
                display_name="Average Revenue Per User (ARPU)",
                description="Average revenue generated per user",
                metric_type=MetricType.RATIO,
                sql_template="SUM(revenue) / NULLIF(COUNT(DISTINCT user_id), 0)",
                dimensions=[
                    MetricDimension("date", "Transaction date", {"transactions": "transaction_date"}),
                    MetricDimension("region", "User region", {"users": "region"}),
                ],
                owner="finance_team",
                status=MetricStatus.APPROVED,
                business_definition="Total revenue divided by number of unique users",
                calculation_logic="SUM of all revenue / COUNT DISTINCT users",
                tags=["revenue", "finance", "kpi", "standard"]
            ),
            MetricDefinition(
                metric_id="customer_lifetime_value",
                name="customer_lifetime_value",
                display_name="Customer Lifetime Value (CLV)",
                description="Predicted total revenue from a customer relationship",
                metric_type=MetricType.COMPOSITE,
                sql_template="(SUM(revenue) / COUNT(DISTINCT customer_id)) * (AVG(customer_lifespan_months))",
                dimensions=[
                    MetricDimension("acquisition_date", "Customer acquisition date", {"customers": "acquisition_date"}),
                    MetricDimension("cohort", "Acquisition cohort", {"customers": "cohort_month"}),
                ],
                owner="analytics_team",
                status=MetricStatus.APPROVED,
                business_definition="Predicted revenue a customer will generate over their lifetime",
                calculation_logic="ARPU × Average customer lifespan in months",
                tags=["revenue", "analytics", "prediction", "standard"]
            ),
            MetricDefinition(
                metric_id="churn_rate",
                name="churn_rate",
                display_name="Customer Churn Rate",
                description="Percentage of customers who stopped using the service",
                metric_type=MetricType.RATIO,
                sql_template="COUNT(DISTINCT CASE WHEN status = 'churned' THEN customer_id END) * 100.0 / NULLIF(COUNT(DISTINCT customer_id), 0)",
                dimensions=[
                    MetricDimension("date", "Churn date", {"customers": "churn_date"}),
                    MetricDimension("plan_type", "Subscription plan", {"customers": "plan_type"}),
                ],
                owner="product_team",
                status=MetricStatus.APPROVED,
                business_definition="Percentage of customers lost in a given period",
                calculation_logic="(Churned customers / Total customers) × 100",
                tags=["retention", "product", "kpi", "standard"]
            ),
            MetricDefinition(
                metric_id="daily_active_users",
                name="daily_active_users",
                display_name="Daily Active Users (DAU)",
                description="Count of unique users active on a given day",
                metric_type=MetricType.DISTINCT_COUNT,
                sql_template="COUNT(DISTINCT user_id) FILTER (WHERE activity_date = CURRENT_DATE)",
                sql_template_doris="COUNT(DISTINCT CASE WHEN activity_date = CURDATE() THEN user_id END)",
                sql_template_oracle="COUNT(DISTINCT CASE WHEN TRUNC(activity_date) = TRUNC(SYSDATE) THEN user_id END)",
                dimensions=[
                    MetricDimension("date", "Activity date", {"activity": "activity_date"}),
                    MetricDimension("platform", "User platform", {"activity": "platform"}),
                ],
                owner="product_team",
                status=MetricStatus.APPROVED,
                business_definition="Unique users who performed an action today",
                calculation_logic="Count distinct user_ids where activity_date = today",
                tags=["user", "engagement", "daily", "standard"]
            ),
        ]
        
        for metric in builtins:
            await self.register_metric(metric)
        
        logger.info(f"Created {len(builtins)} built-in metrics")
    
    async def register_metric(self, metric: MetricDefinition) -> bool:
        """
        Register a new metric definition
        
        Args:
            metric: Metric definition to register
            
        Returns:
            True if successful
        """
        try:
            # Validate metric
            if not self._validate_metric(metric):
                return False
            
            # Store in Redis
            key = f"{self.METRICS_KEY_PREFIX}{metric.metric_id}"
            await redis_client.set(key, asdict(metric))
            
            # Add to index
            await redis_client._client.sadd(self.METRICS_INDEX_KEY, metric.metric_id)
            
            # Update cache
            self._metrics_cache[metric.metric_id] = metric
            
            logger.info(f"Registered metric: {metric.metric_id} (v{metric.version})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register metric {metric.metric_id}: {e}")
            return False
    
    def _validate_metric(self, metric: MetricDefinition) -> bool:
        """Validate metric definition"""
        # Check required fields
        if not metric.metric_id or not metric.sql_template:
            logger.error(f"Metric {metric.metric_id} missing required fields")
            return False
        
        # Validate SQL template has no dangerous operations
        dangerous_patterns = ['DELETE', 'DROP', 'TRUNCATE', 'UPDATE', 'INSERT']
        upper_sql = metric.sql_template.upper()
        for pattern in dangerous_patterns:
            if pattern in upper_sql:
                logger.error(f"Metric {metric.metric_id} contains dangerous SQL: {pattern}")
                return False
        
        return True
    
    async def get_metric(self, metric_id: str) -> Optional[MetricDefinition]:
        """Get a metric definition by ID"""
        # Check cache first
        if metric_id in self._metrics_cache:
            return self._metrics_cache[metric_id]
        
        # Fetch from Redis
        key = f"{self.METRICS_KEY_PREFIX}{metric_id}"
        data = await redis_client.get(key)
        
        if not data:
            return None
        
        metric = MetricDefinition(**data)
        self._metrics_cache[metric_id] = metric
        return metric
    
    async def get_all_metrics(
        self,
        status: Optional[MetricStatus] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None
    ) -> List[MetricDefinition]:
        """
        Get all metrics with optional filtering
        
        Args:
            status: Filter by status
            tags: Filter by tags (all must match)
            owner: Filter by owner
            
        Returns:
            List of matching metrics
        """
        # Get all metric IDs
        metric_ids = await redis_client._client.smembers(self.METRICS_INDEX_KEY)
        
        metrics = []
        for mid in metric_ids:
            mid_str = mid.decode() if isinstance(mid, bytes) else mid
            metric = await self.get_metric(mid_str)
            if not metric:
                continue
            
            # Apply filters
            if status and metric.status != status:
                continue
            if owner and metric.owner != owner:
                continue
            if tags and not all(tag in metric.tags for tag in tags):
                continue
            
            metrics.append(metric)
        
        # Sort by name
        metrics.sort(key=lambda x: x.display_name)
        return metrics
    
    async def generate_sql(
        self,
        metric_id: str,
        table_alias: str = "t",
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        database_type: str = "postgres"
    ) -> Optional[str]:
        """
        Generate SQL expression for a metric
        
        Args:
            metric_id: Metric to generate SQL for
            table_alias: Table alias to use
            dimensions: Dimensions to group by
            filters: Optional filter conditions
            database_type: Target database type
            
        Returns:
            SQL expression string
        """
        metric = await self.get_metric(metric_id)
        if not metric:
            return None
        
        # Select appropriate SQL template
        if database_type == "doris" and metric.sql_template_doris:
            sql = metric.sql_template_doris
        elif database_type in ["oracle"] and metric.sql_template_oracle:
            sql = metric.sql_template_oracle
        else:
            sql = metric.sql_template
        
        # Replace table references with alias
        sql = self._apply_table_alias(sql, table_alias)
        
        # Add dimension grouping if specified
        if dimensions:
            dim_sql = self._generate_dimension_sql(metric, dimensions, table_alias)
            if dim_sql:
                sql = f"{sql}, {dim_sql}"
        
        # Add filters if specified
        if filters:
            filter_sql = self._generate_filter_sql(filters, table_alias)
            if filter_sql:
                sql = f"{sql} FILTER (WHERE {filter_sql})"
        
        # Track usage
        await self._track_metric_usage(metric_id)
        
        return sql
    
    def _apply_table_alias(self, sql: str, alias: str) -> str:
        """Apply table alias to SQL template"""
        # Replace unqualified column references
        # This is a simple implementation - production would need proper SQL parsing
        pattern = r'(?<![.\w])(\w+_id|\w+_date|amount|revenue|status)(?![\w])'
        return re.sub(pattern, rf"{alias}.\1", sql)
    
    def _generate_dimension_sql(
        self,
        metric: MetricDefinition,
        dimensions: List[str],
        table_alias: str
    ) -> str:
        """Generate SQL for dimension columns"""
        dim_cols = []
        for dim_name in dimensions:
            for dim in metric.dimensions:
                if dim.name == dim_name:
                    # Find column mapping
                    for table, column in dim.column_mapping.items():
                        dim_cols.append(f"{table_alias}.{column} AS {dim_name}")
                        break
        return ", ".join(dim_cols)
    
    def _generate_filter_sql(self, filters: Dict[str, Any], table_alias: str) -> str:
        """Generate SQL filter conditions"""
        conditions = []
        for column, value in filters.items():
            if isinstance(value, list):
                values = ", ".join(f"'{v}'" for v in value)
                conditions.append(f"{table_alias}.{column} IN ({values})")
            elif isinstance(value, str):
                conditions.append(f"{table_alias}.{column} = '{value}'")
            else:
                conditions.append(f"{table_alias}.{column} = {value}")
        return " AND ".join(conditions)
    
    async def _track_metric_usage(self, metric_id: str):
        """Track that a metric was used"""
        try:
            metric = await self.get_metric(metric_id)
            if metric:
                metric.usage_count += 1
                metric.last_used_at = datetime.now(timezone.utc).isoformat()
                await self.register_metric(metric)
        except Exception as e:
            logger.warning(f"Failed to track metric usage: {e}")
    
    async def approve_metric(
        self,
        metric_id: str,
        approved_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """
        Approve a metric definition (governance workflow)
        
        Args:
            metric_id: Metric to approve
            approved_by: Person approving
            notes: Approval notes
            
        Returns:
            True if approved
        """
        metric = await self.get_metric(metric_id)
        if not metric:
            return False
        
        if metric.status != MetricStatus.PENDING_APPROVAL:
            logger.warning(f"Metric {metric_id} not in pending state")
            return False
        
        metric.status = MetricStatus.APPROVED
        metric.approved_by = approved_by
        metric.approved_at = datetime.now(timezone.utc).isoformat()
        metric.updated_at = datetime.now(timezone.utc).isoformat()
        
        if notes:
            metric.metadata = metric.metadata or {}
            metric.metadata['approval_notes'] = notes
        
        return await self.register_metric(metric)
    
    async def get_metric_suggestions(
        self,
        query: str,
        limit: int = 5
    ) -> List[Tuple[MetricDefinition, float]]:
        """
        Suggest relevant metrics based on query text
        
        Args:
            query: Natural language query
            limit: Max suggestions
            
        Returns:
            List of (metric, relevance_score) tuples
        """
        query_lower = query.lower()
        words = set(query_lower.split())
        
        all_metrics = await self.get_all_metrics(status=MetricStatus.APPROVED)
        
        scored_metrics = []
        for metric in all_metrics:
            score = 0.0
            
            # Check name match
            metric_words = set(metric.name.lower().split('_'))
            metric_words.update(metric.display_name.lower().split())
            
            # Word overlap
            overlap = words & metric_words
            score += len(overlap) * 0.3
            
            # Tag match
            for tag in metric.tags:
                if tag.lower() in query_lower:
                    score += 0.2
            
            # Description match
            desc_words = set(metric.description.lower().split())
            desc_overlap = words & desc_words
            score += len(desc_overlap) * 0.15
            
            # Boost by usage (popular metrics)
            score += min(metric.usage_count / 100, 0.2)
            
            if score > 0.1:  # Minimum threshold
                scored_metrics.append((metric, score))
        
        # Sort by score
        scored_metrics.sort(key=lambda x: x[1], reverse=True)
        return scored_metrics[:limit]
    
    async def validate_metric_consistency(
        self,
        metric_id: str,
        sql_query: str
    ) -> Dict[str, Any]:
        """
        Validate that a SQL query correctly implements a metric
        
        Args:
            metric_id: Metric to check against
            sql_query: SQL query to validate
            
        Returns:
            Validation result with issues and score
        """
        metric = await self.get_metric(metric_id)
        if not metric:
            return {"valid": False, "error": "Metric not found"}
        
        issues = []
        score = 1.0
        
        # Check for required aggregations
        sql_upper = sql_query.upper()
        metric_upper = metric.sql_template.upper()
        
        # Extract aggregation functions
        agg_pattern = r'(COUNT|SUM|AVG|MIN|MAX)\s*\('
        sql_aggs = set(re.findall(agg_pattern, sql_upper))
        metric_aggs = set(re.findall(agg_pattern, metric_upper))
        
        if not sql_aggs.intersection(metric_aggs):
            issues.append(f"Expected aggregation {metric_aggs} not found")
            score -= 0.3
        
        # Check for correct columns
        for dim in metric.dimensions:
            for col in dim.column_mapping.values():
                if col.upper() not in sql_upper:
                    issues.append(f"Dimension column '{col}' not used")
                    score -= 0.1
        
        return {
            "valid": score >= 0.7,
            "score": max(score, 0),
            "issues": issues,
            "metric_id": metric_id
        }
    
    async def export_metrics_yaml(self) -> str:
        """Export all metrics to YAML format"""
        metrics = await self.get_all_metrics()
        
        data = {
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "metrics": [asdict(m) for m in metrics]
        }
        
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    async def get_metrics_glossary(self) -> List[Dict[str, Any]]:
        """
        Get a business-friendly list of all approved metrics for the glossary.
        """
        metrics = await self.get_all_metrics(status=MetricStatus.APPROVED)
        return [
            {
                "metric_id": m.metric_id,
                "name": m.display_name,
                "description": m.description,
                "owner": m.owner,
                "tags": m.tags,
                "business_definition": m.business_definition,
                "calculation_logic": m.calculation_logic,
                "usage_count": m.usage_count
            }
            for m in metrics
        ]
    
    async def import_metrics_yaml(self, yaml_content: str) -> Tuple[int, int]:
        """
        Import metrics from YAML
        
        Returns:
            (success_count, error_count)
        """
        try:
            data = yaml.safe_load(yaml_content)
            metrics = data.get('metrics', [])
            
            success = 0
            errors = 0
            
            for metric_data in metrics:
                try:
                    # Convert string enum values
                    metric_data['metric_type'] = MetricType(metric_data['metric_type'])
                    metric_data['status'] = MetricStatus(metric_data.get('status', 'draft'))
                    
                    # Convert dimensions
                    if 'dimensions' in metric_data:
                        metric_data['dimensions'] = [
                            MetricDimension(**d) for d in metric_data['dimensions']
                        ]
                    
                    metric = MetricDefinition(**metric_data)
                    if await self.register_metric(metric):
                        success += 1
                    else:
                        errors += 1
                except Exception as e:
                    logger.error(f"Failed to import metric: {e}")
                    errors += 1
            
            return success, errors
            
        except Exception as e:
            logger.error(f"Failed to parse metrics YAML: {e}")
            return 0, 1


# Global instance
metrics_layer_service = MetricsLayerService()


# Convenience functions

async def get_metric_sql(
    metric_name: str,
    database_type: str = "postgres",
    **kwargs
) -> Optional[str]:
    """
    Convenience function to get SQL for a metric
    
    Usage:
        sql = await get_metric_sql("monthly_recurring_revenue", "doris")
    """
    return await metrics_layer_service.generate_sql(
        metric_name,
        database_type=database_type,
        **kwargs
    )


async def suggest_metrics_for_query(query: str) -> List[Dict[str, Any]]:
    """
    Suggest metrics for a natural language query
    
    Returns:
        List of metric suggestions with scores
    """
    suggestions = await metrics_layer_service.get_metric_suggestions(query)
    return [
        {
            "metric_id": m.metric_id,
            "display_name": m.display_name,
            "description": m.description,
            "relevance_score": score,
            "sql_template": m.sql_template[:100] + "..."
        }
        for m, score in suggestions
    ]
