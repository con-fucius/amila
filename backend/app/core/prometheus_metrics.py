"""
Prometheus Metrics Exporter
Exposes performance metrics for Grafana dashboards
"""

import logging
from prometheus_client import Counter, Histogram, Gauge, Info, CollectorRegistry, generate_latest
from prometheus_client import CONTENT_TYPE_LATEST
from functools import wraps
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Create custom registry to avoid conflicts
registry = CollectorRegistry()


class CustomMetricsRegistry:
    """Dynamic custom metrics registry"""

    def __init__(self, registry: CollectorRegistry):
        self._registry = registry
        self._metrics: dict[str, Any] = {}

    def ensure_metric(self, definition: dict) -> None:
        metric_id = definition.get("metric_id")
        if metric_id in self._metrics:
            return
        metric_type = definition.get("type")
        name = definition.get("name")
        description = definition.get("description") or "Custom metric"
        labels = definition.get("labels") or []
        if metric_type == "gauge":
            self._metrics[metric_id] = Gauge(name, description, labels, registry=self._registry)
        elif metric_type == "counter":
            self._metrics[metric_id] = Counter(name, description, labels, registry=self._registry)
        elif metric_type == "histogram":
            self._metrics[metric_id] = Histogram(name, description, labels, registry=self._registry)

    def remove_metric(self, metric_id: str) -> None:
        if metric_id in self._metrics:
            # Prometheus client does not support removal; keep for process lifetime
            self._metrics.pop(metric_id, None)

    def record(self, definition: dict, value: float, labels: dict[str, str]) -> None:
        self.ensure_metric(definition)
        metric_id = definition.get("metric_id")
        metric = self._metrics.get(metric_id)
        if not metric:
            return
        metric_type = definition.get("type")
        if labels:
            metric = metric.labels(**labels)
        if metric_type == "gauge":
            metric.set(value)
        elif metric_type == "counter":
            metric.inc(value)
        elif metric_type == "histogram":
            metric.observe(value)


custom_metrics_registry = CustomMetricsRegistry(registry)

# ====================  QUERY METRICS ====================

# Query execution counters
query_total = Counter(
    'amil_queries_total',
    'Total number of queries processed',
    ['status', 'user_id', 'llm_provider'],
    registry=registry
)

query_errors = Counter(
    'amil_query_errors_total',
    'Total number of query errors',
    ['error_type', 'node', 'llm_provider'],
    registry=registry
)

# Query execution time histogram
query_duration = Histogram(
    'amil_query_duration_seconds',
    'Query execution duration in seconds',
    ['node', 'llm_provider'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0],
    registry=registry
)

# SQL generation time
sql_generation_duration = Histogram(
    'amil_sql_generation_duration_seconds',
    'SQL generation duration in seconds',
    ['llm_provider', 'success'],
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0],
    registry=registry
)

# Database execution time
db_execution_duration = Histogram(
    'amil_db_execution_duration_seconds',
    'Database query execution duration in seconds',
    ['status'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
    registry=registry
)

# ====================  RESULT METRICS ====================

# Result row counts
result_row_count = Histogram(
    'amil_result_row_count',
    'Number of rows returned by queries',
    ['user_id'],
    buckets=[10, 50, 100, 500, 1000, 5000, 10000, 50000],
    registry=registry
)

# Large result warnings
large_result_warnings = Counter(
    'amil_large_result_warnings_total',
    'Number of times progressive disclosure triggered',
    ['user_id'],
    registry=registry
)

# ====================  CACHE METRICS ====================

# Cache hits/misses
cache_hits = Counter(
    'amil_cache_hits_total',
    'Total number of cache hits',
    ['cache_type'],
    registry=registry
)

cache_misses = Counter(
    'amil_cache_misses_total',
    'Total number of cache misses',
    ['cache_type'],
    registry=registry
)

# Cache operation duration
cache_operation_duration = Histogram(
    'amil_cache_operation_duration_seconds',
    'Cache operation duration in seconds',
    ['operation', 'cache_type'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    registry=registry
)

# ====================  LLM METRICS ====================

# LLM token usage
llm_tokens_used = Counter(
    'amil_llm_tokens_used_total',
    'Total tokens consumed by LLM calls',
    ['provider', 'model', 'type'],  # type: prompt or completion
    registry=registry
)

# LLM API calls
llm_api_calls = Counter(
    'amil_llm_api_calls_total',
    'Total LLM API calls',
    ['provider', 'model', 'status'],
    registry=registry
)

# LLM API latency
llm_api_latency = Histogram(
    'amil_llm_api_latency_seconds',
    'LLM API call latency in seconds',
    ['provider', 'model'],
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0],
    registry=registry
)

# ====================  ORCHESTRATOR METRICS ====================

# Node execution counts
node_executions = Counter(
    'amil_node_executions_total',
    'Total executions per orchestrator node',
    ['node_name', 'status'],
    registry=registry
)

# Node execution duration
node_duration = Histogram(
    'amil_node_duration_seconds',
    'Node execution duration in seconds',
    ['node_name'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
    registry=registry
)

# Repair/fallback counts
repair_attempts = Counter(
    'amil_repair_attempts_total',
    'Total SQL repair attempts',
    ['success'],
    registry=registry
)

fallback_attempts = Counter(
    'amil_fallback_attempts_total',
    'Total fallback SQL generation attempts',
    ['success'],
    registry=registry
)

# ====================  SYSTEM METRICS ====================

# Active connections
active_queries = Gauge(
    'amil_active_queries',
    'Number of currently active queries',
    registry=registry
)

# Redis connection status
redis_connected = Gauge(
    'amil_redis_connected',
    'Redis connection status (1=connected, 0=disconnected)',
    registry=registry
)

# SQLcl pool status
sqlcl_pool_size = Gauge(
    'amil_sqlcl_pool_size',
    'Current SQLcl process pool size',
    registry=registry
)

sqlcl_pool_busy = Gauge(
    'amil_sqlcl_pool_busy',
    'Number of busy SQLcl processes',
    registry=registry
)

# System info
system_info = Info(
    'amil_system',
    'System information',
    registry=registry
)

# ====================  HELPER FUNCTIONS ====================

def track_query_execution(func):
    """Decorator to track query execution metrics"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        active_queries.inc()
        start_time = time.time()
        status = "success"
        
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            status = "error"
            error_type = type(e).__name__
            query_errors.labels(
                error_type=error_type,
                node="unknown",
                llm_provider="unknown"
            ).inc()
            raise
        finally:
            duration = time.time() - start_time
            query_duration.labels(
                node="query_orchestrator",
                llm_provider=kwargs.get("llm_provider", "unknown")
            ).observe(duration)
            
            query_total.labels(
                status=status,
                user_id=kwargs.get("user_id", "unknown"),
                llm_provider=kwargs.get("llm_provider", "unknown")
            ).inc()
            
            active_queries.dec()
    
    return wrapper


def track_node_execution(node_name: str):
    """Decorator to track individual node execution"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception:
                status = "error"
                raise
            finally:
                duration = time.time() - start_time
                node_duration.labels(node_name=node_name).observe(duration)
                node_executions.labels(node_name=node_name, status=status).inc()
        
        return wrapper
    return decorator


def record_query_result(row_count: int, user_id: str = "unknown"):
    """Record query result metrics"""
    result_row_count.labels(user_id=user_id).observe(row_count)
    
    # Trigger progressive disclosure warning
    if row_count > 1000:
        large_result_warnings.labels(user_id=user_id).inc()


def record_cache_operation(
    operation: str,
    cache_type: str,
    hit: Optional[bool] = None,
    duration: Optional[float] = None
):
    """Record cache operation metrics"""
    if hit is not None:
        if hit:
            cache_hits.labels(cache_type=cache_type).inc()
        else:
            cache_misses.labels(cache_type=cache_type).inc()
    
    if duration is not None:
        cache_operation_duration.labels(
            operation=operation,
            cache_type=cache_type
        ).observe(duration)


def record_llm_usage(
    provider: str,
    model: str,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    status: str = "success",
    latency: Optional[float] = None
):
    """Record LLM usage metrics"""
    if prompt_tokens > 0:
        llm_tokens_used.labels(
            provider=provider,
            model=model,
            type="prompt"
        ).inc(prompt_tokens)
    
    if completion_tokens > 0:
        llm_tokens_used.labels(
            provider=provider,
            model=model,
            type="completion"
        ).inc(completion_tokens)
    
    llm_api_calls.labels(
        provider=provider,
        model=model,
        status=status
    ).inc()
    
    if latency is not None:
        llm_api_latency.labels(
            provider=provider,
            model=model
        ).observe(latency)


def record_sql_generation(
    duration: float,
    llm_provider: str,
    success: bool
):
    """Record SQL generation metrics"""
    sql_generation_duration.labels(
        llm_provider=llm_provider,
        success="true" if success else "false"
    ).observe(duration)


def record_db_execution(duration: float, status: str):
    """Record database execution metrics"""
    db_execution_duration.labels(status=status).observe(duration)


def update_system_status(
    redis_status: Optional[bool] = None,
    sqlcl_pool: Optional[Dict[str, int]] = None
):
    """Update system status gauges"""
    if redis_status is not None:
        redis_connected.set(1 if redis_status else 0)
    
    if sqlcl_pool:
        sqlcl_pool_size.set(sqlcl_pool.get("total", 0))
        sqlcl_pool_busy.set(sqlcl_pool.get("busy", 0))


def set_system_info(version: str, environment: str, **kwargs):
    """Set system information"""
    info = {
        "version": version,
        "environment": environment,
        **kwargs
    }
    system_info.info(info)


def get_metrics() -> bytes:
    """Generate Prometheus metrics output"""
    return generate_latest(registry)


def get_content_type() -> str:
    """Get Prometheus content type"""
    return CONTENT_TYPE_LATEST
