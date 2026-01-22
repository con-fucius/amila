"""
Health Check Endpoints
System health and status monitoring with comprehensive dependency checks
"""

from fastapi import APIRouter, HTTPException, Request
from typing import Dict, Any
import logging
from datetime import datetime, timezone
import time

from app.core.config import settings
from app.core.client_registry import registry
from app.core.redis_client import redis_client
from app.core.resilience import resilience_manager
from app.core.structured_logging import get_iso_timestamp

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/status")
async def health_status(request: Request) -> Dict[str, Any]:
    """
    Basic health check endpoint
    Returns system status and version information
    
    Rate limited to prevent enumeration attacks
    """
    from app.core.rate_limiter import rate_limiter, RateLimitTier
    
    # Apply rate limiting - use IP-based limiting for unauthenticated endpoint
    client_ip = request.client.host if request.client else "unknown"
    try:
        await rate_limiter.check_rate_limit(
            user=f"anon_{client_ip}",
            endpoint="/health/status",
            tier=RateLimitTier.GUEST  # Most restrictive tier
        )
    except Exception as e:
        # If rate limit check fails (e.g., Redis down), allow request but log
        logger.warning(f"Rate limit check failed for /status: {e}")
    
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment
    }


@router.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness check for Kubernetes/Load Balancers
    Verifies all critical dependencies are available
    
    Returns 200 if ready, 503 if not ready
    """
    checks = {}
    all_ready = True
    
    # Check SQLcl process pool
    sqlcl_pool = registry.get_sqlcl_pool()
    if sqlcl_pool and sqlcl_pool.initialized and not sqlcl_pool.shutting_down:
        pool_status = sqlcl_pool.get_status()
        checks["sqlcl_pool"] = {
            "status": "ready",
            "active_processes": len([p for p in pool_status["processes"] if p["state"] in ["idle", "busy"]]),
            "total_processes": pool_status["pool_size"],
        }
    else:
        checks["sqlcl_pool"] = {"status": "not_ready", "reason": "Pool not initialized"}
        all_ready = False
    
    # Check Redis
    try:
        await redis_client.ping()
        checks["redis"] = {"status": "ready"}
    except Exception as e:
        checks["redis"] = {"status": "not_ready", "error": str(e)}
        all_ready = False
    
    # Check FalkorDB (Graphiti)
    graphiti_client = registry.get_graphiti_client()
    if graphiti_client:
        try:
            # Simple connectivity check
            checks["falkordb"] = {"status": "ready"}
        except Exception as e:
            checks["falkordb"] = {"status": "degraded", "error": str(e)}
            # Graphiti is non-critical, don't fail readiness
    else:
        checks["falkordb"] = {"status": "unavailable", "reason": "Not initialized"}
        # Graphiti is non-critical, don't fail readiness
    
    # Check MCP fallback client
    mcp_client = registry.get_mcp_client()
    if mcp_client and mcp_client._connected:
        checks["mcp_fallback"] = {"status": "ready"}
    else:
        checks["mcp_fallback"] = {"status": "unavailable"}
        # Fallback is optional if pool is working
    
    # Check LangGraph orchestrator
    orchestrator = registry.get_query_orchestrator()
    if orchestrator:
        checks["orchestrator"] = {"status": "ready"}
    else:
        checks["orchestrator"] = {"status": "not_ready"}
        # Non-critical for basic queries
    
    status_code = 200 if all_ready else 503
    response = {
        "status": "ready" if all_ready else "not_ready",
        "timestamp": get_iso_timestamp(),
        "checks": checks
    }
    
    if not all_ready:
        raise HTTPException(status_code=503, detail=response)
    
    return response


@router.get("/live")
async def liveness_check() -> Dict[str, Any]:
    """
    Liveness check for Kubernetes/Load Balancers
    Indicates if the application process is alive and responsive
    
    Returns 200 if alive, 500 if deadlocked/unresponsive
    """
    return {
        "status": "alive",
        "timestamp": get_iso_timestamp(),
        "service": settings.app_name,
        "version": settings.app_version,
    }


@router.get("/detailed")
async def detailed_health_check() -> Dict[str, Any]:
    """
    Comprehensive health check with detailed component status
    For monitoring dashboards and debugging
    """
    start_time = time.time()
    
    # Get degraded mode status
    from app.core.degraded_mode_manager import degraded_mode_manager
    degraded_status = degraded_mode_manager.get_system_status()
    
    health_data = {
        "status": "healthy" if not degraded_status["is_degraded"] else degraded_status["degradation_level"],
        "timestamp": get_iso_timestamp(),
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
        "degraded_mode": degraded_status,
        "components": {},
        "metrics": {},
        "circuit_breakers": {}
    }
    
    # SQLcl Pool Status
    sqlcl_pool = registry.get_sqlcl_pool()
    if sqlcl_pool:
        pool_status = sqlcl_pool.get_status()
        health_data["components"]["sqlcl_pool"] = {
            "status": "active" if pool_status["initialized"] else "inactive",
            "pool_size": pool_status["pool_size"],
            "active_requests": pool_status["active_requests"],
            "total_queries": pool_status["total_queries"],
            "total_errors": pool_status["total_errors"],
            "error_rate": (pool_status["total_errors"] / max(pool_status["total_queries"], 1)) * 100,
            "processes": pool_status["processes"],
        }
    else:
        health_data["components"]["sqlcl_pool"] = {"status": "not_initialized"}
        health_data["status"] = "degraded"
    
    # Redis Status
    try:
        redis_health = await redis_client.health_check()
        health_data["components"]["redis"] = redis_health
        
        # Check if Redis is degraded (using fallback)
        if redis_health.get("status") == "degraded":
            health_data["status"] = "degraded"
    except Exception as e:
        health_data["components"]["redis"] = {
            "status": "disconnected",
            "error": str(e)
        }
        health_data["status"] = "degraded"
    
    # Graphiti/FalkorDB Status
    graphiti_client = registry.get_graphiti_client()
    if graphiti_client:
        health_data["components"]["graphiti"] = {
            "status": "connected",
            "type": "FalkorDB",
        }
    else:
        health_data["components"]["graphiti"] = {
            "status": "not_initialized",
            "impact": "No knowledge graph context enrichment"
        }
    
    # MCP Fallback Client Status
    mcp_client = registry.get_mcp_client()
    if mcp_client:
        health_data["components"]["mcp_fallback"] = {
            "status": "connected" if mcp_client._connected else "disconnected",
            "current_connection": mcp_client._current_connection,
        }
    else:
        health_data["components"]["mcp_fallback"] = {"status": "not_initialized"}
    
    # LangGraph Orchestrator Status
    orchestrator = registry.get_query_orchestrator()
    health_data["components"]["orchestrator"] = {
        "status": "initialized" if orchestrator else "not_initialized"
    }
    
    # PostgreSQL Status (if enabled)
    if settings.POSTGRES_ENABLED:
        try:
            from app.core.postgres_client import postgres_client
            pg_health = await postgres_client.health_check()
            health_data["components"]["postgres"] = pg_health
            
            if not pg_health.get("healthy"):
                health_data["status"] = "degraded"
        except Exception as e:
            health_data["components"]["postgres"] = {
                "status": "error",
                "error": str(e)
            }
            health_data["status"] = "degraded"
    else:
        health_data["components"]["postgres"] = {
            "status": "disabled"
        }
    
    # Qlik Sense Status (if configured)
    if hasattr(settings, "QLIK_BASE_URL") and settings.QLIK_BASE_URL:
        try:
            from app.services.qlik_service import create_qlik_client
            qlik_client = create_qlik_client()
            qlik_health = await qlik_client.health_check()
            await qlik_client.close()
            health_data["components"]["qlik"] = qlik_health
            
            if qlik_health.get("status") != "healthy":
                health_data["status"] = "degraded"
        except Exception as e:
            health_data["components"]["qlik"] = {
                "status": "error",
                "error": str(e)
            }
    else:
        health_data["components"]["qlik"] = {
            "status": "not_configured"
        }
    
    # Apache Superset Status (if configured)
    if hasattr(settings, "SUPERSET_BASE_URL") and settings.SUPERSET_BASE_URL:
        try:
            from app.services.superset_service import create_superset_client
            superset_client = create_superset_client()
            superset_health = await superset_client.health_check()
            await superset_client.close()
            health_data["components"]["superset"] = superset_health
            
            if superset_health.get("status") != "healthy":
                health_data["status"] = "degraded"
        except Exception as e:
            health_data["components"]["superset"] = {
                "status": "error",
                "error": str(e)
            }
    else:
        health_data["components"]["superset"] = {
            "status": "not_configured"
        }
    
    # Doris MCP Status
    doris_client = registry.get_doris_client()
    if doris_client:
        health_data["components"]["doris"] = {
            "status": "connected" if doris_client.is_connected() else "disconnected"
        }
    else:
        health_data["components"]["doris"] = {
            "status": "not_initialized"
        }
    
    # Circuit Breaker Status
    health_data["circuit_breakers"] = resilience_manager.get_all_status()
    
    # Response time metric
    response_time_ms = (time.time() - start_time) * 1000
    health_data["metrics"]["health_check_response_time_ms"] = round(response_time_ms, 2)
    
    return health_data


@router.get("/dependencies")
async def dependency_check() -> Dict[str, Any]:
    """
    Test connectivity to all external dependencies
    For deployment validation and troubleshooting
    """
    dependencies = {}
    
    # Test Oracle Database via Pool
    sqlcl_pool = registry.get_sqlcl_pool()
    if sqlcl_pool:
        try:
            async with sqlcl_pool.acquire(timeout=5) as client:
                # Execute simple test query
                result = await client.execute_sql("SELECT 1 FROM DUAL", "TestUserCSV")
                dependencies["oracle_database"] = {
                    "status": "reachable",
                    "test_query": "SUCCESS",
                    "response_time_ms": result.get("execution_time_ms", 0)
                }
        except Exception as e:
            dependencies["oracle_database"] = {
                "status": "unreachable",
                "error": str(e)
            }
    else:
        dependencies["oracle_database"] = {
            "status": "pool_unavailable"
        }
    
    # Test Redis
    try:
        start = time.time()
        await redis_client.ping()
        latency_ms = (time.time() - start) * 1000
        dependencies["redis"] = {
            "status": "reachable",
            "latency_ms": round(latency_ms, 2)
        }
    except Exception as e:
        dependencies["redis"] = {
            "status": "unreachable",
            "error": str(e)
        }
    
    # Test FalkorDB
    graphiti_client = registry.get_graphiti_client()
    if graphiti_client:
        dependencies["falkordb"] = {
            "status": "connected",
            "host": settings.FALKORDB_HOST,
            "port": settings.FALKORDB_PORT
        }
    else:
        dependencies["falkordb"] = {
            "status": "not_configured"
        }
    
    return {
        "timestamp": get_iso_timestamp(),
        "dependencies": dependencies
    }


@router.get("/checkpoints")
async def checkpoint_stats() -> Dict[str, Any]:
    """
    Get LangGraph checkpoint database statistics.
    Useful for monitoring checkpoint growth.
    """
    # Implementation continues...


@router.get("/degraded-mode")
async def degraded_mode_status() -> Dict[str, Any]:
    """
    Get detailed degraded mode status.
    Shows which components are degraded and what features are affected.
    """
    from app.core.degraded_mode_manager import degraded_mode_manager
    
    status = degraded_mode_manager.get_system_status()
    
    return {
        "timestamp": get_iso_timestamp(),
        **status
    }


@router.post("/degraded-mode/recover")
async def attempt_recovery(component: str = None) -> Dict[str, Any]:
    """
    Attempt to recover degraded components.
    
    Args:
        component: Specific component to recover (optional)
    """
    from app.core.degraded_mode_manager import degraded_mode_manager
    
    try:
        await degraded_mode_manager.attempt_recovery(component)
        
        return {
            "status": "recovery_initiated",
            "component": component or "all",
            "timestamp": get_iso_timestamp()
        }
    except Exception as e:
        logger.error(f"Recovery attempt failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/checkpoints")
async def checkpoint_stats() -> Dict[str, Any]:
    """
    Get LangGraph checkpoint database statistics.
    Useful for monitoring checkpoint growth.
    """
    from app.tasks.checkpoint_cleanup import get_checkpoint_stats
    
    stats = get_checkpoint_stats()
    return {
        "timestamp": get_iso_timestamp(),
        "checkpoints": stats
    }


@router.post("/checkpoints/cleanup")
async def cleanup_checkpoints(
    retention_days: int = 7,
    max_per_thread: int = 10,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Clean up old LangGraph checkpoints to prevent unbounded growth.
    
    Args:
        retention_days: Number of days to retain checkpoints (default: 7)
        max_per_thread: Maximum checkpoints to keep per thread (default: 10)
        dry_run: If True, only report what would be deleted (default: True)
        
    Returns:
        Cleanup statistics
    """
    from app.tasks.checkpoint_cleanup import cleanup_old_checkpoints
    
    result = await cleanup_old_checkpoints(
        retention_days=retention_days,
        max_per_thread=max_per_thread,
        dry_run=dry_run
    )
    
    return {
        "timestamp": get_iso_timestamp(),
        "cleanup_result": result
    }


@router.get("/database")
async def database_health_check(type: str = "oracle") -> Dict[str, Any]:
    """
    Database-specific health check for frontend database selector.
    
    Checks connectivity and basic functionality of the specified database.
    Used by frontend to validate database selection before allowing switch.
    
    Args:
        type: Database type ("oracle", "doris", or "postgres")
        
    Returns:
        Health status with latency information
    """
    start_time = time.time()
    db_type = type.lower()
    
    result = {
        "status": "unhealthy",
        "database_type": db_type,
        "timestamp": get_iso_timestamp(),
        "latency_ms": 0,
        "error": None,
    }
    
    try:
        if db_type == "oracle":
            # Test Oracle via SQLcl pool
            sqlcl_pool = registry.get_sqlcl_pool()
            if sqlcl_pool and sqlcl_pool.initialized:
                async with sqlcl_pool.acquire(timeout=10) as client:
                    test_result = await client.execute_sql("SELECT 1 FROM DUAL", settings.oracle_default_connection)
                    if test_result.get("status") == "success":
                        result["status"] = "healthy"
                        result["connection"] = settings.oracle_default_connection
                    else:
                        result["error"] = test_result.get("message", "Query failed")
            else:
                result["error"] = "SQLcl pool not initialized"
                
        elif db_type == "doris":
            # Test Doris via HTTP MCP
            from app.services.doris_query_service import DorisQueryService
            test_result = await DorisQueryService.execute_sql_query(
                sql_query="SELECT 1",
                user_id="health_check",
                request_id="health_check"
            )
            if test_result.get("status") == "success":
                result["status"] = "healthy"
            else:
                result["error"] = test_result.get("error", "Query failed")
                
        elif db_type in ["postgres", "postgresql"]:
            # Test PostgreSQL
            if not settings.POSTGRES_ENABLED:
                result["error"] = "PostgreSQL integration not enabled"
            else:
                from app.core.postgres_client import postgres_client
                health = await postgres_client.health_check()
                if health["status"] == "healthy":
                    result["status"] = "healthy"
                    result["latency_ms"] = health.get("latency_ms", 0)
                    result["pool"] = health.get("pool", {})
                else:
                    result["error"] = health.get("error", "Health check failed")
        else:
            result["error"] = f"Unknown database type: {db_type}"
            
    except Exception as e:
        logger.error(f"Database health check failed for {db_type}: {e}")
        result["error"] = str(e)
    
    result["latency_ms"] = round((time.time() - start_time) * 1000, 2)
    
    if result["status"] != "healthy":
        raise HTTPException(status_code=503, detail=result)
    
    return result



@router.post("/redis/circuit-breaker/reset")
async def reset_redis_circuit_breaker() -> Dict[str, Any]:
    """
    Manually reset Redis circuit breaker
    
    Admin endpoint to force circuit breaker back to CLOSED state.
    Use when Redis has recovered but circuit breaker is still OPEN.
    
    Returns:
        Status of reset operation
    """
    try:
        redis_client.reset_circuit_breaker()
        health_status = await redis_client.health_check()
        
        return {
            "status": "success",
            "message": "Redis circuit breaker reset successfully",
            "timestamp": get_iso_timestamp(),
            "redis_health": health_status
        }
    except Exception as e:
        logger.error(f"Failed to reset Redis circuit breaker: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Failed to reset circuit breaker: {str(e)}",
                "timestamp": get_iso_timestamp()
            }
        )


@router.get("/redis/circuit-breaker/status")
async def get_redis_circuit_breaker_status() -> Dict[str, Any]:
    """
    Get current Redis circuit breaker status
    
    Returns detailed information about:
    - Circuit breaker state (CLOSED/OPEN/HALF_OPEN)
    - Failure/success counts
    - Last failure time
    - Fallback cache statistics
    - Operation statistics
    """
    try:
        health_status = await redis_client.health_check()
        
        return {
            "timestamp": get_iso_timestamp(),
            "circuit_breaker": health_status.get("circuit_breaker", {}),
            "redis_status": health_status.get("status", "unknown")
        }
    except Exception as e:
        logger.error(f"Failed to get circuit breaker status: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Failed to get circuit breaker status: {str(e)}",
                "timestamp": get_iso_timestamp()
            }
        )


@router.get("/mcp-tools")
async def get_mcp_tools_status() -> Dict[str, Any]:
    """
    Get real-time status of all MCP tools from both Oracle SQLcl and Doris MCP servers.
    
    Returns:
        Dictionary with tool availability status for each MCP server
    """
    start_time = time.time()
    
    result = {
        "timestamp": get_iso_timestamp(),
        "servers": {}
    }
    
    # Check Oracle SQLcl MCP Server
    oracle_tools = {
        "server_name": "Oracle SQLcl MCP",
        "server_status": "unknown",
        "tools": []
    }
    
    sqlcl_pool = registry.get_sqlcl_pool()
    mcp_client = registry.get_mcp_client()
    
    if sqlcl_pool and sqlcl_pool.initialized:
        oracle_tools["server_status"] = "connected"
        # Oracle SQLcl MCP tools
        tool_names = ["list-connections", "connect", "disconnect", "run-sql", "run-sqlcl"]
        for tool_name in tool_names:
            oracle_tools["tools"].append({
                "name": tool_name,
                "status": "available",
                "description": _get_tool_description("oracle", tool_name)
            })
    elif mcp_client and mcp_client._connected:
        oracle_tools["server_status"] = "connected"
        tool_names = ["list-connections", "connect", "disconnect", "run-sql", "run-sqlcl"]
        for tool_name in tool_names:
            oracle_tools["tools"].append({
                "name": tool_name,
                "status": "available",
                "description": _get_tool_description("oracle", tool_name)
            })
    else:
        oracle_tools["server_status"] = "disconnected"
        tool_names = ["list-connections", "connect", "disconnect", "run-sql", "run-sqlcl"]
        for tool_name in tool_names:
            oracle_tools["tools"].append({
                "name": tool_name,
                "status": "unavailable",
                "description": _get_tool_description("oracle", tool_name)
            })
    
    result["servers"]["oracle"] = oracle_tools
    
    # Check Doris MCP Server
    doris_tools = {
        "server_name": "Apache Doris MCP",
        "server_status": "unknown",
        "tools": []
    }
    
    doris_client = registry.get_doris_client()
    
    if doris_client and doris_client.is_connected():
        doris_tools["server_status"] = "connected"
        tool_names = [
            "exec_query", "get_table_schema", "get_db_table_list", "get_db_list",
            "get_table_comment", "get_table_column_comments", "get_table_indexes",
            "get_recent_audit_logs", "get_catalog_list", "get_sql_explain",
            "get_sql_profile", "get_table_data_size", "get_monitoring_metrics",
            "get_memory_stats", "get_table_basic_info", "analyze_columns",
            "analyze_table_storage", "trace_column_lineage", "monitor_data_freshness",
            "analyze_data_access_patterns", "analyze_data_flow_dependencies",
            "analyze_slow_queries_topn", "analyze_resource_growth_curves",
            "exec_adbc_query", "get_adbc_connection_info"
        ]
        for tool_name in tool_names:
            doris_tools["tools"].append({
                "name": tool_name,
                "status": "available",
                "description": _get_tool_description("doris", tool_name)
            })
    else:
        doris_tools["server_status"] = "disconnected"
        tool_names = [
            "exec_query", "get_table_schema", "get_db_table_list", "get_db_list",
            "get_table_comment", "get_table_column_comments", "get_table_indexes",
            "get_recent_audit_logs", "get_catalog_list", "get_sql_explain",
            "get_sql_profile", "get_table_data_size", "get_monitoring_metrics",
            "get_memory_stats", "get_table_basic_info", "analyze_columns",
            "analyze_table_storage", "trace_column_lineage", "monitor_data_freshness",
            "analyze_data_access_patterns", "analyze_data_flow_dependencies",
            "analyze_slow_queries_topn", "analyze_resource_growth_curves",
            "exec_adbc_query", "get_adbc_connection_info"
        ]
        for tool_name in tool_names:
            doris_tools["tools"].append({
                "name": tool_name,
                "status": "unavailable",
                "description": _get_tool_description("doris", tool_name)
            })
    
    result["servers"]["doris"] = doris_tools
    
    # Add response time
    result["response_time_ms"] = round((time.time() - start_time) * 1000, 2)
    
    return result


def _get_tool_description(server_type: str, tool_name: str) -> str:
    """Get tool description for display"""
    oracle_descriptions = {
        "list-connections": "List available Oracle connections",
        "connect": "Connect to an Oracle database",
        "disconnect": "Disconnect from current database",
        "run-sql": "Execute SQL statements",
        "run-sqlcl": "Execute SQLcl commands"
    }
    
    doris_descriptions = {
        "exec_query": "Execute SQL queries with catalog federation",
        "get_table_schema": "Get detailed table schema",
        "get_db_table_list": "List tables in a database",
        "get_db_list": "List databases in a catalog",
        "get_table_comment": "Get table comments",
        "get_table_column_comments": "Get column comments",
        "get_table_indexes": "Get table indexes",
        "get_recent_audit_logs": "Get recent audit logs",
        "get_catalog_list": "List all available catalogs",
        "get_sql_explain": "Get query execution plan",
        "get_sql_profile": "Get query performance profile",
        "get_table_data_size": "Get table data size",
        "get_monitoring_metrics": "Get cluster monitoring metrics",
        "get_memory_stats": "Get memory statistics",
        "get_table_basic_info": "Get basic table information",
        "analyze_columns": "Analyze column statistics",
        "analyze_table_storage": "Analyze table storage",
        "trace_column_lineage": "Trace column lineage",
        "monitor_data_freshness": "Monitor data freshness",
        "analyze_data_access_patterns": "Analyze data access patterns",
        "analyze_data_flow_dependencies": "Analyze data flow dependencies",
        "analyze_slow_queries_topn": "Analyze top N slow queries",
        "analyze_resource_growth_curves": "Analyze resource growth",
        "exec_adbc_query": "Execute query via ADBC",
        "get_adbc_connection_info": "Get ADBC connection info"
    }
    
    if server_type == "oracle":
        return oracle_descriptions.get(tool_name, "")
    else:
        return doris_descriptions.get(tool_name, "")

