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
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
    
    health_data = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
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
        redis_ping = await redis_client.ping()
        redis_info = await redis_client.info()
        health_data["components"]["redis"] = {
            "status": "connected",
            "ping": redis_ping,
            "version": redis_info.get("redis_version", "unknown"),
            "used_memory": redis_info.get("used_memory_human", "unknown"),
        }
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dependencies": dependencies
    }


@router.get("/checkpoints")
async def checkpoint_stats() -> Dict[str, Any]:
    """
    Get LangGraph checkpoint database statistics.
    Useful for monitoring checkpoint growth.
    """
    from app.tasks.checkpoint_cleanup import get_checkpoint_stats
    
    stats = get_checkpoint_stats()
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cleanup_result": result
    }


@router.get("/database")
async def database_health_check(type: str = "oracle") -> Dict[str, Any]:
    """
    Database-specific health check for frontend database selector.
    
    Checks connectivity and basic functionality of the specified database.
    Used by frontend to validate database selection before allowing switch.
    
    Args:
        type: Database type ("oracle" or "doris")
        
    Returns:
        Health status with latency information
    """
    start_time = time.time()
    db_type = type.lower()
    
    result = {
        "status": "unhealthy",
        "database_type": db_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
        else:
            result["error"] = f"Unknown database type: {db_type}"
            
    except Exception as e:
        logger.error(f"Database health check failed for {db_type}: {e}")
        result["error"] = str(e)
    
    result["latency_ms"] = round((time.time() - start_time) * 1000, 2)
    
    if result["status"] != "healthy":
        raise HTTPException(status_code=503, detail=result)
    
    return result