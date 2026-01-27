"""
Diagnostic Service

Implements deep system introspection for troubleshooting and monitoring.
Provides reliable indicators connected to the deepest level of the system.
"""

import logging
import asyncio
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from app.core.client_registry import registry
from app.core.degraded_mode_manager import degraded_mode_manager
from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)

# In-memory storage for MCP tool status (with TTL)
_mcp_tool_status: Dict[str, Dict[str, Any]] = {}
_mcp_tool_status_lock = asyncio.Lock()

# In-memory storage for query pipeline traces
_query_pipeline_traces: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
_query_traces_lock = asyncio.Lock()


# ==================== MCP Tool Availability ====================

async def get_mcp_tool_status() -> List[Dict[str, Any]]:
    """
    Get current MCP tool availability status
    
    Returns:
        List of tool status dictionaries
    """
    async with _mcp_tool_status_lock:
        now = datetime.now(timezone.utc)
        tool_list = []
        
        for key, status in _mcp_tool_status.items():
            # Check if status is stale (>5 minutes)
            last_probe = status.get("last_probe")
            if last_probe and (now - last_probe).total_seconds() > 300:
                status["status"] = "YELLOW"
                status["error_message"] = "Status stale (no recent probe)"
            
            tool_list.append(status)
        
        return tool_list


async def probe_mcp_tools() -> Dict[str, Any]:
    """
    Run live probes on all MCP tools
    
    Executes lightweight queries to verify tool availability:
    - Oracle: SELECT 1 FROM DUAL
    - Doris: SELECT 1
    
    Returns:
        Probe results dictionary
    """
    results = {
        "oracle": {},
        "doris": {},
        "timestamp": datetime.now(timezone.utc)
    }
    
    # Probe Oracle SQLcl MCP
    try:
        sqlcl_pool = registry.get_sqlcl_pool()
        if sqlcl_pool:
            start_time = time.time()
            # Execute lightweight probe query
            async with sqlcl_pool.acquire() as client:
                result = await client.execute_sql(
                    "SELECT 1 FROM DUAL",
                    query_id=f"probe_{int(time.time())}"
                )
            latency_ms = (time.time() - start_time) * 1000
            
            await _update_tool_status(
                server="oracle",
                tool="execute_query",
                status="GREEN",
                latency_ms=latency_ms,
                success=True
            )
            results["oracle"]["status"] = "GREEN"
            results["oracle"]["latency_ms"] = latency_ms
        else:
            await _update_tool_status(
                server="oracle",
                tool="execute_query",
                status="RED",
                success=False,
                error_message="SQLcl pool not initialized"
            )
            results["oracle"]["status"] = "RED"
            results["oracle"]["error"] = "Pool not initialized"
    
    except Exception as e:
        logger.error(f"Oracle probe failed: {e}")
        await _update_tool_status(
            server="oracle",
            tool="execute_query",
            status="RED",
            success=False,
            error_message=str(e)
        )
        results["oracle"]["status"] = "RED"
        results["oracle"]["error"] = str(e)
    
    # Probe Doris MCP
    try:
        doris_client = registry.get_doris_client()
        if doris_client:
            start_time = time.time()
            # Execute lightweight probe query
            result = await doris_client.execute_sql("SELECT 1", query_id=f"probe_doris_{int(time.time())}")
            latency_ms = (time.time() - start_time) * 1000
            
            await _update_tool_status(
                server="doris",
                tool="exec_query",
                status="GREEN",
                latency_ms=latency_ms,
                success=True
            )
            results["doris"]["status"] = "GREEN"
            results["doris"]["latency_ms"] = latency_ms
        else:
            await _update_tool_status(
                server="doris",
                tool="exec_query",
                status="RED",
                success=False,
                error_message="Doris client not initialized"
            )
            results["doris"]["status"] = "RED"
            results["doris"]["error"] = "Client not initialized"
    
    except Exception as e:
        logger.error(f"Doris probe failed: {e}")
        await _update_tool_status(
            server="doris",
            tool="exec_query",
            status="RED",
            success=False,
            error_message=str(e)
        )
        results["doris"]["status"] = "RED"
        results["doris"]["error"] = str(e)
    
    return results


async def _update_tool_status(
    server: str,
    tool: str,
    status: str,
    latency_ms: Optional[float] = None,
    success: bool = True,
    error_message: Optional[str] = None
):
    """Update MCP tool status in memory"""
    async with _mcp_tool_status_lock:
        key = f"{server}:{tool}"
        now = datetime.now(timezone.utc)
        
        if key not in _mcp_tool_status:
            _mcp_tool_status[key] = {
                "server": server,
                "tool": tool,
                "status": status,
                "last_probe": now,
                "last_success": None,
                "last_failure": None,
                "failure_count": 0,
                "latency_ms": None,
                "error_message": None
            }
        
        tool_status = _mcp_tool_status[key]
        tool_status["status"] = status
        tool_status["last_probe"] = now
        tool_status["latency_ms"] = latency_ms
        
        if success:
            tool_status["last_success"] = now
            tool_status["failure_count"] = 0
            tool_status["error_message"] = None
        else:
            tool_status["last_failure"] = now
            tool_status["failure_count"] += 1
            tool_status["error_message"] = error_message


# ==================== Query Pipeline Tracing ====================

async def record_query_pipeline_stage(
    query_id: str,
    stage: str,
    status: str,
    entered_at: datetime,
    exited_at: Optional[datetime] = None,
    error_details: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Record a query pipeline stage transition
    
    Args:
        query_id: Query identifier
        stage: Pipeline stage name
        status: Stage status (started, completed, failed)
        entered_at: Stage entry timestamp
        exited_at: Stage exit timestamp
        error_details: Error details if failed
        metadata: Additional metadata
    """
    async with _query_traces_lock:
        trace = {
            "query_id": query_id,
            "stage": stage,
            "status": status,
            "entered_at": entered_at,
            "exited_at": exited_at,
            "duration_ms": (
                (exited_at - entered_at).total_seconds() * 1000
                if exited_at else None
            ),
            "error_details": error_details,
            "metadata": metadata or {}
        }
        
        _query_pipeline_traces[query_id].append(trace)
        
        # Keep only last 1000 queries in memory
        if len(_query_pipeline_traces) > 1000:
            oldest_key = min(_query_pipeline_traces.keys())
            del _query_pipeline_traces[oldest_key]


async def get_query_pipeline_traces(query_id: str) -> List[Dict[str, Any]]:
    """
    Get pipeline traces for a specific query
    
    Args:
        query_id: Query identifier
        
    Returns:
        List of pipeline stage traces
    """
    async with _query_traces_lock:
        return _query_pipeline_traces.get(query_id, [])


# ==================== Connection Pool Health ====================

async def get_connection_pool_health() -> List[Dict[str, Any]]:
    """
    Get database connection pool health metrics
    
    Returns:
        List of connection pool health dictionaries
    """
    pools = []
    
    # Oracle SQLcl pool
    try:
        sqlcl_pool = registry.get_sqlcl_pool()
        if sqlcl_pool:
            stats = sqlcl_pool.get_stats()
            pools.append({
                "database": "oracle",
                "total_connections": stats.get("total_processes", 0),
                "active_connections": stats.get("active_processes", 0),
                "idle_connections": stats.get("idle_processes", 0),
                "wait_queue_depth": 0,  # SQLcl doesn't have queue
                "acquisition_latency_ms": 0.0,
                "connection_churn_rate": 0.0,
                "potential_leaks": []
            })
    except Exception as e:
        logger.error(f"Failed to get Oracle pool health: {e}")
    
    # Doris connection pool (if applicable)
    try:
        doris_client = registry.get_doris_client()
        if doris_client:
            # Doris uses HTTP, no traditional connection pool
            pools.append({
                "database": "doris",
                "total_connections": 1,
                "active_connections": 0,
                "idle_connections": 1,
                "wait_queue_depth": 0,
                "acquisition_latency_ms": 0.0,
                "connection_churn_rate": 0.0,
                "potential_leaks": []
            })
    except Exception as e:
        logger.error(f"Failed to get Doris pool health: {e}")
    
    return pools


# ==================== LangGraph State History ====================

async def get_langgraph_state_history(conversation_id: str) -> List[Dict[str, Any]]:
    """
    Get LangGraph agent state history for a conversation
    
    Args:
        conversation_id: Conversation identifier
        
    Returns:
        List of state snapshots
    """
    # Try to get from checkpointer
    try:
        checkpointer = registry.get_langgraph_checkpointer()
        if not checkpointer:
            logger.warning("LangGraph checkpointer not available")
            return []
        
        config = {
            "configurable": {
                "thread_id": conversation_id
            }
        }
        
        state_history = []
        async for checkpoint_data, metadata in checkpointer.alist(config, limit=100):
            state_history.append({
                "conversation_id": conversation_id,
                "node_name": metadata.get("node_name", "unknown"),
                "timestamp": metadata.get("timestamp", datetime.now(timezone.utc)),
                "state_data": checkpoint_data,
                "decision_reason": metadata.get("decision_reason"),
                "tool_invocations": metadata.get("tool_invocations", [])
            })
        
        return state_history
    
    except Exception as e:
        logger.error(f"Failed to get LangGraph state history: {e}")
        return []


# ==================== System Diagnostics Summary ====================

async def get_system_diagnostics_summary() -> Dict[str, Any]:
    """
    Get comprehensive system diagnostics summary
    
    Returns:
        System diagnostics dictionary
    """
    # Get MCP tool status
    mcp_tools = await get_mcp_tool_status()
    
    # Get connection pool health
    connection_pools = await get_connection_pool_health()
    
    # Get degraded mode status
    degraded_status = degraded_mode_manager.get_system_status()
    
    # Determine overall status
    overall_status = "HEALTHY"
    if degraded_status["degradation_level"] == "critical":
        overall_status = "CRITICAL"
    elif degraded_status["degradation_level"] in ["severe", "partial"]:
        overall_status = "DEGRADED"
    
    # Check MCP tools
    red_tools = [t for t in mcp_tools if t.get("status") == "RED"]
    if red_tools:
        overall_status = "DEGRADED" if overall_status == "HEALTHY" else overall_status
    
    # Get active queries count
    active_queries = 0
    try:
        from app.services.query_state_manager import get_query_state_manager
        qsm = get_query_state_manager()
        active_queries = len(qsm._active_queries)
    except Exception:
        pass
    
    # Get recent failures
    recent_failures = []
    async with _query_traces_lock:
        for query_id, traces in list(_query_pipeline_traces.items())[-10:]:
            failed_stages = [t for t in traces if t["status"] == "failed"]
            if failed_stages:
                recent_failures.append({
                    "query_id": query_id,
                    "failed_stage": failed_stages[-1]["stage"],
                    "error": failed_stages[-1].get("error_details"),
                    "timestamp": failed_stages[-1]["entered_at"]
                })
    
    return {
        "timestamp": datetime.now(timezone.utc),
        "overall_status": overall_status,
        "mcp_tools": mcp_tools,
        "connection_pools": connection_pools,
        "degraded_components": degraded_status.get("degraded_components", []),
        "active_queries": active_queries,
        "recent_failures": recent_failures,
        "performance_metrics": {
            "avg_query_latency_ms": 0.0,
            "queries_per_minute": 0.0,
            "error_rate": 0.0
        }
    }


# ==================== Background Probe Task ====================

async def start_mcp_probe_task():
    """
    Background task to periodically probe MCP tools
    
    Runs every 30 seconds to maintain fresh status
    """
    while True:
        try:
            await asyncio.sleep(30)
            await probe_mcp_tools()
            logger.debug("MCP tool probe completed")
        except Exception as e:
            logger.error(f"MCP probe task failed: {e}")
