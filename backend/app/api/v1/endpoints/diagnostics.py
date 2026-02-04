"""
Diagnostic Endpoints for System Health and Troubleshooting

Provides deep system introspection for both developers and users:
- MCP tool availability with live probes
- Query pipeline tracing with failure points
- Database connection pool health
- LangGraph agent state inspection
- Component status aggregation
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.auth import get_current_user
from app.core.rbac import require_admin_role, require_developer_role
from app.core.client_registry import registry
from app.core.degraded_mode_manager import degraded_mode_manager
from app.services.diagnostic_service import (
    get_mcp_tool_status,
    probe_mcp_tools,
    get_query_pipeline_traces,
    get_connection_pool_health,
    get_langgraph_state_history,
    get_system_diagnostics_summary,
    get_trace_diffs
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])


# ==================== Response Models ====================

class MCPToolStatus(BaseModel):
    """MCP tool availability status"""
    server: str
    tool: str
    status: str  # GREEN, YELLOW, RED
    last_probe: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    failure_count: int = 0
    latency_ms: Optional[float] = None
    error_message: Optional[str] = None


class QueryPipelineTrace(BaseModel):
    """Query execution pipeline trace"""
    query_id: str
    user_id: str
    stage: str
    status: str
    entered_at: datetime
    exited_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    error_details: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ConnectionPoolHealth(BaseModel):
    """Database connection pool health metrics"""
    database: str
    total_connections: int
    active_connections: int
    idle_connections: int
    wait_queue_depth: int
    acquisition_latency_ms: float
    connection_churn_rate: float
    potential_leaks: List[Dict[str, Any]] = Field(default_factory=list)


class LangGraphStateSnapshot(BaseModel):
    """LangGraph agent state snapshot"""
    conversation_id: str
    node_name: str
    timestamp: datetime
    state_data: Dict[str, Any]
    decision_reason: Optional[str] = None
    tool_invocations: List[Dict[str, Any]] = Field(default_factory=list)


class SystemDiagnostics(BaseModel):
    """Comprehensive system diagnostics"""
    timestamp: datetime
    overall_status: str  # HEALTHY, DEGRADED, CRITICAL
    mcp_tools: List[MCPToolStatus]
    connection_pools: List[ConnectionPoolHealth]
    degraded_components: List[Dict[str, Any]]
    active_queries: int
    recent_failures: List[Dict[str, Any]]
    performance_metrics: Dict[str, Any]
    business_kpis: Optional[Dict[str, Any]] = None
    alerts: Optional[Dict[str, Any]] = None


class RepairTraceEntry(BaseModel):
    """Entry in a SQL repair trace"""
    type: str
    error: str
    action: str
    before_sql: Optional[str] = None
    after_sql: Optional[str] = None
    diff: Optional[str] = None
    timestamp: datetime


# ==================== Endpoints ====================

@router.get("/status", response_model=SystemDiagnostics)
async def get_system_diagnostics(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get comprehensive system diagnostics
    
    Provides a single source of truth for system health including:
    - MCP tool availability
    - Connection pool health
    - Degraded components
    - Active queries
    - Recent failures
    """
    try:
        diagnostics = await get_system_diagnostics_summary()
        return diagnostics
    except Exception as e:
        logger.error(f"Failed to get system diagnostics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mcp-tools", response_model=List[MCPToolStatus])
async def get_mcp_tools_status(
    probe: bool = Query(False, description="Run live probe on tools"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get MCP tool availability matrix
    
    Shows real-time status of all MCP tools across Oracle and Doris servers.
    Optionally runs live probes to verify tool availability.
    """
    try:
        if probe:
            await probe_mcp_tools()
        
        tool_status = await get_mcp_tool_status()
        return tool_status
    except Exception as e:
        logger.error(f"Failed to get MCP tool status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mcp-tools/probe")
async def probe_mcp_tools_endpoint(
    current_user: Dict[str, Any] = Depends(require_developer_role)
):
    """
    Manually trigger MCP tool probes
    
    Runs lightweight probes on all MCP tools to verify availability.
    Requires developer role or higher.
    """
    try:
        results = await probe_mcp_tools()
        return {
            "probed_at": datetime.now(timezone.utc).isoformat(),
            "results": results
        }
    except Exception as e:
        logger.error(f"Failed to probe MCP tools: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query-pipeline/{query_id}", response_model=List[QueryPipelineTrace])
async def get_query_pipeline_trace(
    query_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get query execution pipeline trace
    
    Shows detailed trace of query execution through all stages:
    - User Input
    - LLM SQL Generation
    - Validation
    - Human Approval
    - MCP Execution
    - Result Parsing
    
    Identifies exact failure point if query failed.
    """
    try:
        traces = await get_query_pipeline_traces(query_id)
        if not traces:
            raise HTTPException(status_code=404, detail=f"No traces found for query {query_id}")
        return traces
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get query pipeline trace: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connection-pools", response_model=List[ConnectionPoolHealth])
async def get_connection_pools_health(
    current_user: Dict[str, Any] = Depends(require_developer_role)
):
    """
    Get database connection pool health metrics
    
    Shows:
    - Total, active, and idle connections
    - Wait queue depth
    - Connection acquisition latency
    - Potential connection leaks
    
    Requires developer role or higher.
    """
    try:
        pool_health = await get_connection_pool_health()
        return pool_health
    except Exception as e:
        logger.error(f"Failed to get connection pool health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/langgraph-state/{conversation_id}", response_model=List[LangGraphStateSnapshot])
async def get_langgraph_state(
    conversation_id: str,
    current_user: Dict[str, Any] = Depends(require_developer_role)
):
    """
    Get LangGraph agent state history
    
    Shows agent decision path and state transitions for a conversation.
    Useful for debugging multi-agent orchestration.
    
    Requires developer role or higher.
    """
    try:
        state_history = await get_langgraph_state_history(conversation_id)
        if not state_history:
            raise HTTPException(
                status_code=404,
                detail=f"No state history found for conversation {conversation_id}"
            )
        return state_history
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get LangGraph state history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/degraded-mode")
async def get_degraded_mode_status(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get degraded mode status
    
    Shows which components are degraded and what features are affected.
    """
    try:
        status = degraded_mode_manager.get_system_status()
        return status
    except Exception as e:
        logger.error(f"Failed to get degraded mode status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/degraded-mode/recover")
async def attempt_recovery(
    component: Optional[str] = Query(None, description="Specific component to recover"),
    current_user: Dict[str, Any] = Depends(require_admin_role)
):
    """
    Attempt to recover degraded components
    
    Tries to reconnect/reinitialize degraded components.
    Requires admin role.
    """
    try:
        await degraded_mode_manager.attempt_recovery(component)
        status = degraded_mode_manager.get_system_status()
        return {
            "recovery_attempted": True,
            "component": component or "all",
            "current_status": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/repair-trace/{query_id}", response_model=List[RepairTraceEntry])
async def get_query_repair_trace(
    query_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get SQL repair trajectory for a specific query.
    Shows how the system attempted to fix errors, including SQL diffs.
    """
    try:
        traces = await get_trace_diffs(query_id)
        return traces
    except Exception as e:
        logger.error(f"Failed to get repair trace: {e}")
        raise HTTPException(status_code=500, detail=str(e))
