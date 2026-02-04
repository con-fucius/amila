"""
Shared utilities for orchestrator nodes
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Query lifecycle streaming (SSE)
try:
    from app.services.query_state_manager import get_query_state_manager as _get_qs_manager, QueryState as ExecState
except Exception:
    _get_qs_manager = None
    ExecState = None  # type: ignore


async def emit_state_event(q_state: dict, new_state, metadata: dict | None = None):
    """Safely emit lifecycle state to QueryStateManager (best-effort)."""
    try:
        if _get_qs_manager and ExecState and q_state.get("query_id"):
            manager = await _get_qs_manager()
            meta = metadata or {}
            # Ensure trace correlation
            if q_state.get("trace_id") and "trace_id" not in meta:
                meta["trace_id"] = q_state["trace_id"]
            if "stage" not in meta:
                stage_hint = q_state.get("current_stage") or q_state.get("error_stage")
                if stage_hint:
                    meta["stage"] = stage_hint
            # Include database_type for frontend error handling differentiation
            if q_state.get("database_type") and "database_type" not in meta:
                meta["database_type"] = q_state["database_type"]
            await manager.update_state(q_state["query_id"], new_state, meta)
    except Exception:
        # Non-fatal; logging only
        logger.debug("State emit skipped (manager unavailable)")


async def update_node_history(
    state: dict,
    node_name: str,
    status: str,  # 'pending', 'in-progress', 'completed', 'failed'
    thinking_steps: list | None = None,
    error: str | None = None
) -> list:
    """
    Update node history in state and emit SSE event.
    Returns the updated node_history list.
    """
    history = state.get("node_history", []) or []
    if not isinstance(history, list):
        history = []
    
    # Check if node already exists in history
    existing_idx = next((i for i, n in enumerate(history) if n["name"] == node_name), -1)
    
    now = datetime.now(timezone.utc).isoformat()
    
    node_data = {
        "name": node_name,
        "status": status,
        "thinking_steps": thinking_steps or [],
        "error": error
    }
    
    if existing_idx >= 0:
        # Update existing
        current = history[existing_idx]
        node_data["start_time"] = current.get("start_time")
        if status == "in-progress" and not node_data["start_time"]:
            node_data["start_time"] = now
        if status in ["completed", "failed"]:
            node_data["end_time"] = now
        
        # Merge thinking steps if not provided
        if not thinking_steps and current.get("thinking_steps"):
            node_data["thinking_steps"] = current["thinking_steps"]
            
        history[existing_idx] = node_data
    else:
        # Add new
        if status == "in-progress":
            node_data["start_time"] = now
        history.append(node_data)
    
    state["node_history"] = history
    state["current_node"] = node_name
    
    # Emit SSE event with full history so frontend can render the panel
    if ExecState:
        # Determine overall state based on node status
        sse_state = ExecState.EXECUTING
        if status == "failed":
            sse_state = ExecState.ERROR
        elif node_name == "understand":
            sse_state = ExecState.PLANNING
            
        await emit_state_event(state, sse_state, {
            "node_history": history,
            "current_node": node_name,
            "thinking_steps": thinking_steps # Keep legacy for compatibility
        })
        
    return history


async def set_state_error(state: dict, stage: str, message: str, details: dict | None = None) -> None:
    """Populate state with structured error information for orchestration and SSE."""

    state["error"] = message
    state["error_stage"] = stage

    payload = {"stage": stage, "message": message}
    if details:
        payload["details"] = details

    state["error_payload"] = payload
    
    # Update node history for the failed stage
    await update_node_history(state, stage, "failed", error=message)
    
    # Emit SSE error event (redundant but safe)
    await emit_state_event(state, ExecState.ERROR if ExecState else None, {"error": message, "stage": stage})


# Prometheus metrics tracking
try:
    from app.core.prometheus_metrics import (
        track_node_execution,
        record_llm_usage,
        record_sql_generation,
        record_db_execution,
        record_query_result,
        repair_attempts,
        fallback_attempts
    )
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    track_node_execution = lambda name: lambda f: f  # No-op decorator
    record_llm_usage = lambda *args, **kwargs: None
    record_sql_generation = lambda *args, **kwargs: None
    record_db_execution = lambda *args, **kwargs: None
    record_query_result = lambda *args, **kwargs: None
    repair_attempts = None
    fallback_attempts = None
    logger.warning(f"Prometheus metrics not available")


@asynccontextmanager
async def langfuse_span(
    state: dict,
    span_name: str,
    input_data: dict | None = None,
    metadata: dict | None = None,
):
    """Yield a span data dict if Langfuse tracing is active; otherwise a no-op dict."""

    trace_id = state.get("trace_id")
    default_payload = {
        "output": {},
        "metadata": dict(metadata or {}),
        "level": "DEFAULT",
    }

    if not trace_id:
        yield default_payload
        return

    try:
        from app.core.langfuse_client import trace_span as _trace_span
    except Exception:
        yield default_payload
        return

    async with _trace_span(trace_id, span_name, input_data=input_data, metadata=metadata) as span_data:
        # Ensure expected keys exist and metadata includes caller-provided values
        span_data.setdefault("metadata", {})
        if metadata:
            span_data["metadata"].update(metadata)
        span_data.setdefault("output", {})
        span_data.setdefault("level", "DEFAULT")
        yield span_data
