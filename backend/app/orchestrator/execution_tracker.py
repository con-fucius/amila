"""
Execution Step Tracker for Reasoning Display
Tracks actual execution steps for frontend visibility
Includes node-level timing for execution timeline visualization
"""

import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone


# Track node start times for duration calculation
_node_start_times: Dict[str, float] = {}


def start_node_tracking(state: Dict[str, Any], node_name: str) -> None:
    """
    Start tracking a node's execution time
    
    Args:
        state: Query state dictionary
        node_name: Name of the orchestrator node
    """
    query_id = state.get("query_id", "unknown")
    key = f"{query_id}:{node_name}"
    _node_start_times[key] = time.time()
    
    # Initialize node_history if not present
    if "node_history" not in state:
        state["node_history"] = []
    
    # Add node entry with start time
    state["node_history"].append({
        "name": node_name,
        "status": "running",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "end_time": None,
        "duration_ms": None,
        "error": None,
    })
    
    # Update current node
    state["current_node"] = node_name


def end_node_tracking(
    state: Dict[str, Any], 
    node_name: str, 
    status: str = "completed",
    error: Optional[str] = None,
    thinking_steps: Optional[List[str]] = None
) -> None:
    """
    End tracking a node's execution and record duration
    
    Args:
        state: Query state dictionary
        node_name: Name of the orchestrator node
        status: Final status (completed, error, skipped)
        error: Error message if failed
        thinking_steps: Any thinking steps from this node
    """
    query_id = state.get("query_id", "unknown")
    key = f"{query_id}:{node_name}"
    
    duration_ms = None
    if key in _node_start_times:
        duration_ms = int((time.time() - _node_start_times[key]) * 1000)
        del _node_start_times[key]
    
    # Find and update the node entry
    if "node_history" in state:
        for entry in reversed(state["node_history"]):
            if entry["name"] == node_name and entry["status"] == "running":
                entry["status"] = status
                entry["end_time"] = datetime.now(timezone.utc).isoformat()
                entry["duration_ms"] = duration_ms
                if error:
                    entry["error"] = error
                if thinking_steps:
                    entry["thinking_steps"] = thinking_steps
                break
    
    # Clear current node if it matches
    if state.get("current_node") == node_name:
        state["current_node"] = ""


def get_node_timeline(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get the execution timeline for all nodes
    
    Returns:
        List of node execution entries with timing info
    """
    return state.get("node_history", [])


def add_execution_step(
    state: Dict[str, Any],
    step_name: str,
    status: str = "completed",
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None
) -> None:
    """
    Add an execution step to the state for frontend display
    
    Args:
        state: Query state dictionary
        step_name: Name of the step (e.g., "Query Analysis", "SQL Generation")
        status: "completed", "in_progress", "failed", "skipped"
        details: Additional details about the step
        error: Error message if step failed
    """
    if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
        state["llm_metadata"] = {}
    
    if "execution_steps" not in state["llm_metadata"]:
        state["llm_metadata"]["execution_steps"] = []
    
    step = {
        "name": step_name,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_ms": None,
    }
    
    if details:
        step["details"] = details
    
    if error:
        step["error"] = error
    
    state["llm_metadata"]["execution_steps"].append(step)


def update_last_step(
    state: Dict[str, Any],
    status: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None
) -> None:
    """Update the last execution step with new information"""
    if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
        return
    
    steps = state["llm_metadata"].get("execution_steps", [])
    if not steps:
        return
    
    last_step = steps[-1]
    
    if status:
        last_step["status"] = status
    
    if details:
        if "details" not in last_step:
            last_step["details"] = {}
        last_step["details"].update(details)
    
    if error:
        last_step["error"] = error
        last_step["status"] = "failed"


def get_execution_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get a summary of execution steps for frontend display
    
    Returns:
        Dictionary with:
        - steps: List of execution steps
        - total_duration_ms: Total execution time
        - failed_steps: List of failed step names
        - sql_generated: The final SQL query
        - error_details: Detailed error information if any
    """
    if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
        return {
            "steps": [],
            "total_duration_ms": 0,
            "failed_steps": [],
            "sql_generated": state.get("sql_query"),
            "error_details": state.get("error"),
        }
    
    steps = state["llm_metadata"].get("execution_steps", [])
    start_time = state["llm_metadata"].get("start_time")
    
    total_duration = 0
    if start_time:
        total_duration = int((time.time() - start_time) * 1000)
    
    failed_steps = [step["name"] for step in steps if step.get("status") == "failed"]
    
    return {
        "steps": steps,
        "total_duration_ms": total_duration,
        "failed_steps": failed_steps,
        "sql_generated": state.get("sql_query"),
        "error_details": state.get("error"),
        "validation_result": state.get("validation_result"),
        "execution_result": state.get("execution_result"),
    }
