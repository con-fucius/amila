"""
Execution Step Tracker for Reasoning Display
Tracks actual execution steps for frontend visibility
"""

import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone


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
