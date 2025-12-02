"""
Orchestrator Routing Logic
"""

from typing import Literal
from app.orchestrator.state import QueryState


def route_after_understanding(state: QueryState) -> Literal["retrieve_context", "error"]:
    """Route based on intent understanding"""
    return state["next_action"]


def route_after_validation(state: QueryState) -> Literal["request_approval", "execute", "error"]:
    """Route based on validation result"""
    return state["next_action"]


def route_after_validation_with_probe(state: QueryState) -> Literal["request_approval", "probe_sql", "error"]:
    """Route validation to probe_sql for structural checks before execution.
    
    Always require approval before execution for HITL review.
    All queries must be approved by the user before execution.
    """
    nxt = state.get("next_action")
    if nxt == "error":
        return "error"
    # Force approval for ALL queries before execution
    return "request_approval"


def route_after_probe(state: QueryState) -> Literal["execute", "error"]:
    """Route probe results - repair disabled"""
    nxt = state.get("next_action")
    # Disabled: repair_sql
    # if nxt == "repair_sql":
    #     return "repair_sql"
    if nxt == "execute":
        return "execute"
    return "error"


def route_after_execute(state: QueryState) -> Literal["validate_results", "error"]:
    """Route after execution - repair/fallback disabled"""
    nxt = state.get("next_action")
    # Disabled: repair_sql and generate_fallback_sql
    # if nxt == "repair_sql":
    #     return "repair_sql"
    # if nxt == "generate_fallback_sql":
    #     return "generate_fallback_sql"
    if nxt == "format_results":  # On success, validate results first
        return "validate_results"
    return "error"


def route_after_result_validation(state: QueryState) -> Literal["format_results", "pivot_strategy"]:
    """Route after result validation"""
    nxt = state.get("next_action")
    if nxt == "pivot_strategy":
        return "pivot_strategy"
    return "format_results"


def route_after_pivot(state: QueryState) -> Literal["generate_hypothesis", "format_results"]:
    """Route after strategy pivot"""
    nxt = state.get("next_action")
    if nxt == "generate_hypothesis":
        return "generate_hypothesis"
    return "format_results"


def route_after_sql_generation(state: QueryState) -> Literal["validate", "request_clarification", "error"]:
    """Route after SQL generation"""
    return state.get("next_action", "validate")
