"""
Orchestrator Routing Logic

Defines routing functions for the LangGraph state machine.
Each function examines the current state and returns the next node to execute.

Security considerations:
- All queries require HITL approval before execution
- Error states are always routed to error handler
- Repair/fallback paths are enabled for resilience
"""

import logging
from typing import Literal
from app.orchestrator.state import QueryState

logger = logging.getLogger(__name__)


def route_after_understanding(state: QueryState) -> Literal["retrieve_context", "error"]:
    """Route based on intent understanding result."""
    nxt = state.get("next_action", "retrieve_context")
    if nxt == "error" or state.get("error"):
        return "error"
    return "retrieve_context"


def route_after_validation_with_probe(state: QueryState) -> Literal["await_approval", "error"]:
    """
    Route after SQL validation to approval gate.
    
    All queries MUST go through HITL approval before execution.
    This is a security requirement - no automatic execution.
    """
    nxt = state.get("next_action")
    
    # Error takes precedence
    if nxt == "error" or state.get("error"):
        return "error"
    
    # All queries require approval - route to approval gate
    # The graph will interrupt before await_approval node
    return "await_approval"


def route_after_approval(state: QueryState) -> Literal["probe_sql", "execute", "rejected", "error"]:
    """
    Route after approval decision.
    
    This runs after the graph resumes from HITL interrupt.
    The approval node sets next_action based on user decision.
    """
    nxt = state.get("next_action")
    
    # Handle rejection
    if nxt == "rejected" or (state.get("error") and "rejected" in state.get("error", "").lower()):
        return "rejected"
    
    # Handle errors
    if nxt == "error" or (state.get("error") and "rejected" not in state.get("error", "").lower()):
        return "error"
    
    # Route to probe or execute based on approval node decision
    if nxt == "execute":
        return "execute"
    
    # Default to probe_sql for structural validation
    return "probe_sql"


def route_after_probe(state: QueryState) -> Literal["execute", "repair_sql", "error"]:
    """
    Route after SQL probe validation.
    
    Probe checks structural validity without fetching data.
    On failure, routes to repair_sql for auto-correction.
    """
    nxt = state.get("next_action")
    
    # Error handling
    if state.get("error") and nxt != "repair_sql":
        return "error"
    
    # Enable repair path for probe failures
    if nxt == "repair_sql":
        repair_attempts = state.get("repair_attempts", 0)
        if repair_attempts < 2:  # Allow up to 2 repair attempts
            logger.info(f"Routing to repair_sql (attempt {repair_attempts + 1})")
            return "repair_sql"
        else:
            logger.warning(f"Max repair attempts reached, routing to error")
            return "error"
    
    if nxt == "execute":
        return "execute"
    
    # Default to execute if probe passed
    return "execute"


def route_after_execute(state: QueryState) -> Literal["validate_results", "repair_sql", "generate_fallback_sql", "error"]:
    """
    Route after SQL execution.
    
    On success: validate results
    On transient error: attempt repair
    On persistent error: try fallback SQL generation
    """
    nxt = state.get("next_action")
    error = state.get("error", "")
    
    # Success path - validate results
    if nxt == "format_results" or nxt == "validate_results":
        return "validate_results"
    
    # Check for transient errors that can be repaired
    transient_errors = [
        "ORA-00942",  # Table or view does not exist (might be schema issue)
        "ORA-00904",  # Invalid identifier
        "ORA-00936",  # Missing expression
        "ORA-00933",  # SQL command not properly ended
    ]
    
    is_transient = any(err_code in error for err_code in transient_errors)
    repair_attempts = state.get("repair_attempts", 0)
    fallback_attempts = state.get("fallback_attempts", 0)
    
    # Try repair for transient errors
    if nxt == "repair_sql" or (is_transient and repair_attempts < 2):
        if repair_attempts < 2:
            logger.info(f"Routing to repair_sql for transient error (attempt {repair_attempts + 1})")
            return "repair_sql"
    
    # Try fallback SQL generation if repair exhausted
    if nxt == "generate_fallback_sql" or (repair_attempts >= 2 and fallback_attempts < 1):
        if fallback_attempts < 1:
            logger.info(f"Routing to generate_fallback_sql (attempt {fallback_attempts + 1})")
            return "generate_fallback_sql"
    
    # All recovery options exhausted
    return "error"


def route_after_result_validation(state: QueryState) -> Literal["format_results", "pivot_strategy"]:
    """
    Route after result validation.
    
    If results are empty or suspicious, may pivot to alternative strategy.
    """
    nxt = state.get("next_action")
    
    if nxt == "pivot_strategy":
        pivot_attempts = state.get("pivot_attempts", 0)
        if pivot_attempts < 2:  # Allow up to 2 pivot attempts
            return "pivot_strategy"
    
    return "format_results"


def route_after_pivot(state: QueryState) -> Literal["generate_hypothesis", "format_results"]:
    """
    Route after strategy pivot.
    
    Either retry with new hypothesis or give up and format current results.
    """
    nxt = state.get("next_action")
    pivot_attempts = state.get("pivot_attempts", 0)
    
    if nxt == "generate_hypothesis" and pivot_attempts < 2:
        return "generate_hypothesis"
    
    return "format_results"


def route_after_sql_generation(state: QueryState) -> Literal["validate", "request_clarification", "error"]:
    """
    Route after SQL generation.
    
    May request clarification if query is ambiguous.
    """
    nxt = state.get("next_action", "validate")
    
    if nxt == "error" or state.get("error"):
        return "error"
    
    if nxt == "request_clarification":
        return "request_clarification"
    
    return "validate"
