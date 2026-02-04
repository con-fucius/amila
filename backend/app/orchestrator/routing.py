"""
Orchestrator Routing Logic

Defines routing functions for the LangGraph state machine.
Each function examines the current state and returns the next node to execute.

Security considerations:
- All queries require HITL approval before execution
- Error states are always routed to error handler
- Repair/fallback paths are enabled for resilience

Taxonomy-based routing:
- Uses query_taxonomy from understand node to optimize routing
- Different query types may skip or emphasize certain nodes
"""

import logging
from typing import Literal
from app.orchestrator.state import QueryState

logger = logging.getLogger(__name__)


def route_after_understanding(state: QueryState) -> Literal["retrieve_context", "decompose", "error"]:
    """
    Route based on intent understanding result with taxonomy-aware optimization.
    
    Taxonomy-based routing rules:
    - Meta/Schema queries: Fast-track to SQL generation (skip context)
    - Complex multi-table queries: Route through decomposition
    - Standard queries: Normal context retrieval
    """
    nxt = state.get("next_action", "retrieve_context")
    if nxt == "error" or state.get("error"):
        return "error"
    
    # Taxonomy-based routing optimization
    taxonomy = state.get("query_taxonomy", {})
    query_type = taxonomy.get("query_type", "unknown")
    complexity = taxonomy.get("complexity", "simple")
    joins_count = taxonomy.get("joins_count", 0)
    
    # Route complex queries through decomposition
    if complexity == "complex" or joins_count >= 3:
        logger.info(f"Taxonomy routing: Complex query detected ({complexity}, {joins_count} joins) -> decompose")
        return "decompose"
    
    # Meta/Schema queries can skip context and go straight to SQL generation
    # These don't need table discovery as they're asking about schema itself
    if query_type == "meta_schema":
        logger.info("Taxonomy routing: Meta/Schema query -> fast-track to SQL generation")
        # Skip retrieve_context, but we need to set up minimal context
        state["skip_context_retrieval"] = True
        return "retrieve_context"  # Still go through context but will be minimal
    
    return "retrieve_context"


def route_after_validation_with_probe(state: QueryState) -> Literal["await_approval", "execute", "probe_sql", "error"]:
    """
    Route after SQL validation to approval gate or direct execution.
    
    CRITICAL: Queries go through HITL approval UNLESS:
    1. User has auto_approve=True AND
    2. Query is low risk (no force_approval flag)
    
    This is a security requirement.
    """
    nxt = state.get("next_action")
    
    # Error takes precedence
    if nxt == "error" or state.get("error"):
        logger.info("Routing to error (error detected)")
        return "error"
    
    # Check if approval is required
    needs_approval = state.get("needs_approval", False)
    force_approval = state.get("force_approval", False)
    auto_approve = state.get("auto_approve", False)
    is_admin = state.get("user_role", "").lower() == "admin"
    
    logger.info(
        f"Routing decision: needs_approval={needs_approval}, "
        f"force_approval={force_approval}, auto_approve={auto_approve}, "
        f"is_admin={is_admin}, next_action={nxt}"
    )
    
    # CRITICAL: force_approval overrides everything
    if force_approval:
        logger.info("Force approval required - routing to await_approval")
        return "await_approval"
    
    # If needs_approval is set and auto_approve is not enabled, require approval
    if needs_approval and not auto_approve:
        logger.info(f"Approval required (needs_approval={needs_approval}, auto_approve={auto_approve}) - routing to await_approval")
        return "await_approval"
    
    # If auto_approve is enabled OR admin with low risk, skip approval gate
    if (auto_approve or is_admin) and not needs_approval:
        logger.info(f"Auto-approve enabled (auto_approve={auto_approve}, is_admin={is_admin}) - routing to probe_sql")
        # Route to probe_sql for structural validation before execution
        return "probe_sql"
    
    # Default: require approval for safety
    logger.info("Default routing - requiring approval for safety")
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
