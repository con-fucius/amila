"""
Query Orchestrator Graph Builder

Implements a LangGraph StateGraph for NL-to-SQL query processing with:
- Human-in-the-loop (HITL) approval via interrupt_before
- Checkpointing for state persistence across approval cycles
- Retry policies for transient failures
- Comprehensive error handling
"""

import logging
from langgraph.graph import StateGraph, END, START
from langgraph.types import RetryPolicy

from app.orchestrator.state import QueryState
from app.orchestrator.nodes import (
    understand_query_node,
    retrieve_context_node,
    decompose_query_node,
    generate_hypothesis_node,
    generate_sql_node,
    validate_query_node,
    probe_sql_node,
    execute_query_node,
    repair_sql_node,
    generate_fallback_sql_node,
    validate_results_node,
    format_results_node,
    pivot_strategy_node,
)
from app.orchestrator.nodes.approval import await_approval_node
from app.orchestrator.nodes.error import error_node
from app.orchestrator.routing import (
    route_after_understanding,
    route_after_validation_with_probe,
    route_after_approval,
    route_after_probe,
    route_after_execute,
    route_after_result_validation,
    route_after_pivot,
    route_after_sql_generation,
)

logger = logging.getLogger(__name__)


async def create_query_orchestrator(checkpointer):
    """
    Build LangGraph StateGraph for query processing with HITL support.
    
    Args:
        checkpointer: AsyncSqliteSaver instance managed by application lifespan
    
    Workflow:
    START -> understand -> retrieve_context -> decompose -> hypothesis -> sql -> validate 
          -> await_approval (HITL interrupt) -> probe_sql -> execute -> validate_results -> format -> END
                                                                                                                 
    Error paths:
    - Any node can route to 'error' on failure
    - repair_sql and generate_fallback_sql provide recovery paths
    - pivot_strategy allows query reformulation on empty results
    
    HITL Flow:
    1. Graph pauses at await_approval node (interrupt_before)
    2. Frontend displays SQL for user review
    3. User approves/rejects via /approve endpoint
    4. Graph resumes from await_approval with updated state
    """
    
    # Create graph
    workflow = StateGraph(QueryState)
    
    # Add nodes with retry policies for transient failures
    workflow.add_node("understand", understand_query_node)
    workflow.add_node("retrieve_context", retrieve_context_node)
    workflow.add_node("decompose_query", decompose_query_node)
    workflow.add_node("generate_hypothesis", generate_hypothesis_node)
    workflow.add_node("generate_sql", generate_sql_node, retry=RetryPolicy(max_attempts=2))
    workflow.add_node("validate", validate_query_node)
    
    # HITL approval node - graph will interrupt before this node
    workflow.add_node("await_approval", await_approval_node)
    
    workflow.add_node("probe_sql", probe_sql_node)
    workflow.add_node("execute", execute_query_node, retry=RetryPolicy(max_attempts=3))
    workflow.add_node("validate_results", validate_results_node)
    workflow.add_node("pivot_strategy", pivot_strategy_node)
    workflow.add_node("format_results", format_results_node)
    workflow.add_node("error", error_node)
    
    # Recovery nodes with retry policies
    workflow.add_node("repair_sql", repair_sql_node, retry=RetryPolicy(max_attempts=2))
    workflow.add_node("generate_fallback_sql", generate_fallback_sql_node, retry=RetryPolicy(max_attempts=1))
    
    # === Edge definitions ===
    
    # Entry point
    workflow.add_edge(START, "understand")
    
    # Understanding -> Context retrieval
    workflow.add_conditional_edges(
        "understand",
        route_after_understanding,
        {
            "retrieve_context": "retrieve_context",
            "error": "error",
        }
    )
    
    # Context -> decompose -> hypothesis -> SQL generation (linear flow)
    workflow.add_edge("retrieve_context", "decompose_query")
    workflow.add_edge("decompose_query", "generate_hypothesis")
    workflow.add_edge("generate_hypothesis", "generate_sql")
    
    # SQL generation -> validation or clarification
    workflow.add_conditional_edges(
        "generate_sql",
        route_after_sql_generation,
        {
            "validate": "validate",
            "request_clarification": END,  # Pause for user clarification (different from approval)
            "error": "error",
        }
    )
    
    # Validation -> approval gate or error
    workflow.add_conditional_edges(
        "validate",
        route_after_validation_with_probe,
        {
            "await_approval": "await_approval",  # Route to approval node
            "error": "error",
        }
    )
    
    # Approval gate -> probe/execute or rejection
    workflow.add_conditional_edges(
        "await_approval",
        route_after_approval,
        {
            "probe_sql": "probe_sql",
            "execute": "execute",  # Skip probe for non-Oracle or simple queries
            "rejected": END,  # User rejected the query
            "error": "error",
        }
    )
    
    # Probe SQL -> execute or repair
    workflow.add_conditional_edges(
        "probe_sql",
        route_after_probe,
        {
            "execute": "execute",
            "repair_sql": "repair_sql",
            "error": "error",
        }
    )
    
    # Execute -> result validation or recovery
    workflow.add_conditional_edges(
        "execute",
        route_after_execute,
        {
            "validate_results": "validate_results",
            "repair_sql": "repair_sql",
            "generate_fallback_sql": "generate_fallback_sql",
            "error": "error",
        }
    )
    
    # Result validation -> format or pivot
    workflow.add_conditional_edges(
        "validate_results",
        route_after_result_validation,
        {
            "format_results": "format_results",
            "pivot_strategy": "pivot_strategy",
        }
    )
    
    # Pivot strategy -> retry or give up
    workflow.add_conditional_edges(
        "pivot_strategy",
        route_after_pivot,
        {
            "generate_hypothesis": "generate_hypothesis",  # Loop back for retry
            "format_results": "format_results",  # Give up after max attempts
        }
    )
    
    # Recovery paths loop back to validation
    workflow.add_edge("repair_sql", "validate")
    workflow.add_edge("generate_fallback_sql", "validate")
    
    # Terminal edges
    workflow.add_edge("format_results", END)
    workflow.add_edge("error", END)
    
    # Compile graph with checkpointing and HITL interrupt
    # interrupt_before pauses execution BEFORE the specified node runs
    app = workflow.compile(
        checkpointer=checkpointer,
        interrupt_before=["await_approval"],  # HITL: pause before approval check
    )
    
    logger.info("Query orchestrator graph compiled with HITL support")
    return app
