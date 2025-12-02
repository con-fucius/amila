"""
Query Orchestrator Graph Builder
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
from app.orchestrator.nodes.error import error_node
from app.orchestrator.routing import (
    route_after_understanding,
    route_after_validation_with_probe,
    route_after_probe,
    route_after_execute,
    route_after_result_validation,
    route_after_pivot,
    route_after_sql_generation,
)

logger = logging.getLogger(__name__)


async def create_query_orchestrator(checkpointer):
    """
    Build LangGraph StateGraph for query processing
    
    Args:
        checkpointer: AsyncSqliteSaver instance managed by application lifespan
    
    Workflow:
    START -> understand -> retrieve_context -> decompose -> hypothesis -> sql -> validate -> probe -> execute -> validate_results -> format -> END
                                                                                                                 
                                                                                      approval             pivot_strategy
                                                                                                                 
                                                                                      repair/fallback     -> hypothesis (loop)
    """
    
    # Create graph
    workflow = StateGraph(QueryState)
    
    # Add nodes with retry policies
    workflow.add_node("understand", understand_query_node)
    workflow.add_node("retrieve_context", retrieve_context_node)
    workflow.add_node("decompose_query", decompose_query_node)
    workflow.add_node("generate_hypothesis", generate_hypothesis_node)
    workflow.add_node("generate_sql", generate_sql_node, retry=RetryPolicy(max_attempts=2))
    workflow.add_node("validate", validate_query_node)
    workflow.add_node("probe_sql", probe_sql_node)
    workflow.add_node("execute", execute_query_node, retry=RetryPolicy(max_attempts=2))
    workflow.add_node("validate_results", validate_results_node)
    workflow.add_node("pivot_strategy", pivot_strategy_node)
    workflow.add_node("format_results", format_results_node)
    workflow.add_node("error", error_node)
    workflow.add_node("repair_sql", repair_sql_node, retry=RetryPolicy(max_attempts=2))
    workflow.add_node("generate_fallback_sql", generate_fallback_sql_node, retry=RetryPolicy(max_attempts=1))
    
    # Add edges
    workflow.add_edge(START, "understand")
    workflow.add_conditional_edges(
        "understand",
        route_after_understanding,
        {
            "retrieve_context": "retrieve_context",
            "error": "error",
        }
    )
    
    # Context -> decompose -> hypothesis -> SQL generation
    workflow.add_edge("retrieve_context", "decompose_query")
    workflow.add_edge("decompose_query", "generate_hypothesis")
    workflow.add_edge("generate_hypothesis", "generate_sql")
    
    # Route from generate_sql
    workflow.add_conditional_edges(
        "generate_sql",
        route_after_sql_generation,
        {
            "validate": "validate",
            "request_clarification": END,  # Pause for user clarification
            "error": "error",
        }
    )
    
    # Route from validate to probe_sql
    workflow.add_conditional_edges(
        "validate",
        route_after_validation_with_probe,
        {
            "probe_sql": "probe_sql",
            "request_approval": END,  # Pause for user approval
            "error": "error",
        }
    )
    
    # Route from probe_sql
    workflow.add_conditional_edges(
        "probe_sql",
        route_after_probe,
        {
            "execute": "execute",
            "repair_sql": "repair_sql",
            "error": "error",
        }
    )
    
    # Route after execute
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
    
    # Route after result validation
    workflow.add_conditional_edges(
        "validate_results",
        route_after_result_validation,
        {
            "format_results": "format_results",
            "pivot_strategy": "pivot_strategy",
        }
    )
    
    # Route after pivot strategy
    workflow.add_conditional_edges(
        "pivot_strategy",
        route_after_pivot,
        {
            "generate_hypothesis": "generate_hypothesis",  # Loop back for retry
            "format_results": "format_results",  # Give up after max attempts
        }
    )
    
    # Repair and fallback loop back to validate
    workflow.add_edge("repair_sql", "validate")
    workflow.add_edge("generate_fallback_sql", "validate")
    
    # Terminal edges
    workflow.add_edge("format_results", END)
    workflow.add_edge("error", END)
    
    # Compile graph with checkpointing
    app = workflow.compile(checkpointer=checkpointer)
    
    logger.info(f"Query orchestrator graph compiled")
    return app
