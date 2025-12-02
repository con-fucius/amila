"""
Query Orchestrator State Definition
"""

from typing import TypedDict, Annotated, Sequence
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class QueryState(TypedDict):
    """Agent state tracking query processing workflow"""
    
    # Message history for conversation context
    messages: Annotated[Sequence[BaseMessage], add_messages]
    
    # Query processing pipeline
    user_query: str  # Original natural language query
    intent: str  # Classified intent (read-only, aggregation, etc.)
    hypothesis: str  # Query execution plan
    context: dict  # Knowledge graph context from Graphiti + enriched schema
    sql_query: str  # Generated SQL
    validation_result: dict  # Validation status and feedback
    execution_result: dict  # Query execution result
    result_analysis: dict  # Post-execution result validation
    visualization_hints: dict  # Recommended visualization type
    
    # Metadata
    user_id: str
    user_role: str  # User role for RBAC (admin, analyst, viewer)
    session_id: str
    query_id: str
    timestamp: str
    trace_id: str  # For comprehensive logging & correlation
    database_type: str
    
    # LLM tracking
    llm_metadata: dict  # LLM provider, model, and generation status
    
    # SQL generation enhancements
    sql_confidence: int  # Confidence score 0-100
    optimization_suggestions: list  # Query optimization suggestions
    
    # Control flow
    needs_approval: bool
    approved: bool
    error: str
    next_action: str  # routing decision
    repair_attempts: int  # number of auto-repairs attempted
    fallback_attempts: int  # number of fallback SQL attempts
    pivot_attempts: int  # number of strategy pivots attempted
    pivot_strategies: list  # alternative strategies generated
    preview: dict  # preview data for progressive disclosure

    # Clarification flow
    clarification_message: str
    clarification_details: dict
    # Clarification history for multi-turn context preservation
    clarification_history: list  # [{clarification, timestamp}]

    # Execution visibility
    node_history: list  # [{name, status, start_time, end_time, thinking_steps, error}]
    current_node: str
