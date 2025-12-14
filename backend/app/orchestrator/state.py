"""
Query Orchestrator State Definition
"""

from typing import TypedDict, Annotated, Sequence, Optional, Literal
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

# Valid database types
DatabaseTypeEnum = Literal["oracle", "doris"]

# Maximum history sizes to prevent unbounded memory growth
MAX_CLARIFICATION_HISTORY = 10
MAX_NODE_HISTORY = 50


class QueryState(TypedDict, total=False):
    """Agent state tracking query processing workflow.
    
    Uses total=False to make all fields optional with sensible defaults.
    """
    
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
    database_type: str  # Should be "oracle" or "doris"
    
    # LLM tracking
    llm_metadata: dict  # LLM provider, model, and generation status
    
    # SQL generation enhancements
    sql_confidence: int  # Confidence score 0-100
    optimization_suggestions: list  # Query optimization suggestions
    
    # Query cost estimation and execution plan visibility
    cost_estimate: dict  # {total_cost, cardinality, cost_level, warnings, recommendations}
    execution_plan: str  # Formatted EXPLAIN PLAN output for observability
    
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
    # Clarification history for multi-turn context preservation (bounded)
    clarification_history: list  # [{clarification, timestamp}] - max MAX_CLARIFICATION_HISTORY

    # Execution visibility (bounded)
    node_history: list  # [{name, status, start_time, end_time, thinking_steps, error}] - max MAX_NODE_HISTORY
    current_node: str


def get_default_state() -> dict:
    """Return default state values for initialization."""
    return {
        "messages": [],
        "user_query": "",
        "intent": "",
        "hypothesis": "",
        "context": {},
        "sql_query": "",
        "validation_result": {},
        "execution_result": {},
        "result_analysis": {},
        "visualization_hints": {},
        "user_id": "",
        "user_role": "viewer",
        "session_id": "",
        "query_id": "",
        "timestamp": "",
        "trace_id": "",
        "database_type": "oracle",
        "llm_metadata": {},
        "sql_confidence": 0,
        "optimization_suggestions": [],
        "cost_estimate": {},
        "execution_plan": "",
        "needs_approval": False,
        "approved": False,
        "error": "",
        "next_action": "",
        "repair_attempts": 0,
        "fallback_attempts": 0,
        "pivot_attempts": 0,
        "pivot_strategies": [],
        "preview": {},
        "clarification_message": "",
        "clarification_details": {},
        "clarification_history": [],
        "node_history": [],
        "current_node": "",
    }


def validate_database_type(db_type: str) -> str:
    """Validate and normalize database type."""
    normalized = db_type.lower().strip() if db_type else "oracle"
    if normalized not in ("oracle", "doris"):
        return "oracle"  # Default fallback
    return normalized


def trim_history_lists(state: dict) -> dict:
    """Trim unbounded history lists to prevent memory growth."""
    if "clarification_history" in state and isinstance(state["clarification_history"], list):
        if len(state["clarification_history"]) > MAX_CLARIFICATION_HISTORY:
            state["clarification_history"] = state["clarification_history"][-MAX_CLARIFICATION_HISTORY:]
    
    if "node_history" in state and isinstance(state["node_history"], list):
        if len(state["node_history"]) > MAX_NODE_HISTORY:
            state["node_history"] = state["node_history"][-MAX_NODE_HISTORY:]
    
    return state
