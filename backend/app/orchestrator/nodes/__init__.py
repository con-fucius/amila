"""
Orchestrator Node Functions

Each node represents a step in the query processing pipeline.
Nodes are responsible for:
- Processing state
- Emitting SSE events for frontend updates
- Tracking execution in node_history
- Logging to Langfuse spans (when enabled)
"""

from app.orchestrator.nodes.understand import understand_query_node
from app.orchestrator.nodes.context import retrieve_context_node
from app.orchestrator.nodes.hypothesis import generate_hypothesis_node
from app.orchestrator.nodes.sql_generation import generate_sql_node
from app.orchestrator.nodes.validation import validate_query_node, probe_sql_node
from app.orchestrator.nodes.approval import await_approval_node
from app.orchestrator.nodes.execution import execute_query_node
from app.orchestrator.nodes.repair import repair_sql_node
from app.orchestrator.nodes.fallback import generate_fallback_sql_node
from app.orchestrator.nodes.results import validate_results_node, format_results_node
from app.orchestrator.nodes.pivot import pivot_strategy_node
from app.orchestrator.nodes.decompose import decompose_query_node
from app.orchestrator.nodes.error import error_node

__all__ = [
    "understand_query_node",
    "retrieve_context_node",
    "generate_hypothesis_node",
    "generate_sql_node",
    "validate_query_node",
    "probe_sql_node",
    "await_approval_node",
    "execute_query_node",
    "repair_sql_node",
    "generate_fallback_sql_node",
    "validate_results_node",
    "format_results_node",
    "pivot_strategy_node",
    "decompose_query_node",
    "error_node",
]
