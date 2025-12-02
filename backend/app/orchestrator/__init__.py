"""
Query Orchestrator - Modular LangGraph workflow for intelligent SQL generation
"""

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm

# Lazy imports to avoid circular dependencies
def create_query_orchestrator(checkpointer):
    from app.orchestrator.graph import create_query_orchestrator as _create
    return _create(checkpointer)

async def process_query(
    user_query: str,
    user_id: str,
    session_id: str,
    user_role: str = "analyst",
    thread_id_override: str | None = None,
    database_type: str = "oracle",
):
    from app.orchestrator.processor import process_query as _process
    return await _process(
        user_query,
        user_id,
        session_id,
        user_role,
        thread_id_override,
        database_type=database_type,
    )

def validate_and_fix_state(state: dict):
    from app.orchestrator.processor import validate_and_fix_state as _validate
    return _validate(state)

__all__ = [
    "QueryState",
    "create_query_orchestrator",
    "process_query",
    "validate_and_fix_state",
    "get_llm",
]
