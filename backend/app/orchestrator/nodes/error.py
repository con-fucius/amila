"""
Orchestrator Node: Error Handling
"""

import logging
import asyncio
from datetime import datetime, timezone

from app.orchestrator.state import QueryState

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

from app.orchestrator.utils import emit_state_event, update_node_history

logger = logging.getLogger(__name__)


async def error_node(state: QueryState) -> QueryState:
    """Error handling node with node history tracking"""
    logger.error(f"Error state reached: {state.get('error', 'Unknown error')}")
    
    state["current_stage"] = "error"
    
    # Track node execution for reasoning visibility
    error_msg = state.get('error', 'Unknown error')
    await update_node_history(state, "error", "completed", 
        thinking_steps=[
            {"id": "step-1", "content": f"Error occurred: {error_msg[:200]}", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ],
        error=error_msg
    )
    
    # Stream lifecycle: error
    try:
        if ExecState:
            await emit_state_event(state, ExecState.ERROR, {"error": error_msg})
    except Exception:
        pass
    
    state["next_action"] = "end"
    return state
