"""
Orchestrator Node: Approval Gate (HITL)

This node handles the human-in-the-loop approval workflow.
The graph interrupts BEFORE this node, allowing the user to review
the generated SQL and approve/reject it via the /approve endpoint.

When the graph resumes after approval, this node checks the approval
state and routes accordingly.
"""

import logging
from datetime import datetime, timezone

from app.orchestrator.state import QueryState
from app.orchestrator.utils import (
    emit_state_event,
    langfuse_span,
    update_node_history,
    set_state_error,
)

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


async def await_approval_node(state: QueryState) -> QueryState:
    """
    HITL Approval Gate Node
    
    This node is reached after the graph resumes from interrupt.
    It checks whether the user approved or rejected the query.
    
    Flow:
    1. Graph pauses BEFORE this node (interrupt_before)
    2. User reviews SQL via frontend
    3. User calls /approve endpoint which updates state and resumes graph
    4. This node runs and routes based on approval decision
    
    State fields used:
    - needs_approval: Whether approval was required (should be True when entering)
    - approved: Whether user approved (set by /approve endpoint)
    - sql_query: The SQL to execute (may be modified by user)
    - error: Set if user rejected with a reason
    """
    
    state["current_stage"] = "await_approval"
    
    # Track node execution
    await update_node_history(state, "await_approval", "in-progress", thinking_steps=[
        {
            "id": "approval-check",
            "content": "Checking approval status",
            "status": "in-progress",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    ])
    
    async with langfuse_span(
        state,
        "orchestrator.await_approval",
        input_data={
            "needs_approval": state.get("needs_approval"),
            "approved": state.get("approved"),
            "sql_preview": (state.get("sql_query") or "")[:200],
        },
        metadata={"stage": "await_approval"},
    ) as span:
        span.setdefault("output", {})
        
        query_id = state.get("query_id", "unknown")
        
        # Check if approval was granted
        if state.get("approved"):
            logger.info(f"Query {query_id} approved, proceeding to execution")
            
            # Add thinking step for approval
            if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
                state["llm_metadata"] = {}
            if "thinking_steps" not in state["llm_metadata"]:
                state["llm_metadata"]["thinking_steps"] = []
            
            state["llm_metadata"]["thinking_steps"].append({
                "content": "Query approved by user, proceeding to execution",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stage": "await_approval"
            })
            
            # Determine next action based on database type
            db_type = (state.get("database_type") or "oracle").lower()
            sql_query = state.get("sql_query", "")
            sql_upper = sql_query.upper()
            
            # Skip probe for non-Oracle or queries that don't work well with wrapping
            skip_probe = (
                db_type != "oracle" or
                "GROUP BY" in sql_upper or
                "FETCH FIRST" in sql_upper or
                "OFFSET" in sql_upper or
                "UNION" in sql_upper
            )
            
            if skip_probe:
                state["next_action"] = "execute"
                logger.info(f"Skipping probe (db_type={db_type}, skip_probe={skip_probe})")
            else:
                state["next_action"] = "probe_sql"
            
            # Clear approval flags for next iteration
            state["needs_approval"] = False
            
            span["output"].update({
                "status": "approved",
                "next_action": state["next_action"],
            })
            
            # Emit SSE event
            if ExecState:
                await emit_state_event(state, ExecState.APPROVED, {
                    "sql": state.get("sql_query"),
                    "message": "Query approved, executing...",
                })
            
            await update_node_history(state, "await_approval", "completed", thinking_steps=[
                {
                    "id": "approval-granted",
                    "content": "User approved the query",
                    "status": "completed",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            ])
            
            return state
        
        # Check if explicitly rejected (error field set)
        if state.get("error") and "rejected" in state.get("error", "").lower():
            logger.info(f"Query {query_id} rejected by user: {state.get('error')}")
            
            state["next_action"] = "rejected"
            
            span["output"].update({
                "status": "rejected",
                "reason": state.get("error"),
            })
            span["level"] = "WARNING"
            
            await update_node_history(state, "await_approval", "completed", thinking_steps=[
                {
                    "id": "approval-rejected",
                    "content": f"User rejected the query: {state.get('error')}",
                    "status": "completed",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            ])
            
            return state
        
        # If we reach here without approval, something went wrong
        # This shouldn't happen in normal flow - the graph should have been
        # interrupted before this node and resumed with approval state set
        logger.warning(f"Query {query_id} reached approval node without approval decision")
        
        # Check if this is the initial interrupt (needs_approval=True, approved=None/False)
        if state.get("needs_approval") and not state.get("approved"):
            # This is expected on first entry - graph will interrupt before this node
            # When resumed, approved should be set
            logger.info(f"Query {query_id} awaiting approval (initial state)")
            
            # Emit pending approval event
            if ExecState:
                await emit_state_event(state, ExecState.PENDING_APPROVAL, {
                    "sql": state.get("sql_query"),
                    "approval_context": state.get("approval_context"),
                    "message": "Awaiting user approval",
                })
            
            # Set next_action for routing (will be overridden when resumed)
            state["next_action"] = "pending"
            
            span["output"].update({
                "status": "pending",
                "message": "Awaiting user approval",
            })
            
            return state
        
        # Unexpected state - treat as error
        error_msg = "Approval workflow error: reached approval node in unexpected state"
        await set_state_error(state, "await_approval", error_msg, {
            "needs_approval": state.get("needs_approval"),
            "approved": state.get("approved"),
        })
        state["next_action"] = "error"
        
        span["output"].update({
            "status": "error",
            "error": error_msg,
        })
        span["level"] = "ERROR"
        
        return state
