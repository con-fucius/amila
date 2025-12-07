"""
Query Processor - Main entry point for query orchestration
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from app.orchestrator.state import QueryState

logger = logging.getLogger(__name__)

_orchestrator_init_lock = asyncio.Lock()

# SSE state management
try:
    from app.services.query_state_manager import get_query_state_manager as _get_qs_manager, QueryState as ExecState
except Exception:
    _get_qs_manager = None
    ExecState = None


def validate_and_fix_state(state: dict) -> dict:
    """
    Validate and fix state to ensure all expected dict fields are actually dicts.
    This prevents AttributeError when accessing nested properties.
    """
    dict_fields = [
        "llm_metadata",
        "context",
        "validation_result",
        "execution_result",
        "result_analysis",
        "visualization_hints",
        "approval_context",
    ]
    
    for field in dict_fields:
        if field in state and not isinstance(state[field], dict):
            logger.warning(f"State field '{field}' is not a dict (got {type(state[field]).__name__}), replacing with empty dict")
            state[field] = {}
    
    return state


async def _ensure_orchestrator_initialized():
    """Ensure the LangGraph orchestrator is available, rebuilding if needed."""

    from app.core.client_registry import registry

    orchestrator = registry.get_query_orchestrator()
    if orchestrator:
        logger.info(f"Orchestrator already initialized: id={id(orchestrator)}")
        return orchestrator

    logger.info(f"Rebuilding orchestrator...")
    async with _orchestrator_init_lock:
        orchestrator = registry.get_query_orchestrator()
        if orchestrator:
            logger.info(f"Orchestrator initialized by another thread: id={id(orchestrator)}")
            return orchestrator

        checkpointer = registry.get_langgraph_checkpointer()
        checkpointer_context = registry.get_langgraph_checkpointer_context()
        new_context = None

        try:
            if not checkpointer or not checkpointer_context:
                from app.core.config import settings
                from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

                logger.info(f"Creating new AsyncSqliteSaver context")
                new_context = AsyncSqliteSaver.from_conn_string(settings.LANGGRAPH_CHECKPOINT_DB)
                checkpointer = await new_context.__aenter__()
                checkpointer_context = new_context
                registry.set_langgraph_checkpointer(checkpointer, new_context)
                logger.info(f"Checkpointer created: id={id(checkpointer)}, context id={id(new_context)}")
            else:
                logger.info(f"Reusing existing checkpointer: id={id(checkpointer)}, context id={id(checkpointer_context)}")

            from app.orchestrator import create_query_orchestrator

            orchestrator = await create_query_orchestrator(checkpointer)
            registry.set_query_orchestrator(orchestrator)
            logger.info(f"Orchestrator rebuilt successfully: id={id(orchestrator)}")
            return orchestrator
        except Exception as init_err:
            logger.error(f"Failed to initialize LangGraph orchestrator dynamically: {init_err}", exc_info=True)
            if new_context:
                try:
                    await new_context.__aexit__(None, None, None)
                except Exception:
                    pass
            registry.clear_langgraph_checkpointer()
            return None


async def process_query(
    user_query: str,
    user_id: str,
    session_id: str,
    user_role: str = "analyst",
    thread_id_override: str | None = None,
    database_type: str = "oracle",
    conversation_history: list | None = None,
) -> dict:
    """
    Process a user query through the orchestrator
    
    Args:
        user_query: Natural language query
        user_id: User identifier
        session_id: Conversation session ID
        user_role: User role for RBAC (admin, analyst, viewer)
        conversation_history: Previous messages for context
        
    Returns:
        Processing result dict
    """
    from app.core.client_registry import registry
    from app.services.conversation_router import ConversationRouter, IntentType
    from app.core.redis_client import redis_client
    
    # Use route_with_context for LLM-based intent classification
    schema_context = None
    try:
        from app.services.schema_service import SchemaService
        schema_result = await SchemaService.get_database_schema(use_cache=True)
        if schema_result.get("status") == "success":
            schema_context = schema_result.get("schema", {})
    except Exception:
        pass
    
    # Convert conversation_history to the format expected by route_with_context
    formatted_history = None
    if conversation_history:
        formatted_history = [
            {"role": msg.get("role", "user"), "content": msg.get("content", "")}
            for msg in conversation_history
        ]
    
    # Route the query using enhanced context-aware routing
    routing_result = await ConversationRouter.route_with_context(
        user_query, 
        formatted_history, 
        schema_context,
        use_llm=True  # Enable LLM-based classification
    )
    
    # Store conversation history in Redis for memory persistence
    try:
        history_key = f"conversation:{session_id}:history"
        await redis_client.lpush(history_key, {
            "role": "user",
            "content": user_query,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "intent": routing_result.get("intent"),
        })
        # Keep only last 50 messages
        await redis_client.ltrim(history_key, 0, 49)
        # Set TTL of 24 hours
        await redis_client.expire(history_key, 86400)
    except Exception as e:
        logger.warning(f"Failed to store conversation history in Redis: {e}")
    
    # Handle conversational intents directly without SQL generation
    if not routing_result.get("requires_sql") and routing_result.get("response"):
        query_id = f"conv_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        return {
            "status": "success",
            "query_id": query_id,
            "message": routing_result["response"],
            "intent": routing_result["intent"],
            "is_conversational": True,
            "sql_query": None,
            "results": None,
            "needs_approval": False,
            "llm_metadata": {
                "intent": routing_result["intent"],
                "confidence": routing_result["confidence"],
                "conversational": True,
            },
        }
    from app.core.langfuse_client import (
        create_trace,
        get_langfuse_client,
        trace_span,
        update_trace,
    )
    from app.core.structured_logging import clear_context, set_trace_id, set_user_context
    from app.core.config import settings
    import time
    
    start_time = time.time()
    query_id = f"q_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    langfuse_client = get_langfuse_client()
    trace_metadata = {
        "session_id": session_id,
        "user_role": user_role,
        "database_type": database_type,
        "entrypoint": "process_query",
        # Minimal frontend metadata: this path is driven by the chat interface
        "frontend_surface": "chat",
        "frontend_interface": "RealChatInterface",
        "app_version": getattr(settings, "app_version", "unknown"),
    }
    langfuse_trace_id = create_trace(
        query_id=query_id,
        user_id=user_id,
        user_query=user_query,
        metadata=trace_metadata,
    )
    trace_identifier = langfuse_trace_id or query_id
    set_trace_id(trace_identifier)
    set_user_context(user_id=user_id, session_id=session_id)

    # Stream lifecycle: received
    try:
        if _get_qs_manager and ExecState:
            m = await _get_qs_manager()
            await m.update_state(
                query_id,
                ExecState.RECEIVED,
                {"user_id": user_id, "trace_id": trace_identifier},
            )
    except Exception:
        pass
    
    # Pre-load schema metadata so downstream nodes always have baseline context
    base_schema_metadata: Dict[str, Any] = {}
    base_schema_source: str | None = None
    if database_type == "oracle":
        try:
            from app.services.schema_service import SchemaService

            schema_result = await SchemaService.get_database_schema(use_cache=True)
            if schema_result.get("status") == "success":
                base_schema_metadata = schema_result.get("schema") or {}
                base_schema_source = schema_result.get("source")
            else:
                logger.warning(
                    " Cached schema load failed (%s) - attempting live refresh",
                    schema_result.get("error") or "unknown error",
                )

            if not base_schema_metadata or not base_schema_metadata.get("tables"):
                live_schema_result = await SchemaService.get_database_schema(use_cache=False)
                if live_schema_result.get("status") == "success":
                    base_schema_metadata = live_schema_result.get("schema") or {}
                    base_schema_source = live_schema_result.get("source")
                else:
                    logger.error(
                        " Live schema refresh failed: %s",
                        live_schema_result.get("error") or "unknown error",
                    )
        except Exception as schema_err:
            logger.error(f"Failed to hydrate schema metadata before orchestration: %s", schema_err, exc_info=True)
    else:
        # For non-Oracle backends (e.g. Doris) we rely on downstream dynamic schema
        # discovery nodes instead of preloading the Oracle catalog here.
        logger.info("Skipping Oracle schema pre-hydration for database_type=%s", database_type)

    initial_context: Dict[str, Any] = {
        "schema_metadata": base_schema_metadata or {},
        "enriched_schema": None,
    }
    if base_schema_source:
        initial_context["schema_source"] = base_schema_source
    initial_context["hydrated_at"] = datetime.now(timezone.utc).isoformat()

    initial_state = {
        "messages": [],
        "user_query": user_query,
        "intent": "",
        "hypothesis": "",
        "context": initial_context,
        "sql_query": "",
        "validation_result": {},
        "execution_result": {},
        "result_analysis": {},
        "visualization_hints": {},
        "llm_metadata": {
            "execution_steps": [],  # Track actual execution steps for frontend
            "thinking_steps": [],  # Track reasoning steps for frontend
            "start_time": time.time(),
        },
        "user_id": user_id,
        "user_role": user_role,
        "session_id": session_id,
        "query_id": query_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trace_id": trace_identifier,
        "database_type": database_type,
        "needs_approval": False,
        "approved": False,
        "error": "",
        "next_action": "",
        "repair_attempts": 0,
        "fallback_attempts": 0,
        "pivot_attempts": 0,
        "pivot_strategies": [],
        "sql_confidence": 100,
        "optimization_suggestions": [],
        "total_iterations": 0,  # Track total graph iterations
        "max_iterations": 40,  # Safety limit
        "node_history": [],  # Track node execution for reasoning visibility
        "current_node": "",  # Current executing node
        # Query cost estimation and execution plan
        "cost_estimate": {},
        "execution_plan": "",
        # Clarification flow
        "clarification_message": "",
        "clarification_details": {},
        "clarification_history": [],
        # Preview data for progressive disclosure
        "preview": {},
    }
    
    # Get orchestrator from registry (recover if missing)
    orchestrator = await _ensure_orchestrator_initialized()
    if not orchestrator:
        logger.error(f"Query orchestrator unavailable after recovery attempt")
        
        # Emit SSE error event
        try:
            if _get_qs_manager and ExecState:
                m = await _get_qs_manager()
                await m.update_state(
                    query_id,
                    ExecState.ERROR,
                    {"error": "Query orchestrator not available. System is initializing, please retry in a moment."},
                )
        except Exception:
            pass
        
        clear_context()
        return {
            "status": "error",
            "error": "Query orchestrator not available. System is initializing, please retry in a moment.",
            "query_id": query_id,
        }
    
    try:
        # Invoke graph
        # Use query_id as thread_id to ensure approval resume by query_id, unless override provided
        thread_id = thread_id_override or query_id
        config = {
            "configurable": {"thread_id": thread_id},
            "recursion_limit": 50  # Increase from default 25
        }
        async with trace_span(
            trace_identifier,
            "orchestrator.invoke",
            input_data={
                "query_id": query_id,
                "user_id": user_id,
                "session_id": session_id,
                "thread_id": thread_id,
            },
            metadata={"user_role": user_role},
        ) as orchestrator_span:
            result = await orchestrator.ainvoke(initial_state, config)
            orchestrator_span["output"] = {
                "status": "completed",
                "query_id": query_id,
                "needs_approval": result.get("needs_approval") if isinstance(result, dict) else None,
            }

        # Validate and fix state
        final_state = validate_and_fix_state(result)
        
        # Deep copy execution_result to prevent cleanup code from nullifying rows
        import copy
        execution_result = final_state.get("execution_result")
        execution_result_copy = copy.deepcopy(execution_result) if execution_result else None
        
        logger.info(f"Processing final results: execution_result present={execution_result is not None}")
        if execution_result_copy:
            row_count = execution_result_copy.get("row_count", 0)
            columns = execution_result_copy.get("columns", [])
            rows = execution_result_copy.get("rows", [])
            logger.info(f"  Result structure: {len(columns)} columns, {len(rows)} rows, row_count={row_count}")
            if len(rows) != row_count:
                logger.warning(f"   Row count mismatch: array has {len(rows)} rows but row_count={row_count}")
        
        # SAFETY: Ensure llm_metadata is always a dict
        raw_llm_metadata = final_state.get("llm_metadata", {})
        if not isinstance(raw_llm_metadata, dict):
            logger.warning(f"llm_metadata was not a dict (got {type(raw_llm_metadata).__name__}), replacing with empty dict")
            raw_llm_metadata = {}
            final_state["llm_metadata"] = {}
        
        # Enrich llm_metadata with execution summary for frontend
        if "execution_steps" not in raw_llm_metadata:
            raw_llm_metadata["execution_steps"] = []
        
        # CRITICAL: Also populate thinking_steps for frontend compatibility
        if "thinking_steps" not in raw_llm_metadata:
            raw_llm_metadata["thinking_steps"] = raw_llm_metadata.get("execution_steps", [])
        
        # Add SQL query to metadata for frontend display
        if final_state.get("sql_query"):
            raw_llm_metadata["sql_generated"] = final_state["sql_query"]
        
        # Add execution time
        if "start_time" in raw_llm_metadata:
            raw_llm_metadata["total_execution_ms"] = int((time.time() - raw_llm_metadata["start_time"]) * 1000)
        
        # Add error details if present
        if final_state.get("error"):
            raw_llm_metadata["error_details"] = {
                "message": final_state["error"],
                "failed_at": final_state.get("current_stage", "unknown"),
                "sql_attempted": final_state.get("sql_query"),
            }
            # Also add to top-level for easier frontend access
            raw_llm_metadata["failed_stage"] = final_state.get("current_stage", "unknown")
        
        # Check if this is a clarification request
        is_clarification = (
            final_state.get("next_action") == "request_clarification" or
            raw_llm_metadata.get("clarification_needed", False)
        )
        
        # Check if query is waiting for approval (HITL interrupt)
        # With interrupt_before=["await_approval"], the graph pauses BEFORE that node
        # So needs_approval=True indicates we're waiting for user approval
        is_pending_approval = (
            final_state.get("next_action") in ["request_approval", "await_approval"] or
            (final_state.get("needs_approval", False) and not final_state.get("approved", False))
        )
        
        # Format status based on state
        if is_clarification:
            status = "clarification_needed"
            message = (
                final_state.get("error")
                or raw_llm_metadata.get("clarification_message")
                or final_state.get("clarification_message")
                or "Clarification needed to generate correct SQL"
            )
            # Preserve clarification context for frontend
            if final_state.get("clarification_details"):
                raw_llm_metadata["clarification_details"] = final_state["clarification_details"]
        elif is_pending_approval and not final_state.get("error"):
            # Query is waiting for approval - this is NOT an error
            # Graph was interrupted before await_approval node
            status = "pending_approval"
            approval_ctx = final_state.get("approval_context", {})
            risk_level = approval_ctx.get("risk_level", "unknown")
            message = f"Query requires approval ({risk_level} risk). Please review and approve to execute."
            logger.info(f"Query {query_id} paused for HITL approval (risk: {risk_level})")
        elif final_state.get("error"):
            status = "error"
            message = final_state.get("error")
            # Ensure error context is preserved
            if not raw_llm_metadata.get("error_details"):
                raw_llm_metadata["error_details"] = {
                    "message": message,
                    "failed_at": final_state.get("current_stage", final_state.get("error_stage", "unknown")),
                    "sql_attempted": final_state.get("sql_query"),
                }
        elif not execution_result_copy or not isinstance(execution_result_copy, dict):
            # No execution result - check if it's because of pending approval
            if is_pending_approval:
                status = "pending_approval"
                message = "Query requires approval before execution"
            else:
                status = "error"
                message = "Query execution did not complete - no results returned"
        else:
            status = "success"
            message = "Query processed successfully"
        
        # CRITICAL: When status is error or clarification, clear needs_approval
        needs_approval = final_state.get("needs_approval", False) if status in ["success", "pending_approval"] else False
        
        # Extract row count for visibility
        row_count = 0
        rows_requested = None
        if execution_result_copy and isinstance(execution_result_copy, dict):
            # execution_result has FLAT structure: {columns, rows, row_count, ...}
            row_count = execution_result_copy.get("row_count", len(execution_result_copy.get("rows", [])))
        
        # Normalize result structure - SINGLE consistent format
        # Structure: {columns: [], rows: [], row_count: N, execution_time_ms: T}
        normalized_result = None
        if execution_result_copy:
            # CRITICAL: Use flat structure consistently
            normalized_result = {
                "columns": execution_result_copy.get("columns", []),
                "rows": execution_result_copy.get("rows", []),
                "row_count": execution_result_copy.get("row_count", len(execution_result_copy.get("rows", []))),
                "execution_time_ms": execution_result_copy.get("execution_time_ms", 0),
                "timestamp": execution_result_copy.get("timestamp", ""),
                "status": execution_result_copy.get("status", "success"),
            }
            
            logger.info(f"Normalized result: {len(normalized_result['columns'])} columns, {len(normalized_result['rows'])} rows")
        
        response_payload = {
            "status": status,
            "message": message,
            "error": message if status == "error" else None,  # CRITICAL: Include error field for frontend
            "query_id": query_id,
            "sql_query": final_state.get("sql_query", ""),
            "sql_confidence": final_state.get("sql_confidence", 100),
            "optimization_suggestions": final_state.get("optimization_suggestions", []),
            "results": normalized_result,  # Standard field name used across the codebase
            "row_count": row_count,
            "rows_requested": rows_requested,
            "visualization_hints": final_state.get("visualization_hints", {}),
            "visualization": final_state.get("visualization_hints", {}),
            "result_analysis": final_state.get("result_analysis", {}),
            "approval_context": final_state.get("approval_context"),
            "insights": final_state.get("insights"),
            "suggested_queries": final_state.get("suggested_queries"),
            "validation": final_state.get("validation_result"),
            "needs_approval": needs_approval,
            "llm_metadata": raw_llm_metadata,
            "trace_id": trace_identifier,
            "node_history": final_state.get("node_history", []),  # Include node execution history
            "current_node": final_state.get("current_node", ""),  # Include current node
            # Query cost estimation and execution plan for observability
            "cost_estimate": final_state.get("cost_estimate"),
            "execution_plan": final_state.get("execution_plan"),
            # Clarification context for frontend
            "clarification_message": final_state.get("clarification_message") if is_clarification else None,
            "clarification_details": final_state.get("clarification_details") if is_clarification else None,
        }
        
        # Push to query history for undo/redo functionality
        if status == "success" and normalized_result:
            try:
                from app.services.query_history_service import QueryHistoryService
                await QueryHistoryService.push_query_state(
                    session_id=session_id,
                    user_query=user_query,
                    sql_query=final_state.get("sql_query", ""),
                    result_summary={
                        "status": status,
                        "row_count": row_count,
                        "execution_time_ms": normalized_result.get("execution_time_ms", 0),
                    },
                    metadata={
                        "query_id": query_id,
                        "database_type": database_type,
                        "trace_id": trace_identifier,
                    }
                )
            except Exception as hist_err:
                logger.warning(f"Failed to push query to history: {hist_err}")

        # Surface clarification details at top-level when applicable
        if is_clarification:
            response_payload["clarification_message"] = (
                raw_llm_metadata.get("clarification_message") or message
            )
            if raw_llm_metadata.get("clarification_details") is not None:
                response_payload["clarification_details"] = raw_llm_metadata.get("clarification_details")

        # Emit SSE lifecycle events for frontend streaming
        try:
            if _get_qs_manager and ExecState:
                m = await _get_qs_manager()
                if status == "pending_approval" or needs_approval:
                    await m.update_state(
                        query_id,
                        ExecState.PENDING_APPROVAL,
                        {
                            "sql": final_state.get("sql_query", ""),
                            "approval_context": final_state.get("approval_context"),
                            "thinking_steps": raw_llm_metadata.get("thinking_steps"),
                            "trace_id": trace_identifier,
                            "message": message,
                        },
                    )
                elif status == "success":
                    await m.update_state(
                        query_id,
                        ExecState.FINISHED,
                        {
                            "sql": final_state.get("sql_query", ""),
                            "result": execution_result_copy,
                            "insights": final_state.get("insights"),
                            "suggested_queries": final_state.get("suggested_queries"),
                            "thinking_steps": raw_llm_metadata.get("thinking_steps"),
                            "trace_id": trace_identifier,
                        },
                    )
                elif status == "error":
                    # Surface rich error context to SSE subscribers
                    await m.update_state(
                        query_id,
                        ExecState.ERROR,
                        {
                            "error": message,
                            "trace_id": trace_identifier,
                            "thinking_steps": raw_llm_metadata.get("thinking_steps"),
                            "error_details": raw_llm_metadata.get("error_details"),
                            "failed_stage": final_state.get("current_stage"),
                            "sql": final_state.get("sql_query"),
                        },
                    )
        except Exception:
            # Do not fail the request if SSE emission has issues
            pass

        # Langfuse trace completion (disabled - requires decorator context)
        # Flush any pending events
        if langfuse_client:
            try:
                # Preserve original trace metadata (including frontend_surface/session_id)
                metadata = {
                    **trace_metadata,
                    "error": message if status == "error" else None,
                    "clarification": is_clarification,
                }
                update_trace(
                    trace_identifier,
                    output_data={
                        "status": status,
                        "query_id": query_id,
                        "row_count": row_count,
                        "needs_approval": needs_approval,
                        "execution_time_ms": int((time.time() - start_time) * 1000),
                    },
                    metadata=metadata,
                    tags=[status],
                )
                langfuse_client.flush()
            except Exception:
                pass

        clear_context()
        return response_payload
        
    except AttributeError as attr_err:
        logger.error(f"AttributeError during query processing: {attr_err}", exc_info=True)
        if langfuse_client:
            try:
                update_trace(
                    trace_identifier,
                    output_data={"status": "error", "query_id": query_id},
                    metadata={**trace_metadata, "exception": str(attr_err)},
                    tags=["error"],
                )
            except Exception:
                pass
        clear_context()
        return {
            "status": "error",
            "error": "Internal state management error. The system encountered an unexpected data type.",
            "query_id": query_id,
            "llm_metadata": {"error": str(attr_err)},
        }
    except Exception as e:
        logger.error(f"Query processing failed: {e}", exc_info=True)
        if langfuse_client:
            try:
                update_trace(
                    trace_identifier,
                    output_data={"status": "error", "query_id": query_id},
                    metadata={**trace_metadata, "exception": str(e)},
                    tags=["error"],
                )
            except Exception:
                pass
        clear_context()
        return {
            "status": "error",
            "error": str(e),
            "query_id": query_id,
        }
