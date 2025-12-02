"""
Orchestrator Node: Execution
"""

import logging
import time
import hashlib
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm, get_query_llm_provider, get_query_llm_model
from app.orchestrator.utils import (
    emit_state_event,
    langfuse_span,
    update_node_history,
    METRICS_AVAILABLE,
    record_llm_usage,
    record_db_execution,
    record_query_result,
    set_state_error,
)
from app.core.config import settings
from app.core.client_registry import registry
from app.core.redis_client import redis_client
from app.core.error_normalizer import normalize_database_error

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


async def execute_query_node(state: QueryState) -> QueryState:
    """Execute SQL with Langfuse span instrumentation and structured errors."""

    state["current_stage"] = "execute"
    
    # Track node execution
    await update_node_history(state, "execute", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Executing SQL query on database", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])

    async with langfuse_span(
        state,
        "orchestrator.execute",
        input_data={
            "sql_preview": (state.get("sql_query") or "")[:500],
            "connection": settings.oracle_default_connection,
        },
        metadata={"stage": "execute"},
    ) as span:
        span.setdefault("output", {})
        result = await _execute_query_node_inner(state, span)
        span["output"]["next_action"] = result.get("next_action")
        return result


async def _execute_query_node_inner(state: QueryState, span: dict) -> QueryState:
    """
    Node 4: Execute SQL query via SQLcl MCP
    
    Handles:
    - Query execution
    - Result caching
    - Error handling
    - Performance metrics
    """
    logger.info(f"Executing SQL query...")

    # Stream lifecycle: executing with comprehensive progress
    if ExecState:
        mapped_concepts = state.get("column_mappings", [])
        confidence = state.get("sql_confidence", 100)
        await emit_state_event(state, ExecState.EXECUTING, {
            "sql_len": len(state.get("sql_query", "")),
            "thinking_steps": [
                {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-2", "content": "Retrieved schema context", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-3", "content": "Analyzed schema and identified relevant tables", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-4", "content": f"Mapped {len(mapped_concepts)} concepts to columns", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-5", "content": f"Generated SQL (confidence: {confidence}%)", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-6", "content": "Validated SQL syntax and security", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-7", "content": "Executing query on database...", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
            ],
            "todo_items": [
                {"id": "todo-1", "title": "Schema Analysis", "status": "completed", "details": "Identified tables and columns"},
                {"id": "todo-2", "title": "Column Mapping", "status": "completed", "details": f"Mapped {len(mapped_concepts)} concepts"},
                {"id": "todo-3", "title": "SQL Generation", "status": "completed", "details": f"Generated SQL (confidence: {confidence}%)"},
                {"id": "todo-4", "title": "Validation", "status": "completed", "details": "SQL validated successfully"},
                {"id": "todo-5", "title": "Execution", "status": "in-progress", "details": "Running query on database"}
            ]
        })

    db_type = state.get("database_type", "oracle")

    # Check cache first (normalize SQL before hashing)
    try:
        from app.core.sql_utils import normalize_sql
        norm_sql = normalize_sql(state["sql_query"])
    except Exception:
        norm_sql = state["sql_query"]

    # Include database_type in cache key to avoid cross-database collisions
    query_hash_input = f"{db_type}:{norm_sql}"
    query_hash = hashlib.sha256(query_hash_input.encode()).hexdigest()
    span["output"]["sql_hash"] = query_hash
    cached_result = await redis_client.get_cached_query_result(query_hash)

    if cached_result:
        logger.info(f"Query result retrieved from cache")
        span["output"].update({
            "cache_hit": True,
            "row_count": cached_result.get("row_count", 0),
        })
        state["execution_result"] = cached_result
        state["next_action"] = "format_results"
        return state

    try:
        import time
        db_exec_start = time.time()

        from app.services.database_router import DatabaseRouter

        connection_name = settings.oracle_default_connection if db_type == "oracle" else None
        span["output"]["connection"] = connection_name
        span["output"]["database_type"] = db_type

        logger.info(f"ORCHESTRATOR EXECUTION DEBUG:")
        logger.info(f"  SQL Query: {state['sql_query'][:300]}...")
        logger.info(f"  Database type: {db_type}")
        logger.info(f"  Connection: {connection_name or 'n/a'}")
        logger.info(f"  User: {state.get('user_id', 'unknown')}")
        logger.info(f"  Session: {state.get('session_id', 'unknown')}")

        # Route execution through the shared DatabaseRouter so Oracle and Doris
        # use the appropriate backend services.
        mcp_result = await DatabaseRouter.execute_sql(
            database_type=db_type,
            sql_query=state["sql_query"],
            connection_name=connection_name,
            user_id=state.get("user_id"),
            request_id=state.get("query_id"),
        )

        logger.debug(
            f"Raw execution result keys: {list(mcp_result.keys()) if isinstance(mcp_result, dict) else type(mcp_result)}"
        )
        logger.debug(
            f"Execution result type: {type(mcp_result)}, status: {mcp_result.get('status') if isinstance(mcp_result, dict) else 'N/A'}"
        )

        if mcp_result.get("status") == "success":
            mcp_results = mcp_result.get("results", {})
            row_count = mcp_results.get("row_count", 0)
            columns = mcp_results.get("columns", [])
            rows = mcp_results.get("rows", [])

            logger.info(f"Query executed: {row_count} rows, {len(columns)} columns")
            if rows and len(rows) > 0:
                logger.debug(f"  First row sample: {rows[0]}")
            elif row_count == 0:
                logger.info(f"  Query returned 0 rows (empty result set)")
            else:
                logger.warning(f"   MCP reported %d rows but rows array is empty!", row_count)

            result = {
                "status": "success",
                "columns": columns,
                "rows": rows,
                "row_count": row_count,
                "execution_time_ms": mcp_results.get("execution_time_ms", 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            await redis_client.cache_query_result(
                query_hash,
                result,
                result_size=result["row_count"]
            )

            state["execution_result"] = result
            state["next_action"] = "format_results"

            if METRICS_AVAILABLE:
                db_duration = time.time() - db_exec_start
                record_db_execution(db_duration, "success")
                record_query_result(result['row_count'], state.get('user_id', 'unknown'))

            span["output"].update({
                "status": "success",
                "cache_hit": False,
                "row_count": row_count,
                "column_count": len(columns),
                "execution_time_ms": result["execution_time_ms"],
                "db_duration_ms": round((time.time() - db_exec_start) * 1000, 2),
            })

            logger.info(
                " Query executed successfully: %d rows in %sms",
                result['row_count'],
                result['execution_time_ms'],
            )
            
            # Add thinking step
            if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
                state["llm_metadata"] = {}
            if "thinking_steps" not in state["llm_metadata"]:
                state["llm_metadata"]["thinking_steps"] = []
            state["llm_metadata"]["thinking_steps"].append({
                "content": f"Executed query successfully: {result['row_count']} rows returned in {result['execution_time_ms']}ms",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stage": "execute"
            })
            
            # Mark node as completed
            await update_node_history(state, "execute", "completed", thinking_steps=[
                {"id": "step-1", "content": f"Query executed: {result['row_count']} rows in {result['execution_time_ms']}ms", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ])
            
            return state

        # Failure path - use error normalizer for consistent error handling across databases
        logger.debug(f"Full MCP error result: {mcp_result}")
        
        # Normalize error using database-specific normalizer
        normalized_error = normalize_database_error(db_type, mcp_result)
        
        logger.error(
            f" MCP execution failed ({db_type}): Category={normalized_error.category.value}, "
            f"Code={normalized_error.error_code}, Message={normalized_error.message[:200]}"
        )
        
        # Use user-friendly message for state error
        error_msg = normalized_error.user_message
        
        # Add schema hints for invalid identifier errors
        if "invalid_identifier" in normalized_error.category.value.lower():
            import re
            from_match = re.search(r'FROM\s+(\w+)', state["sql_query"], re.IGNORECASE)
            if from_match:
                table_name = from_match.group(1).upper()
                schema_metadata = state.get("context", {}).get("schema_metadata")
                if schema_metadata:
                    tables = schema_metadata.get("tables", {})
                    if table_name in tables:
                        columns = [col["name"] for col in tables[table_name]]
                        error_msg += f"\n\nAvailable columns in {table_name}: {', '.join(columns)}"
                        error_msg += "\n\nPlease use one of these exact column names in your query."

        await set_state_error(state, "execute", error_msg, normalized_error.metadata)
        span["output"].update({
            "status": "error",
            "error": error_msg,
            "error_category": normalized_error.category.value,
            "error_code": normalized_error.error_code,
            "should_retry": normalized_error.retry_strategy.should_retry,
        })
        span["level"] = "ERROR"
        span["status_message"] = "mcp_execution_failed"

        if ExecState:
            await emit_state_event(state, ExecState.ERROR, {"error": error_msg})

        # Route to error (repair/fallback disabled)
        # Future: Use normalized_error.retry_strategy.should_retry to decide repair vs error
        state["next_action"] = "error"

        return state

    except Exception as e:
        logger.error(f"SQL execution failed: {e}")

        from app.utils.oracle_error_parser import parse_oracle_error

        error_context = parse_oracle_error(str(e))
        if error_context["error_code"]:
            message = (
                f"{error_context['title']}: {error_context['explanation']}. {error_context['suggestion']}"
            )
            await set_state_error(state, "execute", message, {"oracle_error": error_context})
            span["output"]["error_context"] = error_context
        else:
            await set_state_error(state, "execute", str(e))

        span["output"].setdefault("error", state.get("error"))
        span["level"] = "ERROR"
        span["status_message"] = "execution_exception"

        state["next_action"] = "error"
        return state
