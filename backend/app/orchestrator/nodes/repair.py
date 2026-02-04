"""
Orchestrator Node: Repair
"""

import logging
import time
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
    set_state_error,
)
from app.core.config import settings
from app.core.langfuse_client import log_generation

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

import re
import difflib

logger = logging.getLogger(__name__)

def calculate_sql_diff(before: str, after: str) -> str:
    """Calculate a character-level diff between two SQL strings"""
    if before == after:
        return ""
    
    # Use unified_diff for a clean representation
    diff_lines = list(difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile="Original",
        tofile="Repaired",
        n=0
    ))
    
    return "".join(diff_lines)


async def repair_sql_node(state: QueryState) -> QueryState:
    """Repair SQL with Langfuse instrumentation and structured errors."""

    state["current_stage"] = "repair_sql"

    async with langfuse_span(
        state,
        "orchestrator.repair",
        input_data={
            "error": state.get("error"),
            "sql_preview": (state.get("sql_query") or "")[:400],
        },
        metadata={"stage": "repair_sql"},
    ) as span:
        span.setdefault("output", {})
        result = await _repair_sql_node_inner(state, span)
        span["output"]["next_action"] = result.get("next_action")
        return result


async def _repair_sql_node_inner(state: QueryState, span: dict) -> QueryState:
    """
    Attempt to repair SQL using pattern-based fixes and incremental context loading
    
    Handles:
    - ORA-00942 (table does not exist) - expand schema context
    - ORA-00904 (invalid identifier) - quote identifiers, check schema
    - Common Oracle syntax errors - apply auto-fixes
    """
    logger.info(f"Attempting SQL auto-repair with pattern matching...")
    
    # Initialize repair trace
    if "repair_trace" not in state or not isinstance(state["repair_trace"], list):
        state["repair_trace"] = []
    
    # Track node execution for reasoning visibility
    from datetime import datetime, timezone
    await update_node_history(state, "repair_sql", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Analyzing SQL error and attempting repair", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])

    error_text = state.get("error", "")
    sql_query = state.get("sql_query", "")
    schema_context = (
        state.get("context", {}).get("enriched_schema")
        or state.get("context", {}).get("schema_metadata")
        or {}
    )

    span["output"].update({
        "error_text": error_text[:400],
        "has_schema_context": bool(schema_context),
    })

    import re

    if "ORA-00942" in error_text:
        logger.info(f"Detected ORA-00942 - attempting incremental context loading...")

        from_matches = re.findall(r'FROM\s+([\w.]+)', sql_query, re.IGNORECASE)
        join_matches = re.findall(r'JOIN\s+([\w.]+)', sql_query, re.IGNORECASE)
        missing_tables = list(set(from_matches + join_matches))

        span["output"]["missing_tables"] = missing_tables

        if missing_tables:
            logger.info(f"Expanding schema context to include: {missing_tables}")

            try:
                from app.services.schema_service import SchemaService

                expanded_schema_result = await SchemaService.get_database_schema(use_cache=False)

                if expanded_schema_result.get("status") == "success":
                    expanded_schema = expanded_schema_result.get("schema", {})

                    current_tables = state.get("context", {}).get("schema_metadata", {}).get("tables", {})
                    expanded_tables = expanded_schema.get("tables", {})

                    for table in missing_tables:
                        table_upper = table.upper().split('.')[-1]
                        if table_upper in expanded_tables:
                            current_tables[table_upper] = expanded_tables[table_upper]
                            logger.info(f"Added {table_upper} to schema context")

                    state.setdefault("context", {})
                    state["context"].setdefault("schema_metadata", {"tables": {}})
                    state["context"]["schema_metadata"]["tables"] = current_tables

                    schema_context = state["context"]["schema_metadata"]
                    span["output"]["schema_expanded"] = True

            except Exception as e:
                logger.warning(f"Failed to expand schema context: {e}")
                span["output"]["schema_expand_error"] = str(e)
        
        state["repair_trace"].append({
            "type": "schema_expansion",
            "error": "ORA-00942",
            "action": f"Expanded schema context to check for missing tables: {missing_tables}",
            "before_sql": sql_query,
            "after_sql": sql_query,
            "diff": "",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        # Apply fixes
        sql_before = sql_query
        reserved_words = ['DATE', 'USER', 'LEVEL', 'SIZE', 'ACCESS', 'FILE', 'SESSION']
        for word in reserved_words:
            sql_query = re.sub(rf'\b{word}\b(?!\")', f'"{word}"', sql_query, flags=re.IGNORECASE)

        user_schema = settings.oracle_username if hasattr(settings, 'oracle_username') else None
        if user_schema:
            sql_query = re.sub(
                r'FROM\s+(\w+)(?!\.)',
                f'FROM {user_schema}.\\1',
                sql_query,
                flags=re.IGNORECASE
            )

        state["sql_query"] = sql_query
        state["repair_attempts"] = state.get("repair_attempts", 0) + 1
        state["next_action"] = "validate"
        span["output"].update({
            "repair_type": "identifier_quote",
            "next_action": "validate",
        })
        
        state["repair_trace"].append({
            "type": "identifier_fix",
            "error": "ORA-00904",
            "action": "Applied double-quoting to reserved words and identifiers",
            "before_sql": sql_before,
            "after_sql": sql_query,
            "diff": calculate_sql_diff(sql_before, sql_query),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        logger.info(f"Applied identifier auto-fixes; returning to validation")
        return state

    elif "ORA-00933" in error_text:
        logger.info(f"Detected ORA-00933 - fixing command termination...")
        sql_before = sql_query
        sql_query = sql_query.rstrip(';').strip()
        state["sql_query"] = sql_query
        state["repair_attempts"] = state.get("repair_attempts", 0) + 1
        state["next_action"] = "validate"
        span["output"].update({
            "repair_type": "command_termination",
            "next_action": "validate",
        })
        state["repair_trace"].append({
            "type": "command_termination",
            "error": "ORA-00933",
            "action": "Removed trailing semicolon for Oracle execution",
            "before_sql": sql_before,
            "after_sql": sql_query,
            "diff": calculate_sql_diff(sql_before, sql_query),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return state

    logger.info(f"Using LLM for generic SQL repair...")
    llm = get_llm()
    db_type = (state.get("database_type") or "oracle").lower()
    db_name = "PostgreSQL" if db_type in ["postgres", "postgresql"] else ("Doris" if db_type == "doris" else "Oracle")
    
    # Retrieve relevant lessons from ReflectiveMemoryService
    lessons_section = ""
    try:
        from app.services.reflective_memory_service import ReflectiveMemoryService
        
        lessons = await ReflectiveMemoryService.get_relevant_lessons(
            query=state.get("user_query", ""),
            limit=3
        )
        
        if lessons:
            lessons_section = ReflectiveMemoryService.format_lessons_for_prompt(lessons)
            logger.info(f"Injected {len(lessons)} reflective lessons into repair prompt")
            span["output"]["reflective_lessons_count"] = len(lessons)
    except Exception as e:
        logger.warning(f"Failed to retrieve reflective lessons: {e}")
    
    repair_prompt = f"""
You are an {db_name} SQL specialist. The previous SQL failed with this error:
---
{error_text}
---
Original user request:
{state.get('user_query','')}

Database schema (use only these tables/columns):
{str(schema_context)[:4000]}

{lessons_section}

Repair the SQL to a valid {db_name} query that fulfills the user request. Rules:
- Prefer a single WITH (CTE) query as needed; ensure each CTE has a full SELECT body.
- Use {"TO_CHAR(date_column, 'Q')" if db_type in ["postgres", "postgresql", "doris", "oracle"] else "appropriate quarter function"} for quarters.
- If table not found, check if it exists in schema above and use exact name.
- Quote reserved words like DATE, USER, LEVEL.
- Use {"LIMIT n (NOT FETCH FIRST)" if db_type in ["postgres", "postgresql", "doris"] else "FETCH FIRST n ROWS ONLY"} for row limiting.
- Return ONLY the SQL. No markdown, no commentary.
"""
    
    try:
        response = await llm.ainvoke([
            SystemMessage(content="Return only the final SQL without code fences."),
            HumanMessage(content=repair_prompt),
        ])
        repaired_sql = (response.content or "").strip()

        if repaired_sql.startswith('```'):
            parts = repaired_sql.split('```')
            if len(parts) >= 3:
                repaired_sql = parts[1].strip()
            else:
                repaired_sql = repaired_sql.replace('```', '').strip()

        state["sql_query"] = repaired_sql
        state["repair_attempts"] = state.get("repair_attempts", 0) + 1
        state["next_action"] = "validate"

        trace_id = state.get("trace_id")
        if trace_id:
            try:
                log_generation(
                    trace_id=trace_id,
                    name="orchestrator.repair.sql_repair",
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    input_data={
                        "user_query": state.get("user_query"),
                        "error": error_text,
                    },
                    output_data={"sql": repaired_sql[:1000]},
                    metadata={"stage": "repair_sql"},
                )
            except Exception:
                pass

        if METRICS_AVAILABLE and repair_attempts:
            repair_attempts.labels(success="true").inc()

        span["output"].update({
            "repair_type": "llm",
            "next_action": "validate",
            "repaired_sql_preview": repaired_sql[:300],
        })

        state["repair_trace"].append({
            "type": "llm_repair",
            "error": error_text[:100],
            "action": "Generated corrected SQL using LLM with reflective lessons",
            "before_sql": sql_query,
            "after_sql": repaired_sql,
            "diff": calculate_sql_diff(sql_query, repaired_sql),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        logger.info(f"LLM auto-repair produced new SQL; returning to validation")
        
        # Record successful repair as a lesson in ReflectiveMemoryService
        try:
            from app.services.reflective_memory_service import ReflectiveMemoryService
            import hashlib
            
            # Generate schema fingerprint from context
            schema_fingerprint = hashlib.sha256(
                str(schema_context.get("tables", {}).keys()).encode()
            ).hexdigest()[:16]
            
            await ReflectiveMemoryService.record_repair_success(
                original_query=state.get("user_query", ""),
                failed_sql=sql_query,
                repaired_sql=repaired_sql,
                schema_fingerprint=schema_fingerprint,
                repair_type="llm_auto_repair",
                error_message=error_text[:500]
            )
            logger.info("Recorded successful repair as reflective lesson")
            span["output"]["lesson_recorded"] = True
        except Exception as e:
            logger.warning(f"Failed to record repair lesson: {e}")
        
        await update_node_history(state, "repair_sql", "completed", thinking_steps=[
            {"id": "step-1", "content": f"SQL repaired using LLM (attempt {state['repair_attempts']})", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state

    except Exception as e:
        if METRICS_AVAILABLE and repair_attempts:
            repair_attempts.labels(success="false").inc()
        logger.error(f"SQL repair failed: {e}")
        await set_state_error(state, "repair_sql", f"SQL repair failed: {e}")
        span["output"].update({
            "repair_type": "llm",
            "error": str(e),
        })
        span["level"] = "ERROR"
        span["status_message"] = "repair_failed"
        state["next_action"] = "error"
        
        await update_node_history(state, "repair_sql", "failed", error=str(e))
        
        return state
