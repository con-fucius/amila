"""
Orchestrator Node: Fallback
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

logger = logging.getLogger(__name__)


async def generate_fallback_sql_node(state: QueryState) -> QueryState:
    """Generate a simpler fallback SQL variant under strict schema rules."""

    state["current_stage"] = "fallback_sql"

    async with langfuse_span(
        state,
        "orchestrator.fallback_sql",
        input_data={
            "user_query": state.get("user_query"),
            "previous_error": state.get("error"),
        },
        metadata={"stage": "fallback_sql"},
    ) as span:
        span.setdefault("output", {})
        result = await _generate_fallback_sql_node_inner(state, span)
        span["output"]["next_action"] = result.get("next_action")
        return result


async def _generate_fallback_sql_node_inner(state: QueryState, span: dict) -> QueryState:
    """Fallback generation helper with rich logging and error propagation."""

    # Track node execution for reasoning visibility
    await update_node_history(state, "fallback_sql", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Generating simpler fallback SQL query", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])

    try:
        state["fallback_attempts"] = state.get("fallback_attempts", 0) + 1
        span["output"]["attempt"] = state["fallback_attempts"]

        llm = get_llm()

        logger.warning(f"Generating fallback SQL with strict schema validation")

        context = state.get("context", {})
        schema_metadata = context.get("schema_metadata", {})
        user_query_upper = state.get('user_query', '').upper()

        schema_info = "\n\n" + "" * 100 + "\n"
        schema_info += " MANDATORY SCHEMA CONSTRAINTS - COLUMN NAMES ARE STRICT \n"
        schema_info += "" * 100 + "\n\n"

        if schema_metadata and isinstance(schema_metadata, dict):
            tables = schema_metadata.get("tables", {})

            for table_name, columns in tables.items():
                schema_info += f" TABLE: {table_name}\n"
                schema_info += "    EXACT COLUMN NAMES YOU MUST USE:\n"
                for col in columns:
                    schema_info += f"       {col['name']} ({col['type']})\n"
                schema_info += "\n"

            schema_info += "\n" + "" * 100 + "\n"
            schema_info += " ABSOLUTE RULES:\n"
            schema_info += "" * 100 + "\n"
            schema_info += "1. USE ONLY THE EXACT COLUMN NAMES LISTED ABOVE\n"
            schema_info += "2. DO NOT invent: SERVICE_DATE, DAY_OF_MONTH, MONTH_NAME, YEAR, DATE_KEY\n"
            schema_info += "3. IF joining CUSTOMER_DATA with REF_DATE, use: CD.\"DATE\" = RD.ID_DATE (QUOTE \"DATE\"!)\n"
            schema_info += "4. FOR month filtering use: DS_MNTH = 'JANUARY' (UPPERCASE - check sample data for format)\n"
            schema_info += "5. FOR year filtering use: DS_YEAR = '2025' (check sample data for format)\n"
            schema_info += "6. FOR day-of-month display use: DT_DAY (NOT DAY_OF_MONTH)\n"
            schema_info += "7.  ALWAYS quote DATE as \"DATE\" - it's a reserved word in Oracle!\n"
            schema_info += "" * 100 + "\n\n"

            if 'REF_DATE' in user_query_upper:
                schema_info += "\n" + "" * 100 + "\n"
                schema_info += " CORRECT PATTERN FOR REF_DATE JOINS:\n"
                schema_info += "" * 100 + "\n"
                schema_info += "```sql\n"
                schema_info += "SELECT \n"
                schema_info += "  RD.DT_DAY,  --  Day of month (1-31)\n"
                schema_info += "  RD.DS_MNTH, --  Month name ('JANUARY', 'FEBRUARY', etc.)\n"
                schema_info += "  RD.DS_YEAR, --  Year as string ('2025')\n"
                schema_info += "  SUM(CD.USED_RESOURCES) AS total\n"
                schema_info += "FROM CUSTOMER_DATA CD\n"
                schema_info += "JOIN REF_DATE RD ON TO_CHAR(TO_DATE(CD.\"DATE\", 'DD/MM/YYYY'), 'YYYYMMDD') = RD.ID_DATE\n"
                schema_info += "WHERE RD.DS_MNTH = 'JANUARY' -- UPPERCASE\n"
                schema_info += "  AND RD.DS_YEAR = '2025'\n"
                schema_info += "GROUP BY RD.DT_DAY, RD.DS_MNTH, RD.DS_YEAR\n"
                schema_info += "ORDER BY total DESC\n"
                schema_info += "FETCH FIRST 5 ROWS ONLY;\n"
                schema_info += "```\n"
                schema_info += "" * 100 + "\n\n"

        prompt = f"""You are generating a SIMPLER fallback SQL query after a previous attempt failed.

{schema_info}

FALLBACK STRATEGY:
1. SIMPLIFY the query logic (fewer JOINs, simpler WHERE clauses)
2. Use FETCH FIRST 100 ROWS ONLY (not 1000)
3. Prefer direct column references over complex expressions
4.  CRITICAL: Still use ONLY exact column names from schema above

Original user request: {state.get('user_query', '')}
Original hypothesis: {state.get('hypothesis', '')[:500]}

Previous error: {state.get('error', '')[:300]}

Generate a SIMPLER Oracle SQL query that:
- Uses the EXACT column names from the schema
- Simplifies the logic
- Still answers the user's question

Return ONLY the SQL query, no explanations.
"""

        resp = await llm.ainvoke([
            SystemMessage(content="You are an Oracle SQL expert. Return only SQL without code fences or explanations."),
            HumanMessage(content=prompt)
        ])

        sql = (resp.content or "").strip()

        if sql.startswith("```"):
            sql = sql.split("\n", 1)[1].rsplit("\n```", 1)[0].strip()

        logger.info(f"Fallback SQL generated: {sql[:200]}...")

        state["sql_query"] = sql
        state["next_action"] = "validate"

        trace_id = state.get("trace_id")
        if trace_id:
            try:
                log_generation(
                    trace_id=trace_id,
                    name="orchestrator.fallback_sql.sql_generation",
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    input_data={
                        "user_query": state.get("user_query"),
                        "previous_error": state.get("error"),
                    },
                    output_data={"sql": sql[:1000]},
                    metadata={"stage": "fallback_sql"},
                )
            except Exception:
                pass

        span["output"].update({
            "status": "success",
            "sql_preview": sql[:300],
        })
        
        await update_node_history(state, "fallback_sql", "completed", thinking_steps=[
            {"id": "step-1", "content": f"Fallback SQL generated (attempt {state['fallback_attempts']})", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state

    except Exception as e:
        logger.error(f"Fallback SQL generation failed: {e}")
        await set_state_error(state, "fallback_sql", f"Fallback SQL generation error: {str(e)}")
        state["next_action"] = "error"
        span["output"].update({
            "status": "error",
            "error": str(e),
        })
        span["level"] = "ERROR"
        span["status_message"] = "fallback_generation_failed"
        
        await update_node_history(state, "fallback_sql", "failed", error=str(e))
        
        return state
