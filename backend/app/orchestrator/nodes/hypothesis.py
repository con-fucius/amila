"""
Orchestrator Node: Hypothesis
"""

import logging
import time
import json
import re
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm, get_query_llm_provider, get_query_llm_model
from app.orchestrator.utils import emit_state_event, update_node_history, METRICS_AVAILABLE, record_llm_usage
from app.core.config import settings
from app.core.langfuse_client import log_generation

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)

HYPOTHESIS_CONFIDENCE = {"high", "medium", "low"}


def _extract_json_object(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else ""


def _validate_hypothesis_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Hypothesis payload is not an object")

    main_table = payload.get("main_table", "")
    if not isinstance(main_table, str):
        raise ValueError("main_table must be a string")

    def _list_of_strings(value):
        return isinstance(value, list) and all(isinstance(v, str) for v in value)

    additional_tables = payload.get("additional_tables", [])
    if not _list_of_strings(additional_tables):
        raise ValueError("additional_tables must be a list of strings")

    joins = payload.get("joins", [])
    if not isinstance(joins, list):
        raise ValueError("joins must be a list")

    filters = payload.get("filters", [])
    aggregations = payload.get("aggregations", [])
    group_by = payload.get("group_by", [])
    order_by = payload.get("order_by", [])
    for field_name, value in [
        ("filters", filters),
        ("aggregations", aggregations),
        ("group_by", group_by),
        ("order_by", order_by),
    ]:
        if not _list_of_strings(value):
            raise ValueError(f"{field_name} must be a list of strings")

    limit = payload.get("limit", None)
    if limit is not None and not isinstance(limit, int):
        raise ValueError("limit must be an int or null")

    confidence = payload.get("confidence", "low")
    if not isinstance(confidence, str) or confidence.lower() not in HYPOTHESIS_CONFIDENCE:
        raise ValueError("confidence must be high/medium/low")

    risks = payload.get("risks", [])
    if not _list_of_strings(risks):
        raise ValueError("risks must be a list of strings")

    return {
        "main_table": main_table,
        "additional_tables": additional_tables,
        "joins": joins,
        "filters": filters,
        "aggregations": aggregations,
        "group_by": group_by,
        "order_by": order_by,
        "limit": limit,
        "expected_output": payload.get("expected_output", ""),
        "grain": payload.get("grain", ""),
        "confidence": confidence.lower(),
        "risks": risks,
        "source": "llm",
    }


def _render_hypothesis_summary(hypothesis: dict) -> str:
    if not hypothesis:
        return "Hypothesis unavailable"
    summary_lines = [
        f"Main table: {hypothesis.get('main_table') or 'unknown'}",
        f"Additional tables: {', '.join(hypothesis.get('additional_tables') or []) or 'none'}",
        f"Filters: {', '.join(hypothesis.get('filters') or []) or 'none'}",
        f"Aggregations: {', '.join(hypothesis.get('aggregations') or []) or 'none'}",
        f"Group by: {', '.join(hypothesis.get('group_by') or []) or 'none'}",
        f"Order by: {', '.join(hypothesis.get('order_by') or []) or 'none'}",
        f"Limit: {hypothesis.get('limit') if hypothesis.get('limit') is not None else 'none'}",
        f"Confidence: {hypothesis.get('confidence')}",
    ]
    return "\n".join(summary_lines)


async def generate_hypothesis_node(state: QueryState) -> QueryState:
    """
    Node 2.5: Generate SQL hypothesis (query plan) before actual SQL generation
    
    Plans SQL approach:
    - Which tables to join
    - What filters to apply
    - What aggregations are needed
    - Expected result structure
    
    Outputs natural language plan + pseudo-SQL for transparency
    This enables human review before execution and improves error recovery
    """
    logger.info(f"Generating SQL hypothesis (query plan)...")
    state["current_stage"] = "generate_hypothesis"
    
    # Track node execution
    await update_node_history(state, "generate_hypothesis", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Planning query approach and strategy", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    # Stream lifecycle: hypothesis planning
    if ExecState:
        await emit_state_event(state, ExecState.PLANNING, {
            "thinking_steps": [
                {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-2", "content": "Retrieved schema context and samples", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-3", "content": "Planning query approach...", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
            ]
        })
    
    llm = get_llm()
    
    # Get enriched context
    context = state.get("context", {})
    schema_metadata = context.get("schema_metadata", {})
    enriched_schema = context.get("enriched_schema", {})
    sample_data = context.get("sample_data", {})
    
    # Build hypothesis prompt
    hypothesis_prompt = f"""
You are a SQL query planner. Analyze the user's request and schema, then create a structured query execution plan.

User Query: {state['user_query']}
Intent: {state.get('intent', '')}

Available Tables:
{str(list(schema_metadata.get('tables', {}).keys())[:30])}

Sample Data (first 2 rows per table):
{str(sample_data)[:2000]}

Your task: Create a detailed plan as STRICT JSON that captures:
1. Main table
2. Additional tables
3. Join conditions
4. Filters
5. Aggregations
6. Group by
7. Order by
8. Limit
9. Expected output
10. Grain
11. Confidence and risks

Return JSON only, no markdown, no commentary.

JSON schema:
{{
  "main_table": "table_name",
  "additional_tables": ["table_b"],
  "joins": [
    {{"left": "table_a.col", "right": "table_b.col", "type": "inner"}}
  ],
  "filters": ["table_a.status = 'ACTIVE'"],
  "aggregations": ["SUM(table_a.amount) AS total_amount"],
  "group_by": ["table_a.region"],
  "order_by": ["total_amount DESC"],
  "limit": 100,
  "expected_output": "One row per region with total amount",
  "grain": "region",
  "confidence": "high",
  "risks": []
}}
"""
    
    try:
        response = await llm.ainvoke([
            SystemMessage(content="You are a SQL query planning expert. Return JSON only."),
            HumanMessage(content=hypothesis_prompt)
        ])
        hypothesis_raw = response.content.strip()
        hypothesis_structured = None
        hypothesis_json = _extract_json_object(hypothesis_raw)
        if hypothesis_json:
            try:
                hypothesis_structured = _validate_hypothesis_payload(json.loads(hypothesis_json))
            except Exception as parse_err:
                logger.warning(f"Hypothesis JSON validation failed: {parse_err}")
                hypothesis_structured = None
        
        # Store hypothesis in state
        state["hypothesis_raw"] = hypothesis_raw
        if hypothesis_structured:
            state["hypothesis_structured"] = hypothesis_structured
            state["hypothesis"] = json.dumps(hypothesis_structured, separators=(",", ":"), ensure_ascii=True)
            summary = _render_hypothesis_summary(hypothesis_structured)
            state["messages"].append(AIMessage(content=f"Query Plan (structured):\n{summary}"))
        else:
            state["hypothesis_structured"] = {}
            state["hypothesis"] = hypothesis_raw
            state["messages"].append(AIMessage(content=f"Query Plan:\n{hypothesis_raw}"))
        state["next_action"] = "generate_sql"
        
        logger.info(f"Query hypothesis generated: {state['hypothesis'][:200]}...")

        trace_id = state.get("trace_id")
        if trace_id:
            try:
                log_generation(
                    trace_id=trace_id,
                    name="orchestrator.hypothesis.plan_generation",
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    input_data={
                        "user_query": state.get("user_query"),
                        "intent": state.get("intent"),
                    },
                    output_data={"hypothesis": state["hypothesis"]},
                    metadata={"stage": "hypothesis"},
                )
            except Exception:
                pass
        
        # Stream hypothesis success
        if ExecState:
            await emit_state_event(state, ExecState.PLANNING, {
                "thinking_steps": [
                    {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-2", "content": "Retrieved schema context and samples", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-3", "content": "Created query execution plan", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                    {"id": "step-4", "content": "Generating SQL from plan...", "status": "pending", "timestamp": datetime.now(timezone.utc).isoformat()}
                ],
                "hypothesis": state["hypothesis"][:500]
            })
        
        # Mark node as completed
        await update_node_history(state, "generate_hypothesis", "completed", thinking_steps=[
            {"id": "step-1", "content": "Created query execution plan", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state
    
    except Exception as e:
        logger.error(f"Hypothesis generation failed: {e}")
        # Don't fail workflow - proceed to SQL generation without hypothesis
        state["hypothesis"] = ""
        state["next_action"] = "generate_sql"
        logger.warning(f"Proceeding to SQL generation without hypothesis")
        
        # Mark node as completed (with warning)
        await update_node_history(state, "generate_hypothesis", "completed", thinking_steps=[
            {"id": "step-1", "content": "Hypothesis generation skipped, proceeding to SQL generation", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state
