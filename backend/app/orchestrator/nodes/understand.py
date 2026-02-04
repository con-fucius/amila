"""
Orchestrator Node: Understand
"""

import logging
import time
import json
import re
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import (
    get_llm,
    get_query_llm_provider,
    get_query_llm_model,
)
from app.orchestrator.utils import (
    emit_state_event,
    langfuse_span,
    METRICS_AVAILABLE,
    record_llm_usage,
    set_state_error,
    update_node_history,
)
from app.core.config import settings
from app.core.langfuse_client import log_generation
from app.core.security_middleware import input_sanitizer
from app.services.query_taxonomy_classifier import QueryTaxonomyClassifier
from app.models.intent_models import (
    StructuredIntent,
    taxonomy_to_structured_intent,
    structured_intent_to_taxonomy,
    IntentUnderstandingResult,
)

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


TAXONOMY_SCHEMA = {
    "query_type": {
        "select",
        "aggregation",
        "filtered",
        "joined",
        "time-series",
        "ranked",
        "comparative",
        "nested",
    },
    "complexity": {"simple", "medium", "complex"},
    "domain": {"sales", "finance", "operations", "hr", "marketing", "general"},
    "temporal": {"point_in_time", "period", "trailing", "ytd", "mtd", "none"},
    "expected_cardinality": {"single", "few", "many", "summary"},
}


def _extract_json_object(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else ""


def _build_fallback_taxonomy(user_query: str) -> dict:
    classification = QueryTaxonomyClassifier.classify(user_query or "")
    complexity = "simple"
    if classification.complexity_score >= 0.7:
        complexity = "complex"
    elif classification.complexity_score >= 0.4:
        complexity = "medium"

    if classification.primary_type.value in [
        "aggregate",
        "trend_analysis",
        "comparative",
    ]:
        expected_cardinality = "summary"
    else:
        expected_cardinality = "few"

    return {
        "query_type": "select",
        "complexity": complexity,
        "domain": "general",
        "temporal": "none",
        "expected_cardinality": expected_cardinality,
        "tables": [],
        "entities": [],
        "time_period": "",
        "aggregations": [],
        "filters": [],
        "joins_count": 0,
        "source": "fallback",
        "classifier_primary": classification.primary_type.value,
        "classifier_strategy": classification.recommended_strategy.value,
    }


def _validate_taxonomy_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Taxonomy payload is not an object")

    normalized = {}
    for key, allowed in TAXONOMY_SCHEMA.items():
        value = payload.get(key)
        if not isinstance(value, str) or value not in allowed:
            raise ValueError(f"Invalid taxonomy field: {key}")
        normalized[key] = value

    normalized["tables"] = payload.get("tables", [])
    normalized["entities"] = payload.get("entities", [])
    normalized["time_period"] = payload.get("time_period", "")
    normalized["aggregations"] = payload.get("aggregations", [])
    normalized["filters"] = payload.get("filters", [])
    joins_count = payload.get("joins_count", 0)
    if not isinstance(joins_count, int) or joins_count < 0:
        raise ValueError("Invalid joins_count")
    normalized["joins_count"] = joins_count
    normalized["source"] = "llm"
    return normalized


async def understand_query_node(state: QueryState) -> QueryState:
    """
    Node 1: Understand user intent from natural language WITH SECURITY SAFEGUARDS

    Classifies query type:
    - read-only (SELECT)
    - aggregation (COUNT, SUM, AVG)
    - filtered (WHERE clauses)
    - joined (multi-table)
    - time-series (date ranges)

    Security: Includes prompt injection protection system instructions
    """
    logger.info(f"Understanding query: {state['user_query'][:50]}...")
    state["current_stage"] = "understand"

    # Stream lifecycle: planning with thinking step
    # Stream lifecycle: planning with thinking step
    await update_node_history(
        state,
        "understand",
        "in-progress",
        thinking_steps=[
            {
                "id": "step-1",
                "content": "Analyzing user query intent",
                "status": "in-progress",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ],
    )

    llm = get_llm()

    # Get security system prompt prefix
    from app.core.security_middleware import input_sanitizer

    security_prefix = input_sanitizer.get_safe_system_prompt_prefix()

    # Fix 10: Enhanced query taxonomy classification
    system_prompt = f"""{security_prefix}

You are a database query intent classifier for business intelligence.
Analyze the user's natural language query and provide detailed taxonomy classification.

Classify the query across multiple dimensions:

1. Query Type (what operation):
   - select: Simple data retrieval
   - aggregation: COUNT, SUM, AVG, MIN, MAX, GROUP BY
   - filtered: WHERE conditions
   - joined: Multi-table joins
   - time-series: Date/time-based analysis
   - ranked: ORDER BY with LIMIT
   - comparative: Comparing values across groups
   - nested: Subqueries or CTEs

2. Complexity Level (estimated difficulty):
   - simple: Single table, no joins
   - medium: 2-3 tables with joins
   - complex: Multiple joins, subqueries, window functions

3. Business Domain (likely area):
   - sales: Revenue, orders, customers
   - finance: Budgets, costs, P&L
   - operations: Inventory, logistics, supply chain
   - hr: Employees, payroll, performance
   - marketing: Campaigns, leads, conversions
   - general: Cross-domain or unclear

4. Temporal Characteristics:
   - point_in_time: Specific date
   - period: Date range
   - trailing: Last N days/weeks/months
   - ytd: Year-to-date
   - mtd: Month-to-date
   - none: No time component

5. Expected Cardinality:
   - single: One row result
   - few: 2-100 rows
   - many: 100+ rows
   - summary: Aggregated to few rows

Respond in JSON format:
Return JSON only, no markdown, no commentary.
{{
  "query_type": "aggregation",
  "complexity": "medium",
  "domain": "sales",
  "temporal": "trailing",
  "expected_cardinality": "summary",
  "tables": ["employees", "departments"],
  "entities": ["salary", "department_name"],
  "time_period": "last_year",
  "aggregations": ["sum", "count"],
  "filters": ["department = 'Sales'"],
  "joins_count": 1
}}
"""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Query: {state['user_query']}"),
    ]

    async with langfuse_span(
        state,
        "orchestrator.understand",
        input_data={"user_query": state.get("user_query")},
        metadata={"stage": "understand"},
    ) as span:
        try:
            import time

            start_time = time.time()

            response = await llm.ainvoke(messages)
            intent_raw = response.content
            span["output"]["intent"] = intent_raw

            # Record LLM usage metrics
            if METRICS_AVAILABLE:
                latency = time.time() - start_time
                prompt_content = system_prompt + state["user_query"]
                record_llm_usage(
                    provider=get_query_llm_provider(),
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    prompt_tokens=len(prompt_content) // 4,  # Rough token estimate
                    completion_tokens=len(str(intent_raw)) // 4,
                    status="success",
                    latency=latency,
                )
            trace_id = state.get("trace_id")
            if trace_id:
                try:
                    log_generation(
                        trace_id=trace_id,
                        name="orchestrator.understand.intent_classification",
                        model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                        input_data={
                            "user_query": state.get("user_query"),
                        },
                        output_data={"intent": intent_raw},
                        metadata={"stage": "understand"},
                    )
                except Exception:
                    pass

            taxonomy = None
            intent_json = _extract_json_object(intent_raw)
            if intent_json:
                try:
                    taxonomy = _validate_taxonomy_payload(json.loads(intent_json))
                except Exception as parse_err:
                    logger.warning(f"Taxonomy JSON validation failed: {parse_err}")
                    taxonomy = None

            if taxonomy is None:
                taxonomy = _build_fallback_taxonomy(state.get("user_query", ""))

            state["intent_structured"] = taxonomy
            state["query_taxonomy"] = taxonomy
            state["intent"] = json.dumps(
                taxonomy, separators=(",", ":"), ensure_ascii=True
            )
            state["intent_source"] = taxonomy.get("source", "fallback")

            # NEW: Create structured intent using Pydantic model for canonical representation
            structured_intent = taxonomy_to_structured_intent(taxonomy)
            state["structured_intent"] = structured_intent.model_dump()
            state["intent_confidence"] = structured_intent.confidence

            # Create complete understanding result for observability
            understanding_result = IntentUnderstandingResult(
                structured_intent=structured_intent,
                raw_intent=intent_raw if "intent_raw" in locals() else None,
                intent_source=state["intent_source"],
                processing_time_ms=int((time.time() - start_time) * 1000)
                if "start_time" in locals()
                else None,
            )

            logger.info(
                "Query taxonomy: type=%s, complexity=%s, domain=%s, source=%s, confidence=%.2f",
                taxonomy.get("query_type"),
                taxonomy.get("complexity"),
                taxonomy.get("domain"),
                state["intent_source"],
                structured_intent.confidence,
            )
            state["intent_source"] = taxonomy.get("source", "fallback")

            logger.info(
                "Query taxonomy: type=%s, complexity=%s, domain=%s, source=%s",
                taxonomy.get("query_type"),
                taxonomy.get("complexity"),
                taxonomy.get("domain"),
                state["intent_source"],
            )

            state["messages"].append(
                AIMessage(
                    content=(
                        f"Intent understood: type={taxonomy.get('query_type')}, "
                        f"complexity={taxonomy.get('complexity')}, "
                        f"domain={taxonomy.get('domain')}, "
                        f"temporal={taxonomy.get('temporal')}"
                    )
                )
            )
            state["next_action"] = "retrieve_context"

            # Add thinking step
            if "llm_metadata" not in state or not isinstance(
                state["llm_metadata"], dict
            ):
                state["llm_metadata"] = {}
            if "thinking_steps" not in state["llm_metadata"]:
                state["llm_metadata"]["thinking_steps"] = []
            state["llm_metadata"]["thinking_steps"].append(
                {
                    "content": f"Understood query intent: {state['intent'][:200]}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "stage": "understand",
                }
            )

            logger.info(f"Intent classified: {state['intent'][:100]}...")

            # Complete node tracking
            await update_node_history(
                state,
                "understand",
                "completed",
                thinking_steps=state["llm_metadata"]["thinking_steps"],
            )

            return state

        except Exception as e:
            logger.error(f"Intent understanding failed: {e}")
            span["output"]["error"] = str(e)
            span["level"] = "ERROR"
            span["status_message"] = "intent_classification_failed"
            await set_state_error(state, "understand", str(e))
            state["next_action"] = "error"

            # Record failed LLM call
            if METRICS_AVAILABLE:
                record_llm_usage(
                    provider=get_query_llm_provider(),
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    status="error",
                )
            return state
