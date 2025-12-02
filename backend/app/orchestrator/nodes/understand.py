"""
Orchestrator Node: Understand
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
    METRICS_AVAILABLE,
    record_llm_usage,
    set_state_error,
    update_node_history,
)
from app.core.config import settings
from app.core.langfuse_client import log_generation
from app.core.security_middleware import input_sanitizer

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


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
    await update_node_history(state, "understand", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Analyzing user query intent", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    llm = get_llm()
    
    # Get security system prompt prefix
    from app.core.security_middleware import input_sanitizer
    security_prefix = input_sanitizer.get_safe_system_prompt_prefix()
    
    # System prompt for intent classification with security safeguards
    system_prompt = f"""{security_prefix}

You are a database query intent classifier for business intelligence.
Analyze the user's natural language query and determine:
1. Query type (select, aggregation, filtered, joined, time-series)
2. Tables likely needed
3. Key entities mentioned
4. Time period if any

Respond in JSON format:
{{
  "query_type": "aggregation",
  "tables": ["employees", "departments"],
  "entities": ["salary", "department_name"],
  "time_period": "last_year"
}}
"""
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Query: {state['user_query']}")
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
            intent = response.content
            span["output"]["intent"] = intent
            
            # Record LLM usage metrics
            if METRICS_AVAILABLE:
                latency = time.time() - start_time
                prompt_content = system_prompt + state['user_query']
                record_llm_usage(
                    provider=get_query_llm_provider(),
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    prompt_tokens=len(prompt_content) // 4,  # Rough token estimate
                    completion_tokens=len(str(intent)) // 4,
                    status="success",
                    latency=latency
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
                        output_data={"intent": intent},
                        metadata={"stage": "understand"},
                    )
                except Exception:
                    pass
            
            state["intent"] = intent
            state["messages"].append(AIMessage(content=f"Intent understood: {intent}"))
            state["next_action"] = "retrieve_context"
            
            # Add thinking step
            if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
                state["llm_metadata"] = {}
            if "thinking_steps" not in state["llm_metadata"]:
                state["llm_metadata"]["thinking_steps"] = []
            state["llm_metadata"]["thinking_steps"].append({
                "content": f"Understood query intent: {intent[:200]}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stage": "understand"
            })
            
            logger.info(f"Intent classified: {intent[:100]}...")
            
            # Complete node tracking
            await update_node_history(state, "understand", "completed", thinking_steps=state["llm_metadata"]["thinking_steps"])
            
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
                    status="error"
                )
            return state
