"""
Orchestrator Node: Hypothesis
"""

import logging
import time
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
You are a SQL query planner. Analyze the user's request and schema, then create a query execution plan.

User Query: {state['user_query']}
Intent: {state.get('intent', '')}

Available Tables:
{str(list(schema_metadata.get('tables', {}).keys())[:30])}

Sample Data (first 2 rows per table):
{str(sample_data)[:2000]}

Your task: Create a detailed query plan that explains:
1. Which table(s) to query
2. What columns/calculations are needed
3. What filters to apply
4. What aggregations are needed
5. What JOINs are required (if any)
6. Expected result structure

Format your response as:

**Query Plan:**
- Main table: <table_name>
- Additional tables: <table_names if joins needed>
- Key columns: <list of columns>
- Filters: <WHERE conditions>
- Aggregations: <GROUP BY, SUM, AVG, etc>
- Expected output: <description of result structure>

**Pseudo-SQL:**
```
SELECT <columns>
FROM <table>
[JOIN conditions]
WHERE <filters>
GROUP BY <if needed>
ORDER BY <if needed>
```

**Confidence:** <HIGH/MEDIUM/LOW>
**Risks:** <any potential issues or ambiguities>
"""
    
    try:
        response = await llm.ainvoke([SystemMessage(content="You are a SQL query planning expert."), HumanMessage(content=hypothesis_prompt)])
        hypothesis = response.content.strip()
        
        # Store hypothesis in state
        state["hypothesis"] = hypothesis
        state["messages"].append(AIMessage(content=f"Query Plan:\n{hypothesis}"))
        state["next_action"] = "generate_sql"
        
        logger.info(f"Query hypothesis generated: {hypothesis[:200]}...")

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
                    output_data={"hypothesis": hypothesis},
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
                "hypothesis": hypothesis[:500]
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
