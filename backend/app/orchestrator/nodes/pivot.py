"""
Orchestrator Node: Pivot
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


async def pivot_strategy_node(state: QueryState) -> QueryState:
    """
    Node 7: Strategy Pivoting
    
    Triggered when result validation fails or query returns unexpected results.
    Suggests alternative approaches:
    - Broader filters
    - Different tables
    - Aggregation level changes
    - Alternative join strategies
    
    Limits to 2 pivots max to avoid infinite loops
    """
    logger.info(f"Generating alternative query strategy...")
    state["current_stage"] = "pivot_strategy"
    
    # Track node execution for reasoning visibility
    await update_node_history(state, "pivot_strategy", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Analyzing query results and generating alternative strategy", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    pivot_attempts = state.get("pivot_attempts", 0)
    
    if pivot_attempts >= 2:
        logger.warning(f"Max pivot attempts reached (2), proceeding to format results")
        state["next_action"] = "format_results"
        
        await update_node_history(state, "pivot_strategy", "completed", thinking_steps=[
            {"id": "step-1", "content": "Max pivot attempts reached, proceeding with current results", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state
    
    # Increment pivot counter
    state["pivot_attempts"] = pivot_attempts + 1
    
    llm = get_llm()
    
    # Gather context for pivot
    original_query = state.get("user_query", "")
    current_sql = state.get("sql_query", "")
    validation_report = state.get("result_analysis", {})
    hypothesis = state.get("hypothesis", "")
    
    # Build pivot prompt
    pivot_prompt = f"""
You are a SQL optimization expert. The previous query strategy didn't work well.

**Original User Query:** {original_query}

**Previous Hypothesis/Plan:**
{hypothesis}

**Generated SQL:**
{current_sql[:500]}

**Validation Issues:**
{validation_report}

**Task:** Suggest an alternative query strategy to address these issues.

Provide:
1. **Root Cause Analysis:** Why did the previous approach fail?
2. **Alternative Strategy:** What different approach should we try?
3. **Specific Changes:**
   - Different tables to use?
   - Different filters?
   - Different aggregation level?
   - Different join strategy?

Format as:

**Analysis:**
<your analysis>

**Alternative Strategy:**
<high-level strategy>

**Specific Recommendations:**
- Recommendation 1
- Recommendation 2
- Recommendation 3
"""
    
    try:
        response = await llm.ainvoke([SystemMessage(content="You are a SQL troubleshooting expert."), HumanMessage(content=pivot_prompt)])
        pivot_strategy = response.content.strip()
        
        logger.info(f"Alternative strategy generated (attempt {state['pivot_attempts']})")
        
        # Store pivot strategy
        if "pivot_strategies" not in state:
            state["pivot_strategies"] = []
        state["pivot_strategies"].append(pivot_strategy)

        trace_id = state.get("trace_id")
        if trace_id:
            try:
                log_generation(
                    trace_id=trace_id,
                    name="orchestrator.pivot.strategy_generation",
                    model=get_query_llm_model(settings.GRAPHITI_LLM_MODEL),
                    input_data={
                        "user_query": original_query,
                        "current_sql": current_sql,
                        "validation_report": validation_report,
                    },
                    output_data={"strategy": pivot_strategy},
                    metadata={"stage": "pivot"},
                )
            except Exception:
                pass
        
        # Re-enter pipeline at hypothesis generation with new strategy context
        state["next_action"] = "generate_hypothesis"
        state["messages"].append(AIMessage(content=f" Alternative Strategy (Attempt {state['pivot_attempts']}):\n{pivot_strategy}"))
        
        logger.info(f"Re-entering pipeline at hypothesis generation with alternative strategy")
        
        await update_node_history(state, "pivot_strategy", "completed", thinking_steps=[
            {"id": "step-1", "content": f"Generated alternative strategy (attempt {state['pivot_attempts']})", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state
    
    except Exception as e:
        logger.error(f"Strategy pivoting failed: {e}")
        # Don't fail - just proceed to format results with what we have
        state["next_action"] = "format_results"
        
        await update_node_history(state, "pivot_strategy", "completed", thinking_steps=[
            {"id": "step-1", "content": "Strategy pivoting failed, proceeding with current results", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        
        return state
