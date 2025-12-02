"""
Orchestrator Node: Decompose
"""

import logging
import time
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm, get_query_llm_provider, get_query_llm_model
from app.orchestrator.utils import emit_state_event, update_node_history, METRICS_AVAILABLE, record_llm_usage
from app.core.config import settings

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


async def decompose_query_node(state: QueryState) -> QueryState:
    """
    Multi-Query Decomposition
    
    Detects multi-part queries and decomposes them into sub-queries with DAG execution.
    If decomposition is triggered, executes sub-queries sequentially and combines results.
    
    Examples of multi-part queries:
    - "Show me sales by region, then compare with last year"
    - "What are top customers? Also show their recent orders"
    - "Compare Q1 vs Q2 revenue"
    """
    logger.info(f"Checking if query needs decomposition...")
    state["current_stage"] = "decompose_query"
    
    # Track node execution
    await update_node_history(state, "decompose_query", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Checking if query needs decomposition", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    try:
        from app.services.multi_query_decomposition_service import MultiQueryDecompositionService
        
        # Check if query should be decomposed
        user_query = state.get("user_query", "")
        intent = state.get("intent", "")
        
        # Extract complexity hint from intent if available (rough heuristic)
        complexity = 50  # default
        if "complexity" in intent.lower():
            if "high" in intent.lower():
                complexity = 75
            elif "low" in intent.lower():
                complexity = 25
        
        should_decompose = MultiQueryDecompositionService.should_decompose(
            user_query=user_query,
            intent=intent,
            complexity=complexity
        )
        
        if not should_decompose:
            logger.info(f"Query does not need decomposition, proceeding normally")
            state["next_action"] = "generate_hypothesis"
            await update_node_history(state, "decompose_query", "completed", thinking_steps=[
                {"id": "step-1", "content": "Query is simple, no decomposition needed", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ])
            return state
        
        # Detect multi-part structure
        is_multi, parts = MultiQueryDecompositionService.detect_multi_part_query(user_query, intent)
        
        if not is_multi or len(parts) < 2:
            logger.info(f"Query structure is simple, proceeding normally")
            state["next_action"] = "generate_hypothesis"
            await update_node_history(state, "decompose_query", "completed", thinking_steps=[
                {"id": "step-1", "content": "Query structure is simple, no decomposition needed", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ])
            return state
        
        logger.info(f"Query decomposed into {len(parts)} sub-queries")
        
        # Build DAG structure
        dag = MultiQueryDecompositionService.build_query_dag(
            user_query=user_query,
            parts=parts,
            context=state.get("context", {})
        )
        
        # Store DAG in state
        state["query_dag"] = dag
        
        # Stream lifecycle: decomposition detected
        if ExecState:
            await emit_state_event(state, ExecState.PLANNING, {
                "thinking_steps": [
                    {"id": "decomp-1", "content": f"Query decomposed into {len(parts)} parts", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
                ],
                "discoveries": {
                    "decomposition": True,
                    "sub_query_count": len(parts),
                    "parts": parts[:3]  # Show first 3 parts
                }
            })
        
        # Execute DAG with sub-queries
        logger.info(f"Executing multi-query DAG...")
        
        # Create wrapper functions for DAG execution
        async def generate_sql_wrapper(sub_state):
            """Generate SQL for a sub-query"""
            result = await generate_sql_node(sub_state)
            return result
        
        async def execute_query_wrapper(sub_state):
            """Execute SQL for a sub-query"""
            # Run through validation and execution
            validated = await validate_query_node(sub_state)
            if validated.get("next_action") == "error":
                raise Exception(f"Validation failed: {validated.get('error')}")
            
            executed = await execute_query_node(validated)
            return executed
        
        # Execute DAG
        dag_results = await MultiQueryDecompositionService.execute_dag(
            dag=dag,
            state=state,
            generate_sql_func=generate_sql_wrapper,
            execute_query_func=execute_query_wrapper
        )
        
        # Store results in state
        state["sub_queries"] = dag_results
        
        if dag_results["success"]:
            logger.info(f"Multi-query execution completed: {len(dag_results['sub_query_results'])} sub-queries")
            
            # Combine results into main execution_result
            state["execution_result"] = dag_results["combined_results"]
            
            # Skip normal SQL generation - go directly to format results
            state["next_action"] = "format_results"
            
            # Add success message
            state["messages"].append(
                AIMessage(content=f" Executed {len(dag_results['sub_query_results'])} sub-queries successfully")
            )
            
            await update_node_history(state, "decompose_query", "completed", thinking_steps=[
                {"id": "step-1", "content": f"Decomposed into {len(parts)} sub-queries", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-2", "content": f"Executed {len(dag_results['sub_query_results'])} sub-queries successfully", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ])
            
        else:
            logger.warning(f"Multi-query execution failed: {dag_results.get('errors')}")
            # Fallback to single query approach
            state["next_action"] = "generate_hypothesis"
            state["messages"].append(
                AIMessage(content=" Multi-query execution failed, falling back to single query approach")
            )
            
            await update_node_history(state, "decompose_query", "completed", thinking_steps=[
                {"id": "step-1", "content": "Multi-query execution failed, falling back to single query", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ])
        
        return state
        
    except Exception as e:
        logger.error(f"Query decomposition failed: {e}")
        # Non-fatal - proceed with normal flow
        state["next_action"] = "generate_hypothesis"
        await update_node_history(state, "decompose_query", "completed", thinking_steps=[
            {"id": "step-1", "content": "Decomposition check completed, proceeding normally", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
        return state
