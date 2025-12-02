"""
Orchestrator Node: Results
"""

import logging
import time
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from app.orchestrator.state import QueryState
from app.orchestrator.llm_config import get_llm, get_query_llm_provider, get_query_llm_model
from app.orchestrator.utils import emit_state_event, update_node_history, METRICS_AVAILABLE, record_llm_usage
from app.core.config import settings
from app.core.client_registry import registry

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


async def validate_results_node(state: QueryState) -> QueryState:
    """
    Node 5.5: Validate query results
    
    Analyzes query results for:
    - Row count (flag if <5 or >100k)
    - Null ratios (flag if >50%)
    - Value distributions
    - Anomalies
    - Empty result handling
    
    Outputs validation report and triggers strategy pivoting if issues detected
    """
    logger.info(f"Validating query results...")
    state["current_stage"] = "validate_results"
    
    # Track node execution
    await update_node_history(state, "validate_results", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Analyzing query results for anomalies", "status": "in-progress", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    result = state.get("execution_result", {})
    rows = result.get("rows", [])
    columns = result.get("columns", [])
    row_count = result.get("row_count", 0)
    
    validation_report = {
        "row_count": row_count,
        "is_valid": True,
        "warnings": [],
        "anomalies": [],
        "recommendations": []
    }
    
    # Check 1: Row count thresholds
    if row_count == 0:
        validation_report["is_valid"] = False
        validation_report["warnings"].append("I didn't find any data matching your criteria")
        validation_report["recommendations"].append("Would you like me to show the closest matches or broaden the search?")
    elif row_count < 5:
        validation_report["warnings"].append(f"Very few rows returned ({row_count}) - result may be incomplete")
    elif row_count > 100000:
        validation_report["warnings"].append(f"Very large result set ({row_count} rows) - consider adding filters")

    # Check 1b: Expected row count from user query (e.g., "top 5", "first 10")
    try:
        import re
        expected = None
        uq = (state.get("user_query") or "")
        # Match patterns like "first 5", "top 10", "5 rows", "10 records"
        patterns = [
            r"\b(?:top|first)\s+(\d{1,3})\b",
            r"\b(\d{1,3})\s+(?:rows?|records?)\b",
            r"\bshow\s+(?:me\s+)?(\d{1,3})\b",
        ]
        for pattern in patterns:
            m = re.search(pattern, uq, re.IGNORECASE)
            if m:
                expected = int(m.group(1))
                break
        
        if expected:
            validation_report["expected_rows"] = expected
            if row_count != expected:
                if row_count < expected:
                    validation_report["warnings"].append(
                        f" Row count mismatch: You requested {expected} rows but only {row_count} were returned. "
                        f"This may indicate a data issue or the table has fewer rows than expected."
                    )
                    validation_report["recommendations"].append(
                        f"Check if the table has enough data or if filters are too restrictive"
                    )
                else:
                    validation_report["warnings"].append(
                        f"Row count mismatch: expected {expected} rows based on your request, got {row_count}"
                    )
    except Exception as e:
        logger.debug(f"Failed to validate expected row count: {e}")
    
    # Check 2: Null ratios per column
    if rows and row_count > 0:
        for col_idx, col_name in enumerate(columns):
            null_count = sum(1 for row in rows if row[col_idx] is None or str(row[col_idx]).strip() == '')
            null_ratio = null_count / row_count
            
            if null_ratio > 0.5:
                validation_report["warnings"].append(f"Column '{col_name}' has {int(null_ratio*100)}% null values")
                validation_report["recommendations"].append(f"Consider filtering out nulls in '{col_name}' or using NVL/COALESCE")
    
    # Check 3: Value distribution analysis (basic - check for single repeated value)
    if rows and row_count > 10:  # Only for datasets with >10 rows
        for col_idx, col_name in enumerate(columns):
            values = [row[col_idx] for row in rows if row[col_idx] is not None]
            if values:
                unique_values = set(values)
                if len(unique_values) == 1:
                    validation_report["anomalies"].append(f"Column '{col_name}' has only one distinct value: {list(unique_values)[0]}")
    
    # Basic anomaly detection using z-score on numeric columns
    try:
        if rows and columns:
            # Determine numeric columns by sampling
            num_idxs = []
            for idx in range(len(columns)):
                try:
                    # consider numeric if most values are numbers
                    vals = [row[idx] for row in rows if row[idx] is not None]
                    if not vals:
                        continue
                    numeric_vals = []
                    for v in vals[:200]:
                        try:
                            numeric_vals.append(float(v))
                        except Exception:
                            numeric_vals = []
                            break
                    if numeric_vals:
                        num_idxs.append(idx)
                except Exception:
                    continue
            for idx in num_idxs:
                vals = []
                for r in rows:
                    try:
                        if r[idx] is None:
                            continue
                        vals.append(float(r[idx]))
                    except Exception:
                        pass
                if len(vals) >= 10:
                    mean = sum(vals) / len(vals)
                    var = sum((x - mean) ** 2 for x in vals) / len(vals)
                    std = var ** 0.5
                    if std > 0:
                        # Count outliers beyond 3 std
                        outliers = sum(1 for x in vals if abs(x - mean) > 3 * std)
                        if outliers > 0:
                            validation_report["anomalies"].append(
                                f"Column '{columns[idx]}' has {outliers} potential outliers (z-score > 3)"
                            )
    except Exception:
        pass

    # Store validation report in state
    state["result_analysis"] = validation_report
    
    # Determine next action based on validation
    if not validation_report["is_valid"]:
        logger.warning(f"Result validation failed: {validation_report['warnings']}")
        # Trigger strategy pivoting if enabled and not already attempted
        if state.get("pivot_attempts", 0) < 2:
            state["next_action"] = "pivot_strategy"
        else:
            state["next_action"] = "format_results"
        
        await update_node_history(state, "validate_results", "completed", thinking_steps=[
            {"id": "step-1", "content": f"Result validation: {len(validation_report['warnings'])} warnings found", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
    else:
        if validation_report["warnings"]:
            logger.info(f"Result validation passed with warnings: {validation_report['warnings'][:2]}")
        else:
            logger.info(f"Result validation passed")
        state["next_action"] = "format_results"
        
        await update_node_history(state, "validate_results", "completed", thinking_steps=[
            {"id": "step-1", "content": f"Results validated: {row_count} rows", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ])
    
    return state


async def format_results_node(state: QueryState) -> QueryState:
    """
    Node 6: Format results, suggest visualizations, and store episode in Graphiti
    
    Analyzes result structure and recommends:
    - Table view (default)
    - Bar chart (categorical + numeric)
    - Line chart (time-series)
    - Pie chart (distribution)
    - Scatter plot (correlation)
    
    Also stores successful query patterns in the knowledge graph for future context retrieval.
    """
    logger.info(f"Formatting results...")
    
    result = state["execution_result"]
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    row_count = result.get("row_count", 0)
    
    logger.info(f"Formatting results: {row_count} rows, {len(columns)} columns")
    if rows and len(rows) > 0:
        logger.debug(f"  First row sample: {rows[0]}")
    
    # Determine best visualization
    visualization_hints = {
        "default_view": "table",
        "recommended_chart": None,
        "chart_config": {},
    }
    
    # Simple heuristics (can be enhanced with LLM)
    if row_count > 0 and len(columns) == 2:
        # Two columns: likely category + value
        visualization_hints["recommended_chart"] = "bar"
        visualization_hints["chart_config"] = {
            "x_axis": columns[0],
            "y_axis": columns[1],
        }
    elif row_count > 10 and any("DATE" in col for col in columns):
        # Time-series data
        visualization_hints["recommended_chart"] = "line"
    
    state["visualization_hints"] = visualization_hints

    # Proactive insights & suggestions
    try:
        from app.services.insights_service import InsightsService
        ins = await InsightsService.generate_insights(state.get("sql_query", ""), columns, result.get("rows", []) or [])
        state["insights"] = ins.get("insights", [])
        state["suggested_queries"] = ins.get("suggested_queries", [])
    except Exception as e:
        logger.warning(f"Insights generation failed: {e}")
        state["insights"] = []
        state["suggested_queries"] = []
    
    # Store successful query pattern in Graphiti knowledge graph
    try:
        graphiti_client = registry.get_graphiti_client()
        if graphiti_client:
            user_id = state.get('user_id', 'default_user')
            
            # Store general query pattern
            episode_content = f"""
Query Pattern Episode:
- User Query: {state['user_query']}
- Intent: {state['intent'][:200]}
- SQL Generated: {state['sql_query'][:500]}
- Result: {row_count} rows, columns: {', '.join(columns[:10])}
- Execution Time: {result.get('execution_time_ms', 0)}ms
- Visualization: {visualization_hints['recommended_chart'] or 'table'}
"""
            
            # Add general pattern episode
            await graphiti_client.add_episode(
                content=episode_content,
                episode_type="query_pattern",
                source="query_orchestrator",
                reference_time=datetime.now(timezone.utc),
                metadata={"user_query": state['user_query'][:100]}
            )
            
            # Store user-specific pattern
            user_pattern = f"""
User Pattern Episode (user:{user_id}):
- Query: {state['user_query'][:200]}
- Intent: {state['intent'][:100]}
- SQL Pattern: {state['sql_query'][:200]}
- Success: True
- Result Size: {row_count} rows
- Timestamp: {datetime.now(timezone.utc).isoformat()}
"""
            await graphiti_client.add_episode(
                content=user_pattern,
                episode_type="user_query_pattern",
                source=f"user:{user_id}",
                reference_time=datetime.now(timezone.utc),
                metadata={"user_id": user_id}
            )
            
            logger.info(f"Query patterns stored in knowledge graph")
    except Exception as e:
        # Don't fail the workflow if Graphiti storage fails
        logger.warning(f"Failed to store episode in Graphiti: {e}")
    
    # Store conversation in persistent memory (Redis)
    try:
        from app.services.persistent_memory_service import PersistentMemoryService
        
        user_id = state.get("user_id", "default_user")
        session_id = state.get("session_id", "default_session")
        
        # Determine execution status
        execution_status = "success" if result.get("status") == "success" else "error"
        
        # Create result summary for storage
        result_summary = {
            "row_count": row_count,
            "columns": columns[:20],  # Limit columns stored
            "execution_time_ms": result.get("execution_time_ms", 0),
            "visualization": visualization_hints.get("recommended_chart"),
            "warnings": state.get("result_analysis", {}).get("warnings", [])[:5],
            "anomalies": state.get("result_analysis", {}).get("anomalies", [])[:5],
        }
        
        await PersistentMemoryService.store_conversation(
            user_id=user_id,
            session_id=session_id,
            user_query=state.get("user_query", ""),
            intent=state.get("intent", ""),
            sql_query=state.get("sql_query", ""),
            execution_status=execution_status,
            result_summary=result_summary,
            error_message=state.get("error"),
            metadata={
                "query_id": state.get("query_id"),
                "trace_id": state.get("trace_id"),
                "sql_confidence": state.get("sql_confidence", 0),
                "llm_provider": state.get("llm_metadata", {}).get("provider") if isinstance(state.get("llm_metadata"), dict) else None,
            }
        )
        logger.info(f"Conversation stored in persistent memory")
    except Exception as e:
        logger.warning(f"Failed to store conversation in persistent memory: {e}")
    
    state["next_action"] = "end"
    logger.info(f"Results formatted: {visualization_hints}")

    # Stream lifecycle: finished with complete progress
    if ExecState:
        await emit_state_event(state, ExecState.FINISHED, {
            "row_count": row_count,
            "result": result,
            "insights": state.get("insights", []),
            "suggested_queries": state.get("suggested_queries", []),
            "sql": state.get("sql_query", ""),
            "expected_row_count": state.get("result_analysis", {}).get("expected_rows"),
            "rows_match_expectation": (state.get("result_analysis", {}).get("expected_rows") == row_count) if state.get("result_analysis", {}).get("expected_rows") is not None else None,
            "thinking_steps": [
                {"id": "step-1", "content": "Analyzed user query intent", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-2", "content": "Retrieved schema context", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-3", "content": "Validated column mappings", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-4", "content": "Generated SQL query", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-5", "content": "Validated SQL syntax", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
                {"id": "step-6", "content": f"Query executed successfully - {row_count} rows returned", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
            ],
            "complete": True
        })
    return state
