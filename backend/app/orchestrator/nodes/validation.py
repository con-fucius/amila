"""
Orchestrator Node: Validation
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

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

logger = logging.getLogger(__name__)


async def validate_query_node(state: QueryState) -> QueryState:
    """Validate SQL query with span instrumentation and structured errors."""

    state["current_stage"] = "validate"
    
    # Record pipeline stage entry
    from app.services.diagnostic_service import record_query_pipeline_stage
    from datetime import datetime, timezone
    
    query_id = state.get("query_id", "unknown")
    stage_start = datetime.now(timezone.utc)
    
    # Track node execution
    await update_node_history(state, "validate", "in-progress", thinking_steps=[
        {"id": "step-1", "content": "Validating SQL syntax and security", "status": "in-progress", "timestamp": stage_start.isoformat()}
    ])
    
    # Check iteration limit to prevent infinite loops
    state["total_iterations"] = state.get("total_iterations", 0) + 1
    if state["total_iterations"] >= state.get("max_iterations", 40):
        state["error"] = f"Maximum iteration limit reached ({state['total_iterations']}). Stopping to prevent infinite loop."
        state["next_action"] = "error"
        logger.error(f"Iteration limit reached at validation node")
        
        # Record failure
        stage_end = datetime.now(timezone.utc)
        await record_query_pipeline_stage(
            query_id=query_id,
            stage="validation",
            status="failed",
            entered_at=stage_start,
            exited_at=stage_end,
            error_details="Maximum iteration limit reached"
        )
        return state

    async with langfuse_span(
        state,
        "orchestrator.validate",
        input_data={
            "sql_preview": (state.get("sql_query") or "")[:500],
            "user_role": state.get("user_role", "unknown"),
        },
        metadata={"stage": "validate"},
    ) as span:
        span.setdefault("output", {})
        
        try:
            result = await _validate_query_node_inner(state, span)
            
            # Record completion
            stage_end = datetime.now(timezone.utc)
            await record_query_pipeline_stage(
                query_id=query_id,
                stage="validation",
                status="completed",
                entered_at=stage_start,
                exited_at=stage_end,
                metadata={
                    "validation_passed": result.get("next_action") != "error",
                    "needs_approval": result.get("needs_approval"),
                    "risk_level": result.get("validation_result", {}).get("risk_level")
                }
            )
            
        except Exception as e:
            # Record failure
            stage_end = datetime.now(timezone.utc)
            await record_query_pipeline_stage(
                query_id=query_id,
                stage="validation",
                status="failed",
                entered_at=stage_start,
                exited_at=stage_end,
                error_details=str(e)
            )
            raise

        # Expose final routing decision for observability
        span["output"]["next_action"] = result.get("next_action")
        return result


async def _validate_query_node_inner(state: QueryState, span: dict) -> QueryState:
    """
    Node 3: Validate SQL query for security and syntax

    Uses sql_validator.py for comprehensive injection detection and risk assessment
    """

    logger.info(f"Validating SQL query with comprehensive security checks...")

    from app.core.sql_validator import SQLValidator
    from app.core.rbac import Role
    from app.core.sql_dialect_converter import SQLDialectConverter, convert_sql, SQLDialect

    sql_query = state["sql_query"]
    db_type = (state.get("database_type") or "oracle").lower()
    
    # SQL Dialect Conversion - Convert SQL to target database dialect
    original_sql = sql_query
    dialect_conversion_applied = False
    
    try:
        # Validate SQL for target dialect and convert if needed
        if db_type in ["doris", "postgres", "postgresql"]:
            # Convert Oracle-style SQL to target dialect
            if db_type == "doris":
                conversion_result = SQLDialectConverter.convert_to_doris(sql_query, strict=False)
            else:
                conversion_result = SQLDialectConverter.convert_to_postgres(sql_query, strict=False)
            
            if conversion_result.success and conversion_result.sql != sql_query:
                sql_query = conversion_result.sql
                dialect_conversion_applied = True
                logger.info(f"SQL converted to {db_type} dialect")
                span["output"]["dialect_conversion"] = {
                    "from": "oracle",
                    "to": db_type,
                    "applied": True,
                    "warnings": conversion_result.warnings[:3] if conversion_result.warnings else [],
                    "unsupported_features": conversion_result.unsupported_features[:3] if conversion_result.unsupported_features else [],
                }
                if conversion_result.warnings:
                    logger.warning(f"Dialect conversion warnings: {conversion_result.warnings[:3]}")
                if conversion_result.unsupported_features:
                    logger.warning(f"Unsupported features in Doris: {conversion_result.unsupported_features}")
            elif not conversion_result.success:
                logger.warning(f"Dialect conversion failed: {conversion_result.errors}")
                span["output"]["dialect_conversion"] = {
                    "from": "oracle",
                    "to": db_type,
                    "applied": False,
                    "errors": conversion_result.errors[:3] if conversion_result.errors else [],
                }
        elif db_type == "oracle":
            # Validate SQL is valid Oracle syntax (no conversion needed if already Oracle)
            validation_result = SQLDialectConverter.validate_for_dialect(sql_query, SQLDialect.ORACLE)
            if not validation_result.success:
                # Try converting from MySQL/Doris style to Oracle
                conversion_result = SQLDialectConverter.convert_to_oracle(sql_query, strict=False)
                if conversion_result.success and conversion_result.sql != sql_query:
                    sql_query = conversion_result.sql
                    dialect_conversion_applied = True
                    logger.info(f"SQL converted to Oracle dialect")
                    span["output"]["dialect_conversion"] = {
                        "from": "mysql",
                        "to": "oracle",
                        "applied": True,
                        "warnings": conversion_result.warnings[:3] if conversion_result.warnings else [],
                    }
        
        # Update state with converted SQL
        if dialect_conversion_applied:
            state["sql_query"] = sql_query
            state["original_sql_before_conversion"] = original_sql
            
            # Add to thinking steps
            if "llm_metadata" not in state or not isinstance(state["llm_metadata"], dict):
                state["llm_metadata"] = {}
            if "thinking_steps" not in state["llm_metadata"]:
                state["llm_metadata"]["thinking_steps"] = []
            state["llm_metadata"]["thinking_steps"].append({
                "content": f"Converted SQL to {db_type} dialect for compatibility",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stage": "validate"
            })
            
    except Exception as e:
        logger.warning(f"Dialect conversion check failed (non-fatal): {e}")
        span["output"]["dialect_conversion_error"] = str(e)

    # Get user role from state (default to ANALYST if not specified)
    user_role_str = state.get("user_role", "analyst")
    try:
        user_role = Role.ANALYST  # Default safe role
        if "admin" in user_role_str.lower():
            user_role = Role.ADMIN
    except Exception:
        user_role = Role.ANALYST

    validator = SQLValidator()
    validation_result = validator.validate_query(sql_query, user_role)

    state["validation_result"] = validation_result.to_dict()
    span["output"]["validation_result"] = validation_result.to_dict()

    if not validation_result.is_valid:
        logger.error(f"SQL validation failed: {validation_result.errors}")
        error_message = f"SQL validation failed: {'; '.join(validation_result.errors)}"
        await set_state_error(state, "validation", error_message, {"errors": validation_result.errors})
        state["next_action"] = "error"
        state["messages"].append(AIMessage(
            content=f" Query validation failed:\n" + "\n".join(f"- {err}" for err in validation_result.errors)
        ))
        span["output"]["error"] = validation_result.errors
        span["level"] = "ERROR"
        span["status_message"] = "sql_validation_failed"
        if ExecState:
            await emit_state_event(state, ExecState.ERROR, {"errors": validation_result.errors})
        return state

    # Baseline: require human approval for all orchestrator-generated queries
    # for non-admin users. Admins may bypass approval for low-risk queries.
    is_admin = user_role == Role.ADMIN
    state["needs_approval"] = not is_admin

    # Check if query needs approval (HIGH or CRITICAL risk)
    if validation_result.requires_approval:
        state["needs_approval"] = True
        state["approved"] = False
        state["next_action"] = "await_approval"  # Route to HITL approval gate
        logger.warning(f"Query requires approval: {validation_result.risk_level.value} risk")
        state["messages"].append(AIMessage(
            content=f" Query requires admin approval ({validation_result.risk_level.value} risk)"
        ))
        span["output"]["risk_level"] = validation_result.risk_level.value
        span["output"]["requires_approval"] = True
        if ExecState:
            await emit_state_event(state, ExecState.PENDING_APPROVAL, {"risk": validation_result.risk_level.value})
        return state

    logger.info(f"SQL validation passed (risk: {validation_result.risk_level.value})")
    span["output"]["risk_level"] = validation_result.risk_level.value
    if validation_result.warnings:
        state["messages"].append(AIMessage(
            content=f" Warnings: {', '.join(validation_result.warnings)}"
        ))
        span["output"]["warnings"] = validation_result.warnings

    # Enforce row limit on SELECT queries (Oracle FETCH FIRST or Doris LIMIT)
    try:
        db_type = (state.get("database_type") or "oracle").lower()
        if validation_result.query_type.value == "SELECT":
            limited = validator.enforce_row_limit(state["sql_query"], max_rows=1000, dialect=db_type)
            if limited != state["sql_query"]:
                state["sql_query"] = limited
                logger.info(f"Applied row limit for {db_type}")
                span["output"]["row_limit_enforced"] = True
    except Exception:
        span["output"]["row_limit_enforced"] = False

    state["next_action"] = "execute"

    # Syntax check (basic) - strip SQL comments before checking
    sql_without_comments = sql_query.strip()
    while sql_without_comments.startswith("--"):
        newline_pos = sql_without_comments.find("\n")
        if newline_pos == -1:
            sql_without_comments = ""
            break
        sql_without_comments = sql_without_comments[newline_pos + 1:].strip()

    while sql_without_comments.startswith("/*"):
        end_comment = sql_without_comments.find("*/")
        if end_comment == -1:
            break
        sql_without_comments = sql_without_comments[end_comment + 2:].strip()

    state["validation_result"] = validation_result.to_dict()

    # Check if user is ADMIN - auto-approve all queries
    user_role = state.get("user_role", "analyst").lower()
    is_admin = user_role == "admin"

    # Escalate approval requirement based on risk level (for non-admins).
    # Do NOT downgrade an existing approval requirement.
    if validation_result.risk_level.value in ["medium", "high"] and not is_admin:
        state["needs_approval"] = True

    # Query Cost Estimation - estimate before execution with EXPLAIN PLAN visibility
    cost_estimate = None
    try:
        from app.services.query_cost_estimator import QueryCostEstimator, CostLevel

        # Include execution plan for observability (helps debugging slow queries)
        cost_estimate = await QueryCostEstimator.estimate_query_cost(
            sql_query=state["sql_query"],
            connection_name=settings.oracle_default_connection,
            include_plan=True,  # Enable EXPLAIN PLAN visibility
        )

        state["cost_estimate"] = {
            "total_cost": cost_estimate.total_cost,
            "cardinality": cost_estimate.cardinality,
            "cost_level": cost_estimate.cost_level.value,
            "has_full_table_scan": cost_estimate.has_full_table_scan,
            "full_scan_tables": cost_estimate.full_scan_tables,
            "warnings": cost_estimate.warnings,
            "recommendations": cost_estimate.recommendations,
        }
        
        # Store execution plan for frontend visibility
        if cost_estimate.execution_plan:
            state["execution_plan"] = cost_estimate.execution_plan
            state["cost_estimate"]["execution_plan"] = cost_estimate.execution_plan

        span["output"]["cost_estimate"] = {
            "total_cost": cost_estimate.total_cost,
            "cardinality": cost_estimate.cardinality,
            "cost_level": cost_estimate.cost_level.value,
        }

        if cost_estimate.warnings:
            state["messages"].append(AIMessage(
                content=f" Cost Warnings:\n" + "\n".join(f"- {w}" for w in cost_estimate.warnings[:3])
            ))
            span["output"]["cost_warnings"] = cost_estimate.warnings

        if cost_estimate.cost_level == CostLevel.CRITICAL and not is_admin:
            logger.error(f"Query blocked: CRITICAL cost level ({int(cost_estimate.total_cost)})")
            message = (
                f"Query blocked: Estimated cost too high ({int(cost_estimate.total_cost)}). "
                f"Recommendations: {'; '.join(cost_estimate.recommendations[:2])}"
            )
            await set_state_error(state, "validation", message, {
                "cost_level": cost_estimate.cost_level.value,
                "total_cost": float(cost_estimate.total_cost),
            })
            state["next_action"] = "error"
            span["output"]["error"] = message
            span["level"] = "ERROR"
            span["status_message"] = "cost_estimate_blocked"
            return state

        if cost_estimate.cost_level == CostLevel.HIGH and not is_admin:
            state["needs_approval"] = True
            logger.warning(f"HIGH cost query requires approval: {int(cost_estimate.total_cost)}")

        logger.info(f"Cost estimation: {int(cost_estimate.total_cost)} ({cost_estimate.cost_level.value})")

        try:
            est_rows = int(cost_estimate.cardinality)
        except Exception:
            est_rows = 0
        if est_rows and est_rows > 1000 and not is_admin:
            state["needs_approval"] = True
            state["next_action"] = "await_approval"
            state["messages"].append(
                AIMessage(content=f" Large result set expected (~{est_rows} rows). Preview/approval required.")
            )
            span["output"]["requires_approval"] = True
            return state

    except Exception as e:
        logger.warning(f"Cost estimation failed: {e}")
        span["output"]["cost_estimate_failed"] = str(e)

    # Always set approval context for HITL review
    state["approval_context"] = {
        "risk_level": validation_result.risk_level.value,
        "warnings": validation_result.warnings,
        "requires_approval": True,
        "query_type": validation_result.query_type.value if hasattr(validation_result, 'query_type') else "SELECT",
    }
    try:
        if cost_estimate:
            state["approval_context"].update({
                "estimated_cost": float(cost_estimate.total_cost),
                "estimated_rows": int(cost_estimate.cardinality),
                "has_full_table_scan": bool(cost_estimate.has_full_table_scan),
                "recommendations": cost_estimate.recommendations[:5],
            })
    except Exception:
        pass
    
    # Force approval for ALL queries (mandatory HITL)
    state["needs_approval"] = True

    if validation_result.is_valid:
        if state["needs_approval"]:
            state["next_action"] = "await_approval"
            logger.info(f"Query requires approval: {validation_result.risk_level.value} risk")
        else:
            state["next_action"] = "execute"
            if is_admin:
                logger.info(
                    f" Query validated and auto-approved for ADMIN: {validation_result.risk_level.value} risk"
                )
            else:
                logger.info(f"Query validated: {validation_result.risk_level.value} risk")
    else:
        state["next_action"] = "error"
        message = f"Validation failed: {validation_result.errors}"
        await set_state_error(state, "validation", message, {"errors": validation_result.errors})
        logger.warning(message)

    span["output"]["status"] = "success"
    
    # Mark node as completed
    await update_node_history(state, "validate", "completed", thinking_steps=[
        {"id": "step-1", "content": f"SQL validated ({validation_result.risk_level.value} risk)", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
    ])
    
    return state


async def probe_sql_node(state: QueryState) -> QueryState:
    """
    Phase 3: Probe SQL structural validity before execution using lightweight validation
    
    Validates:
    - SQL syntax correctness
    - Table/column existence
    - Join validity
    
    Routes to repair_sql if probe fails
    """
    logger.info(f"Probing SQL structural validity...")

    # Dialect awareness: probe is implemented only for Oracle using ROWNUM
    db_type = (state.get("database_type") or "oracle").lower()
    if db_type != "oracle":
        logger.info(f"Skipping structural probe for non-Oracle database type: %s", db_type)
        state["next_action"] = "execute"
        state["probe_result"] = {
            "status": "skipped",
            "reason": f"Probe only implemented for Oracle (db_type={db_type})",
        }
        return state

    # TEMPORARY: Skip probe for queries that don't work well with wrapping
    sql_query = state.get("sql_query", "")
    sql_upper = sql_query.upper()
    
    # Skip probe for GROUP BY or FETCH FIRST (wrapping breaks them)
    if "GROUP BY" in sql_upper or "FETCH FIRST" in sql_upper or "OFFSET" in sql_upper:
        logger.info(f"Skipping probe (query contains GROUP BY/FETCH FIRST/OFFSET)")
        state["next_action"] = "execute"
        state["probe_result"] = {"status": "skipped", "reason": "Query type not compatible with probe wrapping"}
        return state
    
    try:
        from app.core.client_registry import registry
        mcp_client = registry.get_mcp_client()
        connection_name = settings.oracle_default_connection
        
        if not mcp_client:
            logger.warning(f"MCP client unavailable, skipping probe")
            state["next_action"] = "execute"
            return state
        
        # Use lightweight validation query (SELECT 1 FROM (...) WHERE ROWNUM < 1)
        # This validates structure without fetching data
        probe_sql = f"SELECT 1 FROM ({sql_query}) WHERE ROWNUM < 1"
        
        logger.debug(f"Probe SQL: {probe_sql[:200]}...")
        probe_result = await mcp_client.execute_sql(probe_sql, connection_name=connection_name)
        
        if probe_result.get("status") == "success":
            logger.info(f"SQL probe passed - structure is valid")
            state["next_action"] = "execute"
            state["probe_result"] = {"status": "valid", "method": "SELECT 1 probe"}
            return state
        else:
            # Probe failed - SQL has structural issues
            error_msg = probe_result.get("message", "Probe validation failed")
            logger.warning(f"SQL probe failed: {error_msg}")
            
            # Store probe failure in state
            state["probe_result"] = {
                "status": "invalid",
                "error": error_msg,
                "method": "SELECT 1 probe"
            }
            
            # Route to repair if not already attempted
            if state.get("repair_attempts", 0) < 1:
                state["error"] = f"SQL structural validation failed: {error_msg}"
                state["next_action"] = "repair_sql"
                logger.info(f"Routing to repair_sql for structural fixes")
            else:
                # Already tried repair, route to error
                state["error"] = f"SQL probe failed after repair: {error_msg}"
                state["next_action"] = "error"
            
            return state
    
    except Exception as e:
        logger.warning(f"SQL probe error (non-fatal): {e}")
        # Probe failure is non-fatal - proceed to execution
        state["next_action"] = "execute"
        state["probe_result"] = {"status": "error", "error": str(e)}
        return state
