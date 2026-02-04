"""
Orchestrator Node: Validation
"""

import logging
import time
import re
from typing import List, Dict, Any, Optional, Tuple, Union, Callable, TypeVar
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
from app.orchestrator.utils.sql_explainer import explain_sql_query

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

    Uses sql_validator.py and enhanced SQL injection detector for comprehensive security
    """

    logger.info(f"Validating SQL query with comprehensive security checks...")
    
    # Initialize risk reasons list
    if "risk_reasons" not in state or not isinstance(state["risk_reasons"], list):
        state["risk_reasons"] = []

    from app.core.sql_validator import SQLValidator

    sql_query = state["sql_query"]
    db_type = (state.get("database_type") or "oracle").lower()

    # Initialize Query Plan (Visualizing planned nodes and estimates)
    state["query_plan"] = {
        "steps": [
            {"id": "plan-1", "node": "validate", "status": "active", "description": "Analyzing SQL safety and compliance"},
            {"id": "plan-2", "node": "approval", "status": "pending", "description": "Security and governance review"},
            {"id": "plan-3", "node": "execute", "status": "pending", "description": "Database execution and result retrieval"}
        ],
        "estimated_cost": state.get("cost_estimate", {}).get("total_cost", 0)
    }

    # Generate English explanation for trustworthiness
    if sql_query and not state.get("sql_explanation"):
        state["sql_explanation"] = await explain_sql_query(sql_query, db_type=db_type)

    # Enhanced SQL injection detection
    try:
        from app.services.sql_injection_detector import SQLInjectionDetector, Severity
        
        detector = SQLInjectionDetector()
        injection_result = detector.detect(sql_query)
        
        if not injection_result.is_safe:
            logger.warning(
                f"SQL injection patterns detected: risk_score={injection_result.risk_score:.1f}, "
                f"findings={len(injection_result.findings)}"
            )
            
            # Log detailed findings
            for finding in injection_result.findings:
                logger.warning(
                    f"  [{finding.severity.value.upper()}] {finding.injection_type.value}: "
                    f"{finding.description} (confidence: {finding.confidence:.2f})"
                )
            
            # Block critical or high severity findings
            critical_or_high = any(
                f.severity in (Severity.CRITICAL, Severity.HIGH)
                for f in injection_result.findings
            )
            
            if critical_or_high:
                error_message = f"SQL injection attack detected: {len(injection_result.findings)} suspicious patterns found"
                state["risk_reasons"].append(f"Security: Critical SQL injection risk detected ({injection_result.risk_score:.1f})")
                
                await set_state_error(
                    state,
                    "validation",
                    error_message,
                    {
                        "risk_score": injection_result.risk_score,
                        "findings_count": len(injection_result.findings),
                        "detected_patterns": list(injection_result.detected_patterns),
                        "injection_details": [
                            {
                                "type": f.injection_type.value,
                                "severity": f.severity.value,
                                "description": f.description,
                                "mitigation": f.mitigation
                            }
                            for f in injection_result.findings[:5]  # Limit details
                        ]
                    }
                )
                state["next_action"] = "error"
                state["injection_detection"] = {
                    "blocked": True,
                    "risk_score": injection_result.risk_score
                }
                span["output"]["injection_detected"] = True
                span["output"]["injection_findings"] = injection_result.scan_summary
                span["level"] = "ERROR"
                span["status_message"] = "sql_injection_blocked"
                return state
            
            # For medium/low findings, add warnings but continue
            span["output"]["injection_warnings"] = injection_result.scan_summary
            state["security_warnings"] = [
                {
                    "type": f.injection_type.value,
                    "severity": f.severity.value,
                    "description": f.description
                }
                for f in injection_result.findings
            ]
            state["risk_reasons"].append(f"Security: {len(injection_result.findings)} suspicious SQL patterns detected")
        else:
            span["output"]["injection_scan"] = {
                "safe": True,
                "risk_score": injection_result.risk_score
            }
    except Exception as e:
        logger.warning(f"Enhanced injection detection failed (non-fatal): {e}")

    from app.core.rbac import Role
    from app.core.sql_dialect_converter import SQLDialectConverter, convert_sql, SQLDialect
    
    # SQL Dialect Conversion - Convert only if validation fails for target dialect
    original_sql = sql_query
    dialect_conversion_applied = False
    
    try:
        # Validate SQL for target dialect and convert if needed
        if db_type in ["doris", "postgres", "postgresql"]:
            target_dialect = SQLDialect.DORIS if db_type == "doris" else SQLDialect.POSTGRES
            validation_result = SQLDialectConverter.validate_for_dialect(sql_query, target_dialect)
            if not validation_result.success:
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
                        logger.warning(f"Unsupported features in {db_type}: {conversion_result.unsupported_features}")
                elif not conversion_result.success:
                    logger.warning(f"Dialect conversion failed: {conversion_result.errors}")
                    span["output"]["dialect_conversion"] = {
                        "from": "oracle",
                        "to": db_type,
                        "applied": False,
                        "errors": conversion_result.errors[:3] if conversion_result.errors else [],
                    }
            else:
                span["output"]["dialect_validation"] = {"dialect": db_type, "status": "passed"}
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

    # Scope constraints: enforce max tables/joins by role (risk-aware)
    try:
        from app.services.role_based_limits_service import RoleBasedLimitsService

        role_key = state.get("user_role", "viewer")
        role_limits = RoleBasedLimitsService.get_role_limits(role_key)
        max_tables = role_limits.max_tables
        max_joins = role_limits.max_joins

        table_count = len(validator.extract_tables(sql_query))
        join_count = len(re.findall(r"\bJOIN\b", sql_query, flags=re.IGNORECASE))

        # Risk-aware tightening for high/critical risk
        risk_level = validation_result.risk_level.value
        if risk_level in ["high", "critical"] and max_tables > 0:
            max_tables = max(1, max_tables - 1)
        if risk_level in ["high", "critical"] and max_joins > 0:
            max_joins = max(1, max_joins - 1)

        scope_warnings = []
        if max_tables > 0 and table_count > max_tables:
            scope_warnings.append(f"Table count {table_count} exceeds role limit {max_tables}")
        if max_joins > 0 and join_count > max_joins:
            scope_warnings.append(f"Join count {join_count} exceeds role limit {max_joins}")

        if scope_warnings:
            state.setdefault("validation_result", {}).setdefault("scope", {})
            state["validation_result"]["scope"] = {
                "table_count": table_count,
                "join_count": join_count,
                "max_tables": max_tables,
                "max_joins": max_joins,
                "warnings": scope_warnings,
            }
            state["risk_reasons"].extend(scope_warnings)
            span["output"]["scope_warnings"] = scope_warnings
            # Require approval for out-of-scope queries (non-admin)
            if user_role != Role.ADMIN:
                state["needs_approval"] = True
                state["force_approval"] = True
                state["messages"].append(AIMessage(
                    content=f" Query scope requires approval: {'; '.join(scope_warnings)}"
                ))

        # Partial Approval / Sensitive Item Detection
        try:
            from app.services.role_based_limits_service import RoleBasedLimitsService
            
            tables = validator.extract_tables(sql_query)
            # Basic extraction, actual implementation would use sqlglot/pglast
            
            sensitive_tables_found = [t for t in tables if t.upper() in RoleBasedLimitsService.SENSITIVE_TABLES]
            
            if sensitive_tables_found:
                state["needs_approval"] = True
                state["force_approval"] = True
                state["risk_reasons"].append(f"Governance: Accessing sensitive tables ({', '.join(sensitive_tables_found)})")
                logger.info(f"Sensitive table access detected: {sensitive_tables_found}")
                
        except Exception as e:
            logger.warning(f"Sensitive data check failed (non-fatal): {e}")

    except Exception as e:
        logger.warning(f"Scope limit enforcement failed (non-fatal): {e}")
        span["output"]["scope_limit_error"] = str(e)

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

    # Cartesian join guardrails: enforce explicit approval even for auto-approve
    try:
        issues = validation_result.issues or []
        cartesian_risk = any(
            issue.get("type") == "cartesian_join_risk" for issue in issues
        )
        if cartesian_risk:
            state["needs_approval"] = True
            state["force_approval"] = True
            state["cartesian_guard"] = True
            state["messages"].append(AIMessage(
                content=" Potential Cartesian join detected. Explicit approval required."
            ))
            state["risk_reasons"].append("Data Quality: Potential Cartesian join (missing join predicates)")
            span["output"]["cartesian_guard"] = True
    except Exception as e:
        logger.warning(f"Cartesian join guard check failed: {e}")

    # Baseline: require human approval for orchestrator-generated queries
    # Settings control: users can enable/disable approval via disableSqlApproval setting
    # Admin role does NOT automatically bypass approval - respect user settings
    auto_approve = state.get("auto_approve", False)  # From frontend settings
    
    # CRITICAL FIX: Don't override needs_approval if it was already set to True by earlier checks
    # (scope limits, sensitive tables, cartesian joins, etc.)
    if not state.get("needs_approval"):
        # Default to requiring approval unless explicitly disabled via settings
        state["needs_approval"] = not auto_approve

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

    # Enforce row limit on SELECT queries based on user role
    try:
        from app.services.role_based_limits_service import RoleBasedLimitsService
        
        db_type = (state.get("database_type") or "oracle").lower()
        user_role = state.get("user_role", "viewer")
        user_id = state.get("user_id", "anonymous")
        
        # Check query quota
        quota_allowed, quota_info = await RoleBasedLimitsService.check_and_increment_query_quota(
            user_id=user_id,
            role=user_role
        )
        
        if not quota_allowed:
            logger.warning(f"User {user_id} has exceeded daily query quota")
            await set_state_error(
                state,
                "validation",
                f"Daily query quota exceeded. Limit: {quota_info['limit']}, Used: {quota_info['used']}. "
                f"Quota resets at {quota_info['resets_at']}",
                {"quota_info": quota_info}
            )
            state["next_action"] = "error"
            span["output"]["quota_exceeded"] = quota_info
            return state
        
        # Apply role-based row limits
        if validation_result.query_type.value == "SELECT":
            max_rows = RoleBasedLimitsService.get_max_rows_for_role(user_role)
            
            if max_rows > 0:  # 0 means unlimited (admin)
                limited = RoleBasedLimitsService.apply_row_limit(
                    state["sql_query"],
                    role=user_role,
                    dialect=db_type
                )
                
                if limited != state["sql_query"]:
                    state["sql_query"] = limited
                    logger.info(f"Applied role-based row limit ({max_rows}) for {user_role}")
                    span["output"]["row_limit_enforced"] = True
                    span["output"]["row_limit"] = max_rows
                    state["messages"].append(AIMessage(
                        content=f" Row limit applied: {max_rows} rows max ({user_role} role)"
                    ))
            else:
                logger.info(f"No row limit for admin user {user_id}")
                span["output"]["row_limit_enforced"] = False
                span["output"]["row_limit"] = "unlimited"
        
        # Store quota info in state
        state["quota_info"] = quota_info
        span["output"]["quota_info"] = quota_info
        
    except Exception as e:
        logger.warning(f"Role-based limit enforcement failed (non-fatal): {e}")
        span["output"]["row_limit_error"] = str(e)

    state["next_action"] = "execute"

    # Fix 8: Apply Row-Level Security (RLS) policies to SQL query
    try:
        from app.services.row_level_security_service import RowLevelSecurityService, RLSContext
        
        user_id = state.get("user_id", "unknown")
        user_role = state.get("user_role", "viewer")
        
        rls_context = RLSContext(
            user_id=user_id,
            user_role=user_role,
            attributes=state.get("user_attributes", {})
        )
        
        rls_result = await RowLevelSecurityService.enforce_rls(
            sql=sql_query,
            context=rls_context,
            database_type=db_type
        )
        
        if rls_result.enforced:
            sql_query = rls_result.modified_sql
            state["sql_query"] = sql_query
            state["rls_applied"] = True
            state["rls_explanation"] = f"Row-level security policies applied: {rls_result.reason}. Data filtered for your role/permissions."
            
            span["output"]["rls"] = {
                "applied": True,
                "reason": rls_result.reason,
                "policies": rls_result.policies_applied
            }
            logger.info(f"RLS enforced: {rls_result.reason}")
        else:
            state["rls_applied"] = False
            span["output"]["rls"] = {"applied": False}
                
    except Exception as e:
        logger.warning(f"RLS enforcement failed (non-fatal): {e}")
        span["output"]["rls_error"] = str(e)
        state["rls_applied"] = False

    # Syntax check (basic) - strip SQL comments before checking
    sql_without_comments = sql_query.strip()
    while sql_without_comments.startswith("--"):
        newline_pos = sql_without_comments.find("\n")

    while sql_without_comments.startswith("/*"):
        end_comment = sql_without_comments.find("*/")
        if end_comment == -1:
            break
        sql_without_comments = sql_without_comments[end_comment + 2:].strip()

    state["validation_result"] = validation_result.to_dict()

    # Get auto_approve setting from frontend (user preference)
    auto_approve = state.get("auto_approve", False)

    # Escalate approval requirement based on risk level
    # Do NOT downgrade an existing approval requirement
    if validation_result.risk_level.value in ["medium", "high"] and not auto_approve:
        state["needs_approval"] = True

    # Query Cost Estimation - estimate before execution with EXPLAIN PLAN visibility
    cost_estimate = None
    try:
        from app.services.query_cost_estimator import QueryCostEstimator, CostLevel

        # Include execution plan for observability (helps debugging slow queries)
        # Pass database_type for multi-database support
        db_type_for_cost = (state.get("database_type") or "oracle").lower()
        cost_estimate = await QueryCostEstimator.estimate_query_cost(
            sql_query=state["sql_query"],
            connection_name=settings.oracle_default_connection if db_type_for_cost == "oracle" else None,
            database_type=db_type_for_cost,
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
            state["risk_reasons"].extend([f"Cost: {w}" for w in cost_estimate.warnings])
            span["output"]["cost_warnings"] = cost_estimate.warnings

        # Add cost-based optimization hints
        if cost_estimate.recommendations:
            state.setdefault("optimization_suggestions", [])
            state["optimization_suggestions"].extend([
                {
                    "type": "cost_hint",
                    "severity": "warning",
                    "description": rec,
                    "suggested_fix": rec
                }
                for rec in cost_estimate.recommendations[:5]
            ])

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
            state["risk_reasons"].append(f"Cost: Query blocked due to CRITICAL cost level ({int(cost_estimate.total_cost)})")
            return state

        if cost_estimate.cost_level == CostLevel.HIGH and not auto_approve:
            state["needs_approval"] = True
            logger.warning(f"HIGH cost query requires approval: {int(cost_estimate.total_cost)}")
            state["risk_reasons"].append(f"Cost: HIGH cost level ({int(cost_estimate.total_cost)})")

        logger.info(f"Cost estimation: {int(cost_estimate.total_cost)} ({cost_estimate.cost_level.value})")

        try:
            est_rows = int(cost_estimate.cardinality)
        except Exception:
            est_rows = 0
        if est_rows and est_rows > 1000 and not auto_approve:
            state["needs_approval"] = True
            state["next_action"] = "await_approval"
            state["messages"].append(
                AIMessage(content=f" Large result set expected (~{est_rows} rows). Preview/approval required.")
            )
            state["risk_reasons"].append(f"Performance: Large result set expected (~{est_rows} rows)")
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
    if state.get("sql_explanation"):
        state["approval_context"]["sql_explanation"] = state.get("sql_explanation")
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
    try:
        scope_info = (state.get("validation_result") or {}).get("scope")
        if scope_info:
            state["approval_context"]["scope"] = scope_info
    except Exception:
        pass
    try:
        state["approval_context"]["cartesian_guard"] = bool(state.get("cartesian_guard"))
        state["approval_context"]["force_approval"] = bool(state.get("force_approval"))
        state["approval_context"]["intent_source"] = state.get("intent_source")
        state["approval_context"]["skills_used"] = bool(state.get("skills_used"))
        state["approval_context"]["skills_fallback"] = bool(state.get("skills_fallback"))
        if state.get("skills_fallback_reason"):
            state["approval_context"]["skills_fallback_reason"] = state.get("skills_fallback_reason")
        hypothesis_structured = state.get("hypothesis_structured") if isinstance(state.get("hypothesis_structured"), dict) else {}
        if hypothesis_structured:
            state["approval_context"]["hypothesis_confidence"] = hypothesis_structured.get("confidence")
    except Exception:
        pass
    
    # Check for adaptive HITL - can we auto-approve based on user patterns?
    # CRITICAL FIX: Only apply adaptive HITL if approval wasn't forced by security checks
    if not auto_approve and not state.get("force_approval"):
        try:
            from app.services.adaptive_hitl_service import AdaptiveHITLService, RiskLevel
            
            user_id = state.get("user_id", "anonymous")
            adaptive_decision = await AdaptiveHITLService.should_request_approval(
                sql_query=sql_query,
                user_id=user_id,
                risk_level=validation_result.risk_level if hasattr(validation_result, 'risk_level') else RiskLevel.LOW
            )
            
            if adaptive_decision.decision.value == "auto_approve":
                state["needs_approval"] = False
                state["adaptive_hitl_applied"] = True
                state["adaptive_hitl_reason"] = adaptive_decision.reason
                logger.info(f"Adaptive HITL: Auto-approved query for user {user_id}: {adaptive_decision.reason}")
                span["output"]["adaptive_hitl"] = {
                    "applied": True,
                    "decision": "auto_approve",
                    "reason": adaptive_decision.reason,
                    "confidence": adaptive_decision.confidence,
                    "similar_approved_count": adaptive_decision.similar_approved_count
                }
            else:
                state["needs_approval"] = True
                if adaptive_decision.is_unusual_pattern:
                    logger.info(f"Adaptive HITL: Unusual pattern detected for user {user_id}: {adaptive_decision.reason}")
                span["output"]["adaptive_hitl"] = {
                    "applied": False,
                    "decision": "needs_approval",
                    "reason": adaptive_decision.reason,
                    "is_unusual_pattern": adaptive_decision.is_unusual_pattern
                }
                
        except Exception as e:
            logger.warning(f"Adaptive HITL check failed, falling back to standard approval: {e}")
            # Don't override needs_approval if it was already set
            if not state.get("needs_approval"):
                state["needs_approval"] = True
    elif auto_approve:
        # For auto_approve setting, disable approval unless critical security check failed
        # (which would have routed to error already) OR force_approval is set
        if not state.get("force_approval"):
            state["needs_approval"] = False

    # Enforce explicit approval for guarded conditions (scope/cartesian)
    # CRITICAL: This must be the FINAL check - don't allow anything to override force_approval
    if state.get("force_approval"):
        state["needs_approval"] = True
        logger.info(f"Force approval enabled - overriding all auto-approve logic")

    if validation_result.is_valid:
        if state["needs_approval"]:
            state["next_action"] = "await_approval"
            
            # Create session binding for approval security
            try:
                from app.services.session_binding_service import create_session_binding
                
                session_id = state.get("session_id", "unknown")
                user_id = state.get("user_id", "anonymous")
                correlation_id = state.get("correlation_id", query_id)
                
                # Get session context from state (stored at query start by middleware)
                ip_address = state.get("client_ip", "unknown")
                user_agent = state.get("user_agent", "unknown")
                
                await create_session_binding(
                    query_id=query_id,
                    session_id=session_id,
                    user_id=user_id,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    correlation_id=correlation_id
                )
                state["session_binding_created"] = True
                logger.info(f"Created session binding for approving query {query_id}")
            except Exception as e:
                logger.error(f"Failed to create session binding: {e}")
                # Don't fail validation - log error and continue
                state["session_binding_created"] = False
            
            logger.info(f"Query requires approval: {validation_result.risk_level.value} risk")
        else:
            state["next_action"] = "execute"
            auto_approve_status = "auto-approved (settings)" if auto_approve else "approved"
            logger.info(f"Query validated and {auto_approve_status}: {validation_result.risk_level.value} risk")
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

    # Skip probe for queries that don't work well with wrapping
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
