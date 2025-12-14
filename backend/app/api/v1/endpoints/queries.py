"""
Query Processing Endpoints
Natural language to SQL query processing via MCP
NOW WITH: RBAC, Rate Limiting, Audit Trail, SQL Validation, Timeout Enforcement
"""

import asyncio
import copy
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from app.services.query_service import QueryService
from app.services.doris_query_service import DorisQueryService
from app.services.query_state_manager import get_query_state_manager, QueryState as QueryExecState

# Constants
MAX_QUERY_LENGTH = 10000
MAX_SQL_LENGTH = 50000
QUERY_TIMEOUT_SECONDS = 600.0
QUERY_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{8,64}$")


def validate_query_id(query_id: str) -> str:
    """Validate query_id format to prevent injection attacks."""
    if not query_id or not QUERY_ID_PATTERN.match(query_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid query_id format. Must be 8-64 alphanumeric characters with dashes/underscores."
        )
    return query_id


from app.core.rbac import (
    rbac_manager, 
    require_analyst_role, 
    require_permission,
    Permission,
    Role
)
from app.core.rate_limiter import apply_rate_limit, RateLimitTier
from app.core.audit import audit_query_execution, audit_query_approval, audit_logger, AuditAction, AuditSeverity
from app.core.sql_validator import sql_validator, validate_sql
from app.core.security_middleware import input_sanitizer
from app.core.config import settings
from app.orchestrator.processor import validate_and_fix_state
from app.core.resilience import CircuitBreakerOpenError
from app.core.langfuse_client import (
    create_trace,
    get_langfuse_client,
    trace_span,
    update_trace,
)

router = APIRouter()
logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)  # auto_error=False allows optional auth

# Pydantic models
class QueryRequest(BaseModel):
    query: str
    connection_name: Optional[str] = "TestUserCSV"
    database_type: Optional[str] = "oracle"  # "oracle" or "doris"
    
    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 10000

class QueryResponse(BaseModel):
    query_id: str
    status: str
    message: str
    sql: Optional[str] = None
    results: Optional[Dict[str, Any]] = None
    execution_time_ms: Optional[int] = None
    timestamp: Optional[str] = None

class ApprovalRequest(BaseModel):
    approved: bool
    modified_sql: Optional[str] = None
    rejection_reason: Optional[str] = None
    execution_time_ms: Optional[int] = None
    timestamp: Optional[str] = None

@router.get("/connections")
async def list_connections(
    request: Request,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    List available database connections via MCP client
    
    **RBAC:** Requires viewer role or higher
    **Rate Limit:** 30 req/min for viewers, 100 req/min for analysts
    """
    # Apply rate limiting
    await apply_rate_limit(request, user, RateLimitTier(user["role"].value))
    
    try:
        connections_result = await QueryService.list_connections()
        
        if connections_result.get("status") == "success":
            return connections_result
        else:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to retrieve connections: {connections_result.get('message', 'Unknown error')}"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error listing connections: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Connection listing failed: {str(e)}"
        )


@router.post("/submit", response_model=QueryResponse)
async def submit_query(
    req: Request,
    request: QueryRequest,
    user: dict = Depends(require_analyst_role)
) -> QueryResponse:
    """
    Submit direct SQL query for execution
    
    **RBAC:** Requires analyst role or higher
    **Rate Limit:** 50 req/min for analysts, 500 req/min for admins
    **Security:** SQL injection validation, timeout enforcement, audit trail
    """
    from datetime import datetime, timezone
    
    start_time = time.time()
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Apply rate limiting
    await apply_rate_limit(req, user, RateLimitTier(user["role"].value))
    
    # Input validation
    if not request.query or not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    if len(request.query.strip()) > 10000:
        raise HTTPException(status_code=400, detail="Query too long (max 10000 characters)")
    
    # SQL validation and injection check
    sanitized_query = request.query.strip()
    validation_result = validate_sql(sanitized_query, user["role"].value)
    
    # Enforce row limit on SELECT queries to prevent unbounded results
    if validation_result.is_valid and validation_result.query_type.value == "SELECT":
        sanitized_query = sql_validator.enforce_row_limit(
            sanitized_query, 
            max_rows=1000, 
            dialect=request.database_type or "oracle"
        )
        logger.info(f"Applied row limit to SELECT query for user {user['username']}")
    
    if not validation_result.is_valid:
        # Audit failed query
        await audit_query_execution(
            user=user["username"],
            user_role=user["role"].value,
            sql_query=request.query.strip(),
            success=False,
            error=", ".join(validation_result.errors),
            ip_address=req.client.host if req.client else None,
        )
        
        raise HTTPException(
            status_code=400,
            detail={
                "error": "SQL validation failed",
                "errors": validation_result.errors,
                "warnings": validation_result.warnings,
            }
        )
    
    # Check if approval required
    if validation_result.requires_approval and user["role"] != Role.ADMIN:
        await audit_query_execution(
            user=user["username"],
            user_role=user["role"].value,
            sql_query=request.query.strip(),
            success=False,
            error="Requires admin approval",
            ip_address=req.client.host if req.client else None,
        )
        
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Query requires admin approval",
                "risk_level": validation_result.risk_level.value,
                "query_type": validation_result.query_type.value,
            }
        )
    
    try:
        logger.info(f"Processing SQL query: {request.query[:100]}...")
        
        # Execute with timeout enforcement (600 seconds for large tables)
        query_id = f"direct_sql_{uuid.uuid4().hex[:12]}"
        langfuse_client = get_langfuse_client()
        trace_metadata = {
            "entrypoint": "direct_sql_submit",
            "user": user["username"],
            "connection": request.connection_name or settings.oracle_default_connection,
            # Minimal frontend metadata: this path is driven by the Query Builder UI
            "frontend_surface": "query_builder",
        }
        langfuse_trace_id = create_trace(
            query_id=query_id,
            user_id=user["username"],
            user_query=request.query.strip(),
            metadata=trace_metadata,
        )
        trace_identifier = langfuse_trace_id or query_id

        try:
            async with trace_span(
                trace_identifier,
                "QueryService.execute_sql_query",
                input_data={
                    "query_preview": request.query[:200],
                    "connection": request.connection_name,
                },
                metadata={"user_role": user["role"].value},
            ) as submit_span:
                # Route direct SQL execution based on database_type
                if (request.database_type or "oracle").lower() == "doris":
                    execution_result = await asyncio.wait_for(
                        DorisQueryService.execute_sql_query(
                            sql_query=request.query.strip(),
                            user_id=user["username"],
                            user_role=user["role"].value,
                            request_id=query_id,
                        ),
                        timeout=600.0,
                    )
                else:
                    execution_result = await asyncio.wait_for(
                        QueryService.execute_sql_query(
                            sql_query=request.query.strip(),
                            connection_name=request.connection_name,
                            user_id=user["username"],
                            user_role=user["role"].value,
                            request_id=query_id,
                        ),
                        timeout=600.0,
                    )
                submit_span["output"] = {
                    "status": execution_result.get("status"),
                    "row_count": execution_result.get("results", {}).get("row_count") if isinstance(execution_result.get("results"), dict) else None,
                }
        except asyncio.TimeoutError:
            # Audit timeout
            # Audit timeout handled by ExecutionService
            if langfuse_client:
                try:
                    timeout_metadata = {
                        **trace_metadata,
                        "error": "Query execution timeout (600 seconds)",
                        "frontend_surface": "query_builder",
                    }
                    update_trace(
                        trace_identifier,
                        output_data={
                            "status": "timeout",
                            "query_id": query_id,
                        },
                        metadata=timeout_metadata,
                        tags=["timeout", "error"],
                    )
                    langfuse_client.flush()
                except Exception:
                    pass
            raise HTTPException(
                status_code=504,
                detail="Query execution timeout (600 seconds) - Consider optimizing query or adding indexes"
            )
        
        execution_time = int((time.time() - start_time) * 1000)
        
        if execution_result.get("status") == "success":
            row_count = execution_result.get("results", {}).get("row_count", 0)
            logger.info(f"Query executed successfully: {row_count} rows in {execution_time}ms")
            
            # Audit successful execution
            # Audit successful execution handled by ExecutionService
            
            response = QueryResponse(
                query_id=execution_result.get("query_id", f"query_{hash(request.query) % 10000}"),
                status="success",
                message="Query executed successfully",
                sql=execution_result.get("sql", request.query),
                results=execution_result.get("results"),
                execution_time_ms=execution_time,
                timestamp=timestamp
            )
            if langfuse_client:
                try:
                    success_metadata = {
                        **trace_metadata,
                        "connection": request.connection_name,
                    }
                    update_trace(
                        trace_identifier,
                        output_data={
                            "status": "success",
                            "query_id": response.query_id,
                            "execution_time_ms": execution_time,
                            "row_count": row_count,
                        },
                        metadata=success_metadata,
                        tags=["success"],
                    )
                    langfuse_client.flush()
                except Exception:
                    pass
            return response
        else:
            error_message = (
                execution_result.get("error")
                or execution_result.get("message")
                or "Query execution failed"
            )
            logger.error("Query execution failed: %s", error_message)
            
            # Audit failed execution
            # Audit failed execution handled by ExecutionService
            
            if langfuse_client:
                try:
                    error_metadata = {
                        **trace_metadata,
                        "error": error_message,
                        "connection": request.connection_name,
                    }
                    update_trace(
                        trace_identifier,
                        output_data={
                            "status": "error",
                            "query_id": execution_result.get("query_id", query_id),
                        },
                        metadata=error_metadata,
                        tags=["error"],
                    )
                    langfuse_client.flush()
                except Exception:
                    pass
            raise HTTPException(status_code=400, detail=error_message)
            
    except CircuitBreakerOpenError as e:
        logger.error("SQL execution circuit open: %s", e, exc_info=True)
        # Surface as a service-unavailable style error for the caller
        raise HTTPException(
            status_code=503,
            detail="SQL execution temporarily unavailable due to repeated upstream failures. Please retry later.",
        )
    except HTTPException:
        raise
    except Exception as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"Query submission failed after {execution_time}ms: {e}", exc_info=True)
        if 'trace_identifier' in locals() and langfuse_client:
            try:
                exception_metadata = {
                    **trace_metadata,
                    "error": str(e),
                    "connection": request.connection_name,
                }
                update_trace(
                    trace_identifier,
                    output_data={
                        "status": "error",
                        "query_id": query_id,
                    },
                    metadata=exception_metadata,
                    tags=["error"],
                )
                langfuse_client.flush()
            except Exception:
                pass
        
        # Audit exception
        # Audit exception handled by ExecutionService
        
        raise HTTPException(
            status_code=500, 
            detail=f"Query processing failed: {str(e)}"
        )


@router.get("/{query_id}/status")
async def get_query_status(
    query_id: str,
    user: Optional[dict] = Depends(rbac_manager.get_current_user_optional),
) -> Dict[str, Any]:
    """
    Get query processing status from state manager.
    
    Returns current state and metadata for a query.
    Authorization: Query owner or admin can view full metadata.
    """
    # Validate query_id format
    validate_query_id(query_id)
    
    try:
        state_manager = await get_query_state_manager()
        
        # Get persistent metadata
        query_metadata = await state_manager.get_query_metadata(query_id)
        
        # Get current state
        current_state = await state_manager.get_state(query_id)
        
        if query_metadata:
            # Authorization check: owner or admin sees full metadata
            is_owner = user and (
                user.get("username") == query_metadata.get("username") or
                user.get("username") == query_metadata.get("user_id")
            )
            is_admin = user and user.get("role") == Role.ADMIN
            
            if is_owner or is_admin:
                return {
                    "query_id": query_id,
                    "status": current_state.value if current_state else query_metadata.get("status", "unknown"),
                    "message": "Query status retrieved",
                    "metadata": query_metadata,
                }
            else:
                # Non-owner gets limited info
                return {
                    "query_id": query_id,
                    "status": current_state.value if current_state else query_metadata.get("status", "unknown"),
                    "message": "Query status retrieved",
                }
        
        # Check if we have state but no metadata (legacy queries)
        if current_state:
            return {
                "query_id": query_id,
                "status": current_state.value,
                "message": "Query status retrieved (no metadata)",
            }
        
        return {
            "query_id": query_id,
            "status": "not_found",
            "message": "Query not found or expired",
        }
    except Exception as e:
        logger.warning(f"Failed to get query status for {query_id}: {e}")
        return {
            "query_id": query_id,
            "status": "unknown",
            "message": "Unable to retrieve query status",
        }


@router.post("/{query_id}/approve")
async def approve_query(
    query_id: str,
    request: ApprovalRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Approve or reject query execution and resume orchestrator workflow.
    
    This endpoint handles the HITL (Human-in-the-Loop) approval flow:
    1. Graph pauses at await_approval node (interrupt_before)
    2. User reviews SQL and calls this endpoint
    3. State is updated in checkpoint
    4. Graph resumes from await_approval node
    
    Security:
    - Only admin and analyst roles can approve
    - Modified SQL is re-validated for injection
    - Idempotency check prevents duplicate executions
    """
    from app.core.client_registry import registry
    
    # Validate query_id format
    validate_query_id(query_id)
    
    # Verify user has approval permission
    if user["role"] not in [Role.ADMIN, Role.ANALYST]:
        raise HTTPException(
            status_code=403,
            detail="Only admin and analyst roles can approve queries"
        )
    
    # SECURITY: Verify query ownership before allowing approval/rejection
    state_manager = await get_query_state_manager()
    query_metadata = await state_manager.get_query_metadata(query_id)
    
    if query_metadata:
        query_owner = query_metadata.get("user_id") or query_metadata.get("username")
        # Only the query owner or admin can approve/reject
        if query_owner and query_owner != user.get("username") and user.get("role") != Role.ADMIN:
            logger.warning(f"Approval denied: {user['username']} tried to approve query owned by {query_owner}")
            raise HTTPException(
                status_code=403,
                detail="You can only approve/reject your own queries (unless admin)"
            )
    elif not settings.is_development:
        # In production, reject approval for unregistered queries
        raise HTTPException(
            status_code=404,
            detail="Query not found or not registered"
        )
    
    # Import approval service for idempotency and risk assessment
    from app.services.approval_service import ApprovalService
    
    # Check idempotency - prevent duplicate approvals
    pending_approval = await ApprovalService.get_pending(query_id)
    original_sql = pending_approval.get("original_sql", "") if pending_approval else ""
    sql_to_check = request.modified_sql if request.modified_sql else original_sql
    
    if await ApprovalService.check_idempotency(query_id, sql_to_check):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "Duplicate approval",
                "message": "This query has already been approved and executed",
                "query_id": query_id,
            }
        )
    
    # Validate modified SQL if provided (prevent SQL injection)
    if request.modified_sql:
        validation_result = validate_sql(request.modified_sql, user["role"].value)
        if not validation_result.is_valid:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Modified SQL validation failed",
                    "errors": validation_result.errors,
                    "warnings": validation_result.warnings,
                }
            )
        
        # Re-assess risk for modified SQL
        risk_reassessment = await ApprovalService.reassess_modified_sql(
            query_id,
            request.modified_sql,
            user["role"].value
        )
        
        # If risk increased significantly, require additional confirmation
        if risk_reassessment.get("risk_increased"):
            logger.warning(
                f"Risk increased for modified SQL in query {query_id}: "
                f"{risk_reassessment.get('original_risk')} -> {risk_reassessment.get('new_risk')}"
            )
    
    # Get orchestrator from registry
    orchestrator = registry.get_query_orchestrator()
    if not orchestrator:
        raise HTTPException(
            status_code=500,
            detail="Query orchestrator not available"
        )
    
    try:
        # Configuration for checkpoint access
        config = {
            "configurable": {
                "thread_id": query_id,
            }
        }
        
        # Get current state from checkpoint
        state_snapshot = await orchestrator.aget_state(config)
        
        if not state_snapshot or not state_snapshot.values:
            # Debugging 404 error
            logger.error(f"Query {query_id} state NOT found in checkpoint. Config: {config}")
            try:
                # List recent threads to see what IS there
                recent_threads = [c.config["configurable"]["thread_id"] async for c in checkpointer.alist(config={"configurable": {}})]
                logger.info(f"Available checkpoint threads: {recent_threads[:20]}")
            except Exception as e:
                logger.error(f"Failed to list checkpoints for debug: {e}")
                
            raise HTTPException(
                status_code=404,
                detail=f"Query {query_id} not found or already completed"
            )
        
        current_state = dict(state_snapshot.values)  # Make a mutable copy
        
        # Handle rejection
        if not request.approved:
            # Update state for rejection
            rejection_updates = {
                "approved": False,
                "needs_approval": False,
                "error": request.rejection_reason or "Query rejected by user",
                "next_action": "rejected",
            }
            
            # Update checkpoint state
            await orchestrator.aupdate_state(config, rejection_updates)

            # Notify SSE subscribers that the query was rejected
            try:
                qs_manager = await get_query_state_manager()
                await qs_manager.update_state(
                    query_id,
                    QueryExecState.REJECTED,
                    {"error": rejection_updates["error"]},
                )
            except Exception as e:
                logger.warning(f"Failed to emit SSE rejection state for {query_id}: {e}")
            
            await audit_query_approval(
                user=user["username"],
                user_role=user["role"].value,
                query_id=query_id,
                approved=False,
            )
            
            # Resume graph to complete rejection flow
            try:
                await asyncio.wait_for(
                    orchestrator.ainvoke(None, config),
                    timeout=30.0
                )
            except Exception as e:
                logger.warning(f"Rejection flow completion warning: {e}")
            
            return {
                "query_id": query_id,
                "status": "rejected",
                "message": "Query rejected by user",
                "error": request.rejection_reason,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        
        # Prepare approval state updates
        approval_updates = {
            "approved": True,
            "needs_approval": False,
            "execution_result": {},  # Clear for fresh execution
            "result_analysis": {},
        }
        
        # Apply modified SQL if provided
        if request.modified_sql:
            logger.info(f"User modified SQL for query {query_id}")
            approval_updates["sql_query"] = request.modified_sql
        
        # Update checkpoint state with approval
        await orchestrator.aupdate_state(config, approval_updates)
        
        # Mark approval with idempotency key to prevent duplicates
        final_sql = request.modified_sql if request.modified_sql else current_state.get("sql_query", "")
        await ApprovalService.mark_approved(query_id, final_sql, user["username"])

        # Notify SSE subscribers that execution is resuming
        qs_manager = None
        try:
            qs_manager = await get_query_state_manager()
        except Exception as e:
            logger.warning(f"Failed to obtain QueryStateManager: {e}")

        if qs_manager:
            try:
                await qs_manager.update_state(
                    query_id,
                    QueryExecState.APPROVED,
                    {
                        "sql": final_sql or current_state.get("sql_query", ""),
                        "approval_context": current_state.get("approval_context"),
                    },
                )
                await qs_manager.update_state(
                    query_id,
                    QueryExecState.EXECUTING,
                    {
                        "sql": final_sql or current_state.get("sql_query", ""),
                        "thinking_steps": current_state.get("llm_metadata", {}).get("thinking_steps"),
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to emit SSE execution state for {query_id}: {e}")

        # Resume execution with timeout enforcement (600 seconds)
        logger.info(f"Query {query_id} approved by {user['username']}, resuming execution...")
        
        # Create trace identifier for observability
        trace_identifier = current_state.get("trace_id") or query_id
        langfuse_client = get_langfuse_client()
        
        try:
            async with trace_span(
                trace_identifier,
                "orchestrator.resume_execution",
                input_data={
                    "query_id": query_id,
                    "approver": user["username"],
                },
                metadata={
                    "user_role": user["role"].value,
                    "resume_reason": "approval",
                },
            ) as resume_span:
                # Resume from checkpoint by passing None as input
                # This continues execution from where it was interrupted
                final_state = await asyncio.wait_for(
                    orchestrator.ainvoke(None, config),
                    timeout=600.0
                )
                resume_span["output"] = {
                    "status": "completed",
                    "query_id": query_id,
                    "error": final_state.get("error") if final_state else None,
                }
        except asyncio.TimeoutError:
            logger.error(f"Query {query_id} execution timeout (600s)")
            if qs_manager:
                try:
                    await qs_manager.update_state(
                        query_id,
                        QueryExecState.ERROR,
                        {"error": "Execution timeout after approval"},
                    )
                except Exception:
                    pass
            if langfuse_client:
                try:
                    update_trace(
                        trace_identifier,
                        output_data={
                            "status": "timeout",
                            "query_id": query_id,
                        },
                        metadata={
                            "error": "Execution timeout after approval",
                            "frontend_surface": "chat",
                        },
                        tags=["timeout", "error"],
                    )
                    langfuse_client.flush()
                except Exception:
                    pass
            raise HTTPException(
                status_code=504,
                detail="Query orchestrator execution timeout (600 seconds) - Consider simplifying the query"
            )

        # Normalise final state to avoid missing dict structures
        final_state = validate_and_fix_state(final_state or {})

        execution_result = final_state.get("execution_result")
        execution_result_copy = copy.deepcopy(execution_result) if execution_result else None

        raw_llm_metadata = final_state.get("llm_metadata", {})
        if not isinstance(raw_llm_metadata, dict):
            raw_llm_metadata = {}
            final_state["llm_metadata"] = {}

        error_message = final_state.get("error")
        status = "error" if error_message else "success"
        message = error_message or "Query approved and executed successfully"

        # Normalize result structure - ensure consistent format
        normalized_result = None
        row_count = 0
        rows_requested = None
        if execution_result_copy and isinstance(execution_result_copy, dict):
            # CRITICAL: Use flat structure consistently (no nested 'results')
            normalized_result = {
                "columns": execution_result_copy.get("columns", []),
                "rows": execution_result_copy.get("rows", []),
                "row_count": execution_result_copy.get("row_count", len(execution_result_copy.get("rows", []))),
                "execution_time_ms": execution_result_copy.get("execution_time_ms", 0),
                "timestamp": execution_result_copy.get("timestamp", ""),
                "status": execution_result_copy.get("status", "success"),
            }
            row_count = normalized_result["row_count"]
            rows_requested = execution_result_copy.get("rows_requested")
            logger.info(f"Approval result normalized: {len(normalized_result['columns'])} columns, {len(normalized_result['rows'])} rows")

        response_payload = {
            "query_id": query_id,
            "status": status,
            "completion_state": "completed" if status == "success" else "error",
            "message": message,
            "sql_query": final_state.get("sql_query"),
            "results": normalized_result,  # Standard field name used across the codebase
            "row_count": row_count,
            "rows_requested": rows_requested,
            "error": error_message,
            "llm_metadata": raw_llm_metadata,
            "insights": final_state.get("insights"),
            "suggested_queries": final_state.get("suggested_queries"),
            "visualization": final_state.get("visualization_hints"),
            "visualization_hints": final_state.get("visualization_hints"),
            "result_analysis": final_state.get("result_analysis"),
            "sql_confidence": final_state.get("sql_confidence"),
            "optimization_suggestions": final_state.get("optimization_suggestions"),
            "needs_approval": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if qs_manager:
            try:
                if status == "success":
                    await qs_manager.update_state(
                        query_id,
                        QueryExecState.FINISHED,
                        {
                            "sql": response_payload["sql_query"],
                            "result": execution_result_copy,
                            "insights": response_payload.get("insights"),
                            "suggested_queries": response_payload.get("suggested_queries"),
                        },
                    )
                else:
                    await qs_manager.update_state(
                        query_id,
                        QueryExecState.ERROR,
                        {"error": message},
                    )
            except Exception as e:
                logger.warning(f"Failed to publish final SSE state for {query_id}: {e}")

        if langfuse_client:
            try:
                update_trace(
                    trace_identifier,
                    output_data={
                        "status": status,
                        "query_id": query_id,
                        "row_count": row_count,
                        "execution_time_ms": execution_result_copy.get("execution_time_ms") if execution_result_copy else None,
                    },
                    metadata={
                        "approver": user["username"],
                        "needs_approval": False,
                        "error": error_message,
                        "frontend_surface": "chat",
                    },
                    tags=[status],
                )
                langfuse_client.flush()
            except Exception:
                pass

        # Audit approval
        await audit_query_approval(
            user=user["username"],
            user_role=user["role"].value,
            query_id=query_id,
            approved=True,
        )
        
        return response_payload

    except Exception as e:
        logger.error(f"Failed to approve and execute query {query_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to approve and execute query: {str(e)}"
        )


@router.post("/{query_id}/reject")
async def reject_query(
    query_id: str,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Reject query execution
    """
    # Validate query_id format
    validate_query_id(query_id)
    
    # Verify user has approval permission
    if user["role"] not in [Role.ADMIN, Role.ANALYST]:
        raise HTTPException(
            status_code=403,
            detail="Only admin and analyst roles can reject queries"
        )
    
    try:
        # Audit rejection
        await audit_query_approval(
            user=user["username"],
            user_role=user["role"].value,
            query_id=query_id,
            approved=False,
        )
        
        logger.info(f"Query {query_id} rejected by {user['username']}")
        
        return {
            "query_id": query_id,
            "message": "Query rejected",
            "status": "rejected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error("Failed to reject query %s: %s", query_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reject query: {str(e)}"
        )


@router.get("/{query_id}/stream")
async def stream_query_state(
    query_id: str,
    request: Request,
    token: Optional[str] = None,
    auth_user: Optional[dict] = Depends(rbac_manager.get_current_user_optional),
) -> StreamingResponse:
    """
    Stream real-time query state changes via Server-Sent Events (SSE)
    
    **Auth:** Supports header-based auth (Bearer token) or query param (legacy)
    **Rate Limit:** Applied to prevent resource exhaustion
    **Usage:** GET /api/v1/queries/{query_id}/stream
    
    Streams query lifecycle states:
    - received -> planning -> prepared -> pending_approval -> executing -> finished
    
    Args:
        query_id: Unique query identifier to stream state for
        request: FastAPI request object
        token: Optional auth token as query parameter (legacy)
        auth_user: User authenticated via header (preferred)
    
    Returns:
        StreamingResponse with text/event-stream content type
    """
    from app.core.rate_limiter import rate_limiter, RateLimitTier
    from app.core.audit import audit_sse_access
    
    # Validate query_id format
    validate_query_id(query_id)
    
    # Prioritize header-based auth
    user = auth_user
    
    # Fallback to query parameter if header auth missing
    if not user and token:
        try:
            from app.core.auth import AuthenticationManager
            auth_manager = AuthenticationManager()
            payload = auth_manager.decode_token(token, token_type="access")
            if payload:
                username = payload.get("sub")
                role = payload.get("role", "viewer")
                user = {"username": username, "role": Role(role)}
        except Exception as e:
            logger.warning(f"SSE query param auth failed: {e}")
    
    if not user:
        if settings.is_development:
            logger.warning(f"SSE stream accessed without auth for query {query_id[:8]}")
            user = {"username": "anonymous", "role": Role.VIEWER}
        else:
            raise HTTPException(status_code=401, detail="Authentication required for SSE")
    
    # Rate limiting for SSE endpoint to prevent resource exhaustion
    try:
        user_tier = RateLimitTier(user["role"].value) if hasattr(user["role"], "value") else RateLimitTier.VIEWER
        await rate_limiter.check_rate_limit(
            user=user["username"],
            endpoint="/api/v1/queries/stream",
            tier=user_tier
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"SSE rate limit check failed: {e}")
    
    # Audit SSE access
    try:
        await audit_sse_access(
            user=user["username"],
            user_role=user["role"].value if hasattr(user["role"], "value") else str(user["role"]),
            query_id=query_id,
            ip_address=request.client.host if request.client else None,
        )
    except Exception as e:
        logger.warning(f"SSE audit logging failed: {e}")
    
    logger.info(f"Starting SSE stream for query {query_id[:8]}... (user: {user['username']})")
    
    # Get query state manager instance
    state_manager = await get_query_state_manager()
    
    # Authorization check: verify user owns this query or has admin role
    query_metadata = await state_manager.get_query_metadata(query_id)
    
    # SECURITY: If no metadata exists, the query was never registered
    # In production, reject unregistered queries to prevent enumeration
    if not query_metadata:
        if not settings.is_development:
            logger.warning(f"SSE access denied for unregistered query {query_id[:8]} by {user['username']}")
            raise HTTPException(
                status_code=404,
                detail="Query not found or not registered"
            )
        else:
            logger.warning(f"DEV: Allowing SSE for unregistered query {query_id[:8]}")
    else:
        # Verify ownership
        query_owner = query_metadata.get("user_id") or query_metadata.get("username")
        if query_owner and query_owner != user.get("username") and user.get("role") != Role.ADMIN:
            logger.warning(f"SSE access denied: {user['username']} tried to access query owned by {query_owner}")
            raise HTTPException(
                status_code=403,
                detail="You do not have permission to view this query's state"
            )
    
    async def event_generator():
        """
        Generator function for SSE stream.
        
        Uses QueryStateManager.subscribe() which returns an async generator
        that yields SSE-formatted strings directly.
        """
        try:
            # subscribe() is an async generator - iterate directly
            async for message in state_manager.subscribe(query_id):
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.info(f"Client disconnected from SSE stream {query_id[:8]}")
                    break
                yield message
        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for {query_id[:8]}")
        except Exception as e:
            logger.error(f"SSE stream error for {query_id[:8]}: {e}", exc_info=True)
            yield f"event: error\ndata: {{\"message\": \"{str(e)}\"}}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable Nginx buffering
        }
    )


# ==================== ORCHESTRATOR ENDPOINT ====================

class OrchestratorQueryRequest(BaseModel):
    """Request model for orchestrator-based query processing"""
    query: str
    user_id: Optional[str] = "default_user"
    session_id: Optional[str] = None
    database_type: Optional[str] = "oracle"  # "oracle" or "doris"
    
    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 10000


class OrchestratorQueryResponse(BaseModel):
    """Response model for orchestrator-based query processing"""
    query_id: str
    status: str
    sql_query: Optional[str] = None
    validation: Optional[Dict[str, Any]] = None
    results: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    visualization: Optional[Dict[str, Any]] = None
    needs_approval: Optional[bool] = False
    llm_metadata: Optional[Dict[str, Any]] = None  # LLM verification metadata
    error: Optional[str] = None
    timestamp: Optional[str] = None
    # Enhancements
    sql_explanation: Optional[str] = None
    insights: Optional[list[str]] = None
    suggested_queries: Optional[list[str]] = None
    approval_context: Optional[Dict[str, Any]] = None
    clarification_message: Optional[str] = None
    clarification_details: Optional[Dict[str, Any]] = None
    sql_confidence: Optional[int] = None
    optimization_suggestions: Optional[list[Dict[str, Any]]] = None
    # Conversational response fields
    message: Optional[str] = None
    is_conversational: Optional[bool] = None  # None = not set, avoids always serializing
    intent: Optional[str] = None

    class Config:
        extra = "allow"
        # Exclude None values from serialization to reduce payload size
        json_encoders = {type(None): lambda v: None}


async def _get_user_or_default(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> dict:
    """
    Get authenticated user or return restricted default for development only.
    
    Security:
    - In production: Requires authentication, raises 401 if missing
    - In development: Returns a VIEWER role (not admin) for testing convenience
    """
    if credentials:
        return await rbac_manager.get_current_user(credentials)
    
    # No credentials provided
    if settings.is_development:
        logger.warning("No auth credentials provided, using default VIEWER user for development")
        return {"username": "dev_user", "role": Role.VIEWER}
    else:
        # Production: require authentication
        raise HTTPException(
            status_code=401,
            detail="Authentication required"
        )

@router.post("/process", response_model=OrchestratorQueryResponse)
async def process_query_with_orchestrator(
    req: Request,
    request: OrchestratorQueryRequest,
    user: dict = Depends(_get_user_or_default)
) -> OrchestratorQueryResponse:
    """
    Process natural language query through LangGraph orchestrator with Graphiti context
    
    **RBAC:** Requires analyst role or higher for query execution
    **Rate Limit:** Applied based on user role
    
    This endpoint uses the full AI pipeline:
    1. Understand query intent
    2. Retrieve context from Graphiti knowledge graph
    3. Generate SQL with LLM (Gemini/Bedrock)
    4. Validate SQL for security
    5. Execute via MCP client
    6. Format results with visualization hints
    7. Store episode in Graphiti for future context
    
    Returns:
        OrchestratorQueryResponse with SQL, results, and visualization hints
    """
    import time
    from datetime import datetime, timezone
    import uuid
    
    start_time = time.time()
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # RBAC: Verify user has query execution permission
    # Viewers can only view results, not execute new queries
    if user["role"] not in [Role.ADMIN, Role.ANALYST]:
        # Return proper HTTP 403 for permission errors
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Insufficient permissions. Analyst role or higher required to execute queries.",
                "error_type": "permission_denied",
                "user_role": user["role"].value,
            }
        )
    
    # Apply rate limiting - let HTTPException propagate with proper 429 status
    await apply_rate_limit(req, user, RateLimitTier(user["role"].value))
    
    # Input validation and sanitization (OWASP LLM01 Defense)
    if not request.query or not request.query.strip():
        # CRITICAL: Return structured error response for validation failures
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error="Query cannot be empty",
            timestamp=timestamp,
            sql_query=None,
            validation=None,
            results=None,
            result=None,
            visualization=None,
            needs_approval=False,
            llm_metadata={"validation_error": "empty_query"},
        )
    
    if len(request.query.strip()) > 10000:
        # CRITICAL: Return structured error response for validation failures
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error="Query too long (max 10000 characters)",
            timestamp=timestamp,
            sql_query=None,
            validation=None,
            results=None,
            result=None,
            visualization=None,
            needs_approval=False,
            llm_metadata={"validation_error": "query_too_long", "length": len(request.query.strip())},
        )
    
    # Sanitize user input to prevent prompt injection
    try:
        sanitized_input, warnings = input_sanitizer.sanitize_input(request.query)
        if warnings:
            logger.warning(f"Input sanitization warnings: {warnings}")
        request.query = sanitized_input
    except ValueError as e:
        logger.error(f"Input sanitization failed: {e}")
        # CRITICAL: Return structured error response for input validation failures
        error_msg = f"Input validation failed: {str(e)}"
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error=error_msg,
            timestamp=timestamp,
            sql_query=None,
            validation=None,
            results=None,
            result=None,
            visualization=None,
            needs_approval=False,
            llm_metadata={"validation_error": str(e), "type": "prompt_injection_detected"},
        )
    
    try:
        logger.info(f"Processing query via orchestrator: {request.query[:100]}...")
        logger.info(f"  User: {user['username']}, Session: {request.session_id or 'auto-generated'}")
        logger.info(f"  Database type: {request.database_type or 'oracle'}")
        
        # Generate query_id upfront and register BEFORE calling service
        # This fixes race condition where SSE auth check may fail for fast queries
        pre_generated_query_id = f"query_{uuid.uuid4().hex[:12]}"
        
        # Register query with state manager BEFORE processing starts
        # This ensures SSE subscribers can authenticate immediately
        try:
            state_manager = await get_query_state_manager()
            await state_manager.register_query(
                query_id=pre_generated_query_id,
                user_id=user["username"],
                username=user["username"],
                session_id=request.session_id,
                database_type=request.database_type or "oracle",
                trace_id=None,  # Will be updated after service call
            )
        except Exception as reg_err:
            logger.warning(f"Failed to pre-register query {pre_generated_query_id}: {reg_err}")
        
        # Call service layer with pre-generated query_id
        result = await QueryService.submit_natural_language_query(
            user_query=request.query,
            user_id=user["username"],  # Use authenticated user
            session_id=request.session_id,
            user_role=user["role"].value,  # Pass user role for RBAC
            database_type=request.database_type or "oracle",
            thread_id_override=pre_generated_query_id,  # Use pre-generated ID
        )

        # Audit the successful submission of the NL query
        await audit_logger.log(
            action=AuditAction.QUERY_SUBMIT,
            user=user["username"],
            user_role=user["role"].value,
            success=True,
            resource="nl_query",
            resource_id=pre_generated_query_id,
            details={
                "query": request.query[:1000], # Log the prompt
                "session_id": request.session_id,
                "database_type": request.database_type,
            },
            ip_address=req.client.host if req.client else None,
            session_id=request.session_id,
        )
        
        # Update registration with trace_id if available
        query_id = result.get("query_id", pre_generated_query_id)
        if result.get("trace_id"):
            try:
                # Update metadata with trace_id
                metadata = await state_manager.get_query_metadata(query_id)
                if metadata:
                    async with state_manager._metadata_lock:
                        if query_id in state_manager._query_metadata:
                            state_manager._query_metadata[query_id].trace_id = result.get("trace_id")
            except Exception as upd_err:
                logger.warning(f"Failed to update trace_id for {query_id}: {upd_err}")
        
        execution_time = int((time.time() - start_time) * 1000)
        logger.info(f"Orchestrator completed in {execution_time}ms")
        
        # Transform orchestrator result to API response
        # CRITICAL: Only set error field if status is actually "error"
        result_status = result.get("status", "error")
        result_error = result.get("error") if result_status == "error" else None
        
        return OrchestratorQueryResponse(
            query_id=result.get("query_id", f"query_{uuid.uuid4().hex[:8]}"),
            status=result_status,
            sql_query=result.get("sql_query"),
            validation=result.get("validation"),
            results=result.get("results"),
            result=result.get("result"),
            visualization=result.get("visualization"),
            needs_approval=result.get("needs_approval", False),
            llm_metadata=result.get("llm_metadata"),  # Include LLM verification data
            error=result_error,
            timestamp=timestamp,
            sql_explanation=result.get("sql_explanation"),
            insights=result.get("insights"),
            suggested_queries=result.get("suggested_queries"),
            approval_context=result.get("approval_context"),
            clarification_message=result.get("clarification_message"),
            clarification_details=result.get("clarification_details"),
            sql_confidence=result.get("sql_confidence"),
            optimization_suggestions=result.get("optimization_suggestions"),
            # Conversational response fields
            message=result.get("message"),
            is_conversational=result.get("is_conversational", False),
            intent=result.get("intent"),
        )
        
    except Exception as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"Orchestrator failed after {execution_time}ms: {e}", exc_info=True)
        # CRITICAL: Return structured error response instead of raising HTTPException
        # This ensures frontend receives proper error object with status, error, query_id
        error_msg = f"Query orchestration failed: {str(e)}"
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error=error_msg,
            timestamp=timestamp,
            sql_query=None,
            validation=None,
            results=None,
            result=None,
            visualization=None,
            needs_approval=False,
            llm_metadata={"exception": str(e), "execution_time_ms": execution_time},
        )


class ClarificationRequest(BaseModel):
    """Request model for providing clarification to reprocess query"""
    query_id: str
    clarification: str
    original_query: Optional[str] = None
    database_type: Optional[str] = None
    
    class Config:
        str_strip_whitespace = True
        min_anystr_length = 1
        max_anystr_length = 5000


class ReportRequest(BaseModel):
    """Request model for generating executive reports"""
    query_results: list[Dict[str, Any]]
    format: str = "html"  # html, pdf, docx
    title: Optional[str] = None
    user_queries: Optional[list[str]] = None


@router.post("/report")
async def generate_report(
    request: ReportRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Generate an executive report from query results
    
    Args:
        request: ReportRequest with query results and format
        
    Returns:
        Dict with report content (base64 encoded for binary formats)
    """
    from app.services.report_generation_service import ReportGenerationService
    import base64
    
    if not request.query_results:
        raise HTTPException(status_code=400, detail="Query results are required")
    
    if request.format.lower() not in ["html", "pdf", "docx"]:
        raise HTTPException(status_code=400, detail="Format must be html, pdf, or docx")
    
    try:
        # Use LLM-enhanced insights by default for better reports
        result = await ReportGenerationService.generate_report_with_llm_insights(
            query_results=request.query_results,
            format=request.format,
            title=request.title,
            user_queries=request.user_queries,
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=400, detail=result.get("message"))
        
        # Encode binary content as base64
        content = result.get("content")
        if isinstance(content, bytes):
            result["content"] = base64.b64encode(content).decode("utf-8")
            result["encoding"] = "base64"
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Report generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


class VisualizationRequest(BaseModel):
    """Request model for generating Python-based visualizations"""
    columns: list[str]
    rows: list[list]
    chart_type: Optional[str] = None  # bar, line, pie, scatter, area, heatmap
    title: Optional[str] = None
    # Enhanced visualization options
    show_mean: Optional[bool] = True  # Show mean line on bar/line charts
    show_peaks: Optional[bool] = False  # Annotate peak values
    color_scheme: Optional[str] = None  # Custom color scheme
    
    class Config:
        str_strip_whitespace = True


@router.post("/visualize")
async def generate_visualization(
    request: VisualizationRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Generate Python-based visualization using Plotly
    
    Returns Plotly JSON that can be rendered by the frontend using plotly.js
    
    Enhanced with statistical overlays and annotations
    
    Args:
        request: VisualizationRequest with columns, rows, and optional chart_type
        
    Returns:
        Dict with plotly_json for frontend rendering
    """
    from app.services.visualization_service import VisualizationService
    
    if not request.columns or not request.rows:
        raise HTTPException(status_code=400, detail="Columns and rows are required")
    
    if len(request.rows) > 10000:
        raise HTTPException(status_code=400, detail="Too many rows for visualization (max 10000)")
    
    try:
        # Pass enhanced hints to visualization service
        hints = {
            "show_mean": request.show_mean,
            "show_peaks": request.show_peaks,
            "color_scheme": request.color_scheme,
        }
        
        result = VisualizationService.generate_chart(
            columns=request.columns,
            rows=request.rows,
            chart_type=request.chart_type,
            title=request.title,
            hints=hints
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Visualization generation failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "fallback": "recharts"
        }


@router.post("/clarify", response_model=OrchestratorQueryResponse)
async def reprocess_with_clarification(
    request: ClarificationRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> OrchestratorQueryResponse:
    """
    Reprocess query with user-provided clarification
    
    When the orchestrator requests clarification (e.g., unmapped columns),
    the user can provide additional context or explicit mappings here.
    
    Example clarification formats:
    - "Use REVENUE column for 'total sales'"
    - "customer_count means COUNT(DISTINCT CUSTOMER_ID)"
    - "Use the DATE column in format DD/MM/YYYY"
    
    Returns:
        OrchestratorQueryResponse with SQL generated using clarification
    """
    import time
    from datetime import datetime, timezone
    
    start_time = time.time()
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Validation
    if not request.clarification or not request.clarification.strip():
        raise HTTPException(status_code=400, detail="Clarification cannot be empty")
    
    if len(request.clarification.strip()) > 5000:
        raise HTTPException(status_code=400, detail="Clarification too long (max 5000 characters)")
    
    try:
        logger.info(f"Reprocessing query {request.query_id[:8]}... with clarification")
        logger.info(f"  Clarification: {request.clarification[:100]}...")
        
        # Try to resume from checkpoint with clarification context
        from app.core.client_registry import registry
        
        orchestrator = registry.get_query_orchestrator()
        checkpoint_state = None
        
        if orchestrator:
            try:
                config = {"configurable": {"thread_id": request.query_id}}
                state_snapshot = await orchestrator.aget_state(config)
                if state_snapshot and state_snapshot.values:
                    checkpoint_state = state_snapshot.values
                    logger.info(f"Found checkpoint state for query {request.query_id[:8]}")
            except Exception as e:
                logger.warning(f"Could not retrieve checkpoint state: {e}")
        
        # Combine original query with clarification for reprocessing
        combined_query = request.clarification
        if request.original_query:
            combined_query = f"{request.original_query}\n\nClarification: {request.clarification}"
        
        # If we have checkpoint state, preserve clarification history
        clarification_history = []
        if checkpoint_state:
            # Get existing clarification history
            clarification_history = checkpoint_state.get("clarification_history", [])
            clarification_history.append({
                "clarification": request.clarification,
                "timestamp": timestamp,
            })
            # Preserve context from previous attempt
            if checkpoint_state.get("context"):
                logger.info(f"Preserving context from checkpoint (schema, enriched_schema)")
        
        # Call service layer with clarification as enhanced query
        result = await QueryService.submit_natural_language_query(
            user_query=combined_query,
            user_id=user["username"],  # Use authenticated user
            session_id=request.query_id,  # Use query_id as session for continuity
            user_role=user["role"].value,  # Pass user role for RBAC
            timeout=600.0,
            thread_id_override=request.query_id,
            database_type=request.database_type or "oracle",
            # Pass clarification history for context preservation
            conversation_history=[
                {"role": "user", "content": request.original_query or ""},
                {"role": "assistant", "content": "Clarification needed"},
                {"role": "user", "content": request.clarification},
            ] if request.original_query else None,
        )
        
        execution_time = int((time.time() - start_time) * 1000)
        logger.info(f"Clarification reprocessing completed in {execution_time}ms")
        
        return OrchestratorQueryResponse(
            query_id=request.query_id,
            status=result.get("status", "error"),
            sql_query=result.get("sql_query"),
            validation=result.get("validation"),
            result=result.get("result"),
            results=result.get("result") or result.get("results"),
            visualization=result.get("visualization"),
            needs_approval=result.get("needs_approval", False),
            llm_metadata=result.get("llm_metadata"),
            error=result.get("error") or result.get("message"),
            timestamp=timestamp,
            sql_explanation=result.get("sql_explanation"),
            insights=result.get("insights"),
            suggested_queries=result.get("suggested_queries"),
            approval_context=result.get("approval_context"),
            clarification_message=result.get("clarification_message"),
            clarification_details=result.get("clarification_details"),
            sql_confidence=result.get("sql_confidence"),
            optimization_suggestions=result.get("optimization_suggestions"),
        )
        
    except Exception as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"Clarification reprocessing failed after {execution_time}ms: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Clarification processing failed: {str(e)}"
        )
