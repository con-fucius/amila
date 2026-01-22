
import asyncio
import copy
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request, Depends

from app.services.query_state_manager import get_query_state_manager, QueryState as QueryExecState
from app.core.rbac import rbac_manager, Role
from app.core.audit import audit_query_approval
from app.core.sql_validator import validate_sql
from app.core.config import settings
from app.core.structured_logging import get_iso_timestamp
from app.core.langfuse_client import get_langfuse_client, update_trace, trace_span
from app.orchestrator.processor import validate_and_fix_state
from .models import ApprovalRequest, QueryResponse

router = APIRouter()
logger = logging.getLogger(__name__)

# Helper
def validate_query_id(query_id: str):
    import re
    if not re.match(r"^[a-zA-Z0-9_\-]+$", query_id):
        raise HTTPException(
            status_code=400, 
            detail="Invalid query ID format. Only alphanumeric characters, dashes, and underscores allowed."
        )

@router.post("/{query_id}/approve", response_model=QueryResponse)
async def approve_query(
    query_id: str,
    request: ApprovalRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Approve or reject query execution and resume orchestrator workflow.
    """
    from app.core.client_registry import registry
    
    validate_query_id(query_id)
    
    if user["role"] not in [Role.ADMIN, Role.ANALYST]:
        raise HTTPException(
            status_code=403,
            detail="Only admin and analyst roles can approve queries"
        )
    
    state_manager = await get_query_state_manager()
    query_metadata = await state_manager.get_query_metadata(query_id)
    
    if query_metadata:
        query_owner = query_metadata.get("user_id") or query_metadata.get("username")
        if query_owner and query_owner != user.get("username") and user.get("role") != Role.ADMIN:
            logger.warning(f"Approval denied: {user['username']} tried to approve query owned by {query_owner}")
            raise HTTPException(
                status_code=403,
                detail="You can only approve/reject your own queries (unless admin)"
            )
    elif not settings.is_development:
        raise HTTPException(
            status_code=404,
            detail="Query not found or not registered"
        )
    
    from app.services.approval_service import ApprovalService
    
    pending_approval = await ApprovalService.get_pending(query_id)
    original_sql = pending_approval.get("original_sql", "") if pending_approval else ""
    sql_to_check = request.modified_sql if request.modified_sql else original_sql
    
    if await ApprovalService.check_idempotency(query_id, sql_to_check):
        raise HTTPException(status_code=409, detail={"error": "Duplicate approval", "message": "Query already approved/executed", "query_id": query_id})
    
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
        
        risk_reassessment = await ApprovalService.reassess_modified_sql(query_id, request.modified_sql, user["role"].value)
        if risk_reassessment.get("risk_increased"):
            logger.warning(f"Risk increased for modified SQL in query {query_id}")

    orchestrator = registry.get_query_orchestrator()
    if not orchestrator:
        raise HTTPException(status_code=500, detail="Query orchestrator not available")
    
    try:
        config = {"configurable": {"thread_id": query_id}}
        state_snapshot = await orchestrator.aget_state(config)
        
        if not state_snapshot or not state_snapshot.values:
            raise HTTPException(status_code=404, detail=f"Query {query_id} not found or already completed")
        
        current_state = dict(state_snapshot.values)
        
        if not request.approved:
            rejection_updates = {
                "approved": False,
                "needs_approval": False,
                "error": request.rejection_reason or "Query rejected by user",
                "next_action": "rejected",
            }
            await orchestrator.aupdate_state(config, rejection_updates)

            try:
                qs_manager = await get_query_state_manager()
                await qs_manager.update_state(query_id, QueryExecState.REJECTED, {"error": rejection_updates["error"]})
            except Exception as e:
                logger.warning(f"Failed to emit SSE rejection state: {e}")
            
            await audit_query_approval(user=user["username"], user_role=user["role"].value, query_id=query_id, approved=False)
            
            try:
                await asyncio.wait_for(orchestrator.ainvoke(None, config), timeout=30.0)
            except Exception:
                pass
            
            return {
                "query_id": query_id,
                "status": "rejected",
                "message": "Query rejected by user",
                "error": request.rejection_reason,
                "timestamp": get_iso_timestamp(),
            }
        
        approval_updates = {
            "approved": True,
            "needs_approval": False,
            "execution_result": {},
            "result_analysis": {},
        }
        
        if request.modified_sql:
            approval_updates["sql_query"] = request.modified_sql
        
        await orchestrator.aupdate_state(config, approval_updates)
        
        final_sql = request.modified_sql if request.modified_sql else current_state.get("sql_query", "")
        await ApprovalService.mark_approved(query_id, final_sql, user["username"])

        qs_manager = None
        try:
            qs_manager = await get_query_state_manager()
            await qs_manager.update_state(query_id, QueryExecState.APPROVED, {"sql": final_sql})
            await qs_manager.update_state(query_id, QueryExecState.EXECUTING, {"sql": final_sql})
        except Exception as e:
            logger.warning(f"Failed to emit SSE execution state: {e}")

        logger.info(f"Query {query_id} approved by {user['username']}, resuming execution...")
        
        trace_identifier = current_state.get("trace_id") or query_id
        langfuse_client = get_langfuse_client()
        
        try:
            async with trace_span(
                trace_identifier, 
                "orchestrator.resume_execution",
                input_data={"query_id": query_id, "approver": user["username"]},
                metadata={"user_role": user["role"].value}
            ) as resume_span:
                final_state = await asyncio.wait_for(orchestrator.ainvoke(None, config), timeout=600.0)
                resume_span["output"] = {"status": "completed", "query_id": query_id}
        except asyncio.TimeoutError:
            if qs_manager:
                try:
                    await qs_manager.update_state(query_id, QueryExecState.ERROR, {"error": "Execution timeout"})
                except Exception: pass
            if langfuse_client:
                update_trace(trace_identifier, output_data={"status": "timeout"}, metadata={"error": "Timeout"}, tags=["timeout"])
            raise HTTPException(status_code=504, detail="Query execution timeout")

        final_state = validate_and_fix_state(final_state or {})
        execution_result = final_state.get("execution_result")
        execution_result_copy = copy.deepcopy(execution_result) if execution_result else None
        
        error_message = final_state.get("error")
        status_str = "error" if error_message else "success"
        message = error_message or "Query approved and executed successfully"

        normalized_result = None
        row_count = 0
        if execution_result_copy and isinstance(execution_result_copy, dict):
            normalized_result = {
                "columns": execution_result_copy.get("columns", []),
                "rows": execution_result_copy.get("rows", []),
                "row_count": execution_result_copy.get("row_count", len(execution_result_copy.get("rows", []))),
                "execution_time_ms": execution_result_copy.get("execution_time_ms", 0),
                "timestamp": execution_result_copy.get("timestamp", ""),
                "status": execution_result_copy.get("status", "success"),
            }
            row_count = normalized_result["row_count"]

        response_payload = {
            "query_id": query_id,
            "status": status_str,
            "completion_state": "completed" if status_str == "success" else "error",
            "message": message,
            "sql_query": final_state.get("sql_query"),
            "results": normalized_result,
            "row_count": row_count,
            "error": error_message,
            "llm_metadata": final_state.get("llm_metadata", {}),
            "insights": final_state.get("insights"),
            "suggested_queries": final_state.get("suggested_queries"),
            "visualization": final_state.get("visualization_hints"),
            "timestamp": get_iso_timestamp(),
        }

        if qs_manager:
            try:
                if status_str == "success":
                    await qs_manager.update_state(query_id, QueryExecState.FINISHED, {"sql": final_state.get("sql_query"), "result": execution_result_copy})
                else:
                    await qs_manager.update_state(query_id, QueryExecState.ERROR, {"error": message})
            except Exception: pass

        if langfuse_client:
            update_trace(trace_identifier, output_data={"status": status_str}, metadata={"approver": user["username"]}, tags=[status_str])
            langfuse_client.flush()

        await audit_query_approval(user=user["username"], user_role=user["role"].value, query_id=query_id, approved=True)
        
        return response_payload

    except Exception as e:
        logger.error(f"Failed to approve/execute query {query_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve query: {str(e)}")

@router.post("/{query_id}/reject")
async def reject_query(
    query_id: str,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Reject query execution
    """
    validate_query_id(query_id)
    
    if user["role"] not in [Role.ADMIN, Role.ANALYST]:
        raise HTTPException(status_code=403, detail="Only admin and analyst roles can reject queries")
    
    try:
        await audit_query_approval(user=user["username"], user_role=user["role"].value, query_id=query_id, approved=False)
        logger.info(f"Query {query_id} rejected by {user['username']}")
        
        return {
            "query_id": query_id,
            "message": "Query rejected",
            "status": "rejected",
            "timestamp": get_iso_timestamp(),
        }
    except Exception as e:
        logger.error(f"Failed to reject query {query_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reject query: {str(e)}")
