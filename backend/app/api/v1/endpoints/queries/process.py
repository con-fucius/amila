import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.services.query_service import QueryService
from app.services.query_state_manager import get_query_state_manager
from app.core.rbac import rbac_manager, Role
from app.core.rate_limiter import apply_rate_limit, RateLimitTier
from app.core.audit import audit_logger, AuditAction
from app.core.security_middleware import input_sanitizer
from app.core.config import settings
from app.core.exceptions import MCPException
from app.core.structured_logging import get_iso_timestamp
from app.core.session_id_generator import session_id_generator
from .models import (
    OrchestratorQueryRequest,
    OrchestratorQueryResponse,
    ClarificationRequest,
    EnhanceQueryRequest,
    EnhanceQueryResponse,
)
from app.services.query_results_store import build_transport_payload

router = APIRouter()
logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)

async def _get_user_or_default(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> dict:
    """
    Get authenticated user or return restricted default for development only.
    """
    if credentials:
        return await rbac_manager.get_current_user(credentials)
    
    if settings.is_development:
        logger.warning("No auth credentials provided, using default VIEWER user for development")
        return {"username": "dev_user", "role": Role.VIEWER}
    else:
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
    Process natural language query through LangGraph orchestrator
    """
    start_time = time.time()
    timestamp = get_iso_timestamp()
    
    # RBAC verification
    if user["role"] not in [Role.ADMIN, Role.ANALYST]:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Insufficient permissions. Analyst role or higher required to execute queries.",
                "error_type": "permission_denied",
                "user_role": user["role"].value,
            }
        )
    
    # Rate limiting
    await apply_rate_limit(req, user, RateLimitTier(user["role"].value))
    
    # Input validation
    if not request.query or not request.query.strip():
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error="Query cannot be empty",
            timestamp=timestamp,
            llm_metadata={"validation_error": "empty_query"},
        )
    
    if len(request.query.strip()) > 10000:
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error="Query too long (max 10000 characters)",
            timestamp=timestamp,
            llm_metadata={"validation_error": "query_too_long"},
        )
    
    # Sanitize input
    try:
        sanitized_input, warnings = input_sanitizer.sanitize_input(request.query)
        if warnings:
            logger.warning(f"Input sanitization warnings: {warnings}")
        request.query = sanitized_input
    except ValueError as e:
        logger.error(f"Input sanitization failed: {e}")
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error=f"Input validation failed: {str(e)}",
            timestamp=timestamp,
            llm_metadata={"validation_error": str(e), "type": "prompt_injection_detected"},
        )
    
    try:
        logger.info(f"Processing query via orchestrator: {request.query[:100]}...")
        pre_generated_query_id = f"query_{uuid.uuid4().hex[:12]}"
        
        # Generate secure session ID server-side if not provided or if client-generated
        # Addresses Issue 5: session_id generated client-side can be spoofed
        if not request.session_id or session_id_generator.is_client_generated(request.session_id):
            original_session_id = request.session_id
            request.session_id = session_id_generator.generate(
                user_id=user["username"],
                additional_context=req.client.host if req.client else None
            )
            if original_session_id:
                logger.info(
                    f"Replaced client-generated session ID {original_session_id[:20]}... "
                    f"with server-generated {request.session_id[:20]}..."
                )
            else:
                logger.info(f"Generated server-side session ID: {request.session_id[:20]}...")
        
        # Register query with state manager
        try:
            state_manager = await get_query_state_manager()
            await state_manager.register_query(
                query_id=pre_generated_query_id,
                user_id=user["username"],
                username=user["username"],
                session_id=request.session_id,
                database_type=request.database_type or "oracle",
                trace_id=None,
            )
        except Exception as reg_err:
            logger.warning(f"Failed to pre-register query {pre_generated_query_id}: {reg_err}")
        
        # Call service layer
        result = await QueryService.submit_natural_language_query(
            user_query=request.query,
            user_id=user["username"],
            session_id=request.session_id,
            user_role=user["role"].value,
            database_type=request.database_type or "oracle",
            thread_id_override=pre_generated_query_id,
            auto_approve=request.auto_approve,
        )
        
        # Ensure result is a dictionary
        if not isinstance(result, dict):
            logger.error(f"Query service returned non-dict result: {type(result)}")
            result = {
                "status": "error",
                "error": "Internal error: Invalid response format from query service",
                "query_id": pre_generated_query_id,
            }

        # Audit submission
        await audit_logger.log(
            action=AuditAction.QUERY_SUBMIT,
            user=user["username"],
            user_role=user["role"].value,
            success=True,
            resource="nl_query",
            resource_id=pre_generated_query_id,
            details={
                "query": request.query[:1000],
                "database_type": request.database_type,
            },
            ip_address=req.client.host if req.client else None,
            session_id=request.session_id,
        )
        
        # Update trace_id if available
        query_id = result.get("query_id", pre_generated_query_id)
        if result.get("trace_id"):
            try:
                state_manager = await get_query_state_manager()
                metadata = await state_manager.get_query_metadata(query_id)
                if metadata:
                    async with state_manager._metadata_lock:
                        if query_id in state_manager._query_metadata:
                            state_manager._query_metadata[query_id].trace_id = result.get("trace_id")
            except Exception:
                pass
        
        result_status = result.get("status", "error")
        result_error = result.get("error") if result_status == "error" else None
        
        # Calculate execution time
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Extract data freshness metadata
        data_freshness = {
            "query_executed_at": timestamp,
            "data_source": request.database_type or "oracle",
            "cache_status": "fresh",  # Could be enhanced with actual cache detection
        }
        
        # Extract query cost metadata from LLM metadata
        query_cost = {}
        if result.get("llm_metadata"):
            llm_meta = result["llm_metadata"]
            query_cost = {
                "llm_tokens": llm_meta.get("total_tokens", 0),
                "llm_provider": llm_meta.get("provider", "unknown"),
                "db_execution_time_ms": result.get("results", {}).get("execution_time_ms", 0) if result.get("results") else 0,
                "total_time_ms": execution_time_ms,
            }
        
        # Trim large results for transport and include result reference
        transport_result = result.get("results")
        result_ref = None
        results_truncated = None
        if isinstance(transport_result, dict):
            cache_status = transport_result.get("cache_status") or result.get("cache_status")
            trimmed_result, result_ref, results_truncated = build_transport_payload(
                query_id=query_id,
                result=transport_result,
                cache_status=cache_status,
            )
            result["results"] = trimmed_result
            result["result_ref"] = result_ref
            result["results_truncated"] = results_truncated

        return OrchestratorQueryResponse(
            query_id=result.get("query_id", f"query_{uuid.uuid4().hex[:8]}"),
            status=result_status,
            sql_query=result.get("sql_query"),
            validation=result.get("validation"),
            results=result.get("results"),
            result=result.get("result"),
            visualization=result.get("visualization"),
            needs_approval=result.get("needs_approval", False),
            llm_metadata=result.get("llm_metadata"),
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
            message=result.get("message"),
            is_conversational=result.get("is_conversational", False),
            intent=result.get("intent"),
            # New metadata fields
            execution_time_ms=execution_time_ms,
            data_source=request.database_type or "oracle",
            data_freshness=data_freshness,
            query_cost=query_cost if query_cost else None,
            result_ref=result_ref,
            results_truncated=results_truncated,
        )
    
    except MCPException as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"MCP client error after {execution_time}ms: {e.message}", exc_info=True)
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error=f"Database connection error: {e.message}",
            timestamp=timestamp,
            llm_metadata={
                "exception": e.message,
                "error_code": e.error_code,
                "details": e.details,
                "execution_time_ms": execution_time
            },
        )
        
    except Exception as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"Orchestrator failed after {execution_time}ms: {e}", exc_info=True)
        return OrchestratorQueryResponse(
            query_id=f"query_{uuid.uuid4().hex[:8]}",
            status="error",
            error=f"Query orchestration failed: {str(e)}",
            timestamp=timestamp,
            llm_metadata={"exception": str(e), "execution_time_ms": execution_time},
        )

@router.post("/clarify", response_model=OrchestratorQueryResponse)
async def reprocess_with_clarification(
    request: ClarificationRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> OrchestratorQueryResponse:
    """
    Reprocess query with user-provided clarification
    """
    start_time = time.time()
    timestamp = get_iso_timestamp()
    
    # Validation
    if not request.clarification or not request.clarification.strip():
        raise HTTPException(status_code=400, detail="Clarification cannot be empty")
    
    if len(request.clarification.strip()) > 5000:
        raise HTTPException(status_code=400, detail="Clarification too long (max 5000 characters)")
    
    try:
        logger.info(f"Reprocessing query {request.query_id[:8]}... with clarification")
        
        # Try to retrieve checkpoint state for context
        from app.core.client_registry import registry
        orchestrator = registry.get_query_orchestrator()
        checkpoint_state = None
        
        if orchestrator:
            try:
                config = {"configurable": {"thread_id": request.query_id}}
                state_snapshot = await orchestrator.aget_state(config)
                if state_snapshot and state_snapshot.values:
                    checkpoint_state = state_snapshot.values
            except Exception:
                pass
        
        # Combine queries
        combined_query = request.clarification
        if request.original_query:
            combined_query = f"{request.original_query}\n\nClarification: {request.clarification}"
        
        # Service call
        result = await QueryService.submit_natural_language_query(
            user_query=combined_query,
            user_id=user["username"],
            session_id=request.query_id,
            user_role=user["role"].value,
            timeout=600.0,
            thread_id_override=request.query_id,
            database_type=request.database_type or "oracle",
            conversation_history=[
                {"role": "user", "content": request.original_query or ""},
                {"role": "assistant", "content": "Clarification needed"},
                {"role": "user", "content": request.clarification},
            ] if request.original_query else None,
        )
        
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
        )
    except MCPException as e:
        logger.error(f"MCP client error during clarification: {e.message}", exc_info=True)
        raise HTTPException(
            status_code=e.status_code,
            detail={
                "error": e.message,
                "error_code": e.error_code,
                "details": e.details,
                "correlation_id": e.correlation_id
            }
        )
    except Exception as e:
        logger.error(f"Clarification processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enhance", response_model=EnhanceQueryResponse)
async def enhance_query(
    request: EnhanceQueryRequest,
    user: dict = Depends(_get_user_or_default)
) -> EnhanceQueryResponse:
    """
    Enhance a user's typed query before submission.
    """
    if not request.query or not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    if len(request.query.strip()) > 5000:
        raise HTTPException(status_code=400, detail="Query too long (max 5000 characters)")

    try:
        from app.services.conversation_router import ConversationRouter
        from app.services.schema_service import SchemaService

        if request.use_llm is False:
            return EnhanceQueryResponse(
                original_query=request.query,
                enhanced_query=request.query,
                method="disabled",
                context_used=False
            )

        schema_context = None
        try:
            schema_result = await SchemaService.get_database_schema(use_cache=True)
            if schema_result.get("status") == "success":
                schema_context = schema_result.get("schema", {})
        except Exception:
            schema_context = None

        conv_history = request.conversation_history or []
        enhancement = await ConversationRouter.enhance_query_contextually(
            request.query,
            conv_history,
            schema_context
        )

        enhanced = enhancement.get("enhanced_intent") or request.query
        method = enhancement.get("method", "fallback")
        context_used = bool(enhancement.get("context_used")) if "context_used" in enhancement else None

        return EnhanceQueryResponse(
            original_query=request.query,
            enhanced_query=enhanced,
            method=method,
            context_used=context_used
        )
    except Exception as e:
        logger.error(f"Query enhancement failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Query enhancement failed")
