
import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.security import HTTPBearer

from app.services.query_service import QueryService
from app.services.doris_query_service import DorisQueryService
from app.core.rbac import rbac_manager, require_analyst_role, Role
from app.core.rate_limiter import apply_rate_limit, RateLimitTier
from app.core.audit import audit_query_execution
from app.core.client_registry import registry
from app.core.sql_validator import sql_validator, validate_sql
from app.core.config import settings
from app.core.resilience import CircuitBreakerOpenError
from app.core.exceptions import MCPException
from app.core.structured_logging import get_iso_timestamp
from app.core.langfuse_client import (
    create_trace,
    get_langfuse_client,
    trace_span,
    update_trace,
)
from .models import QueryRequest, QueryResponse

router = APIRouter()
logger = logging.getLogger(__name__)

# Constants
QUERY_TIMEOUT_SECONDS = 600.0

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
    start_time = time.time()
    timestamp = get_iso_timestamp()
    
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
        
        # Execute with timeout enforcement
        query_id = f"direct_sql_{uuid.uuid4().hex[:12]}"
        langfuse_client = get_langfuse_client()
        trace_metadata = {
            "entrypoint": "direct_sql_submit",
            "user": user["username"],
            "connection": request.connection_name or settings.oracle_default_connection,
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
                db_type = (request.database_type or "oracle").lower()
                if db_type == "doris":
                    execution_result = await asyncio.wait_for(
                        DorisQueryService.execute_sql_query(
                            sql_query=request.query.strip(),
                            user_id=user["username"],
                            user_role=user["role"].value,
                            request_id=query_id,
                        ),
                        timeout=QUERY_TIMEOUT_SECONDS,
                    )
                elif db_type == "postgres":
                    from app.core.postgres_client import ValidationException
                    pg_client = registry.get_postgres_client()
                    if not pg_client:
                        raise MCPException("PostgreSQL client not available")
                    
                    try:
                        execution_result = await asyncio.wait_for(
                            pg_client.execute_query(
                                sql=request.query.strip(),
                                request_id=query_id,
                            ),
                            timeout=QUERY_TIMEOUT_SECONDS,
                        )
                    except ValidationException as ve:
                        raise HTTPException(status_code=400, detail=str(ve))
                else:
                    execution_result = await asyncio.wait_for(
                        QueryService.execute_sql_query(
                            sql_query=request.query.strip(),
                            connection_name=request.connection_name,
                            user_id=user["username"],
                            user_role=user["role"].value,
                            request_id=query_id,
                        ),
                        timeout=QUERY_TIMEOUT_SECONDS,
                    )
                submit_span["output"] = {
                    "status": execution_result.get("status"),
                    "row_count": execution_result.get("results", {}).get("row_count") if isinstance(execution_result.get("results"), dict) else None,
                }
        except asyncio.TimeoutError:
            if langfuse_client:
                try:
                    timeout_metadata = {
                        **trace_metadata,
                        "error": "Query execution timeout",
                        "frontend_surface": "query_builder",
                    }
                    update_trace(
                        trace_identifier,
                        output_data={"status": "timeout", "query_id": query_id},
                        metadata=timeout_metadata,
                        tags=["timeout", "error"],
                    )
                    langfuse_client.flush()
                except Exception:
                    pass
            raise HTTPException(
                status_code=504,
                detail="Query execution timeout - Consider optimizing query"
            )
        
        execution_time = int((time.time() - start_time) * 1000)
        
        if execution_result.get("status") == "success":
            row_count = execution_result.get("results", {}).get("row_count", 0)
            logger.info(f"Query executed successfully: {row_count} rows in {execution_time}ms")
            
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
        raise HTTPException(
            status_code=503,
            detail="SQL execution temporarily unavailable due to repeated upstream failures. Please retry later.",
        )
    except MCPException as e:
        logger.error("MCP client error: %s", e.message, exc_info=True)
        raise HTTPException(
            status_code=e.status_code,
            detail={
                "error": e.message,
                "error_code": e.error_code,
                "details": e.details,
                "correlation_id": e.correlation_id
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        execution_time = int((time.time() - start_time) * 1000)
        logger.error(f"Query submission failed after {execution_time}ms: {e}", exc_info=True)
        # Trace exception updates... omitted for brevity if redundant but good to keep
        raise HTTPException(
            status_code=500, 
            detail=f"Query processing failed: {str(e)}"
        )
