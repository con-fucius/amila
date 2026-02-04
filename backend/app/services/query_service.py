"""
Query Service - Business logic for query processing
"""

import asyncio
import logging
import time
import uuid
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from app.orchestrator import process_query
from app.core.client_registry import registry
from app.core.config import settings
from app.core.exceptions import MCPException
from app.core.structured_logging import get_iso_timestamp
from app.core.langfuse_client import (
    create_trace,
    get_langfuse_client,
    trace_span,
    update_trace,
)
from app.core.resilience import circuit_breaker_context, CircuitBreakerConfig, CircuitBreakerOpenError
from app.core.mcp_resilient import safe_mcp_call, validate_mcp_response, MCPTimeoutError
from app.services.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class QueryService:
    """Service for handling query-related business logic"""
    
    @staticmethod
    async def submit_natural_language_query(
        user_query: str,
        user_id: str = "default_user",
        session_id: Optional[str] = None,
        user_role: str = "analyst",
        timeout: float = 600.0,
        thread_id_override: Optional[str] = None,
        database_type: str = "oracle",
        auto_approve: bool = False,
    ) -> Dict[str, Any]:
        """
        Process a natural language query through the orchestrator
        
        Args:
            user_query: Natural language query
            user_id: User identifier
            session_id: Session identifier (generated if not provided)
            user_role: User role for RBAC (admin, analyst, viewer)
            timeout: Query timeout in seconds (default: 600)
            thread_id_override: Optional thread ID override
            
        Returns:
            Query processing result
        """
        if not session_id:
            session_id = str(uuid.uuid4())
        
        logger.info(f"Processing NL query with {timeout}s timeout: {user_query[:100]}...")
        
        try:
            # Wrap orchestrator call with timeout
            result = await asyncio.wait_for(
                process_query(
                    user_query=user_query,
                    user_id=user_id,
                    session_id=session_id,
                    user_role=user_role,
                    thread_id_override=thread_id_override,
                    database_type=database_type,
                    auto_approve=auto_approve,
                ),
                timeout=timeout
            )
            
            logger.info(f"Query processed successfully")
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Query processing timed out after {timeout}s")
            error_msg = f"Query processing timed out after {timeout} seconds"
            return {
                "status": "error",
                "message": error_msg,
                "error": error_msg,  # CRITICAL: Include error field for frontend
                "query_id": str(uuid.uuid4()),
                "timestamp": get_iso_timestamp(),
            }
        except Exception as e:
            logger.error(f"Query processing failed: %s", e, exc_info=True)
            error_msg = str(e)
            return {
                "status": "error",
                "message": error_msg,
                "error": error_msg,  # CRITICAL: Include error field for frontend
                "query_id": str(uuid.uuid4()),
                "timestamp": get_iso_timestamp(),
            }
    
    @staticmethod
    async def execute_sql_query(
        sql_query: str,
        connection_name: Optional[str] = None,
        timeout: float = 600.0,
        user_id: Optional[str] = None,
        user_role: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a direct SQL query via SQL*Plus (SQLcl) pool or MCP client
        
        Args:
            sql_query: SQL query to execute
            connection_name: Database connection name
            timeout: Query timeout in seconds (default: 600.0)
            user_id: Optional user identifier for trace attribution
            user_role: Optional user role for audit logging
            request_id: Optional existing request identifier to reuse for tracing
        
        Returns:
            Query execution result
        """
        logger.info(f"Executing SQL query with {timeout}s timeout: {sql_query[:100]}...")
        
        # Resolve connection name
        conn = connection_name or settings.oracle_default_connection
        query_id = request_id or f"sql_{uuid.uuid4().hex[:12]}"

        async def oracle_execution():
            # Try to use pool first
            sqlcl_pool = registry.get_sqlcl_pool()
            
            if sqlcl_pool:
                try:
                    # Use shorter timeout for acquisition to allow fallback
                    async with sqlcl_pool.acquire(timeout=30) as client:
                        logger.info(f"Executing via SQLcl pool")
                        return await client.execute_sql(sql_query, conn, query_id=query_id)
                except asyncio.TimeoutError:
                    logger.warning(f"Pool acquisition timed out, falling back to MCP client")
                except MCPException as e:
                    logger.warning(f"Pool execution failed ({e.message}), falling back to MCP client")
                except Exception as e:
                    logger.warning(f"Pool execution failed ({e}), falling back to MCP client")

            # Fallback to single MCP client
            mcp_client = registry.get_mcp_client()
            if not mcp_client:
                raise MCPException(
                    "MCP client and pool not available",
                    details={
                        "pool_available": sqlcl_pool is not None,
                        "client_available": False
                    }
                )

            logger.info("Fetching available connections via fallback client...")
            
            # Ensure connection
            # Note: connect_database might internally check cache or status
            connection_result = await mcp_client.connect_database(conn)
            if connection_result.get("status") != "connected":
                raise MCPException(
                    f"Database connection failed: {connection_result.get('message')}",
                    details={
                        "connection_name": conn,
                        "connection_result": connection_result
                    }
                )

            # Execute query
            return await mcp_client.execute_sql(sql_query, conn, query_id=query_id)

        # Use shared execution service
        from app.services.execution_service import ExecutionService
        
        return await ExecutionService.execute_with_observability(
            query_id=query_id,
            query_text=sql_query,
            execute_fn=oracle_execution,
            trace_metadata={
                "entrypoint": "execute_sql_query",
                "connection": conn,
                "frontend_surface": "query_builder",
                "database": "oracle",
                "user_role": user_role or "unknown"
            },
            circuit_breaker_name="sql_execution",
            timeout=timeout,
            user_id=user_id
        )
    
    @staticmethod
    async def list_connections() -> Dict[str, Any]:
        """
        List available database connections
        
        Returns:
            List of available connections
        """
        from app.services.connection_manager import ConnectionManager
        return await ConnectionManager.list_all_connections()