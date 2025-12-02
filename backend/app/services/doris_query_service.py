"""
Doris Query Service - Business logic for Doris query processing
"""

import logging
import uuid
from typing import Dict, Any, Optional

from app.core.client_registry import registry
from app.core.config import settings
from app.services.execution_service import ExecutionService

logger = logging.getLogger(__name__)


class DorisQueryService:
    """Service for handling Doris query-related business logic"""
    
    @staticmethod
    async def execute_sql_query(
        sql_query: str,
        timeout: float = 600.0,
        user_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a direct SQL query via Doris MCP client
        
        Args:
            sql_query: SQL query to execute
            timeout: Query timeout in seconds (default: 600.0)
            user_id: Optional user identifier for trace attribution
            request_id: Optional existing request identifier
        
        Returns:
            Query execution result
        """
        logger.info(f"Executing Doris SQL query with {timeout}s timeout: {sql_query[:100]}...")
        
        query_id = request_id or f"doris_sql_{uuid.uuid4().hex[:12]}"
        
        async def doris_execution():
            if not settings.DORIS_MCP_ENABLED:
                raise RuntimeError("Doris integration is disabled")

            doris_client = registry.get_doris_client()
            if not doris_client:
                raise RuntimeError("Doris MCP client not available")

            if hasattr(doris_client, "is_healthy") and not doris_client.is_healthy:
                raise RuntimeError("Doris MCP client is not healthy")
            
            return await doris_client.execute_sql(sql_query)

        return await ExecutionService.execute_with_observability(
            query_id=query_id,
            query_text=sql_query,
            execute_fn=doris_execution,
            trace_metadata={
                "entrypoint": "execute_doris_sql_query",
                "database": "doris",
                "frontend_surface": "chat",
                "client_type": "doris_mcp"
            },
            circuit_breaker_name="doris_sql_execution",
            timeout=timeout,
            user_id=user_id
        )
    
    @staticmethod
    async def list_connections() -> Dict[str, Any]:
        """
        List available Doris database connections
        
        Returns:
            List of available connections (Doris MCP manages one connection internally)
        """
        logger.info(f"Listing Doris connections...")
        
        try:
            if not settings.DORIS_MCP_ENABLED:
                return {
                    "status": "error",
                    "error": "Doris integration is disabled",
                    "connections": []
                }
            
            doris_client = registry.get_doris_client()
            if not doris_client:
                return {
                    "status": "error",
                    "error": "Doris MCP client not available",
                    "connections": []
                }
            
            if hasattr(doris_client, "is_healthy") and not doris_client.is_healthy:
                return {
                    "status": "error",
                    "error": "Doris MCP client is not healthy",
                    "connections": []
                }
            
            # For Doris MCP, there's typically one connection configured
            return {
                "status": "success",
                "connections": [{
                    "name": f"Doris ({settings.DORIS_DB_DATABASE})",
                    "database": settings.DORIS_DB_DATABASE,
                    "host": settings.DORIS_DB_HOST,
                    "port": settings.DORIS_DB_PORT,
                    "type": "doris"
                }],
                "message": "Doris MCP connection available"
            }
        except Exception as e:
            logger.error(f"Failed to list Doris connections: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "connections": []
            }
