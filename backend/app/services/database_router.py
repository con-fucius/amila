"""
Database router for orchestrating queries to Oracle or Doris based on user selection.
"""

import logging
from typing import Dict, Any, Optional

from app.services.schema_service import SchemaService
from app.services.query_service import QueryService
from app.services.doris_schema_service import DorisSchemaService
from app.services.doris_query_service import DorisQueryService
from app.core.db_timeout_retry import DatabaseType, get_database_policies, RetryableExecutor
from app.core.error_normalizer import normalize_database_error

logger = logging.getLogger(__name__)


class DatabaseRouter:
    """Routes queries to the appropriate database service based on database_type."""
    
    @staticmethod
    async def get_schema(
        database_type: str,
        user_query: str,
        connection_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Route schema request to the appropriate service.
        
        Args:
            database_type: "oracle" or "doris"
            user_query: User's natural language query
            connection_name: Database connection name (for Oracle)
            
        Returns:
            Schema metadata
        """
        if database_type == "doris":
            logger.info(f"Routing schema request to Doris")
            return await DorisSchemaService.get_dynamic_schema(user_query)
        else:
            logger.info(f"Routing schema request to Oracle")
            return await SchemaService.get_dynamic_schema(user_query, connection_name=connection_name)
    
    @staticmethod
    async def execute_sql(
        database_type: str,
        sql_query: str,
        connection_name: Optional[str] = None,
        user_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Route SQL execution request to the appropriate service.
        
        Args:
            database_type: "oracle" or "doris"
            sql_query: SQL query to execute
            connection_name: Database connection name (for Oracle)
            user_id: User identifier
            request_id: Request identifier
            
        Returns:
            Query execution result
        """
        db_enum = DatabaseType.DORIS if database_type == "doris" else DatabaseType.ORACLE
        executor = RetryableExecutor(db_enum)

        async def _execute_operation():
            if database_type == "doris":
                logger.info(f"Routing SQL execution to Doris")
                return await DorisQueryService.execute_sql_query(
                    sql_query=sql_query,
                    user_id=user_id,
                    request_id=request_id,
                )
            else:
                logger.info(f"Routing SQL execution to Oracle")
                return await QueryService.execute_sql_query(
                    sql_query=sql_query,
                    connection_name=connection_name,
                    user_id=user_id,
                    request_id=request_id,
                )

        return await executor.execute_with_retry(_execute_operation)
