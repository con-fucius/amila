"""
Database router for orchestrating queries to Oracle, Doris, or PostgreSQL based on user selection.
"""

import logging
from typing import Dict, Any, Optional

from app.services.schema_service import SchemaService
from app.services.query_service import QueryService
from app.services.doris_schema_service import DorisSchemaService
from app.services.doris_query_service import DorisQueryService
from app.services.postgres_schema_service import PostgresSchemaService
from app.services.postgres_query_service import PostgresQueryService
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
            database_type: "oracle", "doris", or "postgres"
            user_query: User's natural language query
            connection_name: Database connection name (for Oracle)
            
        Returns:
            Schema metadata
        """
        if database_type == "postgres":
            logger.info(f"Routing schema request to PostgreSQL")
            return await PostgresSchemaService.get_dynamic_schema(user_query)
        elif database_type == "doris":
            logger.info(f"Routing schema request to Doris")
            return await DorisSchemaService.get_dynamic_schema(user_query)
        else:
            logger.info(f"Routing schema request to Oracle")
            return await SchemaService.get_dynamic_schema(user_query, connection_name=connection_name)

    @staticmethod
    async def get_database_schema(
        database_type: str,
        connection_name: Optional[str] = None,
        use_cache: bool = True,
        table_names: Optional[list[str]] = None,
    ) -> Dict[str, Any]:
        """
        Route baseline schema request to the appropriate service.
        """
        if database_type == "postgres":
            logger.info(f"Routing database schema request to PostgreSQL")
            if table_names:
                # For specific tables in Postgres, we just return the full schema filtered by table_info
                # or we can just return full and let caller filter.
                # PostgresSchemaService.get_table_info is for single table.
                return await PostgresSchemaService.get_full_schema()
            return await PostgresSchemaService.get_full_schema()
        elif database_type == "doris":
            logger.info(f"Routing database schema request to Doris")
            return await DorisSchemaService.get_database_schema()
        else:
            logger.info(f"Routing database schema request to Oracle")
            return await SchemaService.get_database_schema(
                connection_name=connection_name,
                use_cache=use_cache,
                table_names=table_names
            )
    
    @staticmethod
    async def execute_sql(
        database_type: str,
        sql_query: str,
        connection_name: Optional[str] = None,
        user_id: Optional[str] = None,
        user_role: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Route SQL execution request to the appropriate service.
        
        Args:
            database_type: "oracle", "doris", or "postgres"
            sql_query: SQL query to execute
            connection_name: Database connection name (for Oracle)
            user_id: User identifier
            user_role: User role for audit logging
            request_id: Request identifier
            
        Returns:
            Query execution result
        """
        if database_type == "postgres":
            db_enum = DatabaseType.POSTGRES
        elif database_type == "doris":
            db_enum = DatabaseType.DORIS
        else:
            db_enum = DatabaseType.ORACLE

        async def _execute_operation():
            if database_type == "postgres":
                logger.info(f"Routing SQL execution to PostgreSQL")
                return await PostgresQueryService.execute_sql_query(
                    sql_query=sql_query,
                    user_id=user_id,
                    user_role=user_role,
                    request_id=request_id,
                )
            elif database_type == "doris":
                logger.info(f"Routing SQL execution to Doris")
                return await DorisQueryService.execute_sql_query(
                    sql_query=sql_query,
                    user_id=user_id,
                    user_role=user_role,
                    request_id=request_id,
                )
            else:
                logger.info(f"Routing SQL execution to Oracle")
                return await QueryService.execute_sql_query(
                    sql_query=sql_query,
                    connection_name=connection_name,
                    user_id=user_id,
                    user_role=user_role,
                    request_id=request_id,
                )

        return await RetryableExecutor.execute_with_retry(
            func=_execute_operation,
            database_type=db_enum,
            operation_name=f"sql_execution_{database_type}"
        )
