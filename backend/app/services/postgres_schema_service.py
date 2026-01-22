"""
PostgreSQL Schema Service
Provides schema introspection and metadata for PostgreSQL databases
"""

import logging
from typing import Dict, Any, Optional, List

from app.core.postgres_client import postgres_client
from app.core.exceptions import ExternalServiceException
from app.core.config import settings

logger = logging.getLogger(__name__)


class PostgresSchemaService:
    """Service for PostgreSQL schema operations"""
    
    @staticmethod
    async def get_dynamic_schema(
        user_query: str,
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get schema information relevant to user query
        
        Args:
            user_query: User's natural language query
            schema_name: PostgreSQL schema name (default: public)
            
        Returns:
            Schema metadata
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        try:
            schema_info = await postgres_client.get_schema_info(schema_name)
            
            return {
                "status": "success",
                "source": "postgres",
                "schema_data": {
                    "schema": schema_info["schema"],
                    "tables": schema_info["tables"],
                    "table_count": schema_info["table_count"]
                },
                "metadata": {
                    "database": settings.POSTGRES_DATABASE,
                    "host": settings.POSTGRES_HOST,
                    "read_only": settings.POSTGRES_READ_ONLY
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL schema: {e}")
            raise ExternalServiceException(f"Schema retrieval failed: {str(e)}", service_name="postgres")
    
    @staticmethod
    async def get_full_schema(schema_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get complete schema information
        
        Args:
            schema_name: PostgreSQL schema name (default: public)
            
        Returns:
            Complete schema metadata
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        try:
            schema_info = await postgres_client.get_schema_info(schema_name)
            
            return {
                "status": "success",
                "source": "postgres",
                "schema_data": {
                    "schema": schema_info["schema"],
                    "tables": schema_info["tables"],
                    "table_count": schema_info["table_count"]
                },
                "metadata": {
                    "database": settings.POSTGRES_DATABASE,
                    "host": settings.POSTGRES_HOST,
                    "read_only": settings.POSTGRES_READ_ONLY
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get full PostgreSQL schema: {e}")
            raise ExternalServiceException(f"Schema retrieval failed: {str(e)}", service_name="postgres")
    
    @staticmethod
    async def get_table_info(
        table_name: str,
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed information about a specific table
        
        Args:
            table_name: Table name
            schema_name: Schema name (default: public)
            
        Returns:
            Table metadata
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        try:
            schema_info = await postgres_client.get_schema_info(schema_name)
            
            if table_name not in schema_info["tables"]:
                raise ExternalServiceException(f"Table '{table_name}' not found", service_name="postgres")
            
            table_info = schema_info["tables"][table_name]
            
            return {
                "status": "success",
                "table_name": table_name,
                "schema": schema_info["schema"],
                "columns": table_info["columns"],
                "primary_keys": table_info["primary_keys"],
                "foreign_keys": table_info["foreign_keys"],
                "column_count": len(table_info["columns"])
            }
            
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL table info: {e}")
            raise ExternalServiceException(f"Table info retrieval failed: {str(e)}", service_name="postgres")
