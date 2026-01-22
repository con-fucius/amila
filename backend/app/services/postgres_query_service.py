"""
PostgreSQL Query Service
Secure read-only query execution via direct psycopg3 connection pool
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from app.core.config import settings
from app.core.exceptions import ExternalServiceException
from app.core.audit import log_audit_event
from app.core.postgres_client import postgres_client

logger = logging.getLogger(__name__)


class PostgresQueryService:
    """
    High-level PostgreSQL query service with audit logging and error handling.
    Uses direct psycopg3 connection pool for simplicity and performance.
    """
    
    @classmethod
    async def execute_sql_query(
        cls,
        sql_query: str,
        user_id: str,
        user_role: Optional[str] = None,
        request_id: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query with audit logging.
        
        Args:
            sql_query: SQL query to execute
            user_id: User ID for audit
            user_role: User role for audit
            request_id: Request ID for tracing
            timeout: Query timeout in seconds
            
        Returns:
            Query result
        """
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        start_time = datetime.now(timezone.utc)
        
        try:
            result = await postgres_client.execute_query(
                sql=sql_query,
                user_id=user_id or "unknown",
                request_id=request_id or "unknown",
                timeout=timeout
            )
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            await log_audit_event(
                event_type="postgres_query_success",
                user_id=user_id,
                details={
                    "request_id": request_id,
                    "user_role": user_role,
                    "sql": sql_query[:500],
                    "row_count": result.get("row_count", 0),
                    "execution_time_ms": execution_time,
                    "database": settings.POSTGRES_DATABASE,
                    "read_only": settings.POSTGRES_READ_ONLY
                }
            )
            
            return {
                "status": "success",
                "results": {
                    "columns": result.get("columns", []),
                    "rows": result.get("rows", []),
                    "row_count": result.get("row_count", 0),
                    "execution_time_ms": result.get("execution_time_ms", 0)
                }
            }
            
        except Exception as e:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            await log_audit_event(
                event_type="postgres_query_failed",
                user_id=user_id,
                details={
                    "request_id": request_id,
                    "user_role": user_role,
                    "sql": sql_query[:500],
                    "error": str(e),
                    "execution_time_ms": execution_time,
                    "database": settings.POSTGRES_DATABASE
                }
            )
            
            raise
    
    @classmethod
    async def get_schema_info(cls, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get database schema information"""
        if not settings.POSTGRES_ENABLED:
            raise ExternalServiceException("PostgreSQL integration not enabled", service_name="postgres")
        
        return await postgres_client.get_schema_info(schema_name)
    
    @classmethod
    async def health_check(cls) -> Dict[str, Any]:
        """Check PostgreSQL service health"""
        if not settings.POSTGRES_ENABLED:
            return {
                "status": "disabled",
                "healthy": False
            }
        
        return await postgres_client.health_check()
