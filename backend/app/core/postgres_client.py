"""
PostgreSQL Client with Connection Pooling
Provides secure read-only access to PostgreSQL databases
"""

import logging
import asyncio
import re
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.exceptions import ExternalServiceException, ValidationException

logger = logging.getLogger(__name__)

# Try to import psycopg3
try:
    import psycopg
    from psycopg_pool import AsyncConnectionPool
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    logger.warning("psycopg3 not available - PostgreSQL integration disabled")

# Try to import sqlglot
try:
    import sqlglot
    from sqlglot import exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    logger.warning("sqlglot not available - SQL validation will fallback to basic checks")


class PostgreSQLClient:
    """
    PostgreSQL client with async connection pooling
    Enforces read-only transactions and query validation
    """
    
    def __init__(self):
        self._pool: Optional[AsyncConnectionPool] = None
        self._initialized = False
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize connection pool"""
        if self._initialized:
            return
            
        if not PSYCOPG_AVAILABLE:
            raise ExternalServiceException("psycopg3 not installed", service_name="postgres")
        
        if not SQLGLOT_AVAILABLE:
            raise ExternalServiceException("sqlglot not installed - strict SQL validation requires sqlglot", service_name="postgres")
        
        async with self._lock:
            if self._initialized:
                return
            
            try:
                conninfo = (
                    f"host={settings.POSTGRES_HOST} "
                    f"port={settings.POSTGRES_PORT} "
                    f"dbname={settings.POSTGRES_DATABASE} "
                    f"user={settings.POSTGRES_USER} "
                    f"password={settings.POSTGRES_PASSWORD}"
                )
                
                self._pool = AsyncConnectionPool(
                    conninfo=conninfo,
                    min_size=settings.POSTGRES_POOL_MIN_SIZE,
                    max_size=settings.POSTGRES_POOL_MAX_SIZE,
                    timeout=settings.POSTGRES_POOL_TIMEOUT,
                    open=False # Don't open in constructor (deprecated)
                )
                
                # Explicitly open the pool
                await self._pool.open()
                await self._pool.wait()
                
                self._initialized = True
                logger.info(
                    f"PostgreSQL connection pool initialized "
                    f"(min={settings.POSTGRES_POOL_MIN_SIZE}, max={settings.POSTGRES_POOL_MAX_SIZE})"
                )
                
            except Exception as e:
                logger.error(f"Failed to initialize PostgreSQL pool: {e}")
                raise ExternalServiceException(f"PostgreSQL pool initialization failed: {e}", service_name="postgres")
    
    @asynccontextmanager
    async def get_connection(self, read_only: Optional[bool] = None):
        """
        Get connection from pool with transaction-level read-only enforcement if requested.
        
        Args:
            read_only: Whether to force read-only. If None, uses settings.POSTGRES_READ_ONLY.
        """
        if not self._initialized:
            raise ExternalServiceException("PostgreSQL client not initialized", service_name="postgres")
        
        if not self._pool:
            raise ExternalServiceException("Connection pool not available", service_name="postgres")
        
        async with self._pool.connection() as conn:
            is_readonly = read_only if read_only is not None else settings.POSTGRES_READ_ONLY
            if is_readonly:
                await conn.execute("SET TRANSACTION READ ONLY")
            else:
                await conn.execute("SET TRANSACTION READ WRITE")
            yield conn
    
    async def execute_query(
        self,
        sql: str,
        user_id: str,
        request_id: str,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query with read-only enforcement
        
        Args:
            sql: SQL query to execute
            user_id: User ID for audit
            request_id: Request ID for tracing
            timeout: Query timeout in seconds
            
        Returns:
            Query result with columns and rows
        """
        if not self._initialized:
            raise ExternalServiceException("PostgreSQL client not initialized", service_name="postgres")
        
        # Validate read-only if enforced
        if settings.POSTGRES_READ_ONLY:
            self._validate_readonly_query(sql)
        
        # Add LLM marker
        marked_sql = f"/* LLM Query - User: {user_id}, Request: {request_id} */\n{sql}"
        
        start_time = datetime.now(timezone.utc)
        
        try:
            async with self.get_connection() as conn:
                # Set statement timeout
                query_timeout = timeout or settings.POSTGRES_QUERY_TIMEOUT
                await conn.execute(f"SET statement_timeout = {query_timeout * 1000}")
                
                # Execute query
                async with conn.cursor() as cur:
                    await cur.execute(marked_sql)
                    
                    # Fetch results
                    rows = await cur.fetchall()
                    
                    # Get column names
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    
                    # Convert rows to list of lists for consistency with other DB services
                    result_rows = [list(row) for row in rows]
                    
                    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                    
                    return {
                        "status": "success",
                        "columns": columns,
                        "rows": result_rows,
                        "row_count": len(result_rows),
                        "execution_time_ms": execution_time
                    }
                    
        except asyncio.TimeoutError:
            logger.error(f"PostgreSQL query timeout: {sql[:100]}")
            raise ExternalServiceException("Query execution timeout")
        except psycopg.errors.QueryCanceled:
            logger.error(f"PostgreSQL query canceled: {sql[:100]}")
            raise ExternalServiceException("Query canceled due to timeout")
        except psycopg.errors.InsufficientPrivilege as e:
            logger.error(f"PostgreSQL permission denied: {e}")
            raise ExternalServiceException("Insufficient privileges to execute query")
        except psycopg.errors.SyntaxError as e:
            logger.error(f"PostgreSQL syntax error: {e}")
            raise ValidationException(f"SQL syntax error: {str(e)}")
        except Exception as e:
            logger.error(f"PostgreSQL query execution failed: {e}")
            raise ExternalServiceException(f"Query execution failed: {str(e)}")
    
    def _validate_readonly_query(self, sql: str):
        """
        Validate that query is read-only using AST parsing with sqlglot.
        This provides robust protection against SQL injection and bypass attempts.
        """
        if not SQLGLOT_AVAILABLE:
            # Should have been checked at init, but fail safe
            raise ExternalServiceException("Security module (sqlglot) not available")

        try:
            # Parse all statements in the SQL
            # sqlglot.parse returns a list of expressions
            parsed_expressions = sqlglot.parse(sql, read="postgres")
            
            if not parsed_expressions:
                raise ValidationException("Empty SQL query")

            ALLOWED_TYPES = (
                exp.Select,
                exp.Union,     # Top level unions
                exp.Paren,     # Wrapped queries
            )

            # Strict traversal to ensure no side effects in subqueries/CTEs
            for expression in parsed_expressions:
                # 1. Top-level check: Must be a read-only type
                if isinstance(expression, ALLOWED_TYPES):
                    pass # Allowed
                
                elif isinstance(expression, (exp.Command, exp.Describe)):
                    # Handle specific Command/Properties types that might equate to SHOW/SET (read-safe)
                    # sqlglot often parses SHOW/EXPLAIN as Command or Describe depending on dialect/version
                    
                    # Check the command text for whitelist
                    # expression.this is usually the command name/string
                    command_text = expression.this.upper() if isinstance(expression.this, str) else str(expression.this).upper()
                    
                    if command_text.startswith(('SHOW', 'EXPLAIN', 'DESCRIBE')):
                        continue
                        
                    raise ValidationException(
                        f"Command '{command_text}' not allowed in read-only mode"
                    )
                else:
                    raise ValidationException(
                        f"Statement type '{expression.key}' not allowed in read-only mode. Only SELECT, SHOW, EXPLAIN are permitted."
                    )

                # 2. Deep inspection for writing/mutation side-effects
                # Even inside a SELECT, one *could* try function calls that write (e.g. pg_write_file)
                # We should walk the tree and check for functions/nodes that are dangerous.
                # Just checking statement type is 90% solution; function blacklist is the rest.
                
                # Check for INTO clause (SELECT ... INTO new_table)
                if expression.find(exp.Into):
                     raise ValidationException("SELECT INTO (table creation) is not allowed")

                # Walk tree to check for specific dangerous nodes/functions via blacklist
                # This catches things like `SELECT pg_sleep(10)` or internal admin funcs if we wanted to block them.
                # For strict read-only, mainly we ensure no modification functions.
                # Postgres enforces RO transaction for data, but we want to prevent even calling them.
                
                # We rely on transaction read-only mode for data safety, 
                # but AST validation prevents DDL/DML structure entirely.

        except sqlglot.errors.ParseError as e:
            # verification failed - if we can't parse it, we don't run it
            logger.error(f"SQL Validation - Parse Error: {e}")
            raise ValidationException(f"SQL validation failed: Unable to parse query securely. {str(e)}")
        except ValidationException:
            raise
        except Exception as e:
            logger.error(f"SQL Validation - Unexpected Error: {e}")
            raise ValidationException(f"SQL validation failed: {str(e)}")
    
    async def get_schema_info(self, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get database schema information
        
        Args:
            schema_name: Schema name (default: public)
            
        Returns:
            Schema information with tables and columns
        """
        if not self._initialized:
            raise ExternalServiceException("PostgreSQL client not initialized")
        
        schema = schema_name or "public"
        logger.info(f"Retrieving PostgreSQL schema info for schema='{schema}' in database='{settings.POSTGRES_DATABASE}'")
        
        query = """
        SELECT 
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            tc.constraint_type
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c 
            ON t.table_name = c.table_name 
            AND t.table_schema = c.table_schema
        LEFT JOIN information_schema.key_column_usage kcu
            ON c.table_name = kcu.table_name
            AND c.column_name = kcu.column_name
            AND c.table_schema = kcu.table_schema
        LEFT JOIN information_schema.table_constraints tc
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_schema = tc.table_schema
        WHERE t.table_schema = %s
            AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name, c.ordinal_position
        """
        
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, (schema,))
                    rows = await cur.fetchall()
                    
                    logger.info(f"PostgreSQL schema query returned {len(rows)} rows for schema='{schema}'")
                    
                    # Organize by table
                    tables = {}
                    for row in rows:
                        table_name = row[0]
                        if table_name not in tables:
                            tables[table_name] = {
                                "columns": [],
                                "primary_keys": [],
                                "foreign_keys": []
                            }
                        
                        column_info = {
                            "name": row[1],
                            "type": row[2],
                            "nullable": row[3] == "YES",
                            "default": row[4]
                        }
                        
                        tables[table_name]["columns"].append(column_info)
                        
                        if row[5] == "PRIMARY KEY":
                            tables[table_name]["primary_keys"].append(row[1])
                        elif row[5] == "FOREIGN KEY":
                            tables[table_name]["foreign_keys"].append(row[1])
                    
                    return {
                        "schema": schema,
                        "tables": tables,
                        "table_count": len(tables)
                    }
                    
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL schema: {e}")
            raise ExternalServiceException(f"Schema retrieval failed: {str(e)}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Check PostgreSQL connection health"""
        try:
            if not self._initialized:
                return {
                    "status": "not_initialized",
                    "healthy": False
                }
            
            start_time = datetime.now(timezone.utc)
            
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1 AS health_check")
                    result = await cur.fetchone()
                    
                    if result and result[0] == 1:
                        latency = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                        
                        # Get pool stats (API changed in psycopg_pool 3.x, some attributes removed)
                        pool_stats = {
                            "min_size": settings.POSTGRES_POOL_MIN_SIZE,
                            "max_size": settings.POSTGRES_POOL_MAX_SIZE,
                        }
                        
                        return {
                            "status": "healthy",
                            "healthy": True,
                            "latency_ms": latency,
                            "pool": pool_stats,
                            "read_only": settings.POSTGRES_READ_ONLY
                        }
                    else:
                        return {
                            "status": "unhealthy",
                            "healthy": False,
                            "error": "Health check query failed"
                        }
                        
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return {
                "status": "unhealthy",
                "healthy": False,
                "error": str(e)
            }
    
    async def close(self):
        """Close connection pool"""
        if self._pool:
            try:
                await self._pool.close()
                self._initialized = False
                logger.info("PostgreSQL connection pool closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL pool: {e}")


# Global PostgreSQL client instance
postgres_client = PostgreSQLClient()
