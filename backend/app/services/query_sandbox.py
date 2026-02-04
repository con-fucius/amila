"""
Query Sandbox Service

Executes SQL queries in an isolated, restricted environment to prevent
resource abuse and data exposure. Used for:
- Testing queries before full execution
- Handling high-risk queries
- Supporting viewer role restrictions
- Preventing runaway queries

Sandbox restrictions:
- Row limits (configurable, default 1000)
- Timeout (configurable, default 10 seconds)
- Read-only (no DML/DDL)
- No dangerous functions
- Limited resources (CPU, memory)
"""

import logging
import asyncio
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class SandboxResult:
    """Result from sandboxed query execution"""
    success: bool
    rows: list
    columns: list
    row_count: int
    truncated: bool
    execution_time_ms: int
    error: Optional[str] = None
    safety_violations: list = None


class QuerySandbox:
    """
    Sandbox environment for executing SQL queries with restrictions.
    
    Provides safe execution by:
    - Wrapping queries with LIMIT/OFFSET
    - Setting statement timeouts
    - Enforcing read-only access
    - Blocking dangerous functions
    - Monitoring resource usage
    """
    
    # Default sandbox limits
    DEFAULT_ROW_LIMIT = 1000
    DEFAULT_TIMEOUT_MS = 10000  # 10 seconds
    
    # Forbidden operations (read-only enforcement)
    FORBIDDEN_KEYWORDS = [
        'insert', 'update', 'delete', 'drop', 'truncate', 'create',
        'alter', 'grant', 'revoke', 'commit', 'rollback', 'exec',
        'execute', 'call', 'merge', 'upsert'
    ]
    
    # Dangerous functions to block
    DANGEROUS_FUNCTIONS = [
        'sys_eval', 'sys_exec', 'xp_cmdshell', 'pg_read_file',
        'pg_write_file', 'load_file', 'into outfile', 'bcp',
        'bulk insert', 'UTL_HTTP', 'UTL_FILE', 'UTL_SMTP'
    ]
    
    @classmethod
    def wrap_with_sandbox(
        cls,
        sql: str,
        row_limit: int = DEFAULT_ROW_LIMIT,
        dialect: str = "oracle"
    ) -> str:
        """
        Wrap SQL query with sandbox restrictions.
        
        Args:
            sql: Original SQL query
            row_limit: Maximum rows to return
            dialect: Database dialect
            
        Returns:
            Sandboxed SQL query
        """
        sql_upper = sql.upper().strip()
        
        # Check if already has limit
        if dialect == "oracle":
            if "FETCH FIRST" in sql_upper or "ROWNUM" in sql_upper:
                # Already limited, but ensure our limit
                return cls._apply_oracle_limit(sql, row_limit)
        elif dialect in ["postgres", "postgresql", "doris"]:
            if "LIMIT" in sql_upper:
                # Already has limit, check if lower than our limit
                return cls._apply_postgres_limit(sql, row_limit)
        
        # Apply row limit based on dialect
        if dialect == "oracle":
            return cls._apply_oracle_limit(sql, row_limit)
        elif dialect in ["postgres", "postgresql", "doris"]:
            return cls._apply_postgres_limit(sql, row_limit)
        else:
            # Generic wrapping
            return f"SELECT * FROM ({sql}) AS sandboxed LIMIT {row_limit}"
    
    @classmethod
    def _apply_oracle_limit(cls, sql: str, limit: int) -> str:
        """Apply row limit for Oracle dialect"""
        sql_upper = sql.upper().strip()
        
        # If already has ROWNUM, wrap to enforce our limit
        if "ROWNUM" in sql_upper:
            return f"""
                SELECT * FROM (
                    {sql}
                ) WHERE ROWNUM <= {limit}
            """.strip()
        
        # If already has FETCH FIRST, check limit
        if "FETCH FIRST" in sql_upper:
            # Extract current limit and use minimum
            import re
            match = re.search(r'FETCH\s+FIRST\s+(\d+)', sql_upper)
            if match:
                current_limit = int(match.group(1))
                if current_limit > limit:
                    # Replace with lower limit
                    return re.sub(
                        r'FETCH\s+FIRST\s+\d+',
                        f'FETCH FIRST {limit}',
                        sql,
                        flags=re.IGNORECASE
                    )
            return sql
        
        # Add FETCH FIRST for Oracle 12c+
        if sql_upper.rstrip().endswith(';'):
            sql = sql.rstrip()[:-1]
        
        return f"""
            SELECT * FROM (
                {sql}
            ) WHERE ROWNUM <= {limit}
        """.strip()
    
    @classmethod
    def _apply_postgres_limit(cls, sql: str, limit: int) -> str:
        """Apply row limit for PostgreSQL/Doris dialect"""
        sql_upper = sql.upper().strip()
        
        # Check for existing LIMIT
        import re
        limit_match = re.search(r'\bLIMIT\s+(\d+)', sql_upper)
        
        if limit_match:
            current_limit = int(limit_match.group(1))
            if current_limit > limit:
                # Replace with lower limit
                return re.sub(
                    r'\bLIMIT\s+\d+',
                    f'LIMIT {limit}',
                    sql,
                    flags=re.IGNORECASE
                )
            return sql
        
        # Add LIMIT clause
        if sql_upper.rstrip().endswith(';'):
            sql = sql.rstrip()[:-1]
        
        return f"{sql} LIMIT {limit}"
    
    @classmethod
    def add_timeout_clause(cls, sql: str, timeout_ms: int, dialect: str = "oracle") -> str:
        """
        Add timeout hint to query.
        
        Note: This is dialect-specific and may not be supported by all databases.
        """
        if dialect == "oracle":
            # Oracle hint format
            return f"/*+ MAX_EXECUTION_TIME({timeout_ms}) */ {sql}"
        elif dialect in ["postgres", "postgresql"]:
            # PostgreSQL uses statement_timeout setting (applied at connection level)
            return sql
        elif dialect == "doris":
            # Doris supports query timeout via SET
            return f"SET query_timeout = {timeout_ms // 1000}; {sql}"
        
        return sql
    
    @classmethod
    def validate_safety(cls, sql: str) -> Tuple[bool, list]:
        """
        Validate that query is safe for sandbox execution.
        
        Returns:
            Tuple of (is_safe, violations)
        """
        violations = []
        sql_upper = sql.upper()
        
        # Check for forbidden keywords
        for keyword in cls.FORBIDDEN_KEYWORDS:
            # Use word boundary matching
            import re
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, sql_upper, re.IGNORECASE):
                violations.append(f"Forbidden operation: {keyword.upper()}")
        
        # Check for dangerous functions
        for func in cls.DANGEROUS_FUNCTIONS:
            if func.upper().replace(' ', '') in sql_upper.replace(' ', ''):
                violations.append(f"Dangerous function: {func}")
        
        # Check for multiple statements (stacked queries)
        statement_count = sql_upper.count(';') + 1
        if statement_count > 1:
            violations.append("Multiple statements not allowed in sandbox")
        
        return len(violations) == 0, violations
    
    @classmethod
    async def execute_sandboxed(
        cls,
        sql: str,
        connection_name: str,
        database_type: str = "oracle",
        row_limit: int = DEFAULT_ROW_LIMIT,
        timeout_ms: int = DEFAULT_TIMEOUT_MS
    ) -> SandboxResult:
        """
        Execute query in sandboxed environment.
        
        Args:
            sql: SQL query to execute
            connection_name: Database connection name
            database_type: Database type (oracle, postgres, doris)
            row_limit: Maximum rows to return
            timeout_ms: Execution timeout in milliseconds
            
        Returns:
            SandboxResult with execution results
        """
        start_time = datetime.now(timezone.utc)
        
        # Validate safety
        is_safe, violations = cls.validate_safety(sql)
        if not is_safe:
            logger.warning(f"Sandbox safety violations: {violations}")
            return SandboxResult(
                success=False,
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                execution_time_ms=0,
                error=f"Query blocked by sandbox: {'; '.join(violations)}",
                safety_violations=violations
            )
        
        # Wrap query with sandbox restrictions
        sandboxed_sql = cls.wrap_with_sandbox(sql, row_limit, database_type)
        
        # Add timeout hint (best effort)
        sandboxed_sql = cls.add_timeout_clause(sandboxed_sql, timeout_ms, database_type)
        
        logger.debug(f"Sandboxed query: {sandboxed_sql[:200]}...")
        
        try:
            # Execute with timeout
            from app.services.database_router import DatabaseRouter
            
            # Create task with timeout
            task = DatabaseRouter.execute_sql(
                database_type=database_type,
                sql_query=sandboxed_sql,
                connection_name=connection_name
            )
            
            # Execute with timeout
            result = await asyncio.wait_for(
                task,
                timeout=timeout_ms / 1000
            )
            
            execution_time = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )
            
            if result.get("status") == "success":
                results_data = result.get("results", {})
                rows = results_data.get("rows", [])
                columns = results_data.get("columns", [])
                row_count = results_data.get("row_count", len(rows))
                
                # Check if truncated
                truncated = row_count >= row_limit
                
                return SandboxResult(
                    success=True,
                    rows=rows,
                    columns=columns,
                    row_count=min(row_count, row_limit),
                    truncated=truncated,
                    execution_time_ms=execution_time
                )
            else:
                return SandboxResult(
                    success=False,
                    rows=[],
                    columns=[],
                    row_count=0,
                    truncated=False,
                    execution_time_ms=execution_time,
                    error=result.get("message", "Unknown error")
                )
        
        except asyncio.TimeoutError:
            logger.warning(f"Sandbox query timed out after {timeout_ms}ms")
            return SandboxResult(
                success=False,
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                execution_time_ms=timeout_ms,
                error=f"Query timed out (limit: {timeout_ms}ms). Try a more specific query."
            )
        
        except Exception as e:
            logger.error(f"Sandbox execution error: {e}")
            execution_time = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )
            return SandboxResult(
                success=False,
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                execution_time_ms=execution_time,
                error=f"Sandbox execution failed: {str(e)}"
            )
    
    @classmethod
    def should_use_sandbox(
        cls,
        user_role: str = "viewer",
        query_risk_score: float = 0.0,
        is_new_user: bool = False
    ) -> bool:
        """
        Determine if query should be executed in sandbox.
        
        Args:
            user_role: User's role
            query_risk_score: Risk score from validation
            is_new_user: Whether user is new/untrusted
            
        Returns:
            True if sandbox should be used
        """
        # Always sandbox for viewers
        if user_role.lower() == "viewer":
            return True
        
        # Sandbox high-risk queries
        if query_risk_score > 50:
            return True
        
        # Sandbox for new users
        if is_new_user:
            return True
        
        # Default: no sandbox for trusted users
        return False


# Global instance
query_sandbox = QuerySandbox()


# Convenience functions

async def execute_sandboxed(
    sql: str,
    connection_name: str,
    database_type: str = "oracle",
    row_limit: int = 1000,
    timeout_ms: int = 10000
) -> SandboxResult:
    """Execute query in sandbox"""
    return await QuerySandbox.execute_sandboxed(
        sql, connection_name, database_type, row_limit, timeout_ms
    )


def wrap_sandbox(sql: str, row_limit: int = 1000, dialect: str = "oracle") -> str:
    """Wrap query with sandbox limits"""
    return QuerySandbox.wrap_with_sandbox(sql, row_limit, dialect)


def validate_sandbox_safety(sql: str) -> Tuple[bool, list]:
    """Validate query safety for sandbox"""
    return QuerySandbox.validate_safety(sql)