"""
Database-Specific Error Normalizers
Normalizes error codes, messages, and retry strategies for Oracle and Doris
"""

import logging
import re
from typing import Dict, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorCategory(str, Enum):
    """Categories of database errors for consistent handling"""
    SYNTAX_ERROR = "syntax_error"
    INVALID_IDENTIFIER = "invalid_identifier"
    PERMISSION_DENIED = "permission_denied"
    CONNECTION_ERROR = "connection_error"
    TIMEOUT = "timeout"
    CONSTRAINT_VIOLATION = "constraint_violation"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    NETWORK_ERROR = "network_error"
    PROTOCOL_ERROR = "protocol_error"
    TOOL_CALL_ERROR = "tool_call_error"
    UNKNOWN = "unknown"


class RetryStrategy:
    """Retry strategy for error recovery"""
    def __init__(
        self,
        should_retry: bool = False,
        max_attempts: int = 3,
        backoff_base: float = 2.0,
        backoff_cap: float = 10.0,
        is_transient: bool = False
    ):
        self.should_retry = should_retry
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.is_transient = is_transient


class NormalizedError:
    """Normalized error representation across database types"""
    def __init__(
        self,
        category: ErrorCategory,
        error_code: Optional[str] = None,
        message: str = "",
        original_error: Any = None,
        retry_strategy: Optional[RetryStrategy] = None,
        user_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.category = category
        self.error_code = error_code
        self.message = message
        self.original_error = original_error
        self.retry_strategy = retry_strategy or RetryStrategy()
        self.user_message = user_message or message
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            "status": "error",
            "error_category": self.category.value,
            "error_code": self.error_code,
            "message": self.message,
            "user_message": self.user_message,
            "should_retry": self.retry_strategy.should_retry,
            "is_transient": self.retry_strategy.is_transient,
            "metadata": self.metadata
        }


class OracleErrorNormalizer:
    """
    Normalizes Oracle errors from JSON-RPC/SQLcl MCP responses
    Handles ORA- error codes and Oracle-specific error patterns
    """
    
    # Oracle error code mappings
    ORACLE_ERROR_MAP = {
        # Syntax errors
        "ORA-00900": (ErrorCategory.SYNTAX_ERROR, False, "Invalid SQL statement"),
        "ORA-00902": (ErrorCategory.SYNTAX_ERROR, False, "Invalid datatype"),
        "ORA-00904": (ErrorCategory.INVALID_IDENTIFIER, False, "Invalid identifier"),
        "ORA-00905": (ErrorCategory.SYNTAX_ERROR, False, "Missing keyword"),
        "ORA-00906": (ErrorCategory.SYNTAX_ERROR, False, "Missing left parenthesis"),
        "ORA-00907": (ErrorCategory.SYNTAX_ERROR, False, "Missing right parenthesis"),
        "ORA-00908": (ErrorCategory.SYNTAX_ERROR, False, "Missing column"),
        "ORA-00909": (ErrorCategory.SYNTAX_ERROR, False, "Invalid number of arguments"),
        "ORA-00911": (ErrorCategory.SYNTAX_ERROR, False, "Invalid character"),
        "ORA-00923": (ErrorCategory.SYNTAX_ERROR, False, "FROM keyword not found"),
        "ORA-00933": (ErrorCategory.SYNTAX_ERROR, False, "SQL command not properly ended"),
        "ORA-00936": (ErrorCategory.SYNTAX_ERROR, False, "Missing expression"),
        "ORA-00942": (ErrorCategory.INVALID_IDENTIFIER, False, "Table or view does not exist"),
        "ORA-00979": (ErrorCategory.SYNTAX_ERROR, False, "Not a GROUP BY expression"),
        
        # Permission/access errors
        "ORA-00990": (ErrorCategory.PERMISSION_DENIED, False, "Missing or invalid privilege"),
        "ORA-01031": (ErrorCategory.PERMISSION_DENIED, False, "Insufficient privileges"),
        "ORA-01017": (ErrorCategory.PERMISSION_DENIED, False, "Invalid username/password"),
        
        # Connection errors
        "ORA-01012": (ErrorCategory.CONNECTION_ERROR, True, "Not logged on"),
        "ORA-01033": (ErrorCategory.CONNECTION_ERROR, True, "Oracle initialization or shutdown in progress"),
        "ORA-01034": (ErrorCategory.CONNECTION_ERROR, True, "Oracle not available"),
        "ORA-01089": (ErrorCategory.CONNECTION_ERROR, True, "Immediate shutdown in progress"),
        "ORA-03113": (ErrorCategory.CONNECTION_ERROR, True, "End-of-file on communication channel"),
        "ORA-03114": (ErrorCategory.CONNECTION_ERROR, True, "Not connected to Oracle"),
        "ORA-03135": (ErrorCategory.CONNECTION_ERROR, True, "Connection lost contact"),
        "ORA-12154": (ErrorCategory.CONNECTION_ERROR, True, "TNS: could not resolve the connect identifier"),
        "ORA-12541": (ErrorCategory.CONNECTION_ERROR, True, "TNS: no listener"),
        "ORA-12545": (ErrorCategory.CONNECTION_ERROR, True, "Connect failed because target host or object does not exist"),
        
        # Timeout errors
        "ORA-01013": (ErrorCategory.TIMEOUT, True, "User requested cancel of current operation"),
        "ORA-00054": (ErrorCategory.TIMEOUT, True, "Resource busy and acquire with NOWAIT specified"),
        "ORA-30006": (ErrorCategory.TIMEOUT, True, "Resource busy; acquire with WAIT timeout expired"),
        
        # Resource errors
        "ORA-00018": (ErrorCategory.RESOURCE_EXHAUSTED, True, "Maximum number of sessions exceeded"),
        "ORA-00020": (ErrorCategory.RESOURCE_EXHAUSTED, True, "Maximum number of processes exceeded"),
        "ORA-01555": (ErrorCategory.RESOURCE_EXHAUSTED, False, "Snapshot too old"),
        
        # Constraint violations
        "ORA-00001": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Unique constraint violated"),
        "ORA-02290": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Check constraint violated"),
        "ORA-02291": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Integrity constraint violated - parent key not found"),
        "ORA-02292": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Integrity constraint violated - child record found"),
    }
    
    @staticmethod
    def normalize(error_response: Dict[str, Any]) -> NormalizedError:
        """
        Normalize Oracle error from JSON-RPC/MCP response
        
        Args:
            error_response: Error response from SQLcl MCP client
            
        Returns:
            NormalizedError instance
        """
        # Extract error message from various possible locations
        error_msg = OracleErrorNormalizer._extract_error_message(error_response)
        
        # Extract ORA- error codes
        ora_code = OracleErrorNormalizer._extract_ora_code(error_msg)
        
        # Map to error category and retry strategy
        if ora_code and ora_code in OracleErrorNormalizer.ORACLE_ERROR_MAP:
            category, should_retry, description = OracleErrorNormalizer.ORACLE_ERROR_MAP[ora_code]
            user_message = f"{ora_code}: {description}"
        else:
            # Default classification based on message content
            category, should_retry = OracleErrorNormalizer._classify_unknown_error(error_msg)
            user_message = error_msg
        
        # Build retry strategy
        retry_strategy = RetryStrategy(
            should_retry=should_retry,
            max_attempts=3 if should_retry else 1,
            backoff_base=2.0,
            backoff_cap=10.0,
            is_transient=should_retry
        )
        
        # Extract JSON-RPC error code if present
        json_rpc_code = error_response.get("error", {}).get("code") if isinstance(error_response.get("error"), dict) else None
        
        return NormalizedError(
            category=category,
            error_code=ora_code,
            message=error_msg,
            original_error=error_response,
            retry_strategy=retry_strategy,
            user_message=user_message,
            metadata={
                "database_type": "oracle",
                "json_rpc_code": json_rpc_code,
                "ora_code": ora_code
            }
        )
    
    @staticmethod
    def _extract_error_message(error_response: Dict[str, Any]) -> str:
        """Extract error message from Oracle/SQLcl MCP response"""
        # Try multiple extraction paths
        error_msg = ""
        
        # JSON-RPC error format
        if "error" in error_response and isinstance(error_response["error"], dict):
            error_msg = error_response["error"].get("message", "")
        
        # Direct message field
        if not error_msg:
            error_msg = error_response.get("message", "")
        
        # Content field (from tool responses)
        if not error_msg and "content" in error_response:
            content = error_response["content"]
            if isinstance(content, list) and len(content) > 0:
                error_msg = content[0].get("text", "")
        
        # Result field
        if not error_msg and "result" in error_response:
            result = error_response["result"]
            if isinstance(result, dict):
                error_msg = result.get("text", "") or result.get("message", "")
            elif isinstance(result, str):
                error_msg = result
        
        return error_msg.strip() if error_msg else f"Unknown Oracle error (Raw: {str(error_response)[:200]})"
    
    @staticmethod
    def _extract_ora_code(error_msg: str) -> Optional[str]:
        """Extract ORA-XXXXX code from error message"""
        match = re.search(r'ORA-\d{5}', error_msg, re.IGNORECASE)
        return match.group(0).upper() if match else None
    
    @staticmethod
    def _classify_unknown_error(error_msg: str) -> Tuple[ErrorCategory, bool]:
        """Classify unknown errors based on message content"""
        error_lower = error_msg.lower()
        
        # Connection patterns
        if any(p in error_lower for p in ["not connected", "connection", "tns:", "network"]):
            return ErrorCategory.CONNECTION_ERROR, True
        
        # Timeout patterns
        if any(p in error_lower for p in ["timeout", "timed out", "resource busy"]):
            return ErrorCategory.TIMEOUT, True
        
        # Syntax patterns
        if any(p in error_lower for p in ["syntax", "invalid sql", "parse", "unexpected"]):
            return ErrorCategory.SYNTAX_ERROR, False
        
        # Permission patterns
        if any(p in error_lower for p in ["privilege", "permission", "access denied"]):
            return ErrorCategory.PERMISSION_DENIED, False
        
        return ErrorCategory.UNKNOWN, False


class DorisErrorNormalizer:
    """
    Normalizes Doris errors from HTTP MCP/tool call responses
    Handles Doris-specific error patterns and MCP tool errors
    """
    
    @staticmethod
    def normalize(error_response: Dict[str, Any]) -> NormalizedError:
        """
        Normalize Doris error from MCP tool call response
        
        Args:
            error_response: Error response from Doris MCP client
            
        Returns:
            NormalizedError instance
        """
        # Extract error type (from DorisMCPClient error handling)
        error_type = error_response.get("error_type", "unknown")
        error_msg = error_response.get("error", "") or error_response.get("message", "")
        
        # Map error_type to category and retry strategy
        category, should_retry = DorisErrorNormalizer._map_error_type(error_type, error_msg)
        
        # Build retry strategy
        # Doris typically uses HTTP protocol, so network errors are more common
        retry_strategy = RetryStrategy(
            should_retry=should_retry,
            max_attempts=3 if should_retry else 1,
            backoff_base=1.5,  # Faster backoff for HTTP
            backoff_cap=5.0,   # Shorter cap for OLAP queries
            is_transient=should_retry
        )
        
        # Generate user-friendly message
        user_message = DorisErrorNormalizer._generate_user_message(category, error_msg, error_type)
        
        return NormalizedError(
            category=category,
            error_code=error_type,
            message=error_msg,
            original_error=error_response,
            retry_strategy=retry_strategy,
            user_message=user_message,
            metadata={
                "database_type": "doris",
                "error_type": error_type
            }
        )
    
    @staticmethod
    def _map_error_type(error_type: str, error_msg: str) -> Tuple[ErrorCategory, bool]:
        """Map Doris error type to category and retry decision"""
        error_type_lower = error_type.lower()
        error_msg_lower = error_msg.lower()
        
        # Network errors (from DorisMCPClient)
        if error_type in {"network", "unavailable"}:
            return ErrorCategory.NETWORK_ERROR, True
        
        # Protocol errors (MCP protocol issues)
        if error_type == "protocol":
            return ErrorCategory.PROTOCOL_ERROR, False
        
        # Tool call errors (MCP tool invocation errors)
        if error_type == "tool_call":
            # Check if it's a transient error
            if any(p in error_msg_lower for p in ["timeout", "connection", "network"]):
                return ErrorCategory.NETWORK_ERROR, True
            return ErrorCategory.TOOL_CALL_ERROR, False
        
        # No tools available
        if error_type == "no_tools":
            return ErrorCategory.CONNECTION_ERROR, False
        
        # SQL-level errors (parse message for clues)
        if any(p in error_msg_lower for p in ["syntax", "parse", "invalid sql"]):
            return ErrorCategory.SYNTAX_ERROR, False
        
        if any(p in error_msg_lower for p in ["table", "column", "not found", "does not exist", "unknown table"]):
            return ErrorCategory.INVALID_IDENTIFIER, False
        
        if any(p in error_msg_lower for p in ["permission", "access denied", "privilege"]):
            return ErrorCategory.PERMISSION_DENIED, False
        
        if any(p in error_msg_lower for p in ["timeout", "timed out"]):
            return ErrorCategory.TIMEOUT, True
        
        if any(p in error_msg_lower for p in ["connection", "connect failed", "refused"]):
            return ErrorCategory.CONNECTION_ERROR, True
        
        return ErrorCategory.UNKNOWN, False
    
    @staticmethod
    def _generate_user_message(category: ErrorCategory, error_msg: str, error_type: str) -> str:
        """Generate user-friendly error message"""
        if category == ErrorCategory.NETWORK_ERROR:
            return f"Doris database is currently unavailable. Please try again later. ({error_type})"
        elif category == ErrorCategory.PROTOCOL_ERROR:
            return "Communication error with Doris database. Please contact administrator."
        elif category == ErrorCategory.TOOL_CALL_ERROR:
            return f"Doris query execution failed: {error_msg}"
        elif category == ErrorCategory.SYNTAX_ERROR:
            return f"SQL syntax error: {error_msg}"
        elif category == ErrorCategory.INVALID_IDENTIFIER:
            return f"Table or column not found: {error_msg}"
        elif category == ErrorCategory.PERMISSION_DENIED:
            return f"Access denied: {error_msg}"
        else:
            return error_msg or "An error occurred while querying Doris database"


class PostgreSQLErrorNormalizer:
    """
    Normalizes PostgreSQL errors from psycopg3 exceptions
    Handles PostgreSQL-specific error codes and patterns
    """
    
    POSTGRES_ERROR_MAP = {
        "42P01": (ErrorCategory.INVALID_IDENTIFIER, False, "Table does not exist"),
        "42703": (ErrorCategory.INVALID_IDENTIFIER, False, "Column does not exist"),
        "42883": (ErrorCategory.INVALID_IDENTIFIER, False, "Function does not exist"),
        "42P02": (ErrorCategory.INVALID_IDENTIFIER, False, "Parameter does not exist"),
        
        "42601": (ErrorCategory.SYNTAX_ERROR, False, "Syntax error"),
        "42804": (ErrorCategory.SYNTAX_ERROR, False, "Datatype mismatch"),
        "42P18": (ErrorCategory.SYNTAX_ERROR, False, "Indeterminate datatype"),
        
        "42501": (ErrorCategory.PERMISSION_DENIED, False, "Insufficient privilege"),
        "28P01": (ErrorCategory.PERMISSION_DENIED, False, "Invalid password"),
        "28000": (ErrorCategory.PERMISSION_DENIED, False, "Invalid authorization specification"),
        
        "08000": (ErrorCategory.CONNECTION_ERROR, True, "Connection exception"),
        "08003": (ErrorCategory.CONNECTION_ERROR, True, "Connection does not exist"),
        "08006": (ErrorCategory.CONNECTION_ERROR, True, "Connection failure"),
        "08001": (ErrorCategory.CONNECTION_ERROR, True, "Unable to establish connection"),
        "08004": (ErrorCategory.CONNECTION_ERROR, True, "Server rejected connection"),
        "08007": (ErrorCategory.CONNECTION_ERROR, True, "Transaction resolution unknown"),
        
        "57014": (ErrorCategory.TIMEOUT, True, "Query canceled"),
        "57P01": (ErrorCategory.TIMEOUT, True, "Admin shutdown"),
        "57P02": (ErrorCategory.TIMEOUT, True, "Crash shutdown"),
        "57P03": (ErrorCategory.TIMEOUT, True, "Cannot connect now"),
        
        "53000": (ErrorCategory.RESOURCE_EXHAUSTED, True, "Insufficient resources"),
        "53100": (ErrorCategory.RESOURCE_EXHAUSTED, True, "Disk full"),
        "53200": (ErrorCategory.RESOURCE_EXHAUSTED, True, "Out of memory"),
        "53300": (ErrorCategory.RESOURCE_EXHAUSTED, True, "Too many connections"),
        
        "23000": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Integrity constraint violation"),
        "23001": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Restrict violation"),
        "23502": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Not null violation"),
        "23503": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Foreign key violation"),
        "23505": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Unique violation"),
        "23514": (ErrorCategory.CONSTRAINT_VIOLATION, False, "Check violation"),
    }
    
    @staticmethod
    def normalize(error: Exception) -> NormalizedError:
        """
        Normalize PostgreSQL error from psycopg3 exception
        
        Args:
            error: Exception from psycopg3
            
        Returns:
            NormalizedError instance
        """
        error_msg = str(error)
        error_code = None
        
        try:
            import psycopg.errors
            
            if hasattr(error, 'sqlstate'):
                error_code = error.sqlstate
            elif hasattr(error, 'pgcode'):
                error_code = error.pgcode
            
            if error_code and error_code in PostgreSQLErrorNormalizer.POSTGRES_ERROR_MAP:
                category, should_retry, description = PostgreSQLErrorNormalizer.POSTGRES_ERROR_MAP[error_code]
                user_message = f"PostgreSQL Error {error_code}: {description}"
            else:
                category, should_retry = PostgreSQLErrorNormalizer._classify_unknown_error(error, error_msg)
                user_message = error_msg
                
        except ImportError:
            category, should_retry = PostgreSQLErrorNormalizer._classify_unknown_error(error, error_msg)
            user_message = error_msg
        
        retry_strategy = RetryStrategy(
            should_retry=should_retry,
            max_attempts=3 if should_retry else 1,
            backoff_base=2.0,
            backoff_cap=10.0,
            is_transient=should_retry
        )
        
        return NormalizedError(
            category=category,
            error_code=error_code,
            message=error_msg,
            original_error=error,
            retry_strategy=retry_strategy,
            user_message=user_message,
            metadata={
                "database_type": "postgres",
                "error_code": error_code,
                "error_type": type(error).__name__
            }
        )
    
    @staticmethod
    def _classify_unknown_error(error: Exception, error_msg: str) -> Tuple[ErrorCategory, bool]:
        """Classify unknown PostgreSQL errors based on exception type and message"""
        error_lower = error_msg.lower()
        error_type = type(error).__name__
        
        try:
            import psycopg.errors
            
            if isinstance(error, psycopg.errors.OperationalError):
                if any(p in error_lower for p in ["connection", "connect", "server"]):
                    return ErrorCategory.CONNECTION_ERROR, True
                if any(p in error_lower for p in ["timeout", "canceled"]):
                    return ErrorCategory.TIMEOUT, True
                return ErrorCategory.CONNECTION_ERROR, True
            
            if isinstance(error, psycopg.errors.ProgrammingError):
                return ErrorCategory.SYNTAX_ERROR, False
            
            if isinstance(error, psycopg.errors.IntegrityError):
                return ErrorCategory.CONSTRAINT_VIOLATION, False
            
            if isinstance(error, psycopg.errors.InsufficientPrivilege):
                return ErrorCategory.PERMISSION_DENIED, False
            
            if isinstance(error, psycopg.errors.QueryCanceled):
                return ErrorCategory.TIMEOUT, True
                
        except ImportError:
            pass
        
        if any(p in error_lower for p in ["connection", "connect failed", "refused"]):
            return ErrorCategory.CONNECTION_ERROR, True
        
        if any(p in error_lower for p in ["timeout", "canceled", "timed out"]):
            return ErrorCategory.TIMEOUT, True
        
        if any(p in error_lower for p in ["syntax", "parse", "invalid sql"]):
            return ErrorCategory.SYNTAX_ERROR, False
        
        if any(p in error_lower for p in ["permission", "privilege", "access denied"]):
            return ErrorCategory.PERMISSION_DENIED, False
        
        if any(p in error_lower for p in ["table", "column", "does not exist", "not found"]):
            return ErrorCategory.INVALID_IDENTIFIER, False
        
        return ErrorCategory.UNKNOWN, False


def normalize_database_error(database_type: str, error_response: Any) -> NormalizedError:
    """
    Unified error normalization entry point
    
    Args:
        database_type: "oracle", "doris", or "postgres"/"postgresql"
        error_response: Raw error response from database client (dict or Exception)
        
    Returns:
        NormalizedError instance
    """
    db_type_lower = database_type.lower()
    
    if db_type_lower == "oracle":
        if not isinstance(error_response, dict):
            error_response = {"message": str(error_response)}
        return OracleErrorNormalizer.normalize(error_response)
    elif db_type_lower == "doris":
        if not isinstance(error_response, dict):
            error_response = {"message": str(error_response)}
        return DorisErrorNormalizer.normalize(error_response)
    elif db_type_lower in ["postgres", "postgresql"]:
        if isinstance(error_response, dict):
            error_response = Exception(error_response.get("message", str(error_response)))
        return PostgreSQLErrorNormalizer.normalize(error_response)
    else:
        logger.warning(f"Unknown database type for error normalization: {database_type}")
        return NormalizedError(
            category=ErrorCategory.UNKNOWN,
            message=str(error_response),
            original_error=error_response,
            metadata={"database_type": database_type}
        )
