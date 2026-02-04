"""
SQL Injection Guard and Query Validation
Comprehensive SQL query validation and sanitization
"""

import concurrent.futures
import logging
import re
import signal
import sys
from typing import List, Dict, Any, Optional, Tuple, Callable, TypeVar
from enum import Enum
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Constants
REGEX_TIMEOUT_SECONDS = 1
MAX_QUERY_LENGTH = 50000

T = TypeVar('T')


def run_with_timeout(func: Callable[[], T], timeout_seconds: int) -> T:
    """
    Run a function with a timeout that works on both Windows and Unix.
    Uses concurrent.futures.ThreadPoolExecutor for cross-platform support.
    
    Args:
        func: Zero-argument callable to execute
        timeout_seconds: Maximum execution time in seconds
        
    Returns:
        The result of func()
        
    Raises:
        TimeoutError: If execution exceeds timeout_seconds
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            raise TimeoutError("Operation timed out")


@contextmanager
def timeout(seconds: int):
    """
    Context manager for operation timeout.
    Uses signal.alarm on Unix (more reliable) and falls back to no-op on Windows
    since regex operations are typically fast enough not to need interruption.
    For longer operations, use run_with_timeout() instead.
    """
    if sys.platform == "win32":
        # Windows: yield without timeout protection for context manager usage
        # For actual timeout needs, use run_with_timeout() function
        yield
    else:
        # Unix-like systems: use signal.alarm
        def timeout_handler(signum, frame):
            raise TimeoutError("Operation timed out")
        
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)


class QueryType(str, Enum):
    """SQL query types"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    TRUNCATE = "TRUNCATE"
    UNKNOWN = "UNKNOWN"


class QueryRisk(str, Enum):
    """Query risk levels"""
    SAFE = "safe"        # SELECT without modifications
    LOW = "low"          # SELECT with filters
    MEDIUM = "medium"    # INSERT/UPDATE with WHERE
    HIGH = "high"        # DELETE with WHERE
    CRITICAL = "critical"  # DROP/TRUNCATE/DELETE without WHERE


@dataclass
class ValidationResult:
    """Query validation result"""
    is_valid: bool
    query_type: QueryType
    risk_level: QueryRisk
    errors: List[str]
    warnings: List[str]
    normalized_query: str
    requires_approval: bool
    issues: List[Dict[str, str]] = None  # Structured feedback for repair
    
    def __post_init__(self):
        """Initialize issues list if not provided"""
        if self.issues is None:
            self.issues = []
            # Convert errors to structured issues
            for error in self.errors:
                self.issues.append({
                    "type": "error",
                    "message": error,
                    "severity": "critical"
                })
            # Convert warnings to structured issues
            for warning in self.warnings:
                self.issues.append({
                    "type": "warning",
                    "message": warning,
                    "severity": "medium"
                })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            "is_valid": self.is_valid,
            "query_type": self.query_type.value,
            "risk_level": self.risk_level.value,
            "errors": self.errors,
            "warnings": self.warnings,
            "normalized_query": self.normalized_query,
            "requires_approval": self.requires_approval,
            "issues": self.issues,  # Structured feedback
        }
        # Include DLP scan results if available
        if hasattr(self, '_dlp_result') and self._dlp_result:
            result["dlp_scan"] = self._dlp_result.to_dict()
        return result


class SQLValidator:
    """
    SQL injection guard and query validator
    
    Features:
    - SQL injection detection
    - Query type identification
    - Risk assessment
    - Dangerous pattern detection
    - Query normalization
    - Parameterization enforcement
    """
    
    # Dangerous SQL patterns
    INJECTION_PATTERNS = [
        r";\s*(DROP|DELETE|TRUNCATE|ALTER|CREATE)\s+",  # Command chaining
        r"(WHERE\s+.+)\s*--",  # Comment after WHERE (may hide conditions)
        r"/\*.*?\*/\s*;",  # Multi-line comment before command terminator (suspicious)
        r"(\bUNION\b.*\bSELECT\b)",  # UNION injection
        r"(\bOR\b\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?)",  # OR 1=1
        r"(\bAND\b\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?)",  # AND 1=1
        r"(xp_cmdshell|sp_executesql|exec\s*\()",  # Stored procedure execution
        r"(INTO\s+OUTFILE|INTO\s+DUMPFILE)",  # File operations
        r"(LOAD_FILE|LOAD\s+DATA)",  # File loading
        r"(BENCHMARK|SLEEP|WAITFOR)",  # Time-based attacks
    ]
    
    # Allowed query types for different roles
    ANALYST_ALLOWED_TYPES = {QueryType.SELECT}
    ADMIN_ALLOWED_TYPES = {QueryType.SELECT, QueryType.INSERT, QueryType.UPDATE, QueryType.DELETE}
    
    # Dangerous keywords requiring approval
    DANGEROUS_KEYWORDS = [
        "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE",
        "GRANT", "REVOKE", "EXEC", "EXECUTE", "SYSTEM"
    ]
    
    def __init__(self):
        self.injection_regex = [re.compile(pattern, re.IGNORECASE) for pattern in self.INJECTION_PATTERNS]
    
    def validate_query(self, sql_query: str, user_role: str = "analyst") -> ValidationResult:
        """
        Validate SQL query for security and correctness
        
        Args:
            sql_query: SQL query to validate
            user_role: User's role for permission checking
            
        Returns:
            ValidationResult with detailed analysis
        """
        errors = []
        warnings = []
        issues: List[Dict[str, str]] = []
        normalized = sql_query.strip()
        
        # Query-level DLP (Data Loss Prevention) check
        # Scans for PII patterns, credentials, and sensitive data exposure risks
        try:
            from app.services.query_dlp_service import QueryDLPService
            
            dlp_service = QueryDLPService()
            dlp_result = dlp_service.scan_query(sql_query)
            
            risk_level = getattr(dlp_result, 'risk_level', 'low')
            if risk_level in ["high", "critical"]:
                # High/critical risk: block the query
                error_msg = f"Data Loss Prevention (DLP) violation detected: {dlp_result.summary}"
                errors.append(error_msg)
                logger.warning(
                    f"DLP blocked query: risk={risk_level}, "
                    f"patterns={len(getattr(dlp_result, 'detected_patterns', []))}, "
                    f"findings={len(getattr(dlp_result, 'findings', []))}"
                )
                
                # Log detailed findings for audit
                for finding in dlp_result.findings[:5]:
                    logger.warning(
                        f"  DLP Finding [{finding.risk_level.upper()}]: {finding.pattern_type} "
                        f"- {finding.description}"
                    )
                
                # Add DLP info to result for downstream processing
                self._dlp_result = dlp_result
                
            elif getattr(dlp_result, 'risk_level', 'low') == "medium":
                # Medium risk: add warning but allow
                warning_msg = f"Potential data sensitivity detected: {dlp_result.summary}"
                warnings.append(warning_msg)
                logger.info(f"DLP warning for query: {dlp_result.summary}")
                self._dlp_result = dlp_result
                
            elif dlp_result.findings:
                # Low risk findings: log only
                logger.debug(f"DLP scan found {len(dlp_result.findings)} low-risk patterns")
                self._dlp_result = dlp_result
                
        except ImportError:
            logger.debug("QueryDLPService not available, skipping DLP check")
        except Exception as e:
            # DLP check failure should not block query validation
            logger.warning(f"DLP check failed (non-fatal): {e}")
        
        # Basic validation
        if not normalized:
            errors.append("Query cannot be empty")
            return ValidationResult(
                is_valid=False,
                query_type=QueryType.UNKNOWN,
                risk_level=QueryRisk.SAFE,
                errors=errors,
                warnings=warnings,
                normalized_query="",
                requires_approval=False
            )
        
        # Length validation
        if len(normalized) > 50000:
            errors.append("Query exceeds maximum length (50000 characters)")
        
        # Detect query type
        query_type = self._detect_query_type(normalized)
        
        # Strip SQL comments before injection check to avoid false positives from LLM-generated comments
        query_without_comments = self._strip_comments(normalized)
        
        # Check for SQL injection patterns (on query WITHOUT comments)
        injection_detected, injection_warnings = self._check_sql_injection(query_without_comments)
        if injection_detected:
            errors.extend(injection_warnings)
            for warning in injection_warnings:
                issues.append({
                    "type": "sql_injection_pattern",
                    "message": warning,
                    "severity": "critical"
                })
        
        # Check for dangerous patterns
        dangerous_detected, dangerous_warnings = self._check_dangerous_patterns(normalized)
        if dangerous_detected:
            warnings.extend(dangerous_warnings)
            for warning in dangerous_warnings:
                issues.append({
                    "type": "dangerous_pattern",
                    "message": warning,
                    "severity": "medium"
                })

        cartesian_findings = self._detect_cartesian_joins(query_without_comments)
        if cartesian_findings:
            warnings.extend(cartesian_findings)
            for warning in cartesian_findings:
                issues.append({
                    "type": "cartesian_join_risk",
                    "message": warning,
                    "severity": "high"
                })
        
        # JOIN/CROSS join approval heuristic
        join_count = len(re.findall(r"\bJOIN\b", normalized, flags=re.IGNORECASE))
        has_cross_join = re.search(r"\bCROSS\s+JOIN\b", normalized, flags=re.IGNORECASE) is not None
        has_comma_join = bool(re.search(r"FROM\s+[^;]*,\s*[^;\s]+", normalized, flags=re.IGNORECASE)) and not re.search(r"\bJOIN\b", normalized, flags=re.IGNORECASE)
        
        # Role-based permission check
        if user_role.lower() == "analyst" and query_type not in self.ANALYST_ALLOWED_TYPES:
            errors.append(f"Analyst role can only execute SELECT queries (attempted: {query_type.value})")
        
        # Risk assessment
        risk_level = self._assess_risk(normalized, query_type)
        
        # FIXED: Less restrictive approval logic - only block truly dangerous queries
        requires_approval = (
            risk_level in [QueryRisk.HIGH, QueryRisk.CRITICAL] or
            query_type in [QueryType.DELETE, QueryType.DROP, QueryType.TRUNCATE, QueryType.ALTER] or
            has_cross_join or join_count >= 6 or bool(cartesian_findings)
        )
        
        # Additional warnings
        if "WHERE" not in normalized.upper() and query_type in [QueryType.UPDATE, QueryType.DELETE]:
            warnings.append("Modification query without WHERE clause - affects all rows")
            requires_approval = True
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            query_type=query_type,
            risk_level=risk_level,
            errors=errors,
            warnings=warnings,
            normalized_query=normalized,
            requires_approval=requires_approval,
            issues=issues
        )
    
    def _detect_query_type(self, query: str) -> QueryType:
        """Detect SQL query type from query"""
        # Strip SQL comments before detecting type (LLMs often add explanatory comments)
        query_stripped = query.strip()
        
        # Remove leading single-line comments
        while query_stripped.startswith('--'):
            newline_idx = query_stripped.find('\n')
            if newline_idx == -1:
                query_stripped = ""
                break
            query_stripped = query_stripped[newline_idx + 1:].strip()
        
        # Remove leading multi-line comments
        while query_stripped.startswith('/*'):
            end_idx = query_stripped.find('*/')
            if end_idx == -1:
                break
            query_stripped = query_stripped[end_idx + 2:].strip()
        
        query_upper = query_stripped.upper().strip()
        
        # Handle CTEs (WITH clause) - these are SELECT queries
        if query_upper.startswith("WITH"):
            return QueryType.SELECT
        
        if query_upper.startswith("SELECT"):
            return QueryType.SELECT
        elif query_upper.startswith("INSERT"):
            return QueryType.INSERT
        elif query_upper.startswith("UPDATE"):
            return QueryType.UPDATE
        elif query_upper.startswith("DELETE"):
            return QueryType.DELETE
        elif query_upper.startswith("CREATE"):
            return QueryType.CREATE
        elif query_upper.startswith("DROP"):
            return QueryType.DROP
        elif query_upper.startswith("ALTER"):
            return QueryType.ALTER
        elif query_upper.startswith("TRUNCATE"):
            return QueryType.TRUNCATE
        else:
            return QueryType.UNKNOWN
    
    def _check_sql_injection(self, query: str) -> Tuple[bool, List[str]]:
        """Check for SQL injection patterns with timeout protection"""
        warnings = []
        
        for pattern in self.injection_regex:
            try:
                with timeout(REGEX_TIMEOUT_SECONDS):
                    if pattern.search(query):
                        warnings.append(f"Potential SQL injection detected: {pattern.pattern}")
            except TimeoutError:
                logger.error(f"Regex timeout checking pattern: {pattern.pattern}")
                warnings.append("Query complexity check timed out - possible ReDoS attack")
                return True, warnings
        
        return len(warnings) > 0, warnings
    
    def _check_dangerous_patterns(self, query: str) -> Tuple[bool, List[str]]:
        """Check for dangerous SQL patterns"""
        warnings = []
        query_upper = query.upper()
        
        for keyword in self.DANGEROUS_KEYWORDS:
            if keyword in query_upper:
                warnings.append(f"Dangerous keyword detected: {keyword}")
        
        # Check for wildcard in DELETE/UPDATE
        if ("DELETE" in query_upper or "UPDATE" in query_upper) and "*" in query:
            warnings.append("Wildcard used in modification query")
        
        # SELECT * (best-practice warning only)
        if re.search(r"\bSELECT\s+\*\b", query_upper) and "COUNT(*)" not in query_upper:
            warnings.append("Using SELECT *; specify columns for performance and clarity")
        
        # JOIN heuristics
        join_count = len(re.findall(r"\bJOIN\b", query_upper))
        has_cross_join = "CROSS JOIN" in query_upper
        # Comma join (FROM a, b) prior to WHERE
        has_comma_join = bool(re.search(r"FROM\s+[^;]*,\s*[^;\s]+", query_upper)) and "JOIN" not in query_upper
        if has_cross_join or has_comma_join:
            warnings.append("Potential Cartesian product (CROSS JOIN or comma join)")
        if join_count >= 4:
            warnings.append(f"Query has {join_count} JOINs; may require review")
        
        return len(warnings) > 0, warnings

    def _detect_cartesian_joins(self, query: str) -> List[str]:
        """Detect likely Cartesian joins or joins without predicates."""
        warnings = []
        query_upper = query.upper()

        if "CROSS JOIN" in query_upper:
            warnings.append("CROSS JOIN detected - potential Cartesian product")

        if re.search(r"FROM\s+[^;]*,\s*[^;\s]+", query_upper) and "JOIN" not in query_upper:
            warnings.append("Comma join detected - potential Cartesian product")

        # Detect JOIN without ON/USING before next clause
        join_segments = re.split(r"\bJOIN\b", query_upper)
        if len(join_segments) > 1:
            for segment in join_segments[1:]:
                # Look ahead up to next JOIN/WHERE/GROUP/ORDER/HAVING
                cutoff = re.split(r"\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b", segment, maxsplit=1)[0]
                if " ON " not in cutoff and " USING " not in cutoff:
                    warnings.append("JOIN without ON/USING detected - potential Cartesian product")
                    break

        return warnings
    
    def _assess_risk(self, query: str, query_type: QueryType) -> QueryRisk:
        """Assess query risk level"""
        query_upper = query.upper()
        
        # CRITICAL: DROP/TRUNCATE or DELETE without WHERE
        if query_type in [QueryType.DROP, QueryType.TRUNCATE]:
            return QueryRisk.CRITICAL
        
        if query_type == QueryType.DELETE and "WHERE" not in query_upper:
            return QueryRisk.CRITICAL
        
        # HIGH: DELETE with WHERE
        if query_type == QueryType.DELETE and "WHERE" in query_upper:
            return QueryRisk.HIGH
        
        # MEDIUM: UPDATE/INSERT
        if query_type in [QueryType.UPDATE, QueryType.INSERT]:
            return QueryRisk.MEDIUM
        
        # LOW: SELECT with filters
        if query_type == QueryType.SELECT and "WHERE" in query_upper:
            return QueryRisk.LOW
        
        # SAFE: Simple SELECT
        if query_type == QueryType.SELECT:
            return QueryRisk.SAFE
        
        return QueryRisk.MEDIUM
    
    def enforce_row_limit(self, query: str, max_rows: int = 1000, dialect: str = "oracle") -> str:
        """
        Enforce maximum row limit on SELECT queries to prevent unbounded results
        
        Args:
            query: SQL query to limit
            max_rows: Maximum number of rows (default 1000)
            dialect: Database dialect (oracle, doris, mysql)
            
        Returns:
            str: Query with FETCH FIRST or LIMIT clause if SELECT
        """
        query_upper = query.upper().strip()
        
        # Only apply to SELECT queries
        if not query_upper.startswith("SELECT"):
            return query
        
        dialect = dialect.lower()
        
        if dialect in ["doris", "mysql", "postgres", "postgresql"]:
            # Check if already has LIMIT
            if "LIMIT" in query_upper:
                logger.debug("Query already has LIMIT, skipping enforcement")
                return query
            
            # Append LIMIT at end (Doris/MySQL)
            # Note: This is a simple append. Complex queries with UNION/subqueries might need parsing,
            # but this covers the 90% case for generated queries.
            limited_query = f"{query.rstrip().rstrip(';')} LIMIT {max_rows}"
            logger.info(f"Enforced row limit of {max_rows} on query (Dialect: {dialect})")
            return limited_query
            
        else:
            # Oracle (default)
            # Check if already has FETCH FIRST or ROWNUM limit
            if "FETCH FIRST" in query_upper or "ROWNUM" in query_upper:
                logger.debug("Query already has row limit, skipping enforcement")
                return query
            
            # Add FETCH FIRST clause (Oracle 12c+ syntax)
            # Place before ORDER BY if present, otherwise at end
            # Note: FETCH FIRST should technically be after ORDER BY in standard SQL, 
            # but Oracle allows it. However, standard is ORDER BY ... FETCH FIRST.
            # My previous logic put it BEFORE ORDER BY which might be wrong for standard SQL but maybe works in Oracle?
            # Actually, Oracle syntax is: [ORDER BY ...] OFFSET ... FETCH ...
            # So it should be AT THE END, after ORDER BY.
            
            # Let's just append it at the end, assuming the query ends with the main clause.
            # If there is a semicolon, remove it.
            limited_query = f"{query.rstrip().rstrip(';')} FETCH FIRST {max_rows} ROWS ONLY"
            
            logger.info(f"Enforced row limit of {max_rows} on query (Dialect: {dialect})")
            return limited_query
    
    def _strip_comments(self, query: str) -> str:
        """
        Strip SQL comments from query for validation purposes.
        This prevents LLM-generated explanatory comments from triggering false positives.
        """
        # Remove single-line comments (--)
        stripped = re.sub(r"--.*$", "", query, flags=re.MULTILINE)
        # Remove multi-line comments (/* */)
        stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)
        return stripped.strip()
    
    def sanitize_query(self, query: str) -> str:
        """
        Sanitize query by removing/escaping dangerous characters
        
        Always use parameterized queries when possible.
        """
        # Remove SQL comments
        sanitized = self._strip_comments(query)
        
        # Remove multiple semicolons (command chaining)
        sanitized = re.sub(r";+", ";", sanitized)
        
        # Normalize whitespace
        sanitized = " ".join(sanitized.split())
        
        return sanitized.strip()
    
    def extract_tables(self, query: str) -> List[str]:
        """Extract table names from query"""
        query_upper = query.upper()
        tables = []
        
        # Extract from FROM clause
        from_match = re.search(r"\bFROM\s+(\w+)", query_upper)
        if from_match:
            tables.append(from_match.group(1))
        
        # Extract from JOIN clauses
        join_matches = re.findall(r"\bJOIN\s+(\w+)", query_upper)
        tables.extend(join_matches)
        
        return list(set(tables))  # Remove duplicates


# Global validator instance
sql_validator = SQLValidator()


def validate_sql(sql_query: str, user_role: str = "analyst") -> ValidationResult:
    """
    Convenience function to validate SQL query
    
    Args:
        sql_query: SQL query to validate
        user_role: User's role
        
    Returns:
        ValidationResult
    """
    return sql_validator.validate_query(sql_query, user_role)


def enforce_sql_validation(sql_query: str, user_role: str = "analyst"):
    """
    Validate SQL and raise exception if invalid
    
    Args:
        sql_query: SQL query to validate
        user_role: User's role
        
    Raises:
        ValueError: If query is invalid
    """
    result = sql_validator.validate_query(sql_query, user_role)
    
    if not result.is_valid:
        raise ValueError(f"Invalid SQL query: {', '.join(result.errors)}")
    
    if result.warnings:
        logger.warning(f"SQL validation warnings: {', '.join(result.warnings)}")
