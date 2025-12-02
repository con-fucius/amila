"""
SQL Dialect Service
Handles database-specific SQL syntax differences between Oracle and Doris
"""

import logging
import re
from typing import Dict, Any, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class DatabaseDialect(str, Enum):
    """Supported database dialects"""
    ORACLE = "oracle"
    DORIS = "doris"


class SQLDialectService:
    """
    Service for handling SQL dialect differences between Oracle and Doris
    
    Key differences:
    - LIMIT vs FETCH FIRST
    - String concatenation (|| vs CONCAT)
    - Date functions
    - NULL handling
    - Case sensitivity
    - Data types
    """
    
    # Oracle-specific patterns that need conversion for Doris
    ORACLE_TO_DORIS_PATTERNS = [
        # FETCH FIRST -> LIMIT
        (r"\bFETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY\b", r"LIMIT \1"),
        # ROWNUM -> LIMIT (simple cases)
        (r"\bWHERE\s+ROWNUM\s*<=?\s*(\d+)\b", r"LIMIT \1"),
        # NVL -> IFNULL/COALESCE
        (r"\bNVL\s*\(", "IFNULL("),
        # DECODE -> CASE (basic pattern)
        (r"\bDECODE\s*\(", "CASE "),
        # SYSDATE -> NOW()
        (r"\bSYSDATE\b", "NOW()"),
        # TO_DATE -> STR_TO_DATE (basic)
        (r"\bTO_DATE\s*\(([^,]+),\s*'([^']+)'\)", r"STR_TO_DATE(\1, '\2')"),
        # TO_CHAR for dates -> DATE_FORMAT
        (r"\bTO_CHAR\s*\(([^,]+),\s*'([^']+)'\)", r"DATE_FORMAT(\1, '\2')"),
        # || string concat -> CONCAT
        (r"(\w+)\s*\|\|\s*(\w+)", r"CONCAT(\1, \2)"),
        # DUAL table -> remove
        (r"\bFROM\s+DUAL\b", ""),
    ]
    
    # Doris-specific patterns that need conversion for Oracle
    DORIS_TO_ORACLE_PATTERNS = [
        # LIMIT -> FETCH FIRST
        (r"\bLIMIT\s+(\d+)\b", r"FETCH FIRST \1 ROWS ONLY"),
        # IFNULL -> NVL
        (r"\bIFNULL\s*\(", "NVL("),
        # NOW() -> SYSDATE
        (r"\bNOW\s*\(\s*\)", "SYSDATE"),
        # CONCAT -> ||
        (r"\bCONCAT\s*\(([^,]+),\s*([^)]+)\)", r"\1 || \2"),
    ]
    
    # Oracle-specific keywords/functions
    ORACLE_SPECIFIC = {
        "ROWNUM", "ROWID", "SYSDATE", "SYSTIMESTAMP", "NVL", "NVL2",
        "DECODE", "CONNECT BY", "START WITH", "PRIOR", "LEVEL",
        "MINUS", "FETCH FIRST", "ROWS ONLY", "DUAL", "TRUNC",
        "TO_DATE", "TO_CHAR", "TO_NUMBER", "ADD_MONTHS", "MONTHS_BETWEEN"
    }
    
    # Doris-specific keywords/functions
    DORIS_SPECIFIC = {
        "LIMIT", "OFFSET", "IFNULL", "NOW", "CURDATE", "CURTIME",
        "DATE_FORMAT", "STR_TO_DATE", "DATEDIFF", "DATE_ADD", "DATE_SUB",
        "GROUP_CONCAT", "JSON_EXTRACT", "BITMAP", "HLL"
    }
    
    @classmethod
    def detect_dialect(cls, sql: str) -> Optional[DatabaseDialect]:
        """
        Detect the SQL dialect based on syntax patterns
        
        Args:
            sql: SQL query string
            
        Returns:
            Detected dialect or None if ambiguous
        """
        sql_upper = sql.upper()
        
        oracle_score = 0
        doris_score = 0
        
        for keyword in cls.ORACLE_SPECIFIC:
            if keyword in sql_upper:
                oracle_score += 1
        
        for keyword in cls.DORIS_SPECIFIC:
            if keyword in sql_upper:
                doris_score += 1
        
        if oracle_score > doris_score:
            return DatabaseDialect.ORACLE
        elif doris_score > oracle_score:
            return DatabaseDialect.DORIS
        
        return None
    
    @classmethod
    def convert_to_dialect(
        cls,
        sql: str,
        target_dialect: DatabaseDialect,
        source_dialect: Optional[DatabaseDialect] = None
    ) -> str:
        """
        Convert SQL to target dialect
        
        Args:
            sql: SQL query string
            target_dialect: Target database dialect
            source_dialect: Source dialect (auto-detected if not provided)
            
        Returns:
            Converted SQL string
        """
        if not source_dialect:
            source_dialect = cls.detect_dialect(sql)
        
        if source_dialect == target_dialect:
            return sql
        
        converted = sql
        
        if target_dialect == DatabaseDialect.DORIS:
            # Convert Oracle -> Doris
            for pattern, replacement in cls.ORACLE_TO_DORIS_PATTERNS:
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        elif target_dialect == DatabaseDialect.ORACLE:
            # Convert Doris -> Oracle
            for pattern, replacement in cls.DORIS_TO_ORACLE_PATTERNS:
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        
        return converted

    @classmethod
    def validate_for_dialect(cls, sql: str, dialect: DatabaseDialect) -> Dict[str, Any]:
        """
        Validate SQL for a specific dialect and return issues
        
        Args:
            sql: SQL query string
            dialect: Target database dialect
            
        Returns:
            Dict with validation results and suggestions
        """
        issues = []
        suggestions = []
        sql_upper = sql.upper()
        
        if dialect == DatabaseDialect.DORIS:
            # Check for Oracle-specific syntax
            for keyword in cls.ORACLE_SPECIFIC:
                if keyword in sql_upper:
                    issues.append(f"Oracle-specific syntax detected: {keyword}")
                    suggestions.append(f"Consider converting {keyword} to Doris equivalent")
            
            # Check for FETCH FIRST (Oracle pagination)
            if "FETCH FIRST" in sql_upper:
                suggestions.append("Use LIMIT instead of FETCH FIRST for Doris")
            
            # Check for NVL
            if "NVL(" in sql_upper:
                suggestions.append("Use IFNULL() or COALESCE() instead of NVL() for Doris")
                
        elif dialect == DatabaseDialect.ORACLE:
            # Check for Doris-specific syntax
            for keyword in cls.DORIS_SPECIFIC:
                if keyword in sql_upper and keyword not in {"LIMIT", "OFFSET"}:
                    issues.append(f"Doris-specific syntax detected: {keyword}")
                    suggestions.append(f"Consider converting {keyword} to Oracle equivalent")
            
            # Check for LIMIT (Doris pagination)
            if re.search(r"\bLIMIT\s+\d+", sql_upper):
                suggestions.append("Use FETCH FIRST N ROWS ONLY instead of LIMIT for Oracle")
        
        return {
            "is_valid": len(issues) == 0,
            "dialect": dialect.value,
            "issues": issues,
            "suggestions": suggestions,
            "auto_convertible": len(issues) <= 3  # Simple cases can be auto-converted
        }
    
    @classmethod
    def get_dialect_prompt_hints(cls, dialect: DatabaseDialect) -> str:
        """
        Get SQL generation hints for a specific dialect
        
        Args:
            dialect: Target database dialect
            
        Returns:
            String with dialect-specific SQL generation hints
        """
        if dialect == DatabaseDialect.ORACLE:
            return """
ORACLE SQL DIALECT RULES:
- Use FETCH FIRST N ROWS ONLY for pagination (not LIMIT)
- Use NVL(column, default) for null handling
- Use SYSDATE for current date/time
- Use TO_DATE('value', 'format') for date parsing
- Use TO_CHAR(date, 'format') for date formatting
- Use || for string concatenation
- Use DECODE() or CASE for conditional logic
- Use ROWNUM for row numbering (in WHERE clause)
- Use DUAL table for SELECT without FROM
- Oracle is case-insensitive for identifiers unless quoted
- Use double quotes for case-sensitive identifiers
"""
        elif dialect == DatabaseDialect.DORIS:
            return """
DORIS SQL DIALECT RULES:
- Use LIMIT N for pagination (not FETCH FIRST)
- Use IFNULL(column, default) or COALESCE() for null handling
- Use NOW() for current date/time
- Use STR_TO_DATE('value', 'format') for date parsing
- Use DATE_FORMAT(date, 'format') for date formatting
- Use CONCAT() for string concatenation
- Use CASE WHEN for conditional logic
- Doris is case-sensitive for table/column names
- Use backticks for identifiers with special characters
- Doris supports analytical functions like OVER()
- Doris is optimized for aggregation queries
"""
        return ""
    
    @classmethod
    def get_schema_differences(cls) -> Dict[str, Any]:
        """
        Get documentation of schema/type differences between Oracle and Doris
        """
        return {
            "data_types": {
                "oracle_to_doris": {
                    "VARCHAR2": "VARCHAR",
                    "NUMBER": "DECIMAL or BIGINT",
                    "DATE": "DATETIME",
                    "CLOB": "STRING",
                    "BLOB": "STRING (base64)",
                    "RAW": "STRING",
                },
                "doris_to_oracle": {
                    "VARCHAR": "VARCHAR2",
                    "INT": "NUMBER",
                    "BIGINT": "NUMBER",
                    "DATETIME": "DATE or TIMESTAMP",
                    "STRING": "CLOB",
                    "BOOLEAN": "NUMBER(1)",
                }
            },
            "features": {
                "oracle_only": [
                    "Hierarchical queries (CONNECT BY)",
                    "MINUS operator",
                    "ROWID pseudo-column",
                    "Sequences",
                    "Materialized views with refresh",
                ],
                "doris_only": [
                    "Bitmap indexes",
                    "HyperLogLog",
                    "Rollup tables",
                    "Dynamic partitioning",
                    "Vectorized execution",
                ]
            }
        }
