"""
Enhanced SQL Dialect Converter
Comprehensive dialect mapping for Oracle and Doris using sqlglot
Handles window functions, date functions, JSON paths, CTEs, and more
"""

import logging
import re
from typing import Optional, Dict, Any, List
from enum import Enum

logger = logging.getLogger(__name__)

# Try to import sqlglot for robust transpilation
try:
    import sqlglot
    from sqlglot import exp, parse_one
    from sqlglot.errors import ParseError, ErrorLevel
    SQLGLOT_AVAILABLE = True
    logger.info("[OK] sqlglot library available for advanced SQL dialect conversion")
except ImportError:
    SQLGLOT_AVAILABLE = False
    logger.warning("[WARN] sqlglot not installed - using basic dialect conversion only")


class SQLDialect(str, Enum):
    """Supported SQL dialects"""
    ORACLE = "oracle"
    DORIS = "doris"
    MYSQL = "mysql"  # Doris is MySQL-compatible
    POSTGRES = "postgres"
    POSTGRESQL = "postgresql"  # Alias for postgres
    GENERIC = "generic"


class ConversionResult:
    """Result of SQL dialect conversion"""
    def __init__(
        self,
        sql: str,
        success: bool = True,
        warnings: Optional[List[str]] = None,
        errors: Optional[List[str]] = None,
        unsupported_features: Optional[List[str]] = None
    ):
        self.sql = sql
        self.success = success
        self.warnings = warnings or []
        self.errors = errors or []
        self.unsupported_features = unsupported_features or []
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sql": self.sql,
            "success": self.success,
            "warnings": self.warnings,
            "errors": self.errors,
            "unsupported_features": self.unsupported_features
        }


class SQLDialectConverter:
    """
    Enhanced SQL dialect converter with comprehensive mapping
    Uses sqlglot for robust transpilation when available
    Falls back to regex-based conversion for basic transformations
    """
    
    # Oracle -> Doris/MySQL function mappings (for regex fallback)
    ORACLE_TO_DORIS_FUNCTIONS = {
        # Date functions
        r'\bSYSDATE\b': 'NOW()',
        r'\bCURRENT_DATE\b': 'CURDATE()',
        r'\bCURRENT_TIMESTAMP\b': 'NOW()',
        
        # String functions
        r'\bNVL\s*\(': 'IFNULL(',
        r'\bNVL2\s*\(([^,]+),\s*([^,]+),\s*([^)]+)\)': r'IF(\1 IS NOT NULL, \2, \3)',
        r'\bDECODE\s*\(': 'CASE ',  # Partial - needs more complex handling
        
        # Numeric functions
        r'\bTRUNC\s*\(': 'FLOOR(',
        
        # Type conversion
        r'\bTO_CHAR\s*\(([^,)]+)\)': r'CAST(\1 AS STRING)',
        r'\bTO_NUMBER\s*\(([^,)]+)\)': r'CAST(\1 AS DOUBLE)',
        r'\bTO_DATE\s*\(([^,)]+),\s*([^)]+)\)': r'STR_TO_DATE(\1, \2)',
    }
    
    # Doris/MySQL -> Oracle function mappings (for regex fallback)
    DORIS_TO_ORACLE_FUNCTIONS = {
        # Date functions
        r'\bNOW\s*\(\)': 'SYSDATE',
        r'\bCURDATE\s*\(\)': 'TRUNC(SYSDATE)',
        
        # String functions
        r'\bIFNULL\s*\(': 'NVL(',
        r'\bIF\s*\(([^,]+)\s+IS\s+NOT\s+NULL,\s*([^,]+),\s*([^)]+)\)': r'NVL2(\1, \2, \3)',
        
        # Type conversion
        r'\bCAST\s*\(([^)]+)\s+AS\s+STRING\)': r'TO_CHAR(\1)',
        r'\bCAST\s*\(([^)]+)\s+AS\s+DOUBLE\)': r'TO_NUMBER(\1)',
    }
    
    # PostgreSQL-specific function mappings
    ORACLE_TO_POSTGRES_FUNCTIONS = {
        # Date functions
        r'\bSYSDATE\b': 'NOW()',
        r'\bCURRENT_DATE\b': 'CURRENT_DATE',
        r'\bCURRENT_TIMESTAMP\b': 'CURRENT_TIMESTAMP',
        
        # String functions
        r'\bNVL\s*\(': 'COALESCE(',
        r'\bNVL2\s*\(([^,]+),\s*([^,]+),\s*([^)]+)\)': r'CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END',
        
        # Numeric functions
        r'\bTRUNC\s*\(': 'TRUNC(',
        
        # Type conversion
        r'\bTO_CHAR\s*\(([^,)]+)\)': r'CAST(\1 AS TEXT)',
        r'\bTO_NUMBER\s*\(([^,)]+)\)': r'CAST(\1 AS NUMERIC)',
        r'\bTO_DATE\s*\(([^,)]+),\s*([^)]+)\)': r'TO_DATE(\1, \2)',
    }
    
    POSTGRES_TO_ORACLE_FUNCTIONS = {
        # Date functions
        r'\bNOW\s*\(\)': 'SYSDATE',
        r'\bCURRENT_DATE\b': 'TRUNC(SYSDATE)',
        r'\bCURRENT_TIMESTAMP\b': 'SYSTIMESTAMP',
        
        # String functions
        r'\bCOALESCE\s*\(': 'NVL(',
        
        # Type conversion
        r'\bCAST\s*\(([^)]+)\s+AS\s+TEXT\)': r'TO_CHAR(\1)',
        r'\bCAST\s*\(([^)]+)\s+AS\s+NUMERIC\)': r'TO_NUMBER(\1)',
    }
    
    @staticmethod
    def convert_to_oracle(sql: str, strict: bool = False) -> ConversionResult:
        """
        Convert SQL to Oracle dialect
        
        Args:
            sql: Input SQL query
            strict: If True, fail on unsupported features; if False, best-effort conversion
            
        Returns:
            ConversionResult with converted SQL and metadata
        """
        if SQLGLOT_AVAILABLE:
            return SQLDialectConverter._convert_with_sqlglot(
                sql,
                source_dialect="mysql",  # Doris is MySQL-compatible
                target_dialect="oracle",
                strict=strict
            )
        else:
            return SQLDialectConverter._convert_to_oracle_regex(sql)
    
    @staticmethod
    def convert_to_postgres(sql: str, strict: bool = False) -> ConversionResult:
        """
        Convert SQL to PostgreSQL dialect
        
        Args:
            sql: Input SQL query
            strict: If True, fail on unsupported features; if False, best-effort conversion
            
        Returns:
            ConversionResult with converted SQL and metadata
        """
        if SQLGLOT_AVAILABLE:
            return SQLDialectConverter._convert_with_sqlglot(
                sql,
                source_dialect="oracle",
                target_dialect="postgres",
                strict=strict
            )
        else:
            return SQLDialectConverter._convert_to_postgres_regex(sql)
    
    @staticmethod
    def convert_to_doris(sql: str, strict: bool = False) -> ConversionResult:
        """
        Convert SQL to Doris (MySQL-compatible) dialect
        
        Args:
            sql: Input SQL query
            strict: If True, fail on unsupported features; if False, best-effort conversion
            
        Returns:
            ConversionResult with converted SQL and metadata
        """
        if SQLGLOT_AVAILABLE:
            return SQLDialectConverter._convert_with_sqlglot(
                sql,
                source_dialect="oracle",
                target_dialect="doris",  # Use "mysql" if "doris" not supported
                strict=strict
            )
        else:
            return SQLDialectConverter._convert_to_doris_regex(sql)
    
    @staticmethod
    def _convert_with_sqlglot(
        sql: str,
        source_dialect: str,
        target_dialect: str,
        strict: bool = False
    ) -> ConversionResult:
        """
        Convert SQL using sqlglot library
        
        Args:
            sql: Input SQL
            source_dialect: Source SQL dialect
            target_dialect: Target SQL dialect
            strict: Error handling mode
            
        Returns:
            ConversionResult
        """
        warnings = []
        errors = []
        unsupported_features = []
        
        try:
            # Map "doris" to "mysql" since Doris is MySQL-compatible
            if target_dialect == "doris":
                target_dialect = "mysql"
            
            # Parse SQL from source dialect
            try:
                ast = parse_one(sql, read=source_dialect, error_level=ErrorLevel.WARN)
            except ParseError as e:
                if strict:
                    return ConversionResult(
                        sql=sql,
                        success=False,
                        errors=[f"Parse error: {str(e)}"]
                    )
                else:
                    warnings.append(f"Parse warning: {str(e)}")
                    # Fall back to regex conversion
                    if target_dialect == "mysql":
                        return SQLDialectConverter._convert_to_doris_regex(sql)
                    else:
                        return SQLDialectConverter._convert_to_oracle_regex(sql)
            
            # Generate SQL in target dialect
            converted_sql = ast.sql(dialect=target_dialect, pretty=False)
            
            # Check for unsupported features
            if hasattr(ast, 'find_all'):
                # Check for Oracle-specific features that may not translate well
                if source_dialect == "oracle":
                    # Check for CONNECT BY (hierarchical queries)
                    if "CONNECT BY" in sql.upper():
                        unsupported_features.append("CONNECT BY hierarchical queries")
                    
                    # Check for MODEL clause
                    if "MODEL" in sql.upper():
                        unsupported_features.append("MODEL clause")
                    
                    # Check for MERGE statement
                    if "MERGE" in sql.upper():
                        unsupported_features.append("MERGE statement")
            
            success = len(errors) == 0
            if not success and not strict:
                success = True  # Allow best-effort conversion
            
            return ConversionResult(
                sql=converted_sql,
                success=success,
                warnings=warnings,
                errors=errors,
                unsupported_features=unsupported_features
            )
            
        except Exception as e:
            error_msg = f"sqlglot conversion failed: {str(e)}"
            logger.error(error_msg)
            
            if strict:
                return ConversionResult(
                    sql=sql,
                    success=False,
                    errors=[error_msg]
                )
            else:
                # Fall back to regex conversion
                warnings.append(error_msg + " - falling back to regex conversion")
                if target_dialect == "mysql":
                    return SQLDialectConverter._convert_to_doris_regex(sql)
                else:
                    return SQLDialectConverter._convert_to_oracle_regex(sql)
    
    @staticmethod
    def _convert_to_postgres_regex(sql: str) -> ConversionResult:
        """
        Convert Oracle SQL to PostgreSQL using regex (fallback method)
        
        Args:
            sql: Oracle SQL query
            
        Returns:
            ConversionResult
        """
        warnings = []
        converted = sql
        
        # Apply function mappings
        for pattern, replacement in SQLDialectConverter.ORACLE_TO_POSTGRES_FUNCTIONS.items():
            if re.search(pattern, converted, re.IGNORECASE):
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
                logger.debug(f"Converted pattern: {pattern} -> {replacement}")
        
        # Convert FETCH FIRST n ROWS ONLY to LIMIT n
        fetch_match = re.search(
            r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b',
            converted,
            re.IGNORECASE
        )
        if fetch_match:
            limit_value = fetch_match.group(1)
            converted = re.sub(
                r'\bFETCH\s+FIRST\s+\d+\s+ROWS?\s+ONLY\b',
                f'LIMIT {limit_value}',
                converted,
                flags=re.IGNORECASE
            )
            logger.debug(f"Converted FETCH FIRST to LIMIT {limit_value}")
        
        # Convert Oracle outer join syntax (+) to ANSI joins (best-effort)
        if '(+)' in converted:
            warnings.append("Oracle outer join syntax (+) detected - manual review recommended")
        
        # Convert dual table references
        converted = re.sub(r'\bFROM\s+DUAL\b', '', converted, flags=re.IGNORECASE)
        
        # Check for unsupported features
        unsupported = []
        if re.search(r'\bCONNECT\s+BY\b', converted, re.IGNORECASE):
            unsupported.append("CONNECT BY hierarchical queries")
            warnings.append("CONNECT BY is not supported in PostgreSQL - use WITH RECURSIVE instead")
        
        if re.search(r'\bSTART\s+WITH\b', converted, re.IGNORECASE):
            unsupported.append("START WITH clause")
        
        if re.search(r'\bMERGE\b', converted, re.IGNORECASE):
            unsupported.append("MERGE statement")
            warnings.append("MERGE is not supported in PostgreSQL - use INSERT ... ON CONFLICT instead")
        
        return ConversionResult(
            sql=converted,
            success=True,
            warnings=warnings,
            unsupported_features=unsupported
        )
    
    @staticmethod
    def _convert_to_doris_regex(sql: str) -> ConversionResult:
        """
        Convert Oracle SQL to Doris using regex (fallback method)
        
        Args:
            sql: Oracle SQL query
            
        Returns:
            ConversionResult
        """
        warnings = []
        converted = sql
        
        # Apply function mappings
        for pattern, replacement in SQLDialectConverter.ORACLE_TO_DORIS_FUNCTIONS.items():
            if re.search(pattern, converted, re.IGNORECASE):
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
                logger.debug(f"Converted pattern: {pattern} -> {replacement}")
        
        # Convert FETCH FIRST n ROWS ONLY to LIMIT n
        fetch_match = re.search(
            r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b',
            converted,
            re.IGNORECASE
        )
        if fetch_match:
            limit_value = fetch_match.group(1)
            converted = re.sub(
                r'\bFETCH\s+FIRST\s+\d+\s+ROWS?\s+ONLY\b',
                f'LIMIT {limit_value}',
                converted,
                flags=re.IGNORECASE
            )
            logger.debug(f"Converted FETCH FIRST to LIMIT {limit_value}")
        
        # Convert Oracle outer join syntax (+) to ANSI joins (best-effort)
        if '(+)' in converted:
            warnings.append("Oracle outer join syntax (+) detected - manual review recommended")
        
        # Convert dual table references
        converted = re.sub(r'\bFROM\s+DUAL\b', 'FROM dual', converted, flags=re.IGNORECASE)
        
        # Check for unsupported features
        unsupported = []
        if re.search(r'\bCONNECT\s+BY\b', converted, re.IGNORECASE):
            unsupported.append("CONNECT BY hierarchical queries")
            warnings.append("CONNECT BY is not supported in Doris - query may fail")
        
        if re.search(r'\bSTART\s+WITH\b', converted, re.IGNORECASE):
            unsupported.append("START WITH clause")
        
        if re.search(r'\bMERGE\b', converted, re.IGNORECASE):
            unsupported.append("MERGE statement")
            warnings.append("MERGE is not supported in Doris")
        
        return ConversionResult(
            sql=converted,
            success=True,
            warnings=warnings,
            unsupported_features=unsupported
        )
    
    @staticmethod
    def _convert_to_oracle_regex(sql: str) -> ConversionResult:
        """
        Convert Doris/MySQL SQL to Oracle using regex (fallback method)
        
        Args:
            sql: Doris/MySQL SQL query
            
        Returns:
            ConversionResult
        """
        warnings = []
        converted = sql
        
        # Apply function mappings
        for pattern, replacement in SQLDialectConverter.DORIS_TO_ORACLE_FUNCTIONS.items():
            if re.search(pattern, converted, re.IGNORECASE):
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
                logger.debug(f"Converted pattern: {pattern} -> {replacement}")
        
        # Convert LIMIT n to FETCH FIRST n ROWS ONLY
        limit_match = re.search(r'\bLIMIT\s+(\d+)(?:\s+OFFSET\s+\d+)?\s*$', converted, re.IGNORECASE)
        if limit_match:
            limit_value = limit_match.group(1)
            converted = re.sub(
                r'\bLIMIT\s+\d+(?:\s+OFFSET\s+\d+)?\s*$',
                f'FETCH FIRST {limit_value} ROWS ONLY',
                converted,
                flags=re.IGNORECASE
            )
            logger.debug(f"Converted LIMIT to FETCH FIRST {limit_value} ROWS ONLY")
        
        # Convert backticks to Oracle quotes
        converted = converted.replace('`', '"')
        
        return ConversionResult(
            sql=converted,
            success=True,
            warnings=warnings
        )
    
    @staticmethod
    def validate_for_dialect(sql: str, dialect: SQLDialect) -> ConversionResult:
        """
        Validate SQL for a specific dialect without conversion
        
        Args:
            sql: SQL query
            dialect: Target dialect for validation
            
        Returns:
            ConversionResult with validation results
        """
        if not SQLGLOT_AVAILABLE:
            return ConversionResult(
                sql=sql,
                success=True,
                warnings=["sqlglot not available - validation skipped"]
            )
        
        dialect_str = dialect.value
        if dialect_str == "doris":
            dialect_str = "mysql"  # Doris is MySQL-compatible
        
        errors = []
        warnings = []
        
        try:
            ast = parse_one(sql, read=dialect_str, error_level=ErrorLevel.RAISE)
            return ConversionResult(
                sql=sql,
                success=True
            )
        except ParseError as e:
            errors.append(f"SQL validation error: {str(e)}")
            return ConversionResult(
                sql=sql,
                success=False,
                errors=errors
            )
        except Exception as e:
            warnings.append(f"Validation warning: {str(e)}")
            return ConversionResult(
                sql=sql,
                success=True,
                warnings=warnings
            )


def convert_sql(sql: str, from_dialect: str, to_dialect: str, strict: bool = False) -> ConversionResult:
    """
    Convenience function for SQL dialect conversion
    
    Args:
        sql: Input SQL query
        from_dialect: Source dialect ("oracle", "doris", "mysql", "postgres")
        to_dialect: Target dialect ("oracle", "doris", "mysql", "postgres")
        strict: If True, fail on unsupported features
        
    Returns:
        ConversionResult
    """
    to_dialect_lower = to_dialect.lower()
    
    if to_dialect_lower in ["doris", "mysql"]:
        return SQLDialectConverter.convert_to_doris(sql, strict=strict)
    elif to_dialect_lower == "oracle":
        return SQLDialectConverter.convert_to_oracle(sql, strict=strict)
    elif to_dialect_lower in ["postgres", "postgresql"]:
        return SQLDialectConverter.convert_to_postgres(sql, strict=strict)
    else:
        logger.warning(f"Unknown target dialect: {to_dialect}")
        return ConversionResult(sql=sql, success=False, errors=[f"Unknown dialect: {to_dialect}"])
