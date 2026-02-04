"""
Query Optimizer Service - SQL optimization suggestions

Provides:
- Oracle-specific optimization hints
- Performance issue detection
- Index recommendations
- Query rewrite suggestions
"""

import logging
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OptimizationSuggestion:
    """Single optimization suggestion"""
    type: str  # index, rewrite, hint, partition
    severity: str  # info, warning, critical
    description: str
    original_pattern: str
    suggested_fix: Optional[str] = None
    estimated_improvement: Optional[str] = None


class QueryOptimizerService:
    """Service for analyzing and optimizing SQL queries"""
    
    def __init__(self):
        self._optimization_patterns = self._load_optimization_patterns()
    
    def analyze_query(
        self,
        sql: str,
        schema_context: Optional[Dict[str, Any]] = None
    ) -> List[OptimizationSuggestion]:
        """
        Analyze SQL query and provide optimization suggestions
        
        Args:
            sql: SQL query to analyze
            schema_context: Optional schema context with table metadata
            
        Returns:
            List of optimization suggestions
        """
        logger.info(f"Analyzing query for optimization opportunities...")
        
        suggestions = []
        sql_upper = sql.upper()
        
        # Check for SELECT * (should use specific columns)
        if self._has_select_star(sql_upper):
            suggestions.append(OptimizationSuggestion(
                type="rewrite",
                severity="warning",
                description="Using SELECT * retrieves all columns, which can be inefficient",
                original_pattern="SELECT *",
                suggested_fix="SELECT specific_column1, specific_column2, ...",
                estimated_improvement="20-50% faster with specific columns"
            ))
        
        # Check for missing WHERE clause on large tables
        if schema_context and "WHERE" not in sql_upper:
            suggestions.extend(self._check_full_table_scan(sql, schema_context))
        
        # Check for missing ORDER BY limit (Oracle: FETCH FIRST)
        if "ORDER BY" in sql_upper and "FETCH FIRST" not in sql_upper and "ROWNUM" not in sql_upper:
            suggestions.append(OptimizationSuggestion(
                type="hint",
                severity="info",
                description="ORDER BY without limit may sort unnecessary rows",
                original_pattern="ORDER BY ... (no limit)",
                suggested_fix="Add FETCH FIRST N ROWS ONLY to limit sorted results",
                estimated_improvement="Faster for large result sets"
            ))
        
        # Check for potential index opportunities
        if "WHERE" in sql_upper:
            suggestions.extend(self._suggest_indexes(sql, schema_context))
        
        # Check for JOIN without indexes
        if "JOIN" in sql_upper:
            suggestions.extend(self._check_join_optimization(sql, schema_context))
        
        # Check for Oracle-specific optimizations
        suggestions.extend(self._suggest_oracle_hints(sql, schema_context))
        
        # Check for partition opportunities
        suggestions.extend(self._check_partition_hints(sql, schema_context))
        
        logger.info(f"Found {len(suggestions)} optimization suggestions")
        return suggestions
    
    def _has_select_star(self, sql_upper: str) -> bool:
        """Check if query uses SELECT *"""
        # Match SELECT * but not SELECT COUNT(*)
        return bool(re.search(r'\bSELECT\s+\*\s', sql_upper)) and "COUNT(*)" not in sql_upper
    
    def _check_full_table_scan(
        self,
        sql: str,
        schema_context: Dict[str, Any]
    ) -> List[OptimizationSuggestion]:
        """Check for potential full table scans on large tables"""
        suggestions = []
        
        tables = schema_context.get("tables", {})
        sql_upper = sql.upper()
        
        for table_name, columns in tables.items():
            if table_name.upper() in sql_upper:
                # Check if table is likely large (heuristic: many columns = complex table)
                if len(columns) > 10:
                    suggestions.append(OptimizationSuggestion(
                        type="rewrite",
                        severity="warning",
                        description=f"Full table scan on {table_name} without WHERE clause",
                        original_pattern=f"FROM {table_name} (no WHERE)",
                        suggested_fix=f"Add WHERE clause to filter {table_name} results",
                        estimated_improvement="Can be 100x+ faster with proper filtering"
                    ))
        
        return suggestions
    
    def _suggest_indexes(
        self,
        sql: str,
        schema_context: Optional[Dict[str, Any]]
    ) -> List[OptimizationSuggestion]:
        """Suggest index opportunities based on WHERE clause"""
        suggestions = []
        
        # Extract columns from WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:ORDER BY|GROUP BY|FETCH|$)', sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return suggestions
        
        where_clause = where_match.group(1)
        
        # Find column references in WHERE clause
        # Pattern: column_name = value, column_name IN (...), etc.
        column_patterns = re.findall(r'\b([A-Z_][A-Z0-9_]*)\s*(?:=|IN|<|>|LIKE)', where_clause.upper())
        
        if column_patterns:
            unique_columns = sorted(set(column_patterns))
            suggestions.append(OptimizationSuggestion(
                type="index",
                severity="info",
                description=f"Consider indexes on WHERE clause columns: {', '.join(unique_columns)}",
                original_pattern="WHERE clause columns without confirmed indexes",
                suggested_fix=f"CREATE INDEX idx_name ON table_name({', '.join(unique_columns[:3])})",
                estimated_improvement="10-1000x faster for filtered queries"
            ))
        
        return suggestions
    
    def _check_join_optimization(
        self,
        sql: str,
        schema_context: Optional[Dict[str, Any]]
    ) -> List[OptimizationSuggestion]:
        """Check JOIN optimization opportunities"""
        suggestions = []
        sql_upper = sql.upper()
        
        # Count JOINs
        join_count = sql_upper.count("JOIN")
        
        if join_count > 3:
            suggestions.append(OptimizationSuggestion(
                type="hint",
                severity="warning",
                description=f"Query has {join_count} JOINs - consider query decomposition or materialized views",
                original_pattern=f"{join_count} JOINs in single query",
                suggested_fix="Break into multiple queries or use MATERIALIZED VIEW",
                estimated_improvement="Simpler execution plans, easier to optimize"
            ))
        
        # Check for CROSS JOIN or Cartesian product
        if "CROSS JOIN" in sql_upper or (
            "FROM" in sql_upper and 
            "," in sql.split("WHERE")[0] and 
            "JOIN" not in sql_upper
        ):
            suggestions.append(OptimizationSuggestion(
                type="rewrite",
                severity="critical",
                description="Potential Cartesian product detected",
                original_pattern="CROSS JOIN or comma-separated FROM",
                suggested_fix="Add explicit JOIN conditions to avoid Cartesian product",
                estimated_improvement="Critical - prevents exponential result growth"
            ))
        
        return suggestions
    
    def _suggest_oracle_hints(
        self,
        sql: str,
        schema_context: Optional[Dict[str, Any]]
    ) -> List[OptimizationSuggestion]:
        """Suggest Oracle-specific hints"""
        suggestions = []
        sql_upper = sql.upper()
        
        # Check if hints are already present
        has_hints = "/*+" in sql
        
        if not has_hints:
            # Suggest ALL_ROWS optimizer mode for reporting queries
            if "GROUP BY" in sql_upper or "SUM" in sql_upper or "COUNT" in sql_upper:
                suggestions.append(OptimizationSuggestion(
                    type="hint",
                    severity="info",
                    description="Aggregation query - consider ALL_ROWS hint",
                    original_pattern="SELECT without hint",
                    suggested_fix="SELECT /*+ ALL_ROWS */ ...",
                    estimated_improvement="Optimizes for full result set retrieval"
                ))
            
            # Suggest FIRST_ROWS for LIMIT queries
            if "FETCH FIRST" in sql_upper or "ROWNUM" in sql_upper:
                suggestions.append(OptimizationSuggestion(
                    type="hint",
                    severity="info",
                    description="Limited result query - consider FIRST_ROWS hint",
                    original_pattern="SELECT without hint",
                    suggested_fix="SELECT /*+ FIRST_ROWS(n) */ ...",
                    estimated_improvement="Optimizes for fast first-row retrieval"
                ))
        
        return suggestions
    
    def _check_partition_hints(
        self,
        sql: str,
        schema_context: Optional[Dict[str, Any]]
    ) -> List[OptimizationSuggestion]:
        """Check if partitioning hints could help"""
        suggestions = []
        
        if not schema_context:
            return suggestions
        
        tables = schema_context.get("tables", {})
        sql_upper = sql.upper()
        
        # Check for date-based filtering (common partition key)
        has_date_filter = any(
            pattern in sql_upper 
            for pattern in ["DATE", "TIMESTAMP", "CREATED_AT", "UPDATED_AT"]
        )
        
        if has_date_filter:
            for table_name in tables.keys():
                if table_name.upper() in sql_upper:
                    suggestions.append(OptimizationSuggestion(
                        type="partition",
                        severity="info",
                        description=f"Date-based filtering on {table_name} - may benefit from partition pruning",
                        original_pattern="Date filter without partition hint",
                        suggested_fix=f"If {table_name} is partitioned, optimizer should auto-prune",
                        estimated_improvement="Skip reading irrelevant partitions"
                    ))
        
        return suggestions
    
    def _load_optimization_patterns(self) -> Dict[str, Any]:
        """Load optimization patterns (extensible for future ML-based patterns)"""
        return {
            "avoid_select_star": True,
            "suggest_indexes": True,
            "check_joins": True,
            "oracle_hints": True,
            "partition_awareness": True,
        }
    
    def format_suggestions_for_llm(self, suggestions: List[OptimizationSuggestion]) -> str:
        """Format suggestions for LLM context"""
        if not suggestions:
            return ""
        
        output = "\n\n QUERY OPTIMIZATION SUGGESTIONS:\n"
        output += "=" * 60 + "\n"
        
        # Group by severity
        critical = [s for s in suggestions if s.severity == "critical"]
        warnings = [s for s in suggestions if s.severity == "warning"]
        info = [s for s in suggestions if s.severity == "info"]
        
        for severity_name, severity_suggestions in [
            (" CRITICAL", critical),
            (" WARNINGS", warnings),
            (" INFO", info)
        ]:
            if severity_suggestions:
                output += f"\n{severity_name}:\n"
                for i, sug in enumerate(severity_suggestions, 1):
                    output += f"  {i}. [{sug.type.upper()}] {sug.description}\n"
                    if sug.suggested_fix:
                        output += f"     Fix: {sug.suggested_fix}\n"
                    if sug.estimated_improvement:
                        output += f"     Impact: {sug.estimated_improvement}\n"
        
        output += "=" * 60 + "\n"
        return output
