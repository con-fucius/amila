"""
Tests for SQL generation enhancements

Tests:
- Schema enrichment service
- Query optimizer service
- Confidence scoring
"""

import pytest
from app.services.schema_enrichment_service import SchemaEnrichmentService
from app.services.query_optimizer_service import QueryOptimizerService, OptimizationSuggestion
from app.utils.oracle_identifiers import normalize_oracle_identifiers


class TestSchemaEnrichmentService:
    """Test schema enrichment functionality"""
    
    def test_extract_table_hints(self):
        """Test table name extraction from query"""
        service = SchemaEnrichmentService()
        
        schema_data = {
            "tables": {
                "EMPLOYEES": [],
                "DEPARTMENTS": [],
                "SALES_DATA": []
            }
        }
        
        # Test direct mention
        tables = service._extract_table_hints(
            "Show me all employees",
            "",
            schema_data
        )
        assert "EMPLOYEES" in tables
        
        # Test partial match
        tables = service._extract_table_hints(
            "Get sales information",
            "",
            schema_data
        )
        assert "SALES_DATA" in tables


class TestQueryOptimizerService:
    """Test query optimization analysis"""
    
    def test_detect_select_star(self):
        """Test SELECT * detection"""
        optimizer = QueryOptimizerService()
        
        sql = "SELECT * FROM EMPLOYEES WHERE DEPARTMENT_ID = 10"
        suggestions = optimizer.analyze_query(sql)
        
        # Should suggest avoiding SELECT *
        assert any(s.type == "rewrite" for s in suggestions)
        assert any("SELECT *" in s.description for s in suggestions)
    
    def test_detect_missing_where(self):
        """Test full table scan detection"""
        optimizer = QueryOptimizerService()
        
        schema_context = {
            "tables": {
                "LARGE_TABLE": [{"name": f"COL{i}"} for i in range(20)]
            }
        }
        
        sql = "SELECT COL1, COL2 FROM LARGE_TABLE"
        suggestions = optimizer.analyze_query(sql, schema_context)
        
        # Should warn about full table scan
        assert any("full table scan" in s.description.lower() for s in suggestions)
    
    def test_suggest_oracle_hints(self):
        """Test Oracle hint suggestions"""
        optimizer = QueryOptimizerService()
        
        # Aggregation query
        sql = "SELECT DEPARTMENT_ID, COUNT(*) FROM EMPLOYEES GROUP BY DEPARTMENT_ID"
        suggestions = optimizer.analyze_query(sql)
        
        # Should suggest ALL_ROWS hint for aggregation
        assert any("ALL_ROWS" in str(s.suggested_fix) for s in suggestions if s.suggested_fix)
    
    def test_format_suggestions(self):
        """Test formatting suggestions for display"""
        optimizer = QueryOptimizerService()
        
        suggestions = [
            OptimizationSuggestion(
                type="index",
                severity="warning",
                description="Consider adding index",
                original_pattern="WHERE clause",
                suggested_fix="CREATE INDEX ...",
                estimated_improvement="10-100x faster"
            )
        ]
        
        formatted = optimizer.format_suggestions_for_llm(suggestions)
        assert " WARNINGS" in formatted
        assert "Consider adding index" in formatted


class TestConfidenceScoring:
    """Test confidence score extraction"""
    
    def test_confidence_extraction(self):
        """Test extracting confidence from SQL comment"""
        import re
        
        sql_with_confidence = """
        SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME
        FROM EMPLOYEES
        WHERE DEPARTMENT_ID = 10
        -- CONFIDENCE: 95%
        """
        
        confidence_match = re.search(r'--\s*CONFIDENCE:\s*(\d+)%?', sql_with_confidence, re.IGNORECASE)
        assert confidence_match is not None
        assert int(confidence_match.group(1)) == 95
    
    def test_confidence_removal(self):
        """Test removing confidence comment from SQL"""
        import re
        
        sql_with_confidence = """
        SELECT * FROM EMPLOYEES
        -- CONFIDENCE: 85%
        """
        
        clean_sql = re.sub(r'--\s*CONFIDENCE:.*$', '', sql_with_confidence, flags=re.MULTILINE).strip()
        assert "CONFIDENCE" not in clean_sql
        assert "SELECT * FROM EMPLOYEES" in clean_sql


class TestOracleIdentifierNormalization:
    """Regression coverage for Oracle identifier normalization"""

    def test_lowercase_column_is_quoted(self):
        """Lowercase column names must be quoted after normalization"""
        schema_metadata = {
            "tables": {
                "CUSTOMER_DATA": [
                    {"name": "month", "type": "VARCHAR2"},
                    {"name": "DATE", "type": "DATE"},
                ]
            }
        }

        sql = "SELECT month, \"DATE\" FROM CUSTOMER_DATA"
        normalized = normalize_oracle_identifiers(sql, schema_metadata)

        assert '"month"' in normalized
        # Ensure the reserved DATE column remains quoted exactly once
        assert normalized.count('"DATE"') == 1

    def test_aliased_column_retains_alias_with_quotes(self):
        """Aliased tables should keep alias while quoting underlying columns"""
        schema_metadata = {
            "tables": {
                "CUSTOMER_DATA": [
                    {"name": "month", "type": "VARCHAR2"},
                    {"name": "VALUE_BALANCES", "type": "NUMBER"},
                ]
            }
        }

        sql = "SELECT cd.month, cd.VALUE_BALANCES FROM CUSTOMER_DATA cd"
        normalized = normalize_oracle_identifiers(sql, schema_metadata)

        assert 'cd."month"' in normalized
        # Uppercase columns that already conform should remain unquoted
        assert 'cd.VALUE_BALANCES' in normalized


if __name__ == "__main__":
    pytest.main([__file__, "-v"])