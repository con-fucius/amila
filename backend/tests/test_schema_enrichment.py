"""
Tests for Schema Enrichment
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from app.services.schema_service import SchemaService


@pytest.mark.asyncio
async def test_enrich_schema_with_ai():
    """Test AI-powered schema enrichment"""
    
    # Mock schema data
    mock_schema = {
        "status": "success",
        "schema": {
            "tables": {
                "CUSTOMER_DATA": [
                    {"name": "CUST_ID", "type": "NUMBER", "nullable": False},
                    {"name": "COL01", "type": "VARCHAR2", "nullable": True}
                ]
            }
        }
    }
    
    # Mock LLM response
    mock_llm = Mock()
    mock_response = Mock()
    mock_response.content = """DESCRIPTION: Customer unique identifier
INFERRED_NAME: Customer ID
BUSINESS_MEANING: Primary key for customer records"""
    
    mock_llm.ainvoke = AsyncMock(return_value=mock_response)
    
    with patch.object(SchemaService, 'get_database_schema', return_value=mock_schema):
        with patch('app.core.client_registry.registry.get_llm', return_value=mock_llm):
            result = await SchemaService.enrich_schema_with_ai(database_type="oracle")
            
            assert result["status"] == "success"
            assert "enriched_schema" in result
            assert "CUSTOMER_DATA" in result["enriched_schema"]["tables"]
            
            # Check enriched columns
            columns = result["enriched_schema"]["tables"]["CUSTOMER_DATA"]
            assert len(columns) > 0
            assert all("description" in col for col in columns)
            assert all("inferred_name" in col for col in columns)


@pytest.mark.asyncio
async def test_infer_column_meaning_rule_based():
    """Test rule-based column inference fallback"""
    
    result = SchemaService._rule_based_inference(
        table_name="ORDERS",
        column_name="AMT_01",
        data_type="NUMBER(10,2)",
        samples=[100.50, 250.00, 75.25]
    )
    
    assert result["inferred_name"] != "AMT_01"
    assert "Amount" in result["inferred_name"]
    assert result["confidence"] == "medium"
    assert "numeric" in result["business_meaning"]


@pytest.mark.asyncio
async def test_enrich_schema_no_tables():
    """Test enrichment with no tables"""
    
    mock_schema = {
        "status": "success",
        "schema": {"tables": {}}
    }
    
    with patch.object(SchemaService, 'get_database_schema', return_value=mock_schema):
        result = await SchemaService.enrich_schema_with_ai(database_type="oracle")
        
        assert result["status"] == "error"
        assert "No tables found" in result["error"]


def test_rule_based_patterns():
    """Test various rule-based pattern matches"""
    
    test_cases = [
        ("CUST_ID", "Identifier"),
        ("ORDER_DT", "Date"),
        ("AMT_TOTAL", "Amount"),
        ("STATUS_CD", "Code"),
        ("CUSTOMER_NM", "Name"),
        ("COL01", "Column"),
    ]
    
    for col_name, expected_type in test_cases:
        result = SchemaService._rule_based_inference(
            table_name="TEST_TABLE",
            column_name=col_name,
            data_type="VARCHAR2",
            samples=[]
        )
        
        assert expected_type in result["inferred_name"]
