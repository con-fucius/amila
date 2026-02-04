"""
Tests for Semantic Layer
"""

import pytest
from unittest.mock import patch, AsyncMock

from app.services.semantic_layer import SemanticLayerService


@pytest.mark.asyncio
async def test_resolve_concept_to_table():
    """Test concept-to-table resolution"""
    
    # Mock Redis response
    mock_mapping = {
        "oracle": "CUSTOMER_DIM",
        "doris": "dim_customer",
        "postgres": "customers"
    }
    
    with patch('app.core.redis_client.redis_client.get', return_value=mock_mapping):
        result = await SemanticLayerService.resolve_concept_to_table("customers", "oracle")
        
        assert result == "CUSTOMER_DIM"


@pytest.mark.asyncio
async def test_resolve_concept_from_config():
    """Test fallback to config when Redis cache misses"""
    
    mock_settings = type('MockSettings', (), {
        'SEMANTIC_MAPPINGS': {
            "orders": {
                "oracle": "ORDER_FACT",
                "doris": "fact_orders"
            }
        }
    })()
    
    with patch('app.core.redis_client.redis_client.get', return_value=None):
        with patch('app.core.config.settings', mock_settings):
            result = await SemanticLayerService.resolve_concept_to_table("orders", "oracle")
            
            assert result == "ORDER_FACT"


@pytest.mark.asyncio
async def test_translate_query():
    """Test query translation with semantic references"""
    
    # Mock mappings
    mock_mappings = {
        "customers": {"oracle": "CUSTOMER_DIM", "doris": "dim_customer"},
        "orders": {"oracle": "ORDER_FACT", "doris": "fact_orders"}
    }
    
    with patch.object(SemanticLayerService, 'get_all_mappings', return_value=mock_mappings):
        query = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id"
        
        result = await SemanticLayerService.translate_query(query, "oracle")
        
        assert "CUSTOMER_DIM" in result
        assert "ORDER_FACT" in result
        assert "customers" not in result  # Original concept should be replaced


@pytest.mark.asyncio
async def test_add_mapping():
    """Test adding a semantic mapping"""
    
    with patch('app.core.redis_client.redis_client.set', return_value=AsyncMock()):
        result = await SemanticLayerService.add_mapping(
            concept="products",
            db_mappings={"oracle": "PRODUCT_DIM", "doris": "dim_product"}
        )
        
        assert result["status"] == "success"
        assert result["concept"] == "products"


@pytest.mark.asyncio
async def test_delete_mapping():
    """Test deleting a semantic mapping"""
    
    with patch('app.core.redis_client.redis_client.delete', return_value=AsyncMock()):
        result = await SemanticLayerService.delete_mapping("products")
        
        assert result["status"] == "success"
        assert result["concept"] == "products"


@pytest.mark.asyncio
async def test_resolve_concept_not_found():
    """Test resolving non-existent concept"""
    
    with patch('app.core.redis_client.redis_client.get', return_value=None):
        with patch('app.core.config.settings', type('MockSettings', (), {'SEMANTIC_MAPPINGS': {}})():
            result = await SemanticLayerService.resolve_concept_to_table("unknown", "oracle")
            
            assert result is None
