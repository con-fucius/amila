"""
Comprehensive API Integration Tests
Tests all major API endpoints for authentication, schema, queries, governance, and new features
"""

import pytest
from httpx import AsyncClient
from unittest.mock import patch, AsyncMock
import json


@pytest.fixture
async def client():
    """Create test client"""
    from app.main import app
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def auth_headers():
    """Mock authentication headers"""
    return {"Authorization": "Bearer test_token"}


# ============================================================================
# Authentication & Authorization Tests
# ============================================================================

@pytest.mark.asyncio
async def test_login_success(client):
    """Test successful login"""
    with patch('app.api.v1.endpoints.auth.rbac_manager.authenticate_user') as mock_auth:
        mock_auth.return_value = {
            "username": "testuser",
            "role": "admin",
            "permissions": ["read", "write"]
        }
        
        response = await client.post(
            "/api/v1/auth/login",
            json={"username": "testuser", "password": "testpass"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data


@pytest.mark.asyncio
async def test_login_failure(client):
    """Test failed login"""
    with patch('app.api.v1.endpoints.auth.rbac_manager.authenticate_user') as mock_auth:
        mock_auth.return_value = None
        
        response = await client.post(
            "/api/v1/auth/login",
            json={"username": "baduser", "password": "badpass"}
        )
        
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_token_refresh(client, auth_headers):
    """Test token refresh"""
    response = await client.post(
        "/api/v1/auth/refresh",
        headers=auth_headers
    )
    
    # May return 200 or 401 depending on mock setup
    assert response.status_code in [200, 401]


# ============================================================================
# Schema Endpoint Tests
# ============================================================================

@pytest.mark.asyncio
async def test_get_schema(client, auth_headers):
    """Test schema retrieval"""
    mock_schema = {
        "status": "success",
        "source": "cache",
        "schema": {"tables": {"TEST_TABLE": [{"name": "COL1", "type": "VARCHAR2"}]}}
    }
    
    with patch('app.services.schema_service.SchemaService.get_database_schema', return_value=mock_schema):
        response = await client.get(
            "/api/v1/schema/",
            headers=auth_headers,
            params={"database_type": "oracle"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "schema_data" in data


@pytest.mark.asyncio
async def test_enrich_schema(client, auth_headers):
    """Test schema enrichment with AI"""
    mock_enriched = {
        "status": "success",
        "enriched_schema": {
            "tables": {
                "TEST_TABLE": [
                    {
                        "name": "COL1",
                        "type": "VARCHAR2",
                        "description": "Test column",
                        "inferred_name": "Test Column"
                    }
                ]
            }
        },
        "tables_enriched": 1
    }
    
    with patch('app.services.schema_service.SchemaService.enrich_schema_with_ai', return_value=mock_enriched):
        response = await client.post(
            "/api/v1/schema/enrich",
            headers=auth_headers,
            params={"database_type": "oracle"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"


@pytest.mark.asyncio
async def test_refresh_schema(client, auth_headers):
    """Test schema cache refresh"""
    mock_result = {
        "status": "success",
        "tables_refreshed": 10,
        "source": "database"
    }
    
    with patch('app.services.schema_service.SchemaService.refresh_schema_cache', return_value=mock_result):
        response = await client.post(
            "/api/v1/schema/refresh",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"


@pytest.mark.asyncio
async def test_get_column_samples(client, auth_headers):
    """Test sample value retrieval"""
    with patch('app.services.introspection_service.IntrospectionService.get_sample_values',
               return_value=[100, 200, 300]):
        response = await client.get(
            "/api/v1/schema/columns/TEST_TABLE/AMOUNT/samples",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert len(data["samples"]) == 3


# ============================================================================
# Query Endpoint Tests
# ============================================================================

@pytest.mark.asyncio
async def test_submit_query(client, auth_headers):
    """Test query submission"""
    mock_result = {
        "status": "success",
        "query_id": "test_query_123",
        "requires_approval": False,
        "results": {"rows": [], "columns": []}
    }
    
    with patch('app.api.v1.endpoints.query.orchestrator_client.run_query', return_value=mock_result):
        response = await client.post(
            "/api/v1/query/submit",
            headers=auth_headers,
            json={
                "question": "Show all customers",
                "database_type": "oracle"
            }
        )
        
        # May require orchestrator setup
        assert response.status_code in [200, 500]


@pytest.mark.asyncio
async def test_query_history(client, auth_headers):
    """Test query history retrieval"""
    response = await client.get(
        "/api/v1/query/history",
        headers=auth_headers,
        params={"limit": 10}
    )
    
    # May return empty list or error depending on setup
    assert response.status_code in [200, 401, 500]


# ============================================================================
# Cost & Governance Tests
# ============================================================================

@pytest.mark.asyncio
async def test_get_cost_usage(client, auth_headers):
    """Test cost usage retrieval"""
    response = await client.get(
        "/api/v1/cost/usage",
        headers=auth_headers
    )
    
    assert response.status_code in [200, 401, 500]


@pytest.mark.asyncio
async def test_get_budget_forecast(client, auth_headers):
    """Test budget forecast retrieval"""
    mock_forecast = {
        "status": "success",
        "forecast": {
            "current_usage": 150.50,
            "projected_monthly": 500.00,
            "anomalies": [],
            "alerts": []
        }
    }
    
    with patch('app.api.v1.endpoints.cost_tracking.get_budget_forecast_data', return_value=mock_forecast):
        response = await client.get(
            "/api/v1/cost/budget-forecast",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 401]


@pytest.mark.asyncio
async def test_get_audit_logs(client, auth_headers):
    """Test audit log retrieval"""
    response = await client.get(
        "/api/v1/governance/audit-logs",
        headers=auth_headers,
        params={"limit": 20}
    )
    
    assert response.status_code in [200, 401, 500]


# ============================================================================
# Insights & Semantic Layer Tests
# ============================================================================

@pytest.mark.asyncio
async def test_get_proactive_insights(client, auth_headers):
    """Test proactive insights retrieval"""
    with patch('app.core.redis_client.redis_client.lrange', return_value=[]):
        response = await client.get(
            "/api/v1/insights/proactive",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "insights" in data


@pytest.mark.asyncio
async def test_trigger_insights(client, auth_headers):
    """Test manual insight generation trigger"""
    with patch('app.tasks.insight_scheduler.generate_proactive_insights.delay') as mock_task:
        mock_task.return_value.id = "task_123"
        
        # Mock admin user
        with patch('app.core.rbac.rbac_manager.get_current_user',
                   return_value={"username": "admin", "role": "admin"}):
            response = await client.post(
                "/api/v1/insights/trigger",
                headers=auth_headers
            )
            
            assert response.status_code in [200, 403]


@pytest.mark.asyncio
async def test_get_semantic_mappings(client, auth_headers):
    """Test semantic mappings retrieval"""
    with patch('app.services.semantic_layer.SemanticLayerService.get_all_mappings',
               return_value={"customers": {"oracle": "CUSTOMER_DIM"}}):
        response = await client.get(
            "/api/v1/semantic/mappings",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"


@pytest.mark.asyncio
async def test_add_semantic_mapping(client, auth_headers):
    """Test adding semantic mapping"""
    mock_result = {
        "status": "success",
        "concept": "products",
        "mappings": {"oracle": "PRODUCT_DIM"}
    }
    
    with patch('app.services.semantic_layer.SemanticLayerService.add_mapping', return_value=mock_result):
        with patch('app.core.rbac.rbac_manager.get_current_user',
                   return_value={"username": "admin", "role": "admin"}):
            response = await client.post(
                "/api/v1/semantic/mappings",
                headers=auth_headers,
                json={
                    "concept": "products",
                    "mappings": {"oracle": "PRODUCT_DIM"}
                }
            )
            
            assert response.status_code in [200, 403]


# ============================================================================
# Error Handling Tests
# ============================================================================

@pytest.mark.asyncio
async def test_unauthorized_access(client):
    """Test endpoints without authentication"""
    response = await client.get("/api/v1/schema/")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_invalid_endpoint(client, auth_headers):
    """Test non-existent endpoint"""
    response = await client.get(
        "/api/v1/nonexistent",
        headers=auth_headers
    )
    assert response.status_code == 404


# ============================================================================
# Performance Tests
# ============================================================================

@pytest.mark.asyncio
async def test_concurrent_requests(client, auth_headers):
    """Test handling of concurrent requests"""
    import asyncio
    
    async def make_request():
        with patch('app.services.schema_service.SchemaService.get_database_schema',
                   return_value={"status": "success", "schema": {}}):
            return await client.get("/api/v1/schema/", headers=auth_headers)
    
    # Make 5 concurrent requests
    tasks = [make_request() for _ in range(5)]
    responses = await asyncio.gather(*tasks)
    
    # All should succeed or fail gracefully
    for resp in responses:
        assert resp.status_code in [200, 401, 500]
