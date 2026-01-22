
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi import HTTPException
from app.core.exceptions import ValidationException

@pytest.mark.asyncio
async def test_direct_query_routing_to_postgres():
    """Verify that queries with database_type='postgres' route to pg_client."""
    
    # Mock request and user
    mock_req = MagicMock()
    mock_req.client.host = "127.0.0.1"
    mock_user = {"username": "tester", "role": MagicMock(value="analyst")}
    
    # Mock dependencies
    mock_request = MagicMock()
    mock_request.query = "SELECT 1"
    mock_request.database_type = "postgres"
    mock_request.connection_name = None
    
    mock_pg_client = AsyncMock()
    mock_pg_client.execute_query.return_value = {"status": "success", "results": {"rows": []}}
    
    with patch("app.api.v1.endpoints.queries.direct.registry") as mock_registry, \
         patch("app.api.v1.endpoints.queries.direct.validate_sql") as mock_validate, \
         patch("app.api.v1.endpoints.queries.direct.apply_rate_limit", AsyncMock()), \
         patch("app.api.v1.endpoints.queries.direct.create_trace", MagicMock(return_value="trace_id")), \
         patch("app.api.v1.endpoints.queries.direct.update_trace", MagicMock()):
        
        mock_registry.get_postgres_client.return_value = mock_pg_client
        mock_validate.return_value = MagicMock(
            is_valid=True, 
            query_type=MagicMock(value="SELECT"),
            risk_score=0,
            requires_approval=False,
            analysis=MagicMock(risk_level="low")
        )
        
        from app.api.v1.endpoints.queries.direct import submit_query
        
        response = await submit_query(mock_req, mock_request, mock_user)
        
        assert response.status == "success"
        mock_pg_client.execute_query.assert_called_once()
        # Verify it wasn't routed to QueryService.execute_sql_query (oracle)
        mock_registry.get_sqlcl_pool.assert_not_called()

@pytest.mark.asyncio
async def test_superset_dashboard_generation_with_dataset():
    """Verify that dashboard generation creates a virtual dataset."""
    
    mock_client = AsyncMock()
    mock_client.create_dataset.return_value = {"status": "success", "dataset_id": 123}
    
    query_result = {
        "status": "success",
        "data": [{"id": 1, "name": "test"}],
        "columns": ["id", "name"],
        "sql": "SELECT id, name FROM table"
    }
    
    from app.services.superset_dashboard_service import SupersetDashboardService
    
    result = await SupersetDashboardService.generate_dashboard_from_query(
        superset_client=mock_client,
        query_result=query_result,
        dashboard_title="My Test Dashboard",
        user_id="tester"
    )
    
    assert result["status"] == "success"
    assert "Dataset 'dataset_my_test_dashboard'" in result["message"]
    assert result["dataset_id"] == 123
    
    # Verify create_dataset was called with SQL
    mock_client.create_dataset.assert_called_once()
    args, kwargs = mock_client.create_dataset.call_args
    assert kwargs["sql"] == "SELECT id, name FROM table"
    assert kwargs["table_name"] == "dataset_my_test_dashboard"

if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__]))
