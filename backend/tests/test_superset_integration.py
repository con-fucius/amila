"""
Apache Superset Integration Tests
Tests for Superset API client and dashboard services
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.superset_service import SupersetClient, create_superset_client
from app.services.superset_dashboard_service import SupersetDashboardService
from app.core.exceptions import ExternalServiceException


@pytest.fixture
def mock_settings():
    """Mock settings for Superset"""
    with patch("app.services.superset_service.settings") as mock:
        mock.SUPERSET_BASE_URL = "http://superset:8088"
        mock.SUPERSET_USERNAME = "admin"
        mock.SUPERSET_PASSWORD = "admin"
        yield mock


@pytest.fixture
def superset_client(mock_settings):
    """Create Superset client for testing"""
    return SupersetClient(
        base_url="http://superset:8088",
        username="admin",
        password="admin"
    )


@pytest.mark.asyncio
class TestSupersetClient:
    """Test Superset client functionality"""
    
    @patch("httpx.AsyncClient")
    async def test_authenticate_success(self, mock_client_class, superset_client):
        """Test successful authentication"""
        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {"access_token": "test_token"}
        mock_login_response.raise_for_status = MagicMock()
        
        mock_csrf_response = MagicMock()
        mock_csrf_response.json.return_value = {"result": "csrf_token_123"}
        mock_csrf_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_login_response
        mock_client.get.return_value = mock_csrf_response
        mock_client_class.return_value = mock_client
        
        superset_client._client = mock_client
        
        await superset_client._authenticate()
        
        assert superset_client._access_token == "test_token"
        assert superset_client._csrf_token == "csrf_token_123"
    
    async def test_get_headers(self, superset_client):
        """Test header generation"""
        superset_client._access_token = "test_token"
        superset_client._csrf_token = "csrf_token"
        
        headers = superset_client._get_headers()
        
        assert headers["Authorization"] == "Bearer test_token"
        assert headers["X-CSRFToken"] == "csrf_token"
        assert headers["Content-Type"] == "application/json"
    
    @patch("httpx.AsyncClient")
    async def test_list_dashboards_success(self, mock_client_class, superset_client):
        """Test successful dashboard listing"""
        superset_client._access_token = "test_token"
        superset_client._csrf_token = "csrf_token"
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": [
                {"id": 1, "dashboard_title": "Sales Analytics"},
                {"id": 2, "dashboard_title": "Finance Report"}
            ]
        }
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        superset_client._client = mock_client
        
        result = await superset_client.list_dashboards(user_id="test_user")
        
        assert result["status"] == "success"
        assert result["count"] == 2
        assert len(result["dashboards"]) == 2
    
    @patch("httpx.AsyncClient")
    async def test_get_dashboard_success(self, mock_client_class, superset_client):
        """Test successful dashboard retrieval"""
        superset_client._access_token = "test_token"
        superset_client._csrf_token = "csrf_token"
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "id": 1,
                "dashboard_title": "Sales Analytics",
                "slug": "sales-analytics"
            }
        }
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        superset_client._client = mock_client
        
        result = await superset_client.get_dashboard(dashboard_id=1, user_id="test_user")
        
        assert result["status"] == "success"
        assert result["dashboard"]["id"] == 1
        assert result["dashboard"]["dashboard_title"] == "Sales Analytics"
    
    @patch("httpx.AsyncClient")
    async def test_create_chart_success(self, mock_client_class, superset_client):
        """Test successful chart creation"""
        superset_client._access_token = "test_token"
        superset_client._csrf_token = "csrf_token"
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "id": 10,
                "slice_name": "Revenue Trend"
            }
        }
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        superset_client._client = mock_client
        
        chart_config = {
            "slice_name": "Revenue Trend",
            "viz_type": "line"
        }
        
        result = await superset_client.create_chart(
            chart_config=chart_config,
            user_id="test_user"
        )
        
        assert result["status"] == "success"
        assert result["chart"]["id"] == 10
    
    @patch("httpx.AsyncClient")
    async def test_health_check_success(self, mock_client_class, superset_client):
        """Test successful health check"""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        superset_client._client = mock_client
        
        result = await superset_client.health_check()
        
        assert result["status"] == "healthy"
    
    @patch("httpx.AsyncClient")
    async def test_health_check_failure(self, mock_client_class, superset_client):
        """Test failed health check"""
        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("Connection refused")
        mock_client_class.return_value = mock_client
        
        superset_client._client = mock_client
        
        result = await superset_client.health_check()
        
        assert result["status"] == "unhealthy"
        assert "error" in result


@pytest.mark.asyncio
class TestSupersetDashboardService:
    """Test Superset dashboard service"""
    
    def test_analyze_data_numeric_columns(self):
        """Test data analysis with numeric columns"""
        data = [
            {"revenue": 1000, "orders": 50},
            {"revenue": 1500, "orders": 60}
        ]
        columns = ["revenue", "orders"]
        
        result = SupersetDashboardService.analyze_data_for_visualization(data, columns)
        
        assert result["chart_type"] == "scatter"
        assert result["x_axis"] == "revenue"
        assert result["y_axis"] == "orders"
    
    def test_analyze_data_mixed_columns(self):
        """Test data analysis with mixed columns"""
        data = [
            {"month": "January", "revenue": 1000},
            {"month": "February", "revenue": 1500}
        ]
        columns = ["month", "revenue"]
        
        result = SupersetDashboardService.analyze_data_for_visualization(data, columns)
        
        assert result["chart_type"] == "bar"
        assert result["x_axis"] == "month"
        assert result["y_axis"] == "revenue"
    
    def test_analyze_data_temporal_columns(self):
        """Test data analysis with temporal columns"""
        data = [
            {"date": "2026-01-01", "revenue": 1000},
            {"date": "2026-01-02", "revenue": 1500}
        ]
        columns = ["date", "revenue"]
        
        result = SupersetDashboardService.analyze_data_for_visualization(data, columns)
        
        assert result["chart_type"] == "line"
        assert result["x_axis"] == "date"
        assert result["y_axis"] == "revenue"
    
    def test_analyze_data_no_data(self):
        """Test data analysis with no data"""
        result = SupersetDashboardService.analyze_data_for_visualization([], [])
        
        assert result["chart_type"] == "table"
        assert "No data" in result["reason"]
    
    @patch("app.services.superset_service.SupersetClient")
    async def test_generate_dashboard_success(self, mock_client_class):
        """Test successful dashboard generation"""
        mock_client = AsyncMock()
        
        query_result = {
            "data": [
                {"month": "January", "revenue": 1000},
                {"month": "February", "revenue": 1500}
            ],
            "columns": ["month", "revenue"]
        }
        
        service = SupersetDashboardService()
        result = await service.generate_dashboard_from_query(
            superset_client=mock_client,
            query_result=query_result,
            dashboard_title="Test Dashboard",
            user_id="test_user"
        )
        
        assert result["status"] == "success"
        assert "visualization_recommendation" in result
        assert result["visualization_recommendation"]["chart_type"] == "bar"


@pytest.mark.asyncio
class TestSupersetFactory:
    """Test Superset client factory"""
    
    @patch("app.services.superset_service.settings")
    def test_create_superset_client_success(self, mock_settings):
        """Test successful client creation"""
        mock_settings.SUPERSET_BASE_URL = "http://superset:8088"
        mock_settings.SUPERSET_USERNAME = "admin"
        mock_settings.SUPERSET_PASSWORD = "admin"
        
        client = create_superset_client()
        
        assert client.base_url == "http://superset:8088"
        assert client.username == "admin"
        assert client.password == "admin"
    
    @patch("app.services.superset_service.settings")
    def test_create_superset_client_no_url(self, mock_settings):
        """Test client creation without URL"""
        mock_settings.SUPERSET_BASE_URL = None
        
        with pytest.raises(ValueError) as exc_info:
            create_superset_client()
        
        assert "not configured" in str(exc_info.value)
