"""
Qlik Sense Integration Tests
Tests for Qlik Sense API client and endpoints
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.qlik_service import QlikSenseClient, create_qlik_client
from app.core.exceptions import ExternalServiceException


@pytest.fixture
def mock_settings():
    """Mock settings for Qlik"""
    with patch("app.services.qlik_service.settings") as mock:
        mock.QLIK_BASE_URL = "https://qlik-server:4242"
        mock.QLIK_AUTH_USER = "UserDirectory=INTERNAL;UserId=test_user"
        yield mock


@pytest.fixture
def qlik_client(mock_settings):
    """Create Qlik client for testing"""
    return QlikSenseClient(
        base_url="https://qlik-server:4242",
        auth_user="UserDirectory=INTERNAL;UserId=test_user"
    )


@pytest.mark.asyncio
class TestQlikSenseClient:
    """Test Qlik Sense client functionality"""
    
    async def test_generate_xrfkey(self, qlik_client):
        """Test Xrfkey generation"""
        xrfkey = qlik_client._generate_xrfkey()
        assert len(xrfkey) == 16
        assert isinstance(xrfkey, str)
    
    async def test_get_headers(self, qlik_client):
        """Test header generation"""
        headers = qlik_client._get_headers()
        assert "X-Qlik-Xrfkey" in headers
        assert "X-Qlik-User" in headers
        assert headers["Content-Type"] == "application/json"
        assert len(headers["X-Qlik-Xrfkey"]) == 16
    
    @patch("httpx.AsyncClient")
    async def test_list_apps_success(self, mock_client_class, qlik_client):
        """Test successful app listing"""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": "app1", "name": "Sales Dashboard"},
            {"id": "app2", "name": "Finance Report"}
        ]
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        qlik_client._client = mock_client
        
        result = await qlik_client.list_apps(user_id="test_user")
        
        assert result["status"] == "success"
        assert result["count"] == 2
        assert len(result["apps"]) == 2
    
    @patch("httpx.AsyncClient")
    async def test_list_apps_with_filter(self, mock_client_class, qlik_client):
        """Test app listing with filter"""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": "app1", "name": "Sales Dashboard"}
        ]
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        qlik_client._client = mock_client
        
        result = await qlik_client.list_apps(
            filter_query="name eq 'Sales Dashboard'",
            user_id="test_user"
        )
        
        assert result["status"] == "success"
        assert result["count"] == 1
    
    @patch("httpx.AsyncClient")
    async def test_get_app_success(self, mock_client_class, qlik_client):
        """Test successful app retrieval"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "app1",
            "name": "Sales Dashboard",
            "description": "Monthly sales analysis"
        }
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        qlik_client._client = mock_client
        
        result = await qlik_client.get_app(app_id="app1", user_id="test_user")
        
        assert result["status"] == "success"
        assert result["app"]["id"] == "app1"
        assert result["app"]["name"] == "Sales Dashboard"
    
    @patch("httpx.AsyncClient")
    async def test_list_sheets_success(self, mock_client_class, qlik_client):
        """Test successful sheet listing"""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": "sheet1", "name": "Overview"},
            {"id": "sheet2", "name": "Details"}
        ]
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        qlik_client._client = mock_client
        
        result = await qlik_client.list_sheets(app_id="app1", user_id="test_user")
        
        assert result["status"] == "success"
        assert result["count"] == 2
        assert len(result["sheets"]) == 2
    
    @patch("httpx.AsyncClient")
    async def test_health_check_success(self, mock_client_class, qlik_client):
        """Test successful health check"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "buildVersion": "May 2025",
            "productName": "Qlik Sense Enterprise"
        }
        mock_response.raise_for_status = MagicMock()
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        qlik_client._client = mock_client
        
        result = await qlik_client.health_check()
        
        assert result["status"] == "healthy"
        assert "version" in result
        assert "product" in result
    
    @patch("httpx.AsyncClient")
    async def test_health_check_failure(self, mock_client_class, qlik_client):
        """Test failed health check"""
        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("Connection refused")
        mock_client_class.return_value = mock_client
        
        qlik_client._client = mock_client
        
        result = await qlik_client.health_check()
        
        assert result["status"] == "unhealthy"
        assert "error" in result


@pytest.mark.asyncio
class TestQlikFactory:
    """Test Qlik client factory"""
    
    @patch("app.services.qlik_service.settings")
    def test_create_qlik_client_success(self, mock_settings):
        """Test successful client creation"""
        mock_settings.QLIK_BASE_URL = "https://qlik-server:4242"
        mock_settings.QLIK_AUTH_USER = "UserDirectory=INTERNAL;UserId=test"
        
        client = create_qlik_client()
        
        assert client.base_url == "https://qlik-server:4242"
        assert client.auth_user == "UserDirectory=INTERNAL;UserId=test"
    
    @patch("app.services.qlik_service.settings")
    def test_create_qlik_client_no_url(self, mock_settings):
        """Test client creation without URL"""
        mock_settings.QLIK_BASE_URL = None
        
        with pytest.raises(ValueError) as exc_info:
            create_qlik_client()
        
        assert "not configured" in str(exc_info.value)


@pytest.mark.asyncio
class TestQlikSecurity:
    """Test Qlik security features"""
    
    async def test_xrfkey_uniqueness(self, qlik_client):
        """Test that Xrfkey is unique for each request"""
        key1 = qlik_client._generate_xrfkey()
        key2 = qlik_client._generate_xrfkey()
        
        assert key1 != key2
    
    async def test_headers_include_auth(self, qlik_client):
        """Test that headers include authentication"""
        headers = qlik_client._get_headers()
        
        assert "X-Qlik-User" in headers
        assert "UserDirectory" in headers["X-Qlik-User"]
        assert "UserId" in headers["X-Qlik-User"]
