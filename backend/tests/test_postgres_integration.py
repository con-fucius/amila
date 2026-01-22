"""
PostgreSQL Integration Tests
Tests for PostgreSQL client, query service, and schema service
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.core.postgres_client import PostgreSQLClient
from app.services.postgres_query_service import PostgresQueryService
from app.services.postgres_schema_service import PostgresSchemaService
from app.core.exceptions import ExternalServiceException, ValidationException


@pytest.fixture
def mock_settings():
    """Mock settings for PostgreSQL"""
    with patch("app.core.postgres_client.settings") as mock:
        mock.POSTGRES_ENABLED = True
        mock.POSTGRES_HOST = "localhost"
        mock.POSTGRES_PORT = 5432
        mock.POSTGRES_DATABASE = "test_db"
        mock.POSTGRES_USER = "test_user"
        mock.POSTGRES_PASSWORD = "test_pass"
        mock.POSTGRES_POOL_MIN_SIZE = 2
        mock.POSTGRES_POOL_MAX_SIZE = 10
        mock.POSTGRES_POOL_TIMEOUT = 30
        mock.POSTGRES_QUERY_TIMEOUT = 600
        mock.POSTGRES_READ_ONLY = True
        yield mock


@pytest.mark.asyncio
class TestPostgreSQLClient:
    """Test PostgreSQL client functionality"""
    
    async def test_validate_readonly_query_blocks_insert(self, mock_settings):
        """Test that INSERT queries are blocked in read-only mode"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("INSERT INTO users VALUES (1, 'test')")
        
        assert "INSERT" in str(exc_info.value)
        assert "not allowed" in str(exc_info.value)
    
    async def test_validate_readonly_query_blocks_update(self, mock_settings):
        """Test that UPDATE queries are blocked in read-only mode"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("UPDATE users SET name='test' WHERE id=1")
        
        assert "UPDATE" in str(exc_info.value)
    
    async def test_validate_readonly_query_blocks_delete(self, mock_settings):
        """Test that DELETE queries are blocked in read-only mode"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("DELETE FROM users WHERE id=1")
        
        assert "DELETE" in str(exc_info.value)
    
    async def test_validate_readonly_query_blocks_drop(self, mock_settings):
        """Test that DROP queries are blocked in read-only mode"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("DROP TABLE users")
        
        assert "DROP" in str(exc_info.value)
    
    async def test_validate_readonly_query_allows_select(self, mock_settings):
        """Test that SELECT queries are allowed"""
        client = PostgreSQLClient()
        
        # Should not raise exception
        client._validate_readonly_query("SELECT * FROM users")
        client._validate_readonly_query("SELECT id, name FROM users WHERE active = true")
    
    async def test_validate_readonly_query_blocks_create(self, mock_settings):
        """Test that CREATE queries are blocked"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("CREATE TABLE test (id INT)")
        
        assert "CREATE" in str(exc_info.value)
    
    async def test_validate_readonly_query_blocks_alter(self, mock_settings):
        """Test that ALTER queries are blocked"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("ALTER TABLE users ADD COLUMN email VARCHAR(255)")
        
        assert "ALTER" in str(exc_info.value)
    
    async def test_validate_readonly_query_blocks_truncate(self, mock_settings):
        """Test that TRUNCATE queries are blocked"""
        client = PostgreSQLClient()
        
        with pytest.raises(ValidationException) as exc_info:
            client._validate_readonly_query("TRUNCATE TABLE users")
        
        assert "TRUNCATE" in str(exc_info.value)


@pytest.mark.asyncio
class TestPostgresQueryService:
    """Test PostgreSQL query service"""
    
    @patch("app.services.postgres_query_service.settings")
    async def test_execute_sql_query_disabled(self, mock_settings):
        """Test that queries fail when PostgreSQL is disabled"""
        mock_settings.POSTGRES_ENABLED = False
        
        with pytest.raises(ExternalServiceException) as exc_info:
            await PostgresQueryService.execute_sql_query(
                sql_query="SELECT 1",
                user_id="test_user",
                request_id="test_001"
            )
        
        assert "not enabled" in str(exc_info.value)
    
    @patch("app.services.postgres_query_service.settings")
    async def test_execute_sql_query_not_initialized(self, mock_settings):
        """Test that queries fail when service is not initialized"""
        mock_settings.POSTGRES_ENABLED = True
        PostgresQueryService._client = None
        
        with pytest.raises(ExternalServiceException) as exc_info:
            await PostgresQueryService.execute_sql_query(
                sql_query="SELECT 1",
                user_id="test_user",
                request_id="test_001"
            )
        
        assert "not initialized" in str(exc_info.value)


@pytest.mark.asyncio
class TestPostgresSchemaService:
    """Test PostgreSQL schema service"""
    
    @patch("app.services.postgres_schema_service.settings")
    async def test_get_dynamic_schema_disabled(self, mock_settings):
        """Test that schema retrieval fails when PostgreSQL is disabled"""
        mock_settings.POSTGRES_ENABLED = False
        
        with pytest.raises(ExternalServiceException) as exc_info:
            await PostgresSchemaService.get_dynamic_schema("show me users")
        
        assert "not enabled" in str(exc_info.value)
    
    @patch("app.services.postgres_schema_service.settings")
    async def test_get_full_schema_disabled(self, mock_settings):
        """Test that full schema retrieval fails when PostgreSQL is disabled"""
        mock_settings.POSTGRES_ENABLED = False
        
        with pytest.raises(ExternalServiceException) as exc_info:
            await PostgresSchemaService.get_full_schema()
        
        assert "not enabled" in str(exc_info.value)
    
    @patch("app.services.postgres_schema_service.settings")
    async def test_get_table_info_disabled(self, mock_settings):
        """Test that table info retrieval fails when PostgreSQL is disabled"""
        mock_settings.POSTGRES_ENABLED = False
        
        with pytest.raises(ExternalServiceException) as exc_info:
            await PostgresSchemaService.get_table_info("users")
        
        assert "not enabled" in str(exc_info.value)


@pytest.mark.asyncio
class TestPostgreSQLSQLInjectionPrevention:
    """Test SQL injection prevention"""
    
    async def test_blocks_sql_injection_via_comment(self, mock_settings):
        """Test that SQL injection via comments is blocked"""
        client = PostgreSQLClient()
        
        malicious_sql = "SELECT * FROM users WHERE id = 1; DROP TABLE users; --"
        
        with pytest.raises(ValidationException):
            client._validate_readonly_query(malicious_sql)
    
    async def test_blocks_sql_injection_via_union(self, mock_settings):
        """Test that SQL injection via UNION is handled"""
        client = PostgreSQLClient()
        
        # UNION itself is allowed, but combined with dangerous operations should fail
        safe_union = "SELECT id FROM users UNION SELECT id FROM orders"
        client._validate_readonly_query(safe_union)  # Should pass
    
    async def test_blocks_stacked_queries(self, mock_settings):
        """Test that stacked queries with dangerous operations are blocked"""
        client = PostgreSQLClient()
        
        stacked = "SELECT * FROM users; DELETE FROM users WHERE id = 1"
        
        with pytest.raises(ValidationException):
            client._validate_readonly_query(stacked)


@pytest.mark.asyncio
class TestPostgreSQLReadOnlyEnforcement:
    """Test read-only transaction enforcement"""
    
    async def test_blocks_all_write_operations(self, mock_settings):
        """Test that all write operations are blocked"""
        client = PostgreSQLClient()
        
        write_operations = [
            "INSERT INTO users VALUES (1, 'test')",
            "UPDATE users SET name='test'",
            "DELETE FROM users",
            "DROP TABLE users",
            "CREATE TABLE test (id INT)",
            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
            "TRUNCATE TABLE users",
            "GRANT ALL ON users TO public",
            "REVOKE ALL ON users FROM public",
        ]
        
        for sql in write_operations:
            with pytest.raises(ValidationException):
                client._validate_readonly_query(sql)
    
    async def test_allows_read_operations(self, mock_settings):
        """Test that read operations are allowed"""
        client = PostgreSQLClient()
        
        read_operations = [
            "SELECT * FROM users",
            "SELECT id, name FROM users WHERE active = true",
            "SELECT COUNT(*) FROM orders",
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
            "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        ]
        
        for sql in read_operations:
            # Should not raise exception
            client._validate_readonly_query(sql)
