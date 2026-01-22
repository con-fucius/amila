
import pytest
from unittest.mock import MagicMock, patch
from app.core.exceptions import ValidationException, ExternalServiceException
from app.core.postgres_client import PostgreSQLClient

# Mock psycopg and sqlglot availability
@patch("app.core.postgres_client.PSYCOPG_AVAILABLE", True)
@patch("app.core.postgres_client.SQLGLOT_AVAILABLE", True)
class TestPostgresSecurity:
    
    @pytest.fixture
    def client(self):
        client = PostgreSQLClient()
        # Mock pool and init
        client._pool = MagicMock()
        client._initialized = True
        return client

    def test_allowed_queries(self, client):
        """Test legitimate read-only queries are allowed"""
        queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM users WHERE id = 1",
            "EXPLAIN SELECT * FROM orders",
            "SHOW search_path",
            "SELECT count(*) FROM (SELECT * FROM users) as sub",
            "SELECT 1",
            # Simple CTE
            "WITH regional_sales AS (SELECT region, SUM(amount) AS total_sales FROM orders GROUP BY region) SELECT region, total_sales FROM regional_sales WHERE total_sales > (SELECT SUM(total_sales)/10 FROM regional_sales)"
        ]
        for sql in queries:
            try:
                client._validate_readonly_query(sql)
            except ValidationException as e:
                pytest.fail(f"Allowed query blocked: {sql} - Error: {e}")

    def test_blocked_write_queries(self, client):
        """Test write operations are blocked via AST"""
        queries = [
            "INSERT INTO users (name) VALUES ('hacker')",
            "UPDATE users SET admin = true",
            "DELETE FROM users",
            "DROP TABLE users",
            "TRUNCATE users",
            "CREATE TABLE evil (id int)",
            "ALTER TABLE users DROP COLUMN password",
            "GRANT ALL ON users TO public",
            "REVOKE ALL ON users FROM public",
            "COMMIT",
            "ROLLBACK"
        ]
        for sql in queries:
            with pytest.raises(ValidationException, match="Statement type.*not allowed"):
                client._validate_readonly_query(sql)

    def test_blocked_select_into(self, client):
        """Test SELECT INTO (table creation) is blocked"""
        sql = "SELECT * INTO new_table FROM users"
        with pytest.raises(ValidationException, match="SELECT INTO.*not allowed"):
            client._validate_readonly_query(sql)

    def test_multiple_statements_prevention(self, client):
        """Test chaining allowed and disallowed queries"""
        queries = [
            "SELECT 1; DROP TABLE users",
            "SELECT 1; INSERT INTO log VALUES(1)",
            "DROP TABLE users; SELECT 1"
        ]
        for sql in queries:
            with pytest.raises(ValidationException, match="Statement type.*not allowed"):
                client._validate_readonly_query(sql)

    def test_obfuscation_attempts(self, client):
        """Test obfuscated queries that regex might miss but AST catches"""
        # Comments hiding keywords
        sql = "INSERT/**/INTO/**/users/**/VALUES(1)"
        with pytest.raises(ValidationException, match="Statement type.*not allowed"):
            client._validate_readonly_query(sql)
        
        # Case variation
        sql = "dRoP tAbLe users"
        with pytest.raises(ValidationException, match="Statement type.*not allowed"):
            client._validate_readonly_query(sql)
