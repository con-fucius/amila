"""Doris integration contract tests.

These tests verify that, for Doris paths, the API responses respect the
frontend/backend contracts:

- /api/v1/queries/process with database_type="doris" returns results.columns
  as string[] when successful.
- /api/v1/queries/submit with database_type="doris" returns results.columns
  as string[] when successful and surfaces Doris-specific errors via the
  HTTP 400 detail field.
- /api/v1/schema?database_type=doris returns a schema shape compatible with
  the SchemaBrowser page when DorisSchemaService succeeds.

Heavy dependencies (actual Doris MCP server, Doris DB) are not required;
we stub Doris services to focus purely on contracts.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.core.application import create_application


@pytest_asyncio.fixture
async def app_instance(monkeypatch):
    """Create a FastAPI app instance with lightweight stubs.

    We stub Redis health so /health does not require a live Redis server.
    """

    import app.core.redis_client as redis_mod

    async def fake_health_check():  # pragma: no cover - trivial stub
        return {"status": "healthy"}

    monkeypatch.setattr(redis_mod.redis_client, "health_check", fake_health_check)

    app = create_application()
    return app


@pytest_asyncio.fixture
async def async_client(app_instance):
    """Yield an AsyncClient bound directly to the FastAPI ASGI app."""

    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_process_endpoint_doris_contract(async_client: AsyncClient, monkeypatch):
    """Orchestrator /process with database_type=doris returns columns: string[]."""

    from app.api.v1.endpoints import queries as queries_mod

    async def fake_submit_nl(
        user_query: str,
        user_id: str = "default_user",
        session_id: str | None = None,
        user_role: str = "analyst",
        timeout: float = 600.0,
        thread_id_override: str | None = None,
        database_type: str = "oracle",
    ):  # type: ignore[override]
        # Ensure the endpoint passes through database_type correctly for Doris
        assert database_type == "doris"
        return {
            "query_id": "doris_q_test_1",
            "status": "success",
            "sql_query": "SELECT * FROM CUSTOMER_DATA LIMIT 10",
            "results": {
                "columns": ["ID", "NAME"],
                "rows": [[1, "Alice"]],
                "row_count": 1,
                "execution_time_ms": 5,
            },
            "database_type": "doris",
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.queries.QueryService.submit_natural_language_query",
        fake_submit_nl,
    )

    payload = {
        "query": "Show customers from Doris.",
        "user_id": "test_user",
        "session_id": "session_doris_1",
        "database_type": "doris",
    }

    resp = await async_client.post("/api/v1/queries/process", json=payload)
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "success"
    assert data["query_id"] == "doris_q_test_1"
    assert isinstance(data.get("results"), dict)
    cols = data["results"].get("columns")
    assert isinstance(cols, list)
    assert all(isinstance(c, str) for c in cols)


@pytest.mark.asyncio
async def test_submit_sql_doris_success_contract(async_client: AsyncClient, monkeypatch):
    """/queries/submit with database_type=doris returns columns: string[]."""

    async def fake_doris_execute_sql_query(
        sql_query: str,
        timeout: float = 600.0,
        user_id: str | None = None,
        request_id: str | None = None,
    ):  # type: ignore[override]
        return {
            "status": "success",
            "sql": sql_query,
            "query_id": request_id or "doris_direct_test",
            "results": {
                "columns": ["ID", "NAME"],
                "rows": [[1, "Alice"]],
                "row_count": 1,
                "execution_time_ms": 7,
            },
            "database_type": "doris",
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.queries.DorisQueryService.execute_sql_query",
        fake_doris_execute_sql_query,
    )

    payload = {
        "query": "SELECT * FROM CUSTOMER_DATA LIMIT 10",
        "connection_name": None,
        "database_type": "doris",
    }

    headers = {"Authorization": "Bearer temp-dev-token"}
    resp = await async_client.post("/api/v1/queries/submit", json=payload, headers=headers)
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "success"
    assert isinstance(data.get("results"), dict)
    cols = data["results"].get("columns")
    assert isinstance(cols, list)
    assert all(isinstance(c, str) for c in cols)


@pytest.mark.asyncio
async def test_submit_sql_doris_error_surfaces_message(async_client: AsyncClient, monkeypatch):
    """Doris failures should surface a clear error message via HTTP 400 detail.

    This ensures that the /queries/submit endpoint prefers the service-layer
    ``error`` field over a generic message when routing Doris traffic.
    """

    async def fake_doris_execute_sql_query_error(
        sql_query: str,
        timeout: float = 600.0,
        user_id: str | None = None,
        request_id: str | None = None,
    ):  # type: ignore[override]
        return {
            "status": "error",
            "error": "Doris MCP client is not healthy",
            "query_id": request_id or "doris_err_test",
            "database_type": "doris",
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.queries.DorisQueryService.execute_sql_query",
        fake_doris_execute_sql_query_error,
    )

    payload = {
        "query": "SELECT * FROM CUSTOMER_DATA LIMIT 10",
        "connection_name": None,
        "database_type": "doris",
    }

    headers = {"Authorization": "Bearer temp-dev-token"}
    resp = await async_client.post("/api/v1/queries/submit", json=payload, headers=headers)
    assert resp.status_code == 400

    body = resp.json()
    assert "detail" in body
    assert "Doris MCP client is not healthy" in str(body["detail"])


@pytest.mark.asyncio
async def test_schema_endpoint_doris_contract(async_client: AsyncClient, monkeypatch):
    """/schema with database_type=doris returns schema compatible with SchemaBrowser."""

    from app.api.v1.endpoints import schema as schema_mod

    async def fake_get_doris_schema():  # type: ignore[override]
        return {
            "status": "success",
            "source": "doris_mcp",
            "schema": {
                "tables": {
                    "CUSTOMER_DATA": [
                        {"name": "ID", "type": "INT", "nullable": False},
                        {"name": "NAME", "type": "STRING", "nullable": True},
                    ]
                },
                "views": {},
            },
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.schema.DorisSchemaService.get_database_schema",
        fake_get_doris_schema,
    )

    resp = await async_client.get("/api/v1/schema/?database_type=doris")
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "success"
    schema = data.get("schema_data", {})
    assert "tables" in schema
    assert "CUSTOMER_DATA" in schema["tables"]
    first_col = schema["tables"]["CUSTOMER_DATA"][0]
    assert first_col.get("name") == "ID"

