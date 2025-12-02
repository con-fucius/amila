"""Minimal API integration tests for core query flows.

These tests exercise the FastAPI routing layer in-process using httpx.AsyncClient
with an ASGITransport, while stubbing out heavy dependencies (SQLcl, MCP,
LangGraph, Redis). The goal is to verify response shapes and contracts without
requiring a running backend server or real databases.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.core.application import create_application


@pytest_asyncio.fixture
async def app_instance(monkeypatch):
    """Create a FastAPI app instance with lightweight test stubs.

    We bypass heavy external dependencies where possible to keep tests fast and
    deterministic. Lifespan is not triggered by ASGITransport, so we patch the
    Redis health check used by /health to avoid connection attempts.
    """

    # Stub Redis health check so /health does not depend on a live Redis server
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
async def test_health_endpoint_basic(async_client: AsyncClient):
    """/health should respond with a basic status and components structure."""

    resp = await async_client.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert isinstance(data, dict)
    assert "status" in data
    assert "components" in data
    assert isinstance(data["components"], dict)


@pytest.mark.asyncio
async def test_process_endpoint_success_with_stub(async_client: AsyncClient, monkeypatch):
    """/api/v1/queries/process should return orchestrator response shape.

    The orchestrator itself is exercised elsewhere; here we stub the
    QueryService.submit_natural_language_query call so the test does not depend
    on LangGraph, Graphiti, or Oracle being available.
    """

    from app.api.v1.endpoints import queries as queries_mod

    async def fake_submit_nl(user_query: str, user_id: str, session_id: str | None, user_role: str, timeout: float = 600.0, thread_id_override: str | None = None):  # type: ignore[override]
        return {
            "query_id": "q_test_1234",
            "status": "success",
            "sql_query": "SELECT 1 FROM DUAL",
            "results": {
                "columns": ["VALUE"],
                "rows": [[1]],
                "row_count": 1,
                "execution_time_ms": 5,
            },
            "validation": {"is_valid": True},
            "needs_approval": False,
            "llm_metadata": {"sql_generated": "SELECT 1 FROM DUAL"},
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.queries.QueryService.submit_natural_language_query",
        fake_submit_nl,
    )

    payload = {
        "query": "Show a tiny sample from any safe table.",
        "user_id": "test_user",
        "session_id": "session_abc",
    }

    resp = await async_client.post("/api/v1/queries/process", json=payload)
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "success"
    assert data["query_id"] == "q_test_1234"
    assert data["sql_query"] == "SELECT 1 FROM DUAL"
    assert isinstance(data.get("results"), dict)
    assert data["results"].get("row_count") == 1


@pytest.mark.asyncio
async def test_process_endpoint_validation_empty_query(async_client: AsyncClient):
    """Empty query should return structured error response, not HTTP 400.

    This verifies the contract that even validation failures are surfaced as
    OrchestratorQueryResponse objects for the frontend.
    """

    payload = {
        "query": " ",
        "user_id": "test_user",
        "session_id": "session_empty",
    }

    resp = await async_client.post("/api/v1/queries/process", json=payload)
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "error"
    assert "error" in data and data["error"]
    assert data["llm_metadata"].get("validation_error") == "empty_query"


@pytest.mark.asyncio
async def test_submit_sql_endpoint_stubbed(async_client: AsyncClient, monkeypatch):
    """/api/v1/queries/submit should accept SQL and return a structured result.

    The underlying QueryService.execute_sql_query call is stubbed so the test
    does not depend on SQLcl or Oracle.
    """

    async def fake_execute_sql_query(sql_query: str, connection_name: str | None = None, timeout: float = 600.0, user_id: str | None = None, request_id: str | None = None):  # type: ignore[override]
        return {
            "status": "success",
            "sql": sql_query,
            "query_id": request_id or "direct_sql_test",
            "results": {
                "columns": ["VALUE"],
                "rows": [[1]],
                "row_count": 1,
            },
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.queries.QueryService.execute_sql_query",
        fake_execute_sql_query,
    )

    payload = {
        "query": "SELECT 1 FROM DUAL",
        "connection_name": "TestUserCSV",
    }

    # require_analyst_role accepts a dev token via RBAC for tests
    headers = {"Authorization": "Bearer temp-dev-token"}

    resp = await async_client.post("/api/v1/queries/submit", json=payload, headers=headers)
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "success"
    assert data["results"]["row_count"] == 1


@pytest.mark.asyncio
async def test_clarify_endpoint_stubbed(async_client: AsyncClient, monkeypatch):
    """/api/v1/queries/clarify should proxy through to the service layer.

    We stub QueryService.submit_natural_language_query so the endpoint contract
    can be validated without invoking the real orchestrator.
    """

    async def fake_submit_with_clarification(user_query: str, user_id: str, session_id: str | None, user_role: str, timeout: float = 600.0, thread_id_override: str | None = None):  # type: ignore[override]
        return {
            "query_id": session_id or "clarify_qid",
            "status": "success",
            "sql_query": "SELECT * FROM EMPLOYEES WHERE ROWNUM <= 5",
            "results": {
                "columns": ["ID"],
                "rows": [[1], [2]],
                "row_count": 2,
            },
            "clarification_message": "Using EMPLOYEES table as clarified",
        }

    monkeypatch.setattr(
        "app.api.v1.endpoints.queries.QueryService.submit_natural_language_query",
        fake_submit_with_clarification,
    )

    payload = {
        "query_id": "qid-clarify-1",
        "clarification": "Use EMPLOYEES as the primary table.",
        "original_query": "Show employee details.",
    }

    headers = {"Authorization": "Bearer temp-dev-token"}

    resp = await async_client.post("/api/v1/queries/clarify", json=payload, headers=headers)
    assert resp.status_code == 200

    data = resp.json()
    assert data["query_id"] == "qid-clarify-1"
    assert data["status"] == "success"
    assert data["sql_query"].startswith("SELECT * FROM EMPLOYEES")
    assert data["results"]["row_count"] == 2


@pytest.mark.asyncio
async def test_clarify_endpoint_validation_error(async_client: AsyncClient):
    """Empty clarification should yield HTTP 400 with a clear message."""

    payload = {
        "query_id": "qid-clarify-empty",
        "clarification": " ",
        "original_query": "Show employee details.",
    }

    headers = {"Authorization": "Bearer temp-dev-token"}

    resp = await async_client.post("/api/v1/queries/clarify", json=payload, headers=headers)
    assert resp.status_code == 400
    body = resp.json()
    assert body.get("detail") == "Clarification cannot be empty"
