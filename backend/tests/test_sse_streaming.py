"""SSE streaming tests for /api/v1/queries/{id}/stream.

These tests exercise the SSE endpoint in-process using FastAPI + httpx
with ASGITransport and the real QueryStateManager, without requiring
Oracle, Redis, or Graphiti.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.core.application import create_application
from app.services.query_state_manager import get_query_state_manager, QueryState


@pytest_asyncio.fixture
async def app_instance(monkeypatch):
    """Create a FastAPI app instance with Redis health check stubbed.

    The SSE endpoint does not depend on external services, but the
    /health endpoint used during app creation may touch Redis. We stub
    Redis health to keep this test independent of a live Redis server.
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
async def test_sse_streams_state_changes(async_client: AsyncClient):
    """SSE stream should deliver state transitions from QueryStateManager.

    This test simulates a simple lifecycle: RECEIVED -> FINISHED. It
    verifies that the SSE endpoint forwards these events and that the
    payload structure matches QueryStateEvent.to_sse_message.
    """

    manager = await get_query_state_manager()
    query_id = "sse-test-123"
    url = f"/api/v1/queries/{query_id}/stream?token=temp-dev-token"

    events: list[dict] = []

    async def producer() -> None:
        # Give the SSE subscription a moment to attach
        await asyncio.sleep(0.1)
        await manager.update_state(
            query_id,
            QueryState.RECEIVED,
            {"sql": "SELECT 1", "trace_id": "trace-sse-test"},
        )
        await asyncio.sleep(0.1)
        await manager.update_state(
            query_id,
            QueryState.FINISHED,
            {"result": {"row_count": 1}, "trace_id": "trace-sse-test"},
        )

    producer_task = asyncio.create_task(producer())

    async with async_client.stream("GET", url) as resp:
        assert resp.status_code == 200

        async for line in resp.aiter_lines():
            if not line:
                continue
            if line.startswith(":"):
                # keep-alive comment
                continue
            assert line.startswith("data: ")
            payload = json.loads(line[len("data: ") :])
            events.append(payload)
            # Stop once we see a terminal state
            if payload.get("state") in {"finished", "error", "rejected"}:
                break

    await producer_task

    assert any(e.get("state") == "received" for e in events)
    assert any(e.get("state") == "finished" for e in events)
    assert all(e.get("query_id") == query_id for e in events)
    # Ensure trace_id propagated through metadata/result
    assert any(
        (e.get("metadata") or {}).get("trace_id") == "trace-sse-test" for e in events
    )
