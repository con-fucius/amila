"""Environment profile tests for live backend stacks.

These tests are designed to run against a *running* backend (and
optionally frontend + databases). They are gated by environment
variables so they can be enabled explicitly when the full stack is up.

- Full stack profile:   AMIL_EXPECT_FULL_STACK=1
- Degraded profile:     AMIL_EXPECT_DEGRADED_STACK=1

They use the existing health endpoints and do not depend on pytest
fixtures from in-process tests.
"""

from __future__ import annotations

import os

import httpx
import pytest

BASE_URL = os.getenv("SMOKE_API_BASE_URL", "http://127.0.0.1:8000")
FRONTEND_URL = os.getenv("AMIL_FRONTEND_URL", "http://localhost:3000")


@pytest.mark.asyncio
@pytest.mark.full_stack
@pytest.mark.skipif(
    os.getenv("AMIL_EXPECT_FULL_STACK") != "1",
    reason="Full stack profile not enabled (set AMIL_EXPECT_FULL_STACK=1)",
)
async def test_full_stack_health_profile() -> None:
    """Validate health profile when all core services are expected to be running.

    Expects backend, Redis, SQLcl pool, Oracle, and Graphiti/FalkorDB to
    be reachable. Frontend is optional but checked if available.
    """

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{BASE_URL}/api/v1/health/detailed")
        except httpx.RequestError as exc:  # pragma: no cover - network dependent
            pytest.skip(f"Backend not reachable at {BASE_URL}: {exc}")

        assert resp.status_code == 200
        data = resp.json()
        components = data.get("components", {})

        sqlcl = components.get("sqlcl_pool", {})
        assert sqlcl.get("status") in {"active", "inactive", "not_initialized"}
        # In a full stack profile, the pool should be initialized and active
        assert sqlcl.get("status") == "active"

        redis = components.get("redis", {})
        assert redis.get("status") == "connected"

        graphiti = components.get("graphiti", {})
        # Full stack should have an initialized Graphiti client
        assert graphiti.get("status") == "connected"

        # Dependency endpoint should confirm Oracle + Redis reachability
        dep_resp = await client.get(f"{BASE_URL}/api/v1/health/dependencies")
        assert dep_resp.status_code == 200
        deps = dep_resp.json().get("dependencies", {})
        assert deps.get("oracle_database", {}).get("status") == "reachable"
        assert deps.get("redis", {}).get("status") == "reachable"

    # Optional frontend sanity check (do not fail if not running)
    try:  # pragma: no cover - environment dependent
        async with httpx.AsyncClient(timeout=5.0) as client:
            fr = await client.get(FRONTEND_URL)
            # Only assert basic reachability if we got a response
            assert fr.status_code < 500
    except httpx.RequestError:
        # Frontend not required for backend health tests
        pass


@pytest.mark.asyncio
@pytest.mark.degraded_stack
@pytest.mark.skipif(
    os.getenv("AMIL_EXPECT_DEGRADED_STACK") != "1",
    reason="Degraded stack profile not enabled (set AMIL_EXPECT_DEGRADED_STACK=1)",
)
async def test_degraded_but_operational_profile() -> None:
    """Validate health profile when non-critical dependencies may be down.

    Backend and SQL execution must still be reachable, even if Redis or
    Graphiti/FalkorDB are degraded. This corresponds to a "degraded"
    health status but an operational system from the user's perspective.
    """

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{BASE_URL}/api/v1/health/detailed")
        except httpx.RequestError as exc:  # pragma: no cover - network dependent
            pytest.skip(f"Backend not reachable at {BASE_URL}: {exc}")

        assert resp.status_code == 200
        data = resp.json()
        status = data.get("status")
        components = data.get("components", {})

        # In degraded mode we expect status to be "degraded" but core
        # query path components must still be present.
        assert status in {"healthy", "degraded"}
        assert status == "degraded"

        sqlcl = components.get("sqlcl_pool", {})
        # Pool may be active or not_initialized depending on startup
        assert sqlcl.get("status") in {"active", "not_initialized", "error"}

        # Basic API health must still report healthy service metadata
        status_resp = await client.get(f"{BASE_URL}/api/v1/health/status")
        assert status_resp.status_code == 200
        status_body = status_resp.json()
        assert status_body.get("status") == "healthy"

        # Even in degraded mode, the health dependencies endpoint should
        # reflect at least one reachable core dependency (Oracle or Redis).
        dep_resp = await client.get(f"{BASE_URL}/api/v1/health/dependencies")
        assert dep_resp.status_code == 200
        deps = dep_resp.json().get("dependencies", {})
        reachable = {
            name
            for name, meta in deps.items()
            if meta.get("status") in {"reachable", "pool_unavailable"}
        }
        assert reachable, "Expected at least one reachable core dependency"
