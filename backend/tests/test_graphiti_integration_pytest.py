"""Graphiti / FalkorDB integration tests (stubbed + optional live).

These tests complement the existing script-style Graphiti tests by
providing:

- A stubbed test that verifies the orchestrator context node behaves
  correctly when Graphiti is unavailable.
- An optional live test (gated by AMIL_GRAPHITI_LIVE=1) that executes a
  minimal Graphiti search against a running FalkorDB + graphiti-core
  installation.
"""

from __future__ import annotations

import os

import pytest

from app.core.client_registry import registry
from app.orchestrator.nodes.context import retrieve_context_node


@pytest.mark.asyncio
async def test_context_node_handles_graphiti_unavailable(monkeypatch):
    """Context node should gracefully handle missing Graphiti client.

    This test stubs schema/semantic dependencies so that we exercise the
    Graphiti-unavailable path without requiring Oracle, Redis, or
    FalkorDB. It asserts that the node sets graphiti_available=False and
    still produces a context dict without raising.
    """

    # Ensure registry returns no Graphiti client
    monkeypatch.setattr(
        registry,
        "get_graphiti_client",
        lambda: None,
    )

    # Stub schema enrichment and semantic index services used by the node

    class DummySchemaEnrichmentService:  # pragma: no cover - simple stub
        async def get_enriched_schema_context(
            self,
            user_query: str,
            intent: str = "",
            include_samples: bool = True,
            include_relationships: bool = True,
            sample_limit: int = 3,
        ) -> dict:
            return {"tables": {}, "samples": {}, "relationships": []}

    class DummySchemaService:  # pragma: no cover - simple stub
        @staticmethod
        async def get_dynamic_schema(
            user_query: str,
            intent: str = "",
            max_tables: int = 15,
        ) -> dict:
            return {
                "status": "success",
                "schema": {"tables": {}},
                "source": "dummy",
                "keywords_used": [],
                "tables_analyzed": 0,
            }

        @staticmethod
        async def get_database_schema(use_cache: bool = True) -> dict:
            return {"status": "success", "schema": {"tables": {}}}

    class DummySemanticSchemaIndexService:  # pragma: no cover - simple stub
        async def ensure_built_if_empty(self) -> None:
            return None

        async def search(self, query: str, top_k: int = 10) -> list[dict]:
            return []

    class DummySmartContextManager:  # pragma: no cover - not used, but kept for safety
        pass

    # Patch the service modules that the context node imports from.
    import app.services.schema_enrichment_service as enrich_mod
    import app.services.schema_service as schema_mod
    import app.services.semantic_schema_index_service as sem_mod
    import app.services.context_manager_service as ctxmgr_mod

    monkeypatch.setattr(
        enrich_mod,
        "SchemaEnrichmentService",
        DummySchemaEnrichmentService,
    )
    monkeypatch.setattr(schema_mod, "SchemaService", DummySchemaService)
    monkeypatch.setattr(
        sem_mod,
        "SemanticSchemaIndexService",
        DummySemanticSchemaIndexService,
    )
    monkeypatch.setattr(
        ctxmgr_mod,
        "SmartContextManager",
        DummySmartContextManager,
        raising=False,
    )

    state = {
        "user_query": "Show total revenue by month",
        "user_id": "graphiti_test_user",
        "session_id": "graphiti_test_session",
        "intent": "analyze revenue",
        "messages": [],
    }

    new_state = await retrieve_context_node(state)
    ctx = new_state.get("context", {})

    assert isinstance(ctx, dict)
    assert ctx.get("graphiti_available") is False
    # schema_metadata should be present (even if empty) after fallback logic
    assert "schema_metadata" in ctx


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("AMIL_GRAPHITI_LIVE") != "1",
    reason="Live Graphiti test disabled (set AMIL_GRAPHITI_LIVE=1)",
)
async def test_graphiti_live_search_roundtrip():
    """Minimal live Graphiti roundtrip against FalkorDB (optional).

    Requires:
    - graphiti-core with FalkorDB + Google Gemini configured
    - FalkorDB running and reachable

    This test is intentionally simple and is skipped unless explicitly
    enabled via AMIL_GRAPHITI_LIVE=1.
    """

    try:
        from app.core.graphiti_client import create_graphiti_client
    except Exception as exc:  # pragma: no cover - import-time environment
        pytest.skip(f"Graphiti client not importable: {exc}")

    client = await create_graphiti_client()
    try:
        results = await client.search("system initialization", num_results=1)
        assert isinstance(results, list)
    finally:
        await client.close()
