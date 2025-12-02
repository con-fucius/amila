import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import pytest_asyncio

from app.services.query_service import QueryService
from app.core.client_registry import registry
from app.services.query_state_manager import get_query_state_manager, QueryState
from app.orchestrator import processor as processor_module


@pytest.fixture(autouse=True)
def restore_registry():
    original_pool = registry.get_sqlcl_pool()
    original_client = registry.get_mcp_client()
    original_orchestrator = registry.get_query_orchestrator()
    yield
    if original_pool:
        registry.set_sqlcl_pool(original_pool)
    else:
        registry.set_sqlcl_pool(None)
    if original_client:
        registry.set_mcp_client(original_client)
    else:
        registry.set_mcp_client(None)
    if original_orchestrator:
        registry.set_query_orchestrator(original_orchestrator)
    else:
        registry.set_query_orchestrator(None)


@pytest_asyncio.fixture
async def state_manager():
    manager = await get_query_state_manager()
    try:
        yield manager
    finally:
        # Cleanup stored states to avoid leakage between tests
        for query_id in list(manager._query_states.keys()):  # noqa: SLF001 (tests may inspect internals)
            await manager.cleanup_query(query_id)


@pytest.mark.asyncio
async def test_execute_sql_query_success_traces(monkeypatch):
    events = []

    def fake_create_trace(query_id, user_id, user_query, metadata=None):
        events.append(("create_trace", query_id, metadata))
        return "trace-success"

    def fake_update_trace(trace_id, output_data=None, metadata=None, tags=None):
        events.append(("update_trace", trace_id, output_data, tags))

    class FakeLangfuseClient:
        def flush(self):
            events.append(("flush",))

    @asynccontextmanager
    async def fake_trace_span(trace_id, span_name, input_data=None, metadata=None):
        span_data = {"output": {}, "metadata": metadata or {}, "level": "DEFAULT"}
        events.append(("trace_span", span_name, input_data, metadata))
        yield span_data
        events.append(("trace_span_end", span_name, span_data["output"]))

    class DummyClient:
        async def execute_sql(self, sql, conn):
            await asyncio.sleep(0)
            return {
                "status": "success",
                "results": {"row_count": 2, "rows": [[1], [2]], "columns": ["value"]},
            }

    class DummyAcquire:
        async def __aenter__(self):
            return DummyClient()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummyPool:
        pool_size = 1

        def acquire(self, timeout=30):
            return DummyAcquire()

    monkeypatch.setattr("app.services.query_service.create_trace", fake_create_trace)
    monkeypatch.setattr("app.services.query_service.update_trace", fake_update_trace)
    monkeypatch.setattr("app.services.query_service.get_langfuse_client", lambda: FakeLangfuseClient())
    monkeypatch.setattr("app.services.query_service.trace_span", fake_trace_span)

    registry.set_sqlcl_pool(DummyPool())
    registry.set_mcp_client(None)

    result = await QueryService.execute_sql_query("SELECT 1 FROM dual", connection_name="Test")

    assert result["trace_id"] == "trace-success"
    statuses = [evt for evt in events if evt[0] == "update_trace"]
    assert statuses and statuses[-1][2]["status"] == "success"
    assert any(evt[0] == "flush" for evt in events)


@pytest.mark.asyncio
async def test_execute_sql_query_timeout_records_error(monkeypatch):
    recorded = {}

    def fake_create_trace(*args, **kwargs):
        return "trace-timeout"

    def fake_update_trace(trace_id, output_data=None, metadata=None, tags=None):
        recorded.setdefault("updates", []).append((trace_id, output_data, tags))

    class FakeLangfuseClient:
        def flush(self):
            recorded.setdefault("flushed", True)

    @asynccontextmanager
    async def fake_trace_span(trace_id, span_name, input_data=None, metadata=None):
        span_data = {"output": {}, "metadata": metadata or {}, "level": "DEFAULT"}
        yield span_data

    class DummyClient:
        async def execute_sql(self, sql, conn):
            await asyncio.sleep(0)
            raise asyncio.TimeoutError

    class DummyAcquire:
        async def __aenter__(self):
            return DummyClient()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummyPool:
        pool_size = 1

        def acquire(self, timeout=30):
            return DummyAcquire()

    monkeypatch.setattr("app.services.query_service.create_trace", fake_create_trace)
    monkeypatch.setattr("app.services.query_service.update_trace", fake_update_trace)
    monkeypatch.setattr("app.services.query_service.get_langfuse_client", lambda: FakeLangfuseClient())
    monkeypatch.setattr("app.services.query_service.trace_span", fake_trace_span)

    registry.set_sqlcl_pool(DummyPool())
    registry.set_mcp_client(None)

    with pytest.raises(TimeoutError):
        await QueryService.execute_sql_query("SELECT 1", connection_name="Test")

    updates = recorded.get("updates", [])
    assert updates, "update_trace should be called on timeout"
    last_update = updates[-1]
    assert last_update[0] == "trace-timeout"
    assert last_update[1]["status"] == "error"


@pytest.mark.asyncio
async def test_query_state_manager_logs_langfuse_event(monkeypatch, state_manager):
    log_events = []

    def fake_log_event(trace_id, name, input_data=None, output_data=None, metadata=None, level="DEFAULT"):
        log_events.append((trace_id, name, output_data))

    monkeypatch.setattr("app.core.langfuse_client.log_event", fake_log_event)

    await state_manager.update_state(
        "query-123",
        QueryState.RECEIVED,
        {"trace_id": "trace-state-1", "result": {"row_count": 0}},
    )

    assert log_events
    trace_id, name, output = log_events[0]
    assert trace_id == "trace-state-1"
    assert name == "sse_state.received"
    assert output["trace_id"] == "trace-state-1"


@pytest.mark.asyncio
async def test_emit_state_event_injects_trace(monkeypatch):
    from app.orchestrator import utils as utils_module

    captured = []

    class DummyManager:
        async def update_state(self, query_id, state, metadata=None):
            captured.append((query_id, state, metadata))

    async def fake_get_manager():
        return DummyManager()

    monkeypatch.setattr(utils_module, "_get_qs_manager", fake_get_manager)
    monkeypatch.setattr(utils_module, "ExecState", SimpleNamespace(FINISHED="finished"))

    await utils_module.emit_state_event({"query_id": "trace-q", "trace_id": "trace-emit"}, "finished", {})

    assert captured
    _, _, metadata = captured[0]
    assert metadata["trace_id"] == "trace-emit"


@pytest.mark.asyncio
async def test_process_query_produces_trace(monkeypatch):
    events = []

    def fake_create_trace(query_id, user_id, user_query, metadata=None):
        events.append(("create_trace", query_id))
        return "trace-process"

    def fake_update_trace(trace_id, output_data=None, metadata=None, tags=None):
        events.append(("update_trace", trace_id, output_data, tags))

    class FakeLangfuseClient:
        def flush(self):
            events.append(("flush",))

    @asynccontextmanager
    async def fake_trace_span(trace_id, span_name, input_data=None, metadata=None):
        span_data = {"output": {}, "metadata": metadata or {}, "level": "DEFAULT"}
        events.append(("trace_span", span_name))
        yield span_data
        events.append(("trace_span_end", span_name, span_data["output"]))

    class DummyOrchestrator:
        async def ainvoke(self, initial_state, config):
            state = dict(initial_state)
            state.update(
                {
                    "sql_query": "SELECT 1",
                    "execution_result": {
                        "columns": ["value"],
                        "rows": [[1]],
                        "row_count": 1,
                        "execution_time_ms": 5,
                    },
                    "needs_approval": False,
                }
            )
            return state

    class DummyQSManager:
        def __init__(self):
            self.states = []

        async def update_state(self, query_id, new_state, metadata=None):
            self.states.append((query_id, new_state, metadata))

    dummy_manager = DummyQSManager()

    async def fake_get_manager():
        return dummy_manager

    # Monkeypatch orchestrator dependencies
    monkeypatch.setattr("app.core.langfuse_client.create_trace", fake_create_trace)
    monkeypatch.setattr("app.core.langfuse_client.update_trace", fake_update_trace)
    monkeypatch.setattr("app.core.langfuse_client.get_langfuse_client", lambda: FakeLangfuseClient())
    monkeypatch.setattr("app.core.langfuse_client.trace_span", fake_trace_span)
    monkeypatch.setattr(processor_module, "_get_qs_manager", fake_get_manager)
    monkeypatch.setattr(
        processor_module,
        "ExecState",
        SimpleNamespace(RECEIVED="received", PENDING_APPROVAL="pending", FINISHED="finished", ERROR="error"),
    )

    # Structured logging context tracking
    context_events = []
    monkeypatch.setattr(
        "app.core.structured_logging.set_trace_id",
        lambda trace_id: context_events.append(("set_trace_id", trace_id)),
    )
    monkeypatch.setattr(
        "app.core.structured_logging.set_user_context",
        lambda user_id=None, session_id=None: context_events.append(("set_user", user_id, session_id)),
    )
    monkeypatch.setattr(
        "app.core.structured_logging.clear_context",
        lambda: context_events.append(("clear_context",)),
    )

    # Registry orchestrator stub
    registry.set_query_orchestrator(DummyOrchestrator())

    result = await processor_module.process_query(
        user_query="Show total revenue",
        user_id="tester",
        session_id="session-1",
        user_role="analyst",
    )

    assert result["trace_id"] == "trace-process"
    assert any(evt[0] == "update_trace" and evt[2]["status"] == "success" for evt in events)
    assert any(evt[0] == "flush" for evt in events)
    assert context_events[0] == ("set_trace_id", "trace-process")
    assert context_events[-1] == ("clear_context",)
    assert any(meta.get("trace_id") == "trace-process" for _, _, meta in dummy_manager.states)
