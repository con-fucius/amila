# AMIL Backend - FastAPI + LangGraph

## Architecture

```
FastAPI Router -> LangGraph Orchestrator -> SQLcl MCP (STDIO) -> Oracle DB
                     
   Redis        Skills Pattern (Anthropic Oct 2025)
      
Graphiti/FalkorDB (Knowledge Graph)
```

## Structure

```bash
app/
 orchestrator/              # LangGraph query orchestrator (active)
    graph.py               # StateGraph builder
    processor.py           # Main process_query entrypoint
    state.py               # Orchestrator state TypedDict
    llm_config.py          # LLM configuration (Gemini/Bedrock/Qwen)
    utils.py               # SSE + Langfuse helpers
    nodes/                 # Individual nodes (understand, context, sql_generation, validation, execution, results, ...)
 agents/                    # Reserved for future agents (legacy orchestrator files removed)
 api/v1/endpoints/          # FastAPI routes
    queries.py             # Query submission, approval, clarification, SSE streaming
    schema.py              # Schema exploration
    health.py              # Health checks
    auth.py                # JWT authentication
 core/                      # Infrastructure
    application.py         # FastAPI app factory + startup wiring
    mcp_client.py          # SQLcl STDIO client (fallback)
    sqlcl_pool.py          # SQLcl process pool for concurrency
    config_manager.py      # Pydantic settings
    redis_client.py        # Redis sessions/cache
    graphiti_client.py     # Graphiti + FalkorDB client
    rbac.py                # Role-based access control
    auth.py                # JWT utilities
    rate_limiter.py        # Per-user/per-endpoint rate limiting
    structured_logging.py  # Context-aware structured logging
    observability.py       # OpenTelemetry + Prometheus metrics
 services/                  # Business logic
    query_service.py       # NL -> orchestrator + direct SQL execution
    schema_service.py      # Schema and metadata access
    schema_enrichment_service.py  # Sample data + relationships
    semantic_schema_index_service.py  # Semantic schema index
    insights_service.py    # Result insights and suggested queries
 tasks/                     # Background tasks
     graphiti_cleanup.py    # 30-day retention
```

## Setup

See the root `README.md` for end-to-end startup instructions (backend, frontend, Docker). This backend README focuses on folder structure and key components.

## Configuration

See the root `README.md` for the canonical `.env` variable list.

### LLM provider selection

- `QUERY_LLM_PROVIDER` and `GRAPHITI_LLM_PROVIDER` can be set to `gemini`, `bedrock`, or `qwen` via environment.
- `QUERY_LLM_MODEL` can override the default model; set it in `.env` when you need a specific model.

## Key Components

### LangGraph Orchestrator
Active orchestrator lives under `app/orchestrator/` and is built via `create_query_orchestrator`:

`app/orchestrator/graph.py` and `app/orchestrator/nodes/*` implement the multi-node workflow:
1. **Understand Intent** - Intent classification
2. **Retrieve Context** - Graphiti patterns + schema enrichment
3. **Generate SQL** - Skills pattern + LLM (Bedrock/Gemini)
4. **Validate** - Security + SQL injection detection
5. **Approval Check** - HITL for non-admin users
6. **Execute** - SQLcl MCP
7. **Format Results** - Insights + suggested queries

### MCP Client
`app/core/mcp_client.py` - STDIO/JSON-RPC communication:
- Subprocess pool (max 2 processes)
- Request/response correlation with timeouts (30s default)
- Background I/O threading
- Connection health monitoring
- Graceful fallback to single client

### Query Service
`app/services/query_service.py` - Business logic:
- Query validation
- Execution orchestration
- Result formatting
- Error handling

### Logging

- Console: INFO+ logs for a concise view.
- File: DEBUG+ logs in `backend/logs/bi-agent.log` for detailed MCP/Graphiti diagnostics.
- Adjust level via `LOG_LEVEL` in `.env` (maps to `settings.log_level`).

## API Endpoints

For the full, up-to-date endpoint list, use the root `README.md` or the FastAPI docs at `/docs`. This file focuses on backend internals rather than duplicating API reference material.

## Testing

See the root `README.md` for end-to-end startup and basic testing commands.

For backend-focused scenarios and detailed checklists, use:

- `TESTING_CHECKLIST.md` - end-to-end critical user journeys (frontend + backend).
- `backend/TEST_SCENARIOS.md` - HTTP/API-level backend test scenarios.

## Current Limitations

High-level limitations (SQLcl pool, circuit breaker coverage, error recovery, and test coverage) are documented in the root `README.md` under **Current Limitations**. That section is the canonical source for overall system constraints.

## Next Steps

The root `README.md` "Next Phase" section tracks the project-wide roadmap. Backend work mainly maps to SQLcl tuning, broader resilience coverage, and increased automated test coverage.