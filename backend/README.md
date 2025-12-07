# Amila Backend

FastAPI + LangGraph orchestrator for natural language database queries.

## Structure

```
app/
  orchestrator/     # LangGraph query orchestrator
  api/v1/endpoints/ # FastAPI routes
  core/             # Infrastructure (Redis, auth, logging)
  services/         # Business logic
  tasks/            # Background tasks
```

## Key Components

- **LangGraph Orchestrator** - Multi-node workflow: intent -> context -> SQL generation -> validation -> execution -> results
- **MCP Client** - STDIO/JSON-RPC communication with SQLcl
- **Redis** - Sessions, caching, rate limiting
- **FalkorDB** - Knowledge graph for query patterns

## Configuration

Set in `.env`:
- `QUERY_LLM_PROVIDER` - `gemini`, `bedrock`, or `qwen`
- `LOG_LEVEL` - Logging verbosity

## Logging

- Console: INFO+
- File: `logs/bi-agent.log` (DEBUG+)
