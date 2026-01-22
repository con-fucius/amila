# Amila Backend

FastAPI + LangGraph orchestrator for natural language database queries.

## Structure

```
app/
  orchestrator/     # LangGraph query orchestrator
  api/v1/endpoints/ # FastAPI routes
  core/             # Infrastructure (Redis, auth, logging)
  services/         # Business logic (Qlik, Superset, database services)
  tasks/            # Background tasks
  skills/           # YAML-based SQL generation skills
```

## Key Components

- **LangGraph Orchestrator** - Multi-node workflow: intent -> context -> SQL generation -> validation -> execution -> results
- **MCP Client** - STDIO/JSON-RPC communication with SQLcl (Oracle) and HTTP with Doris
- **PostgreSQL Integration** - Read-only PostgreSQL support via MCP (crystaldba/postgres-mcp)
- **Qlik Sense Integration** - Read-only access to Qlik Sense dashboards and apps via QRS API
- **Apache Superset Integration** - Dashboard auto-generation and management via REST API
- **Redis** - Sessions, caching, rate limiting
- **FalkorDB** - Knowledge graph for query patterns

## Database Support

### Oracle
- Via SQLcl MCP server (STDIO/JSON-RPC)
- Connection pooling with process management
- Full SQL dialect support

### Apache Doris
- Via HTTP MCP server
- MySQL-compatible SQL dialect
- Advanced analytics capabilities

### PostgreSQL
- Via PostgreSQL MCP Pro (crystaldba/postgres-mcp)
- Read-only transactions enforced
- SQL parsing and validation
- Connection pooling

## Visualization & BI Integration

### Qlik Sense (On-Premises)
- Read-only API access via QRS API
- List apps, sheets, and visualizations
- Dashboard metadata retrieval
- Requires: `QLIK_BASE_URL`, `QLIK_AUTH_USER`

### Apache Superset (On-Premises)
- Dashboard auto-generation from query results
- Chart creation and management
- Visualization recommendations
- Requires: `SUPERSET_BASE_URL`, `SUPERSET_USERNAME`, `SUPERSET_PASSWORD`

## Configuration

Set in `.env`:
- `QUERY_LLM_PROVIDER` - `gemini`, `bedrock`, `qwen`, or `openrouter`
- `LOG_LEVEL` - Logging verbosity
- `POSTGRES_ENABLED` - Enable PostgreSQL integration
- `QLIK_BASE_URL` - Qlik Sense server URL
- `SUPERSET_BASE_URL` - Apache Superset server URL

## Logging

- Console: INFO+
- File: `logs/bi-agent.log` (DEBUG+)
