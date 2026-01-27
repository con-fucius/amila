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

## Architecture & Services

The backend uses **LangGraph** to orchestrate the query lifecycle:
`Intent Classification` → `Context Retrieval` → `SQL Generation` → `Security Validation` → `Execution` → `Result Formatting`.

- **DatabaseRouter**: Routes to Oracle, Doris, or PostgreSQL.
- **SkillsLoader**: Dynamic YAML-based generation rules.
- **DiagnosticService**: Real-time health monitoring (`/api/v1/diagnostics/status`).

## Configuration & Environment

Environment variables are managed in `.env`. Key services include:
- **Redis**: Caching, rate limiting, and session state.
- **FalkorDB/Graphiti**: Context storage for the knowledge graph.
- **Celery**: Asynchronous tasks and exports.

## Tracing

Full observability is integrated via **OpenTelemetry** and **Langfuse**. Traces can be accessed when the monitoring profile is active.
