# Backend Documentation

## Quick Start

```bash
cd backend
.venv\Scripts\Activate.ps1
uv sync --all-extras
python main.py
```

## API Endpoints

### Core Endpoints
- `POST /api/v1/queries/process` - LangGraph orchestrator entry (NL to SQL with HITL)
- `POST /api/v1/queries/submit` - Direct SQL execution (analyst/admin)
- `POST /api/v1/queries/{id}/approve` - HITL approval state resumption
- `GET /api/v1/queries/{id}/stream` - SSE state streaming
- `POST /api/v1/clarify` - Multi-turn clarifications
- `POST /api/v1/report` - Executive report generation (HTML/PDF/DOCX)
- `POST /api/v1/visualize` - Plotly visualization configuration
- `GET /api/v1/schema` - Schema information and metadata
- `GET /health` - Enhanced health check with database validation
- `GET /health/degraded-mode` - System degradation status
- `POST /health/degraded-mode/recover` - Manual recovery attempts

### Database Endpoints
- `GET /api/v1/connections` - Connection management
- `POST /api/v1/connections/test` - Test connections
- `GET /api/v1/databases` - List databases
- `GET /api/v1/tables` - List tables

### Authentication
- `POST /api/v1/auth/login` - JWT authentication
- `POST /api/v1/auth/refresh` - Refresh token
- `POST /api/v1/auth/logout` - Logout

### Analytics & Governance
- `GET /api/v1/analytics` - Query analytics
- `POST /api/v1/corrections` - Query corrections
- `GET /api/v1/governance/capabilities/agents` - List all agents
- `GET /api/v1/governance/capabilities/systems` - List all systems
- `GET /api/v1/governance/capabilities/misconfigurations` - Detect misconfigurations
- `GET /api/v1/governance/permissions/matrix` - RBAC permission matrix
- `GET /api/v1/governance/audit/activity` - Agent activity monitoring
- `GET /api/v1/governance/audit/summary` - Audit summary statistics

### Cost Tracking
- `GET /api/v1/cost/usage` - Get usage statistics
- `POST /api/v1/cost/estimate` - Estimate query cost
- `GET /api/v1/cost/forecast` - Get budget forecast
- `GET /api/v1/cost/anomalies` - Get cost anomalies
- `GET /api/v1/cost/budget-alerts` - Get budget alerts
- `GET /api/v1/cost/optimization` - Get optimization recommendations
- `GET /api/v1/cost/dashboard` - Admin dashboard

### BI Integration
- `GET /api/v1/qlik/apps` - List Qlik Sense apps
- `GET /api/v1/qlik/apps/{id}/sheets` - List Qlik sheets
- `GET /api/v1/superset/dashboards` - List Superset dashboards
- `POST /api/v1/superset/dashboards/generate` - Auto-generate dashboards

## Langfuse Setup

### Configuration
```bash
# Enable Langfuse tracing
LANGFUSE_ENABLED=true
LANGFUSE_PUBLIC_KEY=pk-lf-your-public-key
LANGFUSE_SECRET_KEY=sk-lf-your-secret-key
LANGFUSE_HOST=https://cloud.langfuse.com
```

### Traced Operations
- Query orchestration (trace name: `query_orchestration`)
- Orchestrator nodes (understand, retrieve_context, decompose, hypothesis, generate_sql, validate, await_approval, probe_sql, execute, validate_results, format_results, repair_sql, fallback_sql)
- LLM generations (SQL, intent classification, hypothesis, column mapping)
- State transitions, approval decisions, validation results, error events

### Key Metrics
- Trace duration, LLM latency, token usage, error rate, approval rate

## Skills Architecture

### Core Skills
- **SchemaAnalysisSkill** - Identify relevant tables and business concepts
- **ColumnMappingSkill** - Map concepts to columns or derived expressions
- **SQLGenerationSkill** - Build enhanced prompt with validated mappings
- **ValidationSkill** - Validate generated SQL against schema

### Integration
- File: `app/orchestrator/nodes/sql_generation.py`
- Auto-generation: `app/services/skill_generator_service.py`
- Configuration: `QUERY_SQL_SKILLS_ENABLED=true/false`

## Database Support

### Oracle (SQLcl MCP)
- Process pool management
- Connection management with `.dbtools/connections.json`
- Session tagging in V$SESSION (MODULE/ACTION)
- Error normalization

### Doris (HTTP MCP)
- HTTP-based integration
- Connection management
- Error normalization
- Schema introspection

### PostgreSQL (psycopg3)
- Read-only enforcement
- Connection pooling
- Query timeout (configurable, default 30s)
- SQL dialect optimization (LIMIT, COALESCE, NOW())

## Error Responses

| Code | Response |
|------|----------|
| 400 | Invalid parameters |
| 401 | Not authenticated |
| 403 | Insufficient permissions |
| 502 | External service error |
| 503 | Service unavailable |

## Rate Limiting

- Auth: 5 req/5min
- Query: 100 req/min/user
- Schema: 50 req/min/user
- Cost: 50 req/min/user

## Security

- JWT authentication with refresh tokens
- RBAC with 5 roles (guest, viewer, analyst, developer, admin)
- DLP: PII/credentials scanning and redaction
- SQL injection prevention
- Audit logging with immutable trails
- Session binding for query attribution

## Recent Updates (2026-02-03)

- SQL generation is dialect-first with strict intent parsing and structured hypothesis constraints.
- HITL approval surfaces scope/cartesian risk signals in the UI.
- SSE dev token is accepted only in development/test environments for local runs.
