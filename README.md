# Amila

Natural language Oracle database queries via LangGraph orchestrator and SQLcl MCP.

## Architecture

```
React/TypeScript -> FastAPI -> LangGraph Agent -> SQLcl MCP (STDIO) -> Oracle DB
                           
                    Redis + Graphiti/FalkorDB + Celery + Prometheus/Grafana
```

**Communication:** STDIO/JSON-RPC with SQLcl subprocess (NOT HTTP)

### Architecture in 60 seconds

- Frontend (React/TypeScript) sends natural language or SQL queries to the FastAPI backend.
- Backend orchestrator (LangGraph) validates input, retrieves schema/context, and generates SQL.
- SQLcl MCP executes SQL against Oracle; results are validated and formatted.
- Redis and Graphiti/FalkorDB provide caching, sessions, and knowledge graph episodes.
- Metrics and logs are exported to Prometheus/Grafana and structured log files.

## Stack

**Backend:** FastAPI, LangGraph, SQLcl MCP, Redis, Celery  
**Frontend:** React, TypeScript, Material-UI, Vite  
**AI:** AWS Bedrock (prod), Google Gemini (dev)  
**Knowledge Graph:** Graphiti + FalkorDB  
**Monitoring & Observability:** Prometheus, Grafana, OpenTelemetry, Langfuse (optional)  
**Visualization:** Apache Superset  
**Orchestration:** Docker Compose

## Prerequisites

- Oracle SQLcl 25.2+ at `D:\DownloadsInD\sqlcl-latest\sqlcl\bin\sql.exe`
- UV 0.8.20+ (Python package manager)
- pnpm (Node.js package manager)
- Python 3.12
- Node.js 18+
- Docker Desktop

## Quick Start

### 1. Configure Oracle Connection (One-time)
```powershell
Set-Location "path\to\amil\bi-agent-mvp"
.\configure_sqlcl_connection.bat
# Saves: <oracle_username>/<oracle_password>@localhost:1521/XEPDB1
```

### 2. Backend
```powershell
Set-Location "path\to\amil\bi-agent-mvp\backend"
uv venv --python 3.12
.venv\Scripts\Activate.ps1
uv pip install -e .
python main.py
# Server: http://127.0.0.1:8000
```

### 3. Frontend
```powershell
Set-Location "path\to\amil\bi-agent-mvp\frontend"
pnpm install
pnpm dev
# Server: http://localhost:3000
```

### 4. Full Stack (Docker)
```powershell
Set-Location "path\to\amil\bi-agent-mvp"
docker-compose up -d
# Backend: http://localhost:8000
# Frontend: http://localhost:3000
# Grafana: http://localhost:3002
# Superset: http://localhost:8088
# Flower: http://localhost:5555
```

### Data & volumes

Docker Compose creates named volumes for stateful services:

- `bi-agent-redis-data`  Redis data
- `bi-agent-falkordb-data`  FalkorDB / knowledge graph
- `bi-agent-superset-db-data`  Superset Postgres database
- `bi-agent-superset-home`  Superset home directory

These volumes are not committed to git. To reset them, use `docker-compose down -v` or remove individual volumes with `docker volume rm <name>`.

### 5. Local Development (Separate Terminals)

Run each of the following in its own PowerShell terminal.

**Terminal 1 - Redis + FalkorDB**
```powershell
Set-Location "path\to\amil\bi-agent-mvp"
docker-compose up -d redis falkordb
```

**Terminal 2 - Backend (venv / uv)**
```powershell
Set-Location "path\to\amil\bi-agent-mvp\backend"
.venv\Scripts\Activate.ps1
python main.py

# or using uv
uv sync
uv run python main.py
```

**Terminal 3 - Frontend**
```powershell
Set-Location "path\to\amil\bi-agent-mvp\frontend"
pnpm dev
```

**Run a single backend test with uv**
```powershell
Set-Location "path\to\amil\bi-agent-mvp\backend"
.venv\Scripts\Activate.ps1
uv run python tests\name_of_test_script.py
```

**Shutdown Redis + FalkorDB**
```powershell
Set-Location "path\to\amil\bi-agent-mvp"
docker-compose down
```

## Testing

**Health Check:**
```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/health/status"
```

**List Connections:**
```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/queries/connections"
```

**Execute Query:**
```powershell
$body = '{"query": "SELECT * FROM CUSTOMER_DATASET FETCH FIRST 5 ROWS ONLY", "connection_name": "TestUserCSV"}'
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/queries/submit" -Method POST -Headers @{'Content-Type'='application/json'} -Body $body
```

## API Endpoints

**Health:** `GET /health`, `GET /api/v1/health/{status|ready|live}`  
**Queries:** `POST /api/v1/queries/process`, `POST /api/v1/queries/submit`, `POST /api/v1/queries/{id}/approve`, `POST /api/v1/queries/clarify`, `GET /api/v1/queries/{id}/stream`, `GET /api/v1/queries/connections`  
**Schema:** `GET /api/v1/schema/`, `POST /api/v1/schema/refresh`, `DELETE /api/v1/schema/cache`  
**Auth:** `POST /api/v1/auth/login`, `POST /api/v1/auth/refresh`  
**Docs:** http://127.0.0.1:8000/docs

## Query Examples

**Natural Language:**
```
"Show top 10 customers by revenue"
"List employees hired in 2024"
"What's the average sales by region for Q3?"
```

**Prompting guidance:** Always include the exact Oracle table name(s) you want to query in your prompt. Make sure the table name is spelled correctly and that your account has access to it.

**Direct SQL:**
```sql
SELECT * FROM CUSTOMER_DATASET FETCH FIRST 5 ROWS ONLY;
SELECT customer_name, SUM(order_amount) FROM orders GROUP BY customer_name;
```

## Environment Variables

Create `.env` in `backend/`:
```bash
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_REGION=us-east-1
GOOGLE_API_KEY=xxx  # Dev/test only
JWT_SECRET_KEY=xxx
REDIS_PASSWORD=xxx
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=XEPDB1
ORACLE_USERNAME=xxx
ORACLE_PASSWORD=xxx
```

## Services

**Redis:** Sessions, cache (port 6379)  
**FalkorDB:** Knowledge graph (port 6380, UI: 3001)  
**Prometheus:** Metrics (port 9090)  
**Grafana:** Dashboards (port 3002, admin/admin)  
**Superset:** BI dashboards (port 8088)  
**Flower:** Celery monitor (port 5555, admin/admin)

## Development

**Windows 11 + PowerShell**  
**IDE:** VS Code 1.104.3  
**Project Root:** `d:\Projects\Amil\`  
**Never use:** pip, npm (use uv, pnpm)

## Reporting Issues

When reporting issues, please include the following:

1. **Issues encountered** - What issues did you find during your test query? (errors, warnings, unexpected behavior, UI problems, etc.)
2. **Backend logs** - Any warnings or errors in the backend terminal output?
3. **Frontend behavior** - Any console errors or UI glitches you noticed?
4. **Specific pain points** - What felt broken, slow, or confusing during your test?

## Recent Improvements (Nov 2025)

**Phase 6 - Architectural Consolidation & Performance:**
- **Doris MCP Integration:** Added `DORIS_MCP_SERVER_PATH` to support local service paths without runtime git cloning.
- **Directory Cleanup:** Consolidated Doris MCP server into `backend/services/doris-mcp-server`.
- **Code Refactoring:** Created shared `ExecutionService` and unified connection logic with `ConnectionManager`.
- **Frontend Performance:** Implemented schema filtering in HITL dialogs to prevent fetching entire database schemas.
- **Security & UX:** Added client-side SQL validation in approval dialogs (emptiness check and syntax warnings).
- **Robustness:** Improved SSE connection handling with header-based authentication and async iterators.

**Phase 1 - Critical Fixes:**
- Removed Unicode emoji from logging (Windows compatibility)
- Enhanced HITL clarification dialog with structured unmapped concepts
- Added row count visibility (requested vs returned)
- Normalized SSE status for consistency

**Phase 2 - UI/UX Enhancements:**
- Reduced typography scale by 1-2px for better space utilization
- Increased message bubble max-width to 98%
- Improved word-wrap with break-word
- Optimized chat layout and spacing

**Phase 4 & 5 - Quality & Cleanup:**
- Organized test files into `tests/integration/` and `tests/fixtures/`
- Added `logs_fetched.txt` to .gitignore
- Cleaned up root directory clutter

## Current Limitations

- SQLcl process pool (default 2 workers) can still be a concurrency bottleneck under heavy load
- Circuit breaker currently applied only to SQL execution; other dependencies still rely on basic retry behavior
- Basic error recovery (no auto-restart of failed workers or external services)
- Test coverage is still below target; more integration and end-to-end tests are being added
- The active orchestrator is modularized under `app/orchestrator/`; legacy monolithic orchestrator files have been removed as part of cleanup

## Next Phase

1. Tune SQLcl process pool size and limits based on load testing (target 3-5 workers)
2. Extend circuit breaker coverage and resilience policies beyond SQL execution
3. Add/iterate Grafana dashboards and alerts
4. Load testing (50+ concurrent) and profiling
5. UI enhancements (tabs, Monaco editor, exports)
6. Expand pytest-based integration tests (API contracts, SSE streaming, health profiles, Graphiti paths)