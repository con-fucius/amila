# Amila

Natural language database queries via LangGraph orchestrator.

## System Map & Flow

### Summary
*   **Backend**: FastAPI + LangGraph Orchestrator, Celery workers, MCP Client Registry.
*   **Core**: Redis (Cache/Session/Vector), Graphiti (FalkorDB), SQLcl (Process Pool), Doris (HTTP MCP).
*   **Orchestrator**: Intent → Understand → Context → Decompose → Hypothesis → SQL Gen (Skills) → Validation → Approval (HITL) → Execution → Results → Pivot/Repair.
*   **Frontend**: React/TS/Vite, Global DB Selector, Auth Guard, Chat/SQL Builder/Schema Browser, Plotly Viz.
*   **Services**: Conversation Router (LLM/Regex), Skills Loader (YAML), Report Gen (HTML/PDF/DOCX), Schema Enrichment.

### Details

#### Backend (FastAPI, Python 3.11)
*   **Entry**: `main.py` → `create_application()` → `lifespan` (Init: Redis, SQLcl Pool, Doris MCP, Graphiti, Orchestrator).
*   **Middleware**: Request Logging, CORS, Security Headers, Rate Limiting, Audit Logging.
*   **Config**: `.env` managed via `AppSettings` (Feature flags: Skills, Caching, Tracing).
*   **Registry**: Manages lifecycle of MCP clients (SQLcl, Doris) and Graphiti connection.

#### API (`/api/v1`)
*   **Auth**: `/login`, `/refresh`, `/logout` (JWT + Dev Bypass).
*   **Queries**:
    *   `/process`: Orchestrator entry point (Natural Language).
    *   `/submit`: Direct SQL execution (Analyst/Admin).
    *   `/{id}/approve`: HITL state resumption.
    *   `/{id}/stream`: SSE state streaming.
*   **Features**:
    *   `/report`: Generates HTML/PDF/DOCX Executive Reports.
    *   `/visualize`: Generates Plotly JSON configurations.
    *   `/clarify`: Handling multi-turn clarifications with context.

#### Core Execution
*   **Oracle**: JSON-RPC via `sqlcl_pool` (STDIO). Dialect support: `FETCH FIRST`, `NVL`, `SYSDATE`.
*   **Doris**: HTTP via `doris-mcp-server`. Dialect support: `LIMIT`, `IFNULL`, `NOW`.
*   **Conversation Router**: Hybrid LLM + Regex classification. Categories: Greeting, Metadata, Data Query, Refinement.
*   **Skills System**: YAML-based prompt injection (`oracle_query_skills`, `doris_query_skills`). Rules, Anti-patterns, Mappings.
*   **Traceability**: Langfuse integration + Structured Logging + Audit Logs.

#### Orchestrator (LangGraph)
*   **Nodes**: `understand` → `context` (Graphiti) → `decompose` (Multi-step) → `hypothesis` → `sql_generation` → `validation` (Security/Syntax) → `execution` → `results` → `pivot` (Format).
*   **Resilience**: `repair` nodes for SQL errors, `fallback` strategies, `approval` gates for high-risk queries.
*   **State**: Persistent checkpoints via `AsyncSqliteSaver` (`checkpoints.db`).

#### Frontend (Vite + React + TS)
*   **Entry**: `main.tsx` → `App.tsx` (Providers: Auth, Theme, Query).
*   **State**: `chatStore.ts` (Global DB selection, History), `useAuth` (JWT state).
*   **Components**:
    *   `RealChatInterface`: Split view (Chat + Reasoning/Viz).
    *   `QueryBuilder`: Monaco Editor + Result Table.
    *   `SchemaBrowser`: Tree view + Sample Data/Stats tabs.
    *   `ReportGenerator`: Export modal with preview.
*   **Visualization**: Interactive Plotly charts (`PlotlyChart` component).

### User Request Flow
1.  **Startup**: Init Redis & Graphiti → Warm SQLcl Pool → Check Doris MCP → Register Orchestrator.
2.  **Submission**:
    *   **Router**: Classify Intent (LLM/Regex).
    *   **Conversational**: Return direct response (Greeting/Help).
    *   **Data Query**: Pass to Orchestrator (`/process`).
3.  **Orchestration**:
    *   Load Context (Schema/History) & Skills.
    *   Generate SQL (LLM + Dialect Rules).
    *   **Validate**: SQL Injection Check + Syntax (SQLValidator).
    *   **Gate**: If Risk > Medium → Pause for HITL (`/approve`).
    *   **Execute**: Route to Oracle/Doris → Audit Log.
    *   **Result**: Decorate with Viz Hints & Insights.
4.  **Output**:
    *   Stream updates via SSE.
    *   Frontend renders Reasoning Steps + Data Table + Quick Actions (Chart/Report).

## Flow Mapping & User Journeys

### Query Processing Flow (Success Path)

**FE**: User submits NL query → `apiService.submitQuery()` → `POST /api/v1/queries/process`
**BE**: `process_query_with_orchestrator()` → validates auth/RBAC → rate limiting → input sanitization
**BE**: Pre-registers query with QueryStateManager (fixes race condition)
**BE**: `ConversationRouter.route_with_context()` → classifies intent
**Orchestrator**: `START` → `understand` → `retrieve_context` → `decompose` → `hypothesis` → `generate_sql`
**Orchestrator**: `validate` → `await_approval` (HITL interrupt)
**FE**: SSE stream receives `PENDING_APPROVAL` → shows HITLApprovalDialog
**FE**: User approves → `POST /api/v1/queries/{id}/approve`
**BE**: Validates ownership → resumes graph → `probe_sql` → `execute` → `validate_results` → `format_results` → `END`
**FE**: SSE receives `FINISHED` → displays results in QueryResultsTable

### Error Paths

**Auth failures**: 401 at RBAC layer → FE token refresh → retry or redirect to login
**Rate limit**: 429 from rate_limiter → FE shows error, retry-after header
**Validation errors**: 400 from input sanitization → FE displays error message
**SQL generation failure**: Orchestrator routes to error_node → SSE emits ERROR state
**Execution failure**: `normalize_database_error()` categorizes → `repair_sql` (2 attempts) → `fallback_sql` (1 attempt) → error
**HITL rejection**: User rejects → graph routes to rejected → END
**Timeout**: 600s timeout → 504 response

### Database-Specific Paths

**Oracle**: `DatabaseRouter.execute_sql()` → `QueryService.execute_sql_query()` → SQLcl pool → MCP client
**Doris**: `DatabaseRouter.execute_sql()` → `DorisQueryService.execute_sql_query()` → Doris MCP client

## Prerequisites

- Python 3.12
- Node.js 18+
- Docker Desktop
- uv (Python package manager)
- pnpm (Node.js package manager)

## Quick Start

### 1. Configure Environment

- Copy `.env.example` at the repository root to `.env` and set values for your environment.
- Copy `backend/.env.example` to `backend/.env` for backend-specific configuration.
- Do not commit any populated `.env*` files.

### 2. Configure Oracle Connections (SQLcl)

The `.dbtools/` directory contains SQLcl connection configurations for Oracle database access. This directory is not committed to the repository for security reasons.

**Setup Steps:**

1. Create the `.dbtools/` directory in the project root:
   ```bash
   mkdir .dbtools
   ```

2. Create a `connections.json` file with your Oracle connection details. You can either:
   - Copy from your local SQLcl installation (`~/.dbtools/connections.json` on Linux/Mac or `%USERPROFILE%\.dbtools\connections.json` on Windows)
   - Create connections using SQLcl: `conn -save ConnectionName user/password@host:port/service`

3. Ensure the connection name matches `ORACLE_DEFAULT_CONNECTION` in your `.env` file.

See `.dbtools/README.md` for detailed setup instructions and troubleshooting.

### 3. Start with Docker
Start minimal build:

```bash
docker compose up -d --build backend frontend redis falkordb
```

### 4. Access Points (Docker)

| Service      | URL                       |
|-------------|---------------------------|
| Frontend    | http://localhost:3000     |
| Backend API | http://localhost:8000     |
| API Docs    | http://localhost:8000/docs |
| Doris Web UI| http://localhost:8030     |

## Environment Variables

Copy `.env.example` to `.env` in `backend/` and populate:

```bash
GOOGLE_API_KEY=xxx
JWT_SECRET_KEY=xxx
REDIS_PASSWORD=xxx
ORACLE_HOST=
ORACLE_PORT=
ORACLE_SERVICE_NAME=
ORACLE_USERNAME=xxx
ORACLE_PASSWORD=xxx
```

## Startup Procedures

### 1. Minimal / UAT Build (Fastest)
Starts only the essential services (Frontend, Backend, Redis, FalkorDB, Doris). Recommended for UAT and active development.

```bash
docker compose build --no-cache backend frontend && docker compose up -d backend frontend redis falkordb doris
```

### 2. Full System Build
Starts the complete stack including Observability (Prometheus, Grafana, OpenTelemetry) and Async Workers (Celery, Flower).

```bash
docker compose --profile full up -d --build
```

## Developer Tips for Speed

### Faster Re-deployments
To avoid rebuilding unchanging layers, omit `--no-cache` after the initial build:
```bash
docker compose up -d --build backend frontend
```

### Hot Reloading (Volume Mounting)
The `docker-compose.yml` configures volume mounts for active development. Code changes in these directories are reflected immediately inside the containers without rebuilding:
*   **Backend**: `./backend/app` maps to `/app/app` (FastAPI auto-reloads).
*   **Frontend**: Mounts are configured for Vite HMR (Hot Module Replacement).

### Using the Minimal Compose File
For an isolated minimal environment (excluding config for full-stack services completely):
```bash
docker compose -f docker-compose.minimal.yml up -d --build
```
