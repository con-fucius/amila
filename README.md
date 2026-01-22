# Amila

**Natural Language Database Intelligence Platform**

Amila is an AI-powered business intelligence agent that orchestrates natural language queries into safe, executed SQL operations across multiple database engines. It leverages LangGraph for complex reasoning, MCP for standardized tool access, and a modern React frontend for interaction.

## Key Capabilities

*   **Intelligent Orchestration**: Multi-stage LangGraph workflow (Intent → Understand → Decompose → SQL Gen → Validate → Execute).
*   **Multi-Database Support**: 
    *   **Oracle**: via SQLcl MCP (Process Pool).
    *   **Doris**: via HTTP MCP.
    *   **PostgreSQL**: via `psycopg3` (Read-Only enforced).
*   **Flexible LLM Support**:  Mistral (default), Gemini, Bedrock, Qwen, OpenRouter.
*   **Enterprise Hardening**:
    *   Full System Diagnostics & connection pool monitoring.
    *   End-to-end Query Pipeline Tracing.
    *   Langfuse Observability integration.
    *   Robust Audit Logging & Error Normalization.
*   **Interactive Frontend**: React/Vite UI with Monaco SQL editor, Plotly visualizations, and Schema Browser.

## Architecture

### Backend (FastAPI + LangGraph)
*   **Orchestrator**: Manages state, validation loops, and HITL (Human-in-the-Loop) approvals.
*   **Services**: 
    *   `DatabaseRouter`: Routes queries to appropriate engines.
    *   `SkillsLoader`: Injects YAML-based SQL skills and rules.
    *   `DiagnosticService`: Real-time system health probing.
*   **Infrastructure**: Redis (Cache/Session), Graphiti (FalkorDB context), Celery (Async tasks).

### Frontend (React + TypeScript)
*   **RealChatInterface**: Split-pane view for chat and reasoning.
*   **QueryBuilder**: Advanced SQL editor with result visualization.
*   **Visualizations**: Auto-generated Plotly charts based on data types.

## Quick Start

### Prerequisites
*   Docker Desktop
*   Python 3.12+ (managed via `uv`)
*   Node.js 18+ (managed via `pnpm`)

### 1. Configuration
Copy `.env.example` to `.env` in both root and `backend/` directories.

```bash
# Key variables
QUERY_LLM_PROVIDER=mistral  # or gemini, bedrock, qwen
MISTRAL_API_KEY=your_key
POSTGRES_ENABLED=true
```

### 2. Oracle Setup (Optional)
Create `.dbtools/connections.json` with your Oracle connection strings. See `.dbtools/README.md`.

### 3. Launch
Start the minimal development stack:

```bash
docker compose up -d backend frontend redis falkordb doris
```

**Access Points:**
*   **Frontend**: [http://localhost:3000](http://localhost:3000)
*   **Backend API**: [http://localhost:8000/docs](http://localhost:8000/docs)
*   **Diagnostics**: [http://localhost:8000/api/v1/diagnostics/status](http://localhost:8000/api/v1/diagnostics/status)

## Observability

Amila includes a comprehensive observability stack (disabled by default for speed).

To enable full monitoring (Prometheus, Grafana, OpenTelemetry):
```bash
docker compose --profile full up -d
```

## Development

*   **Backend**: `cd backend && uv sync && uvicorn app.main:app --reload`
*   **Frontend**: `cd frontend && pnpm install && pnpm dev`
*   **Monitoring**: See `/api/v1/diagnostics/status` for system health.
