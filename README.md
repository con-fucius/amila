# Amila

Natural language database queries via LangGraph orchestrator.

## Stack

- **Backend:** FastAPI, LangGraph, Redis, Celery
- **Frontend:** React, TypeScript, Vite
- **AI:** AWS Bedrock (prod), Google Gemini (dev)
- **Databases:** Oracle, Doris
- **Knowledge Graph:** Graphiti + FalkorDB

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

### 2. Start with Docker (recommended)

**Windows (PowerShell):**

```powershell
./start.ps1 minimal   # backend, frontend, redis, falkordb
./start.ps1 full      # + doris, celery-worker, flower, otel-collector, prometheus, grafana
./start.ps1 dev       # essential services + LangGraph Studio + observability
```

**macOS / Linux:**

```bash
./start.sh minimal
./start.sh full
./start.sh dev
```

**Minimal UAT stack (Docker Compose file only):**

```bash
docker compose -f docker-compose.minimal.yml up --build
```

### 3. Access Points (Docker)

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
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=XEPDB1
ORACLE_USERNAME=xxx
ORACLE_PASSWORD=xxx
```

## Query Examples

```
"Show top 10 customers by revenue"
"Total revenue by segment"
"List all products"
```

## API Endpoints

- `POST /api/v1/queries/process` - Natural language query
- `POST /api/v1/queries/submit` - Direct SQL
- `POST /api/v1/queries/{id}/approve` - HITL approval
- `GET /api/v1/queries/{id}/stream` - SSE streaming
- `GET /api/v1/schema/` - Schema info
- `GET /health` - Health check
