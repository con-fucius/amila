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

### 1. Setup Doris MCP Server
```powershell
cd backend/services/doris-mcp-server
uv venv
uv pip install -r requirements.txt
```

### 2. Start Docker Services
```powershell
docker-compose up -d redis falkordb doris
```

### 3. Initialize Doris Database
```powershell
cd backend
.\.venv\Scripts\python.exe ..\scripts\init_doris_data.py
```

### 4. Start Backend
```powershell
cd backend
.venv\Scripts\Activate.ps1
python main.py
```

### 5. Start Frontend
```powershell
cd frontend
pnpm install
pnpm dev
```

## Access Points

| Service | URL |
|---------|-----|
| Frontend | http://localhost:5173 |
| Backend API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Doris Web UI | http://localhost:8030 |

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
