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

- **Backend (FastAPI + LangGraph)**: Multi-stage orchestrator (Intent → SQL Gen → HITL → Execute). Includes `DatabaseRouter`, `SkillsLoader`, and `DiagnosticService`.
- **Frontend (React + TypeScript)**: Interactive interface with Monaco editor, Plotly charts, and Schema Browser.
- **Design System**: Compact, high-density enterprise UI with prioritized typography (`Figtree`, `Segoe UI`, `Cantarell`).

## Setup Guide

Follow these steps meticulously to start the Amila platform. All commands should be run from the project root directory unless specified otherwise.

### 1. Start Infrastructure Services
Start the core databases and monitoring infrastructure using Docker Compose. Ensure Docker Desktop is running before proceeding.
Run the commands in project root directory.
```powershell
# Command to start Redis, FalkorDB, Doris, Postgres, Otel-Collector, Grafana, Prometheus, and Oracle
docker-compose --profile full --profile observability up -d redis falkordb doris postgres otel-collector grafana prometheus oracle
```

> [!IMPORTANT]
> Initializing Oracle and Doris can take up to 60 seconds. Wait until the containers are reported as `healthy` in Docker or via `docker-compose ps`.

### 2. Populate Databases
Populate the Doris and Postgres databases with sample data using the unified population script. This requires a Python virtual environment with backend dependencies installed.

```powershell
# 1. Initialize backend virtual environment (if not already done)
cd backend
uv sync --all-extras
.\.venv\Scripts\activate

# 2. Run the unified population script from project root
cd ..
python scripts/populate_all_dbs.py --reset
```

### 3. Start the Backend
Start the FastAPI backend orchestrator in a new terminal.

```powershell
# Access backend directory and activate environment
cd backend
.\.venv\Scripts\activate

# Start the FastAPI server on port 8000
python main.py
```

### 4. Start the Frontend
Start the React development server in a new terminal.

```powershell
# Access frontend directory and start Dev server
cd frontend
npm install
npm run dev
```
The UI will be available at [http://localhost:3000](http://localhost:3000).

### 5. Start Celery
Start the Celery worker to handle background tasks and report exports. Run this in a new terminal.

```powershell
# Access backend directory and activate environment
cd backend
.\.venv\Scripts\activate

# Start Celery worker with solo pool for Windows compatibility
celery -A app.core.celery_app worker --loglevel=info --pool=solo
```

---

## Important Usage Notes

- **Diagnostics**: Once all services are running, verify system health at [http://localhost:3000/settings](http://localhost:3000/settings) or via the API check: `GET http://localhost:8000/api/v1/health`.
- **Port Cleanup**: If you encounter "Port already in use" errors, use the cleanup utility:
  ```powershell
  .\scripts\fix_startup.ps1
  ```

---

## Important Notes

- **Local Development**: Because you are running the backend manually, ensure your `.env` file reflects `localhost` or `127.0.0.1` for service hosts (Redis, Doris, etc.) rather than the Docker service names.
- **Langfuse Cloud**: If using Langfuse Cloud, ensure your keys are set in `.env`.

## Access Points

| Component | URL |
|-----------|-----|
| **Frontend UI** | [http://localhost:3000](http://localhost:3000) |
| **Backend API** | [http://localhost:8000](http://localhost:8000) |
| **API Documentation** | [http://localhost:8000/docs](http://localhost:8000/docs) |
| **Flower (Celery Monitor)** | [http://localhost:5555](http://localhost:5555) |

## Troubleshooting

### Container & Network Issues

**Error:** `"Error response from daemon: failed to set up container networking: network '______' not found"` or `"Network bi-agent-network Resource is still in use"`

**Solution:**
Identify the running container, stop it, and remove it before attempting to remove the network.
1.  **Stop and remove specific containers**: `docker stop bi-agent-postgres` then `docker rm bi-agent-postgres`
2.  **Stop and remove all Amila-related containers**:
    ```bash
    docker rm -f bi-agent-backend bi-agent-redis bi-agent-falkordb bi-agent-doris bi-agent-postgres bi-agent-prometheus bi-agent-otel-collector bi-agent-grafana bi-agent-celery-worker bi-agent-flower
    ```
3.  **Remove the network**: `docker network rm bi-agent-network`

**Error:** `"Error response from daemon: error while removing network: ... has active endpoints"`

**Solution:**
1.  **Stop and remove offending containers**: `docker stop bi-agent-prometheus bi-agent-grafana bi-agent-postgres`
2.  **Clean up**: `docker rm bi-agent-prometheus bi-agent-grafana bi-agent-postgres`
3.  **Remove network**: `docker network rm bi-agent-network`

### Database-Specific Issues

**Oracle: `ORA-01017: invalid username/password; logon denied`**
- **Cause**: Script is using hardcoded defaults or the wrong service name.
- **Solution**: Ensure your `.env` file is in the root directory and contains correct `ORACLE_USERNAME`, `ORACLE_PASSWORD`, and `ORACLE_SERVICE_NAME` (use `FREEPDB1` for 23ai). The initialization scripts now use `load_dotenv()` to automatically pick up these settings.

**Oracle: `ORA-28000: The account is locked`**
- **Cause**: Too many failed login attempts during setup.
- **Solution**: Unlock the account directly via `docker exec`:
  ```bash
  "ALTER USER system ACCOUNT UNLOCK; ALTER USER system IDENTIFIED BY password; EXIT;" | docker exec -i bi-agent-oracle sqlplus / as sysdba
  ```

### Database Provisioning

**Unified Database Provisioner** (`scripts/populate_all_dbs.py`):
1. **Start Services**: `docker-compose --profile full up -d`
2. **Wait ~60s**: Allow Oracle and Doris to initialize.
3. **Provision Data**: Use the backend virtual environment:
   ```powershell
   cd backend
   uv sync
   .\.venv\Scripts\activate
   cd ..
   python scripts/populate_all_dbs.py --reset
   ```
- This script populates **10,000 rows** across **full year 2025** into Oracle, Doris, and Postgres simultaneously.

**Oracle: `DPY-6001: Service "XE" not found`**
- **Cause**: Incorrect service name for the 23ai Free edition.
- **Solution**: Set `ORACLE_SERVICE_NAME=FREEPDB1` in your `.env`.

**Frontend: Fonts not rendering correctly**
- **Note**: The system prioritizes `Figtree` for UI and `Cantarell`/`Consolas` for SQL. Ensure you have an internet connection for Google Fonts or have these fonts installed locally.

