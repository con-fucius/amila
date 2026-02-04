# Amila

AI-powered business intelligence agent with natural language query orchestration.

## 🚀 Overview

Amila is an advanced autonomous data engineering and BI system designed for high-concurrency, multi-dialect environments. It orchestrates complex 15-node LangGraph workflows to translate natural language into secure, validated, and optimized SQL across multiple data platforms.

## 🛠️ Stack & Capabilities

- **Orchestration**: LangGraph (Intent → Context → SQL → Validate → Execute)
- **Databases**: Oracle (SQLcl MCP), Apache Doris (HTTP MCP), PostgreSQL
- **LLM Support**: Gemini (default), Bedrock, Mistral, Qwen, OpenRouter
- **Security**: RBAC, DLP, Native Audit, HMAC Signing, CSRF Protection, adaptive HITL
- **Observability**: OpenTelemetry, Langfuse, Prometheus, Grafana

## 🏁 Quick Start

For detailed step-by-step instructions, see the [Full Startup Guide](./STARTUP_GUIDE.md).

```powershell
# 1. Start core infrastructure (DBs + Obs)
docker-compose up -d redis falkordb doris oracle postgres otel-collector prometheus grafana

# 2. Populate databases (using virtual environment)
.\venv_populate\Scripts\activate
python scripts/populate_all_dbs.py --reset

# 3. Start services
cd backend && uv run python main.py
cd frontend && pnpm dev
```

## 🔐 Environment Configuration

Ensure your `.env` or `.env.docker` files are populated with these mandatory keys:

| Category | Variable | Requirement |
| :--- | :--- | :--- |
| **Auth** | `JWT_SECRET_KEY` | Min 32 chars |
|  | `JWT_REFRESH_SECRET_KEY` | Min 32 chars |
| **Security** | `HMAC_SECRET_KEY` | Min 32 chars |
|  | `ENCRYPTION_KEY` | Min 16 chars (32+ recommended) |
| **Oracle** | `ORACLE_PASSWORD` | Strong password (e.g. Amila!2026) |
| **LLM** | `GOOGLE_API_KEY` | Required for Gemini (Graphiti/Query) |

> [!NOTE]
> Doris uses a **demo** database schema (database) by default. The three substantial test tables (`TEST_INFORMAT...`, `TEST_AGG_EBU...`, `TEST_ALLOT...`) are created inside this schema. This name can be changed via `DORIS_DB_DATABASE`.

## 📊 Monitoring & Logs

- **Frontend**: [http://localhost:3000](http://localhost:3000)
- **Backend API**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Celery/Flower**: [http://localhost:5555](http://localhost:5555)
- **Prometheus**: [http://localhost:9090](http://localhost:9090)

**Check Service Health:**
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

**View Logs:**
```powershell
docker logs -f bi-agent-oracle
docker logs -f bi-agent-doris
```

## 📜 License

Proprietary © 2026 Amila Team
