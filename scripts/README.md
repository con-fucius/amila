# BI Agent MVP - Startup Guide

## Quick Start (Automated)

Run the PowerShell startup script:

```powershell
cd path\to\bi-agent-mvp
.\scripts\start_services.ps1
```

Options:
- `-SkipDataInit` - Skip database initialization (use if data already loaded)
- `-ResetDoris` - Reset Doris container before starting

## Manual Startup (Step-by-Step)

### Step 1: Start Docker Services

```powershell
cd path\to\bi-agent-mvp
docker-compose up -d redis falkordb doris
```

### Step 2: Wait for Doris to be Healthy

Check status (wait until it shows "healthy"):
```powershell
docker ps --filter "name=doris" --format "table {{.Names}}\t{{.Status}}"
```

This typically takes 2-3 minutes. You can watch the logs:
```powershell
docker logs bi-agent-doris -f
```

### Step 3: Initialize Database with Sample Data

**Important:** Since Doris uses tmpfs (in-memory storage), data is lost on container restart. Run this after every Doris restart.

```powershell
cd path\to\bi-agent-mvp

# Install dependencies if needed
pip install pymysql faker

# Run initialization script
python scripts/init_doris_data.py
```

This creates:
- `demo` database
- `CUSTOMER_DATA` table with 10,000 records

### Step 4: Start Backend

```powershell
cd path\to\bi-agent-mvp\backend
uv run python main.py
```

Wait for: `Doris MCP initialized successfully`

### Step 5: Start Frontend

```powershell
cd path\to\bi-agent-mvp\frontend
pnpm dev
```

### Step 6: Test

1. Open http://localhost:3000
2. Ensure database selector shows "Doris"
3. Try a query: `Show me the first 5 rows of CUSTOMER_DATA`

## Troubleshooting

### Doris stuck at "unhealthy"

```powershell
# Reset and restart
docker stop bi-agent-doris
docker rm bi-agent-doris
docker-compose up -d doris
# Wait 2-3 minutes, then check status
```

### "Connection pool is not available" error

The Doris MCP server can't connect to Doris DB. Either:
1. Doris isn't healthy yet - wait and check `docker ps`
2. Database not initialized - run `init_doris_data.py`
3. Restart backend after initializing data

### Backend shows "doris_mcp: connected" but queries fail

Restart the backend to reinitialize the MCP connection:
```powershell
# Ctrl+C to stop backend, then:
uv run python main.py
```

## Service Ports

| Service | Port | URL |
|---------|------|-----|
| Frontend | 3000 | http://localhost:3000 |
| Backend | 8000 | http://localhost:8000 |
| Backend Docs | 8000 | http://localhost:8000/docs |
| Doris FE Web | 8030 | http://localhost:8030 |
| Doris MySQL | 9030 | mysql -h127.0.0.1 -P9030 -uroot |
| Redis | 6379 | - |
| FalkorDB | 6380 | - |
| Doris MCP | 8808 | http://localhost:8808 |

## Sample Queries

After setup, try these queries in the chat:

1. **Simple select:**
   > Show me the first 5 rows of CUSTOMER_DATA

2. **Aggregation:**
   > What is the total revenue by segment?

3. **Filtering:**
   > List all customers in the CORPORATE segment

4. **Complex:**
   > Show monthly revenue trends for ICT_SOLUTIONS product
