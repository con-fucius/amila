# Scripts

## init_doris_data.py

Populates Doris with 10,000 sample CUSTOMER_DATA records.

```powershell
cd backend
.\.venv\Scripts\python.exe ..\scripts\init_doris_data.py
```

## start_services.ps1

Legacy Windows script that starts core Docker services (Redis, FalkorDB, Doris) and then expects you to run backend and frontend locally in separate terminals. For the primary Docker-based workflow (minimal/full/dev profiles), use the root-level `start.ps1` / `start.sh` scripts described in the main README.

```powershell
.\scripts\start_services.ps1
```

Options:
- `-SkipDataInit` - Skip database initialization
- `-ResetDoris` - Reset Doris container before starting
