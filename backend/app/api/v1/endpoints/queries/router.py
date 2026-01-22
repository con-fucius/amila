
from fastapi import APIRouter
from . import direct, process, approval, history, status, reporting, cancel

router = APIRouter()

# Include all sub-routers
# Note: tags are usually defined in the main router, but we can organize them here if needed.
# Since the main router likely prefixed this entire module with /queries, we assume these are root relative to that.

# Direct execution
router.include_router(direct.router, tags=["Queries - Direct"])

# Orchestrator
router.include_router(process.router, tags=["Queries - Orchestrator"])

# Approval
router.include_router(approval.router, tags=["Queries - Approval"])

# History & Status
router.include_router(history.router, tags=["Queries - History"])
router.include_router(status.router, tags=["Queries - Status"])

# Cancellation
router.include_router(cancel.router, tags=["Queries - Cancellation"])

# Reporting
router.include_router(reporting.router, tags=["Queries - Reporting"])
