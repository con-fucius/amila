"""
Orchestrator Node: Error Handling
"""

import logging
import asyncio
from datetime import datetime, timezone

from app.orchestrator.state import QueryState

# SSE state management
try:
    from app.services.query_state_manager import QueryState as ExecState
except Exception:
    ExecState = None

from app.orchestrator.utils import emit_state_event, update_node_history

import json
import re

logger = logging.getLogger(__name__)

ERROR_TAXONOMY_MAP = {
    "ORA-00942": {
        "category": "schema",
        "title": "Table or View Does Not Exist",
        "hint": "The query refers to a table that doesn't exist in the current schema or you lack permissions.",
        "steps": ["Verify table name", "Check schema permissions", "Ensure connection is to the correct environment"]
    },
    "ORA-00904": {
        "category": "syntax",
        "title": "Invalid Identifier",
        "hint": "A column name is misspelled or needs double quotes (e.g., if it's a reserved word like 'DATE').",
        "steps": ["Check column name spelling", "Wrap reserved words in double quotes", "Verify column belongs to the table"]
    },
    "ORA-00933": {
        "category": "syntax",
        "title": "SQL Command Not Properly Ended",
        "hint": "The SQL query has a syntax error at the end, possibly a trailing semicolon or misplaced clause.",
        "steps": ["Remove trailing semicolons", "Check ORDER BY or FETCH FIRST placement"]
    },
    "ORA-01017": {
        "category": "auth",
        "title": "Authentication Failed",
        "hint": "Invalid username or password for the database connection.",
        "steps": ["Check credentials in system settings", "Verify environment variables"]
    },
    "SQL injection": {
        "category": "security",
        "title": "Security Violation",
        "hint": "The query triggered a security filter for potential SQL injection patterns.",
        "steps": ["Contact administrator", "Avoid suspicious keywords in search"]
    },
    "quota exceeded": {
        "category": "limit",
        "title": "Usage Limit Exceeded",
        "hint": "You have exceeded your daily query allowance.",
        "steps": ["Wait for quota reset", "Contact admin to upgrade limits"]
    }
}


async def error_node(state: QueryState) -> QueryState:
    """Error handling node with node history tracking"""
    logger.error(f"Error state reached: {state.get('error', 'Unknown error')}")
    
    state["current_stage"] = "error"
    
    # Track node execution for reasoning visibility
    error_msg = state.get('error', 'Unknown error')
    
    # Map to taxonomy
    taxonomy = {
        "category": "generic",
        "title": "Unexpected Error",
        "hint": "An unclassified error occurred during query processing.",
        "steps": ["Retry the query", "Check system status"]
    }
    
    for pattern, info in ERROR_TAXONOMY_MAP.items():
        if pattern.lower() in error_msg.lower():
            taxonomy = info
            break
            
    state["error_taxonomy"] = taxonomy
    
    await update_node_history(state, "error", "completed", 
        thinking_steps=[
            {"id": "step-1", "content": f"Mapped error to taxonomy: {taxonomy['title']}", "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat()}
        ],
        error=error_msg
    )
    
    # Stream lifecycle: error
    try:
        if ExecState:
            await emit_state_event(state, ExecState.ERROR, {"error": error_msg})
    except Exception:
        pass
    
    state["next_action"] = "end"
    return state
