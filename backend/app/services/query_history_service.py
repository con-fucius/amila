"""
Query History Service
Maintains query history stack in Redis for undo/redo functionality
"""

import logging
import json
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


class QueryHistoryService:
    """Service for query history and undo/redo stack"""
    
    MAX_HISTORY_SIZE = 50  # Max queries per session
    
    @staticmethod
    async def push_query_state(
        session_id: str,
        user_query: str,
        sql_query: str,
        result_summary: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Push query state to history stack
        
        Args:
            session_id: Session identifier
            user_query: Natural language query
            sql_query: Generated SQL
            result_summary: Query result summary
            metadata: Additional metadata
            
        Returns:
            history_entry_id
        """
        entry_id = str(uuid.uuid4())
        
        entry = {
            "entry_id": entry_id,
            "session_id": session_id,
            "user_query": user_query,
            "sql_query": sql_query,
            "result_summary": result_summary or {},
            "metadata": metadata or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
        try:
            # Store entry
            await redis_client.set(f"history_entry:{entry_id}", entry, ttl=7*24*3600)
            
            # Push to session history list (most recent first)
            await redis_client._client.lpush(f"session:{session_id}:history", entry_id)
            
            # Trim to max size
            await redis_client._client.ltrim(
                f"session:{session_id}:history",
                0,
                QueryHistoryService.MAX_HISTORY_SIZE - 1
            )
            
            # Reset redo stack when new query is pushed
            await redis_client._client.delete(f"session:{session_id}:redo")
            
            logger.info(f"Pushed query to history stack: {entry_id}")
            return entry_id
            
        except Exception as e:
            logger.error(f"Failed to push query to history: {e}")
            return entry_id
    
    @staticmethod
    async def get_session_history(
        session_id: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get session query history
        
        Returns:
            List of history entries (most recent first)
        """
        try:
            entry_ids = await redis_client._client.lrange(
                f"session:{session_id}:history",
                0,
                limit - 1
            )
            
            history = []
            for eid in entry_ids:
                entry = await redis_client.get(f"history_entry:{eid}")
                if entry:
                    history.append(entry)
            
            return history
            
        except Exception as e:
            logger.error(f"Failed to get session history: {e}")
            return []
    
    @staticmethod
    async def undo(session_id: str) -> Optional[Dict[str, Any]]:
        """
        Undo last query (move from history to redo stack)
        
        Returns:
            Previous query state or None if nothing to undo
        """
        try:
            # Pop from history
            entry_id = await redis_client._client.lpop(f"session:{session_id}:history")
            
            if not entry_id:
                logger.info(f"Nothing to undo")
                return None
            
            # Push to redo stack
            await redis_client._client.lpush(f"session:{session_id}:redo", entry_id)
            await redis_client._client.ltrim(f"session:{session_id}:redo", 0, 49)
            
            # Get current state (new top of history)
            current_id = await redis_client._client.lindex(f"session:{session_id}:history", 0)
            
            if current_id:
                entry = await redis_client.get(f"history_entry:{current_id}")
                logger.info(f"Undo successful: restored {current_id}")
                return entry
            
            logger.info(f"Reached beginning of history")
            return None
            
        except Exception as e:
            logger.error(f"Undo failed: {e}")
            return None
    
    @staticmethod
    async def redo(session_id: str) -> Optional[Dict[str, Any]]:
        """
        Redo last undone query (move from redo stack back to history)
        
        Returns:
            Next query state or None if nothing to redo
        """
        try:
            # Pop from redo stack
            entry_id = await redis_client._client.lpop(f"session:{session_id}:redo")
            
            if not entry_id:
                logger.info(f"Nothing to redo")
                return None
            
            # Push back to history
            await redis_client._client.lpush(f"session:{session_id}:history", entry_id)
            
            # Return the re-applied entry
            entry = await redis_client.get(f"history_entry:{entry_id}")
            logger.info(f"Redo successful: restored {entry_id}")
            return entry
            
        except Exception as e:
            logger.error(f"Redo failed: {e}")
            return None
    
    @staticmethod
    async def can_undo(session_id: str) -> bool:
        """Check if undo is possible"""
        try:
            length = await redis_client._client.llen(f"session:{session_id}:history")
            return length > 1  # Need at least 2 entries (current + previous)
        except Exception:
            return False
    
    @staticmethod
    async def can_redo(session_id: str) -> bool:
        """Check if redo is possible"""
        try:
            length = await redis_client._client.llen(f"session:{session_id}:redo")
            return length > 0
        except Exception:
            return False
    
    @staticmethod
    async def clear_history(session_id: str):
        """Clear session history and redo stack"""
        try:
            await redis_client._client.delete(f"session:{session_id}:history")
            await redis_client._client.delete(f"session:{session_id}:redo")
            logger.info(f"Cleared history for session: {session_id}")
        except Exception as e:
            logger.error(f"Failed to clear history: {e}")
