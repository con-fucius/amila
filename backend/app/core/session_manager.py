
import asyncio
from typing import Dict, Any, Callable, Optional, Union
import logging

logger = logging.getLogger(__name__)

class SessionManager:
    """
    Singleton class to manage active query sessions and their cancellation handlers.
    This enables killing "zombie queries" by tracking active executions.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SessionManager, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if self.initialized:
            return
        # Stores metadata about the query (e.g., connection ID, process ID)
        self.active_queries: Dict[str, Any] = {}
        # Stores the callable function to cancel/kill the query
        self.cancellation_handlers: Dict[str, Callable] = {}
        self.initialized = True

    def register_query(self, query_id: str, metadata: Any, cancel_callback: Optional[Callable] = None):
        """
        Register a running query.
        
        Args:
            query_id: Unique identifier for the query
            metadata: Info identifying the query resource (e.g. {'type': 'oracle', 'pid': 123})
            cancel_callback: Function to call to cancel the query (async or sync)
        """
        if not query_id:
            return
            
        logger.debug(f"Registering query {query_id} type={metadata.get('type', 'unknown')}")
        self.active_queries[query_id] = metadata
        if cancel_callback:
            self.cancellation_handlers[query_id] = cancel_callback

    def unregister_query(self, query_id: str):
        """Remove a query from tracking (called when query completes naturally)"""
        if not query_id:
            return
            
        if query_id in self.active_queries:
            # logger.debug(f"Unregistering query {query_id}")
            del self.active_queries[query_id]
        if query_id in self.cancellation_handlers:
            del self.cancellation_handlers[query_id]

    def get_query_metadata(self, query_id: str) -> Optional[Any]:
        return self.active_queries.get(query_id)

    async def cancel_query(self, query_id: str) -> bool:
        """
        Execute the cancellation handler for a specific query.
        """
        logger.info(f"Requesting cancellation for query {query_id}")
        
        if query_id not in self.cancellation_handlers:
            logger.warning(f"No cancellation handler found for query {query_id}")
            return False

        try:
            handler = self.cancellation_handlers[query_id]
            if asyncio.iscoroutinefunction(handler):
                await handler()
            else:
                handler()
            
            # We don't verify success here, we assume the handler allows time for cleanup
            logger.info(f"Cancellation signal sent for query {query_id}")
            return True
        except Exception as e:
            logger.error(f"Error executing cancellation handler for query {query_id}: {e}")
            return False

# Global instance
session_manager = SessionManager()
