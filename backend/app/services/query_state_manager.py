"""
Query State Manager Service
Tracks query execution lifecycle and enables SSE streaming of state changes

Provides:
- Persistent query metadata storage (user ownership, timestamps)
- Real-time SSE streaming via async generators
- Thread-safe singleton pattern
"""

import asyncio
import logging
from typing import Dict, Optional, Set, AsyncGenerator
from enum import Enum
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
import json

logger = logging.getLogger(__name__)


class QueryState(str, Enum):
    """Query execution lifecycle states"""
    RECEIVED = "received"
    PLANNING = "planning"
    PREPARED = "prepared"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTING = "executing"
    FINISHED = "finished"
    ERROR = "error"


@dataclass
class QueryMetadata:
    """
    Persistent metadata for a query - used for authorization and status tracking
    """
    query_id: str
    user_id: str
    username: str
    session_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "received"
    database_type: Optional[str] = None
    trace_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class QueryStateEvent:
    """
    Query state change event with detailed progress information
    """
    query_id: str
    state: QueryState
    timestamp: str
    metadata: Optional[Dict] = None
    
    # Agent progress details
    thinking_steps: Optional[list] = None  # [{"id", "content", "status", "timestamp"}]
    todo_items: Optional[list] = None       # [{"id", "title", "status", "details"}]
    discoveries: Optional[Dict] = None       # {"tables": [...], "columns": [...], "mappings": []}
    
    # Top-level result payload for frontend consumption
    result: Optional[Dict] = None
    insights: Optional[list] = None
    suggested_queries: Optional[list] = None
    sql: Optional[str] = None
    
    # Database context for frontend error handling
    database_type: Optional[str] = None
    
    def to_sse_message(self) -> str:
        """Convert to SSE message format"""
        data = asdict(self)
        return f"data: {json.dumps(data)}\n\n"
class QueryStateManager:
    """
    Manages query execution states and enables SSE streaming
    Thread-safe singleton for tracking query lifecycle
    
    Features:
    - Persistent query metadata for authorization (user ownership)
    - Real-time SSE streaming via async generators
    - Automatic cleanup of expired queries
    """
    _instance: Optional['QueryStateManager'] = None
    _lock = asyncio.Lock()
    
    # TTL for query metadata (24 hours)
    METADATA_TTL_SECONDS = 86400
    
    def __init__(self):
        # Query state storage: query_id -> current state
        self._query_states: Dict[str, QueryState] = {}
        
        # Persistent query metadata: query_id -> QueryMetadata
        self._query_metadata: Dict[str, QueryMetadata] = {}
        
        # Event queues for SSE subscribers: query_id -> set of queues
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        
        # Locks for thread-safe operations
        self._state_lock = asyncio.Lock()
        self._metadata_lock = asyncio.Lock()
        self._subscriber_lock = asyncio.Lock()
        
        logger.info("QueryStateManager initialized")
    
    @classmethod
    async def get_instance(cls) -> 'QueryStateManager':
        """Get or create singleton instance"""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    async def register_query(
        self,
        query_id: str,
        user_id: str,
        username: str,
        session_id: Optional[str] = None,
        database_type: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> QueryMetadata:
        """
        Register a new query with ownership metadata.
        Must be called when a query is first submitted.
        
        Args:
            query_id: Unique query identifier
            user_id: User ID who owns this query
            username: Username for display/logging
            session_id: Optional session identifier
            database_type: Database type (oracle/doris)
            trace_id: Optional trace ID for observability
            
        Returns:
            QueryMetadata object
        """
        async with self._metadata_lock:
            metadata = QueryMetadata(
                query_id=query_id,
                user_id=user_id,
                username=username,
                session_id=session_id,
                database_type=database_type,
                trace_id=trace_id,
            )
            self._query_metadata[query_id] = metadata
            logger.info(f"Registered query {query_id[:8]}... for user {username}")
            return metadata
    
    async def get_query_metadata(self, query_id: str) -> Optional[Dict]:
        """
        Get persistent metadata for a query.
        Used for authorization checks and status retrieval.
        
        Args:
            query_id: Query identifier
            
        Returns:
            Dict with query metadata or None if not found
        """
        async with self._metadata_lock:
            metadata = self._query_metadata.get(query_id)
            if metadata:
                return metadata.to_dict()
            return None
    
    async def update_state(
        self,
        query_id: str,
        new_state: QueryState,
        metadata: Optional[Dict] = None
    ) -> None:
        """
        Update query state and notify all subscribers
        
        Args:
            query_id: Unique query identifier
            new_state: New state to transition to
            metadata: Optional metadata about the state change
        """
        async with self._state_lock:
            old_state = self._query_states.get(query_id)
            self._query_states[query_id] = new_state
            
            logger.info(
                f"Query {query_id[:8]}... state: {old_state} -> {new_state}"
            )
        
        # Update persistent metadata status
        async with self._metadata_lock:
            if query_id in self._query_metadata:
                self._query_metadata[query_id].status = new_state.value
                self._query_metadata[query_id].updated_at = datetime.now(timezone.utc).isoformat()
        
        # Create state change event
        # Attach trace_id if provided in metadata
        meta = dict(metadata) if metadata else {}
        trace_id = meta.get("trace_id")
        if not trace_id and "trace_id" in meta:
            trace_id = meta.get("trace_id")
        if "trace_id" not in meta:
            meta["trace_id"] = trace_id

        # Emit Langfuse event for state transition
        try:
            from app.core.langfuse_client import log_event

            if trace_id:
                log_event(
                    trace_id=trace_id,
                    name=f"sse_state.{new_state.value}",
                    input_data={
                        "query_id": query_id,
                        "previous_state": old_state.value if isinstance(old_state, QueryState) else old_state,
                    },
                    output_data=meta,
                    metadata={"source": "QueryStateManager"},
                )
        except Exception:
            # Observability should not break state updates
            pass
        
        # [RESULT_TRACE] Log result data in metadata
        result_data = meta.get("result")
        if result_data:
            if isinstance(result_data, dict):
                rows = result_data.get("rows", [])
                columns = result_data.get("columns", [])
                row_count = result_data.get("row_count", 0)
                logger.info(f"[RESULT_TRACE] query_state_manager.py: Result in metadata - columns={len(columns) if isinstance(columns, list) else 'invalid'}, rows={len(rows) if isinstance(rows, list) else 'invalid'}, row_count={row_count}")
            else:
                logger.warning(f"[RESULT_TRACE] query_state_manager.py: Result in metadata is not a dict: {type(result_data)}")
        
        event = QueryStateEvent(
            query_id=query_id,
            state=new_state,
            timestamp=datetime.now(timezone.utc).isoformat(),
            metadata=meta,
            thinking_steps=meta.get("thinking_steps"),
            todo_items=meta.get("todo_items"),
            discoveries=meta.get("discoveries"),
            result=meta.get("result"),
            insights=meta.get("insights"),
            suggested_queries=meta.get("suggested_queries"),
            sql=meta.get("sql"),
            database_type=meta.get("database_type"),
        )
        
        # Notify all subscribers for this query
        await self._notify_subscribers(query_id, event)
    
    async def _notify_subscribers(
        self,
        query_id: str,
        event: QueryStateEvent
    ) -> None:
        """Notify all SSE subscribers of state change"""
        async with self._subscriber_lock:
            if query_id not in self._subscribers:
                return
            
            # Send event to all subscriber queues
            dead_queues = set()
            for queue in self._subscribers[query_id]:
                try:
                    await asyncio.wait_for(
                        queue.put(event),
                        timeout=1.0
                    )
                except (asyncio.TimeoutError, asyncio.QueueFull):
                    logger.warning(
                        f"Failed to deliver event to subscriber for {query_id[:8]}"
                    )
                    dead_queues.add(queue)
                except Exception as e:
                    logger.error(f"Error notifying subscriber: {e}")
                    dead_queues.add(queue)
            
            # Clean up dead queues
            if dead_queues:
                self._subscribers[query_id] -= dead_queues
                logger.info(f"Removed {len(dead_queues)} dead subscribers")
    
    async def subscribe(self, query_id: str) -> AsyncGenerator[str, None]:
        """
        Subscribe to query state changes via SSE
        
        Args:
            query_id: Query ID to subscribe to
            
        Yields:
            SSE-formatted state change messages
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=50)
        
        # Register subscriber
        async with self._subscriber_lock:
            if query_id not in self._subscribers:
                self._subscribers[query_id] = set()
            self._subscribers[query_id].add(queue)
            logger.info(f"New SSE subscriber for query {query_id[:8]}...")
        
        try:
            # Send current state immediately if available
            current_state = self._query_states.get(query_id)
            if current_state:
                initial_event = QueryStateEvent(
                    query_id=query_id,
                    state=current_state,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    metadata={"initial": True}
                )
                yield initial_event.to_sse_message()
            
            # Stream state changes
            while True:
                try:
                    # Wait for new events with timeout
                    event = await asyncio.wait_for(
                        queue.get(),
                        timeout=30.0
                    )
                    yield event.to_sse_message()
                    
                    # Stop streaming after terminal states
                    if event.state in {
                        QueryState.FINISHED,
                        QueryState.ERROR,
                        QueryState.REJECTED
                    }:
                        logger.info(
                            f"Terminal state reached for {query_id[:8]}, closing stream"
                        )
                        break
                        
                except asyncio.TimeoutError:
                    # Send keep-alive comment to prevent connection timeout
                    yield ": keep-alive\n\n"
                    
        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for query {query_id[:8]}")
        except Exception as e:
            logger.error(f"Error in SSE stream: {e}", exc_info=True)
        finally:
            # Unregister subscriber
            async with self._subscriber_lock:
                if query_id in self._subscribers:
                    self._subscribers[query_id].discard(queue)
                    if not self._subscribers[query_id]:
                        del self._subscribers[query_id]
                        logger.info(f"No more subscribers for {query_id[:8]}")
    
    async def get_state(self, query_id: str) -> Optional[QueryState]:
        """Get current state of a query"""
        async with self._state_lock:
            return self._query_states.get(query_id)
    
    async def cleanup_query(self, query_id: str, preserve_metadata: bool = True) -> None:
        """
        Clean up query state after completion.
        
        Args:
            query_id: Query identifier
            preserve_metadata: If True, keeps metadata for status queries (default)
        """
        async with self._state_lock:
            if query_id in self._query_states:
                del self._query_states[query_id]
                logger.info(f"Cleaned up state for query {query_id[:8]}")
        
        if not preserve_metadata:
            async with self._metadata_lock:
                if query_id in self._query_metadata:
                    del self._query_metadata[query_id]
                    logger.info(f"Cleaned up metadata for query {query_id[:8]}")
    
    async def cleanup_expired_metadata(self) -> int:
        """
        Remove metadata older than TTL.
        Should be called periodically by a background task.
        
        Returns:
            Number of entries cleaned up
        """
        now = datetime.now(timezone.utc)
        expired_ids = []
        
        async with self._metadata_lock:
            for query_id, metadata in list(self._query_metadata.items()):
                try:
                    created = datetime.fromisoformat(metadata.created_at.replace('Z', '+00:00'))
                    age_seconds = (now - created).total_seconds()
                    if age_seconds > self.METADATA_TTL_SECONDS:
                        expired_ids.append(query_id)
                except Exception:
                    pass
            
            for query_id in expired_ids:
                del self._query_metadata[query_id]
        
        if expired_ids:
            logger.info(f"Cleaned up {len(expired_ids)} expired query metadata entries")
        
        return len(expired_ids)
    
    async def cleanup_terminal_states(self) -> int:
        """
        Remove query states that are in terminal states (finished, error, rejected)
        and have no active subscribers. Prevents unbounded _query_states growth.
        
        Returns:
            Number of entries cleaned up
        """
        terminal_states = {QueryState.FINISHED, QueryState.ERROR, QueryState.REJECTED}
        cleaned_ids = []
        
        async with self._state_lock:
            async with self._subscriber_lock:
                for query_id, state in list(self._query_states.items()):
                    # Only clean up terminal states with no active subscribers
                    if state in terminal_states:
                        has_subscribers = query_id in self._subscribers and len(self._subscribers[query_id]) > 0
                        if not has_subscribers:
                            cleaned_ids.append(query_id)
                
                for query_id in cleaned_ids:
                    del self._query_states[query_id]
                    # Also clean up empty subscriber sets
                    if query_id in self._subscribers:
                        del self._subscribers[query_id]
        
        if cleaned_ids:
            logger.info(f"Cleaned up {len(cleaned_ids)} terminal query states")
        
        return len(cleaned_ids)


# Global instance accessor
async def get_query_state_manager() -> QueryStateManager:
    """Get the global QueryStateManager instance"""
    return await QueryStateManager.get_instance()
