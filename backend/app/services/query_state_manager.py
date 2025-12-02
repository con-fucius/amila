"""
Query State Manager Service
Tracks query execution lifecycle and enables SSE streaming of state changes
"""

import asyncio
import logging
from typing import Dict, Optional, Set, AsyncGenerator
from enum import Enum
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
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
    
    def to_sse_message(self) -> str:
        """Convert to SSE message format"""
        data = asdict(self)
        return f"data: {json.dumps(data)}\n\n"
class QueryStateManager:
    """
    Manages query execution states and enables SSE streaming
    Thread-safe singleton for tracking query lifecycle
    """
    _instance: Optional['QueryStateManager'] = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        # Query state storage: query_id -> current state
        self._query_states: Dict[str, QueryState] = {}
        
        # Event queues for SSE subscribers: query_id -> set of queues
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        
        # Locks for thread-safe operations
        self._state_lock = asyncio.Lock()
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
    
    async def cleanup_query(self, query_id: str) -> None:
        """Clean up query state after completion"""
        async with self._state_lock:
            if query_id in self._query_states:
                del self._query_states[query_id]
                logger.info(f"Cleaned up state for query {query_id[:8]}")


# Global instance accessor
async def get_query_state_manager() -> QueryStateManager:
    """Get the global QueryStateManager instance"""
    return await QueryStateManager.get_instance()
