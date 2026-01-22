"""
LangGraph Checkpointer Fallback

Provides graceful degradation for LangGraph checkpointing when SQLite is unavailable.
Falls back to in-memory checkpointing with limited persistence.

Features:
- In-memory checkpoint storage
- Automatic cleanup of old checkpoints
- Thread-safe operations
- Compatible with LangGraph checkpoint interface
"""

import logging
import asyncio
from typing import Any, Optional, Dict, List, Tuple, AsyncIterator
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
import json

logger = logging.getLogger(__name__)

# Import CheckpointTuple from langgraph
try:
    from langgraph.checkpoint.base import CheckpointTuple
except ImportError:
    # Fallback if not available
    CheckpointTuple = None
    logger.warning("CheckpointTuple not available from langgraph.checkpoint.base")


@dataclass
class InMemoryCheckpoint:
    """Represents a checkpoint in memory"""
    thread_id: str
    checkpoint_id: str
    parent_checkpoint_id: Optional[str]
    checkpoint_data: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class InMemoryCheckpointer:
    """
    In-memory fallback checkpointer for LangGraph.
    
    Provides basic checkpointing functionality when SQLite is unavailable.
    Note: Checkpoints are lost on restart.
    """
    
    def __init__(self, max_checkpoints_per_thread: int = 100, max_age_hours: int = 24):
        self.max_checkpoints_per_thread = max_checkpoints_per_thread
        self.max_age_hours = max_age_hours
        
        # Storage: thread_id -> list of checkpoints
        self.checkpoints: Dict[str, List[InMemoryCheckpoint]] = defaultdict(list)
        self.lock = asyncio.Lock()
        
        logger.warning(
            "Using in-memory checkpointer fallback. "
            "Checkpoints will be lost on restart. "
            "This is NOT suitable for production."
        )
    
    async def aput(
        self,
        config: Dict[str, Any],
        checkpoint: Dict[str, Any],
        metadata: Dict[str, Any],
        new_versions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save a checkpoint (LangGraph interface).
        
        Args:
            config: Configuration dict with thread_id
            checkpoint: Checkpoint data
            metadata: Checkpoint metadata
            new_versions: Version information
            
        Returns:
            Updated config with checkpoint_id
        """
        async with self.lock:
            thread_id = config.get("configurable", {}).get("thread_id")
            if not thread_id:
                raise ValueError("thread_id required in config.configurable")
            
            # Generate checkpoint ID
            checkpoint_id = f"cp_{datetime.now(timezone.utc).timestamp()}"
            
            # Get parent checkpoint ID
            parent_checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
            
            # Create checkpoint
            cp = InMemoryCheckpoint(
                thread_id=thread_id,
                checkpoint_id=checkpoint_id,
                parent_checkpoint_id=parent_checkpoint_id,
                checkpoint_data=checkpoint,
                metadata=metadata
            )
            
            # Store checkpoint
            self.checkpoints[thread_id].append(cp)
            
            # Cleanup old checkpoints
            await self._cleanup_thread(thread_id)
            
            logger.debug(f"Checkpoint saved: {thread_id}/{checkpoint_id}")
            
            # Return updated config
            return {
                **config,
                "configurable": {
                    **config.get("configurable", {}),
                    "checkpoint_id": checkpoint_id
                }
            }
    
    async def aget(
        self,
        config: Dict[str, Any]
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """
        Retrieve a checkpoint (LangGraph interface).
        
        Args:
            config: Configuration dict with thread_id and optional checkpoint_id
            
        Returns:
            Tuple of (checkpoint_data, metadata) or None
        """
        async with self.lock:
            thread_id = config.get("configurable", {}).get("thread_id")
            if not thread_id:
                return None
            
            checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
            
            checkpoints = self.checkpoints.get(thread_id, [])
            if not checkpoints:
                return None
            
            # Find specific checkpoint or return latest
            if checkpoint_id:
                for cp in reversed(checkpoints):
                    if cp.checkpoint_id == checkpoint_id:
                        return (cp.checkpoint_data, cp.metadata)
                return None
            else:
                # Return latest checkpoint
                latest = checkpoints[-1]
                return (latest.checkpoint_data, latest.metadata)
    
    async def aget_tuple(
        self,
        config: Dict[str, Any]
    ) -> Optional[Any]:
        """
        Retrieve a checkpoint tuple (LangGraph 0.6.10+ interface).
        
        Args:
            config: Configuration dict with thread_id and optional checkpoint_id
            
        Returns:
            CheckpointTuple or None
        """
        result = await self.aget(config)
        if result is None:
            return None
        
        checkpoint_data, metadata = result
        
        # Find parent config if exists
        async with self.lock:
            thread_id = config.get("configurable", {}).get("thread_id")
            checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
            
            if not thread_id:
                if CheckpointTuple:
                    return CheckpointTuple(
                        config=config,
                        checkpoint=checkpoint_data,
                        metadata=metadata,
                        parent_config=None
                    )
                return None
            
            checkpoints = self.checkpoints.get(thread_id, [])
            
            # Find the checkpoint to get parent_checkpoint_id
            parent_config = None
            for cp in reversed(checkpoints):
                if checkpoint_id and cp.checkpoint_id == checkpoint_id:
                    if cp.parent_checkpoint_id:
                        parent_config = {
                            **config,
                            "configurable": {
                                **config.get("configurable", {}),
                                "checkpoint_id": cp.parent_checkpoint_id
                            }
                        }
                    break
                elif not checkpoint_id and cp == checkpoints[-1]:
                    if cp.parent_checkpoint_id:
                        parent_config = {
                            **config,
                            "configurable": {
                                **config.get("configurable", {}),
                                "checkpoint_id": cp.parent_checkpoint_id
                            }
                        }
                    break
            
            if CheckpointTuple:
                return CheckpointTuple(
                    config=config,
                    checkpoint=checkpoint_data,
                    metadata=metadata,
                    parent_config=parent_config
                )
            
            # Fallback to tuple if CheckpointTuple not available
            return (checkpoint_data, metadata, parent_config)
    
    async def alist(
        self,
        config: Dict[str, Any],
        limit: Optional[int] = None,
        before: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """
        List checkpoints for a thread (LangGraph interface).
        
        Args:
            config: Configuration dict with thread_id
            limit: Maximum number of checkpoints to return
            before: Return checkpoints before this config
            
        Yields:
            Tuples of (checkpoint_data, metadata)
        """
        async with self.lock:
            thread_id = config.get("configurable", {}).get("thread_id")
            if not thread_id:
                return
            
            checkpoints = self.checkpoints.get(thread_id, [])
            if not checkpoints:
                return
            
            # Filter by before if specified
            if before:
                before_id = before.get("configurable", {}).get("checkpoint_id")
                if before_id:
                    checkpoints = [
                        cp for cp in checkpoints
                        if cp.checkpoint_id < before_id
                    ]
            
            # Apply limit
            if limit:
                checkpoints = checkpoints[-limit:]
            
            # Yield checkpoints in reverse order (newest first)
            for cp in reversed(checkpoints):
                yield (cp.checkpoint_data, cp.metadata)
    
    async def aput_writes(
        self,
        config: Dict[str, Any],
        writes: List[Tuple[str, Any]],
        task_id: str
    ) -> None:
        """
        Store pending writes for a checkpoint (LangGraph 0.6.10+ interface).
        
        Args:
            config: Configuration dict with thread_id
            writes: List of (channel, value) tuples to write
            task_id: Task identifier
        """
        # In-memory checkpointer doesn't need to store writes separately
        # They are applied directly to the checkpoint in aput
        logger.debug(f"aput_writes called for task {task_id} (no-op in memory checkpointer)")
        pass
    
    async def _cleanup_thread(self, thread_id: str):
        """Clean up old checkpoints for a thread"""
        checkpoints = self.checkpoints.get(thread_id, [])
        if not checkpoints:
            return
        
        # Remove checkpoints exceeding max count
        if len(checkpoints) > self.max_checkpoints_per_thread:
            self.checkpoints[thread_id] = checkpoints[-self.max_checkpoints_per_thread:]
            logger.debug(f"Cleaned up old checkpoints for thread {thread_id}")
        
        # Remove checkpoints exceeding max age
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.max_age_hours)
        self.checkpoints[thread_id] = [
            cp for cp in self.checkpoints[thread_id]
            if cp.created_at > cutoff_time
        ]
    
    async def cleanup_all(self):
        """Clean up all old checkpoints"""
        async with self.lock:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.max_age_hours)
            
            for thread_id in list(self.checkpoints.keys()):
                self.checkpoints[thread_id] = [
                    cp for cp in self.checkpoints[thread_id]
                    if cp.created_at > cutoff_time
                ]
                
                # Remove empty threads
                if not self.checkpoints[thread_id]:
                    del self.checkpoints[thread_id]
            
            logger.info(f"Cleaned up checkpoints. Active threads: {len(self.checkpoints)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get checkpointer statistics"""
        total_checkpoints = sum(len(cps) for cps in self.checkpoints.values())
        
        return {
            "type": "in_memory",
            "active_threads": len(self.checkpoints),
            "total_checkpoints": total_checkpoints,
            "max_checkpoints_per_thread": self.max_checkpoints_per_thread,
            "max_age_hours": self.max_age_hours,
        }
    
    def get_next_version(self, current: Optional[str], channel: str) -> str:
        """
        Get next version for a channel (required by LangGraph).
        
        Args:
            current: Current version string
            channel: Channel name
            
        Returns:
            Next version string
        """
        # Simple incrementing version
        if current is None:
            return "1"
        
        try:
            version_num = int(current)
            return str(version_num + 1)
        except (ValueError, TypeError):
            # If current is not a number, start from 1
            return "1"


class ResilientCheckpointerWrapper:
    """
    Wrapper that provides automatic fallback from SQLite to in-memory checkpointing.
    
    Monitors SQLite checkpointer health and switches to in-memory fallback when needed.
    """
    
    def __init__(self, sqlite_checkpointer: Any, enable_fallback: bool = True):
        self.sqlite_checkpointer = sqlite_checkpointer
        self.fallback_checkpointer = InMemoryCheckpointer() if enable_fallback else None
        self.enable_fallback = enable_fallback
        self.is_degraded = False
        self.failure_count = 0
        self.failure_threshold = 3
        
        logger.info("Resilient checkpointer wrapper initialized")
    
    async def aput(self, config: Dict[str, Any], checkpoint: Dict[str, Any], 
                   metadata: Dict[str, Any], new_versions: Dict[str, Any]) -> Dict[str, Any]:
        """Save checkpoint with fallback"""
        try:
            result = await self.sqlite_checkpointer.aput(config, checkpoint, metadata, new_versions)
            self.failure_count = 0
            self.is_degraded = False
            return result
        
        except Exception as e:
            self.failure_count += 1
            logger.warning(f"SQLite checkpointer failed ({self.failure_count}/{self.failure_threshold}): {e}")
            
            if self.failure_count >= self.failure_threshold:
                self.is_degraded = True
                logger.error("SQLite checkpointer degraded, using in-memory fallback")
            
            if self.enable_fallback and self.fallback_checkpointer:
                return await self.fallback_checkpointer.aput(config, checkpoint, metadata, new_versions)
            
            raise
    
    async def aget(self, config: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """Get checkpoint with fallback"""
        try:
            if not self.is_degraded:
                result = await self.sqlite_checkpointer.aget(config)
                self.failure_count = 0
                return result
        
        except Exception as e:
            self.failure_count += 1
            logger.warning(f"SQLite checkpointer get failed: {e}")
            
            if self.failure_count >= self.failure_threshold:
                self.is_degraded = True
        
        # Use fallback
        if self.enable_fallback and self.fallback_checkpointer:
            return await self.fallback_checkpointer.aget(config)
        
        return None
    
    async def aget_tuple(
        self,
        config: Dict[str, Any]
    ) -> Optional[Any]:
        """Get checkpoint tuple with fallback (LangGraph 0.6.10+ interface)"""
        try:
            if not self.is_degraded and hasattr(self.sqlite_checkpointer, 'aget_tuple'):
                result = await self.sqlite_checkpointer.aget_tuple(config)
                self.failure_count = 0
                return result
        
        except Exception as e:
            self.failure_count += 1
            logger.warning(f"SQLite checkpointer aget_tuple failed: {e}")
            
            if self.failure_count >= self.failure_threshold:
                self.is_degraded = True
        
        # Use fallback
        if self.enable_fallback and self.fallback_checkpointer:
            return await self.fallback_checkpointer.aget_tuple(config)
        
        return None
    
    async def alist(self, config: Dict[str, Any], limit: Optional[int] = None,
                    before: Optional[Dict[str, Any]] = None) -> AsyncIterator[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """List checkpoints with fallback"""
        try:
            if not self.is_degraded:
                async for item in self.sqlite_checkpointer.alist(config, limit, before):
                    yield item
                return
        
        except Exception as e:
            logger.warning(f"SQLite checkpointer list failed: {e}")
            self.is_degraded = True
        
        # Use fallback
        if self.enable_fallback and self.fallback_checkpointer:
            async for item in self.fallback_checkpointer.alist(config, limit, before):
                yield item
    
    async def aput_writes(
        self,
        config: Dict[str, Any],
        writes: List[Tuple[str, Any]],
        task_id: str
    ) -> None:
        """
        Store pending writes for a checkpoint with fallback (LangGraph 0.6.10+ interface).
        
        Args:
            config: Configuration dict with thread_id
            writes: List of (channel, value) tuples to write
            task_id: Task identifier
        """
        try:
            if not self.is_degraded and hasattr(self.sqlite_checkpointer, 'aput_writes'):
                await self.sqlite_checkpointer.aput_writes(config, writes, task_id)
                self.failure_count = 0
                return
        
        except Exception as e:
            self.failure_count += 1
            logger.warning(f"SQLite checkpointer aput_writes failed: {e}")
            
            if self.failure_count >= self.failure_threshold:
                self.is_degraded = True
        
        # Use fallback
        if self.enable_fallback and self.fallback_checkpointer:
            await self.fallback_checkpointer.aput_writes(config, writes, task_id)
    
    def get_status(self) -> Dict[str, Any]:
        """Get checkpointer status"""
        status = {
            "is_degraded": self.is_degraded,
            "failure_count": self.failure_count,
            "fallback_enabled": self.enable_fallback,
        }
        
        if self.enable_fallback and self.fallback_checkpointer:
            status["fallback_stats"] = self.fallback_checkpointer.get_stats()
        
        return status
    
    def get_next_version(self, current: Optional[str], channel: str) -> str:
        """Get next version for a channel (delegates to active checkpointer)"""
        if self.is_degraded and self.enable_fallback and self.fallback_checkpointer:
            return self.fallback_checkpointer.get_next_version(current, channel)
        
        # Delegate to SQLite checkpointer
        if hasattr(self.sqlite_checkpointer, 'get_next_version'):
            return self.sqlite_checkpointer.get_next_version(current, channel)
        
        # Fallback implementation
        if current is None:
            return "1"
        try:
            version_num = int(current)
            return str(version_num + 1)
        except (ValueError, TypeError):
            return "1"


# Context manager for in-memory checkpointer
class InMemoryCheckpointerContext:
    """Context manager that mimics AsyncSqliteSaver.from_conn_string"""
    
    def __init__(self):
        self.checkpointer = InMemoryCheckpointer()
    
    async def __aenter__(self):
        return self.checkpointer
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cleanup
        await self.checkpointer.cleanup_all()
        return False

