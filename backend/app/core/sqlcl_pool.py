"""
SQLcl Process Pool Manager
Manages a pool of SQLcl MCP client processes for concurrent query execution
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from app.core.mcp_client import SQLclMCPClient, MCPRequest, MCPResponse
from app.core.resilience import CircuitBreaker, CircuitBreakerConfig, resilience_manager
from app.core.config import settings
from app.core.exceptions import MCPException

logger = logging.getLogger(__name__)


class ProcessState(Enum):
    """SQLcl process state"""
    IDLE = "idle"
    BUSY = "busy"
    FAILED = "failed"
    INITIALIZING = "initializing"
    SHUTDOWN = "shutdown"


@dataclass
class PooledProcess:
    """Wrapper for pooled SQLcl process"""
    process_id: int
    client: SQLclMCPClient
    state: ProcessState
    last_used: datetime
    queries_executed: int = 0
    errors: int = 0
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


class SQLclProcessPool:
    """
    Manages a pool of SQLcl MCP client processes for concurrent query execution
    
    Features:
    - Configurable pool size (default 2)
    - Automatic process initialization and recycling
    - Circuit breaker protection
    - Health monitoring and auto-recovery
    - Graceful shutdown with request draining
    - Connection timeout enforcement
    """
    
    def __init__(
        self,
        pool_size: int = None,
        max_queries_per_process: int = 1000,
        process_timeout: int = 600,
        health_check_interval: int = 60,
    ):
        """
        Initialize SQLcl process pool
        
        Args:
            pool_size: Number of processes in pool (default from settings)
            max_queries_per_process: Max queries before process recycling
            process_timeout: Timeout for process operations in seconds (600s for complex queries)
            health_check_interval: Health check interval in seconds
        """
        self.pool_size = pool_size or settings.sqlcl_max_processes
        self.max_queries_per_process = max_queries_per_process
        self.process_timeout = process_timeout
        self.health_check_interval = health_check_interval
        
        # Pool management
        self.processes: List[PooledProcess] = []
        self.pool_lock = asyncio.Lock()
        self.available_queue: asyncio.Queue[PooledProcess] = asyncio.Queue()
        
        # State tracking
        self.initialized = False
        self.shutting_down = False
        self.active_requests = 0
        self.total_queries = 0
        self.total_errors = 0
        
        # Circuit breaker for pool health
        self.circuit_breaker = resilience_manager.get_or_create_circuit_breaker(
            "sqlcl_pool",
            CircuitBreakerConfig(
                name="sqlcl_pool",
                failure_threshold=3,
                recovery_timeout=30,
                success_threshold=2
            )
        )
        
        # Health monitoring task
        self.health_check_task: Optional[asyncio.Task] = None
        
        logger.info(f"SQLcl process pool configured with {self.pool_size} processes")
    
    async def initialize(self) -> bool:
        """
        Initialize the process pool
        
        Returns:
            bool: True if initialization successful
        """
        if self.initialized:
            logger.warning("Pool already initialized")
            return True
        
        logger.info(f"Initializing SQLcl process pool with {self.pool_size} processes...")
        
        async with self.pool_lock:
            # Initialize all processes
            for process_id in range(self.pool_size):
                try:
                    logger.info(f"Initializing process {process_id + 1}/{self.pool_size}...")
                    
                    # Create SQLcl MCP client
                    client = SQLclMCPClient(
                        sqlcl_path=settings.sqlcl_path,
                        sqlcl_args=settings.sqlcl_args,
                        timeout=self.process_timeout,
                    )
                    
                    # Initialize client
                    success = await client.initialize()
                    
                    if not success:
                        logger.error(f"Failed to initialize process {process_id}")
                        continue
                    
                    # Auto-connect to default database
                    connect_result = await client.connect_database(settings.oracle_default_connection)
                    if connect_result.get("status") != "connected":
                        logger.error(f"Failed to connect process {process_id} to database: {connect_result.get('message')} (conn={settings.oracle_default_connection})")
                        await client.close()
                        continue
                    
                    # Create pooled process
                    pooled_process = PooledProcess(
                        process_id=process_id,
                        client=client,
                        state=ProcessState.IDLE,
                        last_used=datetime.now(timezone.utc),
                    )
                    
                    self.processes.append(pooled_process)
                    await self.available_queue.put(pooled_process)
                    
                    logger.info(f"Process {process_id} initialized and ready")
                    
                except Exception as e:
                    logger.error(f"Failed to initialize process {process_id}: {e}")
                    continue
            
            if len(self.processes) == 0:
                logger.error(f"Failed to initialize any processes")
                return False
            
            self.initialized = True
            
            # Start health monitoring
            self.health_check_task = asyncio.create_task(self._health_monitor())
            
            logger.info(f"SQLcl process pool initialized with {len(self.processes)}/{self.pool_size} processes")
            await self.circuit_breaker.record_success()
            return True
    
    @asynccontextmanager
    async def acquire(self, timeout: int = 30):
        """
        Acquire a process from the pool
        
        Args:
            timeout: Timeout for acquiring process
            
        Yields:
            SQLclMCPClient: Available client
            
        Raises:
            MCPException: If pool is not initialized, shutting down, or circuit breaker is open
        """
        if not self.initialized:
            raise MCPException(
                "SQLcl pool not initialized",
                details={"pool_status": "not_initialized"}
            )
        
        if self.shutting_down:
            raise MCPException(
                "SQLcl pool is shutting down",
                details={"pool_status": "shutting_down"}
            )
        
        # Check circuit breaker
        if not await self.circuit_breaker.can_execute():
            raise MCPException(
                "SQLcl pool circuit breaker is OPEN - service temporarily unavailable",
                details={
                    "pool_status": "circuit_breaker_open",
                    "circuit_breaker_status": self.circuit_breaker.get_status()
                }
            )
        
        process: Optional[PooledProcess] = None
        
        try:
            self.active_requests += 1
            
            # Wait for available process
            try:
                process = await asyncio.wait_for(
                    self.available_queue.get(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Timeout waiting for available process (pool exhausted)")
                await self.circuit_breaker.record_failure()
                raise MCPException(
                    "Pool exhausted: no available processes",
                    details={
                        "pool_size": self.pool_size,
                        "active_requests": self.active_requests,
                        "timeout": timeout
                    }
                )
            
            # Mark as busy
            process.state = ProcessState.BUSY
            process.last_used = datetime.now(timezone.utc)
            
            logger.debug(f"Acquired process {process.process_id} from pool")
            
            yield process.client
            
            # Record successful execution
            process.queries_executed += 1
            self.total_queries += 1
            await self.circuit_breaker.record_success()
            
        except Exception as e:
            # Record failure
            if process:
                process.errors += 1
            self.total_errors += 1
            await self.circuit_breaker.record_failure()
            
            logger.error(f"Error during process execution: {e}")
            raise
        
        finally:
            self.active_requests -= 1
            
            if process:
                # Check if process needs recycling
                if process.queries_executed >= self.max_queries_per_process:
                    logger.info(f"Process {process.process_id} reached max queries, recycling...")
                    await self._recycle_process(process)
                elif process.errors >= 3:
                    logger.warning(f"Process {process.process_id} has {process.errors} errors, recycling...")
                    await self._recycle_process(process)
                else:
                    # Return to pool
                    process.state = ProcessState.IDLE
                    await self.available_queue.put(process)
                    logger.debug(f"Returned process {process.process_id} to pool")
    
    async def _recycle_process(self, process: PooledProcess):
        """
        Recycle a process (close and recreate)
        
        Args:
            process: Process to recycle
        """
        logger.info(f"Recycling process {process.process_id}...")
        
        try:
            # Close old process
            await process.client.close()
            
            # Remove from pool
            async with self.pool_lock:
                self.processes.remove(process)
            
            # Create new process
            new_client = SQLclMCPClient(
                sqlcl_path=settings.sqlcl_path,
                sqlcl_args=settings.sqlcl_args,
                timeout=self.process_timeout,
            )
            
            success = await new_client.initialize()
            if not success:
                logger.error(f"Failed to reinitialize process {process.process_id}")
                return
            
            # Auto-connect to default database
            connect_result = await new_client.connect_database(settings.oracle_default_connection)
            if connect_result.get("status") != "connected":
                logger.error(f"Failed to reconnect recycled process {process.process_id}: {connect_result.get('message')} (conn={settings.oracle_default_connection})")
                await new_client.close()
                return
            
            # Create new pooled process
            new_process = PooledProcess(
                process_id=process.process_id,
                client=new_client,
                state=ProcessState.IDLE,
                last_used=datetime.now(timezone.utc),
            )
            
            async with self.pool_lock:
                self.processes.append(new_process)
            
            await self.available_queue.put(new_process)
            
            logger.info(f"Process {process.process_id} recycled successfully")
            
        except Exception as e:
            logger.error(f"Failed to recycle process {process.process_id}: {e}")
    
    async def _health_monitor(self):
        """Background task to monitor pool health"""
        logger.info(f"Starting pool health monitor")
        
        while not self.shutting_down:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                # Check pool health
                async with self.pool_lock:
                    healthy_count = sum(
                        1 for p in self.processes
                        if p.state in [ProcessState.IDLE, ProcessState.BUSY]
                    )
                    
                    logger.info(
                        f" Pool health: {healthy_count}/{self.pool_size} healthy, "
                        f"{self.active_requests} active, "
                        f"{self.total_queries} total queries, "
                        f"{self.total_errors} errors"
                    )
                    
                    # Auto-recovery: recreate failed processes
                    for process in self.processes[:]:
                        if process.state == ProcessState.FAILED:
                            logger.warning(f"Auto-recovering failed process {process.process_id}")
                            await self._recycle_process(process)
                
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    
    async def shutdown(self, drain_timeout: int = 30):
        """
        Gracefully shutdown pool with request draining
        
        Args:
            drain_timeout: Maximum time to wait for active requests
        """
        logger.info(f"Shutting down SQLcl process pool...")
        self.shutting_down = True
        
        # Stop health monitor
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        # Wait for active requests to complete
        if self.active_requests > 0:
            logger.info(f"Waiting for {self.active_requests} active requests to complete...")
            
            wait_start = asyncio.get_event_loop().time()
            while self.active_requests > 0:
                if asyncio.get_event_loop().time() - wait_start > drain_timeout:
                    logger.warning(f"Drain timeout exceeded, {self.active_requests} requests still active")
                    break
                await asyncio.sleep(0.5)
        
        # Close all processes
        async with self.pool_lock:
            for process in self.processes:
                try:
                    logger.info(f"Closing process {process.process_id}...")
                    await process.client.close()
                    process.state = ProcessState.SHUTDOWN
                except Exception as e:
                    logger.error(f"Error closing process {process.process_id}: {e}")
        
        logger.info(f"SQLcl process pool shutdown complete")
    
    def get_status(self) -> Dict[str, Any]:
        """Get pool status for monitoring"""
        return {
            "pool_size": self.pool_size,
            "initialized": self.initialized,
            "shutting_down": self.shutting_down,
            "active_requests": self.active_requests,
            "total_queries": self.total_queries,
            "total_errors": self.total_errors,
            "circuit_breaker": self.circuit_breaker.get_status(),
            "processes": [
                {
                    "process_id": p.process_id,
                    "state": p.state.value,
                    "queries_executed": p.queries_executed,
                    "errors": p.errors,
                    "last_used": p.last_used.isoformat(),
                    "uptime_seconds": (datetime.now(timezone.utc) - p.created_at).total_seconds(),
                }
                for p in self.processes
            ]
        }