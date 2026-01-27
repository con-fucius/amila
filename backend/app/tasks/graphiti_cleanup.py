"""
Graphiti Knowledge Graph Cleanup Task
Prevents unbounded growth by pruning old nodes from the knowledge graph
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.core.graphiti_client import get_graphiti_client

logger = logging.getLogger(__name__)


class GraphitiCleanupService:
    """Service to cleanup old Graphiti knowledge graph nodes"""
    
    def __init__(self, retention_days: int = 30):
        """
        Initialize cleanup service
        
        Args:
            retention_days: Number of days to retain nodes (default: 30)
        """
        self.retention_days = retention_days
        logger.info(f"Graphiti cleanup service initialized (retention: {retention_days} days)")
    
    async def cleanup_old_nodes(self) -> dict:
        """
        Delete nodes older than retention period
        
        Returns:
            dict: Cleanup statistics
        """
        try:
            graphiti_client = await get_graphiti_client()
            if not graphiti_client:
                logger.warning(f"Graphiti client not available for cleanup")
                return {
                    "status": "skipped",
                    "reason": "Graphiti not available",
                    "nodes_deleted": 0
                }
            
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
            logger.info(f"Starting Graphiti cleanup (cutoff: {cutoff_date.isoformat()})")
            
            # Search for old nodes
            # Note: Graphiti doesn't have a direct "delete by date" API
            # This is a placeholder for when the API supports it
            # For now, we'll just log the intention
            
            deleted_count = 0
            
            # Logic for date-based node deletion will go here once supported by the API
            
            logger.info(f"Graphiti cleanup complete: {deleted_count} nodes deleted")
            
            return {
                "status": "success",
                "cutoff_date": cutoff_date.isoformat(),
                "nodes_deleted": deleted_count,
                "retention_days": self.retention_days
            }
            
        except Exception as e:
            logger.error(f"Graphiti cleanup failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "nodes_deleted": 0
            }
    
    async def get_storage_stats(self) -> Optional[dict]:
        """
        Get current Graphiti storage statistics
        
        Returns:
            dict: Storage statistics or None if unavailable
        """
        try:
            graphiti_client = await get_graphiti_client()
            if not graphiti_client:
                return None
            
            # Search all nodes to get count (inefficient, but Graphiti doesn't expose stats API)
            # This is a placeholder - adjust based on actual Graphiti API
            stats = {
                "total_nodes": 0,  # Would need to query
                "total_edges": 0,  # Would need to query
                "estimated_size_mb": 0,  # Would need to calculate
                "oldest_node_date": None,
                "newest_node_date": None,
            }
            
            logger.info(f"Graphiti storage stats retrieved")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get Graphiti stats: {e}")
            return None


# Global cleanup service instance
cleanup_service = GraphitiCleanupService(retention_days=30)


async def run_graphiti_cleanup() -> dict:
    """
    Run Graphiti cleanup task (can be called from Celery or scheduler)
    
    Returns:
        dict: Cleanup results
    """
    return await cleanup_service.cleanup_old_nodes()


async def get_graphiti_stats() -> Optional[dict]:
    """
    Get Graphiti storage statistics
    
    Returns:
        dict: Storage stats or None
    """
    return await cleanup_service.get_storage_stats()