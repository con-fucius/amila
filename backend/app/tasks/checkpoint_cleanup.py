"""
Checkpoint Cleanup Task
Cleans up old LangGraph SQLite checkpoints to prevent unbounded growth
"""

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.core.config import settings

logger = logging.getLogger(__name__)

# Default retention period (7 days)
DEFAULT_RETENTION_DAYS = 7

# Maximum checkpoints to keep per thread
MAX_CHECKPOINTS_PER_THREAD = 10


async def cleanup_old_checkpoints(
    retention_days: int = DEFAULT_RETENTION_DAYS,
    max_per_thread: int = MAX_CHECKPOINTS_PER_THREAD,
    dry_run: bool = False
) -> dict:
    """
    Clean up old LangGraph SQLite checkpoints.
    
    This prevents unbounded growth of the checkpoint database by:
    1. Removing checkpoints older than retention_days
    2. Keeping only the most recent max_per_thread checkpoints per thread
    
    Args:
        retention_days: Number of days to retain checkpoints
        max_per_thread: Maximum checkpoints to keep per thread_id
        dry_run: If True, only report what would be deleted
        
    Returns:
        Dict with cleanup statistics
    """
    db_path = settings.LANGGRAPH_CHECKPOINT_DB
    
    if not os.path.exists(db_path):
        logger.info(f"Checkpoint database not found at {db_path}")
        return {
            "status": "skipped",
            "reason": "database_not_found",
            "path": db_path,
        }
    
    stats = {
        "status": "success",
        "dry_run": dry_run,
        "retention_days": retention_days,
        "max_per_thread": max_per_thread,
        "deleted_by_age": 0,
        "deleted_by_count": 0,
        "total_deleted": 0,
        "total_remaining": 0,
        "threads_processed": 0,
        "errors": [],
    }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if the checkpoints table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='checkpoints'
        """)
        
        if not cursor.fetchone():
            logger.info("Checkpoints table not found, skipping cleanup")
            conn.close()
            return {
                "status": "skipped",
                "reason": "table_not_found",
            }
        
        # Get cutoff timestamp
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        cutoff_ts = cutoff_date.isoformat()
        
        # 1. Delete checkpoints older than retention period
        if dry_run:
            cursor.execute("""
                SELECT COUNT(*) FROM checkpoints 
                WHERE created_at < ?
            """, (cutoff_ts,))
            stats["deleted_by_age"] = cursor.fetchone()[0]
        else:
            cursor.execute("""
                DELETE FROM checkpoints 
                WHERE created_at < ?
            """, (cutoff_ts,))
            stats["deleted_by_age"] = cursor.rowcount
            conn.commit()
        
        logger.info(f"{'Would delete' if dry_run else 'Deleted'} {stats['deleted_by_age']} checkpoints by age")
        
        # 2. Keep only max_per_thread most recent checkpoints per thread
        cursor.execute("""
            SELECT DISTINCT thread_id FROM checkpoints
        """)
        threads = cursor.fetchall()
        stats["threads_processed"] = len(threads)
        
        for (thread_id,) in threads:
            if dry_run:
                # Count how many would be deleted
                cursor.execute("""
                    SELECT COUNT(*) FROM checkpoints 
                    WHERE thread_id = ?
                    AND checkpoint_id NOT IN (
                        SELECT checkpoint_id FROM checkpoints 
                        WHERE thread_id = ?
                        ORDER BY created_at DESC 
                        LIMIT ?
                    )
                """, (thread_id, thread_id, max_per_thread))
                stats["deleted_by_count"] += cursor.fetchone()[0]
            else:
                # Delete excess checkpoints
                cursor.execute("""
                    DELETE FROM checkpoints 
                    WHERE thread_id = ?
                    AND checkpoint_id NOT IN (
                        SELECT checkpoint_id FROM checkpoints 
                        WHERE thread_id = ?
                        ORDER BY created_at DESC 
                        LIMIT ?
                    )
                """, (thread_id, thread_id, max_per_thread))
                stats["deleted_by_count"] += cursor.rowcount
        
        if not dry_run:
            conn.commit()
        
        logger.info(f"{'Would delete' if dry_run else 'Deleted'} {stats['deleted_by_count']} checkpoints by count limit")
        
        # Get remaining count
        cursor.execute("SELECT COUNT(*) FROM checkpoints")
        stats["total_remaining"] = cursor.fetchone()[0]
        
        stats["total_deleted"] = stats["deleted_by_age"] + stats["deleted_by_count"]
        
        # Vacuum the database to reclaim space (only if not dry run and we deleted something)
        if not dry_run and stats["total_deleted"] > 0:
            try:
                cursor.execute("VACUUM")
                logger.info("Database vacuumed successfully")
            except Exception as e:
                logger.warning(f"Failed to vacuum database: {e}")
                stats["errors"].append(f"vacuum_failed: {e}")
        
        conn.close()
        
        logger.info(
            f"Checkpoint cleanup complete: deleted {stats['total_deleted']}, "
            f"remaining {stats['total_remaining']}"
        )
        
        return stats
        
    except Exception as e:
        logger.error(f"Checkpoint cleanup failed: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }


def cleanup_old_checkpoints_task(
    retention_days: int = DEFAULT_RETENTION_DAYS,
    max_per_thread: int = MAX_CHECKPOINTS_PER_THREAD,
    dry_run: bool = False
) -> dict:
    """
    Celery task wrapper for checkpoint cleanup.
    
    This is a synchronous wrapper that can be called by Celery beat.
    """
    import asyncio
    
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(
        cleanup_old_checkpoints(retention_days, max_per_thread, dry_run)
    )


def get_checkpoint_stats() -> dict:
    """
    Get statistics about the checkpoint database.
    
    Returns:
        Dict with checkpoint statistics
    """
    db_path = settings.LANGGRAPH_CHECKPOINT_DB
    
    if not os.path.exists(db_path):
        return {
            "status": "not_found",
            "path": db_path,
        }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='checkpoints'
        """)
        
        if not cursor.fetchone():
            conn.close()
            return {
                "status": "empty",
                "path": db_path,
            }
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM checkpoints")
        total_count = cursor.fetchone()[0]
        
        # Get unique threads
        cursor.execute("SELECT COUNT(DISTINCT thread_id) FROM checkpoints")
        thread_count = cursor.fetchone()[0]
        
        # Get oldest and newest
        cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM checkpoints")
        oldest, newest = cursor.fetchone()
        
        # Get database file size
        file_size = os.path.getsize(db_path)
        
        conn.close()
        
        return {
            "status": "ok",
            "path": db_path,
            "total_checkpoints": total_count,
            "unique_threads": thread_count,
            "oldest_checkpoint": oldest,
            "newest_checkpoint": newest,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
        }
        
    except Exception as e:
        logger.error(f"Failed to get checkpoint stats: {e}")
        return {
            "status": "error",
            "error": str(e),
        }
