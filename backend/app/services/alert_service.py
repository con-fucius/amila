"""
Alert Service

Monitors system operations and triggers alerts for high-risk or anomalous behavior.
Supports:
- High-cost query detection
- Error spike detection per database
- Large result set alerting
- Resource exhaustion warnings
"""

import logging
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)

class AlertLevel:
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

class AlertService:
    """
    Service for managing system alerts.
    Alerts are stored in Redis with a TTL of 24 hours.
    """
    
    ALERT_KEY_PREFIX = "alerts:system:"
    DB_ERROR_COUNTER_PREFIX = "alerts:db_errors:"
    
    @classmethod
    async def trigger_alert(
        cls, 
        title: str, 
        message: str, 
        level: str = AlertLevel.WARNING,
        component: str = "system",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Record a new alert in the system.
        """
        alert_id = f"alert_{int(datetime.now(timezone.utc).timestamp())}"
        alert_data = {
            "id": alert_id,
            "title": title,
            "message": message,
            "level": level,
            "component": component,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata or {},
            "acknowledged": False
        }
        
        try:
            # Store in a Redis list for the last 24h
            key = f"{cls.ALERT_KEY_PREFIX}active"
            await redis_client._client.lpush(key, json.dumps(alert_data))
            await redis_client._client.ltrim(key, 0, 99) # Keep latest 100 alerts
            await redis_client._client.expire(key, 86400) # 24h TTL
            
            logger.warning(f"ALERT [{level}] {title}: {message}")

            # Schedule escalation if policy provided
            if metadata and metadata.get("escalation_policy_id"):
                try:
                    from app.services.alert_escalation_service import AlertEscalationService
                    await AlertEscalationService.schedule_escalation(alert_id, metadata["escalation_policy_id"])
                except Exception as e:
                    logger.warning(f"Failed to schedule escalation: {e}")
            
        except Exception as e:
            logger.error(f"Failed to record alert: {e}")

    @classmethod
    async def track_db_error(cls, database: str, error_msg: str):
        """
        Track database errors and trigger alerts on spikes.
        Threshold: 10 errors in 5 minutes.
        """
        key = f"{cls.DB_ERROR_COUNTER_PREFIX}{database}"
        try:
            # Increment error count for this database
            count = await redis_client._client.incr(key)
            if count == 1:
                await redis_client._client.expire(key, 300) # 5 minute window
            
            if count >= 10:
                await cls.trigger_alert(
                    title=f"Error Spike: {database.upper()}",
                    message=f"Detected high error frequency on {database}. Last error: {error_msg[:100]}...",
                    level=AlertLevel.CRITICAL,
                    component=f"db:{database}",
                    metadata={"error_count_5m": count, "last_error": error_msg}
                )
                # Reset counter to avoid duplicate alerts in the same window
                await redis_client._client.delete(key)
                
        except Exception as e:
            logger.error(f"Failed to track DB error: {e}")

    @classmethod
    async def get_active_alerts(cls, acknowledged: bool = False) -> List[Dict[str, Any]]:
        """
        Get all active system alerts.
        """
        key = f"{cls.ALERT_KEY_PREFIX}active"
        try:
            alerts_raw = await redis_client._client.lrange(key, 0, -1)
            alerts = [json.loads(a) for a in alerts_raw]
            return [a for a in alerts if a["acknowledged"] == acknowledged]
        except Exception as e:
            logger.error(f"Failed to fetch alerts: {e}")
            return []

    @classmethod
    async def get_alert_summary(cls) -> Dict[str, Any]:
        """
        Returns a summary of alerts for the dashboard.
        """
        alerts = await cls.get_active_alerts()
        critical_count = len([a for a in alerts if a["level"] == AlertLevel.CRITICAL])
        warning_count = len([a for a in alerts if a["level"] == AlertLevel.WARNING])
        
        return {
            "total_active": len(alerts),
            "critical": critical_count,
            "warning": warning_count,
            "latest_alerts": alerts[:5]
        }

# Global instance
alert_service = AlertService()
