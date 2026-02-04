"""
Alert Escalation Service
Implements multi-level escalation policies with time-based routing (email only).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from app.core.redis_client import redis_client
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


@dataclass
class EscalationPolicy:
    policy_id: str
    name: str
    levels: List[Dict[str, Any]]  # [{"delay_minutes": 5, "recipients": [...]}, ...]
    created_at: str


class AlertEscalationService:
    POLICY_KEY_PREFIX = "alerts:policy:"
    ACTIVE_KEY = "alerts:active:escalation"

    @classmethod
    async def create_policy(cls, policy_id: str, name: str, levels: List[Dict[str, Any]]) -> EscalationPolicy:
        now = datetime.now(timezone.utc).isoformat()
        policy = EscalationPolicy(policy_id=policy_id, name=name, levels=levels, created_at=now)
        await redis_client.set(f"{cls.POLICY_KEY_PREFIX}{policy_id}", asdict(policy), ttl=365 * 24 * 3600)
        return policy

    @classmethod
    async def get_policy(cls, policy_id: str) -> Optional[Dict[str, Any]]:
        return await redis_client.get(f"{cls.POLICY_KEY_PREFIX}{policy_id}")

    @classmethod
    async def schedule_escalation(cls, alert_id: str, policy_id: str) -> None:
        policy = await cls.get_policy(policy_id)
        if not policy:
            return
        schedule = {
            "alert_id": alert_id,
            "policy_id": policy_id,
            "level_index": 0,
            "next_at": (datetime.now(timezone.utc) + timedelta(minutes=policy["levels"][0]["delay_minutes"])).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await redis_client.set(f"{cls.ACTIVE_KEY}:{alert_id}", schedule, ttl=24 * 3600)

    @classmethod
    async def process_escalations(cls) -> int:
        keys = await redis_client.keys(f"{cls.ACTIVE_KEY}:*")
        now = datetime.now(timezone.utc)
        processed = 0
        for key in keys:
            item = await redis_client.get(key)
            if not item:
                continue
            try:
                next_at = datetime.fromisoformat(item.get("next_at"))
            except Exception:
                continue
            if next_at > now:
                continue
            policy = await cls.get_policy(item.get("policy_id"))
            if not policy:
                continue
            level_idx = int(item.get("level_index", 0))
            levels = policy.get("levels", [])
            if level_idx >= len(levels):
                await redis_client.delete(key)
                continue

            recipients = levels[level_idx].get("recipients", [])
            if recipients:
                try:
                    NotificationService.send_email(
                        to_addresses=recipients,
                        subject="Amila Alert Escalation",
                        body=f"Alert {item.get('alert_id')} requires attention. Escalation level {level_idx + 1}.",
                    )
                except Exception as e:
                    logger.warning("Escalation email failed: %s", e)

            level_idx += 1
            if level_idx >= len(levels):
                await redis_client.delete(key)
            else:
                item["level_index"] = level_idx
                item["next_at"] = (now + timedelta(minutes=levels[level_idx]["delay_minutes"])).isoformat()
                await redis_client.set(key, item, ttl=24 * 3600)
            processed += 1
        return processed

