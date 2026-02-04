"""
Scheduled Report Service
Stores cron-based schedules in Redis and executes reports on schedule
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.redis_client import redis_client
from app.core.cron_utils import next_run, parse_cron

logger = logging.getLogger(__name__)


@dataclass
class ReportSchedule:
    schedule_id: str
    user_id: str
    name: str
    cron: str
    sql_query: str
    database_type: str
    connection_name: Optional[str]
    format: str
    recipients: List[str]
    created_at: str
    updated_at: str
    next_run_at: str
    active: bool
    last_run_at: Optional[str] = None
    last_status: Optional[str] = None
    last_error: Optional[str] = None


class ReportScheduleService:
    KEY_PREFIX = "report:schedule:"
    USER_KEY_PREFIX = "report:schedules:user:"

    @classmethod
    async def create_schedule(
        cls,
        user_id: str,
        name: str,
        cron: str,
        sql_query: str,
        database_type: str,
        connection_name: Optional[str],
        format: str,
        recipients: List[str],
    ) -> ReportSchedule:
        # Validate cron expression
        parse_cron(cron)
        now = datetime.now(timezone.utc)
        schedule_id = f"rs_{uuid.uuid4().hex[:12]}"
        next_run_at = next_run(cron, now)

        schedule = ReportSchedule(
            schedule_id=schedule_id,
            user_id=user_id,
            name=name,
            cron=cron,
            sql_query=sql_query,
            database_type=database_type,
            connection_name=connection_name,
            format=format,
            recipients=recipients,
            created_at=now.isoformat(),
            updated_at=now.isoformat(),
            next_run_at=next_run_at.isoformat(),
            active=True,
        )

        await redis_client.set(f"{cls.KEY_PREFIX}{schedule_id}", asdict(schedule), ttl=90 * 24 * 3600)
        await redis_client._client.lpush(f"{cls.USER_KEY_PREFIX}{user_id}", schedule_id)
        await redis_client._client.ltrim(f"{cls.USER_KEY_PREFIX}{user_id}", 0, 199)
        return schedule

    @classmethod
    async def list_schedules(cls, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        ids = await redis_client._client.lrange(f"{cls.USER_KEY_PREFIX}{user_id}", 0, limit - 1)
        schedules: List[Dict[str, Any]] = []
        for schedule_id in ids:
            item = await redis_client.get(f"{cls.KEY_PREFIX}{schedule_id}")
            if item:
                schedules.append(item)
        return schedules

    @classmethod
    async def get_schedule(cls, schedule_id: str) -> Optional[Dict[str, Any]]:
        return await redis_client.get(f"{cls.KEY_PREFIX}{schedule_id}")

    @classmethod
    async def delete_schedule(cls, schedule_id: str) -> bool:
        item = await cls.get_schedule(schedule_id)
        if not item:
            return False
        user_id = item.get("user_id")
        await redis_client.delete(f"{cls.KEY_PREFIX}{schedule_id}")
        if user_id:
            await redis_client._client.lrem(f"{cls.USER_KEY_PREFIX}{user_id}", 0, schedule_id)
        return True

    @classmethod
    async def update_next_run(
        cls,
        schedule_id: str,
        cron: str,
        last_status: str,
        last_error: Optional[str] = None,
    ) -> None:
        item = await cls.get_schedule(schedule_id)
        if not item:
            return
        now = datetime.now(timezone.utc)
        next_run_at = next_run(cron, now)
        item["updated_at"] = now.isoformat()
        item["next_run_at"] = next_run_at.isoformat()
        item["last_run_at"] = now.isoformat()
        item["last_status"] = last_status
        item["last_error"] = last_error
        await redis_client.set(f"{cls.KEY_PREFIX}{schedule_id}", item, ttl=90 * 24 * 3600)

    @classmethod
    async def list_due_schedules(cls, now: Optional[datetime] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if now is None:
            now = datetime.now(timezone.utc)
        due: List[Dict[str, Any]] = []
        # Scan through recent schedules via users
        keys = await redis_client.keys(f"{cls.KEY_PREFIX}*")
        for key in keys[:limit]:
            item = await redis_client.get(key)
            if not item or not item.get("active", True):
                continue
            next_run_at = item.get("next_run_at")
            if not next_run_at:
                continue
            try:
                next_dt = datetime.fromisoformat(next_run_at)
            except Exception:
                continue
            if next_dt <= now:
                due.append(item)
        return due

