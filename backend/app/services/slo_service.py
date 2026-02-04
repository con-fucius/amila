"""
SLO/SLI Monitoring Service
Stores SLO definitions and tracks SLI events in Redis.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


@dataclass
class SLODefinition:
    slo_id: str
    name: str
    sli_type: str  # availability | latency_p95 | error_rate
    target: float
    window_minutes: int
    created_at: str
    description: Optional[str] = None


class SLOService:
    KEY_PREFIX = "slo:def:"
    EVENTS_KEY = "sli:events"

    @classmethod
    async def create_slo(
        cls,
        name: str,
        sli_type: str,
        target: float,
        window_minutes: int,
        description: Optional[str] = None,
    ) -> SLODefinition:
        if sli_type not in {"availability", "latency_p95", "error_rate"}:
            raise ValueError("Invalid sli_type")
        slo_id = f"slo_{uuid.uuid4().hex[:10]}"
        definition = SLODefinition(
            slo_id=slo_id,
            name=name,
            sli_type=sli_type,
            target=target,
            window_minutes=window_minutes,
            created_at=datetime.now(timezone.utc).isoformat(),
            description=description,
        )
        await redis_client.set(f"{cls.KEY_PREFIX}{slo_id}", asdict(definition), ttl=365 * 24 * 3600)
        return definition

    @classmethod
    async def list_slos(cls, limit: int = 100) -> List[Dict[str, Any]]:
        keys = await redis_client.keys(f"{cls.KEY_PREFIX}*")
        slos = []
        for key in keys[:limit]:
            item = await redis_client.get(key)
            if item:
                slos.append(item)
        return slos

    @classmethod
    async def delete_slo(cls, slo_id: str) -> bool:
        await redis_client.delete(f"{cls.KEY_PREFIX}{slo_id}")
        return True

    @classmethod
    async def record_event(
        cls,
        success: bool,
        latency_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        event = {
            "ts": now.timestamp(),
            "success": bool(success),
            "latency_ms": latency_ms,
            "metadata": metadata or {},
        }
        # Use sorted set for efficient window filtering
        await redis_client.zadd(cls.EVENTS_KEY, {json.dumps(event): event["ts"]})
        # Keep last 7 days
        cutoff = now.timestamp() - (7 * 24 * 3600)
        await redis_client.zremrangebyscore(cls.EVENTS_KEY, 0, cutoff)

    @classmethod
    async def compute_sli(cls, slo: Dict[str, Any]) -> Dict[str, Any]:
        window_minutes = int(slo.get("window_minutes", 60))
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - (window_minutes * 60)
        raw = await redis_client.zrange(cls.EVENTS_KEY, 0, -1, withscores=True)
        events = []
        for item, score in raw:
            if score < cutoff:
                continue
            try:
                events.append(json.loads(item))
            except Exception:
                continue

        if not events:
            return {"status": "no_data", "sli": None}

        sli_type = slo.get("sli_type")
        if sli_type == "availability":
            success_count = sum(1 for e in events if e.get("success"))
            sli = success_count / max(len(events), 1)
        elif sli_type == "error_rate":
            error_count = sum(1 for e in events if not e.get("success"))
            sli = error_count / max(len(events), 1)
        else:
            # latency_p95
            latencies = [e.get("latency_ms") for e in events if e.get("latency_ms") is not None]
            latencies = [float(v) for v in latencies if v is not None]
            if not latencies:
                return {"status": "no_latency_data", "sli": None}
            latencies.sort()
            idx = int(0.95 * (len(latencies) - 1))
            sli = latencies[idx]

        target = float(slo.get("target", 0))
        status = "ok"
        if sli_type == "error_rate":
            if sli > target:
                status = "breach"
        else:
            if sli < target:
                status = "breach"

        return {
            "status": status,
            "sli": round(float(sli), 4) if sli is not None else None,
            "target": target,
            "window_minutes": window_minutes,
            "events": len(events),
        }

