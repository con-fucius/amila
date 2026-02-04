"""
Custom Metrics Registry
Stores user-defined metrics and exposes them via Prometheus registry.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Optional

from datetime import datetime, timezone
from app.core.redis_client import redis_client
from app.core.prometheus_metrics import custom_metrics_registry

logger = logging.getLogger(__name__)


def _sanitize_metric_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    if not name.startswith("amil_custom_"):
        name = "amil_custom_" + name
    return name


@dataclass
class CustomMetricDefinition:
    metric_id: str
    name: str
    description: str
    type: str  # gauge | counter | histogram
    labels: List[str]
    created_at: str


class CustomMetricsService:
    KEY_PREFIX = "metrics:custom:"

    @classmethod
    async def create_metric(
        cls,
        metric_id: str,
        name: str,
        description: str,
        metric_type: str,
        labels: Optional[List[str]] = None,
    ) -> CustomMetricDefinition:
        labels = labels or []
        if metric_type not in {"gauge", "counter", "histogram"}:
            raise ValueError("metric_type must be gauge, counter, or histogram")
        metric_name = _sanitize_metric_name(name)
        definition = CustomMetricDefinition(
            metric_id=metric_id,
            name=metric_name,
            description=description,
            type=metric_type,
            labels=labels,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        await redis_client.set(f"{cls.KEY_PREFIX}{metric_id}", asdict(definition), ttl=365 * 24 * 3600)
        custom_metrics_registry.ensure_metric(definition)
        return definition

    @classmethod
    async def list_metrics(cls, limit: int = 100) -> List[Dict[str, Any]]:
        keys = await redis_client.keys(f"{cls.KEY_PREFIX}*")
        metrics = []
        for key in keys[:limit]:
            item = await redis_client.get(key)
            if item:
                metrics.append(item)
        return metrics

    @classmethod
    async def delete_metric(cls, metric_id: str) -> bool:
        await redis_client.delete(f"{cls.KEY_PREFIX}{metric_id}")
        custom_metrics_registry.remove_metric(metric_id)
        return True

    @classmethod
    async def record_value(
        cls,
        metric_id: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        definition = await redis_client.get(f"{cls.KEY_PREFIX}{metric_id}")
        if not definition:
            raise ValueError("Metric not found")
        custom_metrics_registry.record(definition, value, labels or {})
        return {"status": "success"}
