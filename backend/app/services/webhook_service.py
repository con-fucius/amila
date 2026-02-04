import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


class WebhookSubscription(BaseModel):
    webhook_id: str
    user_id: str
    url: str
    events: List[str] = Field(default_factory=list)
    active: bool = True
    secret: Optional[str] = None
    created_at: str
    updated_at: str

    last_delivery_at: Optional[str] = None
    last_status_code: Optional[int] = None
    last_error: Optional[str] = None
    consecutive_failures: int = 0


class WebhookService:
    KEY_PREFIX = "webhook:"
    USER_INDEX_PREFIX = "webhook_user:"

    @classmethod
    async def create_subscription(
        cls,
        *,
        user_id: str,
        url: str,
        events: List[str],
        secret: Optional[str] = None,
        active: bool = True,
    ) -> WebhookSubscription:
        webhook_id = f"wh_{uuid.uuid4().hex[:16]}"
        now = datetime.now(timezone.utc).isoformat()
        sub = WebhookSubscription(
            webhook_id=webhook_id,
            user_id=user_id,
            url=url,
            events=sorted(list({e.strip() for e in (events or []) if isinstance(e, str) and e.strip()})),
            active=bool(active),
            secret=secret,
            created_at=now,
            updated_at=now,
        )
        await cls._set_subscription(sub)
        await cls._index_subscription(user_id=user_id, webhook_id=webhook_id)
        return sub

    @classmethod
    async def update_subscription(
        cls,
        *,
        webhook_id: str,
        user_id: str,
        url: Optional[str] = None,
        events: Optional[List[str]] = None,
        secret: Optional[str] = None,
        active: Optional[bool] = None,
    ) -> Optional[WebhookSubscription]:
        current = await cls.get_subscription(webhook_id)
        if not current or current.user_id != user_id:
            return None

        now = datetime.now(timezone.utc).isoformat()
        data = current.model_dump()
        if url is not None:
            data["url"] = url
        if events is not None:
            data["events"] = sorted(list({e.strip() for e in (events or []) if isinstance(e, str) and e.strip()}))
        if active is not None:
            data["active"] = bool(active)
        if secret is not None:
            data["secret"] = secret
        data["updated_at"] = now

        sub = WebhookSubscription(**data)
        await cls._set_subscription(sub)
        return sub

    @classmethod
    async def delete_subscription(cls, *, webhook_id: str, user_id: str) -> bool:
        current = await cls.get_subscription(webhook_id)
        if not current or current.user_id != user_id:
            return False

        cache = getattr(redis_client, "_cache_client", None)
        if not cache:
            return False

        await cache.delete(f"{cls.KEY_PREFIX}{webhook_id}")
        await cache.srem(f"{cls.USER_INDEX_PREFIX}{user_id}", webhook_id)
        return True

    @classmethod
    async def get_subscription(cls, webhook_id: str) -> Optional[WebhookSubscription]:
        cache = getattr(redis_client, "_cache_client", None)
        if not cache:
            return None

        raw = await cache.get(f"{cls.KEY_PREFIX}{webhook_id}")
        if not raw:
            return None

        try:
            data = json.loads(raw)
            return WebhookSubscription(**data)
        except Exception as e:
            logger.warning(f"Failed to parse webhook subscription {webhook_id}: {e}")
            return None

    @classmethod
    async def list_subscriptions_for_user(cls, *, user_id: str) -> List[WebhookSubscription]:
        cache = getattr(redis_client, "_cache_client", None)
        if not cache:
            return []

        ids = await cache.smembers(f"{cls.USER_INDEX_PREFIX}{user_id}")
        results: List[WebhookSubscription] = []
        for webhook_id in sorted(list(ids or [])):
            sub = await cls.get_subscription(webhook_id)
            if sub and sub.user_id == user_id:
                results.append(sub)
        return results

    @classmethod
    async def list_active_for_event(cls, *, user_id: str, event: str) -> List[WebhookSubscription]:
        subs = await cls.list_subscriptions_for_user(user_id=user_id)
        event_norm = (event or "").strip()
        out: List[WebhookSubscription] = []
        for s in subs:
            if not s.active:
                continue
            if not s.events:
                continue
            if event_norm in s.events or "*" in s.events:
                out.append(s)
        return out

    @classmethod
    async def record_delivery_attempt(
        cls,
        *,
        webhook_id: str,
        status_code: Optional[int] = None,
        error: Optional[str] = None,
        success: bool,
    ) -> None:
        sub = await cls.get_subscription(webhook_id)
        if not sub:
            return

        now = datetime.now(timezone.utc).isoformat()
        data = sub.model_dump()
        data["last_delivery_at"] = now
        data["last_status_code"] = status_code
        data["last_error"] = error
        if success:
            data["consecutive_failures"] = 0
        else:
            data["consecutive_failures"] = int(data.get("consecutive_failures") or 0) + 1
        data["updated_at"] = now

        await cls._set_subscription(WebhookSubscription(**data))

    @classmethod
    async def _set_subscription(cls, sub: WebhookSubscription) -> None:
        cache = getattr(redis_client, "_cache_client", None)
        if not cache:
            return
        await cache.set(f"{cls.KEY_PREFIX}{sub.webhook_id}", sub.model_dump_json())

    @classmethod
    async def _index_subscription(cls, *, user_id: str, webhook_id: str) -> None:
        cache = getattr(redis_client, "_cache_client", None)
        if not cache:
            return
        await cache.sadd(f"{cls.USER_INDEX_PREFIX}{user_id}", webhook_id)
