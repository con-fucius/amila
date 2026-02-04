import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.core.celery_fallback import execute_with_fallback, TaskPriority
from app.services.webhook_service import WebhookService
from app.services.webhook_delivery_service import WebhookDeliveryService

logger = logging.getLogger(__name__)


class WebhookDispatcher:
    @classmethod
    async def dispatch(
        cls,
        *,
        user_id: str,
        event: str,
        payload: Dict[str, Any],
        force_webhook_id: Optional[str] = None,
    ) -> None:
        try:
            if force_webhook_id:
                sub = await WebhookService.get_subscription(force_webhook_id)
                subs = [sub] if sub and sub.user_id == user_id else []
            else:
                subs = await WebhookService.list_active_for_event(user_id=user_id, event=event)

            if not subs:
                return

            from app.tasks.webhook_tasks import deliver_webhook_event

            for s in subs:
                if not s:
                    continue

                delivery_kwargs = {
                    "webhook_id": s.webhook_id,
                    "url": s.url,
                    "event": event,
                    "payload": payload,
                    "subscription_secret": s.secret,
                }

                async def _deliver_async(
                    *,
                    webhook_id: str,
                    url: str,
                    event: str,
                    payload: Dict[str, Any],
                    subscription_secret: Optional[str] = None,
                ):
                    delivery_id = f"whd_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
                    status_code, error = await WebhookDeliveryService.deliver(
                        url=url,
                        event=event,
                        delivery_id=delivery_id,
                        payload=payload,
                        subscription_secret=subscription_secret,
                    )
                    ok = 200 <= status_code < 300
                    await WebhookService.record_delivery_attempt(
                        webhook_id=webhook_id,
                        status_code=status_code,
                        error=error,
                        success=ok,
                    )
                    if not ok:
                        raise RuntimeError(
                            f"Webhook delivery failed: status={status_code}, error={error}"
                        )

                await execute_with_fallback(
                    task_name="app.tasks.webhook_tasks.deliver_webhook_event",
                    func=_deliver_async,
                    kwargs=delivery_kwargs,
                    priority=TaskPriority.NORMAL,
                    celery_task=deliver_webhook_event,
                )
        except Exception as e:
            logger.warning(f"Webhook dispatch failed (non-fatal): {e}")

    @classmethod
    def dispatch_background(
        cls,
        *,
        user_id: str,
        event: str,
        payload: Dict[str, Any],
        force_webhook_id: Optional[str] = None,
    ) -> None:
        try:
            asyncio.create_task(
                cls.dispatch(
                    user_id=user_id,
                    event=event,
                    payload=payload,
                    force_webhook_id=force_webhook_id,
                )
            )
        except Exception:
            pass

    @classmethod
    def build_terminal_event_payload(
        cls,
        *,
        query_id: str,
        state: str,
        timestamp: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        meta = dict(metadata or {})
        result = meta.get("result")
        if isinstance(result, dict) and isinstance(result.get("rows"), list):
            if len(result["rows"]) > 50:
                result = {**result, "rows": result["rows"][:50], "truncated": True}
                meta["result"] = result

        return {
            "query_id": query_id,
            "state": state,
            "timestamp": timestamp,
            "metadata": meta,
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        }
