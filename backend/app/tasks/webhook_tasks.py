import logging
import uuid
from typing import Any, Dict, Optional

from app.core.celery_app import celery_app
from app.services.webhook_service import WebhookService
from app.services.webhook_delivery_service import WebhookDeliveryService

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.webhook_tasks.deliver_webhook_event",
    bind=True,
    max_retries=10,
    default_retry_delay=60,
)
def deliver_webhook_event(
    self,
    *,
    webhook_id: str,
    url: str,
    event: str,
    payload: Dict[str, Any],
    subscription_secret: Optional[str] = None,
) -> Dict[str, Any]:
    delivery_id = f"whd_{uuid.uuid4().hex[:16]}"
    try:
        status_code, error = _deliver_sync(
            url=url,
            event=event,
            delivery_id=delivery_id,
            payload=payload,
            subscription_secret=subscription_secret,
        )

        ok = 200 <= status_code < 300
        try:
            import asyncio

            asyncio.run(
                WebhookService.record_delivery_attempt(
                    webhook_id=webhook_id,
                    status_code=status_code,
                    error=error,
                    success=ok,
                )
            )
        except Exception:
            pass

        if not ok:
            raise RuntimeError(f"Webhook delivery failed: status={status_code}, error={error}")

        return {
            "status": "delivered",
            "webhook_id": webhook_id,
            "delivery_id": delivery_id,
            "status_code": status_code,
        }

    except Exception as e:
        logger.warning(f"Webhook delivery attempt failed: {webhook_id} ({event}) -> {url}: {e}")
        countdown = min(60 * 60, 2 ** int(self.request.retries or 0))
        raise self.retry(exc=e, countdown=countdown)


def _deliver_sync(
    *,
    url: str,
    event: str,
    delivery_id: str,
    payload: Dict[str, Any],
    subscription_secret: Optional[str],
) -> tuple[int, Optional[str]]:
    import asyncio

    async def _run():
        return await WebhookDeliveryService.deliver(
            url=url,
            event=event,
            delivery_id=delivery_id,
            payload=payload,
            subscription_secret=subscription_secret,
        )

    return asyncio.run(_run())
