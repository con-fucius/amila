import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class WebhookDeliveryService:
    SIGNATURE_HEADER = "X-Amila-Signature"
    TIMESTAMP_HEADER = "X-Amila-Timestamp"
    EVENT_HEADER = "X-Amila-Event"
    ID_HEADER = "X-Amila-Delivery-Id"

    @classmethod
    def sign_payload(cls, *, secret: str, timestamp: str, body: bytes) -> str:
        msg = timestamp.encode("utf-8") + b"." + body
        sig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()
        return sig

    @classmethod
    def _resolve_secret(cls, subscription_secret: Optional[str]) -> str:
        # If subscription_secret is not provided, fall back to global HMAC secret.
        # This matches existing settings usage pattern (already present in config_manager).
        return (subscription_secret or settings.hmac_secret_key).strip()

    @classmethod
    def build_headers(
        cls,
        *,
        event: str,
        delivery_id: str,
        timestamp: str,
        signature: str,
    ) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            cls.EVENT_HEADER: event,
            cls.ID_HEADER: delivery_id,
            cls.TIMESTAMP_HEADER: timestamp,
            cls.SIGNATURE_HEADER: signature,
        }

    @classmethod
    async def deliver(
        cls,
        *,
        url: str,
        event: str,
        delivery_id: str,
        payload: Dict[str, Any],
        subscription_secret: Optional[str],
        timeout_seconds: float = 10.0,
    ) -> Tuple[int, Optional[str]]:
        timestamp = str(int(time.time()))
        body = json.dumps(payload).encode("utf-8")
        secret = cls._resolve_secret(subscription_secret)
        signature = cls.sign_payload(secret=secret, timestamp=timestamp, body=body)
        headers = cls.build_headers(
            event=event,
            delivery_id=delivery_id,
            timestamp=timestamp,
            signature=signature,
        )

        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            resp = await client.post(url, content=body, headers=headers)
            if 200 <= resp.status_code < 300:
                return resp.status_code, None
            return resp.status_code, (resp.text[:2000] if resp.text else resp.reason_phrase)
