"""
Notification Service
Email delivery via SMTP
"""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import List, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class EmailConfig:
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    use_tls: bool
    from_address: str


def _load_email_config() -> EmailConfig:
    return EmailConfig(
        host=settings.SMTP_HOST,
        port=settings.SMTP_PORT,
        username=settings.SMTP_USERNAME,
        password=settings.SMTP_PASSWORD,
        use_tls=settings.SMTP_USE_TLS,
        from_address=settings.SMTP_FROM_ADDRESS,
    )


class NotificationService:
    """Email notification service"""

    @staticmethod
    def send_email(
        to_addresses: List[str],
        subject: str,
        body: str,
        html_body: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> None:
        config = _load_email_config()
        if not config.host or not config.from_address:
            raise ValueError("SMTP_HOST and SMTP_FROM_ADDRESS must be configured")

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = config.from_address
        msg["To"] = ", ".join(to_addresses)
        if reply_to:
            msg["Reply-To"] = reply_to

        if html_body:
            msg.set_content(body)
            msg.add_alternative(html_body, subtype="html")
        else:
            msg.set_content(body)

        try:
            with smtplib.SMTP(config.host, config.port, timeout=20) as server:
                if config.use_tls:
                    server.starttls()
                if config.username and config.password:
                    server.login(config.username, config.password)
                server.send_message(msg)
            logger.info("Email sent to %s", ",".join(to_addresses))
        except Exception as e:
            logger.error("Email delivery failed: %s", e)
            raise

