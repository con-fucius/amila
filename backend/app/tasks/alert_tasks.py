"""
Alert Escalation Tasks
"""

import asyncio

from app.core.celery_app import celery_app
from app.services.alert_escalation_service import AlertEscalationService


@celery_app.task(name="app.tasks.alert_tasks.process_alert_escalations")
def process_alert_escalations():
    return asyncio.run(AlertEscalationService.process_escalations())
