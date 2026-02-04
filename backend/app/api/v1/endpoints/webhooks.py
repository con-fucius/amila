from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.core.rbac import rbac_manager, require_analyst_role
from app.services.webhook_service import WebhookService, WebhookSubscription

router = APIRouter(prefix="/webhooks")


class WebhookCreateRequest(BaseModel):
    url: str
    events: List[str] = Field(default_factory=list)
    active: bool = True
    secret: Optional[str] = None


class WebhookUpdateRequest(BaseModel):
    url: Optional[str] = None
    events: Optional[List[str]] = None
    active: Optional[bool] = None
    secret: Optional[str] = None


@router.get("")
async def list_webhooks(user: dict = Depends(rbac_manager.get_current_user)) -> Dict[str, Any]:
    subs = await WebhookService.list_subscriptions_for_user(user_id=user["username"])
    return {"status": "success", "webhooks": [s.model_dump() for s in subs]}


@router.post("")
async def create_webhook(
    body: WebhookCreateRequest,
    user: dict = Depends(require_analyst_role),
) -> Dict[str, Any]:
    sub = await WebhookService.create_subscription(
        user_id=user["username"],
        url=body.url,
        events=body.events,
        secret=body.secret,
        active=body.active,
    )
    return {"status": "success", "webhook": sub.model_dump()}


@router.put("/{webhook_id}")
async def update_webhook(
    webhook_id: str,
    body: WebhookUpdateRequest,
    user: dict = Depends(require_analyst_role),
) -> Dict[str, Any]:
    sub = await WebhookService.update_subscription(
        webhook_id=webhook_id,
        user_id=user["username"],
        url=body.url,
        events=body.events,
        secret=body.secret,
        active=body.active,
    )
    if not sub:
        raise HTTPException(status_code=404, detail="Webhook not found")
    return {"status": "success", "webhook": sub.model_dump()}


@router.delete("/{webhook_id}")
async def delete_webhook(
    webhook_id: str,
    user: dict = Depends(require_analyst_role),
) -> Dict[str, Any]:
    ok = await WebhookService.delete_subscription(webhook_id=webhook_id, user_id=user["username"])
    if not ok:
        raise HTTPException(status_code=404, detail="Webhook not found")
    return {"status": "success", "deleted": webhook_id}


@router.post("/{webhook_id}/test")
async def test_webhook(
    webhook_id: str,
    user: dict = Depends(require_analyst_role),
) -> Dict[str, Any]:
    sub = await WebhookService.get_subscription(webhook_id)
    if not sub or sub.user_id != user["username"]:
        raise HTTPException(status_code=404, detail="Webhook not found")

    from app.services.webhook_dispatcher import WebhookDispatcher

    await WebhookDispatcher.dispatch(
        user_id=user["username"],
        event="webhook.test",
        payload={"message": "test", "webhook_id": webhook_id},
        force_webhook_id=webhook_id,
    )
    return {"status": "success"}
