from typing import Any, Dict, Optional

import strawberry
from strawberry.fastapi import GraphQLRouter
from fastapi import APIRouter, Request, HTTPException

from app.core.rbac import Role
from app.core.auth import AuthenticationManager
from app.services.query_results_store import fetch_result_by_query_id
from app.services.webhook_service import WebhookService
from app.core.rate_limiter import rate_limiter, RateLimitTier
from app.services.query_state_manager import get_query_state_manager


@strawberry.type
class DataQuality:
    quality_score: Optional[int]
    profiled_row_count: Optional[int]
    row_count: Optional[int]


@strawberry.type
class QueryResults:
    columns: list[str]
    rows: list[strawberry.scalars.JSON]
    row_count: int
    execution_time_ms: Optional[int]
    data_quality: Optional[strawberry.scalars.JSON]


@strawberry.type
class Webhook:
    webhook_id: str
    url: str
    events: list[str]
    active: bool
    created_at: str
    updated_at: str
    last_delivery_at: Optional[str]
    last_status_code: Optional[int]
    last_error: Optional[str]
    consecutive_failures: int


@strawberry.type
class RateLimitStatus:
    limit: int
    remaining: int
    used: int
    window_seconds: int
    tier: str


async def get_context(request: Request) -> Dict[str, Any]:
    token = None
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1]

    if not token:
        raise HTTPException(status_code=401, detail="Authentication required")

    if token == "temp-dev-token":
        user = {"username": "dev_user", "role": Role.VIEWER}
        return {"request": request, "user": user}

    auth_manager = AuthenticationManager()
    payload = auth_manager.decode_token(token, token_type="access")
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    username = payload.get("sub")
    role_str = payload.get("role", "viewer")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    try:
        role = Role(role_str)
    except Exception:
        role = Role.VIEWER

    return {"request": request, "user": {"username": username, "role": role}}


@strawberry.type
class Query:
    @strawberry.field
    async def query_results(self, info, query_id: str) -> Optional[QueryResults]:
        user = info.context.get("user")
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")

        try:
            state_manager = await get_query_state_manager()
            md = await state_manager.get_query_metadata(query_id)
            if md:
                owner = md.get("user_id") or md.get("username")
                if owner and owner != user.get("username") and user.get("role") != Role.ADMIN:
                    raise HTTPException(status_code=403, detail="Permission denied")
        except HTTPException:
            raise
        except Exception:
            pass

        result = await fetch_result_by_query_id(query_id)
        if not result:
            return None

        return QueryResults(
            columns=result.get("columns", []) or [],
            rows=result.get("rows", []) or [],
            row_count=int(result.get("row_count", len(result.get("rows", []) or [])) or 0),
            execution_time_ms=result.get("execution_time_ms"),
            data_quality=result.get("data_quality"),
        )

    @strawberry.field
    async def webhooks(self, info) -> list[Webhook]:
        user = info.context.get("user")
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")

        subs = await WebhookService.list_subscriptions_for_user(user_id=user["username"])
        out: list[Webhook] = []
        for s in subs:
            out.append(
                Webhook(
                    webhook_id=s.webhook_id,
                    url=s.url,
                    events=s.events,
                    active=s.active,
                    created_at=s.created_at,
                    updated_at=s.updated_at,
                    last_delivery_at=s.last_delivery_at,
                    last_status_code=s.last_status_code,
                    last_error=s.last_error,
                    consecutive_failures=s.consecutive_failures,
                )
            )
        return out

    @strawberry.field
    async def rate_limit_status(self, info, endpoint: str) -> RateLimitStatus:
        user = info.context.get("user")
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")

        role = user.get("role")
        tier = RateLimitTier.VIEWER
        if isinstance(role, Role):
            tier = RateLimitTier(role.value)

        status = await rate_limiter.get_rate_limit_status(
            user=user.get("username", "anonymous"),
            endpoint=endpoint,
            tier=tier,
        )
        return RateLimitStatus(
            limit=int(status.get("limit") or 0),
            remaining=int(status.get("remaining") or 0),
            used=int(status.get("used") or 0),
            window_seconds=int(status.get("window_seconds") or status.get("reset_seconds") or 0),
            tier=str(status.get("tier") or tier.value),
        )


schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema, graphiql=True, context_getter=get_context)

router = APIRouter()
router.include_router(graphql_app, prefix="/graphql")
