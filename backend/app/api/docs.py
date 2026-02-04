from fastapi import APIRouter, Depends, Request
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse

from app.core.rbac import require_developer_role
from app.core.config import settings

router = APIRouter()


@router.get("/openapi.json", include_in_schema=False)
async def openapi_json(request: Request, user: dict = Depends(require_developer_role)):
    return JSONResponse(request.app.openapi())


@router.get("/docs", include_in_schema=False)
async def swagger_ui(user: dict = Depends(require_developer_role)) -> HTMLResponse:
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title=f"{settings.app_name} - API Docs",
    )


@router.get("/redoc", include_in_schema=False)
async def redoc_ui(user: dict = Depends(require_developer_role)) -> HTMLResponse:
    return get_redoc_html(
        openapi_url="/openapi.json",
        title=f"{settings.app_name} - ReDoc",
    )
