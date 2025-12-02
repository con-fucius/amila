from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.responses import StreamingResponse
import httpx
from app.core.config import settings
from app.core.doris_client import doris_client
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/sse")
async def proxy_sse(request: Request):
    """
    Proxy SSE connection to the internal Doris MCP server.
    Allows frontend or MCP Inspector to connect via the backend.
    """
    if not settings.DORIS_MCP_ENABLED:
        raise HTTPException(status_code=503, detail="Doris MCP disabled")

    target_url = doris_client.sse_url
    
    async def stream_generator():
        async with httpx.AsyncClient(timeout=None) as client:
            try:
                async with client.stream("GET", target_url) as resp:
                    async for line in resp.aiter_lines():
                        yield f"{line}\n"
            except Exception as e:
                logger.error(f"SSE Proxy error: {e}")
                yield f"event: error\ndata: {str(e)}\n\n"

    return StreamingResponse(stream_generator(), media_type="text/event-stream")

@router.post("/messages")
async def proxy_messages(request: Request):
    """
    Proxy JSON-RPC messages to the internal Doris MCP server.
    """
    if not settings.DORIS_MCP_ENABLED:
        raise HTTPException(status_code=503, detail="Doris MCP disabled")

    target_url = doris_client.messages_url
    body = await request.json()

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(target_url, json=body)
            return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
        except Exception as e:
            logger.error(f"Message Proxy error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
