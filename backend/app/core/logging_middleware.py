"""
Request/Response Logging Middleware
- Adds X-Request-ID / X-Trace-ID headers
- Logs method, path, status, latency, user, and trace ids
"""
from __future__ import annotations

import time
import uuid
import logging
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("app.request")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        start = time.time()
        req_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        trace_id = request.headers.get("X-Trace-ID") or req_id
        # Attach to state for downstream usage
        request.state.request_id = req_id
        request.state.trace_id = trace_id
        # Process
        try:
            response: Response = await call_next(request)
            status = response.status_code
        except Exception as e:
            status = 500
            logger.exception("request_error", extra={
                "request_id": req_id,
                "trace_id": trace_id,
                "method": request.method,
                "path": request.url.path,
                "error": str(e),
            })
            raise
        finally:
            latency_ms = int((time.time() - start) * 1000)
            logger.info("request", extra={
                "request_id": req_id,
                "trace_id": trace_id,
                "method": request.method,
                "path": request.url.path,
                "status": status,
                "latency_ms": latency_ms,
                "client": request.client.host if request.client else None,
            })
        # Set headers
        try:
            response.headers["X-Request-ID"] = req_id
            response.headers["X-Trace-ID"] = trace_id
        except Exception:
            pass
        return response
