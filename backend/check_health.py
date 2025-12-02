"""Minimal backend health check script.

Runs a quick health probe against the running backend and exits non-zero
if status or dependencies endpoints fail.
"""

from __future__ import annotations

import sys

import httpx


BASE_URL = "http://127.0.0.1:8000"


def main() -> int:
    endpoints = [
        "/api/v1/health/status",
        "/api/v1/health/dependencies",
    ]

    try:
        with httpx.Client(timeout=5.0) as client:
            for path in endpoints:
                url = f"{BASE_URL}{path}"
                resp = client.get(url)
                if resp.status_code != 200:
                    print(f"Health check failed: {url} returned {resp.status_code}", file=sys.stderr)
                    return 1
    except Exception as exc:  # pragma: no cover - simple CLI
        print(f"Health check error: {exc}", file=sys.stderr)
        return 1

    print("Backend health checks passed.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
