"""Minimal Langfuse wiring smoke test.

Run with backend running, e.g.:

    cd backend
    uv run python langfuse_smoke.py

This script:
- Checks /health
- Sends a natural language query to the orchestrator (/api/v1/queries/process)
- Sends a direct SQL query via Query Builder path (/api/v1/queries/submit)

It prints query_id and trace_id for each scenario so you can inspect the
corresponding traces in Langfuse.

The assertions are intentionally loose to avoid brittleness across
schemas and environments.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any, Dict

import httpx


BASE_URL = os.environ.get("SMOKE_API_BASE_URL", "http://127.0.0.1:8000")
AUTH_TOKEN = os.environ.get("SMOKE_AUTH_TOKEN", "temp-dev-token")

# You can override these via env vars if needed
NL_TEST_QUERY = os.environ.get(
    "SMOKE_NL_QUERY",
    "Show a small summary of any table you can safely query.",
)
DIRECT_SQL_TEST_QUERY = os.environ.get(
    "SMOKE_SQL_QUERY",
    # Default Oracle-friendly trivial query; override if needed
    "SELECT 1 FROM DUAL",
)
DIRECT_SQL_CONNECTION = os.environ.get("SMOKE_SQL_CONNECTION", "TestUserCSV")

# Last orchestrator response, used to drive approval/clarification follow-ups
LAST_ORCHESTRATOR_RESPONSE = None


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


async def test_health(client: httpx.AsyncClient) -> bool:
    _print_header("[1] Health check")
    try:
        resp = await client.get("/health")
        resp.raise_for_status()
        data = resp.json()
        print("Health response:", data)
        ok = isinstance(data, dict) and data.get("status") in {"healthy", "ok", "OK"}
        print("Result: ", "PASS" if ok else "WARN (unexpected status)")
        return ok
    except Exception as e:  # pragma: no cover - smoke script
        print("Result: FAIL (exception)")
        print("Error:", repr(e))
        return False


async def test_approval_flow(client: httpx.AsyncClient) -> bool:
    """If the orchestrator requested approval, exercise the /{query_id}/approve path.

    This test is intentionally tolerant: if the orchestrator did not request
    approval, the test is treated as a PASS but marked as skipped in output.
    """
    _print_header("[3] Approval flow /{query_id}/approve")

    if not isinstance(LAST_ORCHESTRATOR_RESPONSE, dict):
        print("No orchestrator response available from previous step; skipping approval test.")
        return True

    data = LAST_ORCHESTRATOR_RESPONSE
    query_id = data.get("query_id")
    if not query_id:
        print("Orchestrator response missing query_id; skipping approval test.")
        return True

    needs_approval = bool(data.get("needs_approval")) or data.get("status") == "pending_approval"
    if not needs_approval:
        print("Orchestrator did not request approval; skipping approval test (PASS as not applicable).")
        return True

    print(f"Using query_id for approval test: {query_id}")

    try:
        resp = await client.post(f"/api/v1/queries/{query_id}/approve", json={"approved": True})
        status_code = resp.status_code
        body = resp.json()
        print("Status code:", status_code)
        print("Response:", body)

        ok_shape = isinstance(body, dict) and body.get("query_id") == query_id
        if not ok_shape:
            print("Result: FAIL (unexpected approval response shape)")
            return False

        if 200 <= status_code < 300:
            print("Result: PASS (approval endpoint reachable and structured)")
        else:
            print("Result: WARN (approval endpoint returned non-2xx but structured error)")
        return True
    except Exception as e:  # pragma: no cover - smoke script
        print("Result: FAIL (exception during approval)")
        print("Error:", repr(e))
        return False


async def test_clarification_flow(client: httpx.AsyncClient) -> bool:
    """If the orchestrator requested clarification, exercise the /clarify path."""
    _print_header("[4] Clarification flow /clarify")

    if not isinstance(LAST_ORCHESTRATOR_RESPONSE, dict):
        print("No orchestrator response available from previous step; skipping clarification test.")
        return True

    data = LAST_ORCHESTRATOR_RESPONSE
    query_id = data.get("query_id")
    if not query_id:
        print("Orchestrator response missing query_id; skipping clarification test.")
        return True

    status = data.get("status")
    clarification_message = data.get("clarification_message")
    if status != "clarification_needed" and not clarification_message:
        print("Orchestrator did not request clarification; skipping clarification test (PASS as not applicable).")
        return True

    clarification_text = os.environ.get(
        "SMOKE_CLARIFICATION_TEXT",
        clarification_message or "User clarification for smoke test; please disambiguate any ambiguous columns.",
    )
    original_query = data.get("sql_query") or data.get("user_query") or NL_TEST_QUERY

    payload: Dict[str, Any] = {
        "query_id": query_id,
        "clarification": clarification_text,
        "original_query": original_query,
    }

    print(f"Using query_id for clarification test: {query_id}")

    try:
        resp = await client.post("/api/v1/queries/clarify", json=payload)
        status_code = resp.status_code
        body = resp.json()
        print("Status code:", status_code)
        print("Response:", body)

        ok_shape = isinstance(body, dict) and body.get("query_id") == query_id
        if not ok_shape:
            print("Result: FAIL (unexpected clarification response shape)")
            return False

        if 200 <= status_code < 300:
            print("Result: PASS (clarification endpoint reachable and structured)")
        else:
            print("Result: WARN (clarification endpoint returned non-2xx but structured error)")
        return True
    except Exception as e:  # pragma: no cover - smoke script
        print("Result: FAIL (exception during clarification)")
        print("Error:", repr(e))
        return False


async def test_orchestrator_query(client: httpx.AsyncClient) -> bool:
    _print_header("[2] Orchestrator /process (chat surface)")
    payload: Dict[str, Any] = {
        "query": NL_TEST_QUERY,
        "user_id": "smoke_tester",
        "session_id": f"smoke_session_{int(time.time())}",
    }
    try:
        resp = await client.post("/api/v1/queries/process", json=payload)
        resp.raise_for_status()
        data = resp.json()
        print("Response:", data)

        global LAST_ORCHESTRATOR_RESPONSE
        LAST_ORCHESTRATOR_RESPONSE = data

        query_id = data.get("query_id")
        status = data.get("status")
        trace_id = data.get("trace_id")

        ok = bool(query_id) and bool(status)
        print(f"query_id: {query_id}")
        print(f"status: {status}")
        print(f"trace_id (for Langfuse search): {trace_id}")

        if not ok:
            print("Result: FAIL (missing query_id or status)")
            return False

        # We don't enforce a particular status here; any of
        # {success, pending_approval, clarification_needed, error}
        # is acceptable for wiring validation.
        print("Result: PASS (orchestrator path reachable and structured)")
        return True
    except Exception as e:  # pragma: no cover - smoke script
        print("Result: FAIL (exception)")
        print("Error:", repr(e))
        return False


async def test_direct_sql_submit(client: httpx.AsyncClient) -> bool:
    _print_header("[5] Direct SQL /submit (Query Builder surface)")
    payload: Dict[str, Any] = {
        "query": DIRECT_SQL_TEST_QUERY,
        "connection_name": DIRECT_SQL_CONNECTION,
    }
    try:
        resp = await client.post("/api/v1/queries/submit", json=payload)
        # For wiring, we only require that the endpoint responds with JSON;
        # if the SQL is invalid, we still want to see a structured error.
        data = resp.json()
        print("Status code:", resp.status_code)
        print("Response:", data)

        query_id = data.get("query_id")
        status = data.get("status") or data.get("message")

        # execute_sql_query attaches trace_id and query_id inside "results" in some paths;
        # we surface whichever we find so you can look it up in Langfuse.
        trace_id = None
        if isinstance(data.get("results"), dict):
            trace_id = data["results"].get("trace_id") or data["results"].get("traceId")
        trace_id = trace_id or data.get("trace_id")

        print(f"query_id: {query_id}")
        print(f"status/message: {status}")
        print(f"trace_id (for Langfuse search): {trace_id}")

        ok_shape = isinstance(data, dict) and bool(query_id)
        if not ok_shape:
            print("Result: FAIL (missing query_id or invalid JSON shape)")
            return False

        # 2xx + well-shaped JSON is considered a pass for wiring purposes,
        # regardless of whether the SQL actually succeeded.
        if 200 <= resp.status_code < 300:
            print("Result: PASS (direct SQL path reachable and structured)")
        else:
            print("Result: WARN (non-2xx status but structured error)")
        return True
    except Exception as e:  # pragma: no cover - smoke script
        print("Result: FAIL (exception)")
        print("Error:", repr(e))
        return False


async def main() -> int:
    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
    }

    async with httpx.AsyncClient(base_url=BASE_URL, headers=headers, timeout=30.0) as client:
        results = []
        results.append(await test_health(client))
        results.append(await test_orchestrator_query(client))
        results.append(await test_approval_flow(client))
        results.append(await test_clarification_flow(client))
        results.append(await test_direct_sql_submit(client))

    all_ok = all(results)
    print("\nSUMMARY:")
    for i, ok in enumerate(results, start=1):
        print(f"  [{i}] {'PASS' if ok else 'FAIL'}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        exit_code = 1
    sys.exit(exit_code)
