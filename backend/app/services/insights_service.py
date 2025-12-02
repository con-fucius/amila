"""
Insights Service
Generates natural language insights and suggested follow-up queries from query results.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from app.orchestrator import get_llm  # reuse provider config
from app.core.langfuse_client import log_generation
from app.core.structured_logging import get_trace_id

logger = logging.getLogger(__name__)


class InsightsService:
    @staticmethod
    async def generate_insights(sql: str, columns: List[str], rows: List[List[Any]], max_rows: int = 50) -> Dict[str, List[str]]:
        """Use LLM to produce concise insights and NL suggested queries.
        Returns {"insights": [...], "suggested_queries": [...]}.
        """
        try:
            llm = get_llm()
            # Limit rows to avoid overflow
            rows_limited = rows[:max_rows]
            preview = {"columns": columns[:20], "rows": rows_limited[:50]}
            prompt = (
                "You are a conversational data assistant like GitHub Copilot. Given the SQL and result preview, "
                "provide engaging insights and actionable next steps.\n\n"
                f"SQL:\n{sql[:1500]}\n\nResult Preview:\n{preview}\n\n"
                "Generate:\n"
                "1. insights: 3-5 conversational observations (e.g., 'I found 10 customers with revenue over $100K')\n"
                "2. suggested_queries: 3-5 actionable follow-ups (e.g., 'Show me their purchase history', "
                "'Export this to CSV', 'Compare with last quarter', 'Visualize as a chart')\n\n"
                "Make suggestions feel natural and anticipate user needs. "
                "Return JSON: {\"insights\": [...], \"suggested_queries\": [...]}"
            )
            resp = await llm.ainvoke([{"type": "system", "content": "Return JSON only."}], prompt)  # type: ignore[arg-type]
            text = getattr(resp, "content", "{}")
            import json

            data = {}
            try:
                data = json.loads(text)
            except Exception:
                # try to extract JSON substring
                import re

                m = re.search(r"\{[\s\S]*\}", text)
                if m:
                    data = json.loads(m.group(0))
            insights = data.get("insights") or []
            suggested = data.get("suggested_queries") or []
            # Coerce to strings
            insights = [str(x) for x in insights][:6]
            suggested = [str(x) for x in suggested][:6]

            trace_id = get_trace_id()
            if trace_id:
                try:
                    log_generation(
                        trace_id=trace_id,
                        name="insights.generate_insights",
                        model="unknown",  # model name is resolved inside get_llm; omit here
                        input_data={
                            "sql_preview": sql[:500],
                            "columns": columns[:20],
                        },
                        output_data={
                            "insights_count": len(insights),
                            "suggested_queries_count": len(suggested),
                        },
                        metadata={"stage": "results_insights"},
                    )
                except Exception:
                    pass

            return {"insights": insights, "suggested_queries": suggested}
        except Exception as e:
            logger.warning(f"Insights generation failed: {e}")
            return {"insights": [], "suggested_queries": []}
