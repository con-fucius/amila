"""
Narrative Service
Generates data storytelling narratives with strict anti-hallucination constraints.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from app.orchestrator.llm_config import get_llm

logger = logging.getLogger(__name__)


class NarrativeService:
    @staticmethod
    def build_context_summary(
        query_results: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        trends: List[Dict[str, Any]],
        anomalies: List[Dict[str, Any]],
        user_queries: Optional[List[str]] = None,
    ) -> str:
        summary = []
        for i, result in enumerate(query_results[:3]):
            cols = result.get("columns", [])
            rows = result.get("rows", [])[:5]
            row_count = result.get("row_count", len(result.get("rows", [])))
            query_text = user_queries[i] if user_queries and i < len(user_queries) else f"Query {i+1}"
            summary.append(f"Query: {query_text}")
            summary.append(f"Columns: {', '.join(cols[:8])}")
            summary.append(f"Total rows: {row_count}")
            if rows:
                summary.append(f"Sample rows: {json.dumps(rows[:3], default=str)[:600]}")
            summary.append("")

        metrics_summary = [
            f"- {m.get('name')}: sum={m.get('sum')}, avg={m.get('avg')}, min={m.get('min')}, max={m.get('max')}"
            for m in metrics[:5]
        ]
        trend_summary = [
            f"- {t.get('metric')}: {t.get('direction')} {t.get('percentage_change')}% ({t.get('period_comparison')})"
            for t in trends[:5]
        ]
        anomaly_summary = [
            f"- {a.get('column')}: {a.get('description')}"
            for a in anomalies[:5]
        ]

        return "\n".join(summary + ["METRICS:", *metrics_summary, "TRENDS:", *trend_summary, "ANOMALIES:", *anomaly_summary])

    @staticmethod
    async def generate_narrative(
        query_results: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        trends: List[Dict[str, Any]],
        anomalies: List[Dict[str, Any]],
        user_queries: Optional[List[str]] = None,
    ) -> str:
        """
        Generate a concise narrative (3-6 sentences) using only provided data.
        """
        llm = get_llm()
        if not llm:
            return NarrativeService._fallback_narrative(metrics, trends, anomalies)

        context = NarrativeService.build_context_summary(
            query_results=query_results,
            metrics=metrics,
            trends=trends,
            anomalies=anomalies,
            user_queries=user_queries,
        )

        prompt = f"""You are a business analyst. Write a concise narrative (3-6 sentences) summarizing the data.

STRICT RULES:
1. ONLY reference values explicitly present in the provided metrics/trends/anomalies.
2. Do NOT infer future outcomes or speculate beyond the data.
3. If data is insufficient for a point, say so briefly.

DATA:
{context}

Return plain text narrative only."""

        try:
            response = await llm.ainvoke(prompt)
            text = response.content if hasattr(response, "content") else str(response)
            return text.strip()[:1200]
        except Exception as e:
            logger.warning("Narrative generation failed: %s", e)
            return NarrativeService._fallback_narrative(metrics, trends, anomalies)

    @staticmethod
    def _fallback_narrative(
        metrics: List[Dict[str, Any]],
        trends: List[Dict[str, Any]],
        anomalies: List[Dict[str, Any]],
    ) -> str:
        parts = []
        if metrics:
            m = metrics[0]
            parts.append(
                f"The most prominent metric is {m.get('name')} with a total of {m.get('sum')} and an average of {m.get('avg')}."
            )
        if trends:
            t = trends[0]
            parts.append(
                f"A notable trend is {t.get('metric')} moving {t.get('direction')} by {t.get('percentage_change')}% ({t.get('period_comparison')})."
            )
        if anomalies:
            a = anomalies[0]
            parts.append(f"An anomaly was detected in {a.get('column')}: {a.get('description')}.")
        if not parts:
            return "The data is consistent and does not show significant anomalies or trends."
        return " ".join(parts)

