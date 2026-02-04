"""
Root Cause Analysis Service
Links anomalies to likely categorical drivers using data correlations.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class RootCauseService:
    @staticmethod
    def _is_numeric(values: List[Any]) -> bool:
        numeric = 0
        for v in values:
            if v is None:
                continue
            try:
                float(v)
                numeric += 1
            except Exception:
                pass
        return numeric / max(len(values), 1) >= 0.6

    @staticmethod
    def analyze(
        columns: List[str],
        rows: List[List[Any]],
        anomalies: List[Dict[str, Any]],
        max_causes: int = 3,
    ) -> List[Dict[str, Any]]:
        if not anomalies or not rows or not columns:
            return []

        # Identify categorical columns
        categorical_idxs = []
        for idx, col in enumerate(columns):
            sample = [row[idx] for row in rows[:50] if len(row) > idx and row[idx] is not None]
            if not sample:
                continue
            if not RootCauseService._is_numeric(sample):
                categorical_idxs.append(idx)

        if not categorical_idxs:
            return []

        results = []
        for anomaly in anomalies[:5]:
            row_idx = anomaly.get("row_index")
            if row_idx is None or row_idx >= len(rows):
                continue
            row = rows[row_idx]
            causes = []
            for idx in categorical_idxs:
                if idx >= len(row):
                    continue
                value = row[idx]
                if value is None:
                    continue
                # Frequency of this value vs overall
                total = 0
                match = 0
                for r in rows:
                    if len(r) <= idx:
                        continue
                    if r[idx] is None:
                        continue
                    total += 1
                    if r[idx] == value:
                        match += 1
                if total == 0:
                    continue
                ratio = match / total
                if ratio >= 0.2:
                    causes.append({
                        "column": columns[idx],
                        "value": value,
                        "support_ratio": round(ratio, 3),
                        "support_count": match,
                    })
            causes.sort(key=lambda c: c["support_ratio"], reverse=True)
            if causes:
                results.append({
                    "anomaly": {
                        "column": anomaly.get("column"),
                        "description": anomaly.get("description"),
                        "row_index": row_idx,
                    },
                    "likely_causes": causes[:max_causes],
                })

        return results

