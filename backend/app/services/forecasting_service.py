"""
Time Series Forecasting Service
Simple Holt linear method with confidence intervals.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ForecastPoint:
    timestamp: str
    value: float
    lower: float
    upper: float


class ForecastingService:
    @staticmethod
    def _extract_time_series(columns: List[str], rows: List[List[Any]]) -> Tuple[Optional[int], List[int]]:
        date_idx = None
        numeric_idxs = []
        for idx, col in enumerate(columns):
            col_l = col.lower()
            if date_idx is None and any(k in col_l for k in ["date", "time", "month", "year", "day", "week"]):
                date_idx = idx
                continue
        # numeric columns (skip date column)
        for idx, col in enumerate(columns):
            if idx == date_idx:
                continue
            sample = [row[idx] for row in rows[:50] if len(row) > idx and row[idx] is not None]
            if not sample:
                continue
            numeric = 0
            for v in sample:
                try:
                    float(v)
                    numeric += 1
                except Exception:
                    pass
            if numeric / max(len(sample), 1) >= 0.6:
                numeric_idxs.append(idx)
        return date_idx, numeric_idxs

    @staticmethod
    def _parse_dates(rows: List[List[Any]], date_idx: int) -> List[datetime]:
        dates = []
        for row in rows:
            if len(row) <= date_idx:
                continue
            v = row[date_idx]
            if isinstance(v, datetime):
                dates.append(v)
            else:
                try:
                    dates.append(datetime.fromisoformat(str(v)))
                except Exception:
                    try:
                        dates.append(datetime.strptime(str(v), "%Y-%m-%d"))
                    except Exception:
                        continue
        return dates

    @staticmethod
    def _holt_forecast(values: List[float], steps: int = 6, alpha: float = 0.4, beta: float = 0.2) -> List[float]:
        if len(values) < 2:
            return values[-1:] * steps
        level = values[0]
        trend = values[1] - values[0]
        for i in range(1, len(values)):
            v = values[i]
            prev_level = level
            level = alpha * v + (1 - alpha) * (level + trend)
            trend = beta * (level - prev_level) + (1 - beta) * trend
        return [level + (i + 1) * trend for i in range(steps)]

    @staticmethod
    def forecast_time_series(
        columns: List[str],
        rows: List[List[Any]],
        steps: int = 6,
    ) -> Dict[str, Any]:
        if not rows or not columns:
            return {"status": "skipped", "reason": "no_data"}

        date_idx, numeric_idxs = ForecastingService._extract_time_series(columns, rows)
        if date_idx is None or not numeric_idxs:
            return {"status": "skipped", "reason": "no_time_series"}

        # Sort rows by date
        parsed_dates = ForecastingService._parse_dates(rows, date_idx)
        if not parsed_dates or len(parsed_dates) < 3:
            return {"status": "skipped", "reason": "insufficient_dates"}

        # Build ordered pairs
        pairs = []
        for row in rows:
            if len(row) <= date_idx:
                continue
            try:
                dt = datetime.fromisoformat(str(row[date_idx]))
            except Exception:
                try:
                    dt = datetime.strptime(str(row[date_idx]), "%Y-%m-%d")
                except Exception:
                    continue
            pairs.append((dt, row))
        pairs.sort(key=lambda x: x[0])

        forecasts = {}
        for n_idx in numeric_idxs[:2]:
            series = []
            for _, row in pairs:
                try:
                    series.append(float(row[n_idx]))
                except Exception:
                    pass
            if len(series) < 3:
                continue
            preds = ForecastingService._holt_forecast(series, steps=steps)
            # Confidence interval using residual std
            residuals = [series[i] - (series[i - 1] if i > 0 else series[0]) for i in range(1, len(series))]
            std = (sum((r - (sum(residuals) / len(residuals))) ** 2 for r in residuals) / max(len(residuals), 1)) ** 0.5
            last_date = pairs[-1][0]
            # Assume daily step for now
            points: List[ForecastPoint] = []
            for i, p in enumerate(preds):
                ts = (last_date.replace(hour=0, minute=0, second=0, microsecond=0) + (i + 1) * (last_date - pairs[-2][0]))
                points.append(ForecastPoint(
                    timestamp=ts.isoformat(),
                    value=round(p, 4),
                    lower=round(p - 1.96 * std, 4),
                    upper=round(p + 1.96 * std, 4),
                ))
            forecasts[columns[n_idx]] = [point.__dict__ for point in points]

        if not forecasts:
            return {"status": "skipped", "reason": "insufficient_numeric_series"}

        return {
            "status": "success",
            "date_column": columns[date_idx],
            "forecasts": forecasts,
            "steps": steps,
            "confidence_level": 0.95,
        }

