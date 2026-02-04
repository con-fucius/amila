import hashlib
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


class DataQualityService:
    @staticmethod
    def profile_results(
        *,
        columns: List[str],
        rows: List[Any],
        row_count: int,
        max_profile_rows: int = 5000,
    ) -> Dict[str, Any]:
        sampled_rows, scope = DataQualityService._sample_rows(rows, row_count, max_profile_rows)

        column_profiles = DataQualityService._profile_columns(columns, sampled_rows)
        duplicate_summary = DataQualityService._detect_duplicates(columns, sampled_rows)

        quality_score, quality_factors = DataQualityService._compute_quality_score(
            row_count=row_count,
            column_profiles=column_profiles,
            duplicate_summary=duplicate_summary,
            profiled_row_count=len(sampled_rows),
        )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": scope,
            "profiled_row_count": len(sampled_rows),
            "row_count": row_count,
            "column_profiles": column_profiles,
            "duplicate_summary": duplicate_summary,
            "quality_score": quality_score,
            "quality_factors": quality_factors,
        }

    @staticmethod
    def _sample_rows(rows: List[Any], row_count: int, max_rows: int) -> Tuple[List[Any], str]:
        if not isinstance(rows, list):
            return [], "none"

        effective_count = row_count if isinstance(row_count, int) and row_count >= 0 else len(rows)
        if effective_count <= max_rows and len(rows) <= max_rows:
            return rows, "full"
        return rows[:max_rows], "sample"

    @staticmethod
    def _get_value(row: Any, idx: int, name: str) -> Any:
        if isinstance(row, dict):
            return row.get(name)
        try:
            return row[idx]
        except Exception:
            return None

    @staticmethod
    def _profile_columns(columns: List[str], rows: List[Any]) -> List[Dict[str, Any]]:
        profiles: List[Dict[str, Any]] = []
        if not columns:
            return profiles

        total = len(rows)
        for idx, name in enumerate(columns):
            null_count = 0
            non_null_vals: List[Any] = []
            numeric_vals: List[float] = []

            for r in rows:
                v = DataQualityService._get_value(r, idx, name)
                if v is None or (isinstance(v, str) and v.strip() == ""):
                    null_count += 1
                    continue
                non_null_vals.append(v)
                try:
                    numeric_vals.append(float(v))
                except Exception:
                    pass

            distinct_count = len(set(DataQualityService._stable_value(v) for v in non_null_vals))
            null_ratio = (null_count / total) if total else 0.0

            prof: Dict[str, Any] = {
                "name": name,
                "null_count": null_count,
                "null_ratio": round(null_ratio, 6),
                "distinct_count": distinct_count,
            }

            if numeric_vals:
                prof.update(DataQualityService._numeric_profile(numeric_vals))
            else:
                prof.update(DataQualityService._categorical_profile(non_null_vals))

            profiles.append(prof)

        return profiles

    @staticmethod
    def _stable_value(v: Any) -> str:
        if v is None:
            return "<null>"
        if isinstance(v, (str, int, float, bool)):
            return str(v)
        return repr(v)

    @staticmethod
    def _numeric_profile(vals: List[float]) -> Dict[str, Any]:
        n = len(vals)
        if n == 0:
            return {"type": "numeric"}

        mn = min(vals)
        mx = max(vals)
        mean = sum(vals) / n
        var = sum((x - mean) ** 2 for x in vals) / n
        std = math.sqrt(var)

        outliers = 0
        if std > 0 and n >= 10:
            outliers = sum(1 for x in vals if abs(x - mean) > 3 * std)

        return {
            "type": "numeric",
            "min": mn,
            "max": mx,
            "mean": mean,
            "std": std,
            "outliers_count": outliers,
        }

    @staticmethod
    def _categorical_profile(vals: List[Any]) -> Dict[str, Any]:
        if not vals:
            return {"type": "categorical", "top_values": []}

        normalized = [DataQualityService._stable_value(v) for v in vals]
        counts = Counter(normalized)
        top_values = [{"value": v, "count": c} for v, c in counts.most_common(5)]
        return {"type": "categorical", "top_values": top_values}

    @staticmethod
    def _detect_duplicates(columns: List[str], rows: List[Any]) -> Dict[str, Any]:
        if not rows:
            return {
                "profiled_row_count": 0,
                "duplicate_row_count": 0,
                "duplicate_ratio": 0.0,
            }

        seen: set[str] = set()
        dup = 0
        for r in rows:
            key = DataQualityService._row_fingerprint(columns, r)
            if key in seen:
                dup += 1
            else:
                seen.add(key)

        total = len(rows)
        return {
            "profiled_row_count": total,
            "duplicate_row_count": dup,
            "duplicate_ratio": round((dup / total) if total else 0.0, 6),
        }

    @staticmethod
    def _row_fingerprint(columns: List[str], row: Any) -> str:
        if isinstance(row, dict):
            parts = [f"{c}={DataQualityService._stable_value(row.get(c))}" for c in columns]
        else:
            try:
                parts = [DataQualityService._stable_value(v) for v in row]
            except Exception:
                parts = [DataQualityService._stable_value(row)]
        joined = "|".join(parts)
        return hashlib.sha256(joined.encode("utf-8", errors="ignore")).hexdigest()

    @staticmethod
    def _compute_quality_score(
        *,
        row_count: int,
        column_profiles: List[Dict[str, Any]],
        duplicate_summary: Dict[str, Any],
        profiled_row_count: int,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        score = 100.0
        factors: List[Dict[str, Any]] = []

        if row_count == 0:
            score -= 60
            factors.append({"factor": "empty_result", "penalty": 60})

        high_null_cols = 0
        for p in column_profiles:
            nr = float(p.get("null_ratio") or 0.0)
            if nr >= 0.5:
                high_null_cols += 1

        if high_null_cols > 0:
            penalty = min(30.0, high_null_cols * 5.0)
            score -= penalty
            factors.append({"factor": "high_null_ratio_columns", "count": high_null_cols, "penalty": penalty})

        dup_ratio = float(duplicate_summary.get("duplicate_ratio") or 0.0)
        if dup_ratio >= 0.01:
            penalty = min(40.0, dup_ratio * 200.0)
            score -= penalty
            factors.append({"factor": "duplicate_rows", "duplicate_ratio": dup_ratio, "penalty": penalty})

        outlier_cols = 0
        total_outliers = 0
        for p in column_profiles:
            oc = p.get("outliers_count")
            if isinstance(oc, int) and oc > 0:
                outlier_cols += 1
                total_outliers += oc

        if outlier_cols > 0:
            penalty = min(20.0, outlier_cols * 3.0)
            score -= penalty
            factors.append({"factor": "numeric_outliers", "columns": outlier_cols, "total_outliers": total_outliers, "penalty": penalty})

        if profiled_row_count == 0 and row_count > 0:
            score -= 10
            factors.append({"factor": "no_profile_data", "penalty": 10})

        if score < 0:
            score = 0
        if score > 100:
            score = 100

        return int(round(score)), factors
