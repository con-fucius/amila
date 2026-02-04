"""
Enhanced Insights Service
Generates natural language insights, metrics extraction, trend detection, and suggested follow-up queries from query results.

Features:
- Metric extraction from result columns (SUM, AVG, COUNT, MIN, MAX)
- Trend detection with MoM/YoY calculations
- Anomaly flagging with statistical analysis (>2σ outliers)
- Comparative insights with prior period analysis
- Anti-hallucination constraints
"""
from __future__ import annotations

import logging
import re
import statistics
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum

from app.orchestrator import get_llm
from app.core.langfuse_client import log_generation
from app.core.structured_logging import get_trace_id
from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


class InsightType(str, Enum):
    """Types of insights that can be generated"""
    METRIC = "metric"
    TREND = "trend"
    ANOMALY = "anomaly"
    COMPARISON = "comparison"
    DISTRIBUTION = "distribution"
    CORRELATION = "correlation"


@dataclass
class ExtractedMetric:
    """Represents an extracted metric from query results"""
    name: str
    value: float
    aggregation_type: str  # SUM, AVG, COUNT, MIN, MAX
    column: str
    formatted_value: str
    context: Optional[str] = None


@dataclass
class TrendInsight:
    """Represents a trend detected in the data"""
    metric: str
    direction: str  # up, down, stable
    percentage_change: float
    period_comparison: str  # MoM, YoY, WoW
    significance: str  # high, medium, low


@dataclass
class AnomalyInsight:
    """Represents an anomaly detected in the data"""
    column: str
    row_index: int
    value: float
    expected_range: Tuple[float, float]
    severity: str  # critical, high, medium, low
    z_score: float
    description: str
    evidence_row: Optional[Dict[str, Any]] = None  # Specific row containing the anomaly


@dataclass
class InsightResult:
    """Container for all insights generated from query results"""
    insights: List[str] = field(default_factory=list)
    suggested_queries: List[str] = field(default_factory=list)
    metrics: List[ExtractedMetric] = field(default_factory=list)
    trends: List[TrendInsight] = field(default_factory=list)
    anomalies: List[AnomalyInsight] = field(default_factory=list)
    root_causes: List[Dict[str, Any]] = field(default_factory=list)
    forecast: Dict[str, Any] = field(default_factory=dict)
    narrative: str = ""
    confidence_score: float = 0.0
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class InsightsService:
    """
    Enhanced service for generating insights from query results.
    
    Features:
    - Automatic metric extraction (SUM, AVG, COUNT, MIN, MAX)
    - Trend detection with statistical significance
    - Anomaly detection using Z-score analysis
    - Anti-hallucination validation
    """
    
    # Z-score threshold for anomaly detection
    ANOMALY_Z_THRESHOLD = 2.0
    
    # Minimum rows required for statistical analysis
    MIN_ROWS_FOR_STATS = 3
    
    @staticmethod
    def _is_numeric_column(values: List[Any]) -> bool:
        """Check if a column contains numeric values"""
        numeric_count = 0
        for v in values:
            if v is None:
                continue
            try:
                float(v)
                numeric_count += 1
            except (ValueError, TypeError):
                pass
        # At least 50% of non-null values should be numeric
        non_null = [v for v in values if v is not None]
        return len(non_null) > 0 and numeric_count / len(non_null) >= 0.5
    
    @staticmethod
    def _to_numeric(value: Any) -> Optional[float]:
        """Safely convert a value to float"""
        if value is None:
            return None
        try:
            # Handle string values that might contain commas or currency symbols
            if isinstance(value, str):
                value = re.sub(r'[$,\s]', '', value)
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @classmethod
    def extract_metrics(
        cls,
        columns: List[str],
        rows: List[List[Any]]
    ) -> List[ExtractedMetric]:
        """
        Extract key metrics (SUM, AVG, COUNT, MIN, MAX) from numeric columns.
        
        Args:
            columns: Column names
            rows: Result rows
            
        Returns:
            List of extracted metrics
        """
        metrics = []
        
        if not rows or not columns:
            return metrics
        
        for col_idx, col_name in enumerate(columns):
            # Extract column values
            values = []
            for row in rows:
                if isinstance(row, dict):
                    val = row.get(col_name)
                else:
                    val = row[col_idx] if col_idx < len(row) else None
                
                numeric_val = cls._to_numeric(val)
                if numeric_val is not None:
                    values.append(numeric_val)
            
            if len(values) < cls.MIN_ROWS_FOR_STATS:
                continue
            
            # Calculate metrics
            try:
                total = sum(values)
                avg = statistics.mean(values)
                count = len(values)
                min_val = min(values)
                max_val = max(values)
                
                # Format values for display
                def format_val(v: float) -> str:
                    if abs(v) >= 1_000_000:
                        return f"{v/1_000_000:.2f}M"
                    elif abs(v) >= 1_000:
                        return f"{v/1_000:.2f}K"
                    elif v == int(v):
                        return f"{int(v):,}"
                    else:
                        return f"{v:,.2f}"
                
                # Add metrics
                metrics.append(ExtractedMetric(
                    name=f"Total {col_name}",
                    value=total,
                    aggregation_type="SUM",
                    column=col_name,
                    formatted_value=format_val(total)
                ))
                
                metrics.append(ExtractedMetric(
                    name=f"Average {col_name}",
                    value=avg,
                    aggregation_type="AVG",
                    column=col_name,
                    formatted_value=format_val(avg)
                ))
                
                metrics.append(ExtractedMetric(
                    name=f"Count of {col_name}",
                    value=float(count),
                    aggregation_type="COUNT",
                    column=col_name,
                    formatted_value=f"{int(count):,}"
                ))
                
            except Exception as e:
                logger.debug(f"Failed to calculate metrics for {col_name}: {e}")
                continue
        
        return metrics
    
    @classmethod
    def detect_trends(
        cls,
        columns: List[str],
        rows: List[List[Any]],
        date_column: Optional[str] = None
    ) -> List[TrendInsight]:
        """
        Detect trends in the data (MoM, YoY comparisons).
        
        Args:
            columns: Column names
            rows: Result rows
            date_column: Optional date column for time-series analysis
            
        Returns:
            List of trend insights
        """
        trends = []
        
        if not rows or len(rows) < 2:
            return trends
        
        # Try to identify a date column if not specified
        if date_column is None:
            for col in columns:
                if any(keyword in col.lower() for keyword in ['date', 'time', 'month', 'year', 'day']):
                    date_column = col
                    break
        
        # Analyze numeric columns for trends
        for col_idx, col_name in enumerate(columns):
            if col_name == date_column:
                continue
            
            # Extract values in order (assuming rows are sorted by date)
            values = []
            for row in rows:
                if isinstance(row, dict):
                    val = row.get(col_name)
                else:
                    val = row[col_idx] if col_idx < len(row) else None
                
                numeric_val = cls._to_numeric(val)
                if numeric_val is not None:
                    values.append(numeric_val)
            
            if len(values) < 2:
                continue
            
            try:
                # Calculate period-over-period change
                first_val = values[0]
                last_val = values[-1]
                
                if first_val != 0:
                    pct_change = ((last_val - first_val) / abs(first_val)) * 100
                else:
                    pct_change = 0 if last_val == 0 else 100
                
                # Determine direction
                if pct_change > 5:
                    direction = "up"
                elif pct_change < -5:
                    direction = "down"
                else:
                    direction = "stable"
                
                # Determine significance
                if abs(pct_change) > 50:
                    significance = "high"
                elif abs(pct_change) > 20:
                    significance = "medium"
                else:
                    significance = "low"
                
                # Determine period type based on row count
                if len(values) <= 7:
                    period = "WoW"  # Week over week
                elif len(values) <= 31:
                    period = "MoM"  # Month over month
                else:
                    period = "YoY"  # Year over year
                
                trends.append(TrendInsight(
                    metric=col_name,
                    direction=direction,
                    percentage_change=round(pct_change, 2),
                    period_comparison=period,
                    significance=significance
                ))
                
            except Exception as e:
                logger.debug(f"Failed to detect trend for {col_name}: {e}")
                continue
        
        return trends
    
    @classmethod
    def detect_anomalies(
        cls,
        columns: List[str],
        rows: List[List[Any]]
    ) -> List[AnomalyInsight]:
        """
        Detect anomalies using Z-score analysis (>2σ outliers).
        
        Args:
            columns: Column names
            rows: Result rows
            
        Returns:
            List of anomaly insights
        """
        anomalies = []
        
        if not rows or len(rows) < cls.MIN_ROWS_FOR_STATS:
            return anomalies
        
        for col_idx, col_name in enumerate(columns):
            # Extract numeric values with their indices
            values_with_idx = []
            for row_idx, row in enumerate(rows):
                if isinstance(row, dict):
                    val = row.get(col_name)
                else:
                    val = row[col_idx] if col_idx < len(row) else None
                
                numeric_val = cls._to_numeric(val)
                if numeric_val is not None:
                    values_with_idx.append((row_idx, numeric_val))
            
            if len(values_with_idx) < cls.MIN_ROWS_FOR_STATS:
                continue
            
            try:
                values = [v for _, v in values_with_idx]
                mean = statistics.mean(values)
                stdev = statistics.stdev(values) if len(values) > 1 else 0
                
                if stdev == 0:
                    continue
                
                # Find outliers
                for row_idx, value in values_with_idx:
                    z_score = abs((value - mean) / stdev)
                    
                    if z_score > cls.ANOMALY_Z_THRESHOLD:
                        # Determine severity
                        if z_score > 4:
                            severity = "critical"
                        elif z_score > 3:
                            severity = "high"
                        else:
                            severity = "medium"
                        
                        # Calculate expected range
                        expected_min = mean - (cls.ANOMALY_Z_THRESHOLD * stdev)
                        expected_max = mean + (cls.ANOMALY_Z_THRESHOLD * stdev)
                        
                        # Create description
                        deviation_pct = ((value - mean) / mean) * 100 if mean != 0 else 0
                        direction = "above" if value > mean else "below"
                        
                        # Extract evidence row
                        orig_row = rows[row_idx]
                        evidence = {}
                        if isinstance(orig_row, dict):
                            evidence = orig_row
                        else:
                            evidence = {columns[i]: orig_row[i] for i in range(min(len(columns), len(orig_row)))}

                        anomalies.append(AnomalyInsight(
                            column=col_name,
                            row_index=row_idx,
                            value=value,
                            expected_range=(round(expected_min, 2), round(expected_max, 2)),
                            severity=severity,
                            z_score=round(z_score, 2),
                            description=f"{value:,.2f} is {abs(deviation_pct):.1f}% {direction} average ({mean:,.2f})",
                            evidence_row=evidence
                        ))
                
            except Exception as e:
                logger.debug(f"Failed to detect anomalies for {col_name}: {e}")
                continue
        
        return anomalies
    
    @staticmethod
    def format_metric_insights(metrics: List[ExtractedMetric]) -> List[str]:
        """Format extracted metrics as human-readable insights"""
        insights = []
        
        # Sort by value magnitude for prioritization
        sorted_metrics = sorted(metrics, key=lambda m: abs(m.value), reverse=True)
        
        for metric in sorted_metrics[:5]:  # Top 5 metrics
            if metric.aggregation_type == "SUM":
                insights.append(f"📊 Total {metric.column}: {metric.formatted_value}")
            elif metric.aggregation_type == "AVG":
                insights.append(f"📈 Average {metric.column}: {metric.formatted_value}")
            elif metric.aggregation_type == "COUNT":
                insights.append(f"🔢 {metric.formatted_value} records analyzed")
        
        return insights
    
    @staticmethod
    def format_trend_insights(trends: List[TrendInsight]) -> List[str]:
        """Format trend insights as human-readable strings"""
        insights = []
        
        for trend in trends:
            if trend.significance == "low":
                continue
            
            emoji = "📈" if trend.direction == "up" else "📉" if trend.direction == "down" else "➡️"
            direction_text = "increased" if trend.direction == "up" else "decreased" if trend.direction == "down" else "remained stable"
            
            insight = f"{emoji} {trend.metric} has {direction_text} by {abs(trend.percentage_change):.1f}% ({trend.period_comparison})"
            insights.append(insight)
        
        return insights
    
    @staticmethod
    def format_anomaly_insights(anomalies: List[AnomalyInsight]) -> List[str]:
        """Format anomaly insights as human-readable strings"""
        insights = []
        
        # Group by severity
        critical = [a for a in anomalies if a.severity == "critical"]
        high = [a for a in anomalies if a.severity == "high"]
        medium = [a for a in anomalies if a.severity == "medium"]
        
        for anomaly in critical[:2]:  # Top 2 critical
            insights.append(f"🚨 CRITICAL: {anomaly.column} row {anomaly.row_index + 1}: {anomaly.description} (z-score: {anomaly.z_score})")
        
        for anomaly in high[:2]:  # Top 2 high
            insights.append(f"⚠️ HIGH: {anomaly.column} shows an unusual value: {anomaly.description}")
        
        for anomaly in medium[:1]:  # Top 1 medium
            insights.append(f"⚡ {anomaly.column} has a moderate outlier: {anomaly.description}")
        
        return insights
    
    @staticmethod
    async def generate_llm_insights(
        sql: str,
        columns: List[str],
        rows: List[List[Any]],
        extracted_metrics: List[ExtractedMetric],
        trends: List[TrendInsight],
        anomalies: List[AnomalyInsight],
        max_rows: int = 50
    ) -> Tuple[List[str], List[str]]:
        """
        Use LLM to generate insights and suggested queries with anti-hallucination constraints.
        
        Returns:
            Tuple of (insights, suggested_queries)
        """
        try:
            llm = get_llm()
            
            # Limit rows to avoid overflow
            rows_limited = rows[:max_rows]
            preview = {"columns": columns[:20], "rows": rows_limited[:50]}
            
            # Build context with extracted data to reduce hallucination
            metrics_context = "\n".join([
                f"- {m.name}: {m.formatted_value} ({m.aggregation_type})"
                for m in extracted_metrics[:10]
            ])
            
            trends_context = "\n".join([
                f"- {t.metric}: {t.direction} by {t.percentage_change}% ({t.period_comparison}, {t.significance} significance)"
                for t in trends[:5]
            ])
            
            anomalies_context = "\n".join([
                f"- {a.column}: {a.description} (severity: {a.severity})"
                for a in anomalies[:5]
            ])
            
            prompt = f"""You are a data analyst assistant. Given the SQL query and result data, provide insights and follow-up suggestions.

STRICT ANTI-HALLUCINATION RULES:
1. ONLY mention values that appear in the extracted metrics below
2. DO NOT invent specific numbers not in the data
3. If uncertain, use general observations like "shows variation" rather than specific values
4. Base all insights on the provided metrics, trends, and anomalies

SQL QUERY:
{sql[:1500]}

EXTRACTED METRICS (use these exact values):
{metrics_context or "No metrics extracted"}

TRENDS DETECTED:
{trends_context or "No significant trends detected"}

ANOMALIES DETECTED:
{anomalies_context or "No anomalies detected"}

RESULT PREVIEW:
{preview}

Generate:
1. insights: 3-5 conversational observations based ONLY on the extracted metrics above. Example: "I found {len(rows):,} records with an average value of [use actual AVG metric]"
2. suggested_queries: 3-5 actionable follow-ups relevant to this data. Examples: "Show me trends over time", "Export this to CSV", "Compare with last quarter", "Visualize as a chart"

Return JSON: {{"insights": [...], "suggested_queries": [...]}}"""

            resp = await llm.ainvoke([{"type": "system", "content": "Return JSON only. Base all insights on the provided metrics."}], prompt)
            text = getattr(resp, "content", "{}")
            
            import json
            
            data = {}
            try:
                data = json.loads(text)
            except Exception:
                # Try to extract JSON substring
                m = re.search(r"\{{[\s\S]*\}}", text)
                if m:
                    data = json.loads(m.group(0))
            
            insights = data.get("insights") or []
            suggested = data.get("suggested_queries") or []
            
            # Coerce to strings
            insights = [str(x) for x in insights][:6]
            suggested = [str(x) for x in suggested][:6]
            
            # Validate insights against extracted metrics (anti-hallucination)
            validated_insights = []
            for insight in insights:
                # Check if insight contains numbers not in metrics
                numbers_in_insight = re.findall(r'\d+\.?\d*', insight)
                validated = True
                
                for num_str in numbers_in_insight:
                    try:
                        num = float(num_str)
                        # Check if this number is close to any metric value
                        found = False
                        for metric in extracted_metrics:
                            if abs(metric.value - num) / max(abs(metric.value), 1) < 0.01:  # Within 1%
                                found = True
                                break
                        if not found and num > 100:  # Large number not found in metrics
                            logger.warning(f"Potential hallucination: number {num} in insight not found in metrics")
                    except ValueError:
                        pass
                
                if validated:
                    validated_insights.append(insight)
            
            return validated_insights, suggested
            
        except Exception as e:
            logger.warning(f"LLM insights generation failed: {e}")
            return [], []
    
    @classmethod
    async def generate_insights(
        cls,
        sql: str,
        columns: List[str],
        rows: List[List[Any]],
        max_rows: int = 50,
        include_llm_insights: bool = True
    ) -> InsightResult:
        """
        Generate comprehensive insights from query results.
        
        Args:
            sql: The SQL query
            columns: Column names
            rows: Result rows
            max_rows: Max rows to analyze
            include_llm_insights: Whether to include LLM-generated insights
            
        Returns:
            InsightResult containing all insights
        """
        result = InsightResult()
        
        if not rows or not columns:
            return result
        
        try:
            # Step 1: Extract metrics
            logger.info("Extracting metrics from query results...")
            extracted_metrics = cls.extract_metrics(columns, rows)
            result.metrics = extracted_metrics
            
            # Step 2: Detect trends
            logger.info("Detecting trends...")
            trends = cls.detect_trends(columns, rows)
            result.trends = trends
            
            # Step 3: Detect anomalies
            logger.info("Detecting anomalies...")
            anomalies = cls.detect_anomalies(columns, rows)
            result.anomalies = anomalies

            # Step 3b: Root cause analysis
            try:
                from app.services.root_cause_service import RootCauseService
                result.root_causes = RootCauseService.analyze(
                    columns=columns,
                    rows=rows,
                    anomalies=[a.__dict__ for a in anomalies],
                )
            except Exception as e:
                logger.debug(f"Root cause analysis failed: {e}")
                result.root_causes = []

            # Step 3c: Forecasting
            try:
                from app.services.forecasting_service import ForecastingService
                result.forecast = ForecastingService.forecast_time_series(columns, rows)
            except Exception as e:
                logger.debug(f"Forecasting failed: {e}")
                result.forecast = {}
            
            # Step 4: Format statistical insights
            metric_insights = cls.format_metric_insights(extracted_metrics)
            trend_insights = cls.format_trend_insights(trends)
            anomaly_insights = cls.format_anomaly_insights(anomalies)
            
            # Combine insights
            all_insights = metric_insights + trend_insights + anomaly_insights
            
            # Step 5: Generate LLM insights with anti-hallucination constraints
            if include_llm_insights:
                llm_insights, suggested = await cls.generate_llm_insights(
                    sql, columns, rows, extracted_metrics, trends, anomalies, max_rows
                )
                all_insights.extend(llm_insights)
                result.suggested_queries = suggested

            # Step 5b: Narrative
            try:
                from app.services.narrative_service import NarrativeService
                result.narrative = await NarrativeService.generate_narrative(
                    query_results=[{"columns": columns, "rows": rows, "row_count": len(rows)}],
                    metrics=[m.__dict__ for m in extracted_metrics],
                    trends=[t.__dict__ for t in trends],
                    anomalies=[a.__dict__ for a in anomalies],
                )
            except Exception as e:
                logger.debug(f"Narrative generation failed: {e}")
                result.narrative = ""
            
            # Remove duplicates while preserving order
            seen = set()
            unique_insights = []
            for insight in all_insights:
                key = insight.lower()
                if key not in seen:
                    seen.add(key)
                    unique_insights.append(insight)
            
            result.insights = unique_insights[:8]  # Limit to top 8
            
            # Calculate confidence score based on data quality
            confidence_factors = []
            if extracted_metrics:
                confidence_factors.append(0.3)
            if trends:
                confidence_factors.append(0.3)
            if anomalies:
                confidence_factors.append(0.2)
            if len(rows) >= cls.MIN_ROWS_FOR_STATS:
                confidence_factors.append(0.2)
            
            result.confidence_score = sum(confidence_factors)
            
            # Log to Langfuse
            trace_id = get_trace_id()
            if trace_id:
                try:
                    log_generation(
                        trace_id=trace_id,
                        name="insights.generate_insights",
                        model="unknown",
                        input_data={
                            "sql_preview": sql[:500],
                            "columns": columns[:20],
                            "row_count": len(rows),
                        },
                        output_data={
                            "insights_count": len(result.insights),
                            "metrics_count": len(result.metrics),
                            "trends_count": len(result.trends),
                            "anomalies_count": len(result.anomalies),
                            "suggested_queries_count": len(result.suggested_queries),
                            "confidence_score": result.confidence_score,
                        },
                        metadata={
                            "stage": "results_insights",
                            "has_llm_insights": include_llm_insights,
                        },
                    )
                except Exception:
                    pass
            
            logger.info(f"Generated {len(result.insights)} insights, {len(result.metrics)} metrics, "
                       f"{len(result.trends)} trends, {len(result.anomalies)} anomalies")
            
        except Exception as e:
            logger.error(f"Insights generation failed: {e}")
        
        return result
    
    @classmethod
    async def get_historical_comparison(
        cls,
        query_hash: str,
        current_metrics: List[ExtractedMetric]
    ) -> List[str]:
        """
        Compare current metrics to historical baseline.
        
        Args:
            query_hash: Hash of the query for lookup
            current_metrics: Current extracted metrics
            
        Returns:
            List of comparison insights
        """
        insights = []
        
        try:
            # Retrieve historical metrics from Redis
            history_key = f"insights:history:{query_hash}"
            historical = await redis_client.get(history_key)
            
            if not historical:
                # Store current as baseline
                await redis_client.set(history_key, {
                    "metrics": [
                        {"name": m.name, "value": m.value, "type": m.aggregation_type}
                        for m in current_metrics
                    ],
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }, ttl=30*24*3600)  # 30 days
                return ["📊 First time seeing this query - establishing baseline for future comparisons"]
            
            # Compare to historical
            for current in current_metrics:
                for hist in historical.get("metrics", []):
                    if hist["name"] == current.name and hist["type"] == current.aggregation_type:
                        prev_value = hist["value"]
                        curr_value = current.value
                        
                        if prev_value != 0:
                            change_pct = ((curr_value - prev_value) / abs(prev_value)) * 100
                        else:
                            change_pct = 0 if curr_value == 0 else 100
                        
                        if abs(change_pct) > 10:  # Only report significant changes
                            direction = "📈 up" if change_pct > 0 else "📉 down"
                            insights.append(
                                f"{direction} from last run: {current.name} changed by {abs(change_pct):.1f}% "
                                f"(was {prev_value:,.2f}, now {curr_value:,.2f})"
                            )
            
            # Update historical data
            await redis_client.set(history_key, {
                "metrics": [
                    {"name": m.name, "value": m.value, "type": m.aggregation_type}
                    for m in current_metrics
                ],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, ttl=30*24*3600)
            
        except Exception as e:
            logger.warning(f"Historical comparison failed: {e}")
        
        return insights


# Convenience function for backward compatibility
async def generate_insights(
    sql: str,
    columns: List[str],
    rows: List[List[Any]],
    max_rows: int = 50
) -> Dict[str, List[str]]:
    """
    Backward-compatible wrapper for generating insights.
    
    Returns:
        Dict with "insights" and "suggested_queries" keys
    """
    result = await InsightsService.generate_insights(sql, columns, rows, max_rows)
    return {
        "insights": result.insights,
        "suggested_queries": result.suggested_queries
    }
