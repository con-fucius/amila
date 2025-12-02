"""
Enhanced Result Anomaly Detection Service

Implements:
- Z-score outlier detection for numeric columns
- Temporal anomaly detection (comparing to historical baselines)
- Statistical distribution analysis
- Correlation with historical query results
"""

import logging
import statistics
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AnomalyDetectionService:
    """Service for detecting anomalies in query results"""
    
    # Z-score threshold for outlier detection
    Z_SCORE_THRESHOLD = 3.0
    
    @staticmethod
    def detect_numeric_outliers(
        column_name: str,
        values: List[Any],
        z_threshold: float = Z_SCORE_THRESHOLD
    ) -> Dict[str, Any]:
        """
        Detect outliers using z-score analysis
        
        Args:
            column_name: Name of the column
            values: List of numeric values
            z_threshold: Z-score threshold (default: 3.0 standard deviations)
            
        Returns:
            Dict with outlier analysis results
        """
        # Filter out None/null values
        numeric_values = [float(v) for v in values if v is not None and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()]
        
        if len(numeric_values) < 3:
            return {
                "has_outliers": False,
                "reason": "Insufficient numeric data for outlier detection"
            }
        
        try:
            mean = statistics.mean(numeric_values)
            stdev = statistics.stdev(numeric_values)
            
            if stdev == 0:
                return {
                    "has_outliers": False,
                    "reason": "No variance in data (all values identical)"
                }
            
            # Calculate z-scores
            outliers = []
            for idx, value in enumerate(numeric_values):
                z_score = abs((value - mean) / stdev)
                if z_score > z_threshold:
                    outliers.append({
                        "row_index": idx,
                        "value": value,
                        "z_score": round(z_score, 2)
                    })
            
            return {
                "has_outliers": len(outliers) > 0,
                "outlier_count": len(outliers),
                "outliers": outliers[:10],  # Limit to first 10
                "mean": round(mean, 2),
                "std_dev": round(stdev, 2),
                "threshold": z_threshold,
            }
            
        except Exception as e:
            logger.warning(f"Outlier detection failed for {column_name}: {e}")
            return {"has_outliers": False, "error": str(e)}
    
    @staticmethod
    def detect_distribution_anomalies(
        column_name: str,
        values: List[Any]
    ) -> Dict[str, Any]:
        """
        Detect distribution anomalies (skewness, concentration)
        
        Args:
            column_name: Name of the column
            values: List of values (any type)
            
        Returns:
            Dict with distribution analysis
        """
        # Filter out None values
        non_null_values = [v for v in values if v is not None]
        
        if len(non_null_values) < 3:
            return {"anomalies": []}
        
        anomalies = []
        
        # Check for value concentration (>80% same value)
        value_counts = {}
        for v in non_null_values:
            v_str = str(v)
            value_counts[v_str] = value_counts.get(v_str, 0) + 1
        
        max_count = max(value_counts.values()) if value_counts else 0
        max_value = max(value_counts, key=value_counts.get) if value_counts else None
        concentration = max_count / len(non_null_values)
        
        if concentration > 0.8 and len(value_counts) > 1:
            anomalies.append({
                "type": "high_concentration",
                "description": f"Column '{column_name}' has {int(concentration*100)}% same value: {max_value}",
                "severity": "medium",
            })
        
        # Check for sparse data (>50% nulls)
        null_count = len(values) - len(non_null_values)
        null_ratio = null_count / len(values) if values else 0
        
        if null_ratio > 0.5:
            anomalies.append({
                "type": "high_null_ratio",
                "description": f"Column '{column_name}' has {int(null_ratio*100)}% null values",
                "severity": "low",
            })
        
        return {"anomalies": anomalies}
    
    @staticmethod
    async def compare_to_historical_baseline(
        query_hash: str,
        current_result: Dict[str, Any],
        redis_client: Any
    ) -> Dict[str, Any]:
        """
        Compare current result to historical baseline
        
        Args:
            query_hash: Hash of the normalized query
            current_result: Current query result
            redis_client: Redis client for fetching historical data
            
        Returns:
            Dict with temporal anomaly analysis
        """
        try:
            # Attempt to retrieve historical baseline from Redis
            baseline_key = f"baseline:{query_hash}"
            historical_data = await redis_client.get(baseline_key)
            
            current_row_count = current_result.get("row_count", 0)
            
            if not historical_data:
                # First time seeing this query - store as baseline
                baseline = {
                    "row_count_avg": current_row_count,
                    "row_count_min": current_row_count,
                    "row_count_max": current_row_count,
                    "execution_count": 1,
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                }
                await redis_client.set(baseline_key, baseline, ttl=30*24*3600)  # 30 days
                
                return {
                    "is_anomaly": False,
                    "reason": "First execution - establishing baseline",
                }
            
            # Compare to historical baseline
            baseline = historical_data
            avg_rows = baseline.get("row_count_avg", current_row_count)
            min_rows = baseline.get("row_count_min", current_row_count)
            max_rows = baseline.get("row_count_max", current_row_count)
            
            anomalies = []
            
            # Check for significant deviation (>50% from average)
            if avg_rows > 0:
                deviation = abs(current_row_count - avg_rows) / avg_rows
                if deviation > 0.5:
                    anomalies.append({
                        "type": "row_count_deviation",
                        "description": f"Result size deviates {int(deviation*100)}% from historical average ({avg_rows} rows)",
                        "severity": "medium",
                        "current": current_row_count,
                        "baseline": avg_rows,
                    })
            
            # Check for sudden spike (>2x max seen)
            if current_row_count > max_rows * 2:
                anomalies.append({
                    "type": "sudden_spike",
                    "description": f"Result size ({current_row_count}) exceeds 2x historical max ({max_rows})",
                    "severity": "high",
                })
            
            # Check for sudden drop (empty when historically had data)
            if current_row_count == 0 and avg_rows > 10:
                anomalies.append({
                    "type": "sudden_drop",
                    "description": f"Empty result when historical average is {avg_rows} rows",
                    "severity": "high",
                })
            
            # Update baseline with rolling average
            execution_count = baseline.get("execution_count", 0) + 1
            new_avg = ((avg_rows * (execution_count - 1)) + current_row_count) / execution_count
            
            updated_baseline = {
                "row_count_avg": new_avg,
                "row_count_min": min(min_rows, current_row_count),
                "row_count_max": max(max_rows, current_row_count),
                "execution_count": execution_count,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            await redis_client.set(baseline_key, updated_baseline, ttl=30*24*3600)
            
            return {
                "is_anomaly": len(anomalies) > 0,
                "anomalies": anomalies,
                "baseline": {
                    "avg_rows": round(avg_rows, 1),
                    "execution_count": execution_count,
                }
            }
            
        except Exception as e:
            logger.warning(f"Historical baseline comparison failed: {e}")
            return {"is_anomaly": False, "error": str(e)}
    
    @staticmethod
    def analyze_query_results(
        result: Dict[str, Any],
        columns: List[str],
        rows: List[List[Any]]
    ) -> Dict[str, Any]:
        """
        Comprehensive anomaly analysis for query results
        
        Args:
            result: Query result metadata
            columns: Column names
            rows: Result rows
            
        Returns:
            Comprehensive anomaly report
        """
        if not rows or not columns:
            return {"anomalies": [], "warnings": []}
        
        anomalies = []
        warnings = []
        
        # Analyze each column
        for col_idx, col_name in enumerate(columns):
            try:
                # Extract column values
                values = [row[col_idx] if col_idx < len(row) else None for row in rows]
                
                # Run z-score outlier detection for numeric columns
                outlier_result = AnomalyDetectionService.detect_numeric_outliers(col_name, values)
                if outlier_result.get("has_outliers"):
                    anomalies.append({
                        "column": col_name,
                        "type": "numeric_outliers",
                        "severity": "medium",
                        **outlier_result
                    })
                
                # Run distribution anomaly detection
                dist_result = AnomalyDetectionService.detect_distribution_anomalies(col_name, values)
                if dist_result.get("anomalies"):
                    anomalies.extend(dist_result["anomalies"])
                    
            except Exception as e:
                logger.debug(f"Anomaly analysis failed for column {col_name}: {e}")
                continue
        
        return {
            "anomalies": anomalies,
            "warnings": warnings,
            "columns_analyzed": len(columns),
        }
