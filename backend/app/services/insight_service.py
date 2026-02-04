"""
Insight Service - Anomaly detection and proactive insight generation
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class InsightService:
    """Service for generating proactive insights and detecting anomalies"""
    
    @staticmethod
    def analyze_query_sync(
        query_name: str,
        sql_query: str
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze query results synchronously (for Celery tasks).
        
        Args:
            query_name: Name of the query
            sql_query: SQL query to execute
            
        Returns:
            Dict with insights, anomalies, and trends or None if no insights
        """
        try:
            from app.services.database_router import DatabaseRouter
            import asyncio
            
            # Execute query
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(
                DatabaseRouter.execute_sql(
                    database_type="oracle",
                    sql_query=sql_query
                )
            )
            
            if result.get("status") != "success":
                logger.warning(f"Query failed for {query_name}: {result.get('error')}")
                return None
            
            rows = result.get("rows", [])
            
            if not rows:
                return None
            
            # Detect anomalies and trends
            anomalies = InsightService.detect_anomalies(rows)
            trends = InsightService.detect_trends(rows)
            
            if not anomalies and not trends:
                return None
            
            return {
                "query_name": query_name,
                "timestamp": datetime.utcnow().isoformat(),
                "row_count": len(rows),
                "anomalies": anomalies,
                "trends": trends,
                "data_summary": InsightService._summarize_data(rows)
            }
            
        except Exception as e:
            logger.error(f"Query analysis failed for {query_name}: {e}")
            return None
    
    @staticmethod
    def detect_anomalies(query_results: List[Any]) -> List[Dict[str, Any]]:
        """
        Detect anomalies in query results.
        
        Args:
            query_results: Query result rows
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        try:
            # Simple anomaly detection: Look for outliers in numeric columns
            if not query_results:
                return anomalies
            
            # Check if first row is dict or list
            first_row = query_results[0]
            
            if isinstance(first_row, dict):
                # Dict-based rows
                for key in first_row.keys():
                    values = [row.get(key) for row in query_results if isinstance(row.get(key), (int, float))]
                    
                    if len(values) > 2:
                        mean = sum(values) / len(values)
                        std_dev = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                        
                        # Flag values > 2 standard deviations from mean
                        for i, val in enumerate(values):
                            if abs(val - mean) > 2 * std_dev:
                                anomalies.append({
                                    "type": "outlier",
                                    "column": key,
                                    "value": val,
                                    "threshold": f"{mean:.2f} ± {2*std_dev:.2f}",
                                    "severity": "medium"
                                })
            
        except Exception as e:
            logger.warning(f"Anomaly detection failed: {e}")
        
        return anomalies[:5]  # Limit to top 5 anomalies
    
    @staticmethod
    def detect_trends(query_results: List[Any]) -> List[Dict[str, Any]]:
        """
        Detect trends in time-series data.
        
        Args:
            query_results: Query result rows
            
        Returns:
            List of detected trends
        """
        trends = []
        
        try:
            if len(query_results) < 3:
                return trends
            
            # Simple trend detection: Look for consistent increase/decrease
            first_row = query_results[0]
            
            if isinstance(first_row, dict):
                # Look for numeric columns that might represent time series
                for key in first_row.keys():
                    values = [row.get(key) for row in query_results if isinstance(row.get(key), (int, float))]
                    
                    if len(values) >= 3:
                        # Calculate rate of change
                        changes = [values[i+1] - values[i] for i in range(len(values) - 1)]
                        
                        if all(c > 0 for c in changes):
                            trends.append({
                                "type": "increasing",
                                "column": key,
                                "change_rate": f"{sum(changes)/len(changes):.2f} per period",
                                "confidence": "high"
                            })
                        elif all(c < 0 for c in changes):
                            trends.append({
                                "type": "decreasing",
                                "column": key,
                                "change_rate": f"{sum(changes)/len(changes):.2f} per period",
                                "confidence": "high"
                            })
            
        except Exception as e:
            logger.warning(f"Trend detection failed: {e}")
        
        return trends
    
    @staticmethod
    def _summarize_data(query_results: List[Any]) -> Dict[str, Any]:
        """
        Summarize query results.
        
        Args:
            query_results: Query result rows
            
        Returns:
            Summary statistics
        """
        summary = {
            "total_rows": len(query_results),
            "sample_size": min(len(query_results), 5)
        }
        
        if query_results and isinstance(query_results[0], dict):
            summary["columns"] = list(query_results[0].keys())
        
        return summary
    
    @staticmethod
    def create_notification(
        insight: Dict[str, Any],
        recipients: List[str]
    ) -> None:
        """
        Create notification for insight.
        
        Args:
            insight: Insight dict
            recipients: List of recipient usernames/emails
        """
        try:
            # Store notification in Redis for retrieval
            from app.core.redis_client import redis_client
            import asyncio
            
            notification = {
                "type": "proactive_insight",
                "query_name": insight.get("query_name"),
                "timestamp": insight.get("timestamp"),
                "anomalies_count": len(insight.get("anomalies", [])),
                "trends_count": len(insight.get("trends", [])),
                "recipients": recipients,
                "data": insight
            }
            
            # Store notification with 7-day TTL
            loop = asyncio.get_event_loop()
            for recipient in recipients:
                key = f"notifications:{recipient}:insights"
                loop.run_until_complete(
                    redis_client.lpush(key, notification)
                )
                loop.run_until_complete(
                    redis_client.expire(key, 604800)  # 7 days
                )
            
            logger.info(f"Created notification for {len(recipients)} recipients")
            
        except Exception as e:
            logger.error(f"Notification creation failed: {e}")
