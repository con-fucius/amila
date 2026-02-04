"""
Insight Scheduler - Celery task for proactive insight generation
"""

import logging
from datetime import datetime
from typing import Dict, Any, List

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name="generate_proactive_insights")
def generate_proactive_insights() -> Dict[str, Any]:
    """
    Celery Beat task for scheduled proactive insight generation.
    
    Runs daily (or per configured schedule) to:
    - Query recent data for anomalies/trends
    - Generate unsolicited reports
    - Send notifications to relevant users
    
    Returns:
        Dict with insights generated and notifications sent
    """
    logger.info("=== Starting Proactive Insight Generation ===")
    
    try:
        # Import inside task to avoid circular dependencies
        from app.services.insight_service import InsightService
        from app.core.config import settings
        
        if not getattr(settings, "PROACTIVE_INSIGHTS_ENABLED", True):
            logger.info("Proactive insights disabled in config")
            return {
                "status": "skipped",
                "reason": "disabled_in_config"
            }
        
        # Get pre-configured analytical queries
        queries = getattr(settings, "PROACTIVE_INSIGHTS_QUERIES", [])
        
        if not queries:
            logger.warning("No proactive insight queries configured")
            queries = _get_default_insight_queries()
        
        insights_generated = []
        notifications_sent = 0
        
        for query_config in queries:
            try:
                query_name = query_config.get("name", "Unknown Query")
                sql_query = query_config.get("sql")
                recipients = query_config.get("recipients", ["admin"])
                
                logger.info(f"Executing proactive query: {query_name}")
                
                # Execute query and analyze results
                insight = InsightService.analyze_query_sync(
                    query_name=query_name,
                    sql_query=sql_query
                )
                
                if insight:
                    insights_generated.append(insight)
                    
                    # Create notification
                    if insight.get("anomalies") or insight.get("trends"):
                        InsightService.create_notification(
                            insight=insight,
                            recipients=recipients
                        )
                        notifications_sent += 1
                        
            except Exception as query_err:
                logger.error(f"Failed to process query {query_config.get('name')}: {query_err}")
                continue
        
        logger.info(f"=== Proactive Insight Generation Complete: {len(insights_generated)} insights, {notifications_sent} notifications ===")
        
        return {
            "status": "success",
            "insights_generated": len(insights_generated),
            "notifications_sent": notifications_sent,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Proactive insight generation failed: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


def _get_default_insight_queries() -> List[Dict[str, Any]]:
    """
    Get default proactive insight queries.
    
    Returns:
        List of query configurations
    """
    return [
        {
            "name": "Daily Revenue Trend",
            "sql": """
                SELECT 
                    TRUNC(order_date) as date,
                    SUM(amount) as total_revenue
                FROM orders
                WHERE order_date >= TRUNC(SYSDATE) - 7
                GROUP BY TRUNC(order_date)
                ORDER BY date DESC
            """,
            "recipients": ["admin", "finance"]
        },
        {
            "name": "Top 10 Customers by Volume",
            "sql": """
                SELECT 
                    customer_id,
                    COUNT(*) as order_count,
                    SUM(amount) as total_spent
                FROM orders
                WHERE order_date >= TRUNC(SYSDATE) - 30
                GROUP BY customer_id
                ORDER BY total_spent DESC
                FETCH FIRST 10 ROWS ONLY
            """,
            "recipients": ["admin", "sales"]
        },
        {
            "name": "High-Value Transactions Alert",
            "sql": """
                SELECT 
                    transaction_id,
                    amount,
                    transaction_date,
                    customer_id
                FROM transactions
                WHERE amount > 10000
                AND transaction_date >= TRUNC(SYSDATE) - 1
                ORDER BY amount DESC
            """,
            "recipients": ["admin", "fraud"]
        }
    ]
