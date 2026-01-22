"""
Superset Dashboard Auto-Generation Service
Generates dashboards from query results
"""

import logging
from typing import Dict, Any, Optional, List

from app.services.superset_service import SupersetClient
from app.core.exceptions import ExternalServiceException

logger = logging.getLogger(__name__)


class SupersetDashboardService:
    """Service for auto-generating Superset dashboards from query results"""
    
    @staticmethod
    def analyze_data_for_visualization(
        data: List[Dict[str, Any]],
        columns: List[str],
    ) -> Dict[str, Any]:
        """
        Analyze query results to suggest appropriate visualizations.
        
        Args:
            data: Query result rows
            columns: Column names
            
        Returns:
            Visualization recommendations
        """
        if not data or not columns:
            return {"chart_type": "table", "reason": "No data or columns"}
        
        numeric_cols = []
        categorical_cols = []
        temporal_cols = []
        
        for col in columns:
            sample_values = [row.get(col) for row in data[:10] if row.get(col) is not None]
            if not sample_values:
                continue
                
            sample = sample_values[0]
            
            if isinstance(sample, (int, float)):
                numeric_cols.append(col)
            elif isinstance(sample, str):
                if any(keyword in col.lower() for keyword in ['date', 'time', 'timestamp']):
                    temporal_cols.append(col)
                else:
                    categorical_cols.append(col)
        
        if len(numeric_cols) >= 2:
            return {
                "chart_type": "scatter",
                "x_axis": numeric_cols[0],
                "y_axis": numeric_cols[1],
                "reason": "Multiple numeric columns detected"
            }
        elif len(numeric_cols) == 1 and len(categorical_cols) >= 1:
            return {
                "chart_type": "bar",
                "x_axis": categorical_cols[0],
                "y_axis": numeric_cols[0],
                "reason": "One numeric and one categorical column"
            }
        elif len(temporal_cols) >= 1 and len(numeric_cols) >= 1:
            return {
                "chart_type": "line",
                "x_axis": temporal_cols[0],
                "y_axis": numeric_cols[0],
                "reason": "Temporal data with numeric values"
            }
        else:
            return {
                "chart_type": "table",
                "reason": "Default table view for mixed data types"
            }
    
    @staticmethod
    async def generate_dashboard_from_query(
        superset_client: SupersetClient,
        query_result: Dict[str, Any],
        dashboard_title: str,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate Superset dashboard from query results.
        
        Args:
            superset_client: Superset client instance
            query_result: Query execution result (including 'sql')
            dashboard_title: Title for the dashboard
            user_id: User ID for audit logging
            
        Returns:
            Dashboard creation result
        """
        try:
            data = query_result.get("data", [])
            columns = query_result.get("columns", [])
            sql = query_result.get("sql")
            
            # Analyze visualization
            viz_recommendation = SupersetDashboardService.analyze_data_for_visualization(
                data, columns
            )
            
            logger.info(f"Visualization recommendation: {viz_recommendation}")
            
            # 1. Create Dataset (Virtual if SQL is present)
            # For simplicity, we use a slugified title as table name for the virtual dataset
            import re
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', dashboard_title).lower()
            table_name = f"dataset_{table_name}"
            
            # We assume database_id=1 for now or from config if available. 
            # In a real system, this would be passed in or resolved from the connection name.
            database_id = 1 
            
            dataset_result = await superset_client.create_dataset(
                database_id=database_id,
                schema="public", # Default schema
                table_name=table_name,
                sql=sql,
                user_id=user_id
            )
            
            dataset_id = dataset_result.get("dataset_id")
            
            return {
                "status": "success",
                "message": f"Dashboard generation initiated. Dataset '{table_name}' (ID: {dataset_id}) created.",
                "dataset_id": dataset_id,
                "visualization_recommendation": viz_recommendation,
                "note": "Next step: Create chart and dashboard layouts (not fully implemented in this MVP pass)"
            }
            
        except Exception as e:
            logger.error(f"Failed to generate dashboard: {e}")
            raise ExternalServiceException(f"Dashboard generation error: {e}", service_name="superset")


def create_superset_dashboard_service() -> SupersetDashboardService:
    """Factory function to create Superset dashboard service"""
    return SupersetDashboardService()

