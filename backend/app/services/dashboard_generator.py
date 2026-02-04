"""
Dashboard Generator Service - Automated dashboard creation
Coexists with Plotly implementation, does not replace it
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class DashboardGenerator:
    """
    Automatically generate dashboards from queries and data patterns.
    
    IMPORTANT: This service generates dashboard SPECIFICATIONS that can be:
    1. Rendered by frontend using existing Plotly code
    2. Optionally pushed to Apache Superset
    
    It does NOT replace the existing Plotly implementation.
    """
    
    @staticmethod
    async def generate_from_query(
        sql_query: str,
        query_results: Dict[str, Any],
        title: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate dashboard specification from query results.
        
        Args:
            sql_query: Original SQL query
            query_results: Execute query results with rows/columns
            title: Optional dashboard title
            description: Optional description
            
        Returns:
            Dashboard specification compatible with Plotly frontend
        """
        try:
            logger.info("Generating dashboard from query results")
            
            # 1. Profile the data
            data_profile = DashboardGenerator._profile_data(query_results)
            
            # 2. Recommend chart types
            chart_recommendations = DashboardGenerator._recommend_charts(data_profile)
            
            # 3. Optimize layout
            layout = DashboardGenerator._optimize_layout(chart_recommendations)
            
            # 4. Generate dashboard spec
            dashboard_spec = {
                "id": f"dash_{datetime.utcnow().timestamp()}",
                "title": title or "Auto-Generated Dashboard",
                "description": description or f"Generated from query at {datetime.utcnow().isoformat()}",
                "created_at": datetime.utcnow().isoformat(),
                "sql_query": sql_query,
                "charts": chart_recommendations,
                "layout": layout,
                "data_profile": data_profile,
                "metadata": {
                    "row_count": len(query_results.get("rows", [])),
                    "column_count": len(query_results.get("columns", [])),
                    "auto_generated": True
                }
            }
            
            logger.info(f"Generated dashboard with {len(chart_recommendations)} charts")
            
            return {
                "status": "success",
                "dashboard": dashboard_spec
            }
            
        except Exception as e:
            logger.error(f"Dashboard generation failed: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def _profile_data(query_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profile query results to understand data characteristics.
        
        Args:
            query_results: Query results dict
            
        Returns:
            Data profile with column types, cardinality, etc.
        """
        rows = query_results.get("rows", [])
        columns = query_results.get("columns", [])
        
        if not rows or not columns:
            return {"columns": [], "row_count": 0}
        
        profile = {
            "row_count": len(rows),
            "column_count": len(columns),
            "columns": []
        }
        
        # Analyze each column
        for i, col_name in enumerate(columns):
            col_values = [row[i] if isinstance(row, (list, tuple)) else row.get(col_name) for row in rows]
            
            col_profile = DashboardGenerator._analyze_column(col_name, col_values)
            profile["columns"].append(col_profile)
        
        return profile
    
    @staticmethod
    def _analyze_column(col_name: str, values: List[Any]) -> Dict[str, Any]:
        """
        Analyze a single column.
        
        Args:
            col_name: Column name
            values: List of column values
            
        Returns:
            Column profile dict
        """
        from datetime import date, datetime
        
        # Filter out None values for analysis
        non_null_values = [v for v in values if v is not None]
        
        if not non_null_values:
            return {
                "name": col_name,
                "type": "unknown",
                "nullable": True,
                "cardinality": 0
            }
        
        sample_value = non_null_values[0]
        
        # Determine type
        is_numeric = isinstance(sample_value, (int, float))
        is_temporal = isinstance(sample_value, (date, datetime))
        is_string = isinstance(sample_value, str)
        
        # Additional temporal detection for string dates
        if is_string and not is_temporal:
            # Simple heuristic: check if string looks like a date
            import re
            date_pattern = r'\d{4}-\d{2}-\d{2}'
            if re.match(date_pattern, str(sample_value)):
                is_temporal = True
        
        # Calculate cardinality
        unique_values = set(non_null_values)
        cardinality = len(unique_values)
        
        col_type = "unknown"
        if is_temporal:
            col_type = "temporal"
        elif is_numeric:
            col_type = "numeric"
        elif is_string:
            col_type = "categorical" if cardinality < 50 else "text"
        
        return {
            "name": col_name,
            "type": col_type,
            "is_numeric": is_numeric,
            "is_temporal": is_temporal,
            "is_categorical": col_type == "categorical",
            "nullable": len(values) > len(non_null_values),
            "cardinality": cardinality,
            "sample_values": list(unique_values)[:5]
        }
    
    @staticmethod
    def _recommend_charts(data_profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Recommend chart types based on data profile.
        
        Args:
            data_profile: Data profile from _profile_data
            
        Returns:
            List of chart recommendations
        """
        columns = data_profile.get("columns", [])
        recommendations = []
        
        if len(columns) == 0:
            return recommendations
        
        # Single column: histogram or value counts
        if len(columns) == 1:
            col = columns[0]
            if col["is_numeric"]:
                recommendations.append({
                    "type": "histogram",
                    "x": col["name"],
                    "title": f"Distribution of {col['name']}",
                    "plotly_type": "histogram"
                })
            elif col["is_categorical"]:
                recommendations.append({
                    "type": "bar",
                    "x": col["name"],
                    "title": f"Count by {col['name']}",
                    "plotly_type": "bar",
                    "aggregation": "count"
                })
            return recommendations
        
        # Multi-column analysis
        numeric_cols = [c for c in columns if c["is_numeric"]]
        temporal_cols = [c for c in columns if c["is_temporal"]]
        categorical_cols = [c for c in columns if c["is_categorical"]]
        
        # Time series: temporal + numeric
        for temporal_col in temporal_cols:
            for numeric_col in numeric_cols:
                recommendations.append({
                    "type": "line",
                    "x": temporal_col["name"],
                    "y": numeric_col["name"],
                    "title": f"{numeric_col['name']} Over Time",
                    "plotly_type": "scatter",
                    "mode": "lines+markers"
                })
        
        # Categorical breakdown: categorical + numeric
        for cat_col in categorical_cols[:2]:  # Limit to 2 categorical
            for numeric_col in numeric_cols[:2]:  # Limit to 2 numeric
                recommendations.append({
                    "type": "bar",
                    "x": cat_col["name"],
                    "y": numeric_col["name"],
                    "title": f"{numeric_col['name']} by {cat_col['name']}",
                    "plotly_type": "bar"
                })
        
        # Scatter plot: 2 numeric columns
        if len(numeric_cols) >= 2:
            recommendations.append({
                "type": "scatter",
                "x": numeric_cols[0]["name"],
                "y": numeric_cols[1]["name"],
                "title": f"{numeric_cols[0]['name']} vs {numeric_cols[1]['name']}",
                "plotly_type": "scatter",
                "mode": "markers"
            })
        
        # Limit total recommendations
        return recommendations[:6]
    
    @staticmethod
    def _optimize_layout(charts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Optimize dashboard layout for charts.
        
        Args:
            charts: List of chart specifications
            
        Returns:
            Layout specification
        """
        num_charts = len(charts)
        
        if num_charts == 0:
            return {"grid": {"rows": 0, "cols": 0}, "positions": []}
        
        # Determine grid layout
        if num_charts == 1:
            grid = {"rows": 1, "cols": 1}
        elif num_charts == 2:
            grid = {"rows": 1, "cols": 2}
        elif num_charts <= 4:
            grid = {"rows": 2, "cols": 2}
        elif num_charts <= 6:
            grid = {"rows": 2, "cols": 3}
        else:
            grid = {"rows": 3, "cols": 3}
        
        # Assign positions
        positions = []
        for i in range(num_charts):
            row = i // grid["cols"]
            col = i % grid["cols"]
            positions.append({
                "chart_index": i,
                "row": row,
                "col": col,
                "width": 1,
                "height": 1
            })
        
        return {
            "grid": grid,
            "positions": positions,
            "spacing": {"horizontal": 20, "vertical": 20}
        }
    
    @staticmethod
    async def generate_from_pattern(
        pattern_name: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate dashboard from predefined pattern.
        
        Args:
            pattern_name: Pattern identifier (e.g., 'revenue_analysis', 'customer_segmentation')
            parameters: Optional parameters for the pattern
            
        Returns:
            Dashboard specification
        """
        patterns = {
            "revenue_analysis": {
                "title": "Revenue Analysis Dashboard",
                "description": "Comprehensive revenue metrics and trends",
                "charts": [
                    {"type": "line", "title": "Revenue Over Time", "plotly_type": "scatter", "mode": "lines+markers"},
                    {"type": "bar", "title": "Revenue by Region", "plotly_type": "bar"},
                    {"type": "pie", "title": "Revenue Distribution", "plotly_type": "pie"},
                    {"type": "bar", "title": "YoY Growth", "plotly_type": "bar"}
                ]
            },
            "customer_segmentation": {
                "title": "Customer Segmentation Dashboard",
                "description": "Customer behavior and segmentation analysis",
                "charts": [
                    {"type": "scatter", "title": "Customer Segments (RFM)", "plotly_type": "scatter", "mode": "markers"},
                    {"type": "bar", "title": "Segment Sizes", "plotly_type": "bar"},
                    {"type": "heatmap", "title": "Segment Characteristics", "plotly_type": "heatmap"},
                    {"type": "pie", "title": "Segment Distribution", "plotly_type": "pie"}
                ]
            },
            "sales_performance": {
                "title": "Sales Performance Dashboard",
                "description": "Sales metrics and KPIs tracking",
                "charts": [
                    {"type": "line", "title": "Sales Trend", "plotly_type": "scatter", "mode": "lines"},
                    {"type": "bar", "title": "Top Products", "plotly_type": "bar"},
                    {"type": "bar", "title": "Sales by Rep", "plotly_type": "bar"},
                    {"type": "scatter", "title": "Quota Attainment", "plotly_type": "scatter"}
                ]
            },
            "operational_metrics": {
                "title": "Operational Metrics Dashboard",
                "description": "Key operational indicators and system health",
                "charts": [
                    {"type": "line", "title": "Query Volume", "plotly_type": "scatter", "mode": "lines"},
                    {"type": "bar", "title": "Error Rates", "plotly_type": "bar"},
                    {"type": "line", "title": "Response Times", "plotly_type": "scatter"},
                    {"type": "heatmap", "title": "Usage Heatmap", "plotly_type": "heatmap"}
                ]
            },
            "financial_overview": {
                "title": "Financial Overview Dashboard",
                "description": "Financial metrics and budget tracking",
                "charts": [
                    {"type": "line", "title": "P&L Trend", "plotly_type": "scatter", "mode": "lines+markers"},
                    {"type": "bar", "title": "Expenses by Category", "plotly_type": "bar"},
                    {"type": "line", "title": "Budget vs Actual", "plotly_type": "scatter"},
                    {"type": "pie", "title": "Cost Breakdown", "plotly_type": "pie"}
                ]
            },
            "marketing_analytics": {
                "title": "Marketing Analytics Dashboard",
                "description": "Marketing campaign performance and ROI",
                "charts": [
                    {"type": "line", "title": "Campaign Performance", "plotly_type": "scatter"},
                    {"type": "bar", "title": "Conversion Rates", "plotly_type": "bar"},
                    {"type": "scatter", "title": "ROI Analysis", "plotly_type": "scatter"},
                    {"type": "pie", "title": "Channel Distribution", "plotly_type": "pie"}
                ]
            },
            "data_quality": {
                "title": "Data Quality Dashboard",
                "description": "Data quality metrics and anomaly detection",
                "charts": [
                    {"type": "line", "title": "Data Quality Score", "plotly_type": "scatter"},
                    {"type": "bar", "title": "Null Rate by Table", "plotly_type": "bar"},
                    {"type": "bar", "title": "Duplicate Records", "plotly_type": "bar"},
                    {"type": "heatmap", "title": "Quality by Dimension", "plotly_type": "heatmap"}
                ]
            }
        }
        
        pattern = patterns.get(pattern_name)
        
        if not pattern:
            return {
                "status": "error",
                "error": f"Unknown pattern: {pattern_name}"
            }
        
        return {
            "status": "success",
            "dashboard": {
                **pattern,
                "id": f"pattern_{pattern_name}_{datetime.utcnow().timestamp()}",
                "created_at": datetime.utcnow().isoformat(),
                "pattern": pattern_name,
                "parameters": parameters or {}
            }
        }
    
    @staticmethod
    async def store_dashboard(dashboard_spec: Dict[str, Any], owner_id: Optional[str] = None) -> str:
        """
        Store dashboard specification in Redis.
        
        Args:
            dashboard_spec: Dashboard specification
            
        Returns:
            Dashboard ID
        """
        from app.core.redis_client import redis_client
        
        dashboard_id = dashboard_spec.get("id", f"dash_{datetime.utcnow().timestamp()}")
        cache_key = f"dashboard:spec:{dashboard_id}"
        
        # Store for 30 days
        if owner_id:
            dashboard_spec["owner_id"] = owner_id
        await redis_client.set(cache_key, dashboard_spec, ttl=2592000)
        
        # Add to user's dashboard list
        if owner_id:
            user_key = f"dashboards:user:{owner_id}"
            await redis_client.lpush(user_key, dashboard_id)
        else:
            user_key = "dashboards:all"
            await redis_client.lpush(user_key, dashboard_id)
        
        logger.info(f"Stored dashboard: {dashboard_id}")
        
        return dashboard_id
    
    @staticmethod
    async def get_dashboard(dashboard_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve dashboard specification.
        
        Args:
            dashboard_id: Dashboard ID
            
        Returns:
            Dashboard specification or None
        """
        from app.core.redis_client import redis_client
        
        cache_key = f"dashboard:spec:{dashboard_id}"
        dashboard = await redis_client.get(cache_key)
        
        return dashboard
    
    @staticmethod
    async def list_dashboards(limit: int = 20, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all stored dashboards.
        
        Args:
            limit: Maximum number to return
            
        Returns:
            List of dashboard metadata
        """
        from app.core.redis_client import redis_client
        
        if user_id:
            user_key = f"dashboards:user:{user_id}"
        else:
            user_key = "dashboards:all"
        dashboard_ids = await redis_client.lrange(user_key, 0, limit - 1)
        
        dashboards = []
        for dash_id in dashboard_ids:
            dashboard = await DashboardGenerator.get_dashboard(dash_id)
            if dashboard:
                # Return metadata only
                dashboards.append({
                    "id": dashboard.get("id"),
                    "title": dashboard.get("title"),
                    "description": dashboard.get("description"),
                    "created_at": dashboard.get("created_at"),
                    "chart_count": len(dashboard.get("charts", [])),
                    "owner_id": dashboard.get("owner_id"),
                })
        
        return dashboards
    
    @staticmethod
    async def delete_dashboard(dashboard_id: str) -> bool:
        """
        Delete dashboard.
        
        Args:
            dashboard_id: Dashboard ID
            
        Returns:
            True if deleted
        """
        from app.core.redis_client import redis_client
        
        cache_key = f"dashboard:spec:{dashboard_id}"
        await redis_client.delete(cache_key)
        
        # Remove from list
        user_key = "dashboards:all"
        await redis_client.lrem(user_key, 0, dashboard_id)
        
        logger.info(f"Deleted dashboard: {dashboard_id}")
        
        return True
    
    @staticmethod
    async def create_dashboard_version(
        dashboard_id: str,
        updated_spec: Dict[str, Any],
        version_note: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new version of an existing dashboard.
        
        Args:
            dashboard_id: Dashboard ID
            updated_spec: Updated dashboard specification
            version_note: Optional note about this version
            
        Returns:
            Version info
        """
        from app.core.redis_client import redis_client
        
        # Get current dashboard
        current_spec = await DashboardGenerator.get_dashboard(dashboard_id)
        
        if not current_spec:
            return {
                "status": "error",
                "error": "Dashboard not found"
            }
        
        # Get current version count
        version_key = f"dashboard:versions:{dashboard_id}"
        versions = await redis_client.lrange(version_key, 0, -1) or []
        version_number = len(versions) + 1
        
        # Create version snapshot
        version_data = {
            "version": version_number,
            "timestamp": datetime.utcnow().isoformat(),
            "note": version_note,
            "snapshot": current_spec
        }
        
        # Store version
        await redis_client.lpush(version_key, version_data)
        
        # Update current dashboard
        updated_spec["version"] = version_number
        updated_spec["updated_at"] = datetime.utcnow().isoformat()
        await redis_client.set(f"dashboard:spec:{dashboard_id}", updated_spec, ttl=2592000)
        
        logger.info(f"Created version {version_number} for dashboard {dashboard_id}")
        
        return {
            "status": "success",
            "version": version_number,
            "dashboard_id": dashboard_id
        }
    
    @staticmethod
    async def get_dashboard_versions(dashboard_id: str) -> List[Dict[str, Any]]:
        """
        Get all versions of a dashboard.
        
        Args:
            dashboard_id: Dashboard ID
            
        Returns:
            List of versions
        """
        from app.core.redis_client import redis_client
        
        version_key = f"dashboard:versions:{dashboard_id}"
        versions = await redis_client.lrange(version_key, 0, -1) or []
        
        return versions
    
    @staticmethod
    async def restore_dashboard_version(
        dashboard_id: str,
        version_number: int
    ) -> Dict[str, Any]:
        """
        Restore dashboard to a specific version.
        
        Args:
            dashboard_id: Dashboard ID
            version_number: Version to restore
            
        Returns:
            Restored dashboard
        """
        from app.core.redis_client import redis_client
        
        versions = await DashboardGenerator.get_dashboard_versions(dashboard_id)
        
        target_version = None
        for v in versions:
            if v.get("version") == version_number:
                target_version = v
                break
        
        if not target_version:
            return {
                "status": "error",
                "error": f"Version {version_number} not found"
            }
        
        # Restore snapshot
        restored_spec = target_version["snapshot"]
        restored_spec["restored_from_version"] = version_number
        restored_spec["restored_at"] = datetime.utcnow().isoformat()
        
        await redis_client.set(f"dashboard:spec:{dashboard_id}", restored_spec, ttl=2592000)
        
        logger.info(f"Restored dashboard {dashboard_id} to version {version_number}")
        
        return {
            "status": "success",
            "dashboard": restored_spec
        }
    
    @staticmethod
    async def share_dashboard(
        dashboard_id: str,
        share_with_users: List[str],
        permissions: str = "view"
    ) -> Dict[str, Any]:
        """
        Share dashboard with specific users.
        
        Args:
            dashboard_id: Dashboard ID
            share_with_users: List of user IDs
            permissions: Permission level ('view' or 'edit')
            
        Returns:
            Share status
        """
        from app.core.redis_client import redis_client
        
        share_key = f"dashboard:shares:{dashboard_id}"
        
        share_data = {
            "shared_at": datetime.utcnow().isoformat(),
            "users": share_with_users,
            "permissions": permissions
        }
        
        await redis_client.set(share_key, share_data)
        
        # Add to each user's shared dashboards list
        for user_id in share_with_users:
            user_shares_key = f"user:{user_id}:shared_dashboards"
            await redis_client.lpush(user_shares_key, dashboard_id)
        
        logger.info(f"Shared dashboard {dashboard_id} with {len(share_with_users)} users")
        
        return {
            "status": "success",
            "dashboard_id": dashboard_id,
            "shared_with": len(share_with_users),
            "permissions": permissions
        }
    
    @staticmethod
    async def get_shared_dashboards(user_id: str) -> List[str]:
        """
        Get dashboards shared with a user.
        
        Args:
            user_id: User ID
            
        Returns:
            List of dashboard IDs
        """
        from app.core.redis_client import redis_client
        
        user_shares_key = f"user:{user_id}:shared_dashboards"
        dashboard_ids = await redis_client.lrange(user_shares_key, 0, -1) or []
        
        return dashboard_ids

