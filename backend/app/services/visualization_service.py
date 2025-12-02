"""
Python-based Visualization Service
Generates interactive charts using Plotly for enhanced data visualization
"""

import logging
from typing import Any, Dict, List, Optional
import json

logger = logging.getLogger(__name__)

# Feature flag for Python visualizations
PYTHON_VIZ_ENABLED = True

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.utils import PlotlyJSONEncoder
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logger.warning("Plotly not installed. Python visualizations will be limited.")


class VisualizationService:
    """Service for generating Python-based visualizations"""
    
    @staticmethod
    def is_available() -> bool:
        """Check if Python visualization is available"""
        return PYTHON_VIZ_ENABLED and PLOTLY_AVAILABLE
    
    @staticmethod
    def detect_chart_type(columns: List[str], rows: List[List[Any]], hints: Optional[Dict] = None) -> str:
        """Auto-detect the best chart type based on data characteristics"""
        if not rows or not columns:
            return "table"
        
        # Use hints if provided
        if hints and hints.get("chart_type"):
            return hints["chart_type"]
        
        num_rows = len(rows)
        num_cols = len(columns)
        
        # Detect numeric columns
        numeric_cols = []
        categorical_cols = []
        date_cols = []
        
        for i, col in enumerate(columns):
            col_lower = col.lower()
            sample_values = [row[i] for row in rows[:min(50, num_rows)] if row[i] is not None]
            
            if not sample_values:
                continue
            
            # Check for date columns
            if any(kw in col_lower for kw in ['date', 'time', 'month', 'year', 'quarter', 'week']):
                date_cols.append(i)
            # Check for numeric
            elif all(isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit()) for v in sample_values):
                numeric_cols.append(i)
            else:
                categorical_cols.append(i)
        
        # Decision logic
        if date_cols and numeric_cols:
            return "line"  # Time series
        elif len(categorical_cols) == 1 and len(numeric_cols) == 1 and num_rows <= 10:
            return "pie"  # Single category with single metric
        elif len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
            if num_rows > 20:
                return "bar"  # Many categories
            return "bar"
        elif len(numeric_cols) >= 2:
            return "scatter"  # Multiple numeric columns
        
        return "bar"  # Default
    
    @staticmethod
    def generate_chart(
        columns: List[str],
        rows: List[List[Any]],
        chart_type: Optional[str] = None,
        title: Optional[str] = None,
        hints: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Generate a Plotly chart from query results
        
        Returns:
            Dict with 'plotly_json' (for frontend rendering) and 'chart_type'
        """
        if not VisualizationService.is_available():
            return {
                "status": "unavailable",
                "message": "Python visualization not available",
                "fallback": "recharts"
            }
        
        if not rows or not columns:
            return {
                "status": "error",
                "message": "No data to visualize"
            }
        
        try:
            # Convert to dict format for Plotly
            data_dict = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            
            # Auto-detect chart type if not specified
            if not chart_type:
                chart_type = VisualizationService.detect_chart_type(columns, rows, hints)
            
            # Identify dimension and metric columns
            numeric_cols = []
            categorical_cols = []
            
            for i, col in enumerate(columns):
                sample = [row[i] for row in rows[:50] if row[i] is not None]
                if sample and all(isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit()) for v in sample):
                    numeric_cols.append(col)
                else:
                    categorical_cols.append(col)
            
            # Default selections
            x_col = categorical_cols[0] if categorical_cols else columns[0]
            y_col = numeric_cols[0] if numeric_cols else (columns[1] if len(columns) > 1 else columns[0])
            
            # Generate chart based on type
            fig = None
            
            if chart_type == "bar":
                fig = px.bar(
                    data_dict,
                    x=x_col,
                    y=y_col,
                    title=title or f"{y_col} by {x_col}",
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig.update_layout(
                    xaxis_tickangle=-45,
                    bargap=0.2
                )

            elif chart_type == "line":
                fig = px.line(
                    data_dict,
                    x=x_col,
                    y=y_col,
                    title=title or f"{y_col} over {x_col}",
                    markers=True
                )
                fig.update_traces(line=dict(width=2))
                
            elif chart_type == "pie":
                fig = px.pie(
                    data_dict,
                    names=x_col,
                    values=y_col,
                    title=title or f"{y_col} Distribution",
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                
            elif chart_type == "scatter":
                y2_col = numeric_cols[1] if len(numeric_cols) > 1 else y_col
                fig = px.scatter(
                    data_dict,
                    x=x_col if x_col in numeric_cols else y_col,
                    y=y2_col,
                    title=title or f"{y2_col} vs {x_col}",
                    color=categorical_cols[0] if categorical_cols else None,
                    size_max=15
                )
                
            elif chart_type == "area":
                fig = px.area(
                    data_dict,
                    x=x_col,
                    y=y_col,
                    title=title or f"{y_col} over {x_col}"
                )
                
            elif chart_type == "heatmap" and len(numeric_cols) >= 2:
                # Create correlation matrix for heatmap
                import pandas as pd
                df = pd.DataFrame(data_dict)
                numeric_df = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
                corr = numeric_df.corr()
                
                fig = px.imshow(
                    corr,
                    title=title or "Correlation Heatmap",
                    color_continuous_scale="RdBu_r",
                    aspect="auto"
                )
            else:
                # Default to bar
                fig = px.bar(
                    data_dict,
                    x=x_col,
                    y=y_col,
                    title=title or f"{y_col} by {x_col}"
                )
            
            if fig:
                # Apply consistent styling
                fig.update_layout(
                    template="plotly_white",
                    font=dict(family="Inter, sans-serif", size=12),
                    title_font=dict(size=16, color="#111827"),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    margin=dict(l=40, r=40, t=60, b=40),
                    paper_bgcolor="rgba(255,255,255,0.9)",
                    plot_bgcolor="rgba(255,255,255,0.9)"
                )
                
                # Add statistical overlays if applicable
                stats_info = {}
                if chart_type in ["bar", "line", "scatter"] and y_col in data_dict:
                    y_values = [v for v in data_dict[y_col] if v is not None]
                    try:
                        numeric_y = [float(v) for v in y_values if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit())]
                        if numeric_y:
                            mean_val = sum(numeric_y) / len(numeric_y)
                            stats_info = {
                                "mean": mean_val,
                                "min": min(numeric_y),
                                "max": max(numeric_y),
                                "count": len(numeric_y)
                            }
                            
                            # Add mean line for bar and line charts
                            if chart_type in ["bar", "line"] and hints and hints.get("show_mean", True):
                                fig.add_hline(
                                    y=mean_val,
                                    line_dash="dash",
                                    line_color="#10b981",
                                    annotation_text=f"Mean: {mean_val:,.2f}",
                                    annotation_position="top right"
                                )
                            
                            # Add peak annotation
                            if hints and hints.get("show_peaks", False):
                                max_idx = numeric_y.index(max(numeric_y))
                                if max_idx < len(data_dict[x_col]):
                                    fig.add_annotation(
                                        x=data_dict[x_col][max_idx],
                                        y=max(numeric_y),
                                        text=f"Peak: {max(numeric_y):,.2f}",
                                        showarrow=True,
                                        arrowhead=2,
                                        arrowcolor="#ef4444",
                                        font=dict(color="#ef4444")
                                    )
                    except (ValueError, TypeError):
                        pass
                
                # Convert to JSON for frontend
                plotly_json = json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder))
                
                return {
                    "status": "success",
                    "chart_type": chart_type,
                    "plotly_json": plotly_json,
                    "columns_used": {
                        "x": x_col,
                        "y": y_col
                    },
                    "statistics": stats_info
                }
            
            return {
                "status": "error",
                "message": "Could not generate chart"
            }
            
        except Exception as e:
            logger.error(f"Visualization generation failed: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "fallback": "recharts"
            }
    
    @staticmethod
    def generate_multi_chart(
        columns: List[str],
        rows: List[List[Any]],
        title: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate multiple chart types for the same data"""
        if not VisualizationService.is_available():
            return {"status": "unavailable"}
        
        charts = {}
        for chart_type in ["bar", "line", "pie"]:
            result = VisualizationService.generate_chart(
                columns, rows, chart_type, title
            )
            if result.get("status") == "success":
                charts[chart_type] = result
        
        return {
            "status": "success",
            "charts": charts,
            "recommended": VisualizationService.detect_chart_type(columns, rows)
        }
