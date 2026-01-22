"""
Python-based Visualization Service
Generates interactive charts using Plotly for enhanced data visualization
Enhanced with professional aesthetics, color schemes, and legends
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
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


# Professional color palettes
COLOR_PALETTES = {
    "default": [
        "#3B82F6",  # Blue
        "#10B981",  # Green
        "#F59E0B",  # Amber
        "#EF4444",  # Red
        "#8B5CF6",  # Purple
        "#EC4899",  # Pink
        "#06B6D4",  # Cyan
        "#F97316",  # Orange
    ],
    "business": [
        "#1E40AF",  # Deep Blue
        "#059669",  # Emerald
        "#DC2626",  # Red
        "#7C3AED",  # Violet
        "#EA580C",  # Orange
        "#0891B2",  # Cyan
        "#BE185D",  # Rose
        "#4338CA",  # Indigo
    ],
    "pastel": [
        "#93C5FD",  # Light Blue
        "#86EFAC",  # Light Green
        "#FCD34D",  # Light Yellow
        "#FCA5A5",  # Light Red
        "#C4B5FD",  # Light Purple
        "#F9A8D4",  # Light Pink
        "#A5F3FC",  # Light Cyan
        "#FDBA74",  # Light Orange
    ],
    "vibrant": [
        "#2563EB",  # Bright Blue
        "#16A34A",  # Bright Green
        "#F59E0B",  # Bright Amber
        "#DC2626",  # Bright Red
        "#9333EA",  # Bright Purple
        "#DB2777",  # Bright Pink
        "#0891B2",  # Bright Cyan
        "#EA580C",  # Bright Orange
    ],
    "monochrome": [
        "#1F2937",  # Gray 800
        "#374151",  # Gray 700
        "#4B5563",  # Gray 600
        "#6B7280",  # Gray 500
        "#9CA3AF",  # Gray 400
        "#D1D5DB",  # Gray 300
        "#E5E7EB",  # Gray 200
        "#F3F4F6",  # Gray 100
    ]
}


class VisualizationService:
    """Service for generating Python-based visualizations with enhanced aesthetics"""
    
    @staticmethod
    def is_available() -> bool:
        """Check if Python visualization is available"""
        return PYTHON_VIZ_ENABLED and PLOTLY_AVAILABLE
    
    @staticmethod
    def get_color_palette(palette_name: str = "default", num_colors: int = 8) -> List[str]:
        """
        Get color palette for charts
        
        Args:
            palette_name: Name of color palette
            num_colors: Number of colors needed
        
        Returns:
            List of color hex codes
        """
        palette = COLOR_PALETTES.get(palette_name, COLOR_PALETTES["default"])
        
        # Extend palette if more colors needed
        if num_colors > len(palette):
            # Cycle through palette
            extended = []
            for i in range(num_colors):
                extended.append(palette[i % len(palette)])
            return extended
        
        return palette[:num_colors]
    
    @staticmethod
    def format_number(value: float, compact: bool = True) -> str:
        """
        Format numbers for display with appropriate units
        
        Args:
            value: Number to format
            compact: Use compact notation (K, M, B)
        
        Returns:
            Formatted string
        """
        if not compact:
            return f"{value:,.2f}"
        
        abs_value = abs(value)
        
        if abs_value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        elif abs_value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        elif abs_value >= 1_000:
            return f"{value / 1_000:.2f}K"
        else:
            return f"{value:.2f}"
    
    @staticmethod
    def apply_professional_layout(
        fig: go.Figure,
        title: str,
        chart_type: str,
        show_legend: bool = True,
        palette: str = "default"
    ) -> go.Figure:
        """
        Apply professional styling to Plotly figure
        
        Args:
            fig: Plotly figure
            title: Chart title
            chart_type: Type of chart
            show_legend: Whether to show legend
            palette: Color palette name
        
        Returns:
            Styled figure
        """
        # Enhanced layout with professional styling
        fig.update_layout(
            # Title styling
            title={
                "text": title,
                "font": {
                    "family": "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                    "size": 20,
                    "color": "#111827",
                    "weight": 600
                },
                "x": 0.5,
                "xanchor": "center",
                "y": 0.98,
                "yanchor": "top",
                "pad": {"b": 10}
            },
            
            # Font styling
            font={
                "family": "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                "size": 13,
                "color": "#374151"
            },
            
            # Legend styling
            showlegend=show_legend,
            legend={
                "orientation": "v" if chart_type in ["pie", "box"] else "h",
                "yanchor": "top" if chart_type in ["pie", "box"] else "bottom",
                "y": 0.99 if chart_type in ["pie", "box"] else 1.02,
                "xanchor": "right",
                "x": 0.99,
                "bgcolor": "rgba(255, 255, 255, 0.95)",
                "bordercolor": "#E5E7EB",
                "borderwidth": 1,
                "font": {
                    "size": 12,
                    "color": "#374151"
                },
                "itemsizing": "constant",
                "itemwidth": 30,
                "tracegroupgap": 5
            },
            
            # Margins
            margin={
                "l": 60,
                "r": 40,
                "t": 80,
                "b": 60,
                "pad": 4
            },
            
            # Background colors
            paper_bgcolor="rgba(255, 255, 255, 1)",
            plot_bgcolor="rgba(249, 250, 251, 1)",
            
            # Hover styling
            hovermode="closest" if chart_type == "scatter" else "x unified",
            hoverlabel={
                "bgcolor": "rgba(255, 255, 255, 0.95)",
                "bordercolor": "#E5E7EB",
                "font": {
                    "family": "Inter, sans-serif",
                    "size": 12,
                    "color": "#111827"
                }
            },
            
            # Template
            template="plotly_white",
            
            # Responsive
            autosize=True,
            
            # Modebar (toolbar)
            modebar={
                "bgcolor": "rgba(255, 255, 255, 0.9)",
                "color": "#6B7280",
                "activecolor": "#3B82F6"
            }
        )
        
        # Axis styling
        axis_config = {
            "showgrid": True,
            "gridcolor": "#E5E7EB",
            "gridwidth": 1,
            "showline": True,
            "linecolor": "#D1D5DB",
            "linewidth": 2,
            "zeroline": True,
            "zerolinecolor": "#9CA3AF",
            "zerolinewidth": 2,
            "title": {
                "font": {
                    "size": 14,
                    "color": "#374151",
                    "weight": 500
                }
            },
            "tickfont": {
                "size": 12,
                "color": "#6B7280"
            }
        }
        
        fig.update_xaxes(**axis_config)
        fig.update_yaxes(**axis_config)
        
        return fig
    
    @staticmethod
    def detect_chart_type(columns: List[str], rows: List[List[Any]], hints: Optional[Dict] = None) -> str:
        """
        Auto-detect the best chart type based on data characteristics
        Enhanced with better heuristics for Issue 13
        """
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
        percentage_cols = []
        
        for i, col in enumerate(columns):
            col_lower = col.lower()
            sample_values = [row[i] for row in rows[:min(50, num_rows)] if row[i] is not None]
            
            if not sample_values:
                continue
            
            # Check for percentage columns
            if any(kw in col_lower for kw in ['percent', 'rate', 'ratio', '%']):
                percentage_cols.append(i)
                numeric_cols.append(i)
            # Check for date columns
            elif any(kw in col_lower for kw in ['date', 'time', 'month', 'year', 'quarter', 'week', 'day']):
                date_cols.append(i)
            # Check for numeric
            elif all(isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit()) for v in sample_values):
                numeric_cols.append(i)
            else:
                categorical_cols.append(i)
        
        # Enhanced decision logic
        
        # Time series detection (date + numeric)
        if date_cols and numeric_cols:
            # Multiple metrics over time -> multi-line chart
            if len(numeric_cols) > 1:
                return "multi_line"
            return "line"
        
        # Percentage/rate data -> pie or stacked bar
        if percentage_cols and len(categorical_cols) == 1:
            if num_rows <= 8:
                return "pie"
            return "bar"
        
        # Single category with single metric
        if len(categorical_cols) == 1 and len(numeric_cols) == 1:
            if num_rows <= 10:
                return "pie"
            elif num_rows > 50:
                return "bar"  # Too many for pie
            return "bar"
        
        # Multiple categories with metrics -> grouped/stacked bar
        if len(categorical_cols) >= 2 and len(numeric_cols) >= 1:
            return "grouped_bar"
        
        # Single category with multiple metrics -> grouped bar
        if len(categorical_cols) == 1 and len(numeric_cols) > 1:
            return "grouped_bar"
        
        # Multiple numeric columns -> scatter or correlation
        if len(numeric_cols) >= 2:
            # Check for correlation analysis
            if len(numeric_cols) >= 3:
                return "heatmap"  # Correlation matrix
            return "scatter"
        
        # Trend analysis keywords
        trend_keywords = ['growth', 'trend', 'change', 'over time', 'progression']
        if hints and any(kw in str(hints).lower() for kw in trend_keywords):
            return "line"
        
        # Comparison keywords
        comparison_keywords = ['compare', 'vs', 'versus', 'difference', 'between']
        if hints and any(kw in str(hints).lower() for kw in comparison_keywords):
            return "grouped_bar"
        
        # Distribution keywords
        distribution_keywords = ['distribution', 'breakdown', 'composition', 'share']
        if hints and any(kw in str(hints).lower() for kw in distribution_keywords):
            if num_rows <= 10:
                return "pie"
            return "bar"
        
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
            
            # Select color palette based on chart type
            palette_name = hints.get("palette", "default") if hints else "default"
            colors = VisualizationService.get_color_palette(palette_name, max(len(numeric_cols), 8))
            
            # Generate chart based on type
            fig = None
            
            if chart_type == "bar":
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=data_dict[x_col],
                    y=data_dict[y_col],
                    name=y_col,
                    marker=dict(
                        color=colors[0],
                        line=dict(color=colors[0], width=0),
                        opacity=0.9
                    ),
                    text=[VisualizationService.format_number(v) if isinstance(v, (int, float)) else str(v) 
                          for v in data_dict[y_col]],
                    textposition='outside',
                    textfont=dict(size=11, color="#374151"),
                    hovertemplate="<b>%{x}</b><br>" +
                                  f"{y_col}: %{{y:,.2f}}<br>" +
                                  "<extra></extra>"
                ))
                
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"{y_col} by {x_col}",
                    chart_type,
                    show_legend=False,
                    palette=palette_name
                )
                
                fig.update_xaxes(
                    tickangle=-45 if len(data_dict[x_col]) > 5 else 0,
                    title_text=x_col
                )
                fig.update_yaxes(title_text=y_col)
                fig.update_layout(bargap=0.15, bargroupgap=0.1)
            
            elif chart_type == "grouped_bar":
                # Grouped bar chart for multiple metrics
                if len(numeric_cols) > 1:
                    fig = go.Figure()
                    for idx, metric in enumerate(numeric_cols[:5]):  # Limit to 5 metrics
                        fig.add_trace(go.Bar(
                            name=metric,
                            x=data_dict[x_col],
                            y=data_dict[metric],
                            marker=dict(
                                color=colors[idx % len(colors)],
                                line=dict(color=colors[idx % len(colors)], width=0),
                                opacity=0.9
                            ),
                            hovertemplate="<b>%{x}</b><br>" +
                                          f"{metric}: %{{y:,.2f}}<br>" +
                                          "<extra></extra>"
                        ))
                    
                    fig = VisualizationService.apply_professional_layout(
                        fig,
                        title or f"Comparison by {x_col}",
                        chart_type,
                        show_legend=True,
                        palette=palette_name
                    )
                    
                    fig.update_xaxes(
                        tickangle=-45 if len(data_dict[x_col]) > 5 else 0,
                        title_text=x_col
                    )
                    fig.update_yaxes(title_text="Value")
                    fig.update_layout(barmode='group', bargap=0.15, bargroupgap=0.05)
                else:
                    # Fallback to regular bar
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=data_dict[x_col],
                        y=data_dict[y_col],
                        marker=dict(color=colors[0], opacity=0.9)
                    ))
                    fig = VisualizationService.apply_professional_layout(
                        fig, title or f"{y_col} by {x_col}", chart_type, False, palette_name
                    )

            elif chart_type == "line":
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=data_dict[x_col],
                    y=data_dict[y_col],
                    name=y_col,
                    mode='lines+markers',
                    line=dict(
                        color=colors[0],
                        width=3,
                        shape='spline',  # Smooth curves
                        smoothing=0.3
                    ),
                    marker=dict(
                        size=8,
                        color=colors[0],
                        line=dict(color='white', width=2),
                        opacity=1
                    ),
                    fill='tozeroy',
                    fillcolor=f"rgba({int(colors[0][1:3], 16)}, {int(colors[0][3:5], 16)}, {int(colors[0][5:7], 16)}, 0.1)",
                    hovertemplate="<b>%{x}</b><br>" +
                                  f"{y_col}: %{{y:,.2f}}<br>" +
                                  "<extra></extra>"
                ))
                
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"{y_col} over {x_col}",
                    chart_type,
                    show_legend=False,
                    palette=palette_name
                )
                
                fig.update_xaxes(title_text=x_col)
                fig.update_yaxes(title_text=y_col)
            
            elif chart_type == "multi_line":
                # Multiple lines for time series comparison
                fig = go.Figure()
                for idx, metric in enumerate(numeric_cols[:5]):  # Limit to 5 lines
                    fig.add_trace(go.Scatter(
                        name=metric,
                        x=data_dict[x_col],
                        y=data_dict[metric],
                        mode='lines+markers',
                        line=dict(
                            color=colors[idx % len(colors)],
                            width=3,
                            shape='spline',
                            smoothing=0.3
                        ),
                        marker=dict(
                            size=7,
                            color=colors[idx % len(colors)],
                            line=dict(color='white', width=1.5),
                            opacity=1
                        ),
                        hovertemplate="<b>%{x}</b><br>" +
                                      f"{metric}: %{{y:,.2f}}<br>" +
                                      "<extra></extra>"
                    ))
                
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"Trends over {x_col}",
                    chart_type,
                    show_legend=True,
                    palette=palette_name
                )
                
                fig.update_xaxes(title_text=x_col)
                fig.update_yaxes(title_text="Value")
                
            elif chart_type == "pie":
                fig = go.Figure()
                fig.add_trace(go.Pie(
                    labels=data_dict[x_col],
                    values=data_dict[y_col],
                    marker=dict(
                        colors=colors,
                        line=dict(color='white', width=2)
                    ),
                    textposition='inside',
                    textinfo='label+percent',
                    textfont=dict(size=13, color='white', family='Inter, sans-serif'),
                    insidetextorientation='radial',
                    hovertemplate="<b>%{label}</b><br>" +
                                  f"{y_col}: %{{value:,.2f}}<br>" +
                                  "Percentage: %{percent}<br>" +
                                  "<extra></extra>",
                    hole=0.3,  # Donut chart
                    pull=[0.05 if i == 0 else 0 for i in range(len(data_dict[x_col]))]  # Pull out first slice
                ))
                
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"{y_col} Distribution",
                    chart_type,
                    show_legend=True,
                    palette=palette_name
                )
                
            elif chart_type == "scatter":
                y2_col = numeric_cols[1] if len(numeric_cols) > 1 else y_col
                x_scatter = x_col if x_col in numeric_cols else y_col
                
                # Color by category if available
                color_col = categorical_cols[0] if categorical_cols else None
                
                fig = go.Figure()
                
                if color_col:
                    # Scatter with categories
                    unique_categories = list(set(data_dict[color_col]))
                    for idx, category in enumerate(unique_categories[:8]):
                        mask = [i for i, v in enumerate(data_dict[color_col]) if v == category]
                        fig.add_trace(go.Scatter(
                            name=str(category),
                            x=[data_dict[x_scatter][i] for i in mask],
                            y=[data_dict[y2_col][i] for i in mask],
                            mode='markers',
                            marker=dict(
                                size=10,
                                color=colors[idx % len(colors)],
                                line=dict(color='white', width=1),
                                opacity=0.8
                            ),
                            hovertemplate=f"<b>{category}</b><br>" +
                                          f"{x_scatter}: %{{x:,.2f}}<br>" +
                                          f"{y2_col}: %{{y:,.2f}}<br>" +
                                          "<extra></extra>"
                        ))
                else:
                    # Single scatter
                    fig.add_trace(go.Scatter(
                        x=data_dict[x_scatter],
                        y=data_dict[y2_col],
                        mode='markers',
                        marker=dict(
                            size=10,
                            color=colors[0],
                            line=dict(color='white', width=1),
                            opacity=0.8
                        ),
                        hovertemplate=f"{x_scatter}: %{{x:,.2f}}<br>" +
                                      f"{y2_col}: %{{y:,.2f}}<br>" +
                                      "<extra></extra>"
                    ))
                
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"{y2_col} vs {x_scatter}",
                    chart_type,
                    show_legend=bool(color_col),
                    palette=palette_name
                )
                
                fig.update_xaxes(title_text=x_scatter)
                fig.update_yaxes(title_text=y2_col)
                
            elif chart_type == "area":
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=data_dict[x_col],
                    y=data_dict[y_col],
                    name=y_col,
                    mode='lines',
                    line=dict(
                        color=colors[0],
                        width=2,
                        shape='spline',
                        smoothing=0.3
                    ),
                    fill='tozeroy',
                    fillcolor=f"rgba({int(colors[0][1:3], 16)}, {int(colors[0][3:5], 16)}, {int(colors[0][5:7], 16)}, 0.4)",
                    hovertemplate="<b>%{x}</b><br>" +
                                  f"{y_col}: %{{y:,.2f}}<br>" +
                                  "<extra></extra>"
                ))
                
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"{y_col} over {x_col}",
                    chart_type,
                    show_legend=False,
                    palette=palette_name
                )
                
                fig.update_xaxes(title_text=x_col)
                fig.update_yaxes(title_text=y_col)
                
            elif chart_type == "heatmap" and len(numeric_cols) >= 2:
                # Create correlation matrix for heatmap
                try:
                    import pandas as pd
                    df = pd.DataFrame(data_dict)
                    numeric_df = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
                    corr = numeric_df.corr()
                    
                    fig = go.Figure()
                    fig.add_trace(go.Heatmap(
                        z=corr.values,
                        x=corr.columns,
                        y=corr.columns,
                        colorscale='RdBu_r',
                        zmid=0,
                        text=corr.values,
                        texttemplate='%{text:.2f}',
                        textfont=dict(size=11, color='white'),
                        hovertemplate="<b>%{x} vs %{y}</b><br>" +
                                      "Correlation: %{z:.3f}<br>" +
                                      "<extra></extra>",
                        colorbar=dict(
                            title="Correlation",
                            titleside="right",
                            tickmode="linear",
                            tick0=-1,
                            dtick=0.5,
                            thickness=15,
                            len=0.7
                        )
                    ))
                    
                    fig = VisualizationService.apply_professional_layout(
                        fig,
                        title or "Correlation Heatmap",
                        chart_type,
                        show_legend=False,
                        palette=palette_name
                    )
                    
                    fig.update_xaxes(title_text="", side="bottom")
                    fig.update_yaxes(title_text="", autorange="reversed")
                    fig.update_layout(plot_bgcolor="white")
                    
                except ImportError:
                    logger.warning("Pandas not available for heatmap generation")
                    # Fallback to bar chart
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=data_dict[x_col], y=data_dict[y_col], marker=dict(color=colors[0])))
                    fig = VisualizationService.apply_professional_layout(fig, title or "Data", "bar", False, palette_name)
            
            elif chart_type == "box":
                # Box plot for distribution analysis
                if len(numeric_cols) >= 1:
                    fig = go.Figure()
                    for idx, metric in enumerate(numeric_cols[:5]):
                        fig.add_trace(go.Box(
                            y=data_dict[metric],
                            name=metric,
                            marker=dict(
                                color=colors[idx % len(colors)],
                                opacity=0.7
                            ),
                            line=dict(color=colors[idx % len(colors)], width=2),
                            boxmean='sd',  # Show mean and standard deviation
                            hovertemplate=f"<b>{metric}</b><br>" +
                                          "Max: %{y:.2f}<br>" +
                                          "<extra></extra>"
                        ))
                    
                    fig = VisualizationService.apply_professional_layout(
                        fig,
                        title or "Distribution Analysis",
                        chart_type,
                        show_legend=True,
                        palette=palette_name
                    )
                    
                    fig.update_yaxes(title_text="Value")
                else:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=data_dict[x_col], y=data_dict[y_col], marker=dict(color=colors[0])))
                    fig = VisualizationService.apply_professional_layout(fig, title or "Data", "bar", False, palette_name)
            
            else:
                # Default to bar
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=data_dict[x_col],
                    y=data_dict[y_col],
                    marker=dict(color=colors[0], opacity=0.9)
                ))
                fig = VisualizationService.apply_professional_layout(
                    fig,
                    title or f"{y_col} by {x_col}",
                    "bar",
                    show_legend=False,
                    palette=palette_name
                )
            
            if fig:
                # Add statistical overlays if applicable
                stats_info = {}
                if chart_type in ["bar", "line", "scatter"] and y_col in data_dict:
                    y_values = [v for v in data_dict[y_col] if v is not None]
                    try:
                        numeric_y = [float(v) for v in y_values if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit())]
                        if numeric_y:
                            mean_val = sum(numeric_y) / len(numeric_y)
                            median_val = sorted(numeric_y)[len(numeric_y) // 2]
                            std_dev = (sum((x - mean_val) ** 2 for x in numeric_y) / len(numeric_y)) ** 0.5
                            
                            stats_info = {
                                "mean": mean_val,
                                "median": median_val,
                                "std_dev": std_dev,
                                "min": min(numeric_y),
                                "max": max(numeric_y),
                                "count": len(numeric_y),
                                "range": max(numeric_y) - min(numeric_y)
                            }
                            
                            # Add mean line for bar and line charts (if enabled)
                            if chart_type in ["bar", "line"] and hints and hints.get("show_mean", False):
                                fig.add_hline(
                                    y=mean_val,
                                    line_dash="dash",
                                    line_color="#10B981",
                                    line_width=2,
                                    annotation_text=f"Mean: {VisualizationService.format_number(mean_val)}",
                                    annotation_position="top right",
                                    annotation=dict(
                                        font=dict(size=11, color="#10B981"),
                                        bgcolor="rgba(255, 255, 255, 0.9)",
                                        bordercolor="#10B981",
                                        borderwidth=1,
                                        borderpad=4
                                    )
                                )
                            
                            # Add peak annotation (if enabled)
                            if hints and hints.get("show_peaks", False) and chart_type in ["bar", "line"]:
                                max_idx = numeric_y.index(max(numeric_y))
                                if max_idx < len(data_dict[x_col]):
                                    fig.add_annotation(
                                        x=data_dict[x_col][max_idx],
                                        y=max(numeric_y),
                                        text=f"Peak: {VisualizationService.format_number(max(numeric_y))}",
                                        showarrow=True,
                                        arrowhead=2,
                                        arrowcolor="#EF4444",
                                        arrowwidth=2,
                                        ax=0,
                                        ay=-40,
                                        font=dict(size=11, color="#EF4444"),
                                        bgcolor="rgba(255, 255, 255, 0.9)",
                                        bordercolor="#EF4444",
                                        borderwidth=1,
                                        borderpad=4
                                    )
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Could not calculate statistics: {e}")
                
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
                    "statistics": stats_info,
                    "palette": palette_name
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

    
    @staticmethod
    def get_chart_recommendations(
        columns: List[str],
        rows: List[List[Any]],
        user_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get intelligent chart recommendations based on data and query context
        Enhanced for Issue 13
        
        Returns:
            Dict with recommended chart types, reasoning, and alternatives
        """
        if not rows or not columns:
            return {
                "status": "error",
                "message": "No data to analyze"
            }
        
        # Analyze data characteristics
        num_rows = len(rows)
        num_cols = len(columns)
        
        numeric_cols = []
        categorical_cols = []
        date_cols = []
        
        for i, col in enumerate(columns):
            col_lower = col.lower()
            sample_values = [row[i] for row in rows[:min(50, num_rows)] if row[i] is not None]
            
            if not sample_values:
                continue
            
            if any(kw in col_lower for kw in ['date', 'time', 'month', 'year', 'quarter', 'week', 'day']):
                date_cols.append(col)
            elif all(isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit()) for v in sample_values):
                numeric_cols.append(col)
            else:
                categorical_cols.append(col)
        
        recommendations = []
        
        # Time series analysis
        if date_cols and numeric_cols:
            recommendations.append({
                "chart_type": "line" if len(numeric_cols) == 1 else "multi_line",
                "confidence": 0.95,
                "reasoning": f"Time series data detected with {len(numeric_cols)} metric(s) over time",
                "best_for": "Showing trends and patterns over time",
                "columns": {
                    "x": date_cols[0],
                    "y": numeric_cols
                }
            })
            
            recommendations.append({
                "chart_type": "area",
                "confidence": 0.75,
                "reasoning": "Alternative visualization for cumulative trends",
                "best_for": "Emphasizing volume and accumulation",
                "columns": {
                    "x": date_cols[0],
                    "y": numeric_cols[0] if numeric_cols else None
                }
            })
        
        # Categorical comparison
        if categorical_cols and numeric_cols:
            if num_rows <= 10:
                recommendations.append({
                    "chart_type": "pie",
                    "confidence": 0.85,
                    "reasoning": f"Small dataset ({num_rows} categories) suitable for pie chart",
                    "best_for": "Showing proportions and composition",
                    "columns": {
                        "names": categorical_cols[0],
                        "values": numeric_cols[0]
                    }
                })
            
            recommendations.append({
                "chart_type": "bar" if len(numeric_cols) == 1 else "grouped_bar",
                "confidence": 0.90,
                "reasoning": f"Comparing {len(numeric_cols)} metric(s) across {len(categorical_cols)} dimension(s)",
                "best_for": "Comparing values across categories",
                "columns": {
                    "x": categorical_cols[0],
                    "y": numeric_cols
                }
            })
        
        # Correlation analysis
        if len(numeric_cols) >= 2:
            recommendations.append({
                "chart_type": "scatter",
                "confidence": 0.80,
                "reasoning": f"Multiple numeric columns ({len(numeric_cols)}) suitable for correlation analysis",
                "best_for": "Identifying relationships between variables",
                "columns": {
                    "x": numeric_cols[0],
                    "y": numeric_cols[1]
                }
            })
            
            if len(numeric_cols) >= 3:
                recommendations.append({
                    "chart_type": "heatmap",
                    "confidence": 0.75,
                    "reasoning": "Multiple metrics suitable for correlation matrix",
                    "best_for": "Visualizing correlations between all numeric variables",
                    "columns": numeric_cols
                })
        
        # Distribution analysis
        if len(numeric_cols) >= 1 and num_rows >= 10:
            recommendations.append({
                "chart_type": "box",
                "confidence": 0.70,
                "reasoning": "Sufficient data for distribution analysis",
                "best_for": "Understanding data distribution, outliers, and quartiles",
                "columns": numeric_cols
            })
        
        # Query context analysis
        if user_query:
            query_lower = user_query.lower()
            
            # Trend keywords boost line charts
            if any(kw in query_lower for kw in ['trend', 'over time', 'growth', 'change']):
                for rec in recommendations:
                    if rec["chart_type"] in ["line", "multi_line", "area"]:
                        rec["confidence"] = min(0.98, rec["confidence"] + 0.10)
                        rec["reasoning"] += " (query indicates trend analysis)"
            
            # Comparison keywords boost bar charts
            if any(kw in query_lower for kw in ['compare', 'vs', 'versus', 'difference']):
                for rec in recommendations:
                    if rec["chart_type"] in ["bar", "grouped_bar"]:
                        rec["confidence"] = min(0.98, rec["confidence"] + 0.10)
                        rec["reasoning"] += " (query indicates comparison)"
            
            # Distribution keywords boost pie/box charts
            if any(kw in query_lower for kw in ['distribution', 'breakdown', 'composition', 'share']):
                for rec in recommendations:
                    if rec["chart_type"] in ["pie", "box"]:
                        rec["confidence"] = min(0.98, rec["confidence"] + 0.10)
                        rec["reasoning"] += " (query indicates distribution analysis)"
        
        # Sort by confidence
        recommendations.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Add default table option
        recommendations.append({
            "chart_type": "table",
            "confidence": 0.50,
            "reasoning": "Raw data table for detailed inspection",
            "best_for": "Viewing exact values and detailed data",
            "columns": columns
        })
        
        return {
            "status": "success",
            "primary_recommendation": recommendations[0] if recommendations else None,
            "all_recommendations": recommendations,
            "data_characteristics": {
                "rows": num_rows,
                "columns": num_cols,
                "numeric_columns": len(numeric_cols),
                "categorical_columns": len(categorical_cols),
                "date_columns": len(date_cols)
            }
        }
