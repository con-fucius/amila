"""
Tests for Dashboard Generator
"""

import pytest
from unittest.mock import patch, AsyncMock

from app.services.dashboard_generator import DashboardGenerator


@pytest.mark.asyncio
async def test_generate_from_query_simple():
    """Test dashboard generation from simple query results"""
    query_results = {
        "rows": [
            ["2024-01-01", 100],
            ["2024-01-02", 150],
            ["2024-01-03", 120]
        ],
        "columns": ["date", "revenue"]
    }
    
    result = await DashboardGenerator.generate_from_query(
        sql_query="SELECT date, revenue FROM sales",
        query_results=query_results,
        title="Sales Dashboard"
    )
    
    assert result["status"] == "success"
    dashboard = result["dashboard"]
    assert dashboard["title"] == "Sales Dashboard"
    assert len(dashboard["charts"]) > 0
    assert "layout" in dashboard


@pytest.mark.asyncio
async def test_generate_from_query_multi_column():
    """Test dashboard generation with multiple columns"""
    query_results = {
        "rows": [
            ["2024-01-01", "North", 100, 50],
            ["2024-01-01", "South", 150, 75],
            ["2024-01-02", "North", 120, 60]
        ],
        "columns": ["date", "region", "revenue", "costs"]
    }
    
    result = await DashboardGenerator.generate_from_query(
        sql_query="SELECT * FROM sales",
        query_results=query_results
    )
    
    assert result["status"] == "success"
    dashboard = result["dashboard"]
    charts = dashboard["charts"]
    
    # Should have multiple chart recommendations
    assert len(charts) >= 2
    
    # Check for different chart types
    chart_types = [c["type"] for c in charts]
    assert "line" in chart_types or "bar" in chart_types


@pytest.mark.asyncio
async def test_profile_data():
    """Test data profiling"""
    query_results = {
        "rows": [
            ["2024-01-01", 100, "A"],
            ["2024-01-02", 150, "B"],
            ["2024-01-03", 120, "A"]
        ],
        "columns": ["date", "amount", "category"]
    }
    
    profile = DashboardGenerator._profile_data(query_results)
    
    assert profile["row_count"] == 3
    assert profile["column_count"] == 3
    assert len(profile["columns"]) == 3
    
    # Check column types
    col_types = [c["type"] for c in profile["columns"]]
    assert "numeric" in col_types
    assert "categorical" in col_types


@pytest.mark.asyncio
async def test_recommend_charts_temporal():
    """Test chart recommendation for temporal data"""
    data_profile = {
        "row_count": 10,
        "column_count": 2,
        "columns": [
            {
                "name": "date",
                "type": "temporal",
                "is_temporal": True,
                "is_numeric": False,
                "is_categorical": False
            },
            {
                "name": "revenue",
                "type": "numeric",
                "is_temporal": False,
                "is_numeric": True,
                "is_categorical": False
            }
        ]
    }
    
    charts = DashboardGenerator._recommend_charts(data_profile)
    
    assert len(charts) > 0
    # Should recommend line chart for time series
    assert any(c["type"] == "line" for c in charts)


@pytest.mark.asyncio
async def test_recommend_charts_categorical():
    """Test chart recommendation for categorical data"""
    data_profile = {
        "row_count": 10,
        "column_count": 2,
        "columns": [
            {
                "name": "category",
                "type": "categorical",
                "is_temporal": False,
                "is_numeric": False,
                "is_categorical": True,
                "cardinality": 5
            },
            {
                "name": "value",
                "type": "numeric",
                "is_temporal": False,
                "is_numeric": True,
                "is_categorical": False
            }
        ]
    }
    
    charts = DashboardGenerator._recommend_charts(data_profile)
    
    assert len(charts) > 0
    # Should recommend bar chart for categorical/numeric
    assert any(c["type"] == "bar" for c in charts)


@pytest.mark.asyncio
async def test_optimize_layout():
    """Test layout optimization"""
    charts = [
        {"type": "line", "title": "Chart 1"},
        {"type": "bar", "title": "Chart 2"},
        {"type": "scatter", "title": "Chart 3"}
    ]
    
    layout = DashboardGenerator._optimize_layout(charts)
    
    assert "grid" in layout
    assert "positions" in layout
    assert len(layout["positions"]) == 3
    
    # Check grid dimensions
    grid = layout["grid"]
    assert grid["rows"] > 0
    assert grid["cols"] > 0


@pytest.mark.asyncio
async def test_generate_from_pattern():
    """Test pattern-based dashboard generation"""
    result = await DashboardGenerator.generate_from_pattern(
        pattern_name="revenue_analysis"
    )
    
    assert result["status"] == "success"
    dashboard = result["dashboard"]
    assert dashboard["title"] == "Revenue Analysis Dashboard"
    assert len(dashboard["charts"]) > 0


@pytest.mark.asyncio
async def test_generate_from_pattern_unknown():
    """Test unknown pattern handling"""
    result = await DashboardGenerator.generate_from_pattern(
        pattern_name="nonexistent_pattern"
    )
    
    assert result["status"] == "error"
    assert "Unknown pattern" in result["error"]


@pytest.mark.asyncio
async def test_store_and_retrieve_dashboard():
    """Test dashboard storage and retrieval"""
    dashboard_spec = {
        "id": "test_dashboard_123",
        "title": "Test Dashboard",
        "charts": []
    }
    
    with patch('app.core.redis_client.redis_client.set', new_callable=AsyncMock) as mock_set:
        with patch('app.core.redis_client.redis_client.lpush', new_callable=AsyncMock) as mock_lpush:
            dashboard_id = await DashboardGenerator.store_dashboard(dashboard_spec)
            
            assert dashboard_id == "test_dashboard_123"
            mock_set.assert_called_once()
            mock_lpush.assert_called_once()


@pytest.mark.asyncio
async def test_list_dashboards():
    """Test dashboard listing"""
    with patch('app.core.redis_client.redis_client.lrange', return_value=["dash_1", "dash_2"]):
        with patch('app.services.dashboard_generator.DashboardGenerator.get_dashboard',
                   return_value={"id": "dash_1", "title": "Test", "charts": []}):
            dashboards = await DashboardGenerator.list_dashboards(limit=10)
            
            assert isinstance(dashboards, list)


@pytest.mark.asyncio
async def test_delete_dashboard():
    """Test dashboard deletion"""
    with patch('app.core.redis_client.redis_client.delete', new_callable=AsyncMock) as mock_delete:
        with patch('app.core.redis_client.redis_client.lrem', new_callable=AsyncMock) as mock_lrem:
            success = await DashboardGenerator.delete_dashboard("test_id")
            
            assert success is True
            mock_delete.assert_called_once()
            mock_lrem.assert_called_once()


@pytest.mark.asyncio
async def test_empty_results():
    """Test handling of empty query results"""
    query_results = {
        "rows": [],
        "columns": []
    }
    
    result = await DashboardGenerator.generate_from_query(
        sql_query="SELECT * FROM empty_table",
        query_results=query_results
    )
    
    # Should handle gracefully
    assert result["status"] in ["success", "error"]
