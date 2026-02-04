"""
Tests for Temporal Graph Service
"""

import pytest
from datetime import datetime, timedelta

from app.services.temporal_graph_service import TemporalGraphService


@pytest.mark.asyncio
async def test_add_entity():
    """Test adding entity with timestamp"""
    service = TemporalGraphService()
    
    result = await service.add_entity(
        entity_id="customer_123",
        entity_type="customer",
        properties={"name": "John Doe", "status": "active"}
    )
    
    assert result["status"] == "success"
    assert result["entity"]["id"] == "customer_123"
    assert "timestamp" in result["entity"]


@pytest.mark.asyncio
async def test_query_temporal():
    """Test temporal querying"""
    service = TemporalGraphService()
    
    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)
    
    # Add entities at different times
    await service.add_entity(
        "entity_1", "product", {"price": 100}, timestamp=yesterday
    )
    await service.add_entity(
        "entity_2", "product", {"price": 200}, timestamp=now
    )
    
    # Query for recent entities
    result = await service.query_temporal(
        entity_type="product",
        start_date=now - timedelta(hours=1)
    )
    
    assert result["status"] == "success"
    assert result["count"] >= 1


@pytest.mark.asyncio
async def test_track_entity_evolution():
    """Test entity evolution tracking"""
    service = TemporalGraphService()
    
    entity_id = "customer_456"
    
    # Add initial state
    await service.add_entity(
        entity_id, "customer", {"status": "pending"}, 
        timestamp=datetime(2024, 1, 1)
    )
    
    # Update state
    await service.add_entity(
        entity_id, "customer", {"status": "active"},
        timestamp=datetime(2024, 1, 15)
    )
    
    # Track evolution
    result = await service.track_entity_evolution(entity_id)
    
    assert result["status"] == "success"
    assert result["total_snapshots"] == 2
    assert len(result["timeline"]) == 2
    
    # Check changes detected
    assert "changes" in result["timeline"][1]


@pytest.mark.asyncio
async def test_add_relationship():
    """Test adding temporal relationships"""
    service = TemporalGraphService()
    
    result = await service.add_relationship(
        from_entity="customer_1",
        to_entity="order_1",
        relationship_type="PLACED",
        properties={"amount": 500}
    )
    
    assert result["status"] == "success"
    assert result["relationship"]["type"] == "PLACED"


@pytest.mark.asyncio
async def test_query_relationships():
    """Test querying relationships"""
    service = TemporalGraphService()
    
    # Add relationship
    await service.add_relationship(
        "customer_1", "order_1", "PLACED"
    )
    
    # Query relationships
    result = await service.query_relationships(
        entity_id="customer_1",
        relationship_type="PLACED"
    )
    
    assert result["status"] == "success"
    assert result["count"] >= 1


@pytest.mark.asyncio
async def test_get_entity_state_at():
    """Test getting entity state at specific time"""
    service = TemporalGraphService()
    
    entity_id = "product_789"
    
    time1 = datetime(2024, 1, 1)
    time2 = datetime(2024, 1, 15)
    query_time = datetime(2024, 1, 10)
    
    # Add states
    await service.add_entity(
        entity_id, "product", {"price": 100}, timestamp=time1
    )
    await service.add_entity(
        entity_id, "product", {"price": 150}, timestamp=time2
    )
    
    # Query state at time between the two
    result = await service.get_entity_state_at(entity_id, query_time)
    
    assert result["status"] == "success"
    assert result["entity"]["properties"]["price"] == 100  # Should get earlier state


@pytest.mark.asyncio
async def test_temporal_filter():
    """Test temporal filtering by date range"""
    service = TemporalGraphService()
    
    jan_1 = datetime(2024, 1, 1)
    jan_15 = datetime(2024, 1, 15)
    feb_1 = datetime(2024, 2, 1)
    
    await service.add_entity("e1", "test", {}, timestamp=jan_1)
    await service.add_entity("e2", "test", {}, timestamp=jan_15)
    await service.add_entity("e3", "test", {}, timestamp=feb_1)
    
    # Query for January only
    result = await service.query_temporal(
        entity_type="test",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 31)
    )
    
    assert result["count"] == 2


@pytest.mark.asyncio
async def test_entity_not_found():
    """Test handling of non-existent entity"""
    service = TemporalGraphService()
    
    result = await service.track_entity_evolution("nonexistent")
    
    assert result["status"] == "error"
    assert "not found" in result["error"]
