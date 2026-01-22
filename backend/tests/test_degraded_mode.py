"""
Tests for Degraded Mode Manager and Graceful Degradation

Verifies that the system handles component failures gracefully and provides
appropriate fallback mechanisms.
"""

import pytest
import asyncio
from datetime import datetime, timezone

from app.core.degraded_mode_manager import (
    DegradedModeManager,
    DegradationLevel,
    ComponentStatus,
    ComponentState
)
from app.core.langgraph_checkpointer_fallback import (
    InMemoryCheckpointer,
    ResilientCheckpointerWrapper,
    InMemoryCheckpointerContext
)


class TestDegradedModeManager:
    """Test degraded mode manager functionality"""
    
    def test_initialization(self):
        """Test manager initialization"""
        manager = DegradedModeManager()
        assert manager.degradation_level == DegradationLevel.NORMAL
        assert len(manager.components) == 0
    
    def test_component_registration(self):
        """Test component registration"""
        manager = DegradedModeManager()
        
        manager.register_component(
            "test_component",
            ComponentStatus.OPERATIONAL,
            "Test component impact"
        )
        
        assert "test_component" in manager.components
        assert manager.components["test_component"].status == ComponentStatus.OPERATIONAL
    
    def test_degradation_level_calculation(self):
        """Test automatic degradation level calculation"""
        manager = DegradedModeManager()
        
        # Register components
        manager.register_component("comp1")
        manager.register_component("comp2")
        manager.register_component("comp3")
        manager.register_component("comp4")
        
        # All operational -> NORMAL
        assert manager.degradation_level == DegradationLevel.NORMAL
        
        # One degraded -> PARTIAL
        manager.update_component_status("comp1", ComponentStatus.DEGRADED)
        assert manager.degradation_level == DegradationLevel.PARTIAL
        
        # Two degraded -> SEVERE
        manager.update_component_status("comp2", ComponentStatus.DEGRADED)
        assert manager.degradation_level == DegradationLevel.SEVERE
        
        # Two unavailable -> CRITICAL
        manager.update_component_status("comp3", ComponentStatus.UNAVAILABLE)
        manager.update_component_status("comp4", ComponentStatus.UNAVAILABLE)
        assert manager.degradation_level == DegradationLevel.CRITICAL
    
    def test_system_status(self):
        """Test system status reporting"""
        manager = DegradedModeManager()
        
        manager.register_component("redis", impact_description="Caching")
        manager.update_component_status(
            "redis",
            ComponentStatus.DEGRADED,
            degradation_reason="Connection failed",
            fallback_active=True,
            fallback_type="in_memory"
        )
        
        status = manager.get_system_status()
        
        assert status["is_degraded"] is True
        assert status["degradation_level"] == DegradationLevel.PARTIAL.value
        assert len(status["degraded_components"]) == 1
        assert "Caching" in status["affected_features"]
    
    def test_feature_availability(self):
        """Test feature availability checking"""
        manager = DegradedModeManager()
        
        manager.register_component("redis")
        manager.update_component_status("redis", ComponentStatus.OPERATIONAL)
        
        assert manager.is_feature_available("caching") is True
        
        manager.update_component_status("redis", ComponentStatus.UNAVAILABLE)
        assert manager.is_feature_available("caching") is False


class TestInMemoryCheckpointer:
    """Test in-memory checkpointer fallback"""
    
    @pytest.mark.asyncio
    async def test_checkpoint_save_and_retrieve(self):
        """Test saving and retrieving checkpoints"""
        checkpointer = InMemoryCheckpointer()
        
        config = {
            "configurable": {
                "thread_id": "test_thread"
            }
        }
        
        checkpoint_data = {"state": "test_state", "step": 1}
        metadata = {"user": "test_user"}
        
        # Save checkpoint
        updated_config = await checkpointer.aput(
            config, checkpoint_data, metadata, {}
        )
        
        assert "checkpoint_id" in updated_config["configurable"]
        
        # Retrieve checkpoint
        result = await checkpointer.aget(updated_config)
        assert result is not None
        assert result[0] == checkpoint_data
        assert result[1] == metadata
    
    @pytest.mark.asyncio
    async def test_checkpoint_list(self):
        """Test listing checkpoints"""
        checkpointer = InMemoryCheckpointer()
        
        config = {
            "configurable": {
                "thread_id": "test_thread"
            }
        }
        
        # Save multiple checkpoints
        for i in range(3):
            checkpoint_data = {"state": f"state_{i}", "step": i}
            metadata = {"step": i}
            config = await checkpointer.aput(config, checkpoint_data, metadata, {})
        
        # List checkpoints
        checkpoints = []
        async for cp_data, cp_metadata in checkpointer.alist(config):
            checkpoints.append((cp_data, cp_metadata))
        
        assert len(checkpoints) == 3
        # Should be in reverse order (newest first)
        assert checkpoints[0][0]["step"] == 2
        assert checkpoints[2][0]["step"] == 0
    
    @pytest.mark.asyncio
    async def test_checkpoint_cleanup(self):
        """Test automatic checkpoint cleanup"""
        checkpointer = InMemoryCheckpointer(max_checkpoints_per_thread=5)
        
        config = {
            "configurable": {
                "thread_id": "test_thread"
            }
        }
        
        # Save more than max checkpoints
        for i in range(10):
            checkpoint_data = {"state": f"state_{i}", "step": i}
            metadata = {"step": i}
            config = await checkpointer.aput(config, checkpoint_data, metadata, {})
        
        # Should only keep last 5
        checkpoints = []
        async for cp_data, cp_metadata in checkpointer.alist(config):
            checkpoints.append((cp_data, cp_metadata))
        
        assert len(checkpoints) == 5
        assert checkpoints[0][0]["step"] == 9  # Newest
        assert checkpoints[4][0]["step"] == 5  # Oldest kept
    
    def test_checkpointer_stats(self):
        """Test checkpointer statistics"""
        checkpointer = InMemoryCheckpointer()
        
        stats = checkpointer.get_stats()
        
        assert stats["type"] == "in_memory"
        assert stats["active_threads"] == 0
        assert stats["total_checkpoints"] == 0


class TestResilientCheckpointerWrapper:
    """Test resilient checkpointer wrapper"""
    
    @pytest.mark.asyncio
    async def test_fallback_on_failure(self):
        """Test automatic fallback when SQLite fails"""
        
        # Mock SQLite checkpointer that always fails
        class FailingCheckpointer:
            async def aput(self, config, checkpoint, metadata, new_versions):
                raise Exception("SQLite connection failed")
            
            async def aget(self, config):
                raise Exception("SQLite connection failed")
            
            async def alist(self, config, limit=None, before=None):
                raise Exception("SQLite connection failed")
                yield  # Make it a generator
        
        failing_cp = FailingCheckpointer()
        wrapper = ResilientCheckpointerWrapper(failing_cp, enable_fallback=True)
        
        config = {
            "configurable": {
                "thread_id": "test_thread"
            }
        }
        
        checkpoint_data = {"state": "test_state"}
        metadata = {"user": "test_user"}
        
        # Should use fallback after failures
        for _ in range(3):
            await wrapper.aput(config, checkpoint_data, metadata, {})
        
        assert wrapper.is_degraded is True
        assert wrapper.failure_count >= wrapper.failure_threshold
        
        # Subsequent operations should use fallback
        result = await wrapper.aget(config)
        assert result is not None  # Should get from fallback
    
    def test_wrapper_status(self):
        """Test wrapper status reporting"""
        
        class MockCheckpointer:
            async def aput(self, config, checkpoint, metadata, new_versions):
                return config
        
        mock_cp = MockCheckpointer()
        wrapper = ResilientCheckpointerWrapper(mock_cp, enable_fallback=True)
        
        status = wrapper.get_status()
        
        assert "is_degraded" in status
        assert "failure_count" in status
        assert "fallback_enabled" in status
        assert status["fallback_enabled"] is True


class TestInMemoryCheckpointerContext:
    """Test in-memory checkpointer context manager"""
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test context manager lifecycle"""
        context = InMemoryCheckpointerContext()
        
        async with context as checkpointer:
            assert isinstance(checkpointer, InMemoryCheckpointer)
            
            # Use checkpointer
            config = {
                "configurable": {
                    "thread_id": "test_thread"
                }
            }
            
            checkpoint_data = {"state": "test_state"}
            metadata = {"user": "test_user"}
            
            await checkpointer.aput(config, checkpoint_data, metadata, {})
            
            result = await checkpointer.aget(config)
            assert result is not None


@pytest.mark.asyncio
async def test_integration_degraded_mode_with_checkpointer():
    """Integration test: degraded mode manager with checkpointer fallback"""
    from app.core.degraded_mode_manager import degraded_mode_manager
    
    # Register checkpointer component
    degraded_mode_manager.register_component(
        "langgraph_checkpointer",
        ComponentStatus.OPERATIONAL
    )
    
    # Simulate checkpointer failure
    degraded_mode_manager.update_component_status(
        "langgraph_checkpointer",
        ComponentStatus.DEGRADED,
        degradation_reason="SQLite connection failed",
        fallback_active=True,
        fallback_type="in_memory"
    )
    
    # Check system status
    status = degraded_mode_manager.get_system_status()
    
    assert status["is_degraded"] is True
    assert "Query state persistence" in status["affected_features"]
    
    # Verify feature availability
    assert degraded_mode_manager.is_feature_available("query_checkpointing") is True  # Degraded but available


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

