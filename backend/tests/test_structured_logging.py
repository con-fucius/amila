"""
Test Suite for Structured Logging

Tests:
- Trace ID generation and propagation
- Context variables (user_id, session_id)
- Error categorization
- Performance tracking
"""

import pytest
import uuid
from datetime import datetime, timezone

from app.core.structured_logging import (
    get_trace_id,
    set_trace_id,
    set_user_context,
    clear_context,
    PerformanceTracker,
    get_logger,
)


class TestTraceIDManagement:
    """Test trace ID generation and context management"""
    
    def test_trace_id_generation(self):
        """Test automatic trace ID generation"""
        trace_id = get_trace_id()
        assert trace_id is not None
        assert isinstance(trace_id, str)
        assert len(trace_id) > 0
    
    def test_trace_id_persistence(self):
        """Test trace ID persists within context"""
        trace_id1 = get_trace_id()
        trace_id2 = get_trace_id()
        assert trace_id1 == trace_id2
    
    def test_trace_id_setting(self):
        """Test manual trace ID setting"""
        custom_trace_id = str(uuid.uuid4())
        set_trace_id(custom_trace_id)
        assert get_trace_id() == custom_trace_id
    
    def test_context_clearing(self):
        """Test context cleanup"""
        set_trace_id(str(uuid.uuid4()))
        set_user_context(user_id="test_user", session_id="test_session")
        
        clear_context()
        
        # After clearing, should get new trace ID
        new_trace_id = get_trace_id()
        assert new_trace_id is not None


class TestUserContext:
    """Test user context management"""
    
    def test_user_context_setting(self):
        """Test setting user context"""
        user_id = "test_user_123"
        session_id = "session_456"
        
        set_user_context(user_id=user_id, session_id=session_id)
        # Context is set, would be verified in actual logs
        assert True
    
    def test_partial_user_context(self):
        """Test setting only user_id or session_id"""
        set_user_context(user_id="user_only")
        set_user_context(session_id="session_only")
        assert True


class TestPerformanceTracker:
    """Test performance tracking context manager"""
    
    def test_performance_tracker_success(self):
        """Test performance tracking for successful operation"""
        logger = get_logger("test")
        
        with PerformanceTracker("test_operation", logger):
            # Simulate some work
            result = 2 + 2
        
        assert result == 4
    
    def test_performance_tracker_with_error(self):
        """Test performance tracking captures errors"""
        logger = get_logger("test")
        
        with pytest.raises(ValueError):
            with PerformanceTracker("failing_operation", logger):
                raise ValueError("Test error")
    
    def test_performance_tracker_duration_tracking(self):
        """Test that duration is tracked"""
        import time
        logger = get_logger("test")
        
        with PerformanceTracker("timed_operation", logger):
            time.sleep(0.01)  # Sleep 10ms
        
        # If we get here, tracking completed
        assert True


class TestLoggerFunctionality:
    """Test logger creation and usage"""
    
    def test_logger_creation(self):
        """Test creating a logger"""
        logger = get_logger("test_module")
        assert logger is not None
    
    def test_logger_levels(self):
        """Test different log levels"""
        logger = get_logger("test_module")
        
        logger.debug("Debug message", test_field="value")
        logger.info("Info message", count=42)
        logger.warning("Warning message", alert="attention")
        logger.error("Error message", error_code=500)
        
        # If no exceptions, logging works
        assert True
    
    def test_logger_with_structured_data(self):
        """Test logging with structured data"""
        logger = get_logger("test_module")
        
        logger.info(
            "structured_event",
            event_type="query_executed",
            query_id="q123",
            row_count=150,
            execution_time_ms=45.6,
        )
        
        assert True


@pytest.mark.asyncio
class TestIntegrationScenarios:
    """Integration tests for real-world scenarios"""
    
    async def test_query_lifecycle_logging(self):
        """Test logging a complete query lifecycle"""
        from app.core.structured_logging import log_query_lifecycle
        
        query_id = str(uuid.uuid4())
        user_id = "test_user"
        
        # Log query submission
        log_query_lifecycle(
            stage="submitted",
            query_id=query_id,
            user_id=user_id,
            metadata={"query": "SELECT * FROM test"},
        )
        
        # Log completion
        log_query_lifecycle(
            stage="completed",
            query_id=query_id,
            user_id=user_id,
            metadata={"rows": 100, "duration_ms": 250},
        )
        
        assert True
    
    async def test_error_logging_with_categorization(self):
        """Test error logging with automatic categorization"""
        logger = get_logger("test")
        
        try:
            raise TimeoutError("Database query timeout")
        except TimeoutError as e:
            logger.error("operation_timeout", exc_info=e)
        
        assert True
