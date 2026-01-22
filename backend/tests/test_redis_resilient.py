"""
Tests for Redis Resilient Wrapper

Tests circuit breaker, fallback cache, and graceful degradation
"""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch

from app.core.redis_resilient import (
    CircuitBreaker,
    CircuitState,
    InMemoryFallback,
    ResilientRedisWrapper
)
from app.core.exceptions import ExternalServiceException
from redis.exceptions import RedisError, ConnectionError


class TestCircuitBreaker:
    """Test circuit breaker functionality"""
    
    def test_initial_state(self):
        """Circuit breaker starts in CLOSED state"""
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.can_attempt() is True
    
    def test_open_on_failures(self):
        """Circuit opens after threshold failures"""
        cb = CircuitBreaker(failure_threshold=3)
        
        # Record failures
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.can_attempt() is False
    
    def test_half_open_after_timeout(self):
        """Circuit transitions to HALF_OPEN after recovery timeout"""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        
        # Open circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        import time
        time.sleep(0.15)
        
        # Should allow test request
        assert cb.can_attempt() is True
        assert cb.state == CircuitState.HALF_OPEN
    
    def test_close_on_success(self):
        """Circuit closes after successful recovery"""
        cb = CircuitBreaker(failure_threshold=2, success_threshold=2)
        
        # Open circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        
        # Force half-open
        cb._half_open_circuit()
        
        # Record successes
        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
    
    def test_reopen_on_half_open_failure(self):
        """Circuit reopens if failure occurs in HALF_OPEN state"""
        cb = CircuitBreaker(failure_threshold=2)
        
        # Open circuit
        cb.record_failure()
        cb.record_failure()
        
        # Force half-open
        cb._half_open_circuit()
        
        # Failure in half-open should reopen
        cb.record_failure()
        assert cb.state == CircuitState.OPEN


class TestInMemoryFallback:
    """Test in-memory fallback cache"""
    
    def test_set_and_get(self):
        """Basic set and get operations"""
        cache = InMemoryFallback()
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
    
    def test_ttl_expiration(self):
        """Values expire after TTL"""
        cache = InMemoryFallback()
        cache.set("key1", "value1", ttl=1)
        
        # Should exist immediately
        assert cache.get("key1") == "value1"
        
        # Wait for expiration
        import time
        time.sleep(1.1)
        
        # Should be expired
        assert cache.get("key1") is None
    
    def test_delete(self):
        """Delete removes key"""
        cache = InMemoryFallback()
        cache.set("key1", "value1")
        cache.delete("key1")
        assert cache.get("key1") is None
    
    def test_exists(self):
        """Exists checks for key presence"""
        cache = InMemoryFallback()
        cache.set("key1", "value1")
        assert cache.exists("key1") is True
        assert cache.exists("key2") is False
    
    def test_lru_eviction(self):
        """LRU eviction when cache is full"""
        cache = InMemoryFallback(max_size=3)
        
        # Fill cache
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        
        # Access key1 to make it recently used
        cache.get("key1")
        
        # Add new key, should evict key2 (least recently used)
        cache.set("key4", "value4")
        
        assert cache.exists("key1") is True
        assert cache.exists("key2") is False
        assert cache.exists("key3") is True
        assert cache.exists("key4") is True
    
    def test_clear(self):
        """Clear removes all keys"""
        cache = InMemoryFallback()
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()
        assert cache.get("key1") is None
        assert cache.get("key2") is None


class TestResilientRedisWrapper:
    """Test resilient Redis wrapper"""
    
    @pytest.mark.asyncio
    async def test_successful_operation(self):
        """Successful Redis operation"""
        mock_redis = Mock()
        wrapper = ResilientRedisWrapper(mock_redis)
        
        async def operation():
            return "success"
        
        result = await wrapper.execute_with_fallback(
            operation,
            operation_name="test_op"
        )
        
        assert result == "success"
        assert wrapper.circuit_breaker.state == CircuitState.CLOSED
    
    @pytest.mark.asyncio
    async def test_fallback_on_failure(self):
        """Fallback is used when Redis fails"""
        mock_redis = Mock()
        wrapper = ResilientRedisWrapper(mock_redis, enable_fallback=True)
        
        async def failing_operation():
            raise RedisError("Connection failed")
        
        async def fallback_operation():
            return "fallback_value"
        
        result = await wrapper.execute_with_fallback(
            failing_operation,
            fallback_operation,
            "test_op"
        )
        
        assert result == "fallback_value"
        assert wrapper.operation_stats["test_op"]["fallback"] == 1
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_integration(self):
        """Circuit breaker prevents attempts when open"""
        mock_redis = Mock()
        wrapper = ResilientRedisWrapper(mock_redis, enable_fallback=True)
        
        # Open circuit by recording failures
        for _ in range(5):
            wrapper.circuit_breaker.record_failure()
        
        assert wrapper.circuit_breaker.state == CircuitState.OPEN
        
        async def operation():
            return "should_not_execute"
        
        async def fallback_operation():
            return "fallback_used"
        
        result = await wrapper.execute_with_fallback(
            operation,
            fallback_operation,
            "test_op"
        )
        
        # Should use fallback without attempting Redis
        assert result == "fallback_used"
    
    @pytest.mark.asyncio
    async def test_no_fallback_returns_none(self):
        """Returns None when no fallback available"""
        mock_redis = Mock()
        wrapper = ResilientRedisWrapper(mock_redis, enable_fallback=False)
        
        async def failing_operation():
            raise RedisError("Connection failed")
        
        result = await wrapper.execute_with_fallback(
            failing_operation,
            None,
            "test_op"
        )
        
        assert result is None
    
    def test_health_status(self):
        """Health status includes circuit breaker info"""
        mock_redis = Mock()
        wrapper = ResilientRedisWrapper(mock_redis, enable_fallback=True)
        
        status = wrapper.get_health_status()
        
        assert "available" in status
        assert "circuit_breaker" in status
        assert "fallback_enabled" in status
        assert "operation_stats" in status
    
    def test_reset_circuit(self):
        """Manual circuit reset"""
        mock_redis = Mock()
        wrapper = ResilientRedisWrapper(mock_redis)
        
        # Open circuit
        for _ in range(5):
            wrapper.circuit_breaker.record_failure()
        
        assert wrapper.circuit_breaker.state == CircuitState.OPEN
        
        # Reset
        wrapper.reset_circuit()
        
        assert wrapper.circuit_breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_integration_with_redis_client():
    """Integration test with actual RedisClient"""
    from app.core.redis_client import RedisClient
    
    client = RedisClient()
    
    # Test without connection (should use fallback)
    result = await client.set("test_key", "test_value")
    
    # Should not crash, returns False or uses fallback
    assert result is not None
    
    # Test health status
    status = client.get_health_status()
    assert "connected" in status or "available" in status


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
