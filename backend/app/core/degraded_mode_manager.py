"""
Degraded Mode Manager

Coordinates graceful degradation across all system components when Redis is unavailable.
Provides centralized state management and user-facing indicators for degraded functionality.

Features:
- Centralized degradation state tracking
- Component-specific fallback strategies
- User-facing degradation indicators
- Automatic recovery detection
- Health status aggregation
"""

import logging
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field, asdict
import asyncio

logger = logging.getLogger(__name__)


class DegradationLevel(Enum):
    """System degradation levels"""
    NORMAL = "normal"  # All systems operational
    PARTIAL = "partial"  # Some features degraded
    SEVERE = "severe"  # Major features unavailable
    CRITICAL = "critical"  # Core functionality impaired


class ComponentStatus(Enum):
    """Component operational status"""
    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass
class ComponentState:
    """State of a system component"""
    name: str
    status: ComponentStatus
    degradation_reason: Optional[str] = None
    fallback_active: bool = False
    fallback_type: Optional[str] = None
    last_check: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    impact_description: Optional[str] = None
    recovery_actions: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['status'] = self.status.value
        data['last_check'] = self.last_check.isoformat()
        return data


class DegradedModeManager:
    """
    Manages system-wide degraded mode state and coordinates fallback strategies.
    
    Responsibilities:
    - Track component degradation states
    - Calculate overall system degradation level
    - Provide user-facing status messages
    - Coordinate recovery attempts
    - Emit degradation events for monitoring
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DegradedModeManager, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance
    
    def __init__(self):
        if self.initialized:
            return
        
        self.components: Dict[str, ComponentState] = {}
        self.degradation_level = DegradationLevel.NORMAL
        self.degradation_start_time: Optional[datetime] = None
        self.recovery_attempts = 0
        self.max_recovery_attempts = 3
        self.recovery_in_progress = False
        self.initialized = True
        
        logger.info("Degraded Mode Manager initialized")
    
    def register_component(
        self,
        name: str,
        status: ComponentStatus = ComponentStatus.OPERATIONAL,
        impact_description: Optional[str] = None
    ):
        """
        Register a system component for degradation tracking.
        
        Args:
            name: Component name (e.g., "redis", "celery", "langgraph_checkpointer")
            status: Initial status
            impact_description: User-facing description of impact when degraded
        """
        self.components[name] = ComponentState(
            name=name,
            status=status,
            impact_description=impact_description
        )
        logger.debug(f"Registered component: {name}")
    
    def update_component_status(
        self,
        name: str,
        status: ComponentStatus,
        degradation_reason: Optional[str] = None,
        fallback_active: bool = False,
        fallback_type: Optional[str] = None,
        recovery_actions: Optional[List[str]] = None
    ):
        """
        Update component status and recalculate system degradation level.
        
        Args:
            name: Component name
            status: New status
            degradation_reason: Reason for degradation
            fallback_active: Whether fallback is active
            fallback_type: Type of fallback (e.g., "in_memory", "sqlite", "disabled")
            recovery_actions: Suggested recovery actions
        """
        if name not in self.components:
            self.register_component(name)
        
        component = self.components[name]
        old_status = component.status
        
        component.status = status
        component.degradation_reason = degradation_reason
        component.fallback_active = fallback_active
        component.fallback_type = fallback_type
        component.last_check = datetime.now(timezone.utc)
        
        if recovery_actions:
            component.recovery_actions = recovery_actions
        
        # Log status changes
        if old_status != status:
            if status == ComponentStatus.OPERATIONAL:
                logger.info(f"Component {name} recovered: {old_status.value} -> {status.value}")
            elif status == ComponentStatus.DEGRADED:
                logger.warning(
                    f"Component {name} degraded: {degradation_reason}. "
                    f"Fallback: {fallback_type if fallback_active else 'none'}"
                )
            else:
                logger.error(f"Component {name} unavailable: {degradation_reason}")
        
        # Recalculate system degradation level
        self._recalculate_degradation_level()
    
    def _recalculate_degradation_level(self):
        """Recalculate overall system degradation level based on component states"""
        if not self.components:
            self.degradation_level = DegradationLevel.NORMAL
            return
        
        unavailable_count = sum(
            1 for c in self.components.values()
            if c.status == ComponentStatus.UNAVAILABLE
        )
        degraded_count = sum(
            1 for c in self.components.values()
            if c.status == ComponentStatus.DEGRADED
        )
        total_count = len(self.components)
        
        old_level = self.degradation_level
        
        # Determine degradation level
        if unavailable_count == 0 and degraded_count == 0:
            self.degradation_level = DegradationLevel.NORMAL
            if old_level != DegradationLevel.NORMAL:
                logger.info("System recovered to normal operation")
                self.degradation_start_time = None
        elif unavailable_count >= total_count * 0.5:
            self.degradation_level = DegradationLevel.CRITICAL
        elif unavailable_count > 0 or degraded_count >= total_count * 0.5:
            self.degradation_level = DegradationLevel.SEVERE
        else:
            self.degradation_level = DegradationLevel.PARTIAL
        
        # Track degradation start time
        if self.degradation_level != DegradationLevel.NORMAL and old_level == DegradationLevel.NORMAL:
            self.degradation_start_time = datetime.now(timezone.utc)
            logger.warning(f"System entered degraded mode: {self.degradation_level.value}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get comprehensive system status including all components.
        
        Returns:
            System status dictionary with degradation details
        """
        degraded_components = [
            c.to_dict() for c in self.components.values()
            if c.status != ComponentStatus.OPERATIONAL
        ]
        
        operational_components = [
            c.name for c in self.components.values()
            if c.status == ComponentStatus.OPERATIONAL
        ]
        
        status = {
            "degradation_level": self.degradation_level.value,
            "is_degraded": self.degradation_level != DegradationLevel.NORMAL,
            "degradation_start_time": (
                self.degradation_start_time.isoformat()
                if self.degradation_start_time else None
            ),
            "degraded_components": degraded_components,
            "operational_components": operational_components,
            "total_components": len(self.components),
            "user_message": self._get_user_message(),
            "affected_features": self._get_affected_features(),
            "recovery_suggestions": self._get_recovery_suggestions(),
        }
        
        return status
    
    def _get_user_message(self) -> str:
        """Generate user-facing status message"""
        if self.degradation_level == DegradationLevel.NORMAL:
            return "All systems operational"
        
        degraded_names = [
            c.name for c in self.components.values()
            if c.status != ComponentStatus.OPERATIONAL
        ]
        
        if self.degradation_level == DegradationLevel.CRITICAL:
            return (
                f"System experiencing critical issues. "
                f"Core functionality may be unavailable. "
                f"Affected: {', '.join(degraded_names)}"
            )
        elif self.degradation_level == DegradationLevel.SEVERE:
            return (
                f"System operating in degraded mode. "
                f"Some features unavailable. "
                f"Affected: {', '.join(degraded_names)}"
            )
        else:
            return (
                f"System partially degraded. "
                f"Most features available with reduced performance. "
                f"Affected: {', '.join(degraded_names)}"
            )
    
    def _get_affected_features(self) -> List[str]:
        """Get list of affected features based on degraded components"""
        affected = set()
        
        for component in self.components.values():
            if component.status == ComponentStatus.OPERATIONAL:
                continue
            
            # Map components to user-facing features
            if component.name == "redis":
                affected.update([
                    "Session persistence",
                    "Query result caching",
                    "Schema metadata caching",
                    "Rate limiting",
                    "Audit logging"
                ])
            elif component.name == "celery":
                affected.update([
                    "Background report generation",
                    "Scheduled schema refresh",
                    "Async query processing"
                ])
            elif component.name == "langgraph_checkpointer":
                affected.update([
                    "Query state persistence",
                    "HITL approval resumption",
                    "Multi-step query recovery"
                ])
            elif component.name == "graphiti":
                affected.update([
                    "Context-aware query generation",
                    "Knowledge graph features"
                ])
            
            # Add component-specific impact if provided
            if component.impact_description:
                affected.add(component.impact_description)
        
        return sorted(list(affected))
    
    def _get_recovery_suggestions(self) -> List[str]:
        """Get recovery suggestions for degraded components"""
        suggestions = set()
        
        for component in self.components.values():
            if component.status != ComponentStatus.OPERATIONAL:
                suggestions.update(component.recovery_actions)
        
        return sorted(list(suggestions))
    
    def is_feature_available(self, feature: str) -> bool:
        """
        Check if a specific feature is available.
        
        Args:
            feature: Feature name (e.g., "caching", "sessions", "celery_tasks")
            
        Returns:
            True if feature is available (possibly degraded), False if unavailable
        """
        # Map features to components
        feature_component_map = {
            "caching": "redis",
            "sessions": "redis",
            "rate_limiting": "redis",
            "audit_logging": "redis",
            "celery_tasks": "celery",
            "query_checkpointing": "langgraph_checkpointer",
            "knowledge_graph": "graphiti",
        }
        
        component_name = feature_component_map.get(feature)
        if not component_name or component_name not in self.components:
            return True  # Unknown features assumed available
        
        component = self.components[component_name]
        return component.status != ComponentStatus.UNAVAILABLE
    
    def get_component_status(self, name: str) -> Optional[ComponentState]:
        """Get status of a specific component"""
        return self.components.get(name)
    
    async def attempt_recovery(self, component_name: Optional[str] = None):
        """
        Attempt to recover degraded components.
        
        Args:
            component_name: Specific component to recover, or None for all
        """
        if self.recovery_in_progress:
            logger.warning("Recovery already in progress")
            return
        
        self.recovery_in_progress = True
        self.recovery_attempts += 1
        
        try:
            if self.recovery_attempts > self.max_recovery_attempts:
                logger.warning(
                    f"Max recovery attempts ({self.max_recovery_attempts}) reached. "
                    "Manual intervention required."
                )
                return
            
            logger.info(f"Attempting recovery (attempt {self.recovery_attempts})")
            
            # Attempt recovery for specific component or all degraded components
            components_to_recover = (
                [component_name] if component_name
                else [c.name for c in self.components.values()
                      if c.status != ComponentStatus.OPERATIONAL]
            )
            
            for name in components_to_recover:
                await self._recover_component(name)
            
        finally:
            self.recovery_in_progress = False
    
    async def _recover_component(self, name: str):
        """Attempt to recover a specific component"""
        if name not in self.components:
            return
        
        logger.info(f"Attempting to recover component: {name}")
        
        try:
            if name == "redis":
                await self._recover_redis()
            elif name == "celery":
                await self._recover_celery()
            elif name == "langgraph_checkpointer":
                await self._recover_checkpointer()
            elif name == "graphiti":
                await self._recover_graphiti()
            else:
                logger.warning(f"No recovery procedure defined for component: {name}")
        
        except Exception as e:
            logger.error(f"Recovery failed for {name}: {e}")
    
    async def _recover_redis(self):
        """Attempt to reconnect to Redis"""
        try:
            from app.core.redis_client import redis_client
            
            if redis_client.is_connected():
                # Test connection
                await redis_client._client.ping()
                self.update_component_status(
                    "redis",
                    ComponentStatus.OPERATIONAL,
                    fallback_active=False
                )
                logger.info("Redis connection recovered")
            else:
                # Attempt reconnection
                await redis_client.connect(max_retries=2, retry_delay=1.0)
                self.update_component_status(
                    "redis",
                    ComponentStatus.OPERATIONAL,
                    fallback_active=False
                )
                logger.info("Redis reconnected successfully")
        
        except Exception as e:
            logger.warning(f"Redis recovery failed: {e}")
            self.update_component_status(
                "redis",
                ComponentStatus.DEGRADED,
                degradation_reason=str(e),
                fallback_active=True,
                fallback_type="in_memory"
            )
    
    async def _recover_celery(self):
        """Check Celery worker availability"""
        try:
            from app.core.celery_app import celery_app
            
            # Check if workers are available
            inspect = celery_app.control.inspect()
            stats = inspect.stats()
            
            if stats:
                self.update_component_status(
                    "celery",
                    ComponentStatus.OPERATIONAL
                )
                logger.info("Celery workers available")
            else:
                self.update_component_status(
                    "celery",
                    ComponentStatus.DEGRADED,
                    degradation_reason="No workers available",
                    fallback_active=True,
                    fallback_type="synchronous_execution"
                )
        
        except Exception as e:
            logger.warning(f"Celery recovery check failed: {e}")
    
    async def _recover_checkpointer(self):
        """Check LangGraph checkpointer availability"""
        try:
            from app.core.client_registry import registry
            
            checkpointer = registry.get_langgraph_checkpointer()
            if checkpointer:
                # Test checkpointer
                await checkpointer.alist({"configurable": {"thread_id": "recovery_test"}})
                self.update_component_status(
                    "langgraph_checkpointer",
                    ComponentStatus.OPERATIONAL
                )
                logger.info("LangGraph checkpointer recovered")
            else:
                self.update_component_status(
                    "langgraph_checkpointer",
                    ComponentStatus.UNAVAILABLE,
                    degradation_reason="Checkpointer not initialized"
                )
        
        except Exception as e:
            logger.warning(f"Checkpointer recovery failed: {e}")
    
    async def _recover_graphiti(self):
        """Check Graphiti client availability"""
        try:
            from app.core.client_registry import registry
            
            graphiti_client = registry.get_graphiti_client()
            if graphiti_client:
                self.update_component_status(
                    "graphiti",
                    ComponentStatus.OPERATIONAL
                )
                logger.info("Graphiti client available")
            else:
                self.update_component_status(
                    "graphiti",
                    ComponentStatus.UNAVAILABLE,
                    degradation_reason="Graphiti client not initialized"
                )
        
        except Exception as e:
            logger.warning(f"Graphiti recovery check failed: {e}")
    
    def reset_recovery_attempts(self):
        """Reset recovery attempt counter (called on successful recovery)"""
        self.recovery_attempts = 0
        logger.info("Recovery attempt counter reset")


# Global instance
degraded_mode_manager = DegradedModeManager()
