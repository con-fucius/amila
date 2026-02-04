"""
Temporal Knowledge Graph Service
Enhanced graph capabilities with temporal reasoning
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, date

logger = logging.getLogger(__name__)


class TemporalGraphService:
    """
    Temporal knowledge graph service for tracking entity evolution over time.
    
    Features:
    - Store entities with timestamps
    - Track relationship changes over time
    - Query entities for specific time ranges
    - Analyze entity evolution patterns
    """
    
    def __init__(self):
        """Initialize temporal graph service"""
        self.storage = {}  # In production, use actual graph database
    
    async def add_entity(
        self,
        entity_id: str,
        entity_type: str,
        properties: Dict[str, Any],
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Add or update entity with timestamp.
        
        Args:
            entity_id: Unique entity identifier
            entity_type: Type of entity (e.g., 'customer', 'product')
            properties: Entity properties
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            Created/updated entity with timestamp
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        entity_data = {
            "id": entity_id,
            "type": entity_type,
            "properties": properties,
            "timestamp": timestamp.isoformat(),
            "created_at": timestamp.isoformat()
        }
        
        # Store entity history
        if entity_id not in self.storage:
            self.storage[entity_id] = []
        
        self.storage[entity_id].append(entity_data)
        
        logger.info(f"Added entity {entity_id} at {timestamp}")
        
        return {
            "status": "success",
            "entity": entity_data
        }
    
    async def query_temporal(
        self,
        entity_id: Optional[str] = None,
        entity_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Query entities for a specific time range.
        
        Args:
            entity_id: Optional entity ID filter
            entity_type: Optional entity type filter
            start_date: Start of time range
            end_date: End of time range
            
        Returns:
            List of entities matching criteria
        """
        results = []
        
        for eid, history in self.storage.items():
            # Filter by entity_id
            if entity_id and eid != entity_id:
                continue
            
            for snapshot in history:
                # Filter by entity type
                if entity_type and snapshot["type"] != entity_type:
                    continue
                
                # Filter by date range
                snapshot_time = datetime.fromisoformat(snapshot["timestamp"])
                
                if start_date and snapshot_time < start_date:
                    continue
                if end_date and snapshot_time > end_date:
                    continue
                
                results.append(snapshot)
        
        return {
            "status": "success",
            "entities": results,
            "count": len(results)
        }
    
    async def track_entity_evolution(
        self,
        entity_id: str
    ) -> Dict[str, Any]:
        """
        Track how an entity has evolved over time.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Timeline of entity changes
        """
        if entity_id not in self.storage:
            return {
                "status": "error",
                "error": f"Entity {entity_id} not found"
            }
        
        history = self.storage[entity_id]
        
        # Build timeline
        timeline = []
        for i, snapshot in enumerate(history):
            change = {
                "timestamp": snapshot["timestamp"],
                "snapshot_index": i,
                "properties": snapshot["properties"]
            }
            
            # Calculate changes from previous snapshot
            if i > 0:
                prev_props = history[i-1]["properties"]
                curr_props = snapshot["properties"]
                
                changes = {}
                for key in set(prev_props.keys()) | set(curr_props.keys()):
                    prev_val = prev_props.get(key)
                    curr_val = curr_props.get(key)
                    
                    if prev_val != curr_val:
                        changes[key] = {
                            "from": prev_val,
                            "to": curr_val
                        }
                
                change["changes"] = changes
            
            timeline.append(change)
        
        return {
            "status": "success",
            "entity_id": entity_id,
            "timeline": timeline,
            "total_snapshots": len(timeline)
        }
    
    async def add_relationship(
        self,
        from_entity: str,
        to_entity: str,
        relationship_type: str,
        properties: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Add temporal relationship between entities.
        
        Args:
            from_entity: Source entity ID
            to_entity: Target entity ID
            relationship_type: Type of relationship
            properties: Optional relationship properties
            timestamp: Event timestamp
            
        Returns:
            Created relationship
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        relationship_key = f"rel:{from_entity}:{to_entity}:{relationship_type}"
        
        relationship_data = {
            "from": from_entity,
            "to": to_entity,
            "type": relationship_type,
            "properties": properties or {},
            "timestamp": timestamp.isoformat()
        }
        
        if relationship_key not in self.storage:
            self.storage[relationship_key] = []
        
        self.storage[relationship_key].append(relationship_data)
        
        logger.info(f"Added relationship {relationship_type} from {from_entity} to {to_entity}")
        
        return {
            "status": "success",
            "relationship": relationship_data
        }
    
    async def query_relationships(
        self,
        entity_id: str,
        relationship_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Query relationships for an entity in a time range.
        
        Args:
            entity_id: Entity ID
            relationship_type: Optional relationship type filter
            start_date: Start of time range
            end_date: End of time range
            
        Returns:
            List of relationships
        """
        results = []
        
        for key, rel_history in self.storage.items():
            if not key.startswith("rel:"):
                continue
            
            for rel in rel_history:
                # Check if entity is involved
                if rel["from"] != entity_id and rel["to"] != entity_id:
                    continue
                
                # Filter by type
                if relationship_type and rel["type"] != relationship_type:
                    continue
                
                # Filter by date range
                rel_time = datetime.fromisoformat(rel["timestamp"])
                
                if start_date and rel_time < start_date:
                    continue
                if end_date and rel_time > end_date:
                    continue
                
                results.append(rel)
        
        return {
            "status": "success",
            "relationships": results,
            "count": len(results)
        }
    
    async def get_entity_state_at(
        self,
        entity_id: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Get entity state at a specific point in time.
        
        Args:
            entity_id: Entity ID
            timestamp: Point in time
            
        Returns:
            Entity state at that time
        """
        if entity_id not in self.storage:
            return {
                "status": "error",
                "error": f"Entity {entity_id} not found"
            }
        
        history = self.storage[entity_id]
        
        # Find latest snapshot before timestamp
        relevant_snapshot = None
        for snapshot in history:
            snapshot_time = datetime.fromisoformat(snapshot["timestamp"])
            
            if snapshot_time <= timestamp:
                if relevant_snapshot is None or snapshot_time > datetime.fromisoformat(relevant_snapshot["timestamp"]):
                    relevant_snapshot = snapshot
        
        if relevant_snapshot is None:
            return {
                "status": "error",
                "error": f"No state found for entity {entity_id} at {timestamp}"
            }
        
        return {
            "status": "success",
            "entity": relevant_snapshot,
            "query_timestamp": timestamp.isoformat()
        }
    
    async def setup_auto_refresh(
        self,
        query_config: Dict[str, Any],
        refresh_interval_seconds: int = 60
    ) -> str:
        """
        Setup auto-refresh for a temporal query.
        
        Args:
            query_config: Query configuration (entity_type, start_date, etc.)
            refresh_interval_seconds: Refresh interval
            
        Returns:
            Subscription ID
        """
        import uuid
        
        subscription_id = f"refresh_{uuid.uuid4().hex[:8]}"
        
        refresh_config = {
            "subscription_id": subscription_id,
            "query_config": query_config,
            "refresh_interval": refresh_interval_seconds,
            "created_at": datetime.utcnow().isoformat(),
            "last_refresh": None,
            "active": True
        }
        
        # Store in storage (in production, use Redis or similar)
        refresh_key = f"refresh:{subscription_id}"
        self.storage[refresh_key] = refresh_config
        
        logger.info(f"Setup auto-refresh subscription: {subscription_id}")
        
        return subscription_id
    
    async def execute_auto_refresh(self, subscription_id: str) -> Dict[str, Any]:
        """
        Execute auto-refresh for a subscription.
        
        Args:
            subscription_id: Subscription ID
            
        Returns:
            Refreshed query results
        """
        refresh_key = f"refresh:{subscription_id}"
        
        if refresh_key not in self.storage:
            return {
                "status": "error",
                "error": "Subscription not found"
            }
        
        config = self.storage[refresh_key]
        
        if not config.get("active"):
            return {
                "status": "error",
                "error": "Subscription is inactive"
            }
        
        # Execute the query
        query_config = config["query_config"]
        
        results = await self.query_temporal(
            entity_id=query_config.get("entity_id"),
            entity_type=query_config.get("entity_type"),
            start_date=query_config.get("start_date"),
            end_date=query_config.get("end_date")
        )
        
        # Update last refresh time
        config["last_refresh"] = datetime.utcnow().isoformat()
        self.storage[refresh_key] = config
        
        logger.info(f"Executed auto-refresh for subscription: {subscription_id}")
        
        return {
            "status": "success",
            "subscription_id": subscription_id,
            "results": results,
            "refreshed_at": config["last_refresh"]
        }
    
    async def cancel_auto_refresh(self, subscription_id: str) -> Dict[str, Any]:
        """
        Cancel an auto-refresh subscription.
        
        Args:
            subscription_id: Subscription ID
            
        Returns:
            Cancellation status
        """
        refresh_key = f"refresh:{subscription_id}"
        
        if refresh_key in self.storage:
            self.storage[refresh_key]["active"] = False
            logger.info(f"Cancelled auto-refresh: {subscription_id}")
            
            return {
                "status": "success",
                "message": f"Subscription {subscription_id} cancelled"
            }
        
        return {
            "status": "error",
            "error": "Subscription not found"
        }
    
    async def list_active_subscriptions(self) -> List[Dict[str, Any]]:
        """
        List all active auto-refresh subscriptions.
        
        Returns:
            List of active subscriptions
        """
        subscriptions = []
        
        for key, value in self.storage.items():
            if key.startswith("refresh:") and isinstance(value, dict) and value.get("active"):
                subscriptions.append({
                    "subscription_id": value["subscription_id"],
                    "refresh_interval": value["refresh_interval"],
                    "created_at": value["created_at"],
                    "last_refresh": value.get("last_refresh")
                })
        
        return subscriptions


# Singleton instance
temporal_graph_service = TemporalGraphService()
