"""
Persistent Memory Service
Stores and retrieves conversations, user preferences, learned mappings
"""

import logging
import json
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class PersistentMemoryService:
    """Service for persistent memory operations across sessions"""
    
    @staticmethod
    async def store_conversation(
        user_id: str,
        session_id: str,
        user_query: str,
        intent: str,
        sql_query: str,
        execution_status: str,
        result_summary: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Store conversation in Redis (no Oracle persistence)
        
        Returns:
            conversation_id
        """
        conversation_id = str(uuid.uuid4())
        
        payload = {
            "conversation_id": conversation_id,
            "user_id": user_id,
            "session_id": session_id,
            "user_query": user_query,
            "intent": intent,
            "sql_query": sql_query,
            "execution_status": execution_status,
            "result_summary": result_summary,
            "error_message": error_message,
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        try:
            # Store conversation object
            await redis_client.set(f"conv:{conversation_id}", payload, ttl=7*24*3600)
            
            # Index by user and session
            await redis_client._client.lpush(f"user:{user_id}:conversations", conversation_id)
            await redis_client._client.ltrim(f"user:{user_id}:conversations", 0, 499)
            await redis_client._client.lpush(f"session:{session_id}:conversations", conversation_id)
            await redis_client._client.ltrim(f"session:{session_id}:conversations", 0, 499)
            
            logger.info(f"Stored conversation in Redis: {conversation_id}")
            return conversation_id
        except Exception as e:
            logger.error(f"Failed to store conversation in Redis: {e}")
            return conversation_id
    
    @staticmethod
    async def get_user_conversation_history(
        user_id: str,
        limit: int = 50,
        status_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve user's conversation history from Redis
        """
        try:
            ids = await redis_client._client.lrange(f"user:{user_id}:conversations", 0, max(0, limit-1))
            conversations: List[Dict[str, Any]] = []
            for cid in ids:
                conv = await redis_client.get(f"conv:{cid}")
                if not conv:
                    continue
                if status_filter and conv.get("execution_status") != status_filter:
                    continue
                conversations.append(conv)
            return conversations
        except Exception as e:
            logger.error(f"Failed to retrieve conversation history from Redis: {e}")
            return []
    
    @staticmethod
    async def store_learned_mapping(
        concept: str,
        table_name: str,
        column_name: str,
        mapping_type: str = "semantic",
        confidence: float = 80.0,
        created_by: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Store learned concept-to-column mapping in Redis
        """
        mapping_id = str(uuid.uuid4())
        item = {
            "mapping_id": mapping_id,
            "concept": concept,
            "table_name": table_name,
            "column_name": column_name,
            "mapping_type": mapping_type,
            "confidence": confidence,
            "usage_count": 1,
            "success_rate": 100.0,
            "created_by": created_by,
            "last_used_at": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata or {},
        }
        try:
            await redis_client.set(f"mapping:{mapping_id}", item, ttl=7*24*3600)
            await redis_client._client.sadd(f"mappings:concept:{concept.upper()}", mapping_id)
            await redis_client._client.sadd(f"mappings:table:{table_name.upper()}", mapping_id)
            await redis_client._client.lpush("mappings:index", mapping_id)
            await redis_client._client.ltrim("mappings:index", 0, 999)
            logger.info(f"Stored learned mapping in Redis: {concept} -> {table_name}.{column_name}")
            return mapping_id
        except Exception as e:
            logger.error(f"Failed to store learned mapping in Redis: {e}")
            return mapping_id
    
    @staticmethod
    async def get_learned_mappings(
        concept: Optional[str] = None,
        table_name: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve learned mappings from Redis
        """
        try:
            ids: List[str] = []
            if concept:
                ids = list(await redis_client._client.smembers(f"mappings:concept:{concept.upper()}"))
            elif table_name:
                ids = list(await redis_client._client.smembers(f"mappings:table:{table_name.upper()}"))
            else:
                ids = await redis_client._client.lrange("mappings:index", 0, max(0, limit-1))
            
            mappings: List[Dict[str, Any]] = []
            for mid in ids[:limit]:
                m = await redis_client.get(f"mapping:{mid}")
                if m:
                    mappings.append(m)
            
            # Sort by confidence desc then usage_count desc
            mappings.sort(key=lambda x: (x.get("confidence", 0), x.get("usage_count", 0)), reverse=True)
            return mappings[:limit]
        except Exception as e:
            logger.error(f"Failed to retrieve learned mappings from Redis: {e}")
            return []
    
    @staticmethod
    async def update_mapping_usage(mapping_id: str, success: bool = True):
        """
        Increment usage count and update success rate for a mapping (Redis)
        """
        try:
            key = f"mapping:{mapping_id}"
            m = await redis_client.get(key)
            if not m:
                return
            m["usage_count"] = int(m.get("usage_count", 0)) + 1
            sr = float(m.get("success_rate", 100.0))
            uc = int(m["usage_count"])
            if success:
                # simple rolling success rate towards 100
                sr = (sr * (uc - 1) + 100.0) / uc
            else:
                sr = (sr * (uc - 1)) / uc
            m["success_rate"] = sr
            m["last_used_at"] = datetime.now(timezone.utc).isoformat()
            await redis_client.set(key, m, ttl=7*24*3600)
            logger.debug(f"Updated mapping usage in Redis: {mapping_id}")
        except Exception as e:
            logger.error(f"Failed to update mapping usage in Redis: {e}")
