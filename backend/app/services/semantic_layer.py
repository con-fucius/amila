"""
Semantic Layer Service - Cross-database query abstraction
"""

import logging
from typing import Dict, Any, Optional, List

from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


class SemanticLayerService:
    """Service for semantic layer and cross-database query abstraction"""
    
    @staticmethod
    async def resolve_concept_to_table(
        concept: str,
        db_type: str
    ) -> Optional[str]:
        """
        Resolve a business concept to a physical table name.
        
        Args:
            concept: Business concept (e.g., "customers", "orders", "revenue")
            db_type: Database type (oracle, doris, postgres)
            
        Returns:
            Physical table name or None if not found
        """
        logger.info(f"Resolving concept '{concept}' for {db_type}")
        
        try:
            # Get mapping from Redis
            cache_key = f"semantic:mapping:{concept.lower()}"
            mapping = await redis_client.get(cache_key)
            
            if mapping and isinstance(mapping, dict):
                table_name = mapping.get(db_type.lower())
                if table_name:
                    logger.info(f"Resolved '{concept}' → '{table_name}' for {db_type}")
                    return table_name
            
            # Fallback: Check config
            from app.core.config import settings
            semantic_mappings = getattr(settings, "SEMANTIC_MAPPINGS", {})
            
            if concept.lower() in semantic_mappings:
                table_name = semantic_mappings[concept.lower()].get(db_type.lower())
                if table_name:
                    # Cache it
                    await redis_client.set(
                        cache_key,
                        semantic_mappings[concept.lower()],
                        ttl=86400  # 24 hours
                    )
                    logger.info(f"Resolved '{concept}' → '{table_name}' from config")
                    return table_name
            
            logger.warning(f"No mapping found for concept '{concept}' in {db_type}")
            return None
            
        except Exception as e:
            logger.error(f"Concept resolution failed: {e}")
            return None
    
    @staticmethod
    async def translate_query(
        user_query: str,
        target_db: str
    ) -> str:
        """
        Translate a query with semantic references to actual table names.
        
        Args:
            user_query: User query with semantic references
            target_db: Target database type
            
        Returns:
            Translated query with physical table names
        """
        logger.info(f"Translating query for {target_db}")
        
        try:
            translated_query = user_query
            
            # Get all mappings
            mappings = await SemanticLayerService.get_all_mappings()
            
            # Replace semantic references with actual table names
            for concept, db_mappings in mappings.items():
                table_name = db_mappings.get(target_db.lower())
                if table_name:
                    # Replace concept with table name (case-insensitive)
                    import re
                    pattern = re.compile(re.escape(concept), re.IGNORECASE)
                    translated_query = pattern.sub(table_name, translated_query)
            
            if translated_query != user_query:
                logger.info(f"Query translated: {user_query[:50]}... → {translated_query[:50]}...")
            
            return translated_query
            
        except Exception as e:
            logger.error(f"Query translation failed: {e}")
            return user_query  # Return original on failure
    
    @staticmethod
    async def build_semantic_mapping(
        mappings: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Build and cache semantic mappings.
        
        Args:
            mappings: Dict of concept -> {database_type -> table_name}
            Example: {"customers": {"oracle": "CUSTOMER_DIM", "doris": "dim_customer"}}
            
        Returns:
            Status dict
        """
        logger.info(f"Building semantic mappings for {len(mappings)} concepts")
        
        try:
            count = 0
            
            for concept, db_mappings in mappings.items():
                cache_key = f"semantic:mapping:{concept.lower()}"
                await redis_client.set(
                    cache_key,
                    db_mappings,
                    ttl=86400  # 24 hours
                )
                count += 1
            
            logger.info(f"Built {count} semantic mappings")
            
            return {
                "status": "success",
                "mappings_created": count
            }
            
        except Exception as e:
            logger.error(f"Mapping creation failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    async def get_all_mappings() -> Dict[str, Dict[str, str]]:
        """
        Get all semantic mappings.
        
        Returns:
            Dict of all mappings
        """
        try:
            # Try config first
            from app.core.config import settings
            mappings = getattr(settings, "SEMANTIC_MAPPINGS", {})
            
            # Merge with Redis cache
            # (In production, would scan Redis keys with semantic:mapping:* pattern)
            
            return mappings
            
        except Exception as e:
            logger.warning(f"Failed to get mappings: {e}")
            return {}
    
    @staticmethod
    async def add_mapping(
        concept: str,
        db_mappings: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Add a single semantic mapping.
        
        Args:
            concept: Business concept
            db_mappings: Database-specific table names
            
        Returns:
            Status dict
        """
        try:
            cache_key = f"semantic:mapping:{concept.lower()}"
            await redis_client.set(
                cache_key,
                db_mappings,
                ttl=86400
            )
            
            logger.info(f"Added mapping for '{concept}'")
            
            return {
                "status": "success",
                "concept": concept,
                "mappings": db_mappings
            }
            
        except Exception as e:
            logger.error(f"Failed to add mapping: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    async def delete_mapping(concept: str) -> Dict[str, Any]:
        """
        Delete a semantic mapping.
        
        Args:
            concept: Business concept to delete
            
        Returns:
            Status dict
        """
        try:
            cache_key = f"semantic:mapping:{concept.lower()}"
            await redis_client.delete(cache_key)
            
            logger.info(f"Deleted mapping for '{concept}'")
            
            return {
                "status": "success",
                "concept": concept
            }
            
        except Exception as e:
            logger.error(f"Failed to delete mapping: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
