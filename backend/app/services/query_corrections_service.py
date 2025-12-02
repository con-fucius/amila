"""
Query Corrections Service
Captures user SQL edits and learns from corrections
"""

import logging
import json
import uuid
import difflib
from typing import Dict, Any, List, Optional

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class QueryCorrectionsService:
    """Service for learning from user SQL corrections"""
    
    @staticmethod
    async def store_correction(
        user_id: str,
        session_id: str,
        original_query: str,
        generated_sql: str,
        corrected_sql: str,
        correction_type: str = "user_edit",
        intent: Optional[str] = None,
        success_after_correction: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Store SQL correction for learning
        
        Args:
            user_id: User identifier
            session_id: Session identifier
            original_query: Original natural language query
            generated_sql: AI-generated SQL
            corrected_sql: User-corrected SQL
            correction_type: Type of correction (user_edit, error_fix, optimization, clarification)
            intent: Query intent
            success_after_correction: Whether corrected SQL succeeded
            metadata: Additional metadata
            
        Returns:
            correction_id
        """
        correction_id = str(uuid.uuid4())
        
        # Compute diff summary
        diff_summary = QueryCorrectionsService._compute_diff(generated_sql, corrected_sql)
        
        item = {
            "correction_id": correction_id,
            "user_id": user_id,
            "session_id": session_id,
            "original_query": original_query,
            "generated_sql": generated_sql,
            "corrected_sql": corrected_sql,
            "correction_type": correction_type,
            "diff_summary": diff_summary,
            "intent": intent,
            "success_after_correction": bool(success_after_correction),
            "applied_count": 0,
            "created_at": None,
            "metadata": metadata or {},
        }
        try:
            # Store correction object
            await redis_client.set(f"correction:{correction_id}", item, ttl=14*24*3600)
            # Indexes
            await redis_client._client.lpush("corrections:index", correction_id)
            await redis_client._client.ltrim("corrections:index", 0, 999)
            await redis_client._client.lpush(f"user:{user_id}:corrections", correction_id)
            await redis_client._client.ltrim(f"user:{user_id}:corrections", 0, 999)
            logger.info(f"Stored SQL correction in Redis: {correction_id}")
            return correction_id
        except Exception as e:
            logger.error(f"Failed to store correction in Redis: {e}")
            return correction_id
    
    @staticmethod
    def _compute_diff(original: str, corrected: str) -> Dict[str, Any]:
        """
        Compute diff between original and corrected SQL
        
        Returns:
            Dictionary with added, removed, changed lines
        """
        original_lines = original.strip().split('\n')
        corrected_lines = corrected.strip().split('\n')
        
        differ = difflib.Differ()
        diff = list(differ.compare(original_lines, corrected_lines))
        
        added = [line[2:] for line in diff if line.startswith('+ ')]
        removed = [line[2:] for line in diff if line.startswith('- ')]
        
        return {
            "added_lines": added,
            "removed_lines": removed,
            "total_changes": len(added) + len(removed),
            "similarity_ratio": difflib.SequenceMatcher(None, original, corrected).ratio()
        }
    
    @staticmethod
    async def get_relevant_corrections(
        original_query: Optional[str] = None,
        intent: Optional[str] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant corrections for similar queries
        
        Args:
            original_query: Natural language query to match
            intent: Intent to match
            limit: Max corrections to retrieve
            
        Returns:
            List of relevant correction examples
        """
        where_clauses = ["success_after_correction = 1"]
        
        if original_query:
            # Simple keyword matching - can be enhanced with embeddings
            keywords = original_query.upper().split()[:5]
            keyword_conditions = [f"UPPER(original_query) LIKE '%{kw}%'" for kw in keywords if len(kw) > 3]
            if keyword_conditions:
                where_clauses.append(f"({' OR '.join(keyword_conditions)})")
        
        if intent:
            where_clauses.append(f"UPPER(intent) LIKE '%{intent.upper()}%'")
        
        where_clause = " AND ".join(where_clauses)
        
        try:
            # Get most recent corrections and filter in-memory
            ids = await redis_client._client.lrange("corrections:index", 0, 199)
            results: List[Dict[str, Any]] = []
            
            kws = []
            if original_query:
                kws = [kw for kw in original_query.upper().split() if len(kw) > 3][:5]
            
            for cid in ids:
                item = await redis_client.get(f"correction:{cid}")
                if not item:
                    continue
                if intent and item.get("intent") and intent.upper() not in str(item.get("intent", "")).upper():
                    continue
                if kws and not any(kw in str(item.get("original_query", "")).upper() for kw in kws):
                    continue
                results.append({
                    "correction_id": item.get("correction_id"),
                    "original_query": item.get("original_query"),
                    "generated_sql": item.get("generated_sql"),
                    "corrected_sql": item.get("corrected_sql"),
                    "diff_summary": item.get("diff_summary", {}),
                    "applied_count": int(item.get("applied_count", 0)),
                    "created_at": item.get("created_at"),
                })
            
            # Sort and limit
            results.sort(key=lambda x: (x.get("applied_count", 0), ), reverse=True)
            return results[:limit]
        except Exception as e:
            logger.error(f"Failed to retrieve corrections from Redis: {e}")
            return []
    
    @staticmethod
    async def increment_applied_count(correction_id: str):
        """
        Increment the applied count when a correction pattern is used
        """
        try:
            key = f"correction:{correction_id}"
            item = await redis_client.get(key)
            if not item:
                return
            item["applied_count"] = int(item.get("applied_count", 0)) + 1
            await redis_client.set(key, item, ttl=14*24*3600)
            logger.debug(f"Incremented applied count for correction in Redis: {correction_id}")
        except Exception as e:
            logger.error(f"Failed to increment applied count in Redis: {e}")
    
    @staticmethod
    def format_corrections_for_prompt(corrections: List[Dict[str, Any]]) -> str:
        """
        Format corrections as examples for SQL generation prompt
        
        Returns:
            Formatted string to inject into prompt
        """
        if not corrections:
            return ""
        
        prompt_section = "\n\n" + "="*80 + "\n"
        prompt_section += " LEARNED CORRECTIONS (from user feedback)\n"
        prompt_section += "="*80 + "\n\n"
        prompt_section += "The following corrections were made by users on similar queries.\n"
        prompt_section += "Learn from these patterns to avoid similar mistakes:\n\n"
        
        for idx, correction in enumerate(corrections[:3], 1):  # Top 3
            prompt_section += f"Example {idx}:\n"
            prompt_section += f"User Query: {correction['original_query'][:100]}...\n"
            prompt_section += f"Initial SQL (INCORRECT):\n{correction['generated_sql'][:200]}...\n\n"
            prompt_section += f"Corrected SQL (CORRECT):\n{correction['corrected_sql'][:200]}...\n\n"
            
            diff = correction.get('diff_summary', {})
            if diff.get('added_lines'):
                prompt_section += f"Key Changes: Added {len(diff['added_lines'])} lines\n"
            if diff.get('removed_lines'):
                prompt_section += f"             Removed {len(diff['removed_lines'])} lines\n"
            
            prompt_section += f"Times Applied: {correction.get('applied_count', 0)}\n"
            prompt_section += "-" * 60 + "\n\n"
        
        prompt_section += "="*80 + "\n"
        prompt_section += " Apply similar correction patterns when generating SQL for this query.\n"
        prompt_section += "="*80 + "\n\n"
        
        return prompt_section
