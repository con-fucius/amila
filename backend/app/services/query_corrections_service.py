"""
Query Corrections Service
Captures user SQL edits and learns from corrections with semantic retrieval
"""

import logging
import json
import uuid
import difflib
import struct
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)

# Vector search constants for semantic retrieval
CORRECTIONS_INDEX_NAME = "semantic:corrections:index"
VECTOR_FIELD = "embedding"


def _floats_to_bytes(vec: List[float]) -> bytes:
    """Convert list[float] -> float32 bytes for RediSearch VECTOR field."""
    return struct.pack(f"<{len(vec)}f", *[float(x) for x in vec])


class _EmbeddingClient:
    """Minimal wrapper around LangChain embeddings for async usage."""

    def __init__(self):
        self.provider = settings.GRAPHITI_EMBEDDING_PROVIDER.lower()
        self.model = settings.GRAPHITI_EMBEDDING_MODEL
        self._client = None

    def _get_sync_client(self):
        if self._client:
            return self._client
        if self.provider == "gemini":
            from langchain_google_genai import GoogleGenerativeAIEmbeddings

            model_name = self.model
            if not model_name.startswith("models/"):
                model_name = f"models/{model_name}"
            
            self._client = GoogleGenerativeAIEmbeddings(model=model_name, google_api_key=settings.GOOGLE_API_KEY)
        elif self.provider == "bedrock":
            from langchain_aws import BedrockEmbeddings
            self._client = BedrockEmbeddings(model_id=self.model, region_name=settings.aws_region)
        else:
            raise ValueError(f"Unsupported embedding provider: {self.provider}")
        return self._client

    async def embed_query(self, text: str) -> List[float]:
        loop = asyncio.get_event_loop()
        client = self._get_sync_client()
        return await loop.run_in_executor(None, lambda: client.embed_query(text))

    async def embed_documents(self, docs: List[str]) -> List[List[float]]:
        loop = asyncio.get_event_loop()
        client = self._get_sync_client()
        return await loop.run_in_executor(None, lambda: client.embed_documents(docs))


class QueryCorrectionsService:
    """Service for learning from user SQL corrections with semantic retrieval"""
    
    _emb = None
    _dims = None
    
    @classmethod
    def _get_embedding_client(cls):
        """Get or create embedding client singleton"""
        if cls._emb is None:
            cls._emb = _EmbeddingClient()
            cls._dims = settings.GRAPHITI_EMBEDDING_DIMENSIONS
        return cls._emb, cls._dims
    
    @classmethod
    async def _ensure_vector_index(cls) -> bool:
        """Ensure the corrections vector index exists"""
        try:
            _, dims = cls._get_embedding_client()
            return await redis_client.ensure_vector_index(CORRECTIONS_INDEX_NAME, VECTOR_FIELD, dims)
        except Exception as e:
            logger.warning(f"Failed to ensure corrections vector index: {e}")
            return False
    
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
        Store SQL correction for learning with semantic embedding
        
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
        created_at = datetime.now(timezone.utc).isoformat()
        
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
            "created_at": created_at,
            "metadata": metadata or {},
        }
        
        try:
            # Store correction object
            await redis_client.set(f"correction:{correction_id}", item, ttl=14*24*3600)
            
            # Indexes for quick retrieval
            await redis_client._client.lpush("corrections:index", correction_id)
            await redis_client._client.ltrim("corrections:index", 0, 999)
            await redis_client._client.lpush(f"user:{user_id}:corrections", correction_id)
            await redis_client._client.ltrim(f"user:{user_id}:corrections", 0, 999)
            
            # Store semantic embedding for vector search
            try:
                await QueryCorrectionsService._store_correction_embedding(
                    correction_id, original_query, generated_sql, corrected_sql, intent
                )
            except Exception as embed_err:
                logger.warning(f"Failed to store correction embedding (non-critical): {embed_err}")
            
            logger.info(f"Stored SQL correction in Redis: {correction_id}")
            return correction_id
        except Exception as e:
            logger.error(f"Failed to store correction in Redis: {e}")
            return correction_id
    
    @classmethod
    async def _store_correction_embedding(
        cls,
        correction_id: str,
        original_query: str,
        generated_sql: str,
        corrected_sql: str,
        intent: Optional[str] = None
    ):
        """Store vector embedding for semantic similarity search"""
        try:
            emb, dims = cls._get_embedding_client()
            
            # Ensure index exists
            await cls._ensure_vector_index()
            
            # Create rich text representation for embedding
            text_to_embed = f"""
Query: {original_query}
Intent: {intent or 'unknown'}
Generated SQL: {generated_sql[:200]}
Corrected SQL: {corrected_sql[:200]}
Key changes: {cls._extract_key_changes(generated_sql, corrected_sql)}
""".strip()
            
            # Generate embedding
            vector = await emb.embed_query(text_to_embed)
            
            # Store in vector index
            payload = {
                "correction_id": correction_id,
                "original_query": original_query,
                "intent": intent or "",
                "text": text_to_embed[:500],
                VECTOR_FIELD: _floats_to_bytes(vector),
            }
            
            key = f"correction:vec:{correction_id}"
            await redis_client.upsert_vector_document(key, payload)
            logger.debug(f"Stored correction embedding: {correction_id}")
            
        except Exception as e:
            logger.warning(f"Embedding storage failed: {e}")
            raise
    
    @staticmethod
    def _extract_key_changes(generated: str, corrected: str) -> str:
        """Extract key semantic changes between generated and corrected SQL"""
        # Simple extraction of changed keywords
        gen_upper = set(generated.upper().split())
        corr_upper = set(corrected.upper().split())
        added = corr_upper - gen_upper
        removed = gen_upper - corr_upper
        
        changes = []
        if added:
            changes.append(f"Added: {', '.join(list(added)[:5])}")
        if removed:
            changes.append(f"Removed: {', '.join(list(removed)[:5])}")
        return '; '.join(changes) if changes else "Syntax/formatting changes"
    
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
    
    @classmethod
    async def get_relevant_corrections(
        cls,
        original_query: Optional[str] = None,
        intent: Optional[str] = None,
        limit: int = 5,
        use_semantic: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant corrections using semantic similarity search.
        
        Uses vector embeddings for semantic matching, falling back to keyword
        matching if semantic search is unavailable.
        
        Args:
            original_query: Natural language query to match
            intent: Intent to match
            limit: Max corrections to retrieve
            use_semantic: Whether to use semantic search (vs keyword only)
            
        Returns:
            List of relevant correction examples
        """
        if not original_query:
            return await cls._get_recent_corrections(limit)
        
        # Try semantic search first (if enabled and available)
        if use_semantic:
            try:
                semantic_results = await cls._get_semantic_corrections(original_query, intent, limit)
                if semantic_results:
                    logger.info(f"Retrieved {len(semantic_results)} corrections via semantic search")
                    return semantic_results
            except Exception as e:
                logger.warning(f"Semantic search failed, falling back to keyword: {e}")
        
        # Fallback to keyword matching
        return await cls._get_keyword_corrections(original_query, intent, limit)
    
    @classmethod
    async def _get_semantic_corrections(
        cls,
        original_query: str,
        intent: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Retrieve corrections using vector similarity search"""
        try:
            emb, _ = cls._get_embedding_client()
            
            # Ensure index exists
            await cls._ensure_vector_index()
            
            # Create query embedding
            query_text = f"{original_query} {intent or ''}".strip()
            query_vector = await emb.embed_query(query_text)
            vec_bytes = _floats_to_bytes(query_vector)
            
            # Perform KNN search
            results = await redis_client.knn_search(
                CORRECTIONS_INDEX_NAME, 
                VECTOR_FIELD, 
                vec_bytes, 
                k=limit * 2  # Fetch more for filtering
            )
            
            # Retrieve full correction documents
            corrections = []
            for r in results:
                correction_id = r.get("correction_id")
                if isinstance(correction_id, (bytes, bytearray)):
                    correction_id = correction_id.decode()
                
                # Get full correction data
                item = await redis_client.get(f"correction:{correction_id}")
                if item and item.get("success_after_correction"):
                    corrections.append({
                        "correction_id": item.get("correction_id"),
                        "original_query": item.get("original_query"),
                        "generated_sql": item.get("generated_sql"),
                        "corrected_sql": item.get("corrected_sql"),
                        "diff_summary": item.get("diff_summary", {}),
                        "applied_count": int(item.get("applied_count", 0)),
                        "created_at": item.get("created_at"),
                        "intent": item.get("intent"),
                    })
            
            # Sort by applied_count for relevance
            corrections.sort(key=lambda x: x.get("applied_count", 0), reverse=True)
            return corrections[:limit]
            
        except Exception as e:
            logger.warning(f"Semantic correction retrieval failed: {e}")
            return []
    
    @staticmethod
    async def _get_keyword_corrections(
        original_query: str,
        intent: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fallback: Retrieve corrections using keyword matching"""
        try:
            # Get most recent corrections and filter in-memory
            ids = await redis_client._client.lrange("corrections:index", 0, 199)
            results: List[Dict[str, Any]] = []
            
            kws = [kw for kw in original_query.upper().split() if len(kw) > 3][:5]
            
            for cid in ids:
                item = await redis_client.get(f"correction:{cid}")
                if not item:
                    continue
                if not item.get("success_after_correction"):
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
                    "intent": item.get("intent"),
                })
            
            # Sort by applied_count
            results.sort(key=lambda x: x.get("applied_count", 0), reverse=True)
            logger.info(f"Retrieved {len(results)} corrections via keyword matching")
            return results[:limit]
        except Exception as e:
            logger.error(f"Failed to retrieve keyword corrections: {e}")
            return []
    
    @staticmethod
    async def _get_recent_corrections(limit: int) -> List[Dict[str, Any]]:
        """Get most recent successful corrections"""
        try:
            ids = await redis_client._client.lrange("corrections:index", 0, limit - 1)
            results = []
            for cid in ids:
                item = await redis_client.get(f"correction:{cid}")
                if item and item.get("success_after_correction"):
                    results.append({
                        "correction_id": item.get("correction_id"),
                        "original_query": item.get("original_query"),
                        "generated_sql": item.get("generated_sql"),
                        "corrected_sql": item.get("corrected_sql"),
                        "diff_summary": item.get("diff_summary", {}),
                        "applied_count": int(item.get("applied_count", 0)),
                        "created_at": item.get("created_at"),
                        "intent": item.get("intent"),
                    })
            return results
        except Exception as e:
            logger.error(f"Failed to retrieve recent corrections: {e}")
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
