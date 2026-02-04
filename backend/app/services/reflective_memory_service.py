"""
Reflective Memory Service
Learns from SQL generation and repair patterns to improve future queries.
Stores "lessons learned" from query successes/failures and provides
intelligent insights to the SQL generation process.
"""

import logging
import json
import struct
import hashlib
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)

# Constants for vector storage
REFLECTIVE_MEMORY_INDEX = "semantic:reflective_memory:index"
VECTOR_FIELD = "embedding"
LESSONS_KEY_PREFIX = "reflective:lesson:"
PATTERNS_KEY_PREFIX = "reflective:pattern:"


def _floats_to_bytes(vec: List[float]) -> bytes:
    """Convert list[float] -> float32 bytes for RediSearch VECTOR field."""
    return struct.pack(f"<{len(vec)}f", *[float(x) for x in vec])


class LessonType(str, Enum):
    """Types of lessons learned"""
    COLUMN_MAPPING = "column_mapping"  # Learned column name patterns
    JOIN_PATTERN = "join_pattern"  # Learned table join patterns
    SYNTAX_FIX = "syntax_fix"  # Learned syntax corrections
    SEMANTIC_FIX = "semantic_fix"  # Learned semantic corrections
    OPTIMIZATION = "optimization"  # Learned query optimizations
    SCHEMA_PATTERN = "schema_pattern"  # Schema-specific patterns


@dataclass
class Lesson:
    """A learned lesson from query generation/repair"""
    lesson_id: str
    lesson_type: LessonType
    query_pattern: str  # Natural language query pattern
    schema_fingerprint: str  # Hash of schema context
    failed_attempts: List[str]  # SQL patterns that failed
    successful_sql: str  # SQL pattern that worked
    lesson_text: str  # Human-readable lesson
    confidence: float  # 0.0-1.0 based on success rate
    application_count: int  # How many times applied
    success_count: int  # How many times successful
    created_at: str
    last_applied_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


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
            self._client = GoogleGenerativeAIEmbeddings(
                model=model_name, 
                google_api_key=settings.GOOGLE_API_KEY
            )
        elif self.provider == "bedrock":
            from langchain_aws import BedrockEmbeddings
            self._client = BedrockEmbeddings(
                model_id=self.model, 
                region_name=settings.aws_region
            )
        else:
            raise ValueError(f"Unsupported embedding provider: {self.provider}")
        return self._client

    async def embed_query(self, text: str) -> List[float]:
        loop = asyncio.get_event_loop()
        client = self._get_sync_client()
        return await loop.run_in_executor(None, lambda: client.embed_query(text))


class ReflectiveMemoryService:
    """
    Service for learning from SQL generation and repair patterns.
    
    Records lessons learned from:
    - Failed SQL generation attempts and their fixes
    - Successful query repairs in the repair node
    - User corrections and feedback
    - Schema-specific patterns that work
    
    Provides insights to:
    - SQL generation prompts (few-shot examples)
    - Repair node strategies
    - Column mapping suggestions
    """
    
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
    async def record_lesson(
        cls,
        lesson_type: LessonType,
        query_pattern: str,
        successful_sql: str,
        schema_fingerprint: str,
        failed_attempts: List[str] = None,
        lesson_text: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Record a lesson learned from SQL generation/repair
        
        Args:
            lesson_type: Type of lesson
            query_pattern: Natural language query pattern
            successful_sql: SQL that succeeded
            schema_fingerprint: Schema context hash
            failed_attempts: SQL patterns that failed
            lesson_text: Human-readable lesson description
            metadata: Additional metadata
            
        Returns:
            lesson_id
        """
        lesson_id = hashlib.sha256(
            f"{query_pattern}:{successful_sql}:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]
        
        now = datetime.now(timezone.utc).isoformat()
        
        # Generate lesson text if not provided
        if not lesson_text:
            lesson_text = cls._generate_lesson_text(
                lesson_type, query_pattern, failed_attempts or [], successful_sql
            )
        
        lesson = Lesson(
            lesson_id=lesson_id,
            lesson_type=lesson_type,
            query_pattern=query_pattern,
            schema_fingerprint=schema_fingerprint,
            failed_attempts=failed_attempts or [],
            successful_sql=successful_sql,
            lesson_text=lesson_text,
            confidence=1.0,  # Initial confidence
            application_count=1,
            success_count=1,
            created_at=now,
            last_applied_at=now,
            metadata=metadata or {}
        )
        
        try:
            # Store lesson object
            await redis_client.set(
                f"{LESSONS_KEY_PREFIX}{lesson_id}",
                asdict(lesson),
                ttl=365*24*3600  # 1 year retention
            )
            
            # Add to type index
            await redis_client._client.lpush(
                f"reflective:index:{lesson_type.value}",
                lesson_id
            )
            await redis_client._client.ltrim(
                f"reflective:index:{lesson_type.value}",
                0, 999
            )
            
            # Add to schema-specific index
            await redis_client._client.lpush(
                f"reflective:schema:{schema_fingerprint}",
                lesson_id
            )
            
            # Store vector embedding for semantic search
            await cls._store_lesson_embedding(lesson)
            
            logger.info(f"Recorded reflective lesson: {lesson_id} ({lesson_type.value})")
            return lesson_id
            
        except Exception as e:
            logger.error(f"Failed to record lesson: {e}")
            return lesson_id
    
    @classmethod
    async def _store_lesson_embedding(cls, lesson: Lesson):
        """Store vector embedding for semantic retrieval"""
        try:
            emb, dims = cls._get_embedding_client()
            
            # Ensure index exists
            await redis_client.ensure_vector_index(
                REFLECTIVE_MEMORY_INDEX, 
                VECTOR_FIELD, 
                dims
            )
            
            # Create rich text for embedding
            text_to_embed = f"""
Lesson Type: {lesson.lesson_type.value}
Query Pattern: {lesson.query_pattern}
Lesson: {lesson.lesson_text}
Successful SQL: {lesson.successful_sql[:200]}
Failed Patterns: {'; '.join(lesson.failed_attempts[:3])}
""".strip()
            
            vector = await emb.embed_query(text_to_embed)
            
            payload = {
                "lesson_id": lesson.lesson_id,
                "lesson_type": lesson.lesson_type.value,
                "query_pattern": lesson.query_pattern,
                "schema_fingerprint": lesson.schema_fingerprint,
                "lesson_text": lesson.lesson_text[:500],
                VECTOR_FIELD: _floats_to_bytes(vector),
            }
            
            key = f"reflective:vec:{lesson.lesson_id}"
            await redis_client.upsert_vector_document(key, payload)
            
        except Exception as e:
            logger.warning(f"Failed to store lesson embedding: {e}")
    
    @classmethod
    def _generate_lesson_text(
        cls,
        lesson_type: LessonType,
        query_pattern: str,
        failed_attempts: List[str],
        successful_sql: str
    ) -> str:
        """Auto-generate lesson text from patterns"""
        
        if lesson_type == LessonType.COLUMN_MAPPING:
            return f"For queries like '{query_pattern}', map concepts to columns using: {successful_sql[:100]}"
        
        elif lesson_type == LessonType.JOIN_PATTERN:
            return f"Join pattern for '{query_pattern}': Use {successful_sql[:100]}"
        
        elif lesson_type == LessonType.SYNTAX_FIX:
            failed = failed_attempts[0] if failed_attempts else "previous approach"
            return f"Fix syntax error: Change from '{failed[:50]}' to '{successful_sql[:50]}'"
        
        elif lesson_type == LessonType.SEMANTIC_FIX:
            return f"Semantic correction for '{query_pattern}': {successful_sql[:100]}"
        
        elif lesson_type == LessonType.OPTIMIZATION:
            return f"Optimization for '{query_pattern}': {successful_sql[:100]}"
        
        elif lesson_type == LessonType.SCHEMA_PATTERN:
            return f"Schema-specific pattern for '{query_pattern}': {successful_sql[:100]}"
        
        return f"Lesson for '{query_pattern}': Use {successful_sql[:100]}"
    
    @classmethod
    async def get_relevant_lessons(
        cls,
        query: str,
        schema_fingerprint: Optional[str] = None,
        lesson_type: Optional[LessonType] = None,
        min_confidence: float = 0.5,
        limit: int = 5
    ) -> List[Lesson]:
        """
        Retrieve relevant lessons for a query using semantic similarity
        
        Args:
            query: Natural language query
            schema_fingerprint: Optional schema context filter
            lesson_type: Optional type filter
            min_confidence: Minimum confidence threshold
            limit: Max lessons to return
            
        Returns:
            List of relevant lessons
        """
        try:
            # Try semantic search first
            semantic_lessons = await cls._semantic_lesson_search(
                query, schema_fingerprint, lesson_type, min_confidence, limit
            )
            
            if semantic_lessons:
                return semantic_lessons
            
            # Fallback to keyword-based search
            return await cls._keyword_lesson_search(
                query, schema_fingerprint, lesson_type, min_confidence, limit
            )
            
        except Exception as e:
            logger.warning(f"Failed to retrieve lessons: {e}")
            return []
    
    @classmethod
    async def _semantic_lesson_search(
        cls,
        query: str,
        schema_fingerprint: Optional[str],
        lesson_type: Optional[LessonType],
        min_confidence: float,
        limit: int
    ) -> List[Lesson]:
        """Semantic search for lessons"""
        emb, _ = cls._get_embedding_client()
        
        # Generate query embedding
        query_vector = await emb.embed_query(query)
        vec_bytes = _floats_to_bytes(query_vector)
        
        # Search vector index
        results = await redis_client.knn_search(
            REFLECTIVE_MEMORY_INDEX,
            VECTOR_FIELD,
            vec_bytes,
            k=limit * 2  # Fetch extra for filtering
        )
        
        lessons = []
        for r in results:
            lesson_id = r.get("lesson_id")
            if isinstance(lesson_id, (bytes, bytearray)):
                lesson_id = lesson_id.decode()
            
            # Get full lesson
            lesson_data = await redis_client.get(f"{LESSONS_KEY_PREFIX}{lesson_id}")
            if not lesson_data:
                continue
            
            # Apply filters
            if lesson_type and lesson_data.get("lesson_type") != lesson_type.value:
                continue
            
            if schema_fingerprint and lesson_data.get("schema_fingerprint") != schema_fingerprint:
                # Allow cross-schema lessons with lower priority
                pass
            
            if lesson_data.get("confidence", 0) < min_confidence:
                continue
            
            lessons.append(Lesson(**lesson_data))
        
        # Sort by confidence and limit
        lessons.sort(key=lambda x: x.confidence, reverse=True)
        return lessons[:limit]
    
    @classmethod
    async def _keyword_lesson_search(
        cls,
        query: str,
        schema_fingerprint: Optional[str],
        lesson_type: Optional[LessonType],
        min_confidence: float,
        limit: int
    ) -> List[Lesson]:
        """Keyword-based fallback search"""
        keywords = [kw for kw in query.upper().split() if len(kw) > 3][:5]
        
        # Determine which index to search
        if lesson_type:
            index_key = f"reflective:index:{lesson_type.value}"
        elif schema_fingerprint:
            index_key = f"reflective:schema:{schema_fingerprint}"
        else:
            # Search all type indexes
            lessons = []
            for lt in LessonType:
                type_lessons = await cls._keyword_lesson_search(
                    query, schema_fingerprint, lt, min_confidence, limit // len(LessonType)
                )
                lessons.extend(type_lessons)
            lessons.sort(key=lambda x: x.confidence, reverse=True)
            return lessons[:limit]
        
        # Get lesson IDs from index
        lesson_ids = await redis_client._client.lrange(index_key, 0, 199)
        
        lessons = []
        for lid in lesson_ids:
            lesson_data = await redis_client.get(f"{LESSONS_KEY_PREFIX}{lid}")
            if not lesson_data:
                continue
            
            if lesson_data.get("confidence", 0) < min_confidence:
                continue
            
            # Check keyword match
            query_pattern = lesson_data.get("query_pattern", "")
            if keywords and not any(kw in query_pattern.upper() for kw in keywords):
                continue
            
            lessons.append(Lesson(**lesson_data))
        
        lessons.sort(key=lambda x: x.confidence, reverse=True)
        return lessons[:limit]
    
    @classmethod
    async def update_lesson_success(
        cls,
        lesson_id: str,
        was_successful: bool
    ):
        """
        Update lesson statistics based on application success
        
        Args:
            lesson_id: Lesson to update
            was_successful: Whether the lesson helped succeed
        """
        try:
            lesson_data = await redis_client.get(f"{LESSONS_KEY_PREFIX}{lesson_id}")
            if not lesson_data:
                return
            
            lesson = Lesson(**lesson_data)
            lesson.application_count += 1
            
            if was_successful:
                lesson.success_count += 1
            
            # Recalculate confidence using Bayesian approach
            lesson.confidence = lesson.success_count / lesson.application_count
            lesson.last_applied_at = datetime.now(timezone.utc).isoformat()
            
            # Update stored lesson
            await redis_client.set(
                f"{LESSONS_KEY_PREFIX}{lesson_id}",
                asdict(lesson),
                ttl=365*24*3600
            )
            
        except Exception as e:
            logger.warning(f"Failed to update lesson stats: {e}")
    
    @classmethod
    async def format_lessons_for_prompt(
        cls,
        lessons: List[Lesson],
        max_lessons: int = 3
    ) -> str:
        """
        Format lessons for inclusion in SQL generation prompt
        
        Args:
            lessons: List of lessons to format
            max_lessons: Max number of lessons to include
            
        Returns:
            Formatted prompt section
        """
        if not lessons:
            return ""
        
        prompt = "\n" + "="*80 + "\n"
        prompt += " REFLECTIVE MEMORY - LESSONS LEARNED FROM PAST QUERIES\n"
        prompt += "="*80 + "\n\n"
        
        for i, lesson in enumerate(lessons[:max_lessons], 1):
            prompt += f"Lesson {i} ({lesson.lesson_type.value}, confidence: {lesson.confidence:.0%}):\n"
            prompt += f"  Context: {lesson.query_pattern}\n"
            prompt += f"  Insight: {lesson.lesson_text}\n"
            
            if lesson.failed_attempts:
                prompt += f"  Avoid: {lesson.failed_attempts[0][:100]}...\n"
            
            prompt += f"  Prefer: {lesson.successful_sql[:150]}...\n"
            prompt += f"  Applied {lesson.application_count} times, {lesson.success_count} successes\n\n"
        
        prompt += "="*80 + "\n"
        prompt += " Apply these lessons when generating SQL for this query.\n"
        prompt += "="*80 + "\n\n"
        
        return prompt
    
    @classmethod
    async def record_repair_success(
        cls,
        original_query: str,
        failed_sql: str,
        repaired_sql: str,
        schema_fingerprint: str,
        repair_type: str,
        error_message: str
    ) -> str:
        """
        Convenience method to record a lesson from a successful SQL repair
        
        Args:
            original_query: Original natural language query
            failed_sql: SQL that failed
            repaired_sql: SQL that succeeded
            schema_fingerprint: Schema context
            repair_type: Type of repair performed
            error_message: Original error message
            
        Returns:
            lesson_id
        """
        # Determine lesson type from error and repair
        lesson_type = LessonType.SYNTAX_FIX
        if "column" in error_message.lower() or "identifier" in error_message.lower():
            lesson_type = LessonType.COLUMN_MAPPING
        elif "join" in error_message.lower():
            lesson_type = LessonType.JOIN_PATTERN
        elif "semantic" in repair_type.lower():
            lesson_type = LessonType.SEMANTIC_FIX
        elif "optimize" in repair_type.lower():
            lesson_type = LessonType.OPTIMIZATION
        
        lesson_text = f"Repair from '{repair_type}': {error_message[:100]}"
        
        return await cls.record_lesson(
            lesson_type=lesson_type,
            query_pattern=original_query,
            successful_sql=repaired_sql,
            schema_fingerprint=schema_fingerprint,
            failed_attempts=[failed_sql],
            lesson_text=lesson_text,
            metadata={
                "repair_type": repair_type,
                "error_message": error_message[:200]
            }
        )
    
    @classmethod
    async def get_schema_specific_patterns(
        cls,
        schema_fingerprint: str,
        limit: int = 10
    ) -> List[Lesson]:
        """Get lessons specific to a schema"""
        lesson_ids = await redis_client._client.lrange(
            f"reflective:schema:{schema_fingerprint}",
            0,
            limit - 1
        )
        
        lessons = []
        for lid in lesson_ids:
            lesson_data = await redis_client.get(f"{LESSONS_KEY_PREFIX}{lid}")
            if lesson_data:
                lessons.append(Lesson(**lesson_data))
        
        return sorted(lessons, key=lambda x: x.confidence, reverse=True)


# Global instance
reflective_memory_service = ReflectiveMemoryService()
