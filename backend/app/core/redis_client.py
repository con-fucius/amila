"""
Redis Client for Session Storage, Caching, and Task Queue Backend

Provides unified Redis interface for:
- Session storage (JWT sessions, user data)
- Schema metadata caching (table/column info with TTL)
- Query result caching (frequent queries with LRU eviction)
- Celery task queue backend
"""

import json
import logging
from typing import Any, Optional
from datetime import datetime, timezone, timedelta

import redis.asyncio as redis
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import settings

logger = logging.getLogger(__name__)

# Constants
MAX_REDIS_CONNECTIONS = 50
MAX_SESSION_CONNECTIONS = 20
MAX_CACHE_CONNECTIONS = 20
MAX_VECTOR_CONNECTIONS = 10
MAX_QUERY_CACHE_ENTRIES = 1000  # LRU eviction threshold
QUERY_CACHE_INDEX_KEY = "query:cache_index"  # Sorted set for LRU tracking

class RedisClient:
    """Async Redis client with connection pooling and error handling"""
    
    def __init__(self):
        self._client: Optional[Redis] = None
        self._session_client: Optional[Redis] = None
        self._cache_client: Optional[Redis] = None
        # Separate raw (binary) client for RediSearch vector operations
        self._vector_client: Optional[Redis] = None

    def _require_client(self) -> Redis:
        if not self._client:
            raise RuntimeError("Redis client not initialized. Call connect() before issuing commands.")
        return self._client

    async def connect(self, max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize Redis connection pools with retry logic"""
        import asyncio
        
        last_error = None
        for attempt in range(max_retries):
            try:
                # Main client (default DB 0)
                self._client = redis.from_url(
                    settings.REDIS_URL,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=50,
                    socket_timeout=5.0,  # Add timeout
                    socket_connect_timeout=5.0,
                )

                # Session storage (DB 0)
                self._session_client = redis.from_url(
                    f"redis://:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_SESSION_DB}",
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=20,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )

                # Cache storage (DB 1)
                self._cache_client = redis.from_url(
                    f"redis://:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_CACHE_DB}",
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=20,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )

                # Vector client (binary) for RediSearch HNSW index - use cache DB by default
                # decode_responses=False is REQUIRED for VECTOR fields (binary blobs)
                self._vector_client = redis.from_url(
                    f"redis://:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_CACHE_DB}",
                    decode_responses=False,
                    max_connections=10,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )

                # Test connection
                await self._client.ping()
                logger.info(f"Redis connection established successfully")
                return

            except RedisError as e:
                last_error = e
                logger.warning(f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff
        
        logger.error(f"Failed to connect to Redis after {max_retries} attempts: {last_error}")
        raise last_error

    async def disconnect(self):
        """Close all Redis connections"""
        try:
            if self._client:
                await self._client.aclose()
            if self._session_client:
                await self._session_client.aclose()
            if self._cache_client:
                await self._cache_client.aclose()
            if self._vector_client:
                await self._vector_client.aclose()
            logger.info("Redis connections closed")
        except Exception as e:
            logger.error(f"Error closing Redis connections: {e}")

    # ==================== SESSION MANAGEMENT ====================

    async def set_session(
        self, session_id: str, user_data: dict, ttl: int = 86400
    ) -> bool:
        """
        Store user session data with TTL

        Args:
            session_id: Unique session identifier (JWT jti)
            user_data: User information dict
            ttl: Time to live in seconds (default: 24 hours, max 7 days)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Enforce maximum TTL of 7 days for security
            max_ttl = 7 * 24 * 3600  # 7 days
            ttl = min(ttl, max_ttl)
            
            key = f"session:{session_id}"
            await self._session_client.setex(
                key, ttl, json.dumps(user_data)
            )
            logger.debug(f"Session stored: {session_id}")
            return True
        except RedisError as e:
            logger.error(f"Failed to store session {session_id}: {e}")
            return False

    async def get_session(self, session_id: str) -> Optional[dict]:
        """
        Retrieve session data

        Args:
            session_id: Session identifier

        Returns:
            User data dict or None if not found
        """
        try:
            key = f"session:{session_id}"
            data = await self._session_client.get(key)
            if data:
                return json.loads(data)
            return None
        except RedisError as e:
            logger.error(f"Failed to get session {session_id}: {e}")
            return None

    async def delete_session(self, session_id: str) -> bool:
        """Delete session (logout)"""
        try:
            key = f"session:{session_id}"
            await self._session_client.delete(key)
            logger.debug(f"Session deleted: {session_id}")
            return True
        except RedisError as e:
            logger.error(f"Failed to delete session {session_id}: {e}")
            return False

    async def refresh_session(self, session_id: str, ttl: int = 86400) -> bool:
        """
        Extend session TTL (prevents indefinite accumulation)
        
        Args:
            ttl: Time to live in seconds (default: 24 hours, max 7 days)
        """
        try:
            # Enforce maximum TTL
            max_ttl = 7 * 24 * 3600
            ttl = min(ttl, max_ttl)
            
            key = f"session:{session_id}"
            await self._session_client.expire(key, ttl)
            return True
        except RedisError as e:
            logger.error(f"Failed to refresh session {session_id}: {e}")
            return False

    # ==================== SCHEMA CACHING ====================

    async def cache_schema_metadata(
        self, schema_key: str, metadata: dict, ttl: int = 3600
    ) -> bool:
        """
        Cache database schema metadata with TTL

        Args:
            schema_key: Unique key (e.g., "schema:tables" or "schema:columns:EMPLOYEES")
            metadata: Schema information dict
            ttl: Cache TTL in seconds (default: 1 hour, prevents unbounded growth)

        Returns:
            True if successful
        """
        try:
            key = f"schema:{schema_key}"
            await self._cache_client.setex(
                key, ttl, json.dumps(metadata)
            )
            logger.debug(f"Schema cached: {schema_key}")
            return True
        except RedisError as e:
            logger.error(f"Failed to cache schema {schema_key}: {e}")
            return False

    async def get_schema_metadata(self, schema_key: str) -> Optional[dict]:
        """Retrieve cached schema metadata"""
        try:
            key = f"schema:{schema_key}"
            data = await self._cache_client.get(key)
            if data:
                logger.debug(f"Schema cache hit: {schema_key}")
                return json.loads(data)
            logger.debug(f"Schema cache miss: {schema_key}")
            return None
        except RedisError as e:
            logger.error(f"Failed to get schema {schema_key}: {e}")
            return None

    async def invalidate_schema_cache(self, pattern: str = "schema:*", batch_size: int = 100) -> int:
        """Clear schema cache by pattern using SCAN (non-blocking)"""
        try:
            deleted_total = 0
            keys_batch = []
            
            # Use scan_iter with count hint for batching
            async for key in self._cache_client.scan_iter(match=pattern, count=batch_size):
                keys_batch.append(key)
                # Delete in batches to avoid blocking
                if len(keys_batch) >= batch_size:
                    deleted_total += await self._cache_client.delete(*keys_batch)
                    keys_batch = []
            
            # Delete remaining keys
            if keys_batch:
                deleted_total += await self._cache_client.delete(*keys_batch)
            
            if deleted_total > 0:
                logger.info(f"Invalidated {deleted_total} schema cache entries")
            return deleted_total
        except RedisError as e:
            logger.error(f"Failed to invalidate schema cache: {e}")
            return 0

    # ==================== SAMPLE DATA CACHING ====================
    
    async def cache_sample_data(
        self, table_name: str, sample_rows: list, ttl: int = 1800
    ) -> bool:
        """
        Cache sample data for a table
        
        Args:
            table_name: Name of the table
            sample_rows: List of sample row dicts
            ttl: Cache TTL in seconds (default: 30 minutes)
            
        Returns:
            True if successful
        """
        try:
            key = f"sample:{table_name.upper()}"
            await self._cache_client.setex(
                key, ttl, json.dumps(sample_rows)
            )
            logger.debug(f"Sample data cached for {table_name}: {len(sample_rows)} rows")
            return True
        except RedisError as e:
            logger.error(f"Failed to cache sample data for {table_name}: {e}")
            return False
    
    async def get_sample_data(self, table_name: str) -> Optional[list]:
        """Retrieve cached sample data for a table"""
        try:
            key = f"sample:{table_name.upper()}"
            data = await self._cache_client.get(key)
            if data:
                logger.debug(f"Sample data cache hit: {table_name}")
                return json.loads(data)
            logger.debug(f"Sample data cache miss: {table_name}")
            return None
        except RedisError as e:
            logger.error(f"Failed to get sample data for {table_name}: {e}")
            return None
    
    async def invalidate_sample_cache(self, pattern: str = "sample:*") -> int:
        """Clear sample data cache"""
        try:
            keys = []
            async for key in self._cache_client.scan_iter(match=pattern):
                keys.append(key)
            
            if keys:
                deleted = await self._cache_client.delete(*keys)
                logger.info(f"Invalidated {deleted} sample cache entries")
                return deleted
            return 0
        except RedisError as e:
            logger.error(f"Failed to invalidate sample cache: {e}")
            return 0
    
    # ==================== QUERY RESULT CACHING ====================

    async def cache_query_result(
        self, query_hash: str, result: dict, ttl: int = 300, result_size: int = 0
    ) -> bool:
        """
        Cache query execution results with adaptive TTL and LRU eviction.

        Args:
            query_hash: SHA256 hash of normalized SQL query
            result: Query result dict (rows, columns, metadata)
            ttl: Base cache TTL in seconds (default: 5 minutes)
            result_size: Size of result set (rows) for adaptive TTL

        Returns:
            True if successful
        """
        try:
            # Adaptive TTL based on result size
            # Small results: longer TTL (frequently queried, cheap to cache)
            # Large results: shorter TTL (expensive to cache, likely one-off queries)
            if result_size > 0:
                if result_size <= 100:
                    ttl = 1800  # 30 minutes for small results
                elif result_size <= 1000:
                    ttl = 600   # 10 minutes for medium results
                else:
                    ttl = 300   # 5 minutes for large results
            
            key = f"query:{query_hash}"
            current_time = datetime.now(timezone.utc).timestamp()
            
            # LRU eviction: check cache size and evict oldest entries if needed
            cache_size = await self._cache_client.zcard(QUERY_CACHE_INDEX_KEY)
            if cache_size >= MAX_QUERY_CACHE_ENTRIES:
                # Evict oldest 10% of entries
                evict_count = max(1, MAX_QUERY_CACHE_ENTRIES // 10)
                oldest_keys = await self._cache_client.zrange(QUERY_CACHE_INDEX_KEY, 0, evict_count - 1)
                if oldest_keys:
                    # Delete the actual cache entries - check existence first to avoid errors
                    keys_to_delete = [f"query:{k}" for k in oldest_keys]
                    # Pipeline for efficiency
                    pipe = self._cache_client.pipeline()
                    for k in keys_to_delete:
                        pipe.delete(k)
                    await pipe.execute()
                    # Remove from index
                    await self._cache_client.zremrangebyrank(QUERY_CACHE_INDEX_KEY, 0, evict_count - 1)
                    logger.info(f"LRU evicted {len(oldest_keys)} query cache entries")
            
            # Add cache metadata for monitoring
            cache_entry = {
                "result": result,
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "result_size": result_size,
                "ttl": ttl,
            }
            
            # Store the cache entry
            await self._cache_client.setex(key, ttl, json.dumps(cache_entry))
            
            # Update LRU index (sorted set with timestamp as score)
            await self._cache_client.zadd(QUERY_CACHE_INDEX_KEY, {query_hash: current_time})
            # Set TTL on index entry to auto-cleanup (slightly longer than max cache TTL)
            await self._cache_client.expire(QUERY_CACHE_INDEX_KEY, 7200)
            
            logger.debug(f"Query result cached: {query_hash[:16]}... (TTL: {ttl}s, size: {result_size} rows)")
            return True
        except RedisError as e:
            logger.error(f"Failed to cache query result: {e}")
            return False

    async def get_cached_query_result(self, query_hash: str) -> Optional[dict]:
        """Retrieve cached query result"""
        try:
            key = f"query:{query_hash}"
            data = await self._cache_client.get(key)
            if data:
                cache_entry = json.loads(data)
                
                # Return just the result, not metadata
                result = cache_entry.get("result") if isinstance(cache_entry, dict) else cache_entry
                
                logger.debug(
                    f"Query cache hit: {query_hash[:16]}... "
                    f"(cached: {cache_entry.get('cached_at', 'unknown') if isinstance(cache_entry, dict) else 'legacy'})"
                )
                return result
            logger.debug(f"Query cache miss: {query_hash[:16]}...")
            return None
        except RedisError as e:
            logger.error(f"Failed to get cached query: {e}")
            return None

    async def invalidate_query_cache(self, pattern: str = "query:*") -> int:
        """Clear query result cache"""
        try:
            keys = []
            async for key in self._cache_client.scan_iter(match=pattern):
                keys.append(key)
            
            if keys:
                deleted = await self._cache_client.delete(*keys)
                logger.info(f"Invalidated {deleted} query cache entries")
                return deleted
            return 0
        except RedisError as e:
            logger.error(f"Failed to invalidate query cache: {e}")
            return 0
    
    async def warm_query_cache(
        self, common_queries: list[tuple[str, str]], connection_name: str
    ) -> int:
        """
        Warm cache with commonly executed queries
        
        Args:
            common_queries: List of (query_hash, sql_query) tuples
            connection_name: Database connection name
            
        Returns:
            Number of queries successfully cached
            
        Note: Import of registry is deferred to avoid circular import at module load time.
        """
        # Deferred import to avoid circular dependency
        try:
            from app.core.client_registry import registry
        except ImportError as e:
            logger.error(f"Failed to import registry for cache warming: {e}")
            return 0
        
        warmed = 0
        logger.info(f"Warming query cache with {len(common_queries)} common queries...")
        
        for query_hash, sql_query in common_queries:
            try:
                # Check if already cached
                if await self.get_cached_query_result(query_hash):
                    continue
                
                # Execute and cache
                mcp_client = registry.get_mcp_client()
                if not mcp_client:
                    break
                
                result = await mcp_client.execute_sql(sql_query, connection_name)
                
                if result.get("status") == "success":
                    result_data = {
                        "status": "success",
                        "columns": result.get("results", {}).get("columns", []),
                        "rows": result.get("results", {}).get("rows", []),
                        "row_count": result.get("results", {}).get("row_count", 0),
                        "execution_time_ms": result.get("results", {}).get("execution_time_ms", 0),
                    }
                    
                    await self.cache_query_result(
                        query_hash,
                        result_data,
                        result_size=result_data["row_count"]
                    )
                    warmed += 1
                    
            except Exception as e:
                logger.warning(f"Failed to warm cache for query {query_hash[:16]}: {e}")
                continue
        
        logger.info(f"Warmed {warmed}/{len(common_queries)} queries in cache")
        return warmed

    # ==================== GENERIC KEY-VALUE OPERATIONS ====================

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Generic set operation"""
        try:
            client = self._require_client()
            # Handle serialization - strings pass through, others get JSON encoded
            if isinstance(value, str):
                payload = value
            else:
                try:
                    payload = json.dumps(value)
                except (TypeError, ValueError) as e:
                    logger.warning(f"Failed to serialize value for key {key}: {e}")
                    payload = str(value)
            
            if ttl:
                await client.setex(key, ttl, payload)
            else:
                await client.set(key, payload)
            return True
        except RedisError as e:
            logger.error(f"Failed to set key {key}: {e}")
            return False

    async def setex(self, key: str, ttl: int, value: Any) -> bool:
        """Set a key with expiration (wrapper used by audit logger)."""
        try:
            client = self._require_client()
            # If value is not a string, serialize to JSON for consistency with decode_responses=True
            payload = value if isinstance(value, str) else json.dumps(value)
            await client.setex(key, ttl, payload)
            return True
        except RedisError as e:
            logger.error(f"Failed to setex key {key}: {e}")
            return False

    async def get(self, key: str) -> Optional[Any]:
        """Generic get operation"""
        try:
            data = await self._require_client().get(key)
            if data:
                return json.loads(data)
            return None
        except RedisError as e:
            logger.error(f"Failed to get key {key}: {e}")
            return None

    async def delete(self, key: str) -> bool:
        """Generic delete operation"""
        try:
            await self._require_client().delete(key)
            return True
        except RedisError as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            return await self._require_client().exists(key) > 0
        except RedisError as e:
            logger.error(f"Failed to check key existence {key}: {e}")
            return False

    # ==================== SORTED SET OPERATIONS ====================

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        """Add members to a sorted set."""
        return await self._require_client().zadd(key, mapping)

    async def zcard(self, key: str) -> int:
        """Return the sorted set cardinality."""
        return await self._require_client().zcard(key)

    async def zrange(self, key: str, start: int, stop: int, withscores: bool = False):
        """Return a range of members in a sorted set."""
        return await self._require_client().zrange(key, start, stop, withscores=withscores)

    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        """Remove all members in the sorted set within the given scores."""
        return await self._require_client().zremrangebyscore(key, min_score, max_score)

    async def expire(self, key: str, seconds: int) -> bool:
        """Set a key's time to live in seconds."""
        return await self._require_client().expire(key, seconds)

    async def keys(self, pattern: str) -> list[str]:
        """Find keys matching a given pattern.
        WARNING: Uses KEYS command which is O(n) - use scan_iter for large datasets."""
        return await self._require_client().keys(pattern)
    
    async def zrevrange(self, key: str, start: int, stop: int, withscores: bool = False):
        """Return a range of members in a sorted set, by score, high to low."""
        return await self._require_client().zrevrange(key, start, stop, withscores=withscores)

    # ==================== VECTOR / SEARCH OPERATIONS ====================

    async def ensure_vector_index(self, index_name: str, vector_field: str, dims: int) -> bool:
        """Ensure a RediSearch HNSW vector index exists.
        Uses raw commands to avoid additional client dependencies.
        Returns True on success or if already exists."""
        try:
            if not self._vector_client:
                raise RuntimeError("Vector client not initialized")
            # Check if index exists
            try:
                await self._vector_client.execute_command("FT.INFO", index_name)
                return True
            except Exception:
                pass
            # Create index on HASH with TEXT fields and VECTOR field
            # Schema: name (TEXT), type (TAG), table (TEXT), column (TEXT), text (TEXT), embedding (VECTOR HNSW)
            cmd = [
                "FT.CREATE", index_name,
                "ON", "HASH",
                "SCHEMA",
                "name", "TEXT",
                "type", "TAG",
                "table", "TEXT",
                "column", "TEXT",
                "text", "TEXT",
                vector_field, "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", str(dims),
                "DISTANCE_METRIC", "COSINE",
                "INITIAL_CAP", "1000",
            ]
            await self._vector_client.execute_command(*cmd)
            return True
        except Exception as e:
            logger.error(f"Failed to ensure vector index {index_name}: {e}")
            return False

    async def upsert_vector_document(self, key: str, fields: dict) -> bool:
        """Upsert a HASH document compatible with the vector index.
        'embedding' field must be bytes (float32 array)."""
        try:
            if not self._vector_client:
                raise RuntimeError("Vector client not initialized")
            # Build mapping dict for hset - convert non-string/bytes to string
            mapping = {}
            for k, v in fields.items():
                if isinstance(v, (str, bytes)):
                    mapping[k] = v
                else:
                    mapping[k] = str(v)
            await self._vector_client.hset(key, mapping=mapping)
            return True
        except Exception as e:
            logger.error(f"Failed to upsert vector document {key}: {e}")
            return False

    async def knn_search(self, index_name: str, vector_field: str, query_vector: bytes, k: int = 10, filters: dict | None = None) -> list:
        """Perform KNN search using RediSearch KNN syntax; returns list of docs with scores and basic fields."""
        try:
            if not self._vector_client:
                raise RuntimeError("Vector client not initialized")
            # Build filter expression
            filter_expr = "*"
            if filters:
                tag_filters = []
                for fk, fv in filters.items():
                    tag_filters.append(f"@{fk}:{{{fv}}}")
                if tag_filters:
                    filter_expr = " ".join(tag_filters)
            params = ["2", "vec_param", query_vector]
            cmd = [
                "FT.SEARCH", index_name,
                f"{filter_expr}=>[KNN {k} {vector_field} $vec_param AS vector_score]",
                "PARAMS", *params,
                "RETURN", "6", "name", "type", "table", "column", "text", "vector_score",
                "SORTBY", "vector_score", "ASC",
                "DIALECT", "2",
            ]
            raw = await self._vector_client.execute_command(*cmd)
            # raw format: [total, key1, [field, val, ...], key2, [ ... ] ...]
            # We'll convert to dicts with minimal fields
            results = []
            if isinstance(raw, list) and len(raw) >= 1:
                for i in range(1, len(raw), 2):
                    if i + 1 >= len(raw):
                        break
                    key = raw[i]
                    fields = raw[i + 1]
                    doc = {"key": key.decode() if isinstance(key, bytes) else str(key)}
                    # Fields array like [b'name', b'EMP', b'type', b'table', ...]
                    if isinstance(fields, list):
                        for j in range(0, len(fields), 2):
                            kf = fields[j]
                            vf = fields[j + 1] if j + 1 < len(fields) else None
                            kfs = kf.decode() if isinstance(kf, bytes) else str(kf)
                            if isinstance(vf, (bytes, bytearray)):
                                try:
                                    doc[kfs] = vf.decode()
                                except Exception:
                                    doc[kfs] = str(vf)
                            else:
                                doc[kfs] = vf
                    results.append(doc)
            return results
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []

    # ==================== HEALTH CHECK ====================

    async def ping(self) -> bool:
        """Ping Redis server."""
        return await self._require_client().ping()
    
    async def info(self, section: Optional[str] = None) -> dict:
        """Get Redis server info."""
        if section:
            return await self._require_client().info(section)
        return await self._require_client().info()
    
    async def close(self) -> None:
        """Close all Redis connections (alias for disconnect)."""
        await self.disconnect()
    
    async def health_check(self) -> dict:
        """Redis health check"""
        try:
            # Ping test
            await self._client.ping()
            
            # Get info
            info = await self._client.info()
            
            return {
                "status": "healthy",
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "unknown"),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            }
        except RedisError as e:
            logger.error(f"Redis health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
            }


# Global Redis client instance
redis_client = RedisClient()


async def get_redis_client() -> RedisClient:
    """Dependency injection for FastAPI routes"""
    return redis_client