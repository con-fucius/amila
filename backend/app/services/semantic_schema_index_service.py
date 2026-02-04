"""
Semantic Schema Index Service
- Builds and queries a semantic index of tables and columns using Redis RediSearch HNSW
- Uses embedding provider configured in settings (Gemini or Bedrock) via LangChain
"""

from __future__ import annotations

import asyncio
import logging
import struct
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.schema_service import SchemaService

logger = logging.getLogger(__name__)

INDEX_NAME = "semantic:schema:index"
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

            # Gemini models must start with models/
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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),  # Better to catch specific API errors
        reraise=True
    )
    async def embed_query(self, text: str) -> List[float]:
        loop = asyncio.get_event_loop()
        client = self._get_sync_client()
        return await loop.run_in_executor(None, lambda: client.embed_query(text))

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def embed_documents(self, docs: List[str]) -> List[List[float]]:
        # Batch documents to avoid exceeding provider limits
        batch_size = 50
        all_embeddings = []
        loop = asyncio.get_event_loop()
        client = self._get_sync_client()
        
        for i in range(0, len(docs), batch_size):
            batch = docs[i : i + batch_size]
            embeddings = await loop.run_in_executor(None, lambda: client.embed_documents(batch))
            all_embeddings.extend(embeddings)
            if i + batch_size < len(docs):
                await asyncio.sleep(0.5)  # Simple rate limit between batches
                
        return all_embeddings


class SemanticSchemaIndexService:
    """Service for building and querying semantic schema index in Redis (RediSearch)."""

    def __init__(self):
        self._emb = _EmbeddingClient()
        self._dims = settings.GRAPHITI_EMBEDDING_DIMENSIONS

    async def ensure_index(self) -> bool:
        return await redis_client.ensure_vector_index(INDEX_NAME, VECTOR_FIELD, self._dims)

    async def rebuild_index(self) -> Dict[str, Any]:
        """Rebuild index from current cached DB schema (tables + columns)."""
        ok = await self.ensure_index()
        if not ok:
            return {"status": "error", "message": "Failed to ensure vector index"}

        schema = await SchemaService.get_database_schema(use_cache=True)
        if schema.get("status") != "success":
            return {"status": "error", "message": "Schema unavailable"}

        raw = schema.get("schema", {})
        tables: Dict[str, List[Dict[str, Any]]] = (raw or {}).get("tables", {}) or {}

        # Prepare documents
        docs: List[Tuple[str, Dict[str, Any], str]] = []  # (key, fields, text)
        for tname, cols in tables.items():
            # Table doc
            col_names = ", ".join([c.get("name", "") for c in cols][:50])
            table_text = f"TABLE {tname}: {col_names}"
            docs.append((
                f"schema:obj:table:{tname}",
                {"name": tname, "type": "table", "table": tname, "column": "", "text": table_text},
                table_text,
            ))
            # Column docs
            for c in cols[:200]:
                cname = c.get("name", "")
                ctype = c.get("type", "")
                ctext = f"COLUMN {tname}.{cname} {ctype}"
                docs.append((
                    f"schema:obj:column:{tname}:{cname}",
                    {"name": cname, "type": "column", "table": tname, "column": cname, "text": ctext},
                    ctext,
                ))

        texts = [d[2] for d in docs]
        if not texts:
            return {"status": "success", "indexed": 0}

        try:
            embeddings = await self._emb.embed_documents(texts)
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            return {"status": "error", "message": str(e)}

        # Perform upserts in batches with concurrency control
        semaphore = asyncio.Semaphore(10)
        async def _safe_upsert(key, payload):
            async with semaphore:
                return await redis_client.upsert_vector_document(key, payload)

        tasks = []
        for (key, fields, _), vec in zip(docs, embeddings):
            payload = dict(fields)
            payload[VECTOR_FIELD] = _floats_to_bytes(vec)
            payload["indexed_at"] = datetime.now(timezone.utc).isoformat()
            tasks.append(_safe_upsert(key, payload))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        indexed = sum(1 for r in results if r is True)
        
        return {"status": "success", "indexed": indexed}

    async def search(self, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        try:
            qvec = await self._emb.embed_query(query)
            vec_bytes = _floats_to_bytes(qvec)
            results = await redis_client.knn_search(INDEX_NAME, VECTOR_FIELD, vec_bytes, k=top_k)
            # Normalize
            out: List[Dict[str, Any]] = []
            for r in results:
                out.append({
                    "key": r.get("key"),
                    "name": (r.get("name") or b"").decode() if isinstance(r.get("name"), (bytes, bytearray)) else r.get("name"),
                    "type": (r.get("type") or b"").decode() if isinstance(r.get("type"), (bytes, bytearray)) else r.get("type"),
                    "table": (r.get("table") or b"").decode() if isinstance(r.get("table"), (bytes, bytearray)) else r.get("table"),
                    "column": (r.get("column") or b"").decode() if isinstance(r.get("column"), (bytes, bytearray)) else r.get("column"),
                    "text": (r.get("text") or b"").decode() if isinstance(r.get("text"), (bytes, bytearray)) else r.get("text"),
                })
            return out
        except Exception as e:
            logger.error(f"Semantic search error: {e}")
            return []

    async def ensure_built_if_empty(self) -> None:
        """Best-effort build when index seems empty."""
        try:
            ok = await self.ensure_index()
            if not ok:
                return
            # Check doc count (info["num_docs"]) - use FT.INFO
            try:
                info = await redis_client._vector_client.execute_command("FT.INFO", INDEX_NAME)  # type: ignore[attr-defined]
                # info is a flat list; find 'num_docs'
                num_docs = 0
                if isinstance(info, list):
                    for i in range(0, len(info), 2):
                        if i + 1 < len(info) and info[i] == b"num_docs":
                            try:
                                num_docs = int(info[i + 1])
                            except Exception:
                                num_docs = 0
                            break
                if num_docs == 0:
                    await self.rebuild_index()
            except Exception:
                await self.rebuild_index()
        except Exception:
            pass

    async def auto_discover_and_index(self) -> Dict[str, Any]:
        """
        Fix 7: Automatically discover new tables and index them.
        
        This method compares the current database schema with the indexed schema
        and adds any new tables/columns that haven't been indexed yet.
        """
        try:
            # Ensure index exists
            ok = await self.ensure_index()
            if not ok:
                return {"status": "error", "message": "Failed to ensure vector index"}
            
            # Get current database schema
            db_schema = await SchemaService.get_database_schema(use_cache=True)
            if db_schema.get("status") != "success":
                return {"status": "error", "message": "Schema unavailable"}
            
            raw = db_schema.get("schema", {})
            db_tables: Dict[str, List[Dict[str, Any]]] = (raw or {}).get("tables", {}) or {}
            
            # Get already indexed tables
            indexed_tables = set()
            try:
                # Query for all table entries in the index
                results = await redis_client.knn_search(INDEX_NAME, VECTOR_FIELD, b"\x00" * (self._dims * 4), k=10000)
                for r in results:
                    obj_type = r.get("type")
                    if isinstance(obj_type, bytes):
                        obj_type = obj_type.decode()
                    if obj_type == "table":
                        name = r.get("name")
                        if isinstance(name, bytes):
                            name = name.decode()
                        indexed_tables.add(name)
            except Exception as e:
                logger.warning(f"Could not retrieve indexed tables: {e}")
            
            # Find new tables not in index
            new_tables = [t for t in db_tables.keys() if t not in indexed_tables]
            
            if not new_tables:
                return {"status": "success", "indexed": 0, "message": "No new tables to index"}
            
            # Index new tables and their columns using optimized batching
            indexed_count = 0
            
            for tname in new_tables:
                # Basic validation
                if not tname or not isinstance(tname, str):
                    continue
                    
                cols = db_tables[tname]
                if not cols:
                    continue
                
                # Table doc
                col_names = ", ".join([str(c.get("name", "")) for c in cols][:50])
                table_text = f"TABLE {tname}: {col_names}"
                
                try:
                    table_embedding = await self._emb.embed_query(table_text)
                    table_key = f"schema:obj:table:{tname}"
                    table_payload = {
                        "name": tname,
                        "type": "table",
                        "table": tname,
                        "column": "",
                        "text": table_text,
                        "indexed_at": datetime.now(timezone.utc).isoformat()
                    }
                    table_payload[VECTOR_FIELD] = _floats_to_bytes(table_embedding)
                    
                    if await redis_client.upsert_vector_document(table_key, table_payload):
                        indexed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to index table {tname}: {e}")
                    continue
                
                # Index columns (batch processing)
                column_docs = []
                for c in cols[:200]:
                    cname = c.get("name", "")
                    if not cname: continue
                    ctype = c.get("type", "UNKNOWN")
                    ctext = f"COLUMN {tname}.{cname} {ctype}"
                    column_docs.append((f"schema:obj:column:{tname}:{cname}", {
                        "name": cname,
                        "type": "column",
                        "table": tname,
                        "column": cname,
                        "text": ctext
                    }, ctext))
                
                if column_docs:
                    try:
                        texts = [d[2] for d in column_docs]
                        col_embeddings = await self._emb.embed_documents(texts)
                        
                        # Concurrently upsert columns
                        col_semaphore = asyncio.Semaphore(10)
                        async def _upsert_col(k, p):
                            async with col_semaphore:
                                return await redis_client.upsert_vector_document(k, p)

                        col_tasks = []
                        for (key, fields, _), vec in zip(column_docs, col_embeddings):
                            payload = dict(fields)
                            payload[VECTOR_FIELD] = _floats_to_bytes(vec)
                            payload["indexed_at"] = datetime.now(timezone.utc).isoformat()
                            col_tasks.append(_upsert_col(key, payload))
                        
                        col_results = await asyncio.gather(*col_tasks)
                        indexed_count += sum(1 for r in col_results if r is True)
                    except Exception as e:
                        logger.warning(f"Failed to index columns for {tname}: {e}")
            
            logger.info(f"Auto-discovered and indexed {len(new_tables)} new tables ({indexed_count} total objects)")
            return {
                "status": "success",
                "indexed": indexed_count,
                "tables_added": len(new_tables),
                "new_tables": new_tables
            }
            
        except Exception as e:
            logger.error(f"Auto-discover failed: {e}")
            return {"status": "error", "message": str(e)}
