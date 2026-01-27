"""
Schema Enrichment Service - Dynamic schema context with samples and relationships

Provides:
- Sample data fetching for better column inference
- Table relationship detection
- Dynamic schema context based on query intent
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from app.core.client_registry import registry

logger = logging.getLogger(__name__)


class SchemaEnrichmentService:
    """Service for enriching schema metadata with samples and relationships"""
    
    def __init__(self):
        self._schema_cache = {}
        self._relationship_cache = {}
        self._sample_cache = {}
    
    async def get_enriched_schema_context(
        self,
        user_query: str,
        intent: str = "",
        include_samples: bool = True,
        include_relationships: bool = True,
        sample_limit: int = 3,
        database_type: str = "oracle",
        connection_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get enriched schema context based on query intent
        
        Args:
            user_query: User's natural language query
            intent: Classified intent
            include_samples: Include sample rows for better column inference
            include_relationships: Include table relationships
            sample_limit: Max rows to fetch per table
            database_type: "oracle", "doris", or "postgres"
            connection_name: Database connection name
            
        Returns:
            Enriched schema context with tables, samples, and relationships
        """
        logger.info(f"Extracting enriched schema context for {database_type}...")
        
        from app.services.database_router import DatabaseRouter
        schema_result = await DatabaseRouter.get_database_schema(
            database_type=database_type,
            connection_name=connection_name,
            use_cache=True
        )
        
        if schema_result.get("status") != "success":
            logger.error(f"Failed to retrieve base schema for {database_type}")
            return {"tables": {}, "samples": {}, "relationships": [], "error": "Schema unavailable"}
        
        schema_data = schema_result.get("schema", {})
        
        mentioned_tables = self._extract_table_hints(user_query, intent, schema_data)
        
        missing = [t for t in mentioned_tables if t not in (schema_data.get("tables", {}) or {})]
        if missing:
            logger.info(f"Refreshing schema live for missing tables: {missing}")
            fresh = await DatabaseRouter.get_database_schema(
                database_type=database_type,
                connection_name=connection_name,
                use_cache=False
            )
            if fresh.get("status") == "success":
                schema_data = fresh.get("schema", {})
                mentioned_tables = self._extract_table_hints(user_query, intent, schema_data)
        
        logger.info(f"Identified {len(mentioned_tables)} relevant tables: {mentioned_tables}")
        
        enriched_context = {
            "tables": {},
            "samples": {},
            "relationships": [],
            "derived_hints": {},
            "metadata": {
                "enriched_at": datetime.utcnow().isoformat(),
                "tables_analyzed": len(mentioned_tables),
                "database_type": database_type
            }
        }
        
        tables = schema_data.get("tables", {})
        for table_name in mentioned_tables:
            if table_name in tables:
                enriched_context["tables"][table_name] = tables[table_name]
        
        try:
            enriched_context["derived_hints"] = self._derive_column_hints(mentioned_tables, tables)
        except Exception:
            enriched_context["derived_hints"] = {}
        
        if include_samples and mentioned_tables:
            samples = await self._fetch_sample_data(
                mentioned_tables[:5],  # Limit to 5 tables to avoid token overflow
                limit=sample_limit,
                database_type=database_type,
                connection_name=connection_name
            )
            enriched_context["samples"] = samples
        
        if include_relationships and len(mentioned_tables) > 1:
            if database_type == "oracle":
                relationships = await self._detect_table_relationships(
                    mentioned_tables,
                    schema_data
                )
                enriched_context["relationships"] = relationships
            else:
                # Basic relationship detection for non-Oracle
                relationships = await self._detect_table_relationships_generic(
                    mentioned_tables,
                    schema_data
                )
                enriched_context["relationships"] = relationships
        
        logger.info(
            f"Enriched context: {len(enriched_context['tables'])} tables, "
            f"{len(enriched_context['samples'])} samples, "
            f"{len(enriched_context['relationships'])} relationships, "
            f"{sum(len(v) for v in enriched_context.get('derived_hints', {}).values())} derived hints"
        )
        
        return enriched_context
    
    def _derive_column_hints(self, table_names: List[str], tables: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, str]]]:
        """Build generic, table-agnostic derived column hints for temporal features."""
        hints: Dict[str, List[Dict[str, str]]] = {}
        for t in table_names:
            cols = tables.get(t) or []
            table_hints: List[Dict[str, str]] = []
            date_cols = [c for c in cols if str(c.get('type','')).upper().find('DATE') >= 0 or str(c.get('type','')).upper().find('TIMESTAMP') >= 0 or c.get('name','').upper() in ['DATE','CREATED_AT','UPDATED_AT']]
            month_cols = [c for c in cols if c.get('name','').upper() in ['MONTH','MM','MNTH']]
            if date_cols:
                dc = date_cols[0]['name']
                table_hints.append({'concept': 'QUARTER', 'expression': f"TO_CHAR({t}.{dc}, 'Q')", 'note': f"Derive quarter from {dc}"})
                table_hints.append({'concept': 'YEAR', 'expression': f"TO_CHAR({t}.{dc}, 'YYYY')", 'note': f"Derive year from {dc}"})
                table_hints.append({'concept': 'YEAR_MONTH', 'expression': f"TO_CHAR({t}.{dc}, 'YYYY-MM')", 'note': f"Derive year-month from {dc}"})
            if month_cols:
                mc = month_cols[0]['name']
                table_hints.append({'concept': 'QUARTER', 'expression': f"CEIL({t}.{mc} / 3)", 'note': f"Derive quarter from {mc} (1-12)"})
            if table_hints:
                hints[t] = table_hints
        return hints
    
    def _extract_table_hints(
        self,
        query: str,
        intent: str,
        schema_data: Dict[str, Any]
    ) -> List[str]:
        """
        Extract table names mentioned or implied in the query
        
        Args:
            query: User's natural language query
            intent: Classified intent (may contain table hints)
            schema_data: Full schema metadata
            
        Returns:
            List of relevant table names
        """
        query_upper = query.upper()
        intent_upper = intent.upper()
        combined_text = f"{query_upper} {intent_upper}"
        
        mentioned_tables = []
        
        tables = schema_data.get("tables", {})
        for table_name in tables.keys():
            table_upper = table_name.upper()
            if table_upper in combined_text:
                mentioned_tables.append(table_name)
                continue
            if table_upper in combined_text or any(
                part in combined_text for part in table_upper.split("_")
            ):
                mentioned_tables.append(table_name)
        
        views = schema_data.get("views", {})
        for view_name in views.keys():
            view_upper = view_name.upper()
            if view_upper in combined_text:
                mentioned_tables.append(view_name)
        
        seen = set()
        mentioned_tables = [t for t in mentioned_tables if not (t in seen or seen.add(t))]
        
        return mentioned_tables
    
    async def _fetch_sample_data(
        self,
        table_names: List[str],
        limit: int = 5,
        database_type: str = "oracle",
        connection_name: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch sample rows from specified tables with Redis caching
        
        Args:
            table_names: List of table names
            limit: Max rows per table
            database_type: "oracle", "doris", or "postgres"
            connection_name: Connection name
            
        Returns:
            Dict mapping table names to sample rows
        """
        logger.info(f"Fetching sample data from {len(table_names)} tables ({database_type})...")
        
        samples = {}
        from app.core.config import settings
        from app.core.redis_client import redis_client
        
        for table_name in table_names:
            cache_key = f"sample_data:{database_type}:{table_name}:{limit}"
            
            try:
                cached_samples = await redis_client.get(cache_key)
                if cached_samples:
                    samples[table_name] = cached_samples
                    logger.debug(f"Retrieved {len(cached_samples)} sample rows from cache for {table_name}")
                    continue
            except Exception as e:
                logger.debug(f"Cache miss for {table_name}: {e}")
            
            if cache_key in self._sample_cache:
                samples[table_name] = self._sample_cache[cache_key]
                continue
            
            try:
                if database_type == "postgres":
                    from app.core.postgres_client import postgres_client
                    sql = f"SELECT * FROM {table_name} LIMIT {limit}"
                    result = await postgres_client.execute_query(
                        sql=sql,
                        user_id="system",
                        request_id="enrichment"
                    )
                    if result.get("status") == "success":
                        rows = result.get("rows", [])
                        samples[table_name] = rows
                elif database_type == "doris":
                    from app.core.client_registry import registry
                    mcp_client = registry.get_mcp_client()
                    sql = f"SELECT * FROM {table_name} LIMIT {limit}"
                    result = await mcp_client.execute_sql(sql, connection_name="doris_default")
                    if result.get("status") == "success":
                        rows = result.get("results", {}).get("rows", [])
                        samples[table_name] = rows
                else: # Oracle
                    from app.core.client_registry import registry
                    mcp_client = registry.get_mcp_client()
                    sql = f"SELECT * FROM {table_name} FETCH FIRST {limit} ROWS ONLY"
                    conn = connection_name or settings.oracle_default_connection
                    result = await mcp_client.execute_sql(sql, connection_name=conn)
                    if result.get("status") == "success":
                        rows = result.get("results", {}).get("rows", [])
                        samples[table_name] = rows
                
                if table_name in samples:
                    rows = samples[table_name]
                    self._sample_cache[cache_key] = rows
                    try:
                        await redis_client.set(cache_key, rows, ttl=1800)
                    except Exception as e:
                        logger.debug(f"Failed to cache samples: {e}")
                    logger.debug(f"Fetched {len(rows)} sample rows from {table_name}")
            
            except Exception as e:
                logger.warning(f"Error fetching samples from {table_name}: {e}")
        
        return samples

    async def _detect_table_relationships_generic(
        self,
        table_names: List[str],
        schema_data: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """
        Generic relationship detection based on column name patterns
        """
        relationships = []
        tables = schema_data.get("tables", {})
        
        for i, table1 in enumerate(table_names):
            if table1 not in tables:
                continue
            
            cols1 = [c.get('name', '').upper() for c in tables[table1]]
            
            for table2 in table_names[i+1:]:
                if table2 not in tables:
                    continue
                
                cols2 = [c.get('name', '').upper() for c in tables[table2]]
                
                # Check for shared column names
                common = set(cols1).intersection(set(cols2))
                for col in common:
                    if col.endswith("_ID") or col == "ID" or col.endswith("_KEY"):
                        relationships.append({
                            "type": "shared_column",
                            "table1": table1,
                            "table2": table2,
                            "column": col,
                            "join_hint": f"{table1}.{col} = {table2}.{col}"
                        })
        
        return relationships
    
    async def _detect_table_relationships(
        self,
        table_names: List[str],
        schema_data: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """
        Detect relationships between tables based on column names and foreign keys
        
        Args:
            table_names: List of table names to analyze
            schema_data: Full schema metadata
            
        Returns:
            List of detected relationships
        """
        logger.info(f"Detecting relationships among {len(table_names)} tables...")
        
        relationships = []
        tables = schema_data.get("tables", {})
        
        if str(schema_data.get("source", "")).lower() == "postgres":
            db_type = "postgres"
        elif str(schema_data.get("source", "")).lower() == "doris_mcp":
            db_type = "doris"
        else:
            db_type = "oracle"
            
        fk_relationships = await self._fetch_foreign_keys(table_names, database_type=db_type)
        relationships.extend(fk_relationships)
        
        for i, table1 in enumerate(table_names):
            if table1 not in tables:
                continue
            
            table1_cols = {col['name'].upper(): col for col in tables[table1]}
            
            for table2 in table_names[i+1:]:
                if table2 not in tables:
                    continue
                
                table2_cols = {col['name'].upper(): col for col in tables[table2]}
                
                for col1_name in table1_cols.keys():
                    if col1_name.endswith("_ID") or col1_name == "ID":
                        if col1_name in table2_cols:
                            relationships.append({
                                "type": "shared_key",
                                "table1": table1,
                                "table2": table2,
                                "column": col1_name,
                                "join_hint": f"{table1}.{col1_name} = {table2}.{col1_name}"
                            })
                        
                        table2_singular = table2.rstrip("S").replace("_", "")
                        if table2_singular.upper() in col1_name:
                            relationships.append({
                                "type": "foreign_key_hint",
                                "table1": table1,
                                "table2": table2,
                                "column": col1_name,
                                "join_hint": f"{table1}.{col1_name} = {table2}.ID"
                            })
        
        seen = set()
        unique_relationships = []
        for rel in relationships:
            key = f"{rel['table1']}:{rel['table2']}:{rel.get('column', '')}"
            if key not in seen:
                seen.add(key)
                unique_relationships.append(rel)
        
        logger.info(f"Detected {len(unique_relationships)} relationships")
        return unique_relationships
    
    async def _fetch_foreign_keys(
        self,
        table_names: List[str],
        database_type: str = "oracle"
    ) -> List[Dict[str, str]]:
        """
        Fetch actual foreign key constraints from database
        
        Args:
            table_names: List of table names
            database_type: Database type
            
        Returns:
            List of foreign key relationships
        """
        relationships = []
        mcp_client = registry.get_mcp_client()
        from app.core.config import settings
        
        if not mcp_client:
            return relationships

        # Foreign key queries currently only implemented for Oracle
        if database_type != "oracle":
            return relationships
        
        try:
            tables_list = "', '".join(table_names)
            sql = f"""
                SELECT 
                    a.table_name AS child_table,
                    a.column_name AS child_column,
                    pkc.table_name AS parent_table,
                    pkc.column_name AS parent_column,
                    c.constraint_name
                FROM user_constraints c
                JOIN user_cons_columns a
                  ON c.constraint_name = a.constraint_name
                JOIN user_constraints pk
                  ON c.r_constraint_name = pk.constraint_name AND pk.constraint_type = 'P'
                JOIN user_cons_columns pkc
                  ON pk.constraint_name = pkc.constraint_name AND pkc.position = a.position
                WHERE c.constraint_type = 'R'
                  AND a.table_name IN ('{tables_list}')
                FETCH FIRST 100 ROWS ONLY
            """
            
            result = await mcp_client.execute_sql(sql, connection_name=settings.oracle_default_connection)
            
            if result.get("status") == "success":
                results_block = result.get("results", {}) or {}
                columns = results_block.get("columns", [])
                rows = results_block.get("rows", [])
                col_idx = { (c or "").upper(): i for i, c in enumerate(columns) }
                
                for row in rows:
                    try:
                        child_table = row[col_idx.get("CHILD_TABLE", -1)] if isinstance(row, list) else None
                        parent_table = row[col_idx.get("PARENT_TABLE", -1)] if isinstance(row, list) else None
                        child_column = row[col_idx.get("CHILD_COLUMN", -1)] if isinstance(row, list) else None
                        parent_column = row[col_idx.get("PARENT_COLUMN", -1)] if isinstance(row, list) else None
                        if child_table and parent_table and child_column and parent_column:
                            relationships.append({
                                "type": "foreign_key",
                                "table1": str(child_table),
                                "table2": str(parent_table),
                                "column": str(child_column),
                                "ref_column": str(parent_column),
                                "join_hint": f"{child_table}.{child_column} = {parent_table}.{parent_column}"
                            })
                    except Exception:
                        # Skip malformed rows safely
                        continue
                
                logger.info(f"Found {len(rows)} foreign key constraints")
        
        except Exception as e:
            logger.warning(f"Could not fetch foreign keys: {e}")
        
        return relationships