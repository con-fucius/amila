"""
Native Database Audit Service
Writes audit logs to native database tables (Doris/Postgres) alongside Redis
Provides immutable audit trails for compliance (GDPR, HIPAA)
"""

import logging
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from enum import Enum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.redis_client import redis_client
from app.core.encryption import get_encryption_service
from app.core.postgres_client import postgres_client
from app.core.doris_client import doris_client

logger = logging.getLogger(__name__)


class DatabaseType(str, Enum):
    """Supported database types for audit storage"""
    POSTGRES = "postgres"
    DORIS = "doris"


class NativeAuditService:
    """
    Unified audit service that writes to both Redis (fast access) 
    and native database tables (compliance/immutability)
    """
    
    def __init__(self):
        self.encryption = get_encryption_service()
        self.db_type = self._detect_db_type()
        self.retention_days = 90  # Default retention
        
    def _detect_db_type(self) -> DatabaseType:
        """Detect which database type to use for audit storage"""
        # Default to Postgres for audit (better ACID compliance)
        # Can be overridden via settings
        audit_db = getattr(settings, 'AUDIT_DATABASE_TYPE', 'postgres').lower()
        if audit_db == 'doris':
            return DatabaseType.DORIS
        return DatabaseType.POSTGRES
    
    def _generate_sql_hash(self, sql: str) -> str:
        """Generate SHA256 hash of SQL query"""
        normalized = sql.lower().strip()
        normalized = " ".join(normalized.split())
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    def _classify_gdpr(self, action: str, details: Dict[str, Any]) -> str:
        """Classify GDPR data category"""
        if 'auth' in action:
            return 'authentication'
        elif 'query' in action:
            return 'query_execution'
        elif action in ['admin.user_create', 'admin.user_update', 'admin.role_assign']:
            return 'user_data'
        elif action in ['system.config_update', 'agent.decision', 'security.violation', 'security.rls_enforcement']:
            return 'security_event'
        return 'general'

    def _mask_pii(self, text: str) -> str:
        """Mask PII in strings (emails, phone numbers, SSNs)"""
        if not isinstance(text, str):
            return text
            
        # Email masking
        text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[EMAIL_REDACTED]', text)
        
        # Phone number masking (simple pattern)
        text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE_REDACTED]', text)
        
        # Generic sensitive patterns (e.g., potential card numbers or SSNs)
        text = re.sub(r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b', '[CARD_REDACTED]', text)
        text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]', text)
        
        return text

    def _recursive_mask(self, data: Any) -> Any:
        """Recursively mask PII in dicts or lists"""
        if isinstance(data, dict):
            return {k: self._recursive_mask(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._recursive_mask(v) for v in data]
        elif isinstance(data, str):
            # Check for sensitive keys in the context (often passed as direct strings)
            return self._mask_pii(data)
        return data
    
    async def write_audit_entry(
        self,
        action: str,
        user_id: str,
        user_role: Optional[str] = None,
        success: bool = True,
        severity: str = "info",
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        execution_time_ms: Optional[int] = None,
        row_count: Optional[int] = None,
        database_type: Optional[str] = None,
        database_name: Optional[str] = None,
        llm_provider: Optional[str] = None,
        llm_model: Optional[str] = None,
        db_session: Optional[AsyncSession] = None
    ) -> bool:
        """
        Write audit entry to both Redis and native database
        
        Args:
            action: Audit action type
            user_id: User identifier
            user_role: User's role
            success: Whether action succeeded
            severity: Event severity
            resource_type: Type of resource accessed
            resource_id: Resource identifier
            details: Additional details (will be partially encrypted)
            ip_address: Client IP
            user_agent: Client user agent
            session_id: Session identifier
            correlation_id: Request correlation ID
            execution_time_ms: Query execution time
            row_count: Rows returned/affected
            database_type: Target database type
            database_name: Target database name
            llm_provider: LLM provider used
            llm_model: LLM model used
            db_session: SQLAlchemy async session (if None, uses default)
            
        Returns:
            True if successfully written to at least one store
        """
        timestamp = datetime.now(timezone.utc)
        
        # Separate sensitive from public details
        details = details or {}
        public_details = {}
        sensitive_details = {}
        query_fingerprint = None
        
        for key, value in details.items():
            if key in ['sql_query', 'query', 'password', 'token', 'secret', 'api_key', 'private_key']:
                sensitive_details[key] = value
                if key in ['sql_query', 'query'] and isinstance(value, str):
                    query_fingerprint = self._generate_sql_hash(value)[:32]
            else:
                # Apply PII masking to public details
                public_details[key] = self._recursive_mask(value)
        
        # Encrypt sensitive details
        encrypted_details = None
        if sensitive_details:
            encrypted = self.encryption.encrypt_audit_entry(sensitive_details)
            encrypted_details = json.dumps(encrypted)
        
        # Determine GDPR category
        gdpr_category = self._classify_gdpr(action, details)
        
        # Build audit entry
        entry = {
            "timestamp": timestamp,
            "action": action,
            "user_id": user_id,
            "user_role": user_role,
            "severity": severity,
            "success": success,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "ip_address": ip_address,
            "user_agent": user_agent[:500] if user_agent else None,
            "session_id": session_id,
            "correlation_id": correlation_id,
            "details_encrypted": encrypted_details,
            "details_public": json.dumps(public_details) if public_details else None,
            "query_fingerprint": query_fingerprint,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "execution_time_ms": execution_time_ms,
            "row_count": row_count,
            "database_type": database_type,
            "database_name": database_name,
            "gdpr_category": gdpr_category,
            "is_immutable": True
        }
        
        results = []
        
        # Write to Redis (fast access)
        try:
            await self._write_to_redis(entry)
            results.append(True)
        except Exception as e:
            logger.error(f"Failed to write audit to Redis: {e}")
            results.append(False)
        
        # Write to native database (compliance)
        if db_session:
            try:
                await self._write_to_database(db_session, entry)
                results.append(True)
            except Exception as e:
                logger.error(f"Failed to write audit to native DB via provided session: {e}")
                results.append(False)
        else:
            # Fallback to internal connection management
            try:
                success = await self._write_to_database_internal(entry)
                results.append(success)
            except Exception as e:
                logger.error(f"Failed to write audit to native DB via internal client: {e}")
                results.append(False)
        
        return any(results)
    
    async def _write_to_database_internal(self, entry: Dict[str, Any]) -> bool:
        """Write audit entry using internal database clients (Postgres/Doris)"""
        db_type = self.db_type
        
        try:
            if db_type == DatabaseType.POSTGRES:
                # Initialize postgres client if needed
                await postgres_client.initialize()
                
                # Prepare SQL with positional parameters for psycopg
                sql = """
                    INSERT INTO audit_log (
                        timestamp, action, user_id, user_role, severity, success,
                        resource_type, resource_id, ip_address, user_agent, session_id,
                        correlation_id, details_encrypted, details_public, query_fingerprint,
                        llm_provider, llm_model, execution_time_ms, row_count,
                        database_type, database_name, gdpr_category, is_immutable
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """
                params = (
                    entry["timestamp"], entry["action"], entry["user_id"], entry["user_role"], entry["severity"], entry["success"],
                    entry["resource_type"], entry["resource_id"], entry["ip_address"], entry["user_agent"], entry["session_id"],
                    entry["correlation_id"], entry["details_encrypted"], entry["details_public"], entry["query_fingerprint"],
                    entry["llm_provider"], entry["llm_model"], entry["execution_time_ms"], entry["row_count"],
                    entry["database_type"], entry["database_name"], entry["gdpr_category"], entry["is_immutable"]
                )
                
                async with postgres_client.get_connection(read_only=False) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, params)
                return True
                
            elif db_type == DatabaseType.DORIS:
                # Use Doris MCP client
                await doris_client.initialize()
                
                # Doris uses MySQL syntax
                entry_id = int(entry['timestamp'].timestamp() * 1000000)
                
                sql = f"""
                    INSERT INTO audit_log_doris (
                        timestamp, id, action, user_id, user_role, severity, success,
                        resource_type, resource_id, ip_address, session_id, correlation_id,
                        details_public, query_fingerprint, llm_provider, llm_model,
                        execution_time_ms, row_count, database_type, database_name
                    ) VALUES (
                        '{entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}', 
                        {entry_id}, 
                        '{entry['action']}', 
                        '{entry['user_id']}', 
                        '{entry['user_role'] or ''}', 
                        '{entry['severity']}', 
                        {1 if entry['success'] else 0},
                        '{entry['resource_type'] or ''}', 
                        '{entry['resource_id'] or ''}', 
                        '{entry['ip_address'] or ''}', 
                        '{entry['session_id'] or ''}', 
                        '{entry['correlation_id'] or ''}',
                        '{entry['details_public'] or '{}'}', 
                        '{entry['query_fingerprint'] or ''}', 
                        '{entry['llm_provider'] or ''}', 
                        '{entry['llm_model'] or ''}',
                        {entry['execution_time_ms'] or 0}, 
                        {entry['row_count'] or 0}, 
                        '{entry['database_type'] or ''}', 
                        '{entry['database_name'] or ''}'
                    )
                """
                
                result = await doris_client.execute_sql(sql)
                return result.get("status") == "success"
                
            return False
        except Exception as e:
            logger.error(f"Internal database write failed: {e}")
            return False
    
    async def _write_to_redis(self, entry: Dict[str, Any]):
        """Write audit entry to Redis"""
        redis_key = f"audit:native:{entry['correlation_id'] or entry['session_id'] or entry['user_id']}:{entry['action']}"
        
        # Store with TTL
        await redis_client.setex(
            redis_key,
            self.retention_days * 24 * 60 * 60,
            json.dumps(entry, default=str)
        )
        
        # Add to user's audit trail
        user_key = f"audit:native:user:{entry['user_id']}"
        await redis_client.zadd(user_key, {redis_key: entry['timestamp'].timestamp()})
        await redis_client.expire(user_key, self.retention_days * 24 * 60 * 60)
    
    async def _write_to_database(self, session: AsyncSession, entry: Dict[str, Any]):
        """Write audit entry to native database table"""
        
        if self.db_type == DatabaseType.POSTGRES:
            sql = text("""
                INSERT INTO audit_log (
                    timestamp, action, user_id, user_role, severity, success,
                    resource_type, resource_id, ip_address, user_agent, session_id,
                    correlation_id, details_encrypted, details_public, query_fingerprint,
                    llm_provider, llm_model, execution_time_ms, row_count,
                    database_type, database_name, gdpr_category, is_immutable
                ) VALUES (
                    :timestamp, :action, :user_id, :user_role, :severity, :success,
                    :resource_type, :resource_id, :ip_address, :user_agent, :session_id,
                    :correlation_id, :details_encrypted, :details_public, :query_fingerprint,
                    :llm_provider, :llm_model, :execution_time_ms, :row_count,
                    :database_type, :database_name, :gdpr_category, :is_immutable
                )
            """)
        else:  # Doris
            sql = text("""
                INSERT INTO audit_log_doris (
                    timestamp, id, action, user_id, user_role, severity, success,
                    resource_type, resource_id, ip_address, session_id, correlation_id,
                    details_public, query_fingerprint, llm_provider, llm_model,
                    execution_time_ms, row_count, database_type, database_name
                ) VALUES (
                    :timestamp, :id, :action, :user_id, :user_role, :severity, :success,
                    :resource_type, :resource_id, :ip_address, :session_id, :correlation_id,
                    :details_public, :query_fingerprint, :llm_provider, :llm_model,
                    :execution_time_ms, :row_count, :database_type, :database_name
                )
            """)
            entry['id'] = int(entry['timestamp'].timestamp() * 1000000)  # Microsecond timestamp as ID
        
        await session.execute(sql, entry)
        await session.commit()
    
    async def query_audit_trail(
        self,
        session: AsyncSession,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Query audit trail from native database
        
        Args:
            session: Database session
            user_id: Filter by user
            action: Filter by action type
            start_time: Start of time range
            end_time: End of time range
            limit: Max results
            offset: Pagination offset
            
        Returns:
            List of audit entries
        """
        if session:
            return await self._query_audit_trail_session(
                session, user_id, action, start_time, end_time, limit, offset
            )
        else:
            return await self._query_audit_trail_internal(
                user_id, action, start_time, end_time, limit, offset
            )
            
    async def _query_audit_trail_internal(
        self,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query audit trail using internal database clients"""
        
        # Build SQL
        where_clauses = ["1=1"]
        params = []
        
        if user_id:
            where_clauses.append("user_id = %s" if self.db_type == DatabaseType.POSTGRES else f"user_id = '{user_id}'")
            if self.db_type == DatabaseType.POSTGRES:
                params.append(user_id)
        
        if action:
            where_clauses.append("action = %s" if self.db_type == DatabaseType.POSTGRES else f"action = '{action}'")
            if self.db_type == DatabaseType.POSTGRES:
                params.append(action)
        
        if start_time:
            where_clauses.append("timestamp >= %s" if self.db_type == DatabaseType.POSTGRES else f"timestamp >= '{start_time.isoformat()}'")
            if self.db_type == DatabaseType.POSTGRES:
                params.append(start_time)
        
        if end_time:
            where_clauses.append("timestamp <= %s" if self.db_type == DatabaseType.POSTGRES else f"timestamp <= '{end_time.isoformat()}'")
            if self.db_type == DatabaseType.POSTGRES:
                params.append(end_time)
        
        where_clause = " AND ".join(where_clauses)
        
        if self.db_type == DatabaseType.POSTGRES:
            await postgres_client.initialize()
            
            sql = f"""
                SELECT 
                    id, timestamp, action, user_id, user_role, severity, success,
                    resource_type, resource_id, ip_address, session_id, correlation_id,
                    details_public, query_fingerprint, llm_provider, llm_model,
                    execution_time_ms, row_count, database_type
                FROM audit_log
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, offset])
            
            try:
                async with postgres_client.get_connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, tuple(params))
                        rows = await cur.fetchall()
                        columns = [desc[0] for desc in cur.description]
            except Exception as e:
                logger.error(f"Internal Postgres query failed: {e}")
                return []
                
        else: # Doris
            await doris_client.initialize()
            
            sql = f"""
                SELECT 
                    id, timestamp, action, user_id, user_role, severity, success,
                    resource_type, resource_id, ip_address, session_id, correlation_id,
                    details_public, query_fingerprint, llm_provider, llm_model,
                    execution_time_ms, row_count, database_type
                FROM audit_log_doris
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT {limit} OFFSET {offset}
            """
            
            result = await doris_client.execute_sql(sql)
            if result.get("status") != "success":
                logger.error(f"Internal Doris query failed: {result.get('error')}")
                return []
                
            data = result.get("results", {})
            rows = data.get("rows", [])
            columns = data.get("columns", [])

        # Process results
        results = []
        for row in rows:
            entry = {}
            # Handle different row formats (tuple vs list)
            row_data = row if isinstance(row, (list, tuple)) else []
            
            for i, col_name in enumerate(columns):
                val = row_data[i]
                if col_name == 'details_public' and isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except:
                        val = {}
                elif col_name == 'timestamp' and isinstance(val, str):
                    # Keep as string or parse if needed, usually string is fine for API
                    pass
                
                entry[col_name] = val
            results.append(entry)
            
        return results

    async def _query_audit_trail_session(
        self,
        session: AsyncSession,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query audit trail using SQLAlchemy session"""
        conditions = []
        params = {}
        
        if user_id:
            conditions.append("user_id = :user_id")
            params['user_id'] = user_id
        
        if action:
            conditions.append("action = :action")
            params['action'] = action
        
        if start_time:
            conditions.append("timestamp >= :start_time")
            params['start_time'] = start_time
        
        if end_time:
            conditions.append("timestamp <= :end_time")
            params['end_time'] = end_time
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        sql = text(f"""
            SELECT 
                id, timestamp, action, user_id, user_role, severity, success,
                resource_type, resource_id, ip_address, session_id, correlation_id,
                details_public, query_fingerprint, llm_provider, llm_model,
                execution_time_ms, row_count, database_type
            FROM audit_log
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT :limit OFFSET :offset
        """)
        
        params['limit'] = limit
        params['offset'] = offset
        
        result = await session.execute(sql, params)
        rows = result.fetchall()
        
        return [
            {
                "id": row.id,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "action": row.action,
                "user_id": row.user_id,
                "user_role": row.user_role,
                "severity": row.severity,
                "success": row.success,
                "resource_type": row.resource_type,
                "resource_id": row.resource_id,
                "ip_address": str(row.ip_address) if row.ip_address else None,
                "session_id": row.session_id,
                "correlation_id": row.correlation_id,
                "details_public": json.loads(row.details_public) if row.details_public else {},
                "query_fingerprint": row.query_fingerprint,
                "llm_provider": row.llm_provider,
                "llm_model": row.llm_model,
                "execution_time_ms": row.execution_time_ms,
                "row_count": row.row_count,
                "database_type": row.database_type,
            }
            for row in rows
        ]
    
    async def get_query_fingerprint_history(
        self,
        session: AsyncSession,
        fingerprint: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get audit history for a specific query fingerprint
        Useful for tracking similar queries over time
        """
        sql = text("""
            SELECT 
                timestamp, user_id, success, execution_time_ms, row_count,
                llm_provider, llm_model
            FROM audit_log
            WHERE query_fingerprint = :fingerprint
            ORDER BY timestamp DESC
            LIMIT :limit
        """)
        
        result = await session.execute(sql, {
            'fingerprint': fingerprint,
            'limit': limit
        })
        rows = result.fetchall()
        
        return [
            {
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "user_id": row.user_id,
                "success": row.success,
                "execution_time_ms": row.execution_time_ms,
                "row_count": row.row_count,
                "llm_provider": row.llm_provider,
                "llm_model": row.llm_model,
            }
            for row in rows
        ]
    
    async def generate_summary(
        self,
        session: Optional[AsyncSession] = None,
        date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Generate daily audit summary statistics.
        If session is provided, uses SQLAlchemy. Otherwise uses internal clients.
        """
        if session:
            return await self._generate_summary_session(session, date)
        else:
            return await self._generate_summary_internal(date)

    async def _generate_summary_internal(
        self,
        date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Generate summary using internal clients"""
        date = date or datetime.now(timezone.utc)
        date_str = date.strftime('%Y-%m-%d')
        
        rows = []
        
        if self.db_type == DatabaseType.POSTGRES:
            await postgres_client.initialize()
            sql = f"""
                SELECT 
                    action,
                    COUNT(*) as count,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as error_count,
                    AVG(execution_time_ms) as avg_execution_time,
                    SUM(row_count) as total_rows
                FROM audit_log
                WHERE DATE(timestamp) = '{date_str}'
                GROUP BY action
            """
            
            try:
                # Direct cursor usage since execute_query returns dict, we want tuples to match logic below or dicts
                # execute_query returns dict with 'rows' as list of lists.
                res = await postgres_client.execute_query(
                    sql, 
                    user_id="system", 
                    request_id="audit_summary_gen",
                    timeout=30
                )
                
                # Convert list of lists to list of dicts or objects
                # execute_query returns rows as list of lists, columns in 'columns'
                columns = res.get("columns", [])
                data = res.get("rows", [])
                
                # Transform to object-like for compatibility or just dicts
                for r in data:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col] = r[i]
                    rows.append(row_dict)
                    
            except Exception as e:
                logger.error(f"Internal Postgres summary gen failed: {e}")
                
        else: # Doris
            await doris_client.initialize()
            sql = f"""
                SELECT 
                    action,
                    COUNT(*) as count,
                    SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) as error_count,
                    AVG(execution_time_ms) as avg_execution_time,
                    SUM(row_count) as total_rows
                FROM audit_log_doris
                WHERE to_date(timestamp) = '{date_str}'
                GROUP BY action
            """
            
            res = await doris_client.execute_sql(sql)
            if res.get("status") == "success":
                data = res.get("results", {})
                r_rows = data.get("rows", [])
                r_cols = data.get("columns", [])
                
                for r in r_rows:
                    row_dict = {}
                    # Handle tuple/list
                    r_data = r if isinstance(r, (list, tuple)) else []
                    for i, col in enumerate(r_cols):
                        row_dict[col] = r_data[i]
                    rows.append(row_dict)

        return {
            "date": date_str,
            "actions": [
                {
                    "action": row.get("action"),
                    "count": row.get("count"),
                    "success_count": row.get("success_count"),
                    "error_count": row.get("error_count"),
                    "avg_execution_time_ms": round(float(row.get("avg_execution_time") or 0), 2),
                    "total_rows": int(row.get("total_rows") or 0)
                }
                for row in rows
            ]
        }

    async def _generate_summary_session(
        self,
        session: AsyncSession,
        date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        date = date or datetime.now(timezone.utc)
        date_str = date.strftime('%Y-%m-%d')
        
        sql = text("""
            SELECT 
                action,
                COUNT(*) as count,
                SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as error_count,
                AVG(execution_time_ms) as avg_execution_time,
                SUM(row_count) as total_rows
            FROM audit_log
            WHERE DATE(timestamp) = :date
            GROUP BY action
        """)
        
        result = await session.execute(sql, {'date': date_str})
        rows = result.fetchall()
        
        return {
            "date": date_str,
            "actions": [
                {
                    "action": row.action,
                    "count": row.count,
                    "success_count": row.success_count,
                    "error_count": row.error_count,
                    "avg_execution_time_ms": round(row.avg_execution_time, 2) if row.avg_execution_time else 0,
                    "total_rows": row.total_rows or 0
                }
                for row in rows
            ]
        }


# Global instance
native_audit_service = NativeAuditService()


# Convenience functions

async def audit_to_native_db(
    action: str,
    user_id: str,
    db_session: AsyncSession,
    **kwargs
) -> bool:
    """
    Convenience function to write audit entry to native database
    
    Usage:
        await audit_to_native_db(
            action="query.execute",
            user_id="user@example.com",
            db_session=session,
            success=True,
            execution_time_ms=1500,
            details={"sql_query": "SELECT * FROM ..."}
        )
    """
    return await native_audit_service.write_audit_entry(
        action=action,
        user_id=user_id,
        db_session=db_session,
        **kwargs
    )
