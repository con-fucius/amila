"""
Adaptive HITL (Human-in-the-Loop) Service

Learns user approval patterns to reduce unnecessary interruptions.
Tracks which query types users consistently approve and auto-approves similar queries.

Features:
- User pattern analysis for approval decisions
- Similarity-based query matching to previously approved queries
- Anomaly detection for unusual patterns requiring confirmation
- Configurable confidence thresholds for auto-approval
"""

import logging
import hashlib
import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class ApprovalDecisionType(str, Enum):
    """Types of approval decisions"""
    AUTO_APPROVE = "auto_approve"
    NEEDS_APPROVAL = "needs_approval"
    BLOCK = "block"


class RiskLevel(str, Enum):
    """Query risk levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ApprovalDecision:
    """Result of an approval decision check"""
    decision: ApprovalDecisionType
    confidence: float
    reason: str
    similar_approved_count: int = 0
    user_pattern_score: float = 0.0
    is_unusual_pattern: bool = False


@dataclass
class ApprovalRecord:
    """Record of a user approval decision"""
    record_id: str
    user_id: str
    query_pattern: str
    sql_query: str
    sql_hash: str
    risk_level: str
    approved: bool
    rejection_reason: Optional[str]
    created_at: str
    schema_fingerprint: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class UserApprovalPattern:
    """Aggregated approval patterns for a user"""
    user_id: str
    total_decisions: int
    approval_rate: float
    preferred_tables: List[str]
    common_query_patterns: List[str]
    avg_query_complexity: float
    last_updated: str


class AdaptiveHITLService:
    """
    Service for adaptive HITL approval decisions.
    
    Learns from user approval history to:
    - Auto-approve queries similar to previously approved ones
    - Flag unusual patterns that deviate from user behavior
    - Build user-specific approval profiles
    """
    
    # Redis key prefixes
    APPROVAL_RECORDS_PREFIX = "hitl:approval:"
    USER_PATTERNS_PREFIX = "hitl:pattern:"
    SIMILAR_QUERIES_PREFIX = "hitl:similar:"
    
    # Configuration
    DEFAULT_AUTO_APPROVE_THRESHOLD = 0.85
    MIN_SIMILAR_APPROVALS = 3
    PATTERN_LOOKBACK_DAYS = 30
    
    @classmethod
    def _generate_sql_hash(cls, sql: str) -> str:
        """Generate a normalized hash of the SQL query"""
        # Normalize SQL for comparison
        normalized = sql.lower().strip()
        normalized = re.sub(r'\s+', ' ', normalized)  # Normalize whitespace
        normalized = re.sub(r'\$\d+', '?', normalized)  # Normalize bind variables
        normalized = re.sub(r"'[^']*'", '?', normalized)  # Normalize string literals
        normalized = re.sub(r'\d+', '?', normalized)  # Normalize numbers
        return hashlib.sha256(normalized.encode()).hexdigest()[:32]
    
    @classmethod
    def _extract_tables(cls, sql: str) -> List[str]:
        """Extract table names from SQL query"""
        tables = []
        sql_upper = sql.upper()
        
        # Match FROM clauses
        from_matches = re.findall(r'FROM\s+(\w+)', sql_upper)
        tables.extend(from_matches)
        
        # Match JOIN clauses
        join_matches = re.findall(r'JOIN\s+(\w+)', sql_upper)
        tables.extend(join_matches)
        
        return list(set(tables))  # Remove duplicates
    
    @classmethod
    def _extract_query_pattern(cls, sql: str) -> str:
        """Extract a generalized pattern from SQL for matching"""
        sql_upper = sql.upper()
        
        # Extract operation type
        if sql_upper.startswith('SELECT'):
            op = 'SELECT'
        elif sql_upper.startswith('INSERT'):
            op = 'INSERT'
        elif sql_upper.startswith('UPDATE'):
            op = 'UPDATE'
        elif sql_upper.startswith('DELETE'):
            op = 'DELETE'
        else:
            op = 'OTHER'
        
        # Extract tables
        tables = cls._extract_tables(sql)
        
        # Extract aggregation type
        has_agg = any(agg in sql_upper for agg in ['SUM(', 'AVG(', 'COUNT(', 'MAX(', 'MIN('])
        has_group = 'GROUP BY' in sql_upper
        has_join = 'JOIN' in sql_upper
        
        pattern_parts = [op]
        if tables:
            pattern_parts.append(f"FROM:{','.join(sorted(tables))}")
        if has_agg:
            pattern_parts.append("AGG")
        if has_group:
            pattern_parts.append("GROUP")
        if has_join:
            pattern_parts.append("JOIN")
        
        return "|".join(pattern_parts)
    
    @classmethod
    def _calculate_similarity(cls, sql1: str, sql2: str) -> float:
        """
        Calculate similarity between two SQL queries (0.0 to 1.0).
        Uses a combination of structural and token-based similarity.
        """
        # Pattern-based similarity
        pattern1 = cls._extract_query_pattern(sql1)
        pattern2 = cls._extract_query_pattern(sql2)
        
        if pattern1 == pattern2:
            return 1.0
        
        # Jaccard similarity on words
        words1 = set(sql1.lower().split())
        words2 = set(sql2.lower().split())
        
        intersection = words1 & words2
        union = words1 | words2
        
        if not union:
            return 0.0
        
        jaccard = len(intersection) / len(union)
        
        # Table overlap
        tables1 = set(cls._extract_tables(sql1))
        tables2 = set(cls._extract_tables(sql2))
        
        if tables1 and tables2:
            table_overlap = len(tables1 & tables2) / max(len(tables1), len(tables2))
        else:
            table_overlap = 0.0
        
        # Weighted combination
        similarity = (jaccard * 0.6) + (table_overlap * 0.4)
        
        return round(similarity, 2)
    
    @classmethod
    async def record_approval_decision(
        cls,
        user_id: str,
        sql_query: str,
        risk_level: RiskLevel,
        approved: bool,
        rejection_reason: Optional[str] = None,
        schema_fingerprint: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Record a user's approval decision for learning.
        
        Args:
            user_id: User identifier
            sql_query: The SQL query
            risk_level: Risk level of the query
            approved: Whether the user approved
            rejection_reason: Reason for rejection (if rejected)
            schema_fingerprint: Schema context hash
            metadata: Additional metadata
            
        Returns:
            record_id
        """
        record_id = f"{user_id}:{datetime.now(timezone.utc).timestamp()}"
        
        record = ApprovalRecord(
            record_id=record_id,
            user_id=user_id,
            query_pattern=cls._extract_query_pattern(sql_query),
            sql_query=sql_query[:1000],  # Truncate for storage
            sql_hash=cls._generate_sql_hash(sql_query),
            risk_level=risk_level.value,
            approved=approved,
            rejection_reason=rejection_reason,
            created_at=datetime.now(timezone.utc).isoformat(),
            schema_fingerprint=schema_fingerprint,
            metadata=metadata
        )
        
        try:
            # Store the record
            key = f"{cls.APPROVAL_RECORDS_PREFIX}{record_id}"
            await redis_client.set(key, asdict(record), ttl=cls.PATTERN_LOOKBACK_DAYS * 24 * 3600)
            
            # Add to user's approval history
            user_key = f"{cls.APPROVAL_RECORDS_PREFIX}user:{user_id}"
            await redis_client._client.lpush(user_key, record_id)
            await redis_client._client.ltrim(user_key, 0, 999)  # Keep last 1000
            await redis_client._client.expire(user_key, cls.PATTERN_LOOKBACK_DAYS * 24 * 3600)
            
            # Add to SQL pattern index
            pattern_key = f"{cls.SIMILAR_QUERIES_PREFIX}{record.sql_hash}"
            await redis_client._client.sadd(pattern_key, record_id)
            await redis_client._client.expire(pattern_key, cls.PATTERN_LOOKBACK_DAYS * 24 * 3600)
            
            # Update user pattern cache
            await cls._update_user_pattern(user_id)
            
            logger.info(f"Recorded approval decision for user {user_id}: approved={approved}")
            return record_id
            
        except Exception as e:
            logger.error(f"Failed to record approval decision: {e}")
            return record_id
    
    @classmethod
    async def _update_user_pattern(cls, user_id: str):
        """Update aggregated pattern data for a user"""
        try:
            # Get user's recent approval records
            user_key = f"{cls.APPROVAL_RECORDS_PREFIX}user:{user_id}"
            record_ids = await redis_client._client.lrange(user_key, 0, 99)
            
            if not record_ids:
                return
            
            total = 0
            approved_count = 0
            tables_used = {}
            patterns = {}
            
            for rid in record_ids:
                record_data = await redis_client.get(f"{cls.APPROVAL_RECORDS_PREFIX}{rid}")
                if not record_data:
                    continue
                
                total += 1
                if record_data.get("approved"):
                    approved_count += 1
                
                # Track tables
                sql = record_data.get("sql_query", "")
                for table in cls._extract_tables(sql):
                    tables_used[table] = tables_used.get(table, 0) + 1
                
                # Track patterns
                pattern = record_data.get("query_pattern", "")
                if pattern:
                    patterns[pattern] = patterns.get(pattern, 0) + 1
            
            if total == 0:
                return
            
            # Build pattern summary
            pattern_summary = UserApprovalPattern(
                user_id=user_id,
                total_decisions=total,
                approval_rate=approved_count / total,
                preferred_tables=sorted(tables_used.keys(), key=lambda t: tables_used[t], reverse=True)[:10],
                common_query_patterns=sorted(patterns.keys(), key=lambda p: patterns[p], reverse=True)[:5],
                avg_query_complexity=0.0,  # Could calculate from SQL
                last_updated=datetime.now(timezone.utc).isoformat()
            )
            
            # Store pattern
            pattern_key = f"{cls.USER_PATTERNS_PREFIX}{user_id}"
            await redis_client.set(pattern_key, asdict(pattern_summary), ttl=cls.PATTERN_LOOKBACK_DAYS * 24 * 3600)
            
        except Exception as e:
            logger.warning(f"Failed to update user pattern for {user_id}: {e}")
    
    @classmethod
    async def get_user_pattern(cls, user_id: str) -> Optional[UserApprovalPattern]:
        """Get the approval pattern for a user"""
        try:
            pattern_key = f"{cls.USER_PATTERNS_PREFIX}{user_id}"
            data = await redis_client.get(pattern_key)
            
            if data:
                return UserApprovalPattern(**data)
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get user pattern for {user_id}: {e}")
            return None
    
    @classmethod
    async def find_similar_approved_queries(
        cls,
        user_id: str,
        sql_query: str,
        min_similarity: float = 0.85,
        limit: int = 10
    ) -> List[ApprovalRecord]:
        """
        Find similar queries that the user previously approved.
        
        Args:
            user_id: User identifier
            sql_query: SQL query to match
            min_similarity: Minimum similarity threshold
            limit: Max results to return
            
        Returns:
            List of similar approved approval records
        """
        similar = []
        
        try:
            # Get user's approval history
            user_key = f"{cls.APPROVAL_RECORDS_PREFIX}user:{user_id}"
            record_ids = await redis_client._client.lrange(user_key, 0, 199)
            
            for rid in record_ids:
                record_data = await redis_client.get(f"{cls.APPROVAL_RECORDS_PREFIX}{rid}")
                if not record_data:
                    continue
                
                # Only consider approved queries
                if not record_data.get("approved"):
                    continue
                
                # Calculate similarity
                past_sql = record_data.get("sql_query", "")
                similarity = cls._calculate_similarity(sql_query, past_sql)
                
                if similarity >= min_similarity:
                    similar.append((ApprovalRecord(**record_data), similarity))
            
            # Sort by similarity and return top results
            similar.sort(key=lambda x: x[1], reverse=True)
            return [record for record, _ in similar[:limit]]
            
        except Exception as e:
            logger.warning(f"Failed to find similar approved queries: {e}")
            return []
    
    @classmethod
    async def is_unusual_pattern(
        cls,
        user_id: str,
        sql_query: str,
        risk_level: RiskLevel
    ) -> Tuple[bool, float]:
        """
        Check if a query pattern is unusual for this user.
        
        Returns:
            Tuple of (is_unusual, unusual_score)
        """
        try:
            pattern = await cls.get_user_pattern(user_id)
            
            if not pattern or pattern.total_decisions < cls.MIN_SIMILAR_APPROVALS:
                # Not enough history to determine
                return False, 0.0
            
            unusual_score = 0.0
            
            # Check if tables are commonly used
            tables = cls._extract_tables(sql_query)
            for table in tables:
                if table not in pattern.preferred_tables:
                    unusual_score += 0.3
            
            # Check if query pattern is common
            query_pattern = cls._extract_query_pattern(sql_query)
            if query_pattern not in pattern.common_query_patterns:
                unusual_score += 0.3
            
            # Check risk level deviation
            if risk_level == RiskLevel.HIGH and pattern.approval_rate > 0.8:
                # User usually approves low-risk but this is high-risk
                unusual_score += 0.2
            
            is_unusual = unusual_score >= 0.5
            
            return is_unusual, min(unusual_score, 1.0)
            
        except Exception as e:
            logger.warning(f"Failed to check unusual pattern: {e}")
            return False, 0.0
    
    @classmethod
    async def should_request_approval(
        cls,
        sql_query: str,
        user_id: str,
        risk_level: RiskLevel,
        auto_approve_threshold: Optional[float] = None
    ) -> ApprovalDecision:
        """
        Determine if approval should be requested based on user patterns.
        
        Args:
            sql_query: SQL query to check
            user_id: User identifier
            risk_level: Query risk level
            auto_approve_threshold: Override threshold for auto-approval
            
        Returns:
            ApprovalDecision
        """
        threshold = auto_approve_threshold or cls.DEFAULT_AUTO_APPROVE_THRESHOLD
        
        try:
            # Critical risk always requires approval
            if risk_level == RiskLevel.CRITICAL:
                return ApprovalDecision(
                    decision=ApprovalDecisionType.NEEDS_APPROVAL,
                    confidence=1.0,
                    reason="CRITICAL risk query always requires approval",
                    similar_approved_count=0,
                    is_unusual_pattern=False
                )
            
            # Find similar approved queries
            similar_approved = await cls.find_similar_approved_queries(
                user_id, sql_query, min_similarity=threshold
            )
            
            # Check for unusual pattern
            is_unusual, unusual_score = await cls.is_unusual_pattern(user_id, sql_query, risk_level)
            
            # Get user pattern
            user_pattern = await cls.get_user_pattern(user_id)
            
            # Decision logic
            if len(similar_approved) >= cls.MIN_SIMILAR_APPROVALS and not is_unusual:
                # User consistently approves similar queries
                confidence = min(len(similar_approved) / 10, 0.95)
                
                # Check if all similar were same risk level
                same_risk_count = sum(1 for r in similar_approved if r.risk_level == risk_level.value)
                
                if same_risk_count >= cls.MIN_SIMILAR_APPROVALS:
                    return ApprovalDecision(
                        decision=ApprovalDecisionType.AUTO_APPROVE,
                        confidence=confidence,
                        reason=f"User consistently approves similar queries ({len(similar_approved)} similar approved)",
                        similar_approved_count=len(similar_approved),
                        user_pattern_score=user_pattern.approval_rate if user_pattern else 0.0,
                        is_unusual_pattern=False
                    )
            
            if is_unusual:
                return ApprovalDecision(
                    decision=ApprovalDecisionType.NEEDS_APPROVAL,
                    confidence=unusual_score,
                    reason=f"Unusual pattern for this user (score: {unusual_score:.2f}) - requires confirmation",
                    similar_approved_count=len(similar_approved),
                    user_pattern_score=user_pattern.approval_rate if user_pattern else 0.0,
                    is_unusual_pattern=True
                )
            
            # Default: needs approval
            return ApprovalDecision(
                decision=ApprovalDecisionType.NEEDS_APPROVAL,
                confidence=0.5,
                reason="No established pattern for this query type",
                similar_approved_count=len(similar_approved),
                user_pattern_score=user_pattern.approval_rate if user_pattern else 0.0,
                is_unusual_pattern=False
            )
            
        except Exception as e:
            logger.error(f"Error in approval decision: {e}")
            # Fail safe: require approval
            return ApprovalDecision(
                decision=ApprovalDecisionType.NEEDS_APPROVAL,
                confidence=0.0,
                reason=f"Error checking approval patterns: {e}",
                similar_approved_count=0,
                is_unusual_pattern=False
            )
    
    @classmethod
    async def get_user_approval_stats(cls, user_id: str) -> Dict[str, Any]:
        """Get approval statistics for a user"""
        try:
            pattern = await cls.get_user_pattern(user_id)
            
            if not pattern:
                return {
                    "user_id": user_id,
                    "has_history": False,
                    "message": "No approval history found for this user"
                }
            
            # Get recent decisions
            user_key = f"{cls.APPROVAL_RECORDS_PREFIX}user:{user_id}"
            record_ids = await redis_client._client.lrange(user_key, 0, 49)
            
            recent_approved = 0
            recent_rejected = 0
            
            for rid in record_ids[:20]:  # Last 20 decisions
                record_data = await redis_client.get(f"{cls.APPROVAL_RECORDS_PREFIX}{rid}")
                if record_data:
                    if record_data.get("approved"):
                        recent_approved += 1
                    else:
                        recent_rejected += 1
            
            return {
                "user_id": user_id,
                "has_history": True,
                "total_decisions": pattern.total_decisions,
                "overall_approval_rate": round(pattern.approval_rate, 2),
                "recent_approval_rate": round(recent_approved / (recent_approved + recent_rejected), 2) if (recent_approved + recent_rejected) > 0 else 0,
                "preferred_tables": pattern.preferred_tables[:5],
                "common_patterns": pattern.common_query_patterns[:3],
                "recent_decisions": {
                    "approved": recent_approved,
                    "rejected": recent_rejected
                },
                "last_updated": pattern.last_updated
            }
            
        except Exception as e:
            logger.error(f"Failed to get approval stats for {user_id}: {e}")
            return {"user_id": user_id, "error": str(e)}


# Global instance
adaptive_hitl_service = AdaptiveHITLService()


# Convenience functions

async def should_auto_approve(
    sql_query: str,
    user_id: str,
    risk_level: str,
    **kwargs
) -> Tuple[bool, str]:
    """
    Convenience function to check if a query should be auto-approved.
    
    Returns:
        Tuple of (should_auto_approve, reason)
    """
    decision = await AdaptiveHITLService.should_request_approval(
        sql_query=sql_query,
        user_id=user_id,
        risk_level=RiskLevel(risk_level.lower()),
        **kwargs
    )
    
    return decision.decision == ApprovalDecisionType.AUTO_APPROVE, decision.reason


async def record_approval(
    user_id: str,
    sql_query: str,
    risk_level: str,
    approved: bool,
    **kwargs
) -> str:
    """
    Convenience function to record an approval decision.
    
    Returns:
        record_id
    """
    return await AdaptiveHITLService.record_approval_decision(
        user_id=user_id,
        sql_query=sql_query,
        risk_level=RiskLevel(risk_level.lower()),
        approved=approved,
        **kwargs
    )