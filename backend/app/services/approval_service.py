"""
Approval Service
- Stores pending approvals and exposes helper methods
- Handles idempotency keys to prevent duplicate approvals
- Re-assesses risk when SQL is modified
"""

from __future__ import annotations

import logging
import hashlib
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import uuid

from app.core.redis_client import redis_client
from app.core.sql_validator import sql_validator, ValidationResult

logger = logging.getLogger(__name__)


class ApprovalService:
    PREFIX = "approval:pending:"
    IDEMPOTENCY_PREFIX = "approval:idempotency:"
    TTL = 6 * 60 * 60  # 6 hours
    IDEMPOTENCY_TTL = 24 * 60 * 60  # 24 hours for idempotency keys

    @staticmethod
    async def save_pending(query_id: str, context: Dict[str, Any]) -> None:
        """Save a pending approval request with risk assessment."""
        # Generate idempotency key based on query_id and SQL
        sql_query = context.get("sql_query", "")
        idempotency_key = ApprovalService._generate_idempotency_key(query_id, sql_query)

        # Perform initial risk assessment
        risk_assessment = ApprovalService.assess_sql_risk(
            sql_query, context.get("user_role", "analyst")
        )

        payload = {
            "query_id": query_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "context": context,
            "idempotency_key": idempotency_key,
            "risk_assessment": risk_assessment,
            "approval_status": "pending",
            "modified_sql": None,
            "original_sql": sql_query,
        }
        await redis_client.set(
            f"{ApprovalService.PREFIX}{query_id}", payload, ttl=ApprovalService.TTL
        )
        logger.info(
            f"Saved pending approval for query {query_id} with risk level: {risk_assessment.get('risk_level')}"
        )

    @staticmethod
    async def get_pending(query_id: str) -> Optional[Dict[str, Any]]:
        return await redis_client.get(f"{ApprovalService.PREFIX}{query_id}")

    @staticmethod
    async def list_pending(limit: int = 50) -> list[Dict[str, Any]]:
        # naive scan
        keys = await redis_client.keys(f"{ApprovalService.PREFIX}*")
        items = []
        for k in keys[:limit]:
            v = await redis_client.get(k)
            if v:
                items.append(v)
        return items

    @staticmethod
    async def check_idempotency(query_id: str, sql_query: str) -> bool:
        """
        Check if this approval has already been processed.

        Args:
            query_id: The query ID
            sql_query: The SQL query being approved

        Returns:
            True if this is a duplicate approval (already processed), False otherwise
        """
        idempotency_key = ApprovalService._generate_idempotency_key(query_id, sql_query)
        key = f"{ApprovalService.IDEMPOTENCY_PREFIX}{idempotency_key}"

        exists = await redis_client.exists(key)
        if exists:
            logger.warning(f"Duplicate approval detected for query {query_id}")
            return True
        return False

    @staticmethod
    async def mark_approved(
        query_id: str,
        sql_query: str,
        approver: str,
        decision_reason: Optional[str] = None,
        constraints_applied: Optional[list] = None,
    ) -> bool:
        """
        Mark an approval as processed and set idempotency key.

        Args:
            query_id: The query ID
            sql_query: The SQL query that was approved
            approver: Username of the approver
            decision_reason: Explicit reason for the approval decision
            constraints_applied: List of constraints applied (e.g., LIMIT, column restrictions)

        Returns:
            True if successfully marked, False if duplicate
        """
        # Check for duplicate
        if await ApprovalService.check_idempotency(query_id, sql_query):
            return False

        # Set idempotency key
        idempotency_key = ApprovalService._generate_idempotency_key(query_id, sql_query)
        key = f"{ApprovalService.IDEMPOTENCY_PREFIX}{idempotency_key}"

        approval_record = {
            "query_id": query_id,
            "approved_at": datetime.now(timezone.utc).isoformat(),
            "approver": approver,
            "sql_hash": hashlib.sha256(sql_query.encode()).hexdigest()[:16],
            "decision_reason": decision_reason,
            "constraints_applied": constraints_applied or [],
        }

        await redis_client.set(
            key, approval_record, ttl=ApprovalService.IDEMPOTENCY_TTL
        )

        # Update pending record status
        pending = await ApprovalService.get_pending(query_id)
        if pending:
            pending["approval_status"] = "approved"
            pending["approved_at"] = approval_record["approved_at"]
            pending["approver"] = approver
            pending["decision_reason"] = decision_reason
            pending["constraints_applied"] = constraints_applied
            await redis_client.set(
                f"{ApprovalService.PREFIX}{query_id}", pending, ttl=ApprovalService.TTL
            )

        logger.info(
            f"Approval marked for query {query_id} by {approver}"
            + (f" with reason: {decision_reason}" if decision_reason else "")
        )
        return True

    @staticmethod
    def assess_sql_risk(sql_query: str, user_role: str = "analyst") -> Dict[str, Any]:
        """
        Assess the risk level of a SQL query.

        Args:
            sql_query: The SQL query to assess
            user_role: The user's role

        Returns:
            Risk assessment dict with risk_level, requires_approval, warnings, etc.
        """
        validation_result = sql_validator.validate_query(sql_query, user_role)

        return {
            "risk_level": validation_result.risk_level.value,
            "query_type": validation_result.query_type.value,
            "requires_approval": validation_result.requires_approval,
            "is_valid": validation_result.is_valid,
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
            "assessed_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _check_role_based_bypass(user_role: str, risk_level: str) -> bool:
        """
        Check if the user role can bypass approval for the given risk level.
        """
        from app.core.config import settings
        bypass_config = settings.ROLE_BASED_APPROVAL_BYPASS
        
        allowed_risks = bypass_config.get(user_role, [])
        return risk_level.lower() in [r.lower() for r in allowed_risks]

    @staticmethod
    def assess_sql_risk(sql_query: str, user_role: str = "analyst") -> Dict[str, Any]:
        """
        Assess the risk level of a SQL query.

        Args:
            sql_query: The SQL query to assess
            user_role: The user's role

        Returns:
            Risk assessment dict with risk_level, requires_approval, warnings, etc.
        """
        validation_result = sql_validator.validate_query(sql_query, user_role)
        
        requires_approval = validation_result.requires_approval
        
        # Check for role-based bypass
        if requires_approval:
            if ApprovalService._check_role_based_bypass(user_role, validation_result.risk_level.value):
                requires_approval = False
                logger.info(f"Role {user_role} bypassed approval for risk level {validation_result.risk_level.value}")

        return {
            "risk_level": validation_result.risk_level.value,
            "query_type": validation_result.query_type.value,
            "requires_approval": requires_approval,
            "is_valid": validation_result.is_valid,
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
            "assessed_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    async def reassess_modified_sql(
        query_id: str, modified_sql: str, user_role: str = "analyst"
    ) -> Dict[str, Any]:
        """
        Re-assess risk when SQL is modified during approval.

        This is critical for HITL - if user modifies the SQL, we need to
        re-evaluate the risk level before allowing execution.

        Args:
            query_id: The query ID
            modified_sql: The modified SQL query
            user_role: The user's role

        Returns:
            Updated risk assessment with comparison to original
        """
        pending = await ApprovalService.get_pending(query_id)
        if not pending:
            logger.warning(f"No pending approval found for query {query_id}")
            return {
                "error": "No pending approval found",
                "query_id": query_id,
            }

        original_sql = pending.get("original_sql", "")
        original_assessment = pending.get("risk_assessment", {})

        # Assess the modified SQL
        new_assessment = ApprovalService.assess_sql_risk(modified_sql, user_role)

        # Compare risk levels
        risk_changed = new_assessment["risk_level"] != original_assessment.get(
            "risk_level"
        )
        risk_increased = (
            ApprovalService._compare_risk_levels(
                new_assessment["risk_level"],
                original_assessment.get("risk_level", "safe"),
            )
            > 0
        )

        result = {
            "query_id": query_id,
            "original_risk": original_assessment.get("risk_level"),
            "new_risk": new_assessment["risk_level"],
            "risk_changed": risk_changed,
            "risk_increased": risk_increased,
            "new_assessment": new_assessment,
            "sql_modified": modified_sql != original_sql,
            "requires_reapproval": risk_increased,
        }

        # Update pending record with new assessment
        pending["modified_sql"] = modified_sql
        pending["risk_assessment"] = new_assessment
        pending["risk_reassessed_at"] = datetime.now(timezone.utc).isoformat()
        await redis_client.set(
            f"{ApprovalService.PREFIX}{query_id}", pending, ttl=ApprovalService.TTL
        )

        if risk_increased:
            logger.warning(
                f"Risk increased for query {query_id}: "
                f"{original_assessment.get('risk_level')} -> {new_assessment['risk_level']}"
            )

        return result

    @staticmethod
    def _generate_idempotency_key(query_id: str, sql_query: str) -> str:
        """Generate a unique idempotency key for an approval."""
        content = f"{query_id}:{sql_query}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    @staticmethod
    def _compare_risk_levels(level1: str, level2: str) -> int:
        """
        Compare two risk levels.

        Returns:
            -1 if level1 < level2
            0 if level1 == level2
            1 if level1 > level2
        """
        risk_order = ["safe", "low", "medium", "high", "critical"]

        try:
            idx1 = risk_order.index(level1.lower())
            idx2 = risk_order.index(level2.lower())

            if idx1 < idx2:
                return -1
            elif idx1 > idx2:
                return 1
            return 0
        except ValueError:
            return 0
