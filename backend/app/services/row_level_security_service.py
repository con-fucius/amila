"""
Row-Level Security (RLS) Service

Enforces row-level access controls based on user attributes, roles, and policies.
Dynamically injects WHERE clauses to filter data based on user permissions.

Features:
- Policy-based row filtering
- User attribute matching
- Department/team-based isolation
- Time-based access restrictions
- Audit logging for policy enforcement

Compliance: GDPR, HIPAA, SOC2
"""

import logging
import re
from typing import Dict, Any, List, Optional, Set, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.core.config import settings

from app.services.rls_policy_templates import (
    RLSPolicyTemplates,
    RLSPolicyTemplate,
    RLSPolicyType as TemplatePolicyType
)

from app.core.audit import audit_logger, AuditAction, AuditSeverity

logger = logging.getLogger(__name__)


class RLSPolicyType(str, Enum):
    """Types of RLS policies"""
    USER_OWNED = "user_owned"  # Rows owned by the user
    DEPARTMENT = "department"  # Rows belonging to user's department
    TEAM = "team"  # Rows belonging to user's team
    ROLE_BASED = "role_based"  # Based on user role
    TIME_RESTRICTED = "time_restricted"  # Time-based access
    CUSTOM = "custom"  # Custom predicate


class RLSEnforcementLevel(str, Enum):
    """Enforcement levels for RLS"""
    STRICT = "strict"  # Always enforce, fail if no policy
    PERMISSIVE = "permissive"  # Allow if no policy matches
    AUDIT_ONLY = "audit_only"  # Log but don't enforce


@dataclass
class RLSPolicy:
    """Row-level security policy definition"""
    policy_id: str
    table_name: str
    policy_type: RLSPolicyType
    column_name: Optional[str]  # Column to match against
    predicate: Optional[str]  # Custom SQL predicate
    user_attributes: Dict[str, Any] = field(default_factory=dict)  # Required user attrs
    enforcement_level: RLSEnforcementLevel = RLSEnforcementLevel.STRICT
    enabled: bool = True
    created_at: str = ""


@dataclass
class RLSContext:
    """Context for RLS enforcement"""
    user_id: str
    user_role: str
    department: Optional[str] = None
    team: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RLSEnforcementResult:
    """Result of RLS enforcement"""
    original_sql: str
    modified_sql: str
    policies_applied: List[str]
    predicates_added: List[str]
    enforced: bool
    reason: str


class RowLevelSecurityService:
    """
    Service for enforcing row-level security policies.
    
    Dynamically modifies SQL queries to enforce row-level access controls
    based on user attributes and defined policies.
    """
    
    # Redis key prefixes
    RLS_POLICY_PREFIX = "rls:policy:"
    RLS_AUDIT_PREFIX = "rls:audit:"
    
    # Default policies cache
    _default_policies: Dict[str, List[RLSPolicy]] = {}
    
    @classmethod
    async def create_policy(
        cls,
        table_name: str,
        policy_type: RLSPolicyType,
        column_name: Optional[str] = None,
        predicate: Optional[str] = None,
        user_attributes: Optional[Dict[str, Any]] = None,
        enforcement_level: RLSEnforcementLevel = RLSEnforcementLevel.STRICT
    ) -> RLSPolicy:
        """
        Create a new RLS policy.
        
        Args:
            table_name: Table to apply policy to
            policy_type: Type of policy
            column_name: Column to match (for attribute-based policies)
            predicate: Custom SQL predicate (for custom policies)
            user_attributes: Required user attributes
            enforcement_level: How strictly to enforce
            
        Returns:
            Created policy
        """
        policy_id = f"{table_name}_{policy_type.value}_{datetime.now(timezone.utc).timestamp()}"
        
        policy = RLSPolicy(
            policy_id=policy_id,
            table_name=table_name.upper(),
            policy_type=policy_type,
            column_name=column_name,
            predicate=predicate,
            user_attributes=user_attributes or {},
            enforcement_level=enforcement_level,
            created_at=datetime.now(timezone.utc).isoformat()
        )
        
        # Store in Redis
        policy_key = f"{cls.RLS_POLICY_PREFIX}{table_name}:{policy_id}"
        await redis_client.set(policy_key, {
            "policy_id": policy_id,
            "table_name": policy.table_name,
            "policy_type": policy_type.value,
            "column_name": column_name,
            "predicate": predicate,
            "user_attributes": user_attributes or {},
            "enforcement_level": enforcement_level.value,
            "enabled": True,
            "created_at": policy.created_at
        })
        
        # Add to table index
        table_key = f"{cls.RLS_POLICY_PREFIX}table:{table_name.upper()}"
        await redis_client._client.sadd(table_key, policy_id)
        
        logger.info(f"Created RLS policy {policy_id} for table {table_name}")
        return policy
    
    @classmethod
    async def get_policies_for_table(cls, table_name: str) -> List[RLSPolicy]:
        """Get all policies for a table"""
        table_key = f"{cls.RLS_POLICY_PREFIX}table:{table_name.upper()}"
        
        try:
            policy_ids = await redis_client._client.smembers(table_key)
            policies = []
            
            for pid in policy_ids:
                policy_id = pid.decode() if isinstance(pid, bytes) else pid
                policy_key = f"{cls.RLS_POLICY_PREFIX}{table_name}:{policy_id}"
                data = await redis_client.get(policy_key)
                
                if data and data.get("enabled"):
                    policies.append(RLSPolicy(
                        policy_id=data["policy_id"],
                        table_name=data["table_name"],
                        policy_type=RLSPolicyType(data["policy_type"]),
                        column_name=data.get("column_name"),
                        predicate=data.get("predicate"),
                        user_attributes=data.get("user_attributes", {}),
                        enforcement_level=RLSEnforcementLevel(data.get("enforcement_level", "strict")),
                        enabled=data.get("enabled", True),
                        created_at=data.get("created_at", "")
                    ))
            
            return policies
        except Exception as e:
            logger.error(f"Failed to get policies for {table_name}: {e}")
            return []
    
    @classmethod
    def _build_predicate(
        cls,
        policy: RLSPolicy,
        context: RLSContext
    ) -> Optional[str]:
        """Build SQL predicate for a policy based on context"""
        
        if policy.policy_type == RLSPolicyType.USER_OWNED:
            if not policy.column_name:
                return None
            return f"{policy.column_name} = '{context.user_id}'"
        
        elif policy.policy_type == RLSPolicyType.DEPARTMENT:
            if not policy.column_name or not context.department:
                return None
            return f"{policy.column_name} = '{context.department}'"
        
        elif policy.policy_type == RLSPolicyType.TEAM:
            if not policy.column_name or not context.team:
                return None
            return f"{policy.column_name} = '{context.team}'"
        
        elif policy.policy_type == RLSPolicyType.ROLE_BASED:
            # Check if user has required role
            if context.user_role in policy.user_attributes.get("allowed_roles", []):
                # Admin roles bypass RLS
                return None
            if not policy.column_name:
                return None
            return f"{policy.column_name} IN ('{context.user_role}', 'public')"
        
        elif policy.policy_type == RLSPolicyType.TIME_RESTRICTED:
            # Time-based restrictions (e.g., only recent data)
            lookback_days = policy.user_attributes.get("lookback_days", 30)
            return f"created_at >= CURRENT_DATE - {lookback_days}"
        
        elif policy.policy_type == RLSPolicyType.CUSTOM:
            # Custom predicate with variable substitution
            if policy.predicate:
                predicate = policy.predicate
                # Substitute context variables
                predicate = predicate.replace("{{user_id}}", f"'{context.user_id}'")
                predicate = predicate.replace("{{department}}", f"'{context.department or ''}'")
                predicate = predicate.replace("{{team}}", f"'{context.team or ''}'")
                predicate = predicate.replace("{{role}}", f"'{context.user_role}'")
                return predicate
            return None
        
        return None
    
    @classmethod
    def _extract_tables_from_sql(cls, sql: str) -> Set[str]:
        """
        Extract table names from SQL query more robustly.
        Handles FROM, JOIN, and basic aliases.
        """
        tables = set()
        # Remove comments and strings to avoid false positives
        sql_clean = re.sub(r'--.*?\n', ' ', sql)
        sql_clean = re.sub(r'/\*.*?\*/', ' ', sql_clean, flags=re.DOTALL)
        sql_clean = re.sub(r"'.*?'", "''", sql_clean)
        
        sql_upper = sql_clean.upper()
        
        # Match FROM and JOIN clauses, ignoring subqueries and common keywords
        # Matches patterns like FROM table_name, FROM table_name AS alias, JOIN table_name
        pattern = r'\b(?:FROM|JOIN)\s+([A-Z0-9_"]+)(?:\s+(?:AS\s+)?([A-Z0-9_"]+))?'
        matches = re.finditer(pattern, sql_upper)
        
        for match in matches:
            tname = match.group(1).replace('"', '')
            # Filter out subquery placeholders or common false positives
            if tname not in ('SELECT', 'TABLE', 'VALUES', 'DUAL'):
                tables.add(tname)
        
        return tables

    @classmethod
    def _validate_query_safety(cls, sql: str) -> Tuple[bool, str]:
        """
        Validate if the query has structures that might bypass RLS.
        Detects CTEs, massive nesting, or unions that need careful handling.
        """
        sql_upper = sql.upper()
        
        # Check for CTEs (WITH clause) - RLS might not be easily injected into all branches
        if "WITH " in sql_upper:
            return False, "CTEs (WITH clause) are currently restricted under RLS"
            
        # Check for UNION - needs RLS on both sides
        if " UNION " in sql_upper:
            # We can handle unions if we inject into each part, but for now we restrict it
            # if the logic is too complex for simple string replacement.
            return True, "UNION detected, requiring multi-site injection"
            
        return True, "Safe"
    
    @classmethod
    async def enforce_rls(
        cls,
        sql: str,
        context: RLSContext,
        database_type: str = "oracle"
    ) -> RLSEnforcementResult:
        """
        Enforce RLS policies on a SQL query with safety validation.
        """
        original_sql = sql
        
        # 1. Basic safety validation
        safe, reason = cls._validate_query_safety(sql)
        if not safe:
            await cls._audit_enforcement(context, original_sql, sql, [], severity=AuditSeverity.WARNING, success=False, reason=reason)
            return RLSEnforcementResult(
                original_sql=original_sql,
                modified_sql=sql,
                policies_applied=[],
                predicates_added=[],
                enforced=False,
                reason=f"Safety check failed: {reason}"
            )
            
        tables = cls._extract_tables_from_sql(sql)
        
        if not tables:
            return RLSEnforcementResult(
                original_sql=original_sql,
                modified_sql=sql,
                policies_applied=[],
                predicates_added=[],
                enforced=False,
                reason="No tables found in query"
            )
        
        # Admin bypass
        if context.user_role.lower() == "admin":
            return RLSEnforcementResult(
                original_sql=original_sql,
                modified_sql=sql,
                policies_applied=[],
                predicates_added=[],
                enforced=False,
                reason="Admin role bypasses RLS"
            )
        
        policies_applied = []
        predicates_added = []
        
        for table in tables:
            policies = await cls.get_policies_for_table(table)
            
            for policy in policies:
                if not policy.enabled:
                    continue
                
                predicate = cls._build_predicate(policy, context)
                
                if predicate:
                    # Add predicate to query
                    sql = cls._add_predicate_to_query(sql, table, predicate, database_type)
                    policies_applied.append(policy.policy_id)
                    predicates_added.append(predicate)
                    
                    logger.debug(f"Applied RLS policy {policy.policy_id} to {table}")
        
        # Audit log
        await cls._audit_enforcement(context, original_sql, sql, policies_applied, success=True)
        
        return RLSEnforcementResult(
            original_sql=original_sql,
            modified_sql=sql,
            policies_applied=policies_applied,
            predicates_added=predicates_added,
            enforced=len(policies_applied) > 0,
            reason=f"Applied {len(policies_applied)} policies"
        )
    
    @classmethod
    def _add_predicate_to_query(
        cls,
        sql: str,
        table_name: str,
        predicate: str,
        database_type: str
    ) -> str:
        """Add a predicate to the SQL query for the specified table"""
        
        # Simple approach: Add to WHERE clause or create one
        sql_upper = sql.upper()
        
        # Check if query already has WHERE
        if "WHERE" in sql_upper:
            # Find the WHERE clause and add AND condition
            # This is a simplified approach - production would use SQL parsing
            where_idx = sql_upper.find("WHERE")
            before_where = sql[:where_idx + 5]
            after_where = sql[where_idx + 5:]
            
            # Add predicate with AND
            return f"{before_where} ({predicate}) AND {after_where}"
        else:
            # Add WHERE clause before ORDER BY, GROUP BY, or end
            order_idx = sql_upper.find("ORDER BY")
            group_idx = sql_upper.find("GROUP BY")
            
            insert_idx = len(sql)
            if order_idx > 0:
                insert_idx = order_idx
            elif group_idx > 0:
                insert_idx = group_idx
            
            before = sql[:insert_idx]
            after = sql[insert_idx:]
            
            return f"{before} WHERE {predicate} {after}"
    
    @classmethod
    async def _audit_enforcement(
        cls,
        context: RLSContext,
        original_sql: str,
        modified_sql: str,
        policies_applied: List[str],
        severity: AuditSeverity = AuditSeverity.INFO,
        success: bool = True,
        reason: Optional[str] = None
    ):
        """Log RLS enforcement for audit using centralized audit system"""
        try:
            details = {
                "policies_applied": policies_applied,
                "original_sql_hash": hashlib.sha256(original_sql.encode()).hexdigest()[:16],
                "modified": original_sql != modified_sql,
                "tables_detected": list(cls._extract_tables_from_sql(original_sql))
            }
            if reason:
                details["reason"] = reason

            await audit_logger.log(
                action=AuditAction.RLS_ENFORCEMENT,
                user=context.user_id,
                user_role=context.user_role,
                severity=severity,
                success=success,
                resource="rls_service",
                details=details
            )
        except Exception as e:
            logger.warning(f"Failed to audit RLS enforcement: {e}")
    
    @classmethod
    async def initialize_default_policies(cls):
        """Initialize default RLS policies for common tables"""
        
        # Example: User data policy
        try:
            await cls.create_policy(
                table_name="USER_DATA",
                policy_type=RLSPolicyType.USER_OWNED,
                column_name="owner_id",
                enforcement_level=RLSEnforcementLevel.STRICT
            )
            
            await cls.create_policy(
                table_name="EMPLOYEES",
                policy_type=RLSPolicyType.DEPARTMENT,
                column_name="department_id",
                enforcement_level=RLSEnforcementLevel.PERMISSIVE
            )
            
            logger.info("Initialized default RLS policies")
        except Exception as e:
            logger.warning(f"Failed to initialize default policies: {e}")
    
    # RLS Policy Template Integration Methods
    
    @classmethod
    async def suggest_policy_from_template(
        cls,
        table_name: str,
        table_columns: List[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Suggest an RLS policy based on table columns using templates.
        
        Args:
            table_name: Name of the table
            table_columns: List of column names
            
        Returns:
            Suggested policy configuration or None
        """
        try:
            template_name = RLSPolicyTemplates.get_template_for_table(table_name, table_columns)
            
            if not template_name:
                logger.info(f"No template suggestion for table {table_name}")
                return None
            
            templates = RLSPolicyTemplates.get_all_templates()
            template = templates.get(template_name)
            
            if not template:
                return None
            
            # Map template policy type to service policy type
            policy_type_map = {
                TemplatePolicyType.USER_OWNED: RLSPolicyType.USER_OWNED,
                TemplatePolicyType.DEPARTMENT: RLSPolicyType.DEPARTMENT,
                TemplatePolicyType.TEAM: RLSPolicyType.TEAM,
                TemplatePolicyType.ROLE_BASED: RLSPolicyType.ROLE_BASED,
                TemplatePolicyType.TIME_RESTRICTED: RLSPolicyType.TIME_RESTRICTED,
                TemplatePolicyType.HIERARCHY: RLSPolicyType.CUSTOM,
                TemplatePolicyType.CUSTOM: RLSPolicyType.CUSTOM,
            }
            
            # Get the primary column from template
            primary_column = None
            if template.column_mapping:
                primary_column = list(template.column_mapping.values())[0]
            
            return {
                "template_name": template_name,
                "policy_type": policy_type_map.get(template.policy_type, RLSPolicyType.CUSTOM),
                "description": template.description,
                "column_name": primary_column,
                "user_attributes_required": template.user_attributes_required,
                "examples": template.examples,
                "sql_template": template.sql_template
            }
            
        except Exception as e:
            logger.error(f"Failed to suggest policy from template: {e}")
            return None
    
    @classmethod
    async def create_policy_from_template(
        cls,
        table_name: str,
        template_name: str,
        custom_columns: Optional[Dict[str, str]] = None,
        enforcement_level: RLSEnforcementLevel = RLSEnforcementLevel.STRICT
    ) -> Optional[RLSPolicy]:
        """
        Create an RLS policy from a template.
        
        Args:
            table_name: Table to apply policy to
            template_name: Name of the template to use
            custom_columns: Optional custom column mappings
            enforcement_level: How strictly to enforce
            
        Returns:
            Created policy or None
        """
        try:
            templates = RLSPolicyTemplates.get_all_templates()
            template = templates.get(template_name)
            
            if not template:
                logger.warning(f"Template '{template_name}' not found")
                return None
            
            # Map template policy type
            policy_type_map = {
                TemplatePolicyType.USER_OWNED: RLSPolicyType.USER_OWNED,
                TemplatePolicyType.DEPARTMENT: RLSPolicyType.DEPARTMENT,
                TemplatePolicyType.TEAM: RLSPolicyType.TEAM,
                TemplatePolicyType.ROLE_BASED: RLSPolicyType.ROLE_BASED,
                TemplatePolicyType.TIME_RESTRICTED: RLSPolicyType.TIME_RESTRICTED,
                TemplatePolicyType.HIERARCHY: RLSPolicyType.CUSTOM,
                TemplatePolicyType.CUSTOM: RLSPolicyType.CUSTOM,
            }
            
            policy_type = policy_type_map.get(template.policy_type, RLSPolicyType.CUSTOM)
            
            # Get column name from template or custom mapping
            column_name = None
            if custom_columns and template.column_mapping:
                # Use custom mapping if provided
                primary_key = list(template.column_mapping.keys())[0]
                column_name = custom_columns.get(primary_key) or list(template.column_mapping.values())[0]
            elif template.column_mapping:
                column_name = list(template.column_mapping.values())[0]
            
            # For custom/hierarchy templates, store the SQL template
            predicate = None
            if policy_type == RLSPolicyType.CUSTOM and template.sql_template:
                predicate = template.sql_template.replace("{table}", table_name)
            
            # Create the policy
            policy = await cls.create_policy(
                table_name=table_name,
                policy_type=policy_type,
                column_name=column_name,
                predicate=predicate,
                user_attributes={
                    "template_name": template_name,
                    "user_attributes_required": template.user_attributes_required,
                    "description": template.description
                },
                enforcement_level=enforcement_level
            )
            
            logger.info(f"Created policy from template '{template_name}' for table '{table_name}'")
            return policy
            
        except Exception as e:
            logger.error(f"Failed to create policy from template: {e}")
            return None
    
    @classmethod
    async def auto_apply_template_policies(
        cls,
        table_name: str,
        table_columns: List[str],
        auto_create: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Automatically detect and optionally apply template policies for a table.
        
        Args:
            table_name: Name of the table
            table_columns: List of column names
            auto_create: Whether to automatically create the suggested policy
            
        Returns:
            List of suggested/applied policies
        """
        results = []
        
        try:
            suggestion = await cls.suggest_policy_from_template(table_name, table_columns)
            
            if suggestion:
                result = {
                    "table_name": table_name,
                    "suggested": True,
                    "template_name": suggestion["template_name"],
                    "policy_type": suggestion["policy_type"].value,
                    "description": suggestion["description"],
                    "column_name": suggestion["column_name"],
                    "created": False
                }
                
                if auto_create:
                    policy = await cls.create_policy_from_template(
                        table_name=table_name,
                        template_name=suggestion["template_name"]
                    )
                    if policy:
                        result["created"] = True
                        result["policy_id"] = policy.policy_id
                
                results.append(result)
            else:
                results.append({
                    "table_name": table_name,
                    "suggested": False,
                    "reason": "No matching template found"
                })
                
        except Exception as e:
            logger.error(f"Failed to auto-apply template policies: {e}")
            results.append({
                "table_name": table_name,
                "suggested": False,
                "error": str(e)
            })
        
        return results
    
    @classmethod
    def get_available_templates(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get all available RLS policy templates with descriptions.
        
        Returns:
            Dictionary of template names to template info
        """
        try:
            templates = RLSPolicyTemplates.get_all_templates()
            return {
                name: {
                    "name": template.name,
                    "policy_type": template.policy_type.value,
                    "description": template.description,
                    "column_mapping": template.column_mapping,
                    "user_attributes_required": template.user_attributes_required,
                    "examples": template.examples
                }
                for name, template in templates.items()
            }
        except Exception as e:
            logger.error(f"Failed to get available templates: {e}")
            return {}
    
    @classmethod
    async def get_audit_log(
        cls,
        user_id: Optional[str] = None,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """Get RLS enforcement audit log"""
        logs = []
        
        try:
            if user_id:
                pattern = f"{cls.RLS_AUDIT_PREFIX}{user_id}:*"
            else:
                pattern = f"{cls.RLS_AUDIT_PREFIX}*"
            
            # Scan for keys (simplified)
            # In production, use proper date range scanning
            
        except Exception as e:
            logger.error(f"Failed to get audit log: {e}")
        
        return logs


# Global instance
row_level_security_service = RowLevelSecurityService()


# Convenience functions

async def enforce_row_level_security(
    sql: str,
    user_id: str,
    user_role: str,
    department: Optional[str] = None,
    team: Optional[str] = None,
    database_type: str = "oracle"
) -> RLSEnforcementResult:
    """Convenience function to enforce RLS"""
    context = RLSContext(
        user_id=user_id,
        user_role=user_role,
        department=department,
        team=team
    )
    return await RowLevelSecurityService.enforce_rls(sql, context, database_type)


async def create_rls_policy(
    table_name: str,
    policy_type: str,
    column_name: Optional[str] = None,
    predicate: Optional[str] = None
) -> RLSPolicy:
    """Convenience function to create policy"""
    return await RowLevelSecurityService.create_policy(
        table_name=table_name,
        policy_type=RLSPolicyType(policy_type),
        column_name=column_name,
        predicate=predicate
    )