"""
AD Group Mapping Service

Maps Active Directory groups to database table and column visibility permissions.
Provides enterprise-grade access control integration with AD/LDAP.

Features:
- AD group to table visibility mapping
- Column-level masking based on AD groups
- Hierarchical permission inheritance
- Dynamic permission resolution
- Audit logging

Compliance: GDPR, HIPAA, SOX
"""

import logging
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class PermissionLevel(str, Enum):
    """Permission levels for data access"""
    NONE = "none"  # No access
    MASKED = "masked"  # Access to masked data
    AGGREGATED = "aggregated"  # Access to aggregated data only
    FULL = "full"  # Full access


@dataclass
class TablePermission:
    """Permission for a specific table"""
    table_name: str
    permission_level: PermissionLevel
    row_filter: Optional[str] = None  # SQL predicate for row filtering
    allowed_columns: Optional[List[str]] = None  # None = all columns
    masked_columns: List[str] = field(default_factory=list)  # Columns to mask


@dataclass
class ADGroupMapping:
    """Mapping of AD group to permissions"""
    group_name: str
    display_name: str
    table_permissions: Dict[str, TablePermission] = field(default_factory=dict)
    default_permission: PermissionLevel = PermissionLevel.NONE
    priority: int = 0  # Higher number = higher priority
    enabled: bool = True


@dataclass
class ResolvedPermissions:
    """Resolved permissions for a user"""
    user_id: str
    ad_groups: List[str]
    table_permissions: Dict[str, TablePermission]
    effective_tables: Set[str]  # Tables user can access
    masked_columns: Dict[str, List[str]]  # Columns to mask per table
    row_filters: Dict[str, str]  # Row filters per table


class ADGroupMappingService:
    """
    Service for managing AD group to database permission mappings.
    
    Provides enterprise integration with Active Directory for:
    - Table visibility control
    - Column-level masking
    - Row-level filtering
    """
    
    # Redis key prefixes
    AD_MAPPING_PREFIX = "ad:mapping:"
    AD_GROUP_INDEX = "ad:groups"
    AD_AUDIT_PREFIX = "ad:audit:"
    
    # Default mappings
    DEFAULT_MAPPINGS = {
        "finance_users": {
            "display_name": "Finance Team",
            "priority": 100,
            "tables": {
                "SALES_ORDERS": {"level": "full"},
                "SALES_ITEMS": {"level": "full"},
                "BUDGET": {"level": "full"},
                "EXPENSES": {"level": "full"},
                "EMPLOYEES": {
                    "level": "masked",
                    "masked_columns": ["SSN", "SALARY", "BANK_ACCOUNT"]
                },
            }
        },
        "hr_analysts": {
            "display_name": "HR Analysts",
            "priority": 100,
            "tables": {
                "EMPLOYEES": {"level": "full"},
                "DEPARTMENTS": {"level": "full"},
                "POSITIONS": {"level": "full"},
                "SALARIES": {"level": "full"},
            }
        },
        "sales_managers": {
            "display_name": "Sales Managers",
            "priority": 90,
            "tables": {
                "SALES_ORDERS": {"level": "full"},
                "SALES_ITEMS": {"level": "full"},
                "CUSTOMERS": {"level": "full"},
                "PRODUCTS": {"level": "full"},
            }
        },
        "executives": {
            "display_name": "Executive Team",
            "priority": 200,
            "tables": {
                "*": {"level": "aggregated"}  # Access to all tables, aggregated data only
            }
        },
        "auditors": {
            "display_name": "Audit Team",
            "priority": 150,
            "tables": {
                "*": {"level": "full"}  # Full access for audit
            }
        },
        "developers": {
            "display_name": "Development Team",
            "priority": 50,
            "tables": {
                "*": {"level": "full"}  # Full access for development
            }
        },
    }
    
    @classmethod
    async def initialize_default_mappings(cls):
        """Initialize default AD group mappings"""
        try:
            for group_name, config in cls.DEFAULT_MAPPINGS.items():
                mapping = ADGroupMapping(
                    group_name=group_name,
                    display_name=config["display_name"],
                    priority=config["priority"],
                    table_permissions={}
                )
                
                for table_name, perm_config in config["tables"].items():
                    mapping.table_permissions[table_name] = TablePermission(
                        table_name=table_name,
                        permission_level=PermissionLevel(perm_config["level"]),
                        masked_columns=perm_config.get("masked_columns", [])
                    )
                
                await cls.save_mapping(mapping)
            
            logger.info(f"Initialized {len(cls.DEFAULT_MAPPINGS)} default AD group mappings")
        except Exception as e:
            logger.error(f"Failed to initialize default mappings: {e}")
    
    @classmethod
    async def save_mapping(cls, mapping: ADGroupMapping):
        """Save an AD group mapping"""
        key = f"{cls.AD_MAPPING_PREFIX}{mapping.group_name}"
        
        data = {
            "group_name": mapping.group_name,
            "display_name": mapping.display_name,
            "priority": mapping.priority,
            "enabled": mapping.enabled,
            "default_permission": mapping.default_permission.value,
            "table_permissions": {
                table: {
                    "table_name": perm.table_name,
                    "permission_level": perm.permission_level.value,
                    "row_filter": perm.row_filter,
                    "allowed_columns": perm.allowed_columns,
                    "masked_columns": perm.masked_columns
                }
                for table, perm in mapping.table_permissions.items()
            }
        }
        
        await redis_client.set(key, data)
        await redis_client._client.sadd(cls.AD_GROUP_INDEX, mapping.group_name)
        
        logger.debug(f"Saved AD group mapping for {mapping.group_name}")
    
    @classmethod
    async def get_mapping(cls, group_name: str) -> Optional[ADGroupMapping]:
        """Get mapping for an AD group"""
        key = f"{cls.AD_MAPPING_PREFIX}{group_name}"
        
        try:
            data = await redis_client.get(key)
            if not data:
                return None
            
            return ADGroupMapping(
                group_name=data["group_name"],
                display_name=data.get("display_name", group_name),
                priority=data.get("priority", 0),
                enabled=data.get("enabled", True),
                default_permission=PermissionLevel(data.get("default_permission", "none")),
                table_permissions={
                    table: TablePermission(
                        table_name=perm["table_name"],
                        permission_level=PermissionLevel(perm["permission_level"]),
                        row_filter=perm.get("row_filter"),
                        allowed_columns=perm.get("allowed_columns"),
                        masked_columns=perm.get("masked_columns", [])
                    )
                    for table, perm in data.get("table_permissions", {}).items()
                }
            )
        except Exception as e:
            logger.error(f"Failed to get mapping for {group_name}: {e}")
            return None
    
    @classmethod
    async def get_all_mappings(cls) -> List[ADGroupMapping]:
        """Get all AD group mappings"""
        mappings = []
        
        try:
            group_names = await redis_client._client.smembers(cls.AD_GROUP_INDEX)
            
            for name in group_names:
                group_name = name.decode() if isinstance(name, bytes) else name
                mapping = await cls.get_mapping(group_name)
                if mapping:
                    mappings.append(mapping)
            
            # Sort by priority
            mappings.sort(key=lambda m: m.priority, reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to get all mappings: {e}")
        
        return mappings
    
    @classmethod
    async def resolve_user_permissions(
        cls,
        user_id: str,
        ad_groups: List[str]
    ) -> ResolvedPermissions:
        """
        Resolve effective permissions for a user based on their AD groups.
        
        Args:
            user_id: User identifier
            ad_groups: List of AD groups the user belongs to
            
        Returns:
            ResolvedPermissions with effective permissions
        """
        # Get mappings for all groups
        mappings = []
        for group in ad_groups:
            mapping = await cls.get_mapping(group)
            if mapping and mapping.enabled:
                mappings.append(mapping)
        
        # Sort by priority (highest first)
        mappings.sort(key=lambda m: m.priority, reverse=True)
        
        # Merge permissions
        table_permissions: Dict[str, TablePermission] = {}
        effective_tables: Set[str] = set()
        masked_columns: Dict[str, List[str]] = {}
        row_filters: Dict[str, str] = {}
        
        for mapping in mappings:
            for table_name, perm in mapping.table_permissions.items():
                # Handle wildcard (all tables)
                if table_name == "*":
                    # Apply to all tables
                    effective_tables.add("*")
                    if perm.permission_level != PermissionLevel.NONE:
                        table_permissions["*"] = perm
                    continue
                
                # Skip if already have higher priority permission
                if table_name in table_permissions:
                    existing = table_permissions[table_name]
                    # Higher priority already processed
                    # Merge masked columns
                    existing.masked_columns = list(set(existing.masked_columns + perm.masked_columns))
                    continue
                
                if perm.permission_level != PermissionLevel.NONE:
                    table_permissions[table_name] = perm
                    effective_tables.add(table_name)
                    
                    if perm.masked_columns:
                        masked_columns[table_name] = perm.masked_columns
                    
                    if perm.row_filter:
                        row_filters[table_name] = perm.row_filter
        
        return ResolvedPermissions(
            user_id=user_id,
            ad_groups=ad_groups,
            table_permissions=table_permissions,
            effective_tables=effective_tables,
            masked_columns=masked_columns,
            row_filters=row_filters
        )
    
    @classmethod
    def filter_schema_by_permissions(
        cls,
        schema: Dict[str, Any],
        permissions: ResolvedPermissions,
        user_role: str = "viewer"
    ) -> Dict[str, Any]:
        """
        Filter database schema based on resolved permissions.
        
        Args:
            schema: Full database schema
            permissions: Resolved permissions for user
            user_role: User's role
            
        Returns:
            Filtered schema with only accessible tables/columns
        """
        if user_role.lower() == "admin":
            return schema
        
        if "*" in permissions.effective_tables:
            # Wildcard access - return all tables
            return schema
        
        filtered_schema = {
            "tables": {},
            "views": schema.get("views", {}),
            "source": schema.get("source", ""),
            "filtered": True,
            "accessible_tables": list(permissions.effective_tables)
        }
        
        tables = schema.get("tables", {})
        
        for table_name, columns in tables.items():
            table_upper = table_name.upper()
            
            # Check if user has access to this table
            if table_upper in permissions.effective_tables or table_name in permissions.effective_tables:
                # Get masked columns for this table
                cols_to_mask = set()
                for perm_table, mask_cols in permissions.masked_columns.items():
                    if perm_table.upper() == table_upper:
                        cols_to_mask.update([c.upper() for c in mask_cols])
                
                # Filter columns
                if cols_to_mask:
                    filtered_columns = []
                    for col in columns:
                        col_name = col.get("name", "").upper()
                        if col_name in cols_to_mask:
                            # Mark column as masked
                            col_copy = dict(col)
                            col_copy["masked"] = True
                            col_copy["accessible"] = False
                            filtered_columns.append(col_copy)
                        else:
                            col_copy = dict(col)
                            col_copy["accessible"] = True
                            filtered_columns.append(col_copy)
                    
                    filtered_schema["tables"][table_name] = filtered_columns
                else:
                    # No masking needed
                    filtered_schema["tables"][table_name] = columns
        
        return filtered_schema
    
    @classmethod
    def can_access_table(
        cls,
        table_name: str,
        permissions: ResolvedPermissions
    ) -> bool:
        """Check if user can access a specific table"""
        if "*" in permissions.effective_tables:
            return True
        return table_name.upper() in {t.upper() for t in permissions.effective_tables}
    
    @classmethod
    def get_masked_columns_for_table(
        cls,
        table_name: str,
        permissions: ResolvedPermissions
    ) -> List[str]:
        """Get list of columns that should be masked for a table"""
        table_upper = table_name.upper()
        return permissions.masked_columns.get(table_upper, [])
    
    @classmethod
    async def audit_access_check(
        cls,
        user_id: str,
        table_name: str,
        access_granted: bool,
        reason: str
    ):
        """Log access check for audit"""
        try:
            audit_entry = {
                "user_id": user_id,
                "table_name": table_name,
                "access_granted": access_granted,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            audit_key = f"{cls.AD_AUDIT_PREFIX}{datetime.now(timezone.utc).strftime('%Y%m%d')}"
            await redis_client._client.lpush(audit_key, str(audit_entry))
            await redis_client._client.expire(audit_key, 86400 * 90)  # 90 days
        except Exception as e:
            logger.warning(f"Failed to audit access check: {e}")


# Global instance
ad_group_mapping_service = ADGroupMappingService()


# Convenience functions

async def get_user_table_permissions(
    user_id: str,
    ad_groups: List[str]
) -> ResolvedPermissions:
    """Get resolved permissions for a user"""
    return await ADGroupMappingService.resolve_user_permissions(user_id, ad_groups)


def filter_schema_for_user(
    schema: Dict[str, Any],
    permissions: ResolvedPermissions,
    user_role: str = "viewer"
) -> Dict[str, Any]:
    """Filter schema based on user permissions"""
    return ADGroupMappingService.filter_schema_by_permissions(schema, permissions, user_role)


async def can_user_access_table(
    user_id: str,
    ad_groups: List[str],
    table_name: str
) -> bool:
    """Check if user can access a table"""
    permissions = await ADGroupMappingService.resolve_user_permissions(user_id, ad_groups)
    return ADGroupMappingService.can_access_table(table_name, permissions)