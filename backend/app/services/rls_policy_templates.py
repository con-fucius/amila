"""
Row-Level Security (RLS) Policy Templates

Pre-built RLS policy templates for common enterprise scenarios.
These templates provide standardized row-level access control patterns.

Usage:
    from app.services.rls_policy_templates import RLSPolicyTemplates
    
    # Apply user-owned policy
    policy = RLSPolicyTemplates.user_owned_policy("user_id")
    
    # Apply department-based policy
    policy = RLSPolicyTemplates.department_policy("dept_code")
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class RLSPolicyType(Enum):
    """Types of RLS policies"""
    USER_OWNED = "user_owned"
    DEPARTMENT = "department"
    TEAM = "team"
    ROLE_BASED = "role_based"
    TIME_RESTRICTED = "time_restricted"
    HIERARCHY = "hierarchy"
    CUSTOM = "custom"


@dataclass
class RLSPolicyTemplate:
    """RLS Policy template definition"""
    name: str
    policy_type: RLSPolicyType
    description: str
    column_mapping: Dict[str, str]
    sql_template: str
    user_attributes_required: List[str]
    examples: List[str]


class RLSPolicyTemplates:
    """
    Collection of pre-built RLS policy templates for common scenarios.
    
    Templates cover:
    - User-owned records (created_by, owner_id)
    - Department-based access (dept_code, department_id)
    - Team-based access (team_id)
    - Role-based access (role_level, clearance)
    - Time-restricted access (created_date, valid_until)
    - Hierarchical access (manager_id, org_hierarchy)
    """
    
    @classmethod
    def get_all_templates(cls) -> Dict[str, RLSPolicyTemplate]:
        """Get all available policy templates"""
        return {
            "user_owned": cls.user_owned_policy(),
            "department": cls.department_policy(),
            "team": cls.team_policy(),
            "role_based": cls.role_based_policy(),
            "time_restricted": cls.time_restricted_policy(),
            "hierarchy": cls.hierarchy_policy(),
            "manager_view": cls.manager_view_policy(),
            "sales_territory": cls.sales_territory_policy(),
            "project_based": cls.project_based_policy(),
            "data_classification": cls.data_classification_policy(),
        }
    
    @classmethod
    def user_owned_policy(cls, user_id_column: str = "created_by") -> RLSPolicyTemplate:
        """
        User-owned records policy.
        
        Users can only access records they created or own.
        Common for: Personal data, user settings, individual work items.
        
        Args:
            user_id_column: Column containing the user identifier
        """
        return RLSPolicyTemplate(
            name="user_owned",
            policy_type=RLSPolicyType.USER_OWNED,
            description="Users can only access records they created or own",
            column_mapping={"user_id": user_id_column},
            sql_template=f"{{table}}.{user_id_column} = :user_id",
            user_attributes_required=["user_id"],
            examples=[
                "User can only see their own expense reports",
                "User can only access their personal settings",
                "Employee can only view their own performance reviews"
            ]
        )
    
    @classmethod
    def department_policy(cls, dept_column: str = "department_code") -> RLSPolicyTemplate:
        """
        Department-based access policy.
        
        Users can access records from their department(s).
        Common for: Department budgets, team reports, divisional data.
        
        Args:
            dept_column: Column containing the department identifier
        """
        return RLSPolicyTemplate(
            name="department",
            policy_type=RLSPolicyType.DEPARTMENT,
            description="Users can access records from their department(s)",
            column_mapping={"department": dept_column},
            sql_template=f"{{table}}.{dept_column} IN (:user_departments)",
            user_attributes_required=["department", "department_list"],
            examples=[
                "Finance team can see all finance department records",
                "HR can access employee data for their division",
                "Regional manager can see data for their region"
            ]
        )
    
    @classmethod
    def team_policy(cls, team_id_column: str = "team_id") -> RLSPolicyTemplate:
        """
        Team-based access policy.
        
        Users can access records for teams they belong to.
        Common for: Project data, team assignments, collaboration tools.
        
        Args:
            team_id_column: Column containing the team identifier
        """
        return RLSPolicyTemplate(
            name="team",
            policy_type=RLSPolicyType.TEAM,
            description="Users can access records for teams they belong to",
            column_mapping={"team_id": team_id_column},
            sql_template=f"{{table}}.{team_id_column} IN (:user_teams)",
            user_attributes_required=["team_ids"],
            examples=[
                "Team members can see project tasks for their teams",
                "Support agent can view tickets assigned to their teams",
                "Developer can access repositories for their teams"
            ]
        )
    
    @classmethod
    def role_based_policy(cls, role_column: str = "required_role_level") -> RLSPolicyTemplate:
        """
        Role-based access policy.
        
        Access based on user's role level or clearance.
        Common for: Executive data, sensitive reports, privileged information.
        
        Args:
            role_column: Column containing the required role/clearance level
        """
        return RLSPolicyTemplate(
            name="role_based",
            policy_type=RLSPolicyType.ROLE_BASED,
            description="Access based on user's role level or clearance",
            column_mapping={"role_level": role_column},
            sql_template=f"{{table}}.{role_column} <= :user_role_level",
            user_attributes_required=["role_level"],
            examples=[
                "Managers can see data up to their clearance level",
                "Executives can access all role levels below theirs",
                "Analysts can only see public and internal data"
            ]
        )
    
    @classmethod
    def time_restricted_policy(
        cls,
        date_column: str = "created_date",
        lookback_days: int = 90
    ) -> RLSPolicyTemplate:
        """
        Time-restricted access policy.
        
        Users can only access records within a time window.
        Common for: Recent transactions, active projects, rolling windows.
        
        Args:
            date_column: Column containing the date to filter on
            lookback_days: Number of days to look back
        """
        return RLSPolicyTemplate(
            name="time_restricted",
            policy_type=RLSPolicyType.TIME_RESTRICTED,
            description=f"Users can only access records from last {lookback_days} days",
            column_mapping={"date": date_column},
            sql_template=f"{{table}}.{date_column} >= SYSDATE - {lookback_days}",
            user_attributes_required=[],
            examples=[
                f"Users can only see records from last {lookback_days} days",
                "Access limited to current fiscal year data",
                "Only active (non-expired) records are visible"
            ]
        )
    
    @classmethod
    def hierarchy_policy(
        cls,
        user_id_column: str = "employee_id",
        manager_id_column: str = "manager_id"
    ) -> RLSPolicyTemplate:
        """
        Hierarchical access policy.
        
        Managers can see their own and subordinates' records.
        Common for: Management reports, org-wide dashboards, skip-level access.
        
        Args:
            user_id_column: Column containing the employee/user ID
            manager_id_column: Column containing the manager ID
        """
        return RLSPolicyTemplate(
            name="hierarchy",
            policy_type=RLSPolicyType.HIERARCHY,
            description="Managers can see their own and subordinates' records",
            column_mapping={
                "user_id": user_id_column,
                "manager_id": manager_id_column
            },
            sql_template=(
                f"({{table}}.{user_id_column} = :user_id OR "
                f"{{table}}.{manager_id_column} = :user_id OR "
                f"{{table}}.{user_id_column} IN (:subordinate_ids))"
            ),
            user_attributes_required=["user_id", "subordinate_ids"],
            examples=[
                "Manager can see their own and team's records",
                "VP can access data for entire organization branch",
                "Skip-level management visibility"
            ]
        )
    
    @classmethod
    def manager_view_policy(cls, manager_id_column: str = "manager_id") -> RLSPolicyTemplate:
        """
        Simple manager view policy.
        
        Users can see records of their direct reports.
        Simplified version of hierarchy policy.
        
        Args:
            manager_id_column: Column containing the manager ID
        """
        return RLSPolicyTemplate(
            name="manager_view",
            policy_type=RLSPolicyType.HIERARCHY,
            description="Managers can see records of their direct reports",
            column_mapping={"manager_id": manager_id_column},
            sql_template=f"{{table}}.{manager_id_column} = :user_id",
            user_attributes_required=["user_id"],
            examples=[
                "Manager can view direct reports' timesheets",
                "Team lead can see team member assignments",
                "Supervisor can access staff performance data"
            ]
        )
    
    @classmethod
    def sales_territory_policy(cls, territory_column: str = "territory_code") -> RLSPolicyTemplate:
        """
        Sales territory access policy.
        
        Sales reps can only access data for their assigned territories.
        Common for: CRM data, sales reports, customer information.
        
        Args:
            territory_column: Column containing the territory identifier
        """
        return RLSPolicyTemplate(
            name="sales_territory",
            policy_type=RLSPolicyType.DEPARTMENT,
            description="Sales reps can only access data for their assigned territories",
            column_mapping={"territory": territory_column},
            sql_template=f"{{table}}.{territory_column} IN (:user_territories)",
            user_attributes_required=["territory_codes"],
            examples=[
                "Sales rep can see customers in their territory",
                "Regional manager can access multi-territory data",
                "Territory-based sales reporting"
            ]
        )
    
    @classmethod
    def project_based_policy(cls, project_id_column: str = "project_id") -> RLSPolicyTemplate:
        """
        Project-based access policy.
        
        Users can access data for projects they are assigned to.
        Common for: Project management, resource allocation, time tracking.
        
        Args:
            project_id_column: Column containing the project identifier
        """
        return RLSPolicyTemplate(
            name="project_based",
            policy_type=RLSPolicyType.TEAM,
            description="Users can access data for projects they are assigned to",
            column_mapping={"project_id": project_id_column},
            sql_template=f"{{table}}.{project_id_column} IN (:user_projects)",
            user_attributes_required=["project_ids"],
            examples=[
                "Project member can see project tasks and timelines",
                "PM can access data for their managed projects",
                "Resource manager can see allocation across projects"
            ]
        )
    
    @classmethod
    def data_classification_policy(
        cls,
        classification_column: str = "data_classification"
    ) -> RLSPolicyTemplate:
        """
        Data classification-based policy.
        
        Access based on data classification level and user clearance.
        Common for: Classified documents, sensitive data, compliance scenarios.
        
        Args:
            classification_column: Column containing the classification level
        """
        return RLSPolicyTemplate(
            name="data_classification",
            policy_type=RLSPolicyType.ROLE_BASED,
            description="Access based on data classification and user clearance",
            column_mapping={"classification": classification_column},
            sql_template=(
                f"({{table}}.{classification_column} IN (:user_clearance_levels) OR "
                f"{{table}}.{classification_column} IS NULL)"
            ),
            user_attributes_required=["clearance_levels"],
            examples=[
                "Users can only see data at their clearance level or below",
                "Public, Internal, Confidential, Restricted classification",
                "Need-to-know basis data access"
            ]
        )
    
    @classmethod
    def apply_template(
        cls,
        template_name: str,
        table_name: str,
        user_attributes: Dict[str, Any],
        custom_columns: Optional[Dict[str, str]] = None
    ) -> Optional[str]:
        """
        Apply a policy template to generate SQL predicate.
        
        Args:
            template_name: Name of the template to apply
            table_name: Target table name
            user_attributes: User attributes for the predicate
            custom_columns: Optional custom column mappings
            
        Returns:
            SQL predicate string or None if template not found
        """
        templates = cls.get_all_templates()
        template = templates.get(template_name)
        
        if not template:
            logger.warning(f"RLS template '{template_name}' not found")
            return None
        
        # Apply custom column mappings if provided
        sql = template.sql_template
        if custom_columns:
            for key, value in custom_columns.items():
                sql = sql.replace(f"{{table}}.{key}", f"{{table}}.{value}")
        
        # Replace table placeholder
        sql = sql.replace("{table}", table_name)
        
        logger.info(f"Applied RLS template '{template_name}' to table '{table_name}'")
        return sql
    
    @classmethod
    def get_template_for_table(
        cls,
        table_name: str,
        table_columns: List[str]
    ) -> Optional[str]:
        """
        Automatically suggest the best template for a table based on its columns.
        
        Args:
            table_name: Name of the table
            table_columns: List of column names in the table
            
        Returns:
            Suggested template name or None
        """
        columns_lower = [c.lower() for c in table_columns]
        
        # Check for user ownership patterns
        if any(c in columns_lower for c in ['created_by', 'owner_id', 'user_id', 'assigned_to']):
            return "user_owned"
        
        # Check for department patterns
        if any(c in columns_lower for c in ['department_code', 'dept_id', 'department_id', 'division_code']):
            return "department"
        
        # Check for team patterns
        if any(c in columns_lower for c in ['team_id', 'group_id', 'squad_id']):
            return "team"
        
        # Check for territory patterns
        if any(c in columns_lower for c in ['territory_code', 'region_code', 'sales_territory']):
            return "sales_territory"
        
        # Check for project patterns
        if any(c in columns_lower for c in ['project_id', 'project_code']):
            return "project_based"
        
        # Check for manager/hierarchy patterns
        if any(c in columns_lower for c in ['manager_id', 'supervisor_id', 'reports_to']):
            return "manager_view"
        
        # Check for classification patterns
        if any(c in columns_lower for c in ['classification', 'clearance', 'sensitivity']):
            return "data_classification"
        
        # Default to time-restricted if date columns exist
        if any(c in columns_lower for c in ['created_date', 'created_at', 'date_created']):
            return "time_restricted"
        
        logger.info(f"No matching RLS template found for table '{table_name}'")
        return None


# Convenience functions

def get_rls_template(template_name: str) -> Optional[RLSPolicyTemplate]:
    """Get a specific RLS policy template"""
    templates = RLSPolicyTemplates.get_all_templates()
    return templates.get(template_name)


def suggest_rls_policy(table_name: str, columns: List[str]) -> Optional[str]:
    """Suggest the best RLS policy for a table"""
    return RLSPolicyTemplates.get_template_for_table(table_name, columns)


def apply_rls_policy(
    template_name: str,
    table_name: str,
    user_attributes: Dict[str, Any]
) -> Optional[str]:
    """Apply an RLS policy template"""
    return RLSPolicyTemplates.apply_template(template_name, table_name, user_attributes)