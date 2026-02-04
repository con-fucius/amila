"""
Governance API Endpoints
Provides centralized visibility into system permissions, agent activity, and capabilities mapping.
Implements Issue 22: Centralized view for agent permissions and monitoring.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.core.rbac import (
    require_admin_role, 
    ROLE_PERMISSIONS, 
    Role, 
    Permission,
    rbac_manager
)
from app.core.audit import audit_logger, AuditAction, AuditEntry
from app.services.native_audit_service import native_audit_service
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


class AgentCapability(BaseModel):
    """Model for agent capability information"""
    agent_name: str
    agent_type: str
    databases: List[str]
    permissions: List[str]
    operations: List[str]
    risk_level: str
    status: str
    last_active: Optional[str] = None
    total_requests: int = 0
    error_rate: float = 0.0


class SystemCapability(BaseModel):
    """Model for system/database capability"""
    system_name: str
    system_type: str
    connection_status: str
    accessible_by: List[str]
    operations_allowed: List[str]
    data_classification: str


@router.get("/capabilities/agents", response_model=List[AgentCapability])
async def get_agent_capabilities(
    user: dict = Depends(require_admin_role)
) -> List[AgentCapability]:
    """
    Get comprehensive mapping of all agents and their capabilities.
    Shows which agents can access which systems and what operations they can perform.
    
    This addresses Issue 22: Centralized view for identifying misconfigurations
    like agents with overly broad permissions.
    """
    try:
        # Define all agents in the system
        agents = []
        
        # 1. Query Orchestrator Agent (LangGraph)
        agents.append(AgentCapability(
            agent_name="Query Orchestrator",
            agent_type="LangGraph SQL Agent",
            databases=["Oracle", "Doris"],
            permissions=["READ", "QUERY_GENERATION", "SCHEMA_READ"],
            operations=[
                "Natural language to SQL conversion",
                "Schema introspection",
                "Query execution (SELECT only)",
                "Result formatting",
                "Query validation",
            ],
            risk_level="MEDIUM",
            status="active",
            last_active=datetime.now().isoformat(),
            total_requests=0,  # Would be fetched from metrics
            error_rate=0.0,
        ))
        
        # 2. Schema Service Agent
        agents.append(AgentCapability(
            agent_name="Schema Service",
            agent_type="Metadata Agent",
            databases=["Oracle", "Doris"],
            permissions=["SCHEMA_READ", "METADATA_READ"],
            operations=[
                "Schema discovery",
                "Table metadata extraction",
                "Column information retrieval",
                "Relationship mapping",
            ],
            risk_level="LOW",
            status="active",
            last_active=datetime.now().isoformat(),
            total_requests=0,
            error_rate=0.0,
        ))
        
        # 3. SQL Validation Agent
        agents.append(AgentCapability(
            agent_name="SQL Validator",
            agent_type="Security Agent",
            databases=[],
            permissions=["VALIDATE"],
            operations=[
                "SQL injection detection",
                "Syntax validation",
                "Permission validation",
                "Query risk assessment",
            ],
            risk_level="LOW",
            status="active",
            last_active=datetime.now().isoformat(),
            total_requests=0,
            error_rate=0.0,
        ))
        
        # 4. Visualization Agent
        agents.append(AgentCapability(
            agent_name="Visualization Generator",
            agent_type="Presentation Agent",
            databases=[],
            permissions=["READ"],
            operations=[
                "Chart generation",
                "Data visualization",
                "Plotly configuration",
            ],
            risk_level="LOW",
            status="active",
            last_active=datetime.now().isoformat(),
            total_requests=0,
            error_rate=0.0,
        ))
        
        # 5. Report Generation Agent
        agents.append(AgentCapability(
            agent_name="Report Generator",
            agent_type="Document Agent",
            databases=[],
            permissions=["READ"],
            operations=[
                "HTML report generation",
                "PDF export",
                "DOCX export",
                "Executive summary creation",
            ],
            risk_level="LOW",
            status="active",
            last_active=datetime.now().isoformat(),
            total_requests=0,
            error_rate=0.0,
        ))
        
        # 6. Graphiti Knowledge Graph Agent
        agents.append(AgentCapability(
            agent_name="Graphiti Context Agent",
            agent_type="Knowledge Graph Agent",
            databases=["FalkorDB"],
            permissions=["READ", "WRITE", "GRAPH_QUERY"],
            operations=[
                "Context retrieval",
                "Knowledge graph updates",
                "Temporal reasoning",
                "Entity relationship mapping",
            ],
            risk_level="MEDIUM",
            status="active" if settings.GRAPHITI_LLM_PROVIDER else "inactive",
            last_active=datetime.now().isoformat(),
            total_requests=0,
            error_rate=0.0,
        ))
        
        # 7. Skills Orchestrator Agent
        agents.append(AgentCapability(
            agent_name="Skills Orchestrator",
            agent_type="Column Mapping Agent",
            databases=["Oracle", "Doris"],
            permissions=["SCHEMA_READ", "SEMANTIC_ANALYSIS"],
            operations=[
                "Column semantic mapping",
                "Derived expression generation",
                "Schema analysis",
                "Concept validation",
            ],
            risk_level="LOW",
            status="active" if settings.QUERY_SQL_SKILLS_ENABLED else "inactive",
            last_active=datetime.now().isoformat(),
            total_requests=0,
            error_rate=0.0,
        ))
        
        # Fetch real metrics from audit logs
        recent_logs = await audit_logger.get_recent_audit_trail(limit=500)
        
        # Calculate metrics per agent (simplified - would use proper metrics in production)
        agent_metrics = {}
        for log in recent_logs:
            agent_name = log.metadata.get("agent") if log.metadata else None
            if agent_name:
                if agent_name not in agent_metrics:
                    agent_metrics[agent_name] = {"total": 0, "errors": 0}
                agent_metrics[agent_name]["total"] += 1
                if not log.success:
                    agent_metrics[agent_name]["errors"] += 1
        
        # Update agents with real metrics
        for agent in agents:
            if agent.agent_name in agent_metrics:
                metrics = agent_metrics[agent.agent_name]
                agent.total_requests = metrics["total"]
                agent.error_rate = (
                    metrics["errors"] / metrics["total"] if metrics["total"] > 0 else 0.0
                )
        
        return agents
        
    except Exception as e:
        logger.error(f"Failed to fetch agent capabilities: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/capabilities/systems", response_model=List[SystemCapability])
async def get_system_capabilities(
    user: dict = Depends(require_admin_role)
) -> List[SystemCapability]:
    """
    Get comprehensive mapping of all systems/databases and their access controls.
    Shows which agents can access which systems and what operations are allowed.
    """
    try:
        systems = []
        
        # 1. Oracle Database
        systems.append(SystemCapability(
            system_name="Oracle Database",
            system_type="Relational Database",
            connection_status="active" if settings.ORACLE_USERNAME else "inactive",
            accessible_by=[
                "Query Orchestrator",
                "Schema Service",
                "Skills Orchestrator",
            ],
            operations_allowed=[
                "SELECT queries",
                "Schema introspection",
                "Metadata queries",
            ],
            data_classification="SENSITIVE",
        ))
        
        # 2. Doris Database
        systems.append(SystemCapability(
            system_name="Doris Database",
            system_type="Analytical Database",
            connection_status="active" if settings.DORIS_MCP_ENABLED else "inactive",
            accessible_by=[
                "Query Orchestrator",
                "Schema Service",
                "Skills Orchestrator",
            ],
            operations_allowed=[
                "SELECT queries",
                "Schema introspection",
                "Analytical queries",
            ],
            data_classification="SENSITIVE",
        ))
        
        # 3. Redis Cache
        systems.append(SystemCapability(
            system_name="Redis Cache",
            system_type="Cache/Session Store",
            connection_status="active",
            accessible_by=[
                "All Agents",
            ],
            operations_allowed=[
                "Cache read/write",
                "Session management",
                "Audit log storage",
            ],
            data_classification="INTERNAL",
        ))
        
        # 4. FalkorDB (Graphiti)
        systems.append(SystemCapability(
            system_name="FalkorDB",
            system_type="Graph Database",
            connection_status="active" if settings.FALKORDB_HOST else "inactive",
            accessible_by=[
                "Graphiti Context Agent",
            ],
            operations_allowed=[
                "Graph queries",
                "Entity creation",
                "Relationship mapping",
                "Temporal queries",
            ],
            data_classification="INTERNAL",
        ))
        
        # 5. LLM Providers
        llm_status = "active"
        llm_provider = settings.QUERY_LLM_PROVIDER
        systems.append(SystemCapability(
            system_name=f"LLM Provider ({llm_provider})",
            system_type="AI Service",
            connection_status=llm_status,
            accessible_by=[
                "Query Orchestrator",
                "Graphiti Context Agent",
                "Skills Orchestrator",
            ],
            operations_allowed=[
                "Text generation",
                "SQL generation",
                "Semantic analysis",
                "Embeddings",
            ],
            data_classification="EXTERNAL",
        ))
        
        return systems
        
    except Exception as e:
        logger.error(f"Failed to fetch system capabilities: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/capabilities/misconfigurations")
async def detect_misconfigurations(
    user: dict = Depends(require_admin_role)
) -> Dict[str, Any]:
    """
    Detect potential security misconfigurations in agent permissions.
    
    Checks for:
    - Agents with overly broad permissions
    - Inactive agents with active permissions
    - Agents accessing sensitive systems without proper controls
    - High error rates indicating permission issues
    """
    try:
        issues = []
        warnings = []
        
        # Get current agent capabilities
        agents = await get_agent_capabilities(user)
        
        for agent in agents:
            # Check 1: Inactive agents with database access
            if agent.status == "inactive" and agent.databases:
                issues.append({
                    "severity": "HIGH",
                    "agent": agent.agent_name,
                    "issue": "Inactive agent has database access",
                    "recommendation": f"Revoke database permissions for {agent.agent_name}",
                    "databases": agent.databases,
                })
            
            # Check 2: High error rates
            if agent.error_rate > 0.2:  # >20% error rate
                warnings.append({
                    "severity": "MEDIUM",
                    "agent": agent.agent_name,
                    "issue": f"High error rate: {agent.error_rate:.1%}",
                    "recommendation": "Investigate permission or configuration issues",
                })
            
            # Check 3: Agents with WRITE permissions (should be minimal)
            if "WRITE" in agent.permissions or "SCHEMA_MODIFY" in agent.permissions:
                warnings.append({
                    "severity": "MEDIUM",
                    "agent": agent.agent_name,
                    "issue": "Agent has write permissions",
                    "recommendation": "Verify write permissions are necessary",
                    "permissions": agent.permissions,
                })
            
            # Check 4: High-risk agents accessing multiple databases
            if agent.risk_level == "HIGH" and len(agent.databases) > 1:
                issues.append({
                    "severity": "HIGH",
                    "agent": agent.agent_name,
                    "issue": "High-risk agent has multi-database access",
                    "recommendation": "Restrict to single database or reduce risk level",
                    "databases": agent.databases,
                })
        
        # Check 5: Development mode security warnings
        if settings.environment == "development":
            warnings.append({
                "severity": "INFO",
                "issue": "Running in development mode",
                "recommendation": "Ensure production deployment uses stricter security settings",
            })
        
        return {
            "status": "success",
            "scan_timestamp": datetime.now().isoformat(),
            "total_issues": len(issues),
            "total_warnings": len(warnings),
            "issues": issues,
            "warnings": warnings,
            "overall_status": "CRITICAL" if issues else ("WARNING" if warnings else "HEALTHY"),
        }
        
    except Exception as e:
        logger.error(f"Failed to detect misconfigurations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/permissions/matrix", response_model=Dict[str, List[str]])
async def get_permission_matrix(
    user: dict = Depends(require_admin_role)
) -> Dict[str, List[str]]:
    """
    Get the static permission matrix showing what each role can do.
    Used for the "Governance View" UI.
    """
    matrix = {}
    for role, permissions in ROLE_PERMISSIONS.items():
        matrix[role.value] = [p.value for p in permissions]
    return matrix

@router.get("/audit/activity")
async def get_agent_activity(
    limit: int = Query(50, ge=1, le=200),
    user_filter: Optional[str] = None,
    action_type: Optional[str] = None,
    user: dict = Depends(require_admin_role)
) -> Dict[str, Any]:
    """
    Get a correlated view of agent activity.
    Allows admins to monitor requests made to AI agents and their subsequent actions.
    """
    try:
        activity_log = []
        
        # Prefer native audit service for richer data (Postgres/Doris)
        if settings.NATIVE_AUDIT_ENABLED:
            try:
                native_entries = await native_audit_service.query_audit_trail(
                    session=None, # Use internal client
                    user_id=user_filter,
                    action=action_type,
                    limit=limit
                )
                
                for entry in native_entries:
                    # Normalize native entry to match frontend expectations
                    # entry is a dict
                    
                    # Risk assessment enhancement
                    risk_level = "low"
                    act = entry.get('action')
                    success = entry.get('success')
                    if act in [AuditAction.SCHEMA_MODIFY.value, AuditAction.USER_DELETE.value, AuditAction.ROLE_ASSIGN.value]:
                        risk_level = "critical"
                    elif act in [AuditAction.QUERY_EXECUTE.value, AuditAction.CONFIG_UPDATE.value]:
                        risk_level = "medium"
                    
                    entry["risk_metrics"] = {
                        "level": risk_level,
                        "requires_attention": risk_level in ["critical", "medium"] and not success
                    }
                    activity_log.append(entry)
                    
                # If we got results, return them. If empty, fall back to Redis? 
                # Native DB should be source of truth if enabled.
            except Exception as e:
                logger.error(f"Native audit query failed, falling back to Redis: {e}")
                # Fallback to Redis below
        
        # Fallback to Redis if native didn't yield results or wasn't enabled/failed
        if not activity_log:
            # Fetch raw logs from Redis
            if user_filter:
                entries = await audit_logger.get_user_audit_trail(user_filter, limit=limit)
            elif action_type:
                # Try to map string to Enum if possible, otherwise fail gracefully
                try:
                    action_enum = AuditAction(action_type)
                    entries = await audit_logger.get_action_audit_trail(action_enum, limit=limit)
                except ValueError:
                    entries = await audit_logger.get_recent_audit_trail(limit=limit)
            else:
                entries = await audit_logger.get_recent_audit_trail(limit=limit)

            # Post-process for "Governance View"
            for entry in entries:
                log_item = entry.to_dict()
                
                # Risk assessment enhancement
                risk_level = "low"
                if entry.action in [AuditAction.SCHEMA_MODIFY.value, AuditAction.USER_DELETE.value, AuditAction.ROLE_ASSIGN.value]:
                    risk_level = "critical"
                elif entry.action in [AuditAction.QUERY_EXECUTE.value, AuditAction.CONFIG_UPDATE.value]:
                    risk_level = "medium"
                
                log_item["risk_metrics"] = {
                    "level": risk_level,
                    "requires_attention": risk_level in ["critical", "medium"] and not entry.success
                }
                
                activity_log.append(log_item)

        return {
            "status": "success",
            "count": len(activity_log),
            "logs": activity_log,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "view": "governance_audit",
                "source": "native" if settings.NATIVE_AUDIT_ENABLED and activity_log and activity_log[0].get('database_type') else "redis"
            }
        }

    except Exception as e:
        logger.error(f"Failed to fetch governance logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/audit/summary")
async def get_audit_summary(
    user: dict = Depends(require_admin_role)
) -> Dict[str, Any]:
    """
    Get summary statistics for the dashboard.
    """
    try:
        if settings.NATIVE_AUDIT_ENABLED:
            try:
                # Use native audit service for accurate aggregation
                native_summary = await native_audit_service.generate_summary(session=None)
                
                # Transform to match frontend expectations if needed
                # native_summary has "actions" list
                
                total_actions = 0
                errors = 0
                query_execs = 0
                schema_mods = 0
                unique_users = set()  # Not easily available from summary agg, maybe skip or fetch separate?
                
                # Native summary aggregation is by action
                for act in native_summary.get("actions", []):
                    cnt = act.get("count", 0)
                    err = act.get("error_count", 0)
                    action_name = act.get("action")
                    
                    total_actions += cnt
                    errors += err
                    
                    if action_name == AuditAction.QUERY_EXECUTE.value:
                        query_execs += cnt
                    if action_name == AuditAction.SCHEMA_MODIFY.value:
                        schema_mods += cnt
                    
                return {
                    "total_actions": total_actions,
                    "errors": errors,
                    "query_executions": query_execs,
                    "schema_modifications": schema_mods,
                    "unique_users": [], # Expensive to compute from pre-aggregated summary
                    "source": "native",
                    "breakdown": native_summary
                }
            except Exception as e:
                logger.error(f"Native audit summary failed: {e}")
                
        # Fallback to Redis sampling
        recent_logs = await audit_logger.get_recent_audit_trail(limit=200)
        
        summary = {
            "total_actions": len(recent_logs),
            "errors": 0,
            "query_executions": 0,
            "schema_modifications": 0,
            "unique_users": set(),
            "source": "redis_sample"
        }
        
        for log in recent_logs:
            if not log.success:
                summary["errors"] += 1
            if log.action == AuditAction.QUERY_EXECUTE.value:
                summary["query_executions"] += 1
            if log.action == AuditAction.SCHEMA_MODIFY.value:
                summary["schema_modifications"] += 1
            summary["unique_users"].add(log.user)
            
        summary["unique_users"] = list(summary["unique_users"])
        
        return summary
    except Exception as e:
        logger.error(f"Audit summary generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
