"""
Query Cost Estimation Service
Analyzes execution plans to estimate query cost and warn about expensive operations
Supports Oracle, PostgreSQL, and Doris databases
"""

import logging
import re
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

from app.core.client_registry import registry
from app.core.config import settings

logger = logging.getLogger(__name__)


class CostLevel(Enum):
    """Cost level classification"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class CostEstimate:
    """Query cost estimation result"""
    total_cost: float
    cardinality: int
    cost_level: CostLevel
    has_full_table_scan: bool
    full_scan_tables: List[str]
    warnings: List[str]
    recommendations: List[str]
    execution_plan: Optional[str] = None


class QueryCostEstimator:
    """Service for estimating query execution cost"""
    
    # Cost thresholds
    COST_LOW_THRESHOLD = 1000
    COST_MEDIUM_THRESHOLD = 10000
    COST_HIGH_THRESHOLD = 100000
    
    # Cardinality thresholds
    CARDINALITY_HIGH_THRESHOLD = 100000
    
    @staticmethod
    async def estimate_query_cost(
        sql_query: str,
        connection_name: Optional[str] = None,
        database_type: Optional[str] = None,
        include_plan: bool = False,
    ) -> CostEstimate:
        """
        Estimate query execution cost using EXPLAIN PLAN
        
        Supports Oracle, PostgreSQL, and Doris databases
        
        Args:
            sql_query: SQL query to analyze
            connection_name: Database connection name
            database_type: Database type (oracle, postgres, postgresql, doris)
            include_plan: Whether to include full execution plan in result
            
        Returns:
            CostEstimate with cost analysis and warnings
        """
        logger.info(f"Estimating query cost for database type: {database_type or 'oracle'}")
        
        db_type = (database_type or "oracle").lower()
        
        try:
            if db_type in ["postgres", "postgresql"]:
                return await QueryCostEstimator._estimate_postgres_cost(sql_query, connection_name, include_plan)
            elif db_type == "doris":
                return await QueryCostEstimator._estimate_doris_cost(sql_query, connection_name, include_plan)
            else:
                return await QueryCostEstimator._estimate_oracle_cost(sql_query, connection_name, include_plan)
        except Exception as e:
            logger.error(f"Cost estimation failed: {e}")
            return QueryCostEstimator._create_default_estimate()
    
    @staticmethod
    async def _estimate_oracle_cost(
        sql_query: str,
        connection_name: Optional[str] = None,
        include_plan: bool = False,
    ) -> CostEstimate:
        """Estimate cost for Oracle database"""
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            logger.warning(f"MCP client unavailable for cost estimation")
            return QueryCostEstimator._create_default_estimate()
        
        conn = connection_name or settings.oracle_default_connection
        
        # Step 1: Clean up previous explain plan data
        cleanup_sql = "DELETE FROM PLAN_TABLE"
        try:
            await mcp_client.execute_sql(cleanup_sql, conn)
        except Exception:
            pass  # Table might not exist yet
        
        # Step 2: Execute EXPLAIN PLAN
        explain_sql = f"EXPLAIN PLAN FOR {sql_query}"
        explain_result = await mcp_client.execute_sql(explain_sql, conn)
        
        if explain_result.get("status") != "success":
            logger.error(f"EXPLAIN PLAN execution failed")
            return QueryCostEstimator._create_default_estimate()
        
        # Step 3: Query plan table for cost analysis
        plan_query = """
        SELECT 
            operation,
            options,
            object_name,
            cost,
            cardinality,
            access_predicates,
            filter_predicates
        FROM PLAN_TABLE
        WHERE statement_id IS NULL
        ORDER BY id
        """
        
        plan_result = await mcp_client.execute_sql(plan_query, conn)
        
        if plan_result.get("status") != "success":
            logger.error(f"Failed to retrieve execution plan")
            return QueryCostEstimator._create_default_estimate()
        
        # Parse plan results
        rows = plan_result.get("results", {}).get("rows", [])
        
        if not rows:
            logger.warning(f"No execution plan data found")
            return QueryCostEstimator._create_default_estimate()
        
        # Analyze plan
        total_cost = 0.0
        total_cardinality = 0
        full_scan_tables = []
        warnings = []
        recommendations = []
        
        def get_val(r, idx, name):
            if isinstance(r, dict):
                return r.get(name)
            try:
                return r[idx]
            except (IndexError, TypeError):
                return None

        for row in rows:
            operation = get_val(row, 0, "operation") or ""
            options = get_val(row, 1, "options") or ""
            object_name = get_val(row, 2, "object_name") or ""
            cost_val = get_val(row, 3, "cost")
            card_val = get_val(row, 4, "cardinality")
            
            cost = float(cost_val) if cost_val else 0.0
            cardinality = int(card_val) if card_val else 0
            
            # Accumulate total cost (take max cost from plan)
            if cost > total_cost:
                total_cost = cost
            
            # Accumulate cardinality
            total_cardinality += cardinality
            
            # Detect full table scans
            if "TABLE ACCESS FULL" in operation:
                full_scan_tables.append(object_name)
                warnings.append(f"Full table scan detected on {object_name}")
                recommendations.append(f"Consider adding an index on {object_name}")
            
            # Detect Cartesian joins
            if "CARTESIAN" in operation or "MERGE JOIN CARTESIAN" in operation:
                warnings.append("Cartesian join detected - may be very expensive")
                recommendations.append("Review join conditions to avoid Cartesian product")
        
        # Determine cost level
        cost_level = QueryCostEstimator._classify_cost(total_cost)
        
        # Add cost-based warnings
        if cost_level == CostLevel.HIGH:
            warnings.append(f"High query cost: {int(total_cost)} (threshold: {QueryCostEstimator.COST_MEDIUM_THRESHOLD})")
            recommendations.append("Consider optimizing joins or adding indexes")
        elif cost_level == CostLevel.CRITICAL:
            warnings.append(f"CRITICAL query cost: {int(total_cost)} (threshold: {QueryCostEstimator.COST_HIGH_THRESHOLD})")
            recommendations.append("Query may time out or consume excessive resources")
            recommendations.append("Consider breaking into smaller queries or adding filters")
        
        # Add cardinality warnings
        if total_cardinality > QueryCostEstimator.CARDINALITY_HIGH_THRESHOLD:
            warnings.append(f"Large result set expected: ~{total_cardinality} rows")
            recommendations.append("Consider adding FETCH FIRST clause or more restrictive filters")
        
        # Get formatted execution plan if requested
        execution_plan = None
        if include_plan:
            execution_plan = await QueryCostEstimator._get_formatted_plan(conn)
        
        estimate = CostEstimate(
            total_cost=total_cost,
            cardinality=total_cardinality,
            cost_level=cost_level,
            has_full_table_scan=len(full_scan_tables) > 0,
            full_scan_tables=full_scan_tables,
            warnings=warnings,
            recommendations=recommendations,
            execution_plan=execution_plan,
        )
        
        logger.info(f"Oracle cost estimation complete: cost={int(total_cost)}, level={cost_level.value}")
        return estimate
    
    @staticmethod
    async def _estimate_postgres_cost(
        sql_query: str,
        connection_name: Optional[str] = None,
        include_plan: bool = False,
    ) -> CostEstimate:
        """Estimate cost for PostgreSQL database using EXPLAIN (FORMAT JSON)"""
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            logger.warning(f"MCP client unavailable for cost estimation")
            return QueryCostEstimator._create_default_estimate()
        
        conn = connection_name
        
        # Use PostgreSQL EXPLAIN with JSON format for structured parsing
        explain_sql = f"EXPLAIN (FORMAT JSON) {sql_query}"
        explain_result = await mcp_client.execute_sql(explain_sql, conn)
        
        if explain_result.get("status") != "success":
            logger.error(f"PostgreSQL EXPLAIN execution failed")
            return QueryCostEstimator._create_default_estimate()
        
        try:
            # Parse JSON plan result
            rows = explain_result.get("results", {}).get("rows", [])
            if not rows:
                return QueryCostEstimator._create_default_estimate()
            
            import json
            
            # Extract plan JSON from result
            plan_json = None
            if isinstance(rows[0], dict):
                # Find the first non-null value which should be the JSON plan
                for key, value in rows[0].items():
                    if value:
                        if isinstance(value, str):
                            plan_json = json.loads(value)
                        else:
                            plan_json = value
                        break
            elif isinstance(rows[0], (list, tuple)) and len(rows[0]) > 0:
                plan_json = json.loads(rows[0][0]) if isinstance(rows[0][0], str) else rows[0][0]
            
            if not plan_json or not isinstance(plan_json, list) or len(plan_json) == 0:
                return QueryCostEstimator._create_default_estimate()
            
            plan = plan_json[0].get("Plan", {})
            
            total_cost = float(plan.get("Total Cost", 0))
            total_cardinality = int(plan.get("Plan Rows", 0))
            full_scan_tables = []
            warnings = []
            recommendations = []
            
            # Recursively analyze plan nodes
            def analyze_plan_node(node: dict):
                nonlocal full_scan_tables, warnings, recommendations
                
                node_type = node.get("Node Type", "")
                relation_name = node.get("Relation Name", "")
                
                # Detect sequential scans (full table scans in PostgreSQL)
                if node_type == "Seq Scan" and relation_name:
                    full_scan_tables.append(relation_name)
                    warnings.append(f"Sequential scan detected on {relation_name}")
                    recommendations.append(f"Consider adding an index on {relation_name}")
                
                # Detect nested loops on large datasets
                if node_type == "Nested Loop":
                    rows = node.get("Plan Rows", 0)
                    if rows > 10000:
                        warnings.append(f"Nested loop join on large dataset (~{rows} rows)")
                        recommendations.append("Consider adding hash or merge join hints")
                
                # Recurse into child nodes
                for child in node.get("Plans", []):
                    analyze_plan_node(child)
            
            analyze_plan_node(plan)
            
            # Determine cost level (PostgreSQL uses different cost units)
            cost_level = QueryCostEstimator._classify_cost(total_cost)
            
            # Add cost-based warnings
            if cost_level == CostLevel.HIGH:
                warnings.append(f"High query cost: {int(total_cost)}")
                recommendations.append("Consider adding indexes or materialized views")
            elif cost_level == CostLevel.CRITICAL:
                warnings.append(f"CRITICAL query cost: {int(total_cost)}")
                recommendations.append("Query may be too complex - consider splitting into subqueries")
            
            # Add cardinality warnings
            if total_cardinality > QueryCostEstimator.CARDINALITY_HIGH_THRESHOLD:
                warnings.append(f"Large result set expected: ~{total_cardinality} rows")
                recommendations.append("Consider adding LIMIT clause or more restrictive WHERE conditions")
            
            # Get formatted TEXT plan if requested
            execution_plan = None
            if include_plan:
                text_explain_sql = f"EXPLAIN (FORMAT TEXT) {sql_query}"
                text_result = await mcp_client.execute_sql(text_explain_sql, conn)
                if text_result.get("status") == "success":
                    text_rows = text_result.get("results", {}).get("rows", [])
                    execution_plan = "\n".join([str(r[0]) if isinstance(r, (list, tuple)) else str(r) for r in text_rows])
            
            estimate = CostEstimate(
                total_cost=total_cost,
                cardinality=total_cardinality,
                cost_level=cost_level,
                has_full_table_scan=len(full_scan_tables) > 0,
                full_scan_tables=full_scan_tables,
                warnings=warnings,
                recommendations=recommendations,
                execution_plan=execution_plan,
            )
            
            logger.info(f"PostgreSQL cost estimation complete: cost={int(total_cost)}, level={cost_level.value}")
            return estimate
            
        except Exception as e:
            logger.error(f"Failed to parse PostgreSQL plan: {e}")
            return QueryCostEstimator._create_default_estimate()
    
    @staticmethod
    async def _estimate_doris_cost(
        sql_query: str,
        connection_name: Optional[str] = None,
        include_plan: bool = False,
    ) -> CostEstimate:
        """Estimate cost for Doris database using EXPLAIN"""
        mcp_client = registry.get_mcp_client()
        if not mcp_client:
            logger.warning(f"MCP client unavailable for cost estimation")
            return QueryCostEstimator._create_default_estimate()
        
        conn = connection_name
        
        # Doris uses MySQL-compatible EXPLAIN syntax
        explain_sql = f"EXPLAIN {sql_query}"
        explain_result = await mcp_client.execute_sql(explain_sql, conn)
        
        if explain_result.get("status") != "success":
            logger.error(f"Doris EXPLAIN execution failed")
            return QueryCostEstimator._create_default_estimate()
        
        try:
            rows = explain_result.get("results", {}).get("rows", [])
            
            if not rows:
                return QueryCostEstimator._create_default_estimate()
            
            # Parse Doris EXPLAIN output (text-based)
            full_scan_tables = []
            warnings = []
            recommendations = []
            total_cost = 0.0
            total_cardinality = 0
            
            plan_text = "\n".join([str(r[0]) if isinstance(r, (list, tuple)) else str(r) for r in rows])
            
            # Detect full table scans in Doris plan
            scan_patterns = [r"OlapScanNode", r"OlapScan", r"FULL SCAN", r"TableScan"]
            for pattern in scan_patterns:
                if re.search(pattern, plan_text, re.IGNORECASE):
                    # Try to extract table name
                    table_match = re.search(r"table[s]?[:\s=]+(\w+)", plan_text, re.IGNORECASE)
                    if table_match:
                        table_name = table_match.group(1)
                        if table_name not in full_scan_tables:
                            full_scan_tables.append(table_name)
                            warnings.append(f"Full table scan detected on {table_name}")
                            recommendations.append(f"Consider adding an index or bucket key on {table_name}")
            
            # Detect hash joins on large tables
            if "HASH JOIN" in plan_text.upper():
                warnings.append("Hash join detected - ensure adequate memory is available")
                recommendations.append("Consider adding bloom filters for join optimization")
            
            # Detect cross joins
            if "CROSS JOIN" in plan_text.upper():
                warnings.append("Cross join detected - may be very expensive")
                recommendations.append("Review join conditions to avoid Cartesian product")
            
            # Estimate cost based on plan complexity (heuristic)
            plan_lines = plan_text.count('\n')
            total_cost = float(plan_lines * 100)  # Rough heuristic
            
            # Try to extract cardinality from plan
            card_match = re.search(r"rows[=:](\d+)", plan_text, re.IGNORECASE)
            if card_match:
                total_cardinality = int(card_match.group(1))
            
            # Determine cost level
            cost_level = QueryCostEstimator._classify_cost(total_cost)
            
            # Add cost-based warnings
            if cost_level == CostLevel.HIGH:
                warnings.append(f"High query complexity detected")
                recommendations.append("Consider query rewriting or materialized views")
            elif cost_level == CostLevel.CRITICAL:
                warnings.append(f"CRITICAL query complexity")
                recommendations.append("Query may timeout - consider breaking into smaller queries")
            
            estimate = CostEstimate(
                total_cost=total_cost,
                cardinality=total_cardinality,
                cost_level=cost_level,
                has_full_table_scan=len(full_scan_tables) > 0,
                full_scan_tables=full_scan_tables,
                warnings=warnings,
                recommendations=recommendations,
                execution_plan=plan_text if include_plan else None,
            )
            
            logger.info(f"Doris cost estimation complete: cost={int(total_cost)}, level={cost_level.value}")
            return estimate
            
        except Exception as e:
            logger.error(f"Failed to parse Doris plan: {e}")
            return QueryCostEstimator._create_default_estimate()
    
    @staticmethod
    def _classify_cost(cost: float) -> CostLevel:
        """Classify cost into levels"""
        if cost < QueryCostEstimator.COST_LOW_THRESHOLD:
            return CostLevel.LOW
        elif cost < QueryCostEstimator.COST_MEDIUM_THRESHOLD:
            return CostLevel.MEDIUM
        elif cost < QueryCostEstimator.COST_HIGH_THRESHOLD:
            return CostLevel.HIGH
        else:
            return CostLevel.CRITICAL
    
    @staticmethod
    def _create_default_estimate() -> CostEstimate:
        """Create default estimate when cost estimation fails"""
        return CostEstimate(
            total_cost=0.0,
            cardinality=0,
            cost_level=CostLevel.LOW,
            has_full_table_scan=False,
            full_scan_tables=[],
            warnings=["Cost estimation unavailable"],
            recommendations=[],
        )
    
    @staticmethod
    async def _get_formatted_plan(connection_name: str) -> Optional[str]:
        """Get formatted execution plan using DBMS_XPLAN"""
        try:
            mcp_client = registry.get_mcp_client()
            if not mcp_client:
                return None
            
            plan_query = "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"
            result = await mcp_client.execute_sql(plan_query, connection_name)
            
            if result.get("status") == "success":
                rows = result.get("results", {}).get("rows", [])
                def get_first_value(r):
                    if isinstance(r, dict):
                        return next(iter(r.values())) if r else ""
                    return r[0] if r else ""
                return "\n".join([str(get_first_value(row)) for row in rows if row])
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get formatted plan: {e}")
            return None
