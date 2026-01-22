"""
Query Cost Estimation Service
Analyzes Oracle execution plans to estimate query cost and warn about expensive operations
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
        include_plan: bool = False,
    ) -> CostEstimate:
        """
        Estimate query execution cost using EXPLAIN PLAN
        
        Args:
            sql_query: SQL query to analyze
            connection_name: Database connection name
            include_plan: Whether to include full execution plan in result
            
        Returns:
            CostEstimate with cost analysis and warnings
        """
        logger.info(f"Estimating query cost...")
        
        try:
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
            
            logger.info(f"Cost estimation complete: cost={int(total_cost)}, level={cost_level.value}")
            return estimate
            
        except Exception as e:
            logger.error(f"Cost estimation failed: {e}")
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
