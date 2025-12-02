"""
Multi-Query Decomposition Service
Breaks down complex multi-part queries into sub-queries with DAG execution
"""

import logging
import uuid
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class SubQueryType(Enum):
    """Types of sub-queries in decomposition"""
    BASE = "base"  # Foundation query (no dependencies)
    DEPENDENT = "dependent"  # Depends on other sub-queries
    AGGREGATION = "aggregation"  # Aggregates results from multiple sub-queries
    COMPARISON = "comparison"  # Compares results across sub-queries


class MultiQueryDecompositionService:
    """Service for decomposing complex queries into executable sub-queries"""
    
    @staticmethod
    def detect_multi_part_query(user_query: str, intent: str) -> Tuple[bool, List[str]]:
        """
        Detect if query contains multiple distinct parts
        
        Args:
            user_query: Natural language query
            intent: Query intent classification
            
        Returns:
            (is_multi_part, list_of_parts)
        """
        multi_part_indicators = [
            # Explicit conjunctions
            " and then ", " then ", " followed by ",
            # Comparisons
            " compare ", " versus ", " vs ", " compared to ",
            # Multiple questions
            "? ", "also show", "along with", "as well as",
            # Temporal comparisons
            " and last ", " and previous ", " and prior ",
            # Multiple metrics
            " and also ", " additionally ",
        ]
        
        query_lower = user_query.lower()
        
        # Check for multiple question marks (strong indicator)
        if query_lower.count("?") > 1:
            parts = [p.strip() + "?" for p in query_lower.split("?") if p.strip()]
            return True, parts[:5]  # Limit to 5 sub-queries
        
        # Check for explicit indicators
        for indicator in multi_part_indicators:
            if indicator in query_lower:
                # Split on the indicator
                parts = [p.strip() for p in query_lower.split(indicator, 1)]
                if len(parts) > 1:
                    return True, parts[:2]  # Binary split for now
        
        # Check intent for complexity hints
        if intent:
            intent_lower = intent.lower()
            if any(word in intent_lower for word in ["multiple", "several", "various", "compare", "contrast"]):
                # Try to identify parts from intent structure
                return True, [user_query]  # Single part but flagged for careful handling
        
        return False, []
    
    @staticmethod
    def build_query_dag(
        user_query: str,
        parts: List[str],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build a DAG (Directed Acyclic Graph) structure for sub-query execution
        
        Args:
            user_query: Original query
            parts: Detected query parts
            context: Schema and other context
            
        Returns:
            DAG structure with nodes and edges
        """
        dag = {
            "dag_id": str(uuid.uuid4()),
            "original_query": user_query,
            "nodes": [],
            "edges": [],
            "execution_order": []
        }
        
        # Simple strategy: Create sequential execution plan
        for idx, part in enumerate(parts):
            node = {
                "node_id": f"subquery_{idx}",
                "query_text": part,
                "type": SubQueryType.BASE.value if idx == 0 else SubQueryType.DEPENDENT.value,
                "dependencies": [] if idx == 0 else [f"subquery_{idx-1}"],
                "status": "pending",
                "sql": None,
                "results": None,
                "error": None
            }
            dag["nodes"].append(node)
            
            # Add edge from previous node
            if idx > 0:
                dag["edges"].append({
                    "from": f"subquery_{idx-1}",
                    "to": f"subquery_{idx}",
                    "type": "sequential"
                })
        
        # Build execution order (topological sort for simple sequential case)
        dag["execution_order"] = [f"subquery_{i}" for i in range(len(parts))]
        
        logger.info(f"Built query DAG with {len(parts)} nodes")
        return dag
    
    @staticmethod
    async def execute_dag(
        dag: Dict[str, Any],
        state: Dict[str, Any],
        generate_sql_func,
        execute_query_func
    ) -> Dict[str, Any]:
        """
        Execute the query DAG in topological order
        
        Args:
            dag: DAG structure
            state: Current query state
            generate_sql_func: Function to generate SQL for sub-query
            execute_query_func: Function to execute SQL
            
        Returns:
            Execution results with all sub-query results
        """
        results = {
            "dag_id": dag["dag_id"],
            "sub_query_results": [],
            "combined_results": None,
            "success": True,
            "errors": []
        }
        
        # Execute in order
        for node_id in dag["execution_order"]:
            node = next((n for n in dag["nodes"] if n["node_id"] == node_id), None)
            if not node:
                logger.error(f"Node {node_id} not found in DAG")
                continue
            
            logger.info(f"Executing sub-query: {node_id}")
            
            try:
                # Create sub-state for this query
                sub_state = state.copy()
                sub_state["user_query"] = node["query_text"]
                sub_state["query_id"] = f"{state.get('query_id', 'unknown')}_{node_id}"
                
                # Check dependencies - inject previous results as context
                if node["dependencies"]:
                    dep_results = []
                    for dep_id in node["dependencies"]:
                        dep_node = next((n for n in dag["nodes"] if n["node_id"] == dep_id), None)
                        if dep_node and dep_node.get("results"):
                            dep_results.append({
                                "query": dep_node["query_text"],
                                "results": dep_node["results"]
                            })
                    
                    # Inject dependency context
                    if not sub_state.get("context"):
                        sub_state["context"] = {}
                    sub_state["context"]["previous_results"] = dep_results
                
                # Generate SQL for sub-query
                sql_result = await generate_sql_func(sub_state)
                node["sql"] = sql_result.get("sql_query", "")
                
                if not node["sql"]:
                    raise Exception("Failed to generate SQL for sub-query")
                
                # Execute sub-query
                exec_result = await execute_query_func(sql_result)
                node["results"] = exec_result.get("execution_result", {})
                node["status"] = "completed"
                
                results["sub_query_results"].append({
                    "node_id": node_id,
                    "query": node["query_text"],
                    "sql": node["sql"],
                    "results": node["results"],
                    "status": "success"
                })
                
                logger.info(f"Sub-query {node_id} completed successfully")
                
            except Exception as e:
                logger.error(f"Sub-query {node_id} failed: {e}")
                node["status"] = "failed"
                node["error"] = str(e)
                results["success"] = False
                results["errors"].append({
                    "node_id": node_id,
                    "error": str(e)
                })
                
                # Decide whether to continue or halt
                # For now, halt on first error
                break
        
        # Combine results if all succeeded
        if results["success"] and len(results["sub_query_results"]) > 0:
            results["combined_results"] = MultiQueryDecompositionService._combine_results(
                results["sub_query_results"]
            )
        
        return results
    
    @staticmethod
    def _combine_results(sub_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine results from multiple sub-queries
        
        Args:
            sub_results: List of sub-query results
            
        Returns:
            Combined result structure
        """
        combined = {
            "type": "multi_query_result",
            "sub_query_count": len(sub_results),
            "results": []
        }
        
        for sub in sub_results:
            combined["results"].append({
                "query": sub["query"],
                "data": sub["results"].get("data", []),
                "row_count": sub["results"].get("row_count", 0),
                "columns": sub["results"].get("columns", [])
            })
        
        # Calculate total row count across all sub-queries
        combined["total_rows"] = sum(
            r["results"].get("row_count", 0) for r in sub_results
        )
        
        return combined
    
    @staticmethod
    def should_decompose(user_query: str, intent: str, complexity: int = 50) -> bool:
        """
        Decide whether a query should be decomposed
        
        Args:
            user_query: Natural language query
            intent: Query intent
            complexity: Complexity score (0-100)
            
        Returns:
            True if query should be decomposed
        """
        # Detect multi-part
        is_multi, parts = MultiQueryDecompositionService.detect_multi_part_query(user_query, intent)
        
        if not is_multi:
            return False
        
        # Check if complexity warrants decomposition
        if complexity < 40:
            # Simple multi-part queries might not need decomposition
            return False
        
        # Check number of parts (too few or too many)
        if len(parts) < 2:
            return False
        if len(parts) > 5:
            # Too many parts - might be better as single complex query
            logger.warning(f"Query has {len(parts)} parts, might be too complex to decompose")
            return False
        
        return True
