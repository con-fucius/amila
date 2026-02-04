"""
Skill Generator Pattern Library
Predefined patterns for common skill generation scenarios
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class SkillPatternLibrary:
    """
    Library of predefined skill generation patterns.
    Enables quick skill creation from templates.
    """
    
    PATTERNS = {
        "data_quality_check": {
            "name": "Data Quality Validation",
            "description": "Automated data quality checks for tables",
            "category": "data_quality",
            "template": {
                "input_schema": {
                    "table_name": "string",
                    "columns": "array",
                    "quality_rules": "object"
                },
                "logic": """
def check_data_quality(table_name, columns, quality_rules):
    issues = []
    
    # Check for nulls
    if quality_rules.get('check_nulls'):
        null_counts = query(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")
        if null_counts > 0:
            issues.append(f"Found {null_counts} null values in {col}")
    
    # Check for duplicates
    if quality_rules.get('check_duplicates'):
        dup_query = f"SELECT {col}, COUNT(*) as cnt FROM {table_name} GROUP BY {col} HAVING cnt > 1"
        duplicates = query(dup_query)
        if duplicates:
            issues.append(f"Found duplicate values in {col}")
    
    return {"status": "success" if not issues else "warning", "issues": issues}
""",
                "output_format": "json"
            }
        },
        
        "schema_drift_detection": {
            "name": "Schema Drift Detection",
            "description": "Detect schema changes between environments",
            "category": "schema_management",
            "template": {
                "input_schema": {
                    "source_table": "string",
                    "target_table": "string"
                },
                "logic": """
def detect_schema_drift(source_table, target_table):
    source_schema = get_schema(source_table)
    target_schema = get_schema(target_table)
    
    differences = {
        "added_columns": [],
        "removed_columns": [],
        "type_changes": []
    }
    
    source_cols = {col['name']: col for col in source_schema}
    target_cols = {col['name']: col for col in target_schema}
    
    for col_name in source_cols:
        if col_name not in target_cols:
            differences['removed_columns'].append(col_name)
        elif source_cols[col_name]['type'] != target_cols[col_name]['type']:
            differences['type_changes'].append({
                'column': col_name,
                'source_type': source_cols[col_name]['type'],
                'target_type': target_cols[col_name]['type']
            })
    
    for col_name in target_cols:
        if col_name not in source_cols:
            differences['added_columns'].append(col_name)
    
    return differences
""",
                "output_format": "json"
            }
        },
        
        "cost_anomaly_detection": {
            "name": "Cost Anomaly Detection",
            "description": "Detect unusual query cost patterns",
            "category": "cost_governance",
            "template": {
                "input_schema": {
                    "lookback_days": "integer",
                    "threshold_std_dev": "float"
                },
                "logic": """
def detect_cost_anomalies(lookback_days=30, threshold_std_dev=2.0):
    import statistics
    
    query = f'''
        SELECT query_date, SUM(cost_usd) as daily_cost
        FROM query_history
        WHERE query_date >= CURRENT_DATE - {lookback_days}
        GROUP BY query_date
    '''
    
    daily_costs = execute_query(query)
    costs = [row['daily_cost'] for row in daily_costs]
    
    mean_cost = statistics.mean(costs)
    std_dev = statistics.stdev(costs)
    
    anomalies = []
    for row in daily_costs:
        z_score = (row['daily_cost'] - mean_cost) / std_dev
        if abs(z_score) > threshold_std_dev:
            anomalies.append({
                'date': row['query_date'],
                'cost': row['daily_cost'],
                'z_score': z_score,
                'severity': 'high' if abs(z_score) > 3 else 'medium'
            })
    
    return {
        'mean_cost': mean_cost,
        'std_dev': std_dev,
        'anomalies': anomalies
    }
""",
                "output_format": "json"
            }
        },
        
        "table_usage_analysis": {
            "name": "Table Usage Analysis",
            "description": "Analyze table access patterns and usage frequency",
            "category": "analytics",
            "template": {
                "input_schema": {
                    "table_name": "string",
                    "time_period_days": "integer"
                },
                "logic": """
def analyze_table_usage(table_name, time_period_days=30):
    query = f'''
        SELECT 
            DATE_TRUNC('day', query_timestamp) as day,
            COUNT(*) as query_count,
            COUNT(DISTINCT user_id) as unique_users,
            AVG(execution_time_ms) as avg_execution_time
        FROM query_history
        WHERE table_name = '{table_name}'
        AND query_timestamp >= CURRENT_DATE - {time_period_days}
        GROUP BY day
        ORDER BY day
    '''
    
    usage_data = execute_query(query)
    
    total_queries = sum(row['query_count'] for row in usage_data)
    total_users = len(set(row['unique_users'] for row in usage_data))
    
    return {
        'table_name': table_name,
        'period_days': time_period_days,
        'total_queries': total_queries,
        'unique_users': total_users,
        'daily_usage': usage_data,
        'avg_queries_per_day': total_queries / time_period_days
    }
""",
                "output_format": "json"
            }
        },
        
        "stale_data_detector": {
            "name": "Stale Data Detector",
            "description": "Identify tables with outdated data",
            "category": "data_quality",
            "template": {
                "input_schema": {
                    "timestamp_column": "string",
                    "staleness_threshold_hours": "integer"
                },
                "logic": """
def detect_stale_data(timestamp_column='updated_at', staleness_threshold_hours=24):
    query = f'''
        SELECT 
            table_name,
            MAX({timestamp_column}) as last_update,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({timestamp_column}))) / 3600 as hours_since_update
        FROM information_schema.tables
        WHERE {timestamp_column} IS NOT NULL
        GROUP BY table_name
        HAVING hours_since_update > {staleness_threshold_hours}
        ORDER BY hours_since_update DESC
    '''
    
    stale_tables = execute_query(query)
    
    return {
        'threshold_hours': staleness_threshold_hours,
        'stale_tables_count': len(stale_tables),
        'stale_tables': stale_tables
    }
""",
                "output_format": "json"
            }
        },
        
        "query_optimization_suggester": {
            "name": "Query Optimization Suggester",
            "description": "Suggest optimizations for slow queries",
            "category": "performance",
            "template": {
                "input_schema": {
                    "min_execution_time_ms": "integer",
                    "lookback_days": "integer"
                },
                "logic": """
def suggest_query_optimizations(min_execution_time_ms=5000, lookback_days=7):
    query = f'''
        SELECT 
            query_id,
            sql_text,
            execution_time_ms,
            rows_scanned,
            rows_returned
        FROM query_history
        WHERE execution_time_ms > {min_execution_time_ms}
        AND query_timestamp >= CURRENT_DATE - {lookback_days}
        ORDER BY execution_time_ms DESC
        LIMIT 10
    '''
    
    slow_queries = execute_query(query)
    
    suggestions = []
    for q in slow_queries:
        suggestion = {'query_id': q['query_id'], 'optimizations': []}
        
        # Check for missing WHERE clause
        if 'WHERE' not in q['sql_text'].upper():
            suggestion['optimizations'].append('Add WHERE clause to filter data')
        
        # Check for SELECT *
        if 'SELECT *' in q['sql_text'].upper():
            suggestion['optimizations'].append('Specify columns instead of SELECT *')
        
        # Check for inefficient scans
        if q['rows_scanned'] / max(q['rows_returned'], 1) > 100:
            suggestion['optimizations'].append('High scan ratio - consider adding indexes')
        
        suggestions.append(suggestion)
    
    return {'slow_queries': len(slow_queries), 'suggestions': suggestions}
""",
                "output_format": "json"
            }
        }
    }
    
    @staticmethod
    def get_pattern(pattern_id: str) -> Optional[Dict[str, Any]]:
        """Get pattern by ID"""
        return SkillPatternLibrary.PATTERNS.get(pattern_id)
    
    @staticmethod
    def list_patterns(category: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all patterns, optionally filtered by category"""
        patterns = []
        for pattern_id, pattern in SkillPatternLibrary.PATTERNS.items():
            if category is None or pattern.get("category") == category:
                patterns.append({
                    "id": pattern_id,
                    "name": pattern["name"],
                    "description": pattern["description"],
                    "category": pattern["category"]
                })
        return patterns
    
    @staticmethod
    def get_categories() -> List[str]:
        """Get all unique categories"""
        categories = set()
        for pattern in SkillPatternLibrary.PATTERNS.values():
            categories.add(pattern.get("category", "uncategorized"))
        return sorted(list(categories))
    
    @staticmethod
    def instantiate_pattern(
        pattern_id: str,
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a skill instance from a pattern.
        
        Args:
            pattern_id: Pattern identifier
            parameters: Parameter values for the pattern
            
        Returns:
            Instantiated skill configuration
        """
        pattern = SkillPatternLibrary.get_pattern(pattern_id)
        
        if not pattern:
            return {
                "status": "error",
                "error": f"Pattern {pattern_id} not found"
            }
        
        return {
            "status": "success",
            "skill": {
                "name": pattern["name"],
                "description": pattern["description"],
                "category": pattern["category"],
                "logic": pattern["template"]["logic"],
                "parameters": parameters,
                "output_format": pattern["template"]["output_format"]
            }
        }


# Singleton instance
skill_pattern_library = SkillPatternLibrary()
