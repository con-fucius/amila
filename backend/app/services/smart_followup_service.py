"""
Smart Follow-Up Suggestions Service

Generates intelligent follow-up queries after a user receives results.
Analyzes query context, results, and user patterns to suggest next steps.

Features:
- Context-aware follow-up suggestions
- Drill-down recommendations
- Comparison suggestions
- Time-based follow-ups
- Related metric suggestions
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.services.query_taxonomy_classifier import QueryTaxonomyClassifier, QueryType

logger = logging.getLogger(__name__)


@dataclass
class FollowUpSuggestion:
    """A follow-up query suggestion"""
    suggestion_id: str
    query_text: str
    description: str
    category: str
    confidence: float
    requires_clarification: bool
    estimated_result_type: str
    icon: str


class SmartFollowUpService:
    """
    Service for generating smart follow-up suggestions.
    
    Analyzes:
    - Current query results
    - Query taxonomy classification
    - Schema context
    - User query history
    """
    
    # Follow-up categories
    CATEGORIES = {
        "drill_down": {
            "icon": "🔍",
            "description": "Explore details"
        },
        "compare": {
            "icon": "📊",
            "description": "Compare data"
        },
        "time": {
            "icon": "📅",
            "description": "Time analysis"
        },
        "aggregate": {
            "icon": "📈",
            "description": "Summarize"
        },
        "filter": {
            "icon": "🔎",
            "description": "Filter further"
        },
        "export": {
            "icon": "📥",
            "description": "Export data"
        }
    }
    
    def __init__(self):
        self.taxonomy_classifier = QueryTaxonomyClassifier()
    
    async def generate_follow_ups(
        self,
        original_query: str,
        sql_query: str,
        results: Dict[str, Any],
        user_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[FollowUpSuggestion]:
        """
        Generate smart follow-up suggestions.
        
        Args:
            original_query: Original natural language query
            sql_query: Generated SQL query
            results: Query results
            user_id: User identifier
            context: Additional context
            
        Returns:
            List of follow-up suggestions
        """
        suggestions = []
        context = context or {}
        
        # Classify the original query
        taxonomy = self.taxonomy_classifier.classify(original_query)
        
        # Extract result metadata
        result_metadata = self._extract_result_metadata(results)
        
        # Generate category-specific suggestions
        drill_down = self._generate_drill_down_suggestions(
            original_query, sql_query, result_metadata, taxonomy
        )
        suggestions.extend(drill_down)
        
        compare = self._generate_comparison_suggestions(
            original_query, sql_query, result_metadata, taxonomy
        )
        suggestions.extend(compare)
        
        time = self._generate_time_based_suggestions(
            original_query, sql_query, result_metadata, taxonomy
        )
        suggestions.extend(time)
        
        aggregate = self._generate_aggregation_suggestions(
            original_query, sql_query, result_metadata, taxonomy
        )
        suggestions.extend(aggregate)
        
        filter_suggestions = self._generate_filter_suggestions(
            original_query, sql_query, result_metadata
        )
        suggestions.extend(filter_suggestions)
        
        # Add export suggestion
        suggestions.append(self._create_export_suggestion())
        
        # Sort by confidence and take top 5
        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        return suggestions[:5]
    
    def _extract_result_metadata(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from query results"""
        metadata = {
            "row_count": results.get("row_count", 0),
            "columns": results.get("columns", []),
            "has_numeric": False,
            "has_date": False,
            "has_categories": False,
            "category_columns": [],
            "numeric_columns": [],
            "date_columns": []
        }
        
        for col in metadata["columns"]:
            col_lower = col.lower()
            
            # Check for date columns
            if any(d in col_lower for d in ["date", "time", "month", "year", "day"]):
                metadata["date_columns"].append(col)
                metadata["has_date"] = True
            
            # Check for numeric columns
            elif any(n in col_lower for n in ["amount", "total", "count", "sum", "avg", "price", "cost", "qty"]):
                metadata["numeric_columns"].append(col)
                metadata["has_numeric"] = True
            
            # Check for category columns
            elif any(c in col_lower for c in ["type", "category", "status", "name", "code", "group"]):
                metadata["category_columns"].append(col)
                metadata["has_categories"] = True
        
        return metadata
    
    def _generate_drill_down_suggestions(
        self,
        original_query: str,
        sql_query: str,
        metadata: Dict[str, Any],
        taxonomy
    ) -> List[FollowUpSuggestion]:
        """Generate drill-down suggestions"""
        suggestions = []
        
        if metadata["row_count"] > 1 and metadata["has_categories"]:
            for col in metadata["category_columns"][:2]:
                query = f"Show details for each {col}"
                suggestions.append(FollowUpSuggestion(
                    suggestion_id=f"drill_{col}",
                    query_text=query,
                    description=f"Break down by {col}",
                    category="drill_down",
                    confidence=0.85,
                    requires_clarification=False,
                    estimated_result_type="detailed_list",
                    icon=self.CATEGORIES["drill_down"]["icon"]
                ))
        
        # Add "Show top 10" if results are large
        if metadata["row_count"] > 10:
            suggestions.append(FollowUpSuggestion(
                suggestion_id="drill_top10",
                query_text="Show top 10 results",
                description="Limit to top 10 records",
                category="drill_down",
                confidence=0.75,
                requires_clarification=False,
                estimated_result_type="filtered_list",
                icon=self.CATEGORIES["drill_down"]["icon"]
            ))
        
        return suggestions
    
    def _generate_comparison_suggestions(
        self,
        original_query: str,
        sql_query: str,
        metadata: Dict[str, Any],
        taxonomy
    ) -> List[FollowUpSuggestion]:
        """Generate comparison suggestions"""
        suggestions = []
        
        # Suggest period comparison if temporal
        if metadata["has_date"]:
            suggestions.append(FollowUpSuggestion(
                suggestion_id="compare_periods",
                query_text="Compare with previous period",
                description="Compare current vs previous period",
                category="compare",
                confidence=0.80,
                requires_clarification=False,
                estimated_result_type="comparison",
                icon=self.CATEGORIES["compare"]["icon"]
            ))
            
            suggestions.append(FollowUpSuggestion(
                suggestion_id="compare_yoy",
                query_text="Compare year over year",
                description="Compare with same period last year",
                category="compare",
                confidence=0.75,
                requires_clarification=False,
                estimated_result_type="comparison",
                icon=self.CATEGORIES["compare"]["icon"]
            ))
        
        # Suggest category comparison
        if metadata["has_categories"] and len(metadata["category_columns"]) > 0:
            cat_col = metadata["category_columns"][0]
            suggestions.append(FollowUpSuggestion(
                suggestion_id=f"compare_{cat_col}",
                query_text=f"Compare across different {cat_col}s",
                description=f"Compare metrics by {cat_col}",
                category="compare",
                confidence=0.78,
                requires_clarification=False,
                estimated_result_type="comparison",
                icon=self.CATEGORIES["compare"]["icon"]
            ))
        
        return suggestions
    
    def _generate_time_based_suggestions(
        self,
        original_query: str,
        sql_query: str,
        metadata: Dict[str, Any],
        taxonomy
    ) -> List[FollowUpSuggestion]:
        """Generate time-based suggestions"""
        suggestions = []
        
        if metadata["has_date"]:
            suggestions.append(FollowUpSuggestion(
                suggestion_id="trend_monthly",
                query_text="Show monthly trend",
                description="View as monthly trend",
                category="time",
                confidence=0.82,
                requires_clarification=False,
                estimated_result_type="time_series",
                icon=self.CATEGORIES["time"]["icon"]
            ))
            
            suggestions.append(FollowUpSuggestion(
                suggestion_id="trend_rolling",
                query_text="Show 12-month rolling average",
                description="View 12-month rolling trend",
                category="time",
                confidence=0.75,
                requires_clarification=False,
                estimated_result_type="time_series",
                icon=self.CATEGORIES["time"]["icon"]
            ))
        
        # Suggest different time period
        if "last" in original_query.lower() or "this" in original_query.lower():
            suggestions.append(FollowUpSuggestion(
                suggestion_id="time_different",
                query_text="Show for different time period",
                description="Adjust the time period",
                category="time",
                confidence=0.70,
                requires_clarification=True,
                estimated_result_type="filtered_list",
                icon=self.CATEGORIES["time"]["icon"]
            ))
        
        return suggestions
    
    def _generate_aggregation_suggestions(
        self,
        original_query: str,
        sql_query: str,
        metadata: Dict[str, Any],
        taxonomy
    ) -> List[FollowUpSuggestion]:
        """Generate aggregation suggestions"""
        suggestions = []
        
        # If query was detailed, suggest summary
        if taxonomy.primary_type in [QueryType.TARGETED, QueryType.EXPLORATORY]:
            if metadata["has_numeric"]:
                suggestions.append(FollowUpSuggestion(
                    suggestion_id="agg_summary",
                    query_text="Show summary statistics",
                    description="View totals, averages, min/max",
                    category="aggregate",
                    confidence=0.80,
                    requires_clarification=False,
                    estimated_result_type="summary",
                    icon=self.CATEGORIES["aggregate"]["icon"]
                ))
        
        # If query was aggregated, suggest breakdown
        if taxonomy.primary_type in [QueryType.AGGREGATE]:
            if metadata["has_categories"]:
                suggestions.append(FollowUpSuggestion(
                    suggestion_id="agg_breakdown",
                    query_text="Show breakdown by subcategory",
                    description="Drill into subcategories",
                    category="aggregate",
                    confidence=0.75,
                    requires_clarification=False,
                    estimated_result_type="detailed_list",
                    icon=self.CATEGORIES["aggregate"]["icon"]
                ))
        
        return suggestions
    
    def _generate_filter_suggestions(
        self,
        original_query: str,
        sql_query: str,
        metadata: Dict[str, Any]
    ) -> List[FollowUpSuggestion]:
        """Generate filter suggestions"""
        suggestions = []
        
        # Suggest filtering by categories
        if metadata["has_categories"]:
            for col in metadata["category_columns"][:1]:
                suggestions.append(FollowUpSuggestion(
                    suggestion_id=f"filter_{col}",
                    query_text=f"Filter by specific {col}",
                    description=f"Focus on particular {col}",
                    category="filter",
                    confidence=0.70,
                    requires_clarification=True,
                    estimated_result_type="filtered_list",
                    icon=self.CATEGORIES["filter"]["icon"]
                ))
        
        # Suggest top/bottom filter
        if metadata["has_numeric"]:
            suggestions.append(FollowUpSuggestion(
                suggestion_id="filter_top",
                query_text="Show top performers",
                description="Filter to top values",
                category="filter",
                confidence=0.72,
                requires_clarification=False,
                estimated_result_type="filtered_list",
                icon=self.CATEGORIES["filter"]["icon"]
            ))
        
        return suggestions
    
    def _create_export_suggestion(self) -> FollowUpSuggestion:
        """Create export suggestion"""
        return FollowUpSuggestion(
            suggestion_id="export_data",
            query_text="Export these results",
            description="Download as CSV/Excel",
            category="export",
            confidence=0.65,
            requires_clarification=False,
            estimated_result_type="export",
            icon=self.CATEGORIES["export"]["icon"]
        )
    
    async def personalize_suggestions(
        self,
        suggestions: List[FollowUpSuggestion],
        user_id: str
    ) -> List[FollowUpSuggestion]:
        """
        Personalize suggestions based on user history.
        
        Args:
            suggestions: Base suggestions
            user_id: User identifier
            
        Returns:
            Personalized and re-ranked suggestions
        """
        try:
            # Get user's recent query patterns
            history_key = f"followup:history:{user_id}"
            history = await redis_client.get(history_key) or []
            
            # Boost confidence for previously used patterns
            for suggestion in suggestions:
                for past in history[-20:]:  # Look at last 20
                    if suggestion.category == past.get("category"):
                        suggestion.confidence = min(1.0, suggestion.confidence + 0.1)
            
            # Re-sort by adjusted confidence
            suggestions.sort(key=lambda x: x.confidence, reverse=True)
            
        except Exception as e:
            logger.warning(f"Failed to personalize suggestions: {e}")
        
        return suggestions
    
    async def record_follow_up_usage(
        self,
        user_id: str,
        suggestion: FollowUpSuggestion,
        was_successful: bool
    ):
        """Record follow-up usage for personalization"""
        try:
            history_key = f"followup:history:{user_id}"
            
            entry = {
                "suggestion_id": suggestion.suggestion_id,
                "category": suggestion.category,
                "successful": was_successful,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            await redis_client.lpush(history_key, entry, ttl=90 * 24 * 3600)  # 90 days
        except Exception as e:
            logger.warning(f"Failed to record follow-up usage: {e}")


# Global instance
smart_followup_service = SmartFollowUpService()


# Convenience functions

async def get_smart_followups(
    original_query: str,
    sql_query: str,
    results: Dict[str, Any],
    user_id: str
) -> List[FollowUpSuggestion]:
    """Get smart follow-up suggestions"""
    return await smart_followup_service.generate_follow_ups(
        original_query, sql_query, results, user_id
    )