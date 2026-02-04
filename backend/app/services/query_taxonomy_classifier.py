"""
Query Taxonomy Classifier

Classifies natural language queries into categories to optimize:
- Execution strategy selection
- LLM prompt templates
- HITL thresholds
- Result presentation format
- Caching strategies

Taxonomy categories:
1. Exploratory: "Show me all tables", "What data do we have?"
2. Targeted: "Get John's email", "Find order #12345"
3. Comparative: "Compare sales by region", "Which product is better?"
4. Trend/Time-series: "Show sales over time", "Growth trend?"
5. Aggregate: "Average sales", "Total revenue"
6. Diagnostic: "Why did sales drop?", "What went wrong?"
7. Predictive: "Forecast next quarter", "Will sales increase?"
8. Operational: "Update status", "Create new record"
9. Compliance/Security: "Who accessed this?", "Audit log"
10. Meta/Schema: "What columns in table X?", "Table relationships"

Output influences:
- Router (execution strategy)
- LLM (prompt template)
- UI (visualization type)
- HITL (approval threshold)
"""

import re
import logging
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Primary query classification types"""
    EXPLORATORY = "exploratory"
    TARGETED = "targeted"
    COMPARATIVE = "comparative"
    TREND_ANALYSIS = "trend_analysis"
    AGGREGATE = "aggregate"
    DIAGNOSTIC = "diagnostic"
    PREDICTIVE = "predictive"
    OPERATIONAL = "operational"
    COMPLIANCE = "compliance"
    META_SCHEMA = "meta_schema"


class ExecutionStrategy(Enum):
    """Recommended execution strategy"""
    FAST = "fast"  # Use cache, simple queries
    STANDARD = "standard"  # Normal execution
    THOROUGH = "thorough"  # Full context, no shortcuts
    CAREFUL = "careful"  # Extra validation, HITL likely


@dataclass
class TaxonomyResult:
    """Query taxonomy classification result"""
    primary_type: QueryType
    confidence: float
    secondary_types: List[QueryType]
    recommended_strategy: ExecutionStrategy
    complexity_score: float  # 0.0 - 1.0
    requires_context: bool


class QueryTaxonomyClassifier:
    """
    Classifies NL queries into taxonomy categories.
    
    Uses keyword matching, pattern detection, and heuristics to
    determine query intent and optimize downstream processing.
    """
    
    # Category patterns (keywords grouped by type)
    CATEGORY_PATTERNS = {
        QueryType.EXPLORATORY: {
            "keywords": ["what", "show", "list", "find", "browse", "explore",
                        "overview", "summary", "tell me about"],
            "weight": 1.0,
        },
        QueryType.TARGETED: {
            "keywords": ["specific", "exact", "number", "id", "code", "name",
                        "lookup", "retrieve", "get me", "where is"],
            "weight": 1.0,
        },
        QueryType.COMPARATIVE: {
            "keywords": ["compare", "versus", "vs", "difference", "better",
                        "higher", "lower", "rank", "top", "bottom"],
            "weight": 1.2,
        },
        QueryType.TREND_ANALYSIS: {
            "keywords": ["trend", "over time", "growth", "decline", "change",
                        "monthly", "weekly", "historical", "pattern"],
            "weight": 1.1,
        },
        QueryType.AGGREGATE: {
            "keywords": ["total", "sum", "average", "avg", "count", "min",
                        "max", "how many", "how much", "aggregate"],
            "weight": 1.0,
        },
        QueryType.DIAGNOSTIC: {
            "keywords": ["why", "reason", "cause", "problem", "issue",
                        "error", "failed", "wrong", "what happened"],
            "weight": 1.3,  # Higher complexity
        },
        QueryType.PREDICTIVE: {
            "keywords": ["predict", "forecast", "will", "future", "estimate",
                        "projection", "expected", "trend"],
            "weight": 1.4,  # Highest complexity
        },
        QueryType.OPERATIONAL: {
            "keywords": ["update", "insert", "create", "delete", "modify",
                        "change", "add", "remove", "set"],
            "weight": 1.5,  # Requires write permissions
        },
        QueryType.COMPLIANCE: {
            "keywords": ["audit", "log", "who accessed", "who modified",
                        "permission", "security", "compliance", "gdpr"],
            "weight": 1.2,
        },
        QueryType.META_SCHEMA: {
            "keywords": ["schema", "table structure", "columns in", "fields",
                        "metadata", "relationship", "foreign key"],
            "weight": 0.8,  # Usually simple
        },
    }
    
    # Complexity modifiers
    COMPLEXITY_INDICATORS = {
        "high": ["join", "multiple", "complex", "advanced", "across", "between"],
        "medium": ["and", "also", "plus", "with", "including"],
        "low": ["simple", "basic", "just", "only"]
    }
    
    @classmethod
    def classify(cls, query: str) -> TaxonomyResult:
        """
        Classify a natural language query.
        
        Args:
            query: Natural language query text
            
        Returns:
            TaxonomyResult with classification and recommendations
        """
        if not query or not isinstance(query, str):
            return TaxonomyResult(
                primary_type=QueryType.EXPLORATORY,
                confidence=0.0,
                secondary_types=[],
                recommended_strategy=ExecutionStrategy.STANDARD,
                complexity_score=0.5,
                requires_context=False
            )
        
        query_lower = query.lower()
        scores = {}
        
        # Score each category
        for query_type, patterns in cls.CATEGORY_PATTERNS.items():
            score = 0.0
            keywords_found = []
            
            for keyword in patterns["keywords"]:
                if keyword in query_lower:
                    score += 1.0
                    keywords_found.append(keyword)
            
            # Apply weight
            score *= patterns["weight"]
            
            # Boost for multiple keywords
            if len(keywords_found) > 1:
                score *= 1.2
            
            scores[query_type] = score
        
        # Determine primary and secondary types
        sorted_types = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        primary_type = sorted_types[0][0] if sorted_types else QueryType.EXPLORATORY
        primary_score = sorted_types[0][1] if sorted_types else 0
        
        # Get secondary types (within 50% of primary score)
        secondary_types = [
            qt for qt, score in sorted_types[1:] 
            if score > 0 and score >= primary_score * 0.5
        ][:2]  # Max 2 secondary types
        
        # Calculate confidence
        total_score = sum(scores.values())
        confidence = primary_score / total_score if total_score > 0 else 0.5
        
        # Calculate complexity
        complexity = cls._calculate_complexity(query_lower, primary_type)
        
        # Determine strategy
        strategy = cls._determine_strategy(primary_type, complexity)
        
        # Determine if context is required
        requires_context = primary_type in [
            QueryType.DIAGNOSTIC, 
            QueryType.PREDICTIVE, 
            QueryType.COMPARATIVE
        ]
        
        return TaxonomyResult(
            primary_type=primary_type,
            confidence=round(confidence, 2),
            secondary_types=secondary_types,
            recommended_strategy=strategy,
            complexity_score=round(complexity, 2),
            requires_context=requires_context
        )
    
    @classmethod
    def _calculate_complexity(cls, query: str, primary_type: QueryType) -> float:
        """Calculate query complexity score"""
        base_complexity = {
            QueryType.EXPLORATORY: 0.3,
            QueryType.TARGETED: 0.2,
            QueryType.AGGREGATE: 0.4,
            QueryType.COMPARATIVE: 0.5,
            QueryType.TREND_ANALYSIS: 0.5,
            QueryType.DIAGNOSTIC: 0.7,
            QueryType.PREDICTIVE: 0.8,
            QueryType.OPERATIONAL: 0.4,
            QueryType.COMPLIANCE: 0.6,
            QueryType.META_SCHEMA: 0.1,
        }.get(primary_type, 0.5)
        
        # Adjust based on query length
        word_count = len(query.split())
        if word_count > 20:
            base_complexity += 0.1
        elif word_count < 5:
            base_complexity -= 0.1
        
        # Adjust for complexity indicators
        for indicator in cls.COMPLEXITY_INDICATORS["high"]:
            if indicator in query:
                base_complexity += 0.15
        
        for indicator in cls.COMPLEXITY_INDICATORS["medium"]:
            if indicator in query:
                base_complexity += 0.05
        
        return min(1.0, max(0.0, base_complexity))
    
    @classmethod
    def _determine_strategy(cls, query_type: QueryType, complexity: float) -> ExecutionStrategy:
        """Determine execution strategy based on type and complexity"""
        if query_type == QueryType.OPERATIONAL:
            return ExecutionStrategy.CAREFUL
        
        if query_type in (QueryType.DIAGNOSTIC, QueryType.PREDICTIVE):
            return ExecutionStrategy.THOROUGH
        
        if query_type == QueryType.META_SCHEMA:
            return ExecutionStrategy.FAST
        
        if complexity < 0.3:
            return ExecutionStrategy.FAST
        elif complexity > 0.7:
            return ExecutionStrategy.THOROUGH
        
        return ExecutionStrategy.STANDARD
    
    @classmethod
    def get_type_description(cls, query_type: QueryType) -> str:
        """Get human-readable description of query type"""
        descriptions = {
            QueryType.EXPLORATORY: "Data exploration and discovery",
            QueryType.TARGETED: "Specific record lookup",
            QueryType.COMPARATIVE: "Comparison between entities or metrics",
            QueryType.TREND_ANALYSIS: "Time-series analysis and trends",
            QueryType.AGGREGATE: "Statistical aggregation",
            QueryType.DIAGNOSTIC: "Root cause analysis",
            QueryType.PREDICTIVE: "Forecasting and prediction",
            QueryType.OPERATIONAL: "Data modification",
            QueryType.COMPLIANCE: "Audit and security queries",
            QueryType.META_SCHEMA: "Schema/metadata queries",
        }
        return descriptions.get(query_type, "Unknown query type")


# Global instance
query_taxonomy_classifier = QueryTaxonomyClassifier()


# Convenience functions

def classify_query(query: str) -> TaxonomyResult:
    """Classify a query"""
    return QueryTaxonomyClassifier.classify(query)


def get_query_type(query: str) -> QueryType:
    """Get primary query type"""
    result = QueryTaxonomyClassifier.classify(query)
    return result.primary_type


def get_execution_strategy(query: str) -> ExecutionStrategy:
    """Get recommended execution strategy"""
    result = QueryTaxonomyClassifier.classify(query)
    return result.recommended_strategy