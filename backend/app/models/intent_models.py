"""
Structured Intent Models for Query Understanding

Defines Pydantic models for persisting structured query intent data,
enabling better observability and downstream processing.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class QueryTypeEnum(str, Enum):
    """Primary query classification types"""

    SELECT = "select"
    AGGREGATION = "aggregation"
    FILTERED = "filtered"
    JOINED = "joined"
    TIME_SERIES = "time-series"
    RANKED = "ranked"
    COMPARATIVE = "comparative"
    NESTED = "nested"


class ComplexityEnum(str, Enum):
    """Query complexity levels"""

    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


class DomainEnum(str, Enum):
    """Business domain classifications"""

    SALES = "sales"
    FINANCE = "finance"
    OPERATIONS = "operations"
    HR = "hr"
    MARKETING = "marketing"
    GENERAL = "general"


class TemporalEnum(str, Enum):
    """Temporal characteristics"""

    POINT_IN_TIME = "point_in_time"
    PERIOD = "period"
    TRAILING = "trailing"
    YTD = "ytd"
    MTD = "mtd"
    NONE = "none"


class CardinalityEnum(str, Enum):
    """Expected result cardinality"""

    SINGLE = "single"
    FEW = "few"
    MANY = "many"
    SUMMARY = "summary"


class FilterSpec(BaseModel):
    """Specification of a filter condition"""

    column: str = Field(..., description="Column being filtered")
    operator: str = Field(..., description="Filter operator (e.g., =, >, <, LIKE)")
    value: Optional[str] = Field(None, description="Filter value")
    value_type: str = Field("string", description="Type of the filter value")


class AggregationSpec(BaseModel):
    """Specification of an aggregation"""

    function: str = Field(
        ..., description="Aggregation function (COUNT, SUM, AVG, etc.)"
    )
    column: str = Field(..., description="Column being aggregated")
    alias: Optional[str] = Field(None, description="Result alias")


class EntitySpec(BaseModel):
    """Specification of an entity mentioned in the query"""

    name: str = Field(..., description="Entity name")
    type: str = Field(..., description="Entity type (table, column, value, etc.)")
    confidence: float = Field(1.0, description="Confidence score 0.0-1.0")


class StructuredIntent(BaseModel):
    """
    Structured representation of query intent.

    This model provides a canonical, typed representation of what the user
    is asking for, extracted during the 'understand' node of the orchestrator.

    Attributes:
        query_type: Primary query operation type
        complexity: Estimated complexity level
        domain: Business domain
        temporal: Time-based characteristics
        expected_cardinality: Expected result size
        tables: Tables referenced in the query
        entities: Named entities detected
        time_period: Specific time period mentioned
        aggregations: Aggregation operations requested
        filters: Filter conditions specified
        joins_count: Number of joins required
        source: Source of classification (llm or fallback)
        classifier_info: Additional info from classifier
        confidence: Overall confidence score 0.0-1.0
    """

    # Core classification
    query_type: QueryTypeEnum = Field(
        QueryTypeEnum.SELECT, description="Primary query operation type"
    )
    complexity: ComplexityEnum = Field(
        ComplexityEnum.MEDIUM, description="Estimated complexity level"
    )
    domain: DomainEnum = Field(DomainEnum.GENERAL, description="Business domain")
    temporal: TemporalEnum = Field(
        TemporalEnum.NONE, description="Time-based characteristics"
    )
    expected_cardinality: CardinalityEnum = Field(
        CardinalityEnum.FEW, description="Expected result size"
    )

    # Structured components
    tables: List[str] = Field(default_factory=list, description="Tables referenced")
    entities: List[EntitySpec] = Field(
        default_factory=list, description="Named entities detected"
    )
    time_period: Optional[str] = Field(
        None, description="Specific time period mentioned"
    )
    aggregations: List[AggregationSpec] = Field(
        default_factory=list, description="Aggregation operations"
    )
    filters: List[FilterSpec] = Field(
        default_factory=list, description="Filter conditions"
    )

    # Metadata
    joins_count: int = Field(0, ge=0, description="Number of joins required")
    source: str = Field(
        "fallback", description="Source of classification (llm or fallback)"
    )
    classifier_primary: Optional[str] = Field(
        None, description="Primary type from classifier"
    )
    classifier_strategy: Optional[str] = Field(
        None, description="Recommended strategy from classifier"
    )
    confidence: float = Field(
        0.5, ge=0.0, le=1.0, description="Overall confidence score"
    )

    # Additional metadata
    raw_entities: List[str] = Field(
        default_factory=list, description="Raw entity strings"
    )
    measures: List[str] = Field(
        default_factory=list, description="Business measures requested"
    )
    dimensions: List[str] = Field(
        default_factory=list, description="Dimensions for grouping"
    )

    class Config:
        """Pydantic configuration"""

        use_enum_values = True
        validate_assignment = True
        extra = "allow"  # Allow additional fields for extensibility


class IntentUnderstandingResult(BaseModel):
    """
    Complete result from the intent understanding node.

    Includes both the structured intent and metadata about the understanding process.
    """

    structured_intent: StructuredIntent = Field(
        ..., description="Structured intent representation"
    )
    raw_intent: Optional[str] = Field(None, description="Raw LLM response")
    intent_source: str = Field("fallback", description="Source: llm or fallback")
    processing_time_ms: Optional[int] = Field(
        None, description="Processing time in milliseconds"
    )
    validation_errors: List[str] = Field(
        default_factory=list, description="Any validation errors"
    )

    def to_state_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage in QueryState"""
        return {
            "structured": self.structured_intent.model_dump(),
            "raw": self.raw_intent,
            "source": self.intent_source,
            "processing_time_ms": self.processing_time_ms,
        }

    @classmethod
    def from_state_dict(cls, data: Dict[str, Any]) -> "IntentUnderstandingResult":
        """Create from QueryState dictionary"""
        structured_data = data.get("structured", {})
        return cls(
            structured_intent=StructuredIntent(**structured_data),
            raw_intent=data.get("raw"),
            intent_source=data.get("source", "fallback"),
            processing_time_ms=data.get("processing_time_ms"),
        )


# Convenience conversion functions


def taxonomy_to_structured_intent(taxonomy: Dict[str, Any]) -> StructuredIntent:
    """
    Convert legacy taxonomy dict to StructuredIntent model.

    Args:
        taxonomy: Dict from query_taxonomy_classifier or LLM response

    Returns:
        StructuredIntent model instance
    """
    # Map legacy values to enums
    query_type = taxonomy.get("query_type", "select")
    complexity = taxonomy.get("complexity", "medium")
    domain = taxonomy.get("domain", "general")
    temporal = taxonomy.get("temporal", "none")
    expected_cardinality = taxonomy.get("expected_cardinality", "few")

    # Parse entities
    raw_entities = taxonomy.get("entities", [])
    entities = []
    if isinstance(raw_entities, list):
        for entity in raw_entities:
            if isinstance(entity, str):
                entities.append(EntitySpec(name=entity, type="unknown"))
            elif isinstance(entity, dict):
                entities.append(EntitySpec(**entity))

    # Parse aggregations
    raw_aggregations = taxonomy.get("aggregations", [])
    aggregations = []
    if isinstance(raw_aggregations, list):
        for agg in raw_aggregations:
            if isinstance(agg, str):
                aggregations.append(AggregationSpec(function=agg, column="*"))
            elif isinstance(agg, dict):
                aggregations.append(AggregationSpec(**agg))

    # Parse filters
    raw_filters = taxonomy.get("filters", [])
    filters = []
    if isinstance(raw_filters, list):
        for filt in raw_filters:
            if isinstance(filt, str):
                # Try to parse simple filter string
                filters.append(FilterSpec(column=filt, operator="="))
            elif isinstance(filt, dict):
                filters.append(FilterSpec(**filt))

    return StructuredIntent(
        query_type=query_type
        if query_type in [e.value for e in QueryTypeEnum]
        else QueryTypeEnum.SELECT,
        complexity=complexity
        if complexity in [e.value for e in ComplexityEnum]
        else ComplexityEnum.MEDIUM,
        domain=domain
        if domain in [e.value for e in DomainEnum]
        else DomainEnum.GENERAL,
        temporal=temporal
        if temporal in [e.value for e in TemporalEnum]
        else TemporalEnum.NONE,
        expected_cardinality=expected_cardinality
        if expected_cardinality in [e.value for e in CardinalityEnum]
        else CardinalityEnum.FEW,
        tables=taxonomy.get("tables", []),
        entities=entities,
        time_period=taxonomy.get("time_period"),
        aggregations=aggregations,
        filters=filters,
        joins_count=taxonomy.get("joins_count", 0),
        source=taxonomy.get("source", "fallback"),
        classifier_primary=taxonomy.get("classifier_primary"),
        classifier_strategy=taxonomy.get("classifier_strategy"),
        confidence=0.8 if taxonomy.get("source") == "llm" else 0.5,
        raw_entities=raw_entities if isinstance(raw_entities, list) else [],
        measures=taxonomy.get("measures", []),
        dimensions=taxonomy.get("dimensions", []),
    )


def structured_intent_to_taxonomy(intent: StructuredIntent) -> Dict[str, Any]:
    """
    Convert StructuredIntent back to legacy taxonomy dict format.

    Args:
        intent: StructuredIntent model instance

    Returns:
        Dict in legacy taxonomy format
    """
    return {
        "query_type": intent.query_type,
        "complexity": intent.complexity,
        "domain": intent.domain,
        "temporal": intent.temporal,
        "expected_cardinality": intent.expected_cardinality,
        "tables": intent.tables,
        "entities": [e.name for e in intent.entities] or intent.raw_entities,
        "time_period": intent.time_period,
        "aggregations": [
            {"function": a.function, "column": a.column, "alias": a.alias}
            for a in intent.aggregations
        ]
        if intent.aggregations
        else [],
        "filters": [
            {"column": f.column, "operator": f.operator, "value": f.value}
            for f in intent.filters
        ]
        if intent.filters
        else [],
        "joins_count": intent.joins_count,
        "source": intent.source,
        "classifier_primary": intent.classifier_primary,
        "classifier_strategy": intent.classifier_strategy,
        "measures": intent.measures,
        "dimensions": intent.dimensions,
    }
