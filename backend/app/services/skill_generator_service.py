"""
Skill Auto-Generation Service

Automatically generates YAML skills from successful query patterns.
Analyzes query history to identify common patterns and creates reusable skills.

Features:
- Pattern extraction from successful queries
- Business term inference
- YAML skill generation
- Skill effectiveness tracking
- Similarity-based deduplication

Usage:
    from app.services.skill_generator_service import skill_generator_service
    
    # Generate skill from successful query
    skill = await skill_generator_service.generate_skill_from_query(
        user_query="Show me sales by region",
        sql_query="SELECT region, SUM(sales) FROM orders GROUP BY region",
        schema_context={...}
    )
"""

import logging
import hashlib
import re
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)

# Feature flag
SKILL_GENERATION_ENABLED = getattr(settings, 'SKILL_GENERATION_ENABLED', True)

# Optional YAML import
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logger.warning("PyYAML not installed. Skill generation will be limited.")


class SkillType(Enum):
    """Types of auto-generated skills"""
    COLUMN_MAPPING = "column_mapping"  # Maps business terms to columns
    QUERY_PATTERN = "query_pattern"    # Common query patterns
    TABLE_JOIN = "table_join"          # Table relationship patterns
    AGGREGATION = "aggregation"        # Aggregation patterns
    FILTER = "filter"                  # Common filter patterns


@dataclass
class ExtractedPattern:
    """Extracted pattern from a query"""
    pattern_type: SkillType
    pattern_hash: str
    description: str
    tables: List[str]
    columns: List[str]
    sql_pattern: str  # Generalized SQL with placeholders
    business_terms: List[str]
    confidence: float
    example_queries: List[str] = field(default_factory=list)
    frequency: int = 1


@dataclass
class GeneratedSkill:
    """Auto-generated skill"""
    skill_id: str
    skill_type: SkillType
    name: str
    description: str
    yaml_content: str
    source_queries: List[str]
    confidence: float
    generated_at: str
    effectiveness_score: float = 0.0
    usage_count: int = 0


class SkillGeneratorService:
    """
    Service for automatically generating skills from successful queries.
    
    Analyzes query patterns to:
    - Extract column mappings from natural language to SQL
    - Identify common query patterns
    - Generate reusable YAML skills
    - Track skill effectiveness
    """
    
    # Redis key prefixes
    PATTERN_PREFIX = "skillgen:pattern:"
    SKILL_PREFIX = "skillgen:skill:"
    GENERATION_LOG_PREFIX = "skillgen:log:"
    
    # Configuration
    MIN_CONFIDENCE = 0.7
    MIN_FREQUENCY = 3  # Minimum occurrences to generate skill
    MAX_SKILLS_PER_TYPE = 50
    
    @classmethod
    async def analyze_query(
        cls,
        user_query: str,
        sql_query: str,
        schema_context: Dict[str, Any],
        execution_success: bool = True
    ) -> Optional[ExtractedPattern]:
        """
        Analyze a query to extract patterns for skill generation.
        
        Args:
            user_query: Natural language query
            sql_query: Generated SQL
            schema_context: Schema information used
            execution_success: Whether query executed successfully
            
        Returns:
            Extracted pattern or None
        """
        if not execution_success or not SKILL_GENERATION_ENABLED:
            return None
        
        try:
            # Extract tables
            tables = cls._extract_tables(sql_query)
            
            # Extract columns
            columns = cls._extract_columns(sql_query)
            
            # Generalize SQL pattern
            sql_pattern = cls._generalize_sql(sql_query)
            
            # Extract business terms
            business_terms = cls._extract_business_terms(user_query)
            
            # Determine pattern type
            pattern_type = cls._classify_pattern(sql_query, user_query)
            
            # Generate pattern hash
            pattern_hash = cls._hash_pattern(sql_pattern, tables)
            
            # Calculate confidence
            confidence = cls._calculate_confidence(
                user_query, sql_query, schema_context, execution_success
            )
            
            return ExtractedPattern(
                pattern_type=pattern_type,
                pattern_hash=pattern_hash,
                description=cls._generate_description(pattern_type, tables, columns),
                tables=tables,
                columns=columns,
                sql_pattern=sql_pattern,
                business_terms=business_terms,
                confidence=confidence,
                example_queries=[user_query]
            )
            
        except Exception as e:
            logger.error(f"Failed to analyze query: {e}")
            return None
    
    @classmethod
    async def record_pattern(
        cls,
        pattern: ExtractedPattern
    ) -> bool:
        """
        Record an extracted pattern for aggregation.
        
        Args:
            pattern: Extracted pattern
            
        Returns:
            True if recorded successfully
        """
        try:
            key = f"{cls.PATTERN_PREFIX}{pattern.pattern_type.value}:{pattern.pattern_hash}"
            
            # Get existing pattern data
            existing = await redis_client.get(key) or {}
            
            if existing:
                # Update existing pattern
                existing["frequency"] = existing.get("frequency", 0) + 1
                existing["example_queries"].append(pattern.example_queries[0])
                # Keep only last 10 examples
                existing["example_queries"] = existing["example_queries"][-10:]
                existing["last_seen"] = datetime.now(timezone.utc).isoformat()
                
                # Update confidence with weighted average
                old_conf = existing.get("confidence", 0)
                new_conf = pattern.confidence
                freq = existing["frequency"]
                existing["confidence"] = (old_conf * (freq - 1) + new_conf) / freq
            else:
                # New pattern
                existing = {
                    "pattern_type": pattern.pattern_type.value,
                    "pattern_hash": pattern.pattern_hash,
                    "description": pattern.description,
                    "tables": pattern.tables,
                    "columns": pattern.columns,
                    "sql_pattern": pattern.sql_pattern,
                    "business_terms": pattern.business_terms,
                    "confidence": pattern.confidence,
                    "example_queries": pattern.example_queries,
                    "frequency": 1,
                    "first_seen": datetime.now(timezone.utc).isoformat(),
                    "last_seen": datetime.now(timezone.utc).isoformat()
                }
            
            # Store with 30-day TTL
            await redis_client.set(key, existing, ttl=86400 * 30)
            
            logger.debug(f"Recorded pattern {pattern.pattern_hash} (freq: {existing['frequency']})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to record pattern: {e}")
            return False
    
    @classmethod
    async def generate_skills_from_patterns(
        cls,
        min_frequency: int = None,
        min_confidence: float = None
    ) -> List[GeneratedSkill]:
        """
        Generate skills from frequently occurring patterns.
        
        Args:
            min_frequency: Minimum frequency to generate skill
            min_confidence: Minimum confidence to generate skill
            
        Returns:
            List of generated skills
        """
        min_freq = min_frequency or cls.MIN_FREQUENCY
        min_conf = min_confidence or cls.MIN_CONFIDENCE
        
        generated_skills = []
        
        try:
            # Get all pattern keys
            pattern_keys = await redis_client.keys(f"{cls.PATTERN_PREFIX}*")
            
            for key in pattern_keys[:200]:  # Limit for performance
                try:
                    pattern_data = await redis_client.get(key)
                    if not pattern_data:
                        continue
                    
                    freq = pattern_data.get("frequency", 0)
                    conf = pattern_data.get("confidence", 0)
                    
                    if freq >= min_freq and conf >= min_conf:
                        skill = await cls._generate_skill_from_pattern(pattern_data)
                        if skill:
                            generated_skills.append(skill)
                            
                except Exception as e:
                    logger.warning(f"Failed to process pattern {key}: {e}")
                    continue
            
            logger.info(f"Generated {len(generated_skills)} skills from patterns")
            return generated_skills
            
        except Exception as e:
            logger.error(f"Failed to generate skills from patterns: {e}")
            return []
    
    @classmethod
    async def _generate_skill_from_pattern(
        cls,
        pattern_data: Dict[str, Any]
    ) -> Optional[GeneratedSkill]:
        """Generate a skill from pattern data"""
        try:
            pattern_type = SkillType(pattern_data["pattern_type"])
            
            # Generate skill based on type
            if pattern_type == SkillType.COLUMN_MAPPING:
                skill = cls._generate_column_mapping_skill(pattern_data)
            elif pattern_type == SkillType.QUERY_PATTERN:
                skill = cls._generate_query_pattern_skill(pattern_data)
            elif pattern_type == SkillType.TABLE_JOIN:
                skill = cls._generate_table_join_skill(pattern_data)
            elif pattern_type == SkillType.AGGREGATION:
                skill = cls._generate_aggregation_skill(pattern_data)
            else:
                skill = cls._generate_generic_skill(pattern_data)
            
            if skill:
                # Store generated skill
                skill_key = f"{cls.SKILL_PREFIX}{skill.skill_id}"
                await redis_client.set(skill_key, {
                    "skill_id": skill.skill_id,
                    "skill_type": skill.skill_type.value,
                    "name": skill.name,
                    "description": skill.description,
                    "yaml_content": skill.yaml_content,
                    "source_queries": skill.source_queries,
                    "confidence": skill.confidence,
                    "generated_at": skill.generated_at,
                    "effectiveness_score": skill.effectiveness_score,
                    "usage_count": skill.usage_count
                }, ttl=86400 * 90)  # 90 days
                
                return skill
                
        except Exception as e:
            logger.error(f"Failed to generate skill: {e}")
            return None
    
    @classmethod
    def _generate_column_mapping_skill(
        cls,
        pattern_data: Dict[str, Any]
    ) -> Optional[GeneratedSkill]:
        """Generate a column mapping skill"""
        try:
            business_terms = pattern_data.get("business_terms", [])
            columns = pattern_data.get("columns", [])
            tables = pattern_data.get("tables", [])
            
            if not business_terms or not columns:
                return None
            
            # Map first business term to first column
            term = business_terms[0]
            column = columns[0]
            table = tables[0] if tables else "unknown"
            
            skill_id = f"auto_colmap_{cls._hash_string(term)}"
            generated_at = datetime.now(timezone.utc).isoformat()
            
            yaml_content = f"""# Auto-generated column mapping skill
# Generated: {generated_at}
# Source queries: {pattern_data.get('frequency', 0)} occurrences

name: auto_generated_mappings
version: "1.0"
enabled: true

business_term_mappings:
  {term.lower()}:
    description: "Auto-generated mapping for '{term}'"
    synonyms: {business_terms[1:3] if len(business_terms) > 1 else "[]"}
    likely_columns:
      - {column}
    likely_tables:
      - {table}
    confidence: {pattern_data.get('confidence', 0.7):.2f}
"""
            
            return GeneratedSkill(
                skill_id=skill_id,
                skill_type=SkillType.COLUMN_MAPPING,
                name=f"Auto Column Map: {term}",
                description=f"Maps '{term}' to column '{column}'",
                yaml_content=yaml_content,
                source_queries=pattern_data.get("example_queries", []),
                confidence=pattern_data.get("confidence", 0.7),
                generated_at=generated_at
            )
            
        except Exception as e:
            logger.error(f"Failed to generate column mapping skill: {e}")
            return None
    
    @classmethod
    def _generate_query_pattern_skill(
        cls,
        pattern_data: Dict[str, Any]
    ) -> Optional[GeneratedSkill]:
        """Generate a query pattern skill"""
        try:
            sql_pattern = pattern_data.get("sql_pattern", "")
            description = pattern_data.get("description", "")
            
            skill_id = f"auto_pattern_{pattern_data['pattern_hash'][:16]}"
            generated_at = datetime.now(timezone.utc).isoformat()
            
            yaml_content = f"""# Auto-generated query pattern skill
# Generated: {generated_at}
# Source queries: {pattern_data.get('frequency', 0)} occurrences

name: auto_generated_patterns
version: "1.0"
enabled: true

query_patterns:
  - name: "{description[:50]}"
    description: "{description}"
    sql_template: |
      {sql_pattern}
    tables_required: {pattern_data.get('tables', [])}
    confidence: {pattern_data.get('confidence', 0.7):.2f}
"""
            
            return GeneratedSkill(
                skill_id=skill_id,
                skill_type=SkillType.QUERY_PATTERN,
                name=f"Pattern: {description[:40]}",
                description=description,
                yaml_content=yaml_content,
                source_queries=pattern_data.get("example_queries", []),
                confidence=pattern_data.get("confidence", 0.7),
                generated_at=generated_at
            )
            
        except Exception as e:
            logger.error(f"Failed to generate query pattern skill: {e}")
            return None
    
    @classmethod
    def _generate_table_join_skill(
        cls,
        pattern_data: Dict[str, Any]
    ) -> Optional[GeneratedSkill]:
        """Generate a table join skill"""
        try:
            tables = pattern_data.get("tables", [])
            
            if len(tables) < 2:
                return None
            
            skill_id = f"auto_join_{cls._hash_string('_'.join(tables))[:16]}"
            generated_at = datetime.now(timezone.utc).isoformat()
            
            yaml_content = f"""# Auto-generated table join skill
# Generated: {generated_at}
# Source queries: {pattern_data.get('frequency', 0)} occurrences

name: auto_generated_joins
version: "1.0"
enabled: true

table_relationships:
  - tables: {tables}
    join_type: "INNER"
    description: "Common join pattern for {', '.join(tables)}"
    confidence: {pattern_data.get('confidence', 0.7):.2f}
"""
            
            return GeneratedSkill(
                skill_id=skill_id,
                skill_type=SkillType.TABLE_JOIN,
                name=f"Join: {', '.join(tables[:2])}",
                description=f"Join pattern for {', '.join(tables)}",
                yaml_content=yaml_content,
                source_queries=pattern_data.get("example_queries", []),
                confidence=pattern_data.get("confidence", 0.7),
                generated_at=generated_at
            )
            
        except Exception as e:
            logger.error(f"Failed to generate table join skill: {e}")
            return None
    
    @classmethod
    def _generate_aggregation_skill(
        cls,
        pattern_data: Dict[str, Any]
    ) -> Optional[GeneratedSkill]:
        """Generate an aggregation skill"""
        try:
            columns = pattern_data.get("columns", [])
            sql_pattern = pattern_data.get("sql_pattern", "")
            
            skill_id = f"auto_agg_{pattern_data['pattern_hash'][:16]}"
            generated_at = datetime.now(timezone.utc).isoformat()
            
            yaml_content = f"""# Auto-generated aggregation skill
# Generated: {generated_at}
# Source queries: {pattern_data.get('frequency', 0)} occurrences

name: auto_generated_aggregations
version: "1.0"
enabled: true

aggregation_patterns:
  - description: "{pattern_data.get('description', '')}"
    sql_template: |
      {sql_pattern}
    group_by_columns: {columns}
    confidence: {pattern_data.get('confidence', 0.7):.2f}
"""
            
            return GeneratedSkill(
                skill_id=skill_id,
                skill_type=SkillType.AGGREGATION,
                name=f"Aggregation: {pattern_data.get('description', '')[:40]}",
                description=pattern_data.get("description", ""),
                yaml_content=yaml_content,
                source_queries=pattern_data.get("example_queries", []),
                confidence=pattern_data.get("confidence", 0.7),
                generated_at=generated_at
            )
            
        except Exception as e:
            logger.error(f"Failed to generate aggregation skill: {e}")
            return None
    
    @classmethod
    def _generate_generic_skill(
        cls,
        pattern_data: Dict[str, Any]
    ) -> Optional[GeneratedSkill]:
        """Generate a generic skill from pattern"""
        try:
            skill_id = f"auto_generic_{pattern_data['pattern_hash'][:16]}"
            generated_at = datetime.now(timezone.utc).isoformat()
            
            yaml_content = f"""# Auto-generated skill
# Generated: {generated_at}
# Source queries: {pattern_data.get('frequency', 0)} occurrences

name: auto_generated_skills
version: "1.0"
enabled: true

auto_generated:
  pattern_type: {pattern_data.get('pattern_type', 'unknown')}
  description: "{pattern_data.get('description', '')}"
  sql_pattern: |
    {pattern_data.get('sql_pattern', '')}
  confidence: {pattern_data.get('confidence', 0.7):.2f}
"""
            
            return GeneratedSkill(
                skill_id=skill_id,
                skill_type=SkillType(pattern_data.get("pattern_type", "query_pattern")),
                name=f"Auto: {pattern_data.get('description', '')[:40]}",
                description=pattern_data.get("description", ""),
                yaml_content=yaml_content,
                source_queries=pattern_data.get("example_queries", []),
                confidence=pattern_data.get("confidence", 0.7),
                generated_at=generated_at
            )
            
        except Exception as e:
            logger.error(f"Failed to generate generic skill: {e}")
            return None
    
    @classmethod
    async def get_generated_skills(
        cls,
        skill_type: Optional[SkillType] = None,
        min_confidence: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Get all generated skills with optional filtering"""
        try:
            skill_keys = await redis_client.keys(f"{cls.SKILL_PREFIX}*")
            skills = []
            
            for key in skill_keys:
                skill_data = await redis_client.get(key)
                if not skill_data:
                    continue
                
                # Filter by type if specified
                if skill_type and skill_data.get("skill_type") != skill_type.value:
                    continue
                
                # Filter by confidence
                if skill_data.get("confidence", 0) < min_confidence:
                    continue
                
                skills.append(skill_data)
            
            # Sort by confidence
            skills.sort(key=lambda x: x.get("confidence", 0), reverse=True)
            return skills
            
        except Exception as e:
            logger.error(f"Failed to get generated skills: {e}")
            return []
    
    @classmethod
    async def track_skill_effectiveness(
        cls,
        skill_id: str,
        used_successfully: bool
    ):
        """Track how effective a generated skill is"""
        try:
            key = f"{cls.SKILL_PREFIX}{skill_id}"
            skill_data = await redis_client.get(key)
            
            if skill_data:
                # Update usage count
                skill_data["usage_count"] = skill_data.get("usage_count", 0) + 1
                
                # Update effectiveness score (moving average)
                current_score = skill_data.get("effectiveness_score", 0.5)
                usage_count = skill_data["usage_count"]
                
                if used_successfully:
                    new_score = (current_score * (usage_count - 1) + 1.0) / usage_count
                else:
                    new_score = (current_score * (usage_count - 1) + 0.0) / usage_count
                
                skill_data["effectiveness_score"] = new_score
                
                # Update TTL
                await redis_client.set(key, skill_data, ttl=86400 * 90)
                
        except Exception as e:
            logger.warning(f"Failed to track skill effectiveness: {e}")
    
    # Helper methods
    
    @staticmethod
    def _extract_tables(sql: str) -> List[str]:
        """Extract table names from SQL"""
        tables = []
        sql_upper = sql.upper()
        
        # Match FROM clauses
        from_matches = re.findall(r'FROM\s+(\w+)', sql_upper)
        tables.extend(from_matches)
        
        # Match JOIN clauses
        join_matches = re.findall(r'JOIN\s+(\w+)', sql_upper)
        tables.extend(join_matches)
        
        return list(set(tables))
    
    @staticmethod
    def _extract_columns(sql: str) -> List[str]:
        """Extract column names from SQL"""
        columns = []
        
        # Match column references (simplified)
        col_matches = re.findall(r'(\w+)\.(\w+)', sql)
        columns.extend([col for _, col in col_matches])
        
        return list(set(columns))
    
    @staticmethod
    def _generalize_sql(sql: str) -> str:
        """Generalize SQL by replacing literals with placeholders"""
        # Replace string literals
        generalized = re.sub(r"'[^']*'", "'?'", sql)
        
        # Replace numbers
        generalized = re.sub(r'\b\d+\b', '?', generalized)
        
        # Normalize whitespace
        generalized = re.sub(r'\s+', ' ', generalized).strip()
        
        return generalized
    
    @staticmethod
    def _extract_business_terms(query: str) -> List[str]:
        """Extract potential business terms from natural language query"""
        # Common business terms to look for
        business_keywords = [
            'sales', 'revenue', 'profit', 'cost', 'budget', 'expense',
            'customer', 'client', 'user', 'employee', 'staff',
            'order', 'product', 'item', 'service',
            'region', 'territory', 'area', 'location',
            'month', 'quarter', 'year', 'date', 'period',
            'total', 'sum', 'average', 'count', 'maximum', 'minimum'
        ]
        
        query_lower = query.lower()
        found_terms = []
        
        for term in business_keywords:
            if term in query_lower:
                found_terms.append(term)
        
        return found_terms
    
    @staticmethod
    def _classify_pattern(sql: str, query: str) -> SkillType:
        """Classify the type of pattern"""
        sql_upper = sql.upper()
        query_lower = query.lower()
        
        # Check for aggregations
        if any(agg in sql_upper for agg in ['SUM(', 'AVG(', 'COUNT(', 'MAX(', 'MIN(']):
            return SkillType.AGGREGATION
        
        # Check for joins
        if 'JOIN' in sql_upper:
            return SkillType.TABLE_JOIN
        
        # Check for column mapping patterns
        if any(term in query_lower for term in ['show me', 'get me', 'find', 'what is']):
            return SkillType.COLUMN_MAPPING
        
        # Check for filters
        if 'WHERE' in sql_upper:
            return SkillType.FILTER
        
        return SkillType.QUERY_PATTERN
    
    @staticmethod
    def _hash_pattern(sql_pattern: str, tables: List[str]) -> str:
        """Generate hash for pattern"""
        content = f"{sql_pattern}:{','.join(sorted(tables))}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    @staticmethod
    def _hash_string(content: str) -> str:
        """Generate hash for string"""
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    @staticmethod
    def _calculate_confidence(
        user_query: str,
        sql_query: str,
        schema_context: Dict[str, Any],
        execution_success: bool
    ) -> float:
        """Calculate confidence score for pattern"""
        confidence = 0.5
        
        # Boost for successful execution
        if execution_success:
            confidence += 0.2
        
        # Boost for longer, more specific queries
        if len(user_query) > 20:
            confidence += 0.1
        
        # Boost if schema context was used
        if schema_context and schema_context.get("tables"):
            confidence += 0.1
        
        # Boost for well-formed SQL
        if sql_query.upper().startswith("SELECT"):
            confidence += 0.1
        
        return min(1.0, confidence)
    
    @staticmethod
    def _generate_description(pattern_type: SkillType, tables: List[str], columns: List[str]) -> str:
        """Generate human-readable description for pattern"""
        type_desc = {
            SkillType.COLUMN_MAPPING: "Column mapping pattern",
            SkillType.QUERY_PATTERN: "Query pattern",
            SkillType.TABLE_JOIN: "Table join pattern",
            SkillType.AGGREGATION: "Aggregation pattern",
            SkillType.FILTER: "Filter pattern"
        }
        
        base = type_desc.get(pattern_type, "Query pattern")
        
        if tables:
            base += f" on {', '.join(tables[:2])}"
        
        return base


# Global instance
skill_generator_service = SkillGeneratorService()


# Convenience functions

async def analyze_and_record_query(
    user_query: str,
    sql_query: str,
    schema_context: Dict[str, Any],
    execution_success: bool = True
) -> bool:
    """Analyze a query and record the pattern for skill generation"""
    pattern = await SkillGeneratorService.analyze_query(
        user_query=user_query,
        sql_query=sql_query,
        schema_context=schema_context,
        execution_success=execution_success
    )
    
    if pattern:
        return await SkillGeneratorService.record_pattern(pattern)
    
    return False


async def generate_skills_from_history() -> List[GeneratedSkill]:
    """Generate skills from recorded patterns"""
    return await SkillGeneratorService.generate_skills_from_patterns()


async def get_auto_generated_skills(
    skill_type: Optional[str] = None,
    min_confidence: float = 0.0
) -> List[Dict[str, Any]]:
    """Get auto-generated skills"""
    st = SkillType(skill_type) if skill_type else None
    return await SkillGeneratorService.get_generated_skills(st, min_confidence)
