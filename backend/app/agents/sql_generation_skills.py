"""
SQL Generation Skills - Anthropic's Skills Pattern for Accurate SQL Generation

Implements discrete, composable skills for SQL generation:
1. SchemaAnalysisSkill - Identifies relevant tables and columns
2. ColumnMappingSkill - Maps user concepts to physical columns OR derived expressions
3. SQLGenerationSkill - Generates SQL using validated column mappings
4. ValidationSkill - Validates and self-corrects SQL

Based on Anthropic's Agent Skills best practices (Oct 2025):
- Each skill has clear inputs/outputs
- Skills are testable and composable
- Skills validate their own outputs
- Context flows through skills
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from difflib import SequenceMatcher

from app.utils.oracle_identifiers import format_qualified_identifier

logger = logging.getLogger(__name__)

# Generic abbreviation patterns - domain-agnostic
# These are common business/database abbreviations that work across industries
COMMON_ABBREVIATIONS = {
    'date': ['DATE', 'DT', 'TIME', 'TIMESTAMP', 'TS'],
    'month': ['MONTH', 'MON', 'MM', 'MTH'],
    'year': ['YEAR', 'YR', 'YYYY'],
    'quarter': ['QUARTER', 'QTR', 'Q'],
    'id': ['ID', 'IDENT', 'IDENTIFIER', 'KEY'],
    'name': ['NAME', 'NM', 'LABEL', 'TITLE'],
    'number': ['NUMBER', 'NUM', 'NR', 'NO'],
    'count': ['COUNT', 'CNT', 'NR'],
    'average': ['AVERAGE', 'AVG', 'MEAN'],
    'total': ['TOTAL', 'TOT', 'SUM'],
    'amount': ['AMOUNT', 'AMT'],
    'value': ['VALUE', 'VAL'],
    'type': ['TYPE', 'TYP'],
    'status': ['STATUS', 'STAT', 'STS'],
    'description': ['DESCRIPTION', 'DESC', 'DESCR'],
}

# Generic semantic synonyms - domain-agnostic
COMMON_SYNONYMS = {
    'date': ['date', 'timestamp', 'time'],
    'quarter': ['quarter', 'qtr', 'q'],
    'month': ['month', 'mon', 'mm'],
    'year': ['year', 'yyyy', 'yr'],
    'customer': ['customer', 'client', 'user'],
    'amount': ['amount', 'value', 'total'],
}


class ColumnMappingType(Enum):
    """Types of column mappings"""
    PHYSICAL = "physical"  # Direct column from schema
    DERIVED = "derived"  # Computed expression (e.g., TO_CHAR(DATE, 'Q'))
    AGGREGATED = "aggregated"  # Aggregate function (e.g., SUM(COLUMN))
    NOT_FOUND = "not_found"  # Concept cannot be mapped


@dataclass
class ColumnMapping:
    """Result of mapping a user concept to a database column or expression"""
    concept: str  # User's term (e.g., "quarter", "revenue")
    mapping_type: ColumnMappingType
    expression: str  # SQL expression to use (e.g., "TO_CHAR(DATE, 'Q')", "REVENUE")
    table: str  # Table name
    confidence: int  # 0-100
    note: str = ""  # Explanation for LLM


@dataclass
class SkillResult:
    """Generic skill execution result"""
    success: bool
    data: Dict[str, Any]
    confidence: int  # 0-100
    errors: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]


class SchemaAnalysisSkill:
    """
    Skill 1: Analyze schema and identify relevant tables/columns
    
    Input: user query, intent, full schema
    Output: relevant tables, columns, and metadata
    """
    
    def execute(
        self,
        user_query: str,
        intent: str,
        schema_data: Dict[str, Any],
        enriched_schema: Optional[Dict[str, Any]] = None,
    ) -> SkillResult:
        """
        Analyze schema to find relevant tables and columns
        
        Returns:
            SkillResult with:
            - data: {"tables": [...], "mentioned_concepts": [...]}
            - confidence: 0-100
        """
        logger.info(f"[SchemaAnalysisSkill] Analyzing schema...")
        
        errors = []
        warnings = []
        
        # Extract all mentioned concepts (tables, columns, business terms)
        query_upper = user_query.upper()
        intent_upper = intent.upper()
        combined_text = f"{query_upper} {intent_upper}"
        
        # Find explicitly mentioned tables (prioritize exact matches)
        tables = schema_data.get("tables", {})
        views = schema_data.get("views", {})
        mentioned_tables = []
        
        # First pass: Exact table name matches (highest priority)
        for table_name in list(tables.keys()) + list(views.keys()):
            table_upper = table_name.upper()
            if table_upper in combined_text:
                mentioned_tables.append(table_name)
        
        # Only do partial matching if no exact matches found
        if not mentioned_tables:
            for table_name in list(tables.keys()) + list(views.keys()):
                table_upper = table_name.upper()
                # Partial matching (e.g., "mobile" matches "MOBILE_DATA")
                parts = table_upper.split("_")
                if any(part in combined_text for part in parts if len(part) > 3):
                    mentioned_tables.append(table_name)
                    break  # Only take the first partial match to avoid multiple tables
        
        if not mentioned_tables:
            warnings.append("No tables explicitly mentioned - may need to infer from context")
            confidence = 40
        else:
            confidence = 90
        
        # Extract business concepts (keywords that might be columns or derived fields)
        business_concepts = self._extract_business_concepts(user_query, intent)
        
        logger.info(f"[SchemaAnalysisSkill] Found {len(mentioned_tables)} tables, {len(business_concepts)} concepts")
        
        return SkillResult(
            success=True,
            data={
                "mentioned_tables": mentioned_tables,
                "business_concepts": business_concepts,
                "all_tables": list(tables.keys()) + list(views.keys()),
            },
            confidence=confidence,
            errors=errors,
            warnings=warnings,
            metadata={"skill": "SchemaAnalysis"}
        )
    
    def _extract_business_concepts(self, user_query: str, intent: str) -> List[str]:
        """
        Extract business terms from user query ONLY - ignore intent metadata
        
        Conservative approach:
        - Only extract from actual user query
        - Focus on explicit metric/temporal terms
        - Avoid generic words that might be metadata
        """
        import json
        concepts = []
        
        # ONLY extract from user query - ignore intent to avoid metadata pollution
        query_text = user_query.lower()
        
        # Conservative patterns - only explicit business terms
        temporal_patterns = [
            r'\b(quarter|quarterly|qtr)\b',
            r'\b(year|yearly|annual)\b',
            r'\b(month|monthly)\b',
            r'\b(week|weekly)\b',
            r'\b(day|daily|date)\b',
        ]
        
        metric_patterns = [
            r'\b(revenue|sales|income)\b',
            r'\b(cost|expense)\b',
            r'\b(profit|margin)\b',
            r'\b(volume|quantity|count)\b',
            r'\b(growth|change)\b',
            r'\b(average|avg|mean)\b',
            r'\b(total|sum)\b',
        ]
        
        # Only extract if explicitly mentioned in user query
        all_patterns = [temporal_patterns, metric_patterns]
        for patterns in all_patterns:
            for pattern in patterns:
                matches = re.findall(pattern, query_text, re.IGNORECASE)
                concepts.extend(matches)
        
        # Normalize synonyms
        concepts = self._normalize_concept_synonyms(concepts)
        
        # Deduplicate
        unique_concepts = list(set(concepts))
        
        logger.debug(f"Extracted {len(unique_concepts)} concepts from user query: {unique_concepts}")
        return unique_concepts
    
    def _normalize_concept_synonyms(self, concepts: List[str]) -> List[str]:
        """Normalize common abbreviations to standard terms"""
        synonym_map = {
            # Only normalize clear abbreviations
            'qtr': 'quarter',
            'yr': 'year',
            'mth': 'month',
            'wk': 'week',
            'dt': 'date',
            'avg': 'average',
        }
        
        normalized = []
        for concept in concepts:
            normalized_concept = synonym_map.get(concept.lower(), concept.lower())
            normalized.append(normalized_concept)
        
        return normalized
    
    def detect_implicit_operations(self, user_query: str, intent: str) -> Dict[str, Any]:
        """
        Detect implicit SQL operations from natural language
        
        Returns:
            Dict with detected operations:
            - group_by_hints: List of dimensions to group by
            - order_by_hints: List of (column, direction) tuples
            - limit_hint: Integer limit if detected
            - aggregation_hints: List of detected aggregations
        """
        query_lower = f"{user_query} {intent}".lower()
        
        operations = {
            "group_by_hints": [],
            "order_by_hints": [],
            "limit_hint": None,
            "aggregation_hints": []
        }
        
        # Detect GROUP BY patterns
        group_by_patterns = [
            r'\bby\s+(\w+)',  # "by sector", "by region"
            r'\bfor\s+each\s+(\w+)',  # "for each customer"
            r'\bper\s+(\w+)',  # "per month"
            r'\bgrouped?\s+by\s+(\w+)',  # "grouped by"
        ]
        
        for pattern in group_by_patterns:
            matches = re.findall(pattern, query_lower)
            operations["group_by_hints"].extend(matches)
        
        # Detect ORDER BY with direction
        if re.search(r'\btop\s+(\d+)', query_lower):
            match = re.search(r'\btop\s+(\d+)', query_lower)
            operations["limit_hint"] = int(match.group(1))
            operations["order_by_hints"].append(("metric", "DESC"))
        
        if re.search(r'\bbottom\s+(\d+)', query_lower):
            match = re.search(r'\bbottom\s+(\d+)', query_lower)
            operations["limit_hint"] = int(match.group(1))
            operations["order_by_hints"].append(("metric", "ASC"))
        
        if re.search(r'\b(highest|maximum|max|largest|most)\b', query_lower):
            operations["order_by_hints"].append(("metric", "DESC"))
            if not operations["limit_hint"]:
                operations["limit_hint"] = 10
        
        if re.search(r'\b(lowest|minimum|min|smallest|least)\b', query_lower):
            operations["order_by_hints"].append(("metric", "ASC"))
            if not operations["limit_hint"]:
                operations["limit_hint"] = 10
        
        # Detect aggregations
        agg_patterns = {
            'sum': r'\b(total|sum|add up)\b',
            'avg': r'\b(average|avg|mean)\b',
            'count': r'\b(count|number of|how many)\b',
            'max': r'\b(maximum|max|highest)\b',
            'min': r'\b(minimum|min|lowest)\b',
        }
        
        for agg_func, pattern in agg_patterns.items():
            if re.search(pattern, query_lower):
                operations["aggregation_hints"].append(agg_func)
        
        # Deduplicate
        operations["group_by_hints"] = list(set(operations["group_by_hints"]))
        operations["aggregation_hints"] = list(set(operations["aggregation_hints"]))
        
        logger.info(f"Detected implicit operations: {operations}")
        return operations


class ColumnMappingSkill:
    """
    Skill 2: Map user concepts to physical columns OR derived expressions
    
    This is the CRITICAL skill that solves the "quarter doesn't exist" problem.
    
    Input: business concepts, relevant tables, schema, derived hints, user clarifications
    Output: validated column mappings (physical or derived)
    """

    # ------------------------ helpers ------------------------
    def _aliases_for_concept(self, concept: str) -> List[str]:
        base = concept.lower()
        aliases = set([base, base.upper()])
        # Expand via common synonyms
        for syn in COMMON_SYNONYMS.get(base, []):
            aliases.add(syn)
            aliases.add(syn.upper())
        # Expand via common abbreviations
        for key, abbrs in COMMON_ABBREVIATIONS.items():
            if base == key or base in abbrs or base.upper() in abbrs:
                aliases.add(key)
                aliases.add(key.upper())
                for a in abbrs:
                    aliases.add(a)
                    aliases.add(a.upper())
        return list(aliases)

    def _fuzzy_score(self, a: str, b: str) -> float:
        return SequenceMatcher(None, a.upper(), b.upper()).ratio()



    def _best_date_column(self, columns: List[Dict]) -> Optional[Dict]:
        # Rank by strongest date-like names first
        pri = ['DATE', 'SRC_ID_DATE', 'EVENT_DATE', 'TIMESTAMP', 'DT', 'TS']
        best = None
        best_rank = -1
        for col in columns:
            name = col.get('name', '').upper()
            rank = -1
            for i, token in enumerate(pri[::-1]):
                if token in name:
                    rank = max(rank, len(pri) - i)
            if best is None or rank > best_rank:
                best = col
                best_rank = rank
        # Fallback to first DATE/TIMESTAMP type
        if (not best or best_rank < 0):
            for col in columns:
                t = col.get('type', '').upper()
                if 'DATE' in t or 'TIMESTAMP' in t:
                    return col
        return best

    def _domain_semantic_mapping(
        self,
        concept: str,
        mentioned_tables: List[str],
        tables: Dict[str, List[Dict]],
        views: Dict[str, List[Dict]],
    ) -> Optional[ColumnMapping]:
        concept_l = concept.lower()
        aliases = self._aliases_for_concept(concept_l)
        # 1) Derived temporal concepts
        if concept_l in ('day', 'daily', 'quarter', 'month', 'year'):
            # Use ONLY the explicitly mentioned tables (no cross-table mappings)
            for table in mentioned_tables:
                cols = tables.get(table) or views.get(table) or []
                date_col = self._best_date_column(cols)
                if not date_col:
                    continue
                dc = format_qualified_identifier(table, date_col['name'])
                if concept_l in ('day', 'daily'):
                    expr = f"TRUNC({dc})"
                    note = f"Derived day from {date_col['name']}"
                elif concept_l == 'quarter':
                    expr = f"TO_CHAR({dc}, 'Q')"
                    note = f"Derived quarter from {date_col['name']}"
                elif concept_l == 'month':
                    expr = f"TO_CHAR({dc}, 'MM')"
                    note = f"Derived month from {date_col['name']}"
                else:  # year
                    expr = f"EXTRACT(YEAR FROM {dc})"
                    note = f"Extracted year from {date_col['name']}"
                return ColumnMapping(
                    concept=concept,
                    mapping_type=ColumnMappingType.DERIVED,
                    expression=expr,
                    table=table,
                    confidence=95,
                    note=note,
                )
        # 2) Heuristic physical columns by alias + fuzzy
        threshold = 0.78
        best: Optional[Tuple[float, str, Dict]] = None  # score, table, col
        for table in mentioned_tables:
            cols = tables.get(table) or views.get(table) or []
            for col in cols:
                name_u = col.get('name', '').upper()
                # direct alias containment
                if any(alias.upper() in name_u for alias in aliases):
                    score = max(self._fuzzy_score(alias, name_u) for alias in aliases)
                    if (not best) or score > best[0]:
                        best = (score, table, col)
                else:
                    # token-level fuzzy
                    for alias in aliases:
                        if self._fuzzy_score(alias, name_u) >= threshold:
                            score = self._fuzzy_score(alias, name_u)
                            if (not best) or score > best[0]:
                                best = (score, table, col)
        if best and best[0] >= threshold:
            _, table, col = best
            note = f"Semantic alias/fuzzy match for '{concept}'"
            conf = int(60 + (best[0] - threshold) * 40)  # Lowered starting confidence
            return ColumnMapping(
                concept=concept,
                mapping_type=ColumnMappingType.PHYSICAL,
                expression=format_qualified_identifier(table, col['name']),
                table=table,
                confidence=min(conf, 92), # Reduced max confidence for fuzzy matches
                note=note,
            )
        # 3) Generic numeric metric matching (domain-agnostic)
        # Look for numeric columns that match the concept semantically
        numeric_types = ['NUMBER', 'DECIMAL', 'FLOAT', 'INTEGER', 'NUMERIC']
        for table in mentioned_tables:
            cols = tables.get(table) or views.get(table) or []
            for col in cols:
                name_u = col.get('name', '').upper()
                col_type = col.get('type', '').upper()
                # Check if column name contains the concept and is numeric
                if concept_l.upper() in name_u and any(t in col_type for t in numeric_types):
                    return ColumnMapping(
                        concept=concept,
                        mapping_type=ColumnMappingType.PHYSICAL,
                        expression=format_qualified_identifier(table, col['name']),
                        table=table,
                        confidence=85,
                        note=f"Numeric metric column matching '{concept}'",
                    )
        return None

    # ------------------------ parsing ------------------------
    def _parse_user_clarifications(self, user_query: str) -> Dict[str, str]:
        """
        Parse explicit column mappings from user clarifications
        
        Examples:
        - "Use NR_SBSC as the subscriber" -> {"subscriber": "NR_SBSC"}
        - "sector = SECTOR" -> {"sector": "SECTOR"}
        - "Use DATE for quarter" -> {"quarter": "DATE"}
        - "Volume = UL_GOODPUT_MB + DL_GOODPUT_MB" -> {"volume": "UL_GOODPUT_MB + DL_GOODPUT_MB"}
        - "Calculate growth as (current - previous)/previous" -> {"growth": "LAG_CALCULATION"}
        """
        clarifications = {}
        
        # Pattern 1: "Use X as the Y" or "Use X for Y"
        pattern1 = r'[Uu]se\s+(\w+)\s+(?:as\s+the\s+|for\s+)(\w+)'
        for match in re.finditer(pattern1, user_query):
            column_name = match.group(1)
            concept = match.group(2)
            clarifications[concept.lower()] = column_name
            logger.info(f"Parsed clarification: '{concept}' -> {column_name}")
        
        # Pattern 2: "X = expression" (derived expressions with operators)
        # Matches: "Volume = UL_GOODPUT_MB + DL_GOODPUT_MB"
        pattern2 = r'(\w+)\s*=\s*([A-Z_][A-Z0-9_]*(?:\s*[+\-*/]\s*[A-Z_][A-Z0-9_]*)+)'
        for match in re.finditer(pattern2, user_query):
            concept = match.group(1)
            expression = match.group(2).strip()
            clarifications[concept.lower()] = expression
            logger.info(f"Parsed derived expression: '{concept}' -> {expression}")
        
        # Pattern 3: "X = Y" (simple column mapping)
        pattern3 = r'(\w+)\s*=\s*([A-Z_][A-Z0-9_]*)\s*(?:[,\.]|$)'
        for match in re.finditer(pattern3, user_query):
            concept = match.group(1)
            column_name = match.group(2)
            # Skip if already matched by pattern 2
            if concept.lower() not in clarifications:
                clarifications[concept.lower()] = column_name
                logger.info(f"Parsed simple mapping: '{concept}' -> {column_name}")
        
        # Pattern 4: "Calculate X as ..." - indicates user provided calculation logic
        pattern4 = r'[Cc]alculate\s+(\w+)\s+as\s+\([^)]+\)'
        for match in re.finditer(pattern4, user_query):
            concept = match.group(1)
            clarifications[concept.lower()] = "USER_DEFINED_CALCULATION"
            logger.info(f"Parsed calculation clarification: '{concept}' -> USER_DEFINED_CALCULATION")
        
        return clarifications
    
    def execute(
        self,
        business_concepts: List[str],
        mentioned_tables: List[str],
        schema_data: Dict[str, Any],
        enriched_schema: Optional[Dict[str, Any]] = None,
        user_query: str = "",
    ) -> SkillResult:
        """
        Map each business concept to a physical column or derived expression
        
        Returns:
            SkillResult with:
            - data: {"mappings": [ColumnMapping, ...]}
            - confidence: 0-100 (lowest mapping confidence)
        """
        logger.info(f"[ColumnMappingSkill] Mapping {len(business_concepts)} concepts...")
        logger.debug(
            "[ColumnMappingSkill] Inputs -> concepts=%s, mentioned_tables=%s",
            business_concepts,
            mentioned_tables,
        )
        
        # Parse user clarifications FIRST (highest priority)
        logger.debug(f"[DEBUG] Parsing clarifications from user_query: {user_query[:100]}...")
        user_clarifications = self._parse_user_clarifications(user_query)
        logger.debug(f"[DEBUG] Found clarifications: {user_clarifications}")
        if user_clarifications:
            logger.info(f"Found {len(user_clarifications)} user clarifications: {user_clarifications}")
        
        mappings: List[ColumnMapping] = []
        tables = schema_data.get("tables", {})
        views = schema_data.get("views", {})
        derived_hints = (enriched_schema or {}).get("derived_hints", {})
        sample_data = (enriched_schema or {}).get("samples", {})
        
        errors = []
        warnings = []
        
        for concept in business_concepts:
            concept_upper = concept.upper()
            mapping = None
            
            # Strategy 0: User clarification (HIGHEST PRIORITY)
            if concept.lower() in user_clarifications:
                clarified_value = user_clarifications[concept.lower()]
                
                # Check if it's a derived expression (contains operators)
                if any(op in clarified_value for op in ['+', '-', '*', '/', '(', ')']):
                    # Validate that all column names in expression exist
                    column_names = re.findall(r'[A-Z_][A-Z0-9_]*', clarified_value)
                    all_valid = True
                    for col_name in column_names:
                        found = False
                        for table in mentioned_tables:
                            columns = tables.get(table) or views.get(table) or []
                            if any(col.get("name", "").upper() == col_name.upper() for col in columns):
                                found = True
                                break
                        if not found:
                            logger.warning(f"Column '{col_name}' in expression not found in schema")
                            all_valid = False
                    
                    if all_valid:
                        # Qualify column names with table
                        qualified_expr = clarified_value
                        for col_name in column_names:
                            for table in mentioned_tables:
                                columns = tables.get(table) or views.get(table) or []
                                if any(col.get("name", "").upper() == col_name.upper() for col in columns):
                                    qualified_name = format_qualified_identifier(table, col_name)
                                    qualified_expr = qualified_expr.replace(col_name, qualified_name)
                                    break
                        
                        mapping = ColumnMapping(
                            concept=concept,
                            mapping_type=ColumnMappingType.DERIVED,
                            expression=qualified_expr,
                            table=mentioned_tables[0] if mentioned_tables else "",
                            confidence=100,
                            note=f"User-defined derived expression: {clarified_value}"
                        )
                        mappings.append(mapping)
                        logger.info(f"Used derived expression: '{concept}' -> {qualified_expr}")
                        continue
                
                # Handle user-defined calculations
                if clarified_value == "USER_DEFINED_CALCULATION":
                    mapping = ColumnMapping(
                        concept=concept,
                        mapping_type=ColumnMappingType.DERIVED,
                        expression=f"-- User will define {concept} calculation",
                        table=mentioned_tables[0] if mentioned_tables else "",
                        confidence=100,
                        note=f"User provided calculation logic for '{concept}'"
                    )
                    mappings.append(mapping)
                    logger.info(f"Used calculation: '{concept}'")
                    continue
                
                # Simple column mapping
                # Find which table has this column - search mentioned tables first
                for table in mentioned_tables:
                    columns = tables.get(table) or views.get(table) or []
                    for col in columns:
                        if col.get("name", "").upper() == clarified_value.upper():
                            mapping = ColumnMapping(
                                concept=concept,
                                mapping_type=ColumnMappingType.PHYSICAL,
                                expression=format_qualified_identifier(table, col['name']),
                                table=table,
                                confidence=100,
                                note=f"User clarification: '{concept}' -> {col['name']}"
                            )
                            break
                    if mapping:
                        break
                
                # If not found in mentioned tables, search ALL tables
                if not mapping:
                    for table_name, columns in {**tables, **views}.items():
                        for col in columns:
                            if col.get("name", "").upper() == clarified_value.upper():
                                mapping = ColumnMapping(
                                    concept=concept,
                                    mapping_type=ColumnMappingType.PHYSICAL,
                                    expression=format_qualified_identifier(table_name, col['name']),
                                    table=table_name,
                                    confidence=100,
                                    note=f"User clarification: '{concept}' -> {col['name']} (found in {table_name})"
                                )
                                # Add table to mentioned_tables if not already there
                                if table_name not in mentioned_tables:
                                    mentioned_tables.append(table_name)
                                break
                        if mapping:
                            break
                
                # If clarified column found, skip auto-detection
                if mapping:
                    mappings.append(mapping)
                    logger.info(f"Used clarification: '{concept}' -> {mapping.expression}")
                    continue
                else:
                    logger.warning(f"User clarified '{concept}' -> {clarified_value}, but column not found in schema!")
            
            # Strategy 1: Check physical columns in mentioned tables
            for table in mentioned_tables:
                columns = tables.get(table) or views.get(table) or []
                
                for col in columns:
                    col_name = col.get("name", "").upper()
                    
                    # Exact match
                    if col_name == concept_upper:
                        mapping = ColumnMapping(
                            concept=concept,
                            mapping_type=ColumnMappingType.PHYSICAL,
                            expression=format_qualified_identifier(table, col['name']),
                            table=table,
                            confidence=100,
                            note=f"Exact column match in {table}"
                        )
                        break
                    
                    # Partial match (concept is part of column name)
                    if concept_upper in col_name or col_name in concept_upper:
                        if not mapping or mapping.confidence < 80:
                            mapping = ColumnMapping(
                                concept=concept,
                                mapping_type=ColumnMappingType.PHYSICAL,
                                expression=format_qualified_identifier(table, col['name']),
                                table=table,
                                confidence=65, # Lowered from 80 to favor clarification
                                note=f"Partial column match: {col['name']} in {table}"
                            )
                
                if mapping and mapping.confidence == 100:
                    break

            # Strategy 1.5: Domain semantic synonyms + fuzzy in mentioned tables
            if not mapping or mapping.confidence < 90:
                sem = self._domain_semantic_mapping(concept, mentioned_tables, tables, views)
                if sem:
                    mapping = sem
            
            # Strategy 2: Check derived hints (THIS IS KEY FOR QUARTER/YEAR/etc)
            if not mapping or mapping.confidence < 90:
                for table in mentioned_tables:
                    table_hints = derived_hints.get(table, [])
                    
                    for hint in table_hints:
                        hint_concept = hint.get("concept", "").upper()
                        
                        if hint_concept == concept_upper or concept_upper in hint_concept:
                            # Found a derived expression!
                            mapping = ColumnMapping(
                                concept=concept,
                                mapping_type=ColumnMappingType.DERIVED,
                                expression=hint.get("expression", ""),
                                table=table,
                                confidence=95,
                                note=f"Derived from {hint.get('note', 'column expression')}"
                            )
                            break
                    
                    if mapping and mapping.mapping_type == ColumnMappingType.DERIVED:
                        break
            
            # Strategy 3: Semantic matching using sample data
            if not mapping or mapping.confidence < 70:
                semantic_mapping = self._semantic_match(
                    concept, mentioned_tables, tables, views, sample_data
                )
                if semantic_mapping:
                    mapping = semantic_mapping
            
            # Strategy 4: Enhanced aggregation and grouping detection
            if not mapping:
                agg_mapping = self._detect_aggregation_and_grouping(concept, mentioned_tables, tables, user_query)
                if agg_mapping:
                    mapping = agg_mapping
            
            # Record result
            if mapping:
                mappings.append(mapping)
                logger.debug(f"Mapped '{concept}' -> {mapping.expression} ({mapping.mapping_type.value}, {mapping.confidence}%)")
            else:
                # Cannot map - this will trigger clarification
                unmapped = ColumnMapping(
                    concept=concept,
                    mapping_type=ColumnMappingType.NOT_FOUND,
                    expression="",
                    table="",
                    confidence=0,
                    note=f"Could not map concept '{concept}' to any column or expression"
                )
                mappings.append(unmapped)
                errors.append(f"Cannot map concept: {concept}")
                logger.warning(f"Cannot map '{concept}' to any column")
        
        # Calculate overall confidence with strict penalties
        successfully_mapped = [m for m in mappings if m.mapping_type != ColumnMappingType.NOT_FOUND]
        if successfully_mapped:
            base_confidence = sum(m.confidence for m in successfully_mapped) // len(successfully_mapped)
            
            # Apply strict penalties for quality issues
            penalty = 0
            
            # STRICT PENALTY: Cross-table mappings (different tables used)
            tables_used = set(m.table for m in successfully_mapped if m.table)
            if len(tables_used) > 1:
                penalty += 50  # Heavy penalty for cross-table mappings
                logger.warning(f"Cross-table mappings detected: {tables_used}")
            
            # PENALTY: Unmapped concepts
            unmapped_count = len([m for m in mappings if m.mapping_type == ColumnMappingType.NOT_FOUND])
            if unmapped_count > 0:
                penalty += unmapped_count * 20  # 20% penalty per unmapped concept
                logger.warning(f"{unmapped_count} unmapped concepts detected")
            
            # PENALTY: Low-confidence mappings
            low_confidence_mappings = [m for m in successfully_mapped if m.confidence < 80]
            if low_confidence_mappings:
                penalty += len(low_confidence_mappings) * 10  # 10% penalty per low-confidence mapping
            
            overall_confidence = max(0, base_confidence - penalty)
            
            # Additional validation: If confidence is too low, force clarification
            # Increased threshold from 50 to 65 to be more proactive in seeking clarification
            if overall_confidence < 65:
                logger.warning(f"Overall confidence too low: {overall_confidence}% - will request clarification")
        elif mappings:
            # All concepts are unmapped
            overall_confidence = 0
        else:
            overall_confidence = 0
            errors.append("No concept mappings found")
        
        mapped_count = len([m for m in mappings if m.mapping_type != ColumnMappingType.NOT_FOUND])
        logger.info(f"[ColumnMappingSkill] Mapped {mapped_count}/{len(business_concepts)} concepts")

        if mapped_count == 0 and business_concepts:
            schema_preview = {
                table: [col.get("name", "") for col in (tables.get(table) or [])[:5]]
                for table in mentioned_tables
            }
            logger.warning(
                " [ColumnMappingSkill] No validated mappings produced. Concepts=%s MentionedTables=%s PreviewColumns=%s",
                business_concepts,
                mentioned_tables,
                schema_preview,
            )

        return SkillResult(
            success=len(errors) == 0,
            data={"mappings": mappings},
            confidence=overall_confidence,
            errors=errors,
            warnings=warnings,
            metadata={
                "skill": "ColumnMapping",
                "physical_mappings": len([m for m in mappings if m.mapping_type == ColumnMappingType.PHYSICAL]),
                "derived_mappings": len([m for m in mappings if m.mapping_type == ColumnMappingType.DERIVED]),
                "unmapped": len([m for m in mappings if m.mapping_type == ColumnMappingType.NOT_FOUND]),
            }
        )
    
    def _semantic_match(
        self,
        concept: str,
        tables: List[str],
        table_schemas: Dict[str, List[Dict]],
        view_schemas: Dict[str, List[Dict]],
        sample_data: Dict[str, List[Dict]],
    ) -> Optional[ColumnMapping]:
        """
        Use semantic matching based on column data types, names, abbreviations,
        and sample values (lightweight, conservative)
        """
        concept_lower = concept.lower()

        # Abbreviation/name-based semantic signals across mentioned tables
        aliases = self._aliases_for_concept(concept_lower)
        for table in tables:
            cols = table_schemas.get(table) or view_schemas.get(table) or []
            for col in cols:
                col_name = col.get('name', '').upper()
                col_type = col.get('type', '').upper()
                # Strong type + alias suggesters
                if concept_lower in ("date", "time", "timestamp"):
                    if ('DATE' in col_type or 'TIMESTAMP' in col_type) and any(a.upper() in col_name for a in aliases):
                        return ColumnMapping(
                            concept=concept,
                            mapping_type=ColumnMappingType.PHYSICAL,
                            expression=format_qualified_identifier(table, col['name']),
                            table=table,
                            confidence=72,
                            note=f"Semantic date-like column {col['name']}"
                        )
                # Numeric financials
                if concept_lower in ("revenue", "sales", "income", "cost", "expense"):
                    if any(tp in col_type for tp in ['NUMBER', 'DECIMAL', 'FLOAT', 'INTEGER']):
                        if any(term in col_name for term in ["REVENUE", "SALES", "INCOME", "AMOUNT", "TOTAL", "COST", "EXPENSE"]):
                            return ColumnMapping(
                                concept=concept,
                                mapping_type=ColumnMappingType.PHYSICAL,
                                expression=format_qualified_identifier(table, col['name']),
                                table=table,
                                confidence=68,
                                note=f"Semantic numeric column {col['name']}"
                            )
        
        # As last resort, try sample values to detect MSISDN-like numeric strings length 10-15
        if concept_lower in ('subscriber', 'msisdn', 'user', 'customer'):
            for table in tables:
                samples = sample_data.get(table, [])
                if not samples:
                    continue
                # inspect first row
                row = samples[0]
                for k, v in row.items():
                    if isinstance(v, str) and v.isdigit() and 10 <= len(v) <= 15:
                        return ColumnMapping(
                            concept=concept,
                            mapping_type=ColumnMappingType.PHYSICAL,
                            expression=format_qualified_identifier(table, k),
                            table=table,
                            confidence=60,
                            note="Sample data suggests MSISDN-like identifier",
                        )

        return None
    
    def _detect_aggregation_and_grouping(
        self,
        concept: str,
        tables: List[str],
        table_schemas: Dict[str, List[Dict]],
        user_query: str,
    ) -> Optional[ColumnMapping]:
        """
        Enhanced aggregation detection with grouping and sorting intent
        """
        concept_lower = concept.lower()
        query_lower = user_query.lower()
        
        # Enhanced aggregation patterns
        agg_patterns = {
            "total": "SUM", "sum": "SUM", "aggregate": "SUM", "cumulative": "SUM",
            "average": "AVG", "avg": "AVG", "mean": "AVG", "typical": "AVG",
            "count": "COUNT", "number": "COUNT", "quantity": "COUNT",
            "maximum": "MAX", "max": "MAX", "highest": "MAX", "peak": "MAX",
            "minimum": "MIN", "min": "MIN", "lowest": "MIN", "bottom": "MIN",
        }
        
        # Detect grouping intent patterns
        grouping_indicators = [
            r'\bby\s+(\w+)', r'\bper\s+(\w+)', r'\beach\s+(\w+)', 
            r'\bfor\s+each\s+(\w+)', r'\bbroken\s+down\s+by\s+(\w+)',
            r'\bsegmented\s+by\s+(\w+)', r'\bgrouped\s+by\s+(\w+)'
        ]
        
        # Detect sorting intent patterns  
        sorting_indicators = [
            r'\btop\s+(\d+)', r'\bbottom\s+(\d+)', r'\bhighest\s+(\d+)',
            r'\blowest\s+(\d+)', r'\bfirst\s+(\d+)', r'\blast\s+(\d+)',
            r'\blargest\s+(\d+)', r'\bsmallest\s+(\d+)'
        ]
        
        # Check for aggregation patterns
        for pattern, agg_func in agg_patterns.items():
            if pattern in concept_lower:
                # Find appropriate column for aggregation
                for table in tables:
                    cols = table_schemas.get(table, [])
                    
                    # For COUNT, prefer ID or primary key columns
                    if agg_func == "COUNT":
                        for col in cols:
                            col_name = col.get('name', '').upper()
                            if any(key_indicator in col_name for key_indicator in ['ID', 'KEY', 'PK']):
                                column_ref = format_qualified_identifier(table, col['name'])
                                expression = f"COUNT(DISTINCT {column_ref})"
                                
                                # Check for grouping intent
                                grouping_note = self._detect_grouping_context(query_lower, grouping_indicators)
                                sorting_note = self._detect_sorting_context(query_lower, sorting_indicators)
                                
                                note = f"Count distinct {col['name']}"
                                if grouping_note:
                                    note += f" (detected grouping: {grouping_note})"
                                if sorting_note:
                                    note += f" (detected sorting: {sorting_note})"
                                
                                return ColumnMapping(
                                    concept=concept,
                                    mapping_type=ColumnMappingType.AGGREGATED,
                                    expression=expression,
                                    table=table,
                                    confidence=75,
                                    note=note
                                )
                    
                    # For other aggregations, prefer numeric columns
                    else:
                        for col in cols:
                            col_type = col.get("type", "").upper()
                            if "NUMBER" in col_type or "DECIMAL" in col_type:
                                column_ref = format_qualified_identifier(table, col['name'])
                                expression = f"{agg_func}({column_ref})"
                                
                                # Check for grouping and sorting context
                                grouping_note = self._detect_grouping_context(query_lower, grouping_indicators)
                                sorting_note = self._detect_sorting_context(query_lower, sorting_indicators)
                                
                                note = f"Aggregate function: {agg_func} on {col['name']}"
                                if grouping_note:
                                    note += f" (detected grouping: {grouping_note})"
                                if sorting_note:
                                    note += f" (detected sorting: {sorting_note})"
                                
                                return ColumnMapping(
                                    concept=concept,
                                    mapping_type=ColumnMappingType.AGGREGATED,
                                    expression=expression,
                                    table=table,
                                    confidence=70,
                                    note=note
                                )
        
        return None
    
    def _detect_grouping_context(self, query_lower: str, grouping_indicators: List[str]) -> Optional[str]:
        """Detect grouping context from query"""
        for pattern in grouping_indicators:
            match = re.search(pattern, query_lower)
            if match:
                return match.group(1) if match.groups() else "detected"
        return None
    
    def _detect_sorting_context(self, query_lower: str, sorting_indicators: List[str]) -> Optional[str]:
        """Detect sorting context from query"""
        for pattern in sorting_indicators:
            match = re.search(pattern, query_lower)
            if match:
                return match.group(0) if match else "detected"
        return None


class SQLGenerationSkill:
    """
    Skill 3: Generate SQL using validated column mappings
    
    Input: user query, column mappings, schema context
    Output: SQL query with ONLY validated columns/expressions
    """
    
    def build_enhanced_prompt(
        self,
        user_query: str,
        intent: str,
        column_mappings: List[ColumnMapping],
        schema_context: Dict[str, Any],
        implicit_operations: Optional[Dict[str, Any]] = None,
        database_type: str = "oracle",
    ) -> str:
        """
        Build LLM prompt with EXPLICIT column mappings
        
        This prompt is much more directive - it tells the LLM exactly which
        expressions to use, removing ambiguity.
        """
        
        # Build validated column mapping section
        mapping_section = """

 VALIDATED COLUMN MAPPINGS - USE THESE EXPRESSIONS EXACTLY


These mappings have been validated against the actual database schema.
You MUST use these exact expressions in your SQL query.

"""
        
        physical_cols = [m for m in column_mappings if m.mapping_type == ColumnMappingType.PHYSICAL]
        derived_cols = [m for m in column_mappings if m.mapping_type == ColumnMappingType.DERIVED]
        agg_cols = [m for m in column_mappings if m.mapping_type == ColumnMappingType.AGGREGATED]
        unmapped = [m for m in column_mappings if m.mapping_type == ColumnMappingType.NOT_FOUND]
        
        if physical_cols:
            mapping_section += "\n PHYSICAL COLUMNS (exist in database):\n"
            for m in physical_cols:
                mapping_section += f"   -  User term: '{m.concept}' -> SQL: {m.expression}\n"
                mapping_section += f"     Note: {m.note}\n"
        
        if derived_cols:
            mapping_section += "\n DERIVED EXPRESSIONS (compute from existing columns):\n"
            for m in derived_cols:
                mapping_section += f"   -  User term: '{m.concept}' -> SQL: {m.expression}\n"
                mapping_section += f"     Note: {m.note}\n"
                mapping_section += f"       This is NOT a physical column - use the expression as-is!\n"
        
        if agg_cols:
            mapping_section += "\n AGGREGATIONS (computed metrics):\n"
            for m in agg_cols:
                mapping_section += f"   -  User term: '{m.concept}' -> SQL: {m.expression}\n"
                mapping_section += f"     Note: {m.note}\n"
        
        if unmapped:
            mapping_section += "\n UNMAPPED CONCEPTS (cannot generate SQL):\n"
            for m in unmapped:
                mapping_section += f"   -  '{m.concept}' - no matching column or expression found\n"
        
        mapping_section += """


"""
        
        # Add schema context
        tables = schema_context.get("tables", {})
        schema_section = "\nAVAILABLE TABLES AND COLUMNS:\n\n"
        
        for table_name, columns in tables.items():
            schema_section += f" TABLE: {table_name}\n"
            for col in columns[:20]:  # Limit to avoid token overflow
                schema_section += f"   -  {col['name']} ({col['type']})\n"
            schema_section += "\n"
        
        # Add implicit operations hints
        operations_section = ""
        if implicit_operations:
            operations_section = "\n DETECTED IMPLICIT SQL OPERATIONS:\n"
            
            if implicit_operations.get("group_by_hints"):
                operations_section += f"   -  GROUP BY: {', '.join(implicit_operations['group_by_hints'])}\n"
            
            if implicit_operations.get("order_by_hints"):
                order_hints = implicit_operations['order_by_hints']
                operations_section += f"   -  ORDER BY: {', '.join([f'{col} {dir}' for col, dir in order_hints])}\n"
            
            if implicit_operations.get("limit_hint"):
                limit_val = implicit_operations['limit_hint']
                if database_type in ["postgres", "postgresql", "doris"]:
                    operations_section += f"   -  LIMIT: {limit_val} rows\n"
                else:
                    operations_section += f"   -  FETCH FIRST: {limit_val} ROWS ONLY\n"
            
            if implicit_operations.get("aggregation_hints"):
                operations_section += f"   -  AGGREGATIONS: {', '.join(implicit_operations['aggregation_hints'])}\n"
            
            operations_section += "\n"
        
        # Build complete prompt
        prompt = f"""{mapping_section}

{schema_section}

{operations_section}


 SQL GENERATION INSTRUCTIONS


User Query: {user_query}
Intent: {intent}

CRITICAL RULES:
1. Use ONLY the expressions from "VALIDATED COLUMN MAPPINGS" above
2. For DERIVED EXPRESSIONS, copy the expression exactly - DO NOT treat as column name
3. If any concepts are UNMAPPED, return an error request clarification
4. Use {database_type.upper()} SQL syntax ({"LIMIT" if database_type in ["postgres", "postgresql", "doris"] else "FETCH FIRST"}, etc.)
5. Apply detected implicit operations (GROUP BY, ORDER BY, LIMIT) from hints above
6. Always include: SELECT, FROM, GROUP BY (if aggregations), ORDER BY, {"LIMIT N" if database_type in ["postgres", "postgresql", "doris"] else "FETCH FIRST N ROWS ONLY"}

Example correct usage for quarter:
- User asks for "quarter"
- Mapping shows: TO_CHAR(DATE, 'Q')
- Your SQL: SELECT TO_CHAR(SRC_ID_DATE, 'Q') AS QUARTER, ...
- Your SQL: GROUP BY TO_CHAR(SRC_ID_DATE, 'Q')
-  WRONG: SELECT QUARTER (this column doesn't exist!)

Example implicit operations:
- User asks "top 10 by revenue" -> ORDER BY REVENUE DESC {"LIMIT 10" if database_type in ["postgres", "postgresql", "doris"] else "FETCH FIRST 10 ROWS ONLY"}
- User asks "by sector" -> GROUP BY SECTOR
- User asks "for each customer" -> GROUP BY CUSTOMER_ID

Generate ONLY the SQL query. No explanations. No markdown code blocks.
Add confidence comment at end: -- CONFIDENCE: X%
"""
        
        return prompt


class ValidationSkill:
    """
    Skill 4: Validate generated SQL against schema
    
    Input: generated SQL, schema, column mappings
    Output: validation result with corrections if needed
    """
    
    def execute(
        self,
        sql_query: str,
        column_mappings: List[ColumnMapping],
        schema_data: Dict[str, Any],
    ) -> SkillResult:
        """
        Validate SQL against schema and mappings
        
        Returns:
            SkillResult with:
            - data: {"valid": bool, "issues": [...], "corrected_sql": str}
            - confidence: 0-100
        """
        logger.info(f"[ValidationSkill] Validating generated SQL...")
        
        errors = []
        warnings = []
        issues = []

        identifier_pattern = re.compile(r'"([^"]+)"|([A-Za-z_][A-Za-z0-9_$#]*)')

        def extract_identifiers(sql: str) -> List[str]:
            identifiers: List[str] = []
            for match in identifier_pattern.finditer(sql):
                quoted, unquoted = match.groups()
                if quoted is not None:
                    identifiers.append(quoted)
                elif unquoted is not None:
                    identifiers.append(unquoted.upper())
            return identifiers

        sql_keywords = {
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON", "AND", "OR", "NOT", "IN",
            "BETWEEN", "LIKE", "IS", "NULL", "AS", "DISTINCT", "ALL", "FETCH", "FIRST", "ROWS",
            "ONLY", "WITH", "CASE", "WHEN", "THEN", "ELSE", "END", "OVER", "PARTITION", "UNION",
            "INTERSECT", "MINUS", "EXISTS"
        }

        sql_functions = {
            "SUM", "AVG", "COUNT", "MAX", "MIN", "TRUNC", "TO_CHAR", "TO_DATE", "NVL", "COALESCE",
            "ROUND", "CEIL", "FLOOR", "SUBSTR", "INSTR", "LENGTH", "UPPER", "LOWER", "TRIM", "DECODE",
            "LAG", "LEAD", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "ADD_MONTHS", "SYSDATE",
            "MONTHS_BETWEEN", "EXTRACT", "CAST", "STDDEV"
        }

        column_references = extract_identifiers(sql_query)

        valid_expressions: set[str] = set()
        for mapping in column_mappings:
            if mapping.mapping_type != ColumnMappingType.NOT_FOUND:
                for identifier in extract_identifiers(mapping.expression):
                    # Preserve lowercase identifiers (quoted columns) to enforce exact casing
                    if identifier != identifier.upper():
                        valid_expressions.add(identifier)
                    else:
                        valid_expressions.add(identifier.upper())

        tables = schema_data.get("tables", {})
        for table_name, columns in tables.items():
            normalized_table = table_name if table_name != table_name.upper() else table_name.upper()
            valid_expressions.add(normalized_table)
            for col in columns:
                col_name = col.get("name", "")
                if not col_name:
                    continue
                if col_name != col_name.upper():
                    valid_expressions.add(col_name)
                else:
                    valid_expressions.add(col_name.upper())

        invalid_refs = []
        for ref in set(column_references):
            if ref in sql_keywords or ref in sql_functions:
                continue
            if ref not in valid_expressions:
                invalid_refs.append(ref)
        
        if invalid_refs:
            errors.append(f"Invalid column references: {', '.join(invalid_refs[:5])}")
            issues.append({
                "type": "invalid_column",
                "columns": invalid_refs,
                "message": "SQL references columns that don't exist in schema or mappings"
            })
        
        # Check 2: Verify derived expressions are used correctly
        for mapping in column_mappings:
            if mapping.mapping_type == ColumnMappingType.DERIVED:
                concept_upper = mapping.concept.upper()
                # Check if concept appears as bare column name (wrong)
                if re.search(rf'\bSELECT\s+.*\b{concept_upper}\b', sql_query.upper()):
                    if mapping.expression not in sql_query:
                        warnings.append(f"Derived concept '{mapping.concept}' used as column name instead of expression")
                        issues.append({
                            "type": "derived_as_column",
                            "concept": mapping.concept,
                            "correct_expression": mapping.expression,
                            "message": f"Use expression '{mapping.expression}' instead of '{mapping.concept}'"
                        })
        
        # Calculate validation confidence
        if errors:
            confidence = 0
            valid = False
        elif warnings:
            confidence = 50
            valid = False
        else:
            confidence = 100
            valid = True
        
        logger.info(f"[ValidationSkill] Validation complete: valid={valid}, confidence={confidence}%")
        
        return SkillResult(
            success=valid,
            data={
                "valid": valid,
                "issues": issues,
                "invalid_columns": invalid_refs if invalid_refs else [],
            },
            confidence=confidence,
            errors=errors,
            warnings=warnings,
            metadata={"skill": "Validation"}
        )


# ==================== SKILLS ORCHESTRATOR ====================

class SQLGenerationSkillsOrchestrator:
    """
    Orchestrates all skills in the correct sequence
    
    This is the main entry point for skills-based SQL generation.
    """
    
    def __init__(self):
        self.schema_skill = SchemaAnalysisSkill()
        self.mapping_skill = ColumnMappingSkill()
        self.generation_skill = SQLGenerationSkill()
        self.validation_skill = ValidationSkill()
    
    async def generate_sql(
        self,
        user_query: str,
        intent: str,
        schema_data: Dict[str, Any],
        enriched_schema: Optional[Dict[str, Any]] = None,
        database_type: str = "oracle",
    ) -> Dict[str, Any]:
        """
        Execute skills pipeline to generate accurate SQL
        
        Returns:
            {
                "success": bool,
                "sql_query": str,
                "confidence": int,
                "column_mappings": List[ColumnMapping],
                "validation_result": SkillResult,
                "clarification_needed": bool,
                "clarification_message": str,
            }
        """
        logger.info(f"[SkillsOrchestrator] Starting skills-based SQL generation...")
        
        # Skill 1: Analyze schema
        analysis_result = self.schema_skill.execute(
            user_query, intent, schema_data, enriched_schema
        )
        
        if not analysis_result.success:
            return {
                "success": False,
                "error": "Schema analysis failed",
                "clarification_needed": True,
                "clarification_message": "Could not identify relevant tables in schema",
            }
        
        # Detect implicit operations (GROUP BY, ORDER BY, LIMIT)
        implicit_ops = self.schema_skill.detect_implicit_operations(user_query, intent)
        logger.info(f"Detected implicit operations: {implicit_ops}")
        
        # Skill 2: Map columns (pass user_query for clarification parsing)
        mapping_result = self.mapping_skill.execute(
            business_concepts=analysis_result.data["business_concepts"],
            mentioned_tables=analysis_result.data["mentioned_tables"],
            schema_data=schema_data,
            enriched_schema=enriched_schema,
            user_query=user_query,  # CRITICAL: enables clarification parsing
        )
        
        column_mappings: List[ColumnMapping] = mapping_result.data.get("mappings", [])
        
        # Check if any concepts are unmapped
        unmapped = [m for m in column_mappings if m.mapping_type == ColumnMappingType.NOT_FOUND]
        
        # CRITICAL FIX: Only request clarification for TRULY MISSING base columns
        # Don't block on derivable temporal concepts or aggregates
        # NOTE: 'growth', 'change' etc. removed - they need specific context for calculation
        derivable_concepts = ['day', 'daily', 'week', 'weekly', 'month', 'monthly', 'quarter', 'quarterly', 
                             'year', 'yearly', 'total', 'sum', 'average', 'avg', 'count', 'max', 'min']
        
        # Filter out derivable concepts from unmapped list
        truly_unmapped = [
            m for m in unmapped 
            if not any(deriv in m.concept.lower() for deriv in derivable_concepts)
        ]
        
        # ALWAYS request clarification if ANY concepts are unmapped, regardless of confidence
        if truly_unmapped:
            # Cannot generate SQL - need clarification for base columns only
            clarification_payload = self._build_clarification_payload(
                truly_unmapped, analysis_result.data["mentioned_tables"], schema_data
            )
            return {
                "success": False,
                "clarification_needed": True,
                "clarification_message": clarification_payload["message"],
                "clarification_details": clarification_payload["details"],
                "column_mappings": column_mappings,
                "confidence": mapping_result.confidence,
            }
        
        # Skill 3: Build enhanced prompt for LLM with implicit operations
        enhanced_prompt = self.generation_skill.build_enhanced_prompt(
            user_query=user_query,
            intent=intent,
            column_mappings=column_mappings,
            schema_context={"tables": schema_data.get("tables", {})},
            implicit_operations=implicit_ops,
            database_type=database_type,
        )
        
        return {
            "success": True,
            "enhanced_prompt": enhanced_prompt,
            "column_mappings": column_mappings,
            "confidence": mapping_result.confidence,
            "clarification_needed": False,
            "metadata": {
                "analysis": analysis_result.metadata,
                "mapping": mapping_result.metadata,
            }
        }
    
    def _build_clarification_payload(
        self,
        unmapped_concepts: List[ColumnMapping],
        mentioned_tables: List[str],
        schema_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build user-friendly clarification request and structured payload"""

        tables = schema_data.get("tables", {})

        msg = "CLARIFICATION NEEDED: Cannot map some concepts to database columns.\n\n"
        
        structured_unmapped = []
        for m in unmapped_concepts:
            msg += f" UNMAPPED: '{m.concept}' - no matching column found\n"
            structured_unmapped.append({
                "concept": m.concept,
                "note": m.note,
            })

        msg += f"\n AVAILABLE COLUMNS IN {mentioned_tables[0] if mentioned_tables else 'TABLES'}:\n"
        structured_tables = []
        
        # Show only the primary table (first mentioned table)
        primary_table = mentioned_tables[0] if mentioned_tables else None
        if primary_table:
            cols = tables.get(primary_table, [])
            if cols:
                col_names = [c.get("name", "") for c in cols]
                msg += f"   {primary_table}: {', '.join(col_names)}\n"
                structured_tables.append({
                    "name": primary_table,
                    "columns": col_names,
                })

        msg += "\n PLEASE CLARIFY:\n"
        msg += "Which column(s) should I use for the unmapped concepts?\n"
        msg += "Example: 'Use REVENUE_GROWTH column for growth' or 'Calculate growth as (current - previous)/previous'\n"

        return {
            "message": msg,
            "details": {
                "unmapped_concepts": structured_unmapped,
                "referenced_tables": structured_tables,
            },
        }
