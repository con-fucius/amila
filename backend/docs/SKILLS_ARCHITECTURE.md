# Skills-Based SQL Generation Architecture

## Overview

Implements Anthropic's **Skills pattern** (October 2025) for accurate SQL generation. Skills **complement** existing SQL generation, preventing "column doesn't exist" errors.

**Reference**: https://docs.claude.com/en/docs/agents-and-tools/agent-skills/best-practices

## Skills Pattern Principles

- Discrete, testable, composable functions
- Clear inputs/outputs with validation
- Independently debuggable
- Chainable for complex workflows

## Architecture: Hybrid Approach

```

                    User Natural Language Query               

                         
                         

              SKILLS ORCHESTRATOR (New Layer)                 
                                                              
       
   Schema         ->  Column         ->  Validation     
   Analysis          Mapping           Skill          
   Skill             Skill                            
       
                                                              
  Output: Validated column mappings                          
          - Physical columns                                  
          - Derived expressions (QUARTER, YEAR, etc.)         
          - Confidence scores                                 

                         
                         

           LEGACY SQL GENERATION (Enhanced)                   
                                                              
  System Prompt Components:                                   
  1. [NEW] Validated Column Mappings <- From Skills           
  2. Schema Context                                           
  3. Sample Data                                              
  4. Table Relationships                                      
  5. Derived Hints                                            
  6. Few-shot Examples                                        
  7. Critical Rules                                           
                                                              

                         
                         

                  LLM (Gemini/Claude)                         
                                                              
  Sees:                                                       
  - Validated mappings with high confidence                   
  - Full schema context for flexibility                       
  - Sample data for inference                                 
                                                              
  Benefits of Hybrid:                                         
   Skills prevent "quarter doesn't exist" errors            
   Legacy provides rich context and flexibility             
   Fallback if Skills fails                                 

```

## Four Core Skills

### 1. SchemaAnalysisSkill

**Purpose**: Identify relevant tables and extract business concepts

**Input**:
- User query
- Intent classification
- Full database schema

**Output**:
```python
{
    "mentioned_tables": ["MOBILE_DATA", "CUSTOMER_INFO"],
    "business_concepts": ["quarter", "revenue", "subscriber"],
    "confidence": 90
}
```

**How it works**:
- Pattern matching for table names (exact + partial)
- Regex extraction of temporal, metric, and entity concepts
- Confidence scoring based on findings

### 2. ColumnMappingSkill ( CRITICAL)

**Purpose**: Map user concepts to actual columns OR derived expressions

**Input**:
- Business concepts from SchemaAnalysisSkill
- Relevant tables
- Schema metadata
- Derived hints (from enrichment service)

**Output**:
```python
[
    ColumnMapping(
        concept="quarter",
        mapping_type=ColumnMappingType.DERIVED,
        expression="TO_CHAR(MOBILE_DATA.SRC_ID_DATE, 'Q')",
        table="MOBILE_DATA",
        confidence=95,
        note="Derived from DATE column using TO_CHAR"
    ),
    ColumnMapping(
        concept="revenue",
        mapping_type=ColumnMappingType.PHYSICAL,
        expression="MOBILE_DATA.DL_GOODPUT_MB",
        table="MOBILE_DATA",
        confidence=80,
        note="Partial column match"
    )
]
```

**Mapping Strategies** (in order):
1. **Physical Columns**: Exact or partial match with schema columns
2. **Derived Expressions**: Check derived hints (QUARTER, YEAR from DATE)
3. **Semantic Matching**: Use data types and sample data
4. **Aggregations**: Detect SUM, AVG, COUNT needs

**This solves the "quarter doesn't exist" problem**:
- User says "quarter" -> Skills maps to `TO_CHAR(DATE, 'Q')`
- LLM sees: "Use TO_CHAR(SRC_ID_DATE, 'Q') for quarter"
-  LLM won't try to SELECT QUARTER (which doesn't exist)
-  LLM will use TO_CHAR(SRC_ID_DATE, 'Q') AS QUARTER

### 3. SQLGenerationSkill

**Purpose**: Build enhanced prompt with explicit column mappings

**Input**:
- User query + intent
- Validated column mappings
- Schema context

**Output**:
```

 VALIDATED COLUMN MAPPINGS


 PHYSICAL COLUMNS:
   -  'revenue' -> MOBILE_DATA.DL_GOODPUT_MB

 DERIVED EXPRESSIONS:
   -  'quarter' -> TO_CHAR(MOBILE_DATA.SRC_ID_DATE, 'Q')
       This is NOT a physical column - use the expression as-is!


  CRITICAL: Prefer using these validated mappings!


[Rest of legacy prompt with schema, samples, etc.]
```

This prompt is **injected at the top** of the legacy prompt, so LLM sees validated mappings first.

### 4. ValidationSkill

**Purpose**: Validate generated SQL against schema and mappings

**Input**:
- Generated SQL
- Column mappings
- Schema data

**Output**:
```python
{
    "valid": True/False,
    "issues": [
        {"type": "derived_as_column", "concept": "quarter", ...}
    ],
    "confidence": 0-100
}
```

**Checks**:
- All column references exist in schema or mappings
- Derived expressions used correctly (not as bare column names)
- No invalid identifiers

## Integration with Existing System

### What Changed

**File**: `app/orchestrator/nodes/sql_generation.py` + `app/agents/sql_generation_skills.py`

**Changes**:
1. Import `SQLGenerationSkillsOrchestrator` in `generate_sql_node()`
2. Run Skills orchestrator BEFORE legacy prompt construction
3. If Skills succeeds -> inject validated mappings into legacy prompt
4. If Skills fails or is disabled -> legacy prompt works standalone (fallback)

**Critical**: The legacy system is **NOT replaced**, it's **enhanced**.

### Execution Flow

```python
async def generate_sql_node(state: QueryState):
    # 1. Initialize Skills
    skills_orchestrator = SQLGenerationSkillsOrchestrator()
    
    # 2. Run Skills pipeline
    skills_result = await skills_orchestrator.generate_sql(
        user_query, intent, schema_data, enriched_schema
    )
    
    # 3. Check for clarification (unmapped concepts)
    if skills_result["clarification_needed"]:
        # Return error with helpful message
        return state  # Exit early
    
    # 4. Extract validated column mappings
    column_mappings = skills_result["column_mappings"]
    
    # 5. Build column mapping enhancement
    column_mapping_enhancement = format_mappings(column_mappings)
    
    # 6. Inject into legacy prompt
    system_prompt = f"""You are an expert Oracle SQL generator.
    {column_mapping_enhancement}  <- NEW: Skills validation
    {schema_context}              <- Legacy context
    {sample_data_section}          <- Legacy context
    ...
    """
    
    # 7. Call LLM with enhanced prompt
    response = await llm.ainvoke(messages)
    
    # 8. Continue with validation, execution, etc.
```

## Benefits of Hybrid Approach

###  Accuracy
- Skills **validate** concepts before SQL generation
- Prevents "column doesn't exist" errors
- Maps concepts to derived expressions correctly

###  Flexibility
- LLM still has full schema context
- Can handle edge cases Skills might miss
- Few-shot examples provide patterns

###  Resilience
- If Skills fails -> legacy prompt still works
- Gradual migration without breaking changes
- A/B testable (can compare with/without Skills)

###  Debuggability
- Each Skill returns confidence scores
- Clear error messages when mapping fails
- Logging at each stage

###  Maintainability
- Skills are independent, testable units
- Easy to add new Skills (e.g., JoinOptimizationSkill)
- Clear separation of concerns

## Solving "Column Doesn't Exist" Errors

**Before**: LLM generates `SELECT QUARTER` -> ORA-00904 (column doesn't exist)

**After**: Skills maps "quarter" -> `TO_CHAR(SRC_ID_DATE, 'Q')` -> LLM uses correct expression

## Edge Cases

1. **No Physical Column** - Maps to derived expression
2. **Ambiguous Mapping** - Returns candidates with confidence scores
3. **Unmapped Concepts** - Requests clarification with available columns
4. **Multiple Tables** - Provides JOIN hints

## Configuration

### Enabling/Disabling Skills

**To disable Skills** (fallback to legacy only):

Use the existing configuration flag checked in `generate_sql_node`:

```python
from app.core.config import settings

if getattr(settings, "QUERY_SQL_SKILLS_ENABLED", True):
    skills_result = await skills_orchestrator.generate_sql(...)
    # ... inject mappings into prompt ...
else:
    # Legacy prompt path only
    ...
```

### Confidence Thresholds

**Adjust mapping confidence requirements**:
```python
# In sql_generation_skills.py, line ~695
if unmapped and mapping_result.confidence < 30:  # Adjust threshold
    # Request clarification
```

### Logging

**Enable debug logging for Skills**:
```python
import logging
logging.getLogger('app.agents.sql_generation_skills').setLevel(logging.DEBUG)
```

## Testing

Each Skill is independently testable. See `tests/test_sql_generation_skills.py` for examples.

## Future Enhancements

1. **JoinOptimizationSkill** - Optimal JOIN strategies
2. **IndexHintSkill** - Oracle index hints
3. **Self-Correction Loop** - Retry with error context
4. **Pattern Learning** - Store successful queries in Graphiti

## Troubleshooting

- **Clarification when mappable** - Check `enriched_schema["derived_hints"]`
- **LLM ignores mappings** - Increase prompt emphasis
- **Performance issues** - Add caching layer with `@lru_cache`

## Summary

The Skills-based architecture provides:

 **Accuracy**: Systematic column mapping prevents errors  
 **Flexibility**: LLM retains full context and reasoning ability  
 **Resilience**: Fallback to legacy if Skills fails  
 **Debuggability**: Clear logging and error messages  
 **Maintainability**: Composable, testable Skills  

**Key Innovation**: Skills **enhance** rather than **replace** the existing system, providing the best of both worlds.
