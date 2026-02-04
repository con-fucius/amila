"""
Schema Description Auto-Generation Service

Automatically generates column descriptions using LLM analysis of:
- Column names and types
- Sample data values
- Table context
- Business domain patterns

Features:
- LLM-based description generation
- Caching to reduce API calls
- Batch processing for efficiency
- Confidence scoring
- Admin review workflow support
"""

import logging
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.orchestrator.llm_config import get_llm

logger = logging.getLogger(__name__)


@dataclass
class ColumnDescription:
    """Generated column description"""
    column_name: str
    data_type: str
    description: str
    business_meaning: str
    example_values: List[str]
    confidence_score: float
    inferred_category: str
    suggested_aggregations: List[str]
    generated_at: str


@dataclass
class TableDescription:
    """Generated table description"""
    table_name: str
    description: str
    business_purpose: str
    column_descriptions: Dict[str, ColumnDescription]
    related_tables: List[str]
    data_volume_estimate: str
    update_frequency: str
    confidence_score: float
    generated_at: str


class SchemaDescriptionService:
    """
    Service for auto-generating schema descriptions using LLM.
    
    Analyzes:
    - Column naming patterns
    - Sample data values
    - Table relationships
    - Business context
    """
    
    # Common column patterns for quick inference
    COLUMN_PATTERNS = {
        "id": {"category": "identifier", "description": "Unique identifier"},
        "_id": {"category": "identifier", "description": "Foreign key reference"},
        "name": {"category": "name", "description": "Name or title"},
        "_name": {"category": "name", "description": "Descriptive name"},
        "date": {"category": "temporal", "description": "Date value"},
        "_date": {"category": "temporal", "description": "Date timestamp"},
        "time": {"category": "temporal", "description": "Time value"},
        "_at": {"category": "temporal", "description": "Timestamp"},
        "amount": {"category": "financial", "description": "Monetary amount"},
        "price": {"category": "financial", "description": "Price value"},
        "cost": {"category": "financial", "description": "Cost value"},
        "total": {"category": "financial", "description": "Total amount"},
        "status": {"category": "status", "description": "Current status"},
        "type": {"category": "categorization", "description": "Type or category"},
        "code": {"category": "categorization", "description": "Code value"},
        "flag": {"category": "indicator", "description": "Boolean indicator"},
        "is_": {"category": "indicator", "description": "Boolean flag"},
        "count": {"category": "metric", "description": "Count value"},
        "num": {"category": "metric", "description": "Numeric value"},
        "qty": {"category": "metric", "description": "Quantity"},
        "email": {"category": "contact", "description": "Email address"},
        "phone": {"category": "contact", "description": "Phone number"},
        "address": {"category": "location", "description": "Physical address"},
        "city": {"category": "location", "description": "City name"},
        "notes": {"category": "text", "description": "Notes or comments"},
        "desc": {"category": "text", "description": "Description"},
        "comment": {"category": "text", "description": "Comment or note"},
    }
    
    # Cache TTL
    DESCRIPTION_CACHE_TTL = 86400 * 30  # 30 days
    
    async def generate_column_description(
        self,
        column_name: str,
        data_type: str,
        sample_values: List[Any],
        table_context: Optional[str] = None,
        use_llm: bool = True
    ) -> ColumnDescription:
        """
        Generate description for a single column.
        
        Args:
            column_name: Name of the column
            data_type: SQL data type
            sample_values: Sample values from the column
            table_context: Optional table context
            use_llm: Whether to use LLM for generation
            
        Returns:
            ColumnDescription with generated metadata
        """
        # Check cache first
        cache_key = f"schema:desc:col:{column_name}:{data_type}"
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                return ColumnDescription(**cached)
        except Exception:
            pass
        
        # Pattern-based inference
        pattern_info = self._infer_from_pattern(column_name)
        
        # Sample-based inference
        sample_info = self._infer_from_samples(sample_values)
        
        # Generate description
        if use_llm and len(sample_values) > 0:
            description, confidence = await self._generate_llm_description(
                column_name, data_type, sample_values, table_context
            )
        else:
            description = pattern_info.get("description", f"{column_name} column")
            confidence = pattern_info.get("confidence", 0.5)
        
        # Determine category
        category = pattern_info.get("category", sample_info.get("category", "general"))
        
        # Suggest aggregations
        aggregations = self._suggest_aggregations(data_type, category)
        
        col_desc = ColumnDescription(
            column_name=column_name,
            data_type=data_type,
            description=description,
            business_meaning=self._generate_business_meaning(column_name, category),
            example_values=[str(v)[:50] for v in sample_values[:3] if v is not None],
            confidence_score=confidence,
            inferred_category=category,
            suggested_aggregations=aggregations,
            generated_at=datetime.now(timezone.utc).isoformat()
        )
        
        # Cache result
        try:
            await redis_client.set(cache_key, col_desc.__dict__, ttl=self.DESCRIPTION_CACHE_TTL)
        except Exception:
            pass
        
        return col_desc
    
    async def generate_table_description(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        sample_data: Optional[List[Dict]] = None,
        related_tables: Optional[List[str]] = None,
        use_llm: bool = True
    ) -> TableDescription:
        """
        Generate description for an entire table.
        
        Args:
            table_name: Name of the table
            columns: List of column metadata
            sample_data: Optional sample rows
            related_tables: Optional related table names
            use_llm: Whether to use LLM for generation
            
        Returns:
            TableDescription with generated metadata
        """
        # Check cache
        cache_key = f"schema:desc:table:{table_name}"
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                return TableDescription(**cached)
        except Exception:
            pass
        
        # Generate column descriptions
        column_descriptions = {}
        total_confidence = 0
        
        for col in columns:
            col_name = col.get('name', '')
            col_type = col.get('type', 'UNKNOWN')
            
            # Get sample values for this column
            samples = []
            if sample_data:
                samples = [row.get(col_name) for row in sample_data[:10]]
            
            col_desc = await self.generate_column_description(
                column_name=col_name,
                data_type=col_type,
                sample_values=samples,
                table_context=table_name,
                use_llm=use_llm and len(samples) > 0
            )
            
            column_descriptions[col_name] = col_desc
            total_confidence += col_desc.confidence_score
        
        # Generate table-level description
        avg_confidence = total_confidence / len(columns) if columns else 0
        
        if use_llm:
            table_desc_text = await self._generate_table_llm_description(
                table_name, columns, sample_data
            )
        else:
            table_desc_text = self._generate_table_description_pattern(table_name)
        
        table_desc = TableDescription(
            table_name=table_name,
            description=table_desc_text,
            business_purpose=self._infer_business_purpose(table_name),
            column_descriptions=column_descriptions,
            related_tables=related_tables or [],
            data_volume_estimate="Unknown",
            update_frequency="Unknown",
            confidence_score=avg_confidence,
            generated_at=datetime.now(timezone.utc).isoformat()
        )
        
        # Cache result
        try:
            await redis_client.set(cache_key, table_desc.__dict__, ttl=self.DESCRIPTION_CACHE_TTL)
        except Exception:
            pass
        
        return table_desc
    
    def _infer_from_pattern(self, column_name: str) -> Dict[str, Any]:
        """Infer column information from naming patterns"""
        col_lower = column_name.lower()
        
        # Check exact matches
        if col_lower in self.COLUMN_PATTERNS:
            return {
                **self.COLUMN_PATTERNS[col_lower],
                "confidence": 0.8
            }
        
        # Check suffix/prefix matches
        for pattern, info in self.COLUMN_PATTERNS.items():
            if pattern.startswith('_') and col_lower.endswith(pattern):
                return {**info, "confidence": 0.7}
            if pattern.endswith('_') and col_lower.startswith(pattern[:-1]):
                return {**info, "confidence": 0.7}
        
        # Check partial matches
        for pattern, info in self.COLUMN_PATTERNS.items():
            clean_pattern = pattern.strip('_')
            if clean_pattern in col_lower:
                return {**info, "confidence": 0.6}
        
        return {
            "category": "general",
            "description": f"{column_name} value",
            "confidence": 0.3
        }
    
    def _infer_from_samples(self, sample_values: List[Any]) -> Dict[str, Any]:
        """Infer column information from sample values"""
        if not sample_values:
            return {"category": "unknown", "confidence": 0.0}
        
        # Filter out None values
        values = [v for v in sample_values if v is not None]
        if not values:
            return {"category": "unknown", "confidence": 0.0}
        
        # Check for date patterns
        date_patterns = ['-', '/', ':']
        sample_str = str(values[0])
        if any(p in sample_str for p in date_patterns):
            if len(sample_str) in [10, 19, 23]:  # Common date lengths
                return {"category": "temporal", "confidence": 0.7}
        
        # Check for email
        if '@' in sample_str and '.' in sample_str:
            return {"category": "contact", "confidence": 0.9}
        
        # Check for numeric
        if all(str(v).replace('.', '').isdigit() for v in values[:5] if v):
            return {"category": "metric", "confidence": 0.6}
        
        # Check for boolean
        bool_values = {'true', 'false', '0', '1', 'yes', 'no', 'y', 'n'}
        if all(str(v).lower() in bool_values for v in values[:5] if v):
            return {"category": "indicator", "confidence": 0.8}
        
        return {"category": "general", "confidence": 0.3}
    
    def _generate_business_meaning(self, column_name: str, category: str) -> str:
        """Generate business meaning description"""
        category_meanings = {
            "identifier": "Used to uniquely identify records",
            "name": "Descriptive name for display purposes",
            "temporal": "Used for time-based analysis and filtering",
            "financial": "Monetary value for reporting and calculations",
            "status": "Indicates current state or condition",
            "categorization": "Used for grouping and classification",
            "indicator": "Boolean flag for filtering",
            "metric": "Quantitative value for measurements",
            "contact": "Contact information for communication",
            "location": "Geographic or physical location data",
            "text": "Descriptive text or comments"
        }
        
        return category_meanings.get(category, f"Business data for {column_name}")
    
    def _suggest_aggregations(self, data_type: str, category: str) -> List[str]:
        """Suggest appropriate aggregations for the column"""
        type_upper = data_type.upper()
        
        if category in ["financial", "metric"]:
            if 'INT' in type_upper or 'NUM' in type_upper or 'DEC' in type_upper or 'FLOAT' in type_upper:
                return ["SUM", "AVG", "MIN", "MAX", "COUNT"]
        
        if category in ["temporal"]:
            return ["MIN", "MAX", "COUNT"]
        
        if category in ["indicator", "status", "categorization"]:
            return ["COUNT", "GROUP BY"]
        
        if category in ["identifier"]:
            return ["COUNT", "COUNT DISTINCT"]
        
        return ["COUNT"]
    
    async def _generate_llm_description(
        self,
        column_name: str,
        data_type: str,
        sample_values: List[Any],
        table_context: Optional[str] = None
    ) -> tuple:
        """Generate description using LLM"""
        try:
            llm = get_llm()
            
            # Prepare sample values string
            samples_str = ", ".join([str(v)[:30] for v in sample_values[:5] if v is not None])
            
            prompt = f"""Generate a concise business description for a database column.

Column Name: {column_name}
Data Type: {data_type}
Sample Values: {samples_str}
Table Context: {table_context or 'Unknown'}

Provide:
1. A clear business description (1 sentence)
2. Confidence score (0.0-1.0)

Format: Description|Confidence"""

            response = await llm.ainvoke([{"type": "human", "content": prompt}])
            content = response.content if hasattr(response, 'content') else str(response)
            
            # Parse response
            parts = content.split('|')
            if len(parts) >= 2:
                description = parts[0].strip()
                try:
                    confidence = float(parts[1].strip())
                except ValueError:
                    confidence = 0.7
            else:
                description = content.strip()
                confidence = 0.7
            
            return description, min(1.0, max(0.0, confidence))
            
        except Exception as e:
            logger.warning(f"LLM description generation failed: {e}")
            return f"{column_name} column ({data_type})", 0.5
    
    async def _generate_table_llm_description(
        self,
        table_name: str,
        columns: List[Dict],
        sample_data: Optional[List[Dict]]
    ) -> str:
        """Generate table description using LLM"""
        try:
            llm = get_llm()
            
            col_summary = ", ".join([c.get('name', '') for c in columns[:10]])
            
            prompt = f"""Generate a concise business description for a database table.

Table Name: {table_name}
Columns: {col_summary}
Total Columns: {len(columns)}

Provide a single sentence describing the business purpose of this table."""

            response = await llm.ainvoke([{"type": "human", "content": prompt}])
            return response.content.strip() if hasattr(response, 'content') else str(response)
            
        except Exception as e:
            logger.warning(f"LLM table description failed: {e}")
            return self._generate_table_description_pattern(table_name)
    
    def _generate_table_description_pattern(self, table_name: str) -> str:
        """Generate table description from naming patterns"""
        table_lower = table_name.lower()
        
        patterns = {
            "user": "Contains user account information and profiles",
            "customer": "Stores customer information and details",
            "order": "Contains order transactions and details",
            "product": "Stores product catalog information",
            "transaction": "Contains financial transaction records",
            "payment": "Stores payment processing information",
            "employee": "Contains employee records and details",
            "department": "Stores organizational department information",
            "account": "Contains account records and balances",
            "log": "Stores audit log or activity records",
            "config": "Contains configuration settings",
            "ref": "Reference data and lookup values",
            "lookup": "Lookup and reference values",
            "hist": "Historical data and archive records",
            "audit": "Audit trail and compliance records"
        }
        
        for pattern, description in patterns.items():
            if pattern in table_lower:
                return description
        
        return f"Contains {table_name} information and data"
    
    def _infer_business_purpose(self, table_name: str) -> str:
        """Infer business purpose from table name"""
        table_lower = table_name.lower()
        
        purposes = {
            "sales": "Sales operations and revenue tracking",
            "finance": "Financial management and reporting",
            "hr": "Human resources and employee management",
            "inventory": "Inventory and stock management",
            "customer": "Customer relationship management",
            "product": "Product catalog and management",
            "order": "Order processing and fulfillment",
            "marketing": "Marketing campaigns and analytics",
            "operations": "Operational data and processes",
            "compliance": "Compliance and regulatory data",
        }
        
        for key, purpose in purposes.items():
            if key in table_lower:
                return purpose
        
        return "General business operations"
    
    async def batch_generate_descriptions(
        self,
        tables: List[Dict[str, Any]],
        use_llm: bool = True
    ) -> Dict[str, TableDescription]:
        """
        Generate descriptions for multiple tables.
        
        Args:
            tables: List of table metadata
            use_llm: Whether to use LLM for generation
            
        Returns:
            Dict mapping table names to TableDescription
        """
        results = {}
        
        for table_info in tables:
            table_name = table_info.get('name', '')
            columns = table_info.get('columns', [])
            sample_data = table_info.get('sample_data', [])
            
            try:
                table_desc = await self.generate_table_description(
                    table_name=table_name,
                    columns=columns,
                    sample_data=sample_data,
                    use_llm=use_llm
                )
                results[table_name] = table_desc
            except Exception as e:
                logger.error(f"Failed to generate description for {table_name}: {e}")
        
        return results


# Global instance
schema_description_service = SchemaDescriptionService()


# Convenience functions

async def generate_column_desc(
    column_name: str,
    data_type: str,
    sample_values: List[Any],
    use_llm: bool = True
) -> ColumnDescription:
    """Generate description for a column"""
    return await schema_description_service.generate_column_description(
        column_name, data_type, sample_values, use_llm=use_llm
    )


async def generate_table_desc(
    table_name: str,
    columns: List[Dict[str, Any]],
    sample_data: Optional[List[Dict]] = None,
    use_llm: bool = True
) -> TableDescription:
    """Generate description for a table"""
    return await schema_description_service.generate_table_description(
        table_name, columns, sample_data, use_llm=use_llm
    )