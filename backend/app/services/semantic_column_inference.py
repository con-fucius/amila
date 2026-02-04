"""
Semantic Column Name Inference Service

Infers semantic meaning of ambiguous column names like:
- col01, col02 → actual business meaning
- table1, field_a → semantic names
- Generic codes → business terms

Uses:
- Sample data analysis
- Pattern matching
- LLM-based inference
- Business glossary
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

from app.core.redis_client import redis_client
from app.orchestrator.llm_config import get_llm

logger = logging.getLogger(__name__)


@dataclass
class InferredColumn:
    """Inferred column information"""
    original_name: str
    inferred_name: str
    inferred_type: str
    confidence: float
    reasoning: str
    suggested_business_name: str
    sample_analysis: Dict[str, Any]
    inferred_at: str


class SemanticColumnInference:
    """
    Service for inferring semantic meaning of ambiguous column names.
    
    Handles patterns like:
    - Generic: COL01, FIELD_A, ATTR1
    - Abbreviated: DPTCD, EMID, TXAMT
    - Legacy: TABLE1_COL, F_001
    """
    
    # Pattern matchers for ambiguous names
    AMBIGUOUS_PATTERNS = [
        (r'^col\d+$', 'generic_column_number'),  # col1, col01, col123
        (r'^column\d*$', 'generic_column_word'),  # column, column1
        (r'^field[_-]?\w*$', 'generic_field'),  # field_a, field1
        (r'^attr\d*$', 'generic_attribute'),  # attr, attr1
        (r'^val\d*$', 'generic_value'),  # val, val1
        (r'^f\d+$', 'generic_f_number'),  # f01, f123
        (r'^c\d+$', 'generic_c_number'),  # c1, c01
        (r'^[a-z]_\d+$', 'legacy_underscore'),  # a_1, x_99
        (r'^table\d*_', 'legacy_table_prefix'),  # table1_col
        (r'^(data|text|desc|info)\d*$', 'generic_data'),  # data1, text2
    ]
    
    # Abbreviation expansion dictionary
    ABBREVIATIONS = {
        # People
        'em': 'employee', 'emp': 'employee', 'empl': 'employee',
        'cust': 'customer', 'cst': 'customer', 'cli': 'client',
        'usr': 'user', 'usrid': 'user_id',
        'mgr': 'manager', 'spvr': 'supervisor',
        'fst': 'first', 'lst': 'last', 'mid': 'middle',
        'nm': 'name', 'fn': 'first_name', 'ln': 'last_name',
        
        # Dates
        'dt': 'date', 'dat': 'date', 'dte': 'date',
        'ts': 'timestamp', 'tms': 'timestamp',
        'crtdt': 'created_date', 'upddt': 'updated_date',
        'stdt': 'start_date', 'endt': 'end_date',
        
        # Financial
        'amt': 'amount', 'amnt': 'amount',
        'prc': 'price', 'cost': 'cost',
        'tot': 'total', 'totamt': 'total_amount',
        'bal': 'balance', 'outbal': 'outstanding_balance',
        'pmt': 'payment', 'pymt': 'payment',
        'tx': 'transaction', 'txn': 'transaction',
        'ccy': 'currency', 'curr': 'currency',
        'disc': 'discount', 'dscnt': 'discount',
        'tax': 'tax', 'txamt': 'tax_amount',
        
        # Identifiers
        'id': 'id', 'num': 'number', 'no': 'number',
        'cd': 'code', 'cde': 'code', 'cod': 'code',
        'ref': 'reference', 'refno': 'reference_number',
        'ext': 'external', 'extid': 'external_id',
        
        # Locations
        'addr': 'address', 'adrs': 'address',
        'cty': 'city', 'st': 'state', 'ste': 'state',
        'zip': 'zip_code', 'zipcd': 'zip_code',
        'cntry': 'country', 'ctry': 'country',
        'reg': 'region', 'terr': 'territory',
        
        # Organization
        'dept': 'department', 'dpt': 'department', 'dptcd': 'department_code',
        'div': 'division', 'divcd': 'division_code',
        'br': 'branch', 'brcd': 'branch_code',
        'co': 'company', 'cmpy': 'company', 'ent': 'entity',
        'bu': 'business_unit', 'sbu': 'strategic_business_unit',
        
        # Status/Type
        'sts': 'status', 'stat': 'status', 'stts': 'status',
        'typ': 'type', 'ty': 'type', 'cat': 'category',
        'cls': 'class', 'clsf': 'classification',
        'flg': 'flag', 'ind': 'indicator',
        
        # Quantities
        'qty': 'quantity', 'qnty': 'quantity',
        'cnt': 'count', 'ct': 'count',
        'vol': 'volume', 'wt': 'weight',
        
        # Communication
        'ph': 'phone', 'phn': 'phone', 'tel': 'telephone',
        'eml': 'email', 'mail': 'email',
        'fax': 'fax', 'fx': 'fax',
        'url': 'url', 'web': 'website',
        
        # Products
        'prd': 'product', 'prod': 'product', 'prdid': 'product_id',
        'sku': 'sku', 'itm': 'item', 'item': 'item',
        'svc': 'service', 'serv': 'service',
        
        # Orders
        'ord': 'order', 'ordno': 'order_number',
        'lnitm': 'line_item', 'lnno': 'line_number',
        'po': 'purchase_order', 'pono': 'po_number',
        'inv': 'invoice', 'invno': 'invoice_number',
        
        # Descriptions
        'desc': 'description', 'descr': 'description',
        'dsc': 'description', 'txt': 'text',
        'cmt': 'comment', 'cmnt': 'comment', 'note': 'notes',
        'titl': 'title', 'ttl': 'title',
        
        # Misc
        'pct': 'percent', 'pcnt': 'percent', '%': 'percentage',
        'src': 'source', 'orig': 'origin',
        'dest': 'destination', 'tgt': 'target',
        'prio': 'priority', 'pri': 'priority',
        'seq': 'sequence', 'sq': 'sequence',
        'ver': 'version', 'rev': 'revision',
    }
    
    # Data type inference patterns
    DATA_TYPE_PATTERNS = {
        'id': 'identifier',
        '_id': 'foreign_key',
        '_date': 'date',
        '_dt': 'date',
        '_ts': 'timestamp',
        '_time': 'time',
        '_at': 'timestamp',
        '_amt': 'amount',
        '_amount': 'amount',
        '_price': 'price',
        '_cost': 'cost',
        '_qty': 'quantity',
        '_quantity': 'quantity',
        '_count': 'count',
        '_num': 'number',
        '_code': 'code',
        '_flg': 'flag',
        '_flag': 'flag',
        '_ind': 'indicator',
        '_desc': 'description',
        '_text': 'text',
        '_note': 'notes',
        '_name': 'name',
        '_email': 'email',
        '_phone': 'phone',
        '_addr': 'address',
    }
    
    def __init__(self):
        self._cache_ttl = 86400 * 30  # 30 days
    
    async def infer_column(
        self,
        column_name: str,
        sample_values: List[Any],
        data_type: Optional[str] = None,
        table_context: Optional[str] = None,
        use_llm: bool = True
    ) -> InferredColumn:
        """
        Infer semantic meaning of an ambiguous column name.
        
        Args:
            column_name: The ambiguous column name
            sample_values: Sample values from the column
            data_type: Optional SQL data type
            table_context: Optional table name for context
            use_llm: Whether to use LLM for inference
            
        Returns:
            InferredColumn with semantic information
        """
        # Check cache
        cache_key = f"col:inference:{column_name}:{table_context or 'generic'}"
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                return InferredColumn(**cached)
        except Exception:
            pass
        
        # Check if name is ambiguous
        is_ambiguous = self._is_ambiguous_name(column_name)
        
        # Analyze sample data
        sample_analysis = self._analyze_samples(sample_values)
        
        # Try abbreviation expansion
        expanded = self._expand_abbreviations(column_name)
        
        # Try pattern-based inference
        pattern_type = self._infer_from_pattern(column_name)
        
        # Use LLM for complex cases
        if use_llm and (is_ambiguous or confidence_low(expanded, 0.7)):
            llm_result = await self._llm_inference(
                column_name, sample_values, data_type, table_context
            )
            inferred_name = llm_result.get('name', expanded)
            inferred_type = llm_result.get('type', pattern_type)
            confidence = llm_result.get('confidence', 0.6)
            reasoning = llm_result.get('reasoning', 'LLM inference')
        else:
            inferred_name = expanded if not is_ambiguous else self._generate_name_from_samples(
                column_name, sample_analysis
            )
            inferred_type = pattern_type or sample_analysis.get('inferred_type', 'unknown')
            confidence = 0.8 if not is_ambiguous else 0.5
            reasoning = f"Pattern-based inference from {column_name}"
        
        # Generate business name
        business_name = self._generate_business_name(inferred_name)
        
        result = InferredColumn(
            original_name=column_name,
            inferred_name=inferred_name,
            inferred_type=inferred_type,
            confidence=confidence,
            reasoning=reasoning,
            suggested_business_name=business_name,
            sample_analysis=sample_analysis,
            inferred_at=datetime.now(timezone.utc).isoformat()
        )
        
        # Cache result
        try:
            await redis_client.set(cache_key, result.__dict__, ttl=self._cache_ttl)
        except Exception:
            pass
        
        return result
    
    def _is_ambiguous_name(self, column_name: str) -> bool:
        """Check if column name is ambiguous/generic"""
        col_lower = column_name.lower()
        
        for pattern, pattern_type in self.AMBIGUOUS_PATTERNS:
            if re.match(pattern, col_lower):
                return True
        
        return False
    
    def _analyze_samples(self, sample_values: List[Any]) -> Dict[str, Any]:
        """Analyze sample values to infer meaning"""
        if not sample_values:
            return {"inferred_type": "unknown", "patterns": []}
        
        # Filter None values
        values = [str(v) for v in sample_values if v is not None]
        if not values:
            return {"inferred_type": "unknown", "patterns": []}
        
        analysis = {
            "sample_count": len(values),
            "unique_count": len(set(values)),
            "avg_length": sum(len(v) for v in values) / len(values),
            "patterns": []
        }
        
        # Check for specific patterns
        if all(v.isdigit() for v in values[:10]):
            analysis["patterns"].append("numeric")
            analysis["inferred_type"] = "number"
        
        elif all(re.match(r'^\d{4}-\d{2}-\d{2}', v) for v in values[:5]):
            analysis["patterns"].append("date_iso")
            analysis["inferred_type"] = "date"
        
        elif all('@' in v for v in values[:5]):
            analysis["patterns"].append("email")
            analysis["inferred_type"] = "email"
        
        elif all(v.lower() in ['true', 'false', '0', '1', 'yes', 'no'] for v in values[:10]):
            analysis["patterns"].append("boolean")
            analysis["inferred_type"] = "flag"
        
        elif analysis["avg_length"] > 50:
            analysis["patterns"].append("long_text")
            analysis["inferred_type"] = "description"
        
        elif analysis["unique_count"] / len(values) < 0.1:
            analysis["patterns"].append("low_cardinality")
            analysis["inferred_type"] = "category"
        
        else:
            analysis["inferred_type"] = "text"
        
        return analysis
    
    def _expand_abbreviations(self, column_name: str) -> str:
        """Expand abbreviated column name"""
        parts = re.split(r'[_\-\s]', column_name.lower())
        expanded_parts = []
        
        for part in parts:
            if part in self.ABBREVIATIONS:
                expanded_parts.append(self.ABBREVIATIONS[part])
            else:
                expanded_parts.append(part)
        
        return '_'.join(expanded_parts)
    
    def _infer_from_pattern(self, column_name: str) -> Optional[str]:
        """Infer type from column naming pattern"""
        col_lower = column_name.lower()
        
        for pattern, data_type in self.DATA_TYPE_PATTERNS.items():
            if col_lower.endswith(pattern):
                return data_type
        
        return None
    
    def _generate_name_from_samples(
        self,
        original_name: str,
        sample_analysis: Dict[str, Any]
    ) -> str:
        """Generate inferred name from sample analysis"""
        inferred_type = sample_analysis.get("inferred_type", "value")
        
        # Extract base from original name
        base = re.sub(r'\d+$', '', original_name)
        base = re.sub(r'^col|column|field|attr|val', '', base, flags=re.IGNORECASE)
        
        if base:
            return f"{base}_{inferred_type}"
        
        return inferred_type
    
    def _generate_business_name(self, inferred_name: str) -> str:
        """Generate business-friendly name"""
        # Convert snake_case to Title Case
        words = inferred_name.split('_')
        return ' '.join(word.capitalize() for word in words)
    
    async def _llm_inference(
        self,
        column_name: str,
        sample_values: List[Any],
        data_type: Optional[str],
        table_context: Optional[str]
    ) -> Dict[str, Any]:
        """Use LLM to infer column meaning"""
        try:
            llm = get_llm()
            
            samples_str = ", ".join([str(v)[:30] for v in sample_values[:5] if v is not None])
            
            prompt = f"""Infer the semantic meaning of a database column.

Column Name: {column_name}
Data Type: {data_type or 'Unknown'}
Table Context: {table_context or 'Unknown'}
Sample Values: {samples_str}

Provide JSON with:
- name: inferred semantic column name (snake_case)
- type: inferred business type
- confidence: 0.0-1.0
- reasoning: brief explanation

Response:"""

            response = await llm.ainvoke([{"type": "human", "content": prompt}])
            content = response.content if hasattr(response, 'content') else str(response)
            
            # Try to extract JSON
            try:
                import json
                # Find JSON in response
                json_match = re.search(r'\{[^}]+\}', content)
                if json_match:
                    return json.loads(json_match.group())
            except Exception:
                pass
            
            # Fallback
            return {
                "name": column_name,
                "type": "unknown",
                "confidence": 0.3,
                "reasoning": "LLM parsing failed"
            }
            
        except Exception as e:
            logger.warning(f"LLM inference failed: {e}")
            return {
                "name": column_name,
                "type": "unknown",
                "confidence": 0.0,
                "reasoning": f"Error: {str(e)}"
            }
    
    async def batch_infer_columns(
        self,
        columns: List[Dict[str, Any]],
        table_context: Optional[str] = None,
        use_llm: bool = True
    ) -> Dict[str, InferredColumn]:
        """
        Infer semantic meaning for multiple columns.
        
        Args:
            columns: List of column metadata dicts with 'name', 'type', 'samples'
            table_context: Optional table name
            use_llm: Whether to use LLM
            
        Returns:
            Dict mapping original names to InferredColumn
        """
        results = {}
        
        for col in columns:
            col_name = col.get('name', '')
            col_type = col.get('type')
            samples = col.get('samples', [])
            
            try:
                inferred = await self.infer_column(
                    column_name=col_name,
                    sample_values=samples,
                    data_type=col_type,
                    table_context=table_context,
                    use_llm=use_llm
                )
                results[col_name] = inferred
            except Exception as e:
                logger.error(f"Failed to infer column {col_name}: {e}")
                # Add fallback
                results[col_name] = InferredColumn(
                    original_name=col_name,
                    inferred_name=col_name,
                    inferred_type=col_type or "unknown",
                    confidence=0.0,
                    reasoning="Inference failed",
                    suggested_business_name=col_name,
                    sample_analysis={},
                    inferred_at=datetime.now(timezone.utc).isoformat()
                )
        
        return results


def confidence_low(confidence_or_value, threshold: float) -> bool:
    """Helper to check if confidence/value is below threshold"""
    if isinstance(confidence_or_value, (int, float)):
        return confidence_or_value < threshold
    return False


# Global instance
semantic_inference = SemanticColumnInference()


# Convenience functions

async def infer_column_name(
    column_name: str,
    sample_values: List[Any],
    data_type: Optional[str] = None,
    use_llm: bool = True
) -> InferredColumn:
    """Infer semantic meaning of a column name"""
    return await semantic_inference.infer_column(
        column_name, sample_values, data_type, use_llm=use_llm
    )


async def expand_column_abbreviations(column_name: str) -> str:
    """Expand abbreviations in column name"""
    return semantic_inference._expand_abbreviations(column_name)