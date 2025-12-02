"""
Smart Context Management
Ranks and limits schema tables for LLM context using keyword relevance + semantic similarity.
Enhanced with token budget tracking and smart truncation.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class SmartContextManager:
    # Approximate tokens per character (rough estimate: 1 token  4 chars)
    CHARS_PER_TOKEN = 4
    
    # Default token budgets for different LLM contexts
    DEFAULT_TOKEN_BUDGET = 8000  # Conservative budget for schema context
    GEMINI_TOKEN_BUDGET = 10000  # Gemini Flash has larger context
    BEDROCK_TOKEN_BUDGET = 8000   # Claude 3.5 Sonnet
    
    @staticmethod
    def rank_tables(
        user_query: str,
        dynamic_tables: List[str],
        semantic_hits: List[Dict],
    ) -> List[Tuple[str, float]]:
        """Combine keyword/dynamic discovery with semantic similarity.
        Returns list of (table, score) descending.
        """
        # Base scores
        scores: Dict[str, float] = {t: 1.0 for t in dynamic_tables}
        # Keyword bonus
        uq = user_query.upper()
        for t in list(scores.keys()):
            bonus = 0.0
            if t.upper() in uq:
                bonus += 2.0
            for part in t.upper().split("_"):
                if part and part in uq:
                    bonus += 0.25
            scores[t] += bonus
        # Semantic hits bonus
        for hit in semantic_hits[:20]:
            table = (hit.get("table") or hit.get("name") or "").upper()
            if not table:
                continue
            # Normalize table name by dropping column component if present in key
            tname = table
            # Increment score or create
            scores[tname] = scores.get(tname, 0.0) + 1.5
        # Sort
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        return ranked

    @staticmethod
    def select_top_tables(ranked: List[Tuple[str, float]], max_tables: int = 12) -> List[str]:
        return [t for t, _ in ranked[:max_tables]]
    
    @staticmethod
    def estimate_schema_tokens(schema_data: Dict) -> int:
        """
        Estimate token count for schema data
        
        Args:
            schema_data: Schema metadata dict with tables/columns
            
        Returns:
            Estimated token count
        """
        total_chars = 0
        tables = schema_data.get("tables", {}) or {}
        
        for table_name, columns in tables.items():
            # Table name
            total_chars += len(table_name) + 10  # + formatting overhead
            
            # Column details
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                total_chars += len(col_name) + len(col_type) + 5  # + formatting
        
        # Convert chars to tokens (approximate)
        return total_chars // SmartContextManager.CHARS_PER_TOKEN
    
    @staticmethod
    def filter_schema_by_token_budget(
        schema_data: Dict,
        ranked_tables: List[Tuple[str, float]],
        token_budget: int = DEFAULT_TOKEN_BUDGET,
    ) -> Dict:
        """
        Filter schema to fit within token budget, prioritizing highest-ranked tables
        
        Args:
            schema_data: Full schema metadata
            ranked_tables: Tables ranked by relevance (table, score)
            token_budget: Maximum tokens allowed for schema context
            
        Returns:
            Filtered schema dict that fits within budget
        """
        filtered_schema = {"tables": {}, "views": {}}
        current_tokens = 0
        tables = schema_data.get("tables", {}) or {}
        
        for table_name, score in ranked_tables:
            if table_name not in tables:
                continue
            
            # Estimate tokens for this table
            table_chars = len(table_name) + 10
            columns = tables[table_name]
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                table_chars += len(col_name) + len(col_type) + 5
            
            table_tokens = table_chars // SmartContextManager.CHARS_PER_TOKEN
            
            # Check if adding this table would exceed budget
            if current_tokens + table_tokens > token_budget:
                logger.warning(
                    f"Token budget reached ({current_tokens}/{token_budget}). "
                    f"Excluding lower-priority tables (starting with {table_name})"
                )
                break
            
            # Add table to filtered schema
            filtered_schema["tables"][table_name] = columns
            current_tokens += table_tokens
        
        logger.info(
            f"Schema filtered to {len(filtered_schema['tables'])} tables "
            f"({current_tokens} tokens, budget: {token_budget})"
        )
        
        return filtered_schema
    
    @staticmethod
    def get_token_budget_for_provider(provider: str) -> int:
        """
        Get appropriate token budget based on LLM provider
        
        Args:
            provider: LLM provider name ('gemini' or 'bedrock')
            
        Returns:
            Token budget for schema context
        """
        if provider.lower() == "gemini":
            return SmartContextManager.GEMINI_TOKEN_BUDGET
        elif provider.lower() == "bedrock":
            return SmartContextManager.BEDROCK_TOKEN_BUDGET
        else:
            return SmartContextManager.DEFAULT_TOKEN_BUDGET
