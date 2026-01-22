"""
Metadata QA Service
Answers natural language questions about the database schema and data definitions
using the schema metadata and LLM.
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class MetadataQAService:
    """
    Service for answering questions about database schema, table definitions,
    and column meanings.
    """
    
    @classmethod
    async def answer_metadata_question(
        cls, 
        user_query: str, 
        schema_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Answer a question about the schema using the provided metadata
        
        Args:
            user_query: User's natural language question
            schema_metadata: Database schema information
            
        Returns:
            Dict containing the answer and related metadata
        """
        try:
            from app.orchestrator.llm_config import get_llm
            
            llm = get_llm()
            if not llm:
                return {
                    "answer": "I cannot answer schema questions right now because the LLM is unavailable.",
                    "status": "error"
                }

            # Prepare schema summary for context
            tables_info = []
            if schema_metadata and schema_metadata.get("tables"):
                for table_name, table_def in schema_metadata["tables"].items():
                    desc = table_def.get("description", "")
                    col_count = len(table_def.get("columns", []))
                    tables_info.append(f"- {table_name}: {desc} ({col_count} columns)")
            
            schema_summary = "\n".join(tables_info[:20])  # Limit to avoid context overflow
            
            # Construct prompt
            prompt = f"""You are a database schema expert. Answer the user's question about the database structure based ONLY on the provided schema metadata.
            
Schema Summary:
{schema_summary}

User Question: "{user_query}"

If the question asks about specific columns or details not in the summary, explain what tables are available and suggest asking about a specific table to get more details.
Keep the answer concise and helpful for a business user. Do not write SQL code.
"""
            
            # Generate answer
            response = await llm.ainvoke(prompt)
            answer_text = response.content.strip() if hasattr(response, 'content') else str(response).strip()
            
            return {
                "answer": answer_text,
                "status": "success",
                "metadata": {
                    "source": "MetadataQAService",
                    "schema_tables": list(schema_metadata.get("tables", {}).keys())
                }
            }
            
        except Exception as e:
            logger.error(f"Metadata QA failed: {e}", exc_info=True)
            return {
                "answer": "I encountered an error while trying to answer your question about the data structure.",
                "status": "error",
                "error": str(e)
            }
