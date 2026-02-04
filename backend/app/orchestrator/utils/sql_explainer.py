import logging
from typing import Optional
from app.orchestrator.llm_config import get_llm

logger = logging.getLogger(__name__)

async def explain_sql_query(sql: str, db_type: str = "oracle") -> Optional[str]:
    """
    Generates a natural language explanation of what the SQL query does.
    
    Args:
        sql: The SQL query to explain
        db_type: database dialect
        
    Returns:
        A concise English explanation or None if generation fails
    """
    if not sql:
        return None
        
    try:
        llm = get_llm()
        
        prompt = f"""
        Explain the following {db_type} SQL query to a business user in plain, concise English.
        Focus on WHAT data is being retrieved and HOW it is filtered or aggregated.
        Keep it under 3 sentences. Do not use technical jargon like 'inner join' or 'where clause' if possible.
        
        SQL:
        {sql}
        
        Explanation:
        """
        
        response = await llm.ainvoke(prompt)
        explanation = response.content.strip() if hasattr(response, 'content') else str(response).strip()
        
        return explanation
    except Exception as e:
        logger.error(f"Failed to explain SQL query: {e}")
        return None
