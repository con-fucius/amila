"""
Conversation Router Service
Routes user inputs between conversational responses and SQL query processing

Enhanced with LLM-based intent classification for better accuracy
and spelling resilience. Falls back to pattern matching when LLM unavailable.
"""

import logging
import re
from typing import Dict, Any, Optional, Tuple, List
from enum import Enum
from datetime import datetime, timezone
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


class IntentType(str, Enum):
    """Types of user intents"""
    GREETING = "greeting"
    FAREWELL = "farewell"
    THANKS = "thanks"
    HELP = "help"
    META_QUESTION = "meta_question"  # Questions about the system
    CLARIFICATION = "clarification"  # Follow-up clarification
    DATA_QUERY = "data_query"  # Actual SQL query needed
    REFINEMENT = "refinement"  # Refine previous query
    UNKNOWN = "unknown"


class ConversationRouter:
    """
    Routes user inputs to appropriate handlers
    
    Classifies inputs as:
    - Conversational (greetings, thanks, help requests)
    - Meta questions (about the system, capabilities)
    - Data queries (need SQL generation)
    - Refinements (modify previous query)
    """
    
    # Greeting patterns
    GREETING_PATTERNS = [
        r"^(hi|hello|hey|good\s*(morning|afternoon|evening)|howdy|greetings)\b",
        r"^(what'?s\s*up|sup|yo)\b",
    ]
    
    # Farewell patterns
    FAREWELL_PATTERNS = [
        r"^(bye|goodbye|see\s*you|later|take\s*care|ciao)\b",
        r"^(good\s*night|have\s*a\s*good\s*(day|one))\b",
    ]
    
    # Thanks patterns
    THANKS_PATTERNS = [
        r"^(thanks?|thank\s*you|thx|ty|appreciate)\b",
        r"(thanks?\s*(a\s*lot|so\s*much|very\s*much))",
    ]
    
    # Help patterns
    HELP_PATTERNS = [
        r"^(help|how\s*do\s*i|what\s*can\s*you\s*do|capabilities)\b",
        r"(show\s*me\s*how|guide\s*me|tutorial)",
        r"^(what\s*are\s*you|who\s*are\s*you)\b",
    ]
    
    # Meta question patterns (about the system)
    META_PATTERNS = [
        r"(how\s*does\s*(this|it)\s*work)",
        r"(what\s*databases?\s*(do\s*you|can\s*you))",
        r"(can\s*you\s*(export|save|download))",
        r"(how\s*to\s*(use|export|filter|sort))",
        r"(what\s*format|which\s*format)",
    ]
    
    # Refinement patterns (modify previous query)
    REFINEMENT_PATTERNS = [
        r"^(now|also|and|but)\s+(show|filter|sort|group|add|remove|exclude|include)\b",
        r"^(filter|sort|group|limit)\s+(by|to|it)\b",
        r"^(only|just)\s+(show|include|the)\b",
        r"^(remove|exclude|without)\s+",
        r"^(add|include)\s+(the|a)?\s*\w+\s*(column|field)?",
        r"(same\s*(query|thing)|previous\s*(query|result))",
        r"^(more|less|fewer)\s+(rows|results|data)",
    ]
    
    # Data query indicators
    DATA_QUERY_INDICATORS = [
        r"\b(show|get|find|list|display|retrieve|fetch|query|select)\b",
        r"\b(how\s*many|count|total|sum|average|avg|max|min)\b",
        r"\b(top|bottom|first|last|recent|latest)\s*\d*\b",
        r"\b(sales|revenue|customers?|orders?|products?|transactions?)\b",
        r"\b(by|per|for|in|from|where|when|which)\b.*\b(month|year|quarter|week|day|region|category)\b",
        r"\b(compare|trend|growth|change|difference)\b",
        r"\b(between|greater|less|more|fewer|above|below)\b",
    ]
    
    # Conversational responses
    RESPONSES = {
        IntentType.GREETING: [
            "Hello! I'm ready to help you explore your data. What would you like to know?",
            "Hi there! Ask me anything about your data - I can help with sales, customers, trends, and more.",
            "Hey! Ready to dive into some data analysis. What are you curious about?",
        ],
        IntentType.FAREWELL: [
            "Goodbye! Feel free to come back anytime you need data insights.",
            "See you later! Your query history will be here when you return.",
            "Take care! Happy to help with your data analysis anytime.",
        ],
        IntentType.THANKS: [
            "You're welcome! Let me know if you need anything else.",
            "Happy to help! Feel free to ask more questions.",
            "Anytime! Is there anything else you'd like to explore?",
        ],
        IntentType.HELP: [
            """I can help you analyze your data using natural language! Here's what I can do:

- **Ask questions** like "Show me top 10 customers by revenue"
- **Filter data** with "Sales in Q3 2024" or "Orders above $1000"
- **Aggregate** using "Total revenue by region" or "Average order value"
- **Compare** with "Compare sales this year vs last year"
- **Trend analysis** like "Monthly revenue trend for 2024"

Just type your question naturally and I'll generate the SQL and show you the results!""",
        ],
        IntentType.META_QUESTION: [
            "I can query both Oracle and Doris databases. Use the database selector in the sidebar to switch between them. I'll generate the appropriate SQL for whichever database you're connected to.",
        ],
    }

    @classmethod
    def classify_intent(cls, user_input: str, conversation_history: Optional[list] = None) -> Tuple[IntentType, float]:
        """
        Classify user input intent
        
        Args:
            user_input: User's message
            conversation_history: Previous messages for context
            
        Returns:
            Tuple of (IntentType, confidence score 0-1)
        """
        if not user_input or not user_input.strip():
            return IntentType.UNKNOWN, 0.0
        
        text = user_input.strip().lower()
        
        # Check greeting patterns
        for pattern in cls.GREETING_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.GREETING, 0.95
        
        # Check farewell patterns
        for pattern in cls.FAREWELL_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.FAREWELL, 0.95
        
        # Check thanks patterns
        for pattern in cls.THANKS_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.THANKS, 0.95
        
        # Check help patterns
        for pattern in cls.HELP_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.HELP, 0.9
        
        # Check meta question patterns
        for pattern in cls.META_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.META_QUESTION, 0.85
        
        # Check refinement patterns (needs conversation history)
        if conversation_history and len(conversation_history) > 0:
            for pattern in cls.REFINEMENT_PATTERNS:
                if re.search(pattern, text, re.IGNORECASE):
                    return IntentType.REFINEMENT, 0.85
        
        # Check data query indicators
        data_score = 0
        for pattern in cls.DATA_QUERY_INDICATORS:
            if re.search(pattern, text, re.IGNORECASE):
                data_score += 1
        
        if data_score >= 2:
            return IntentType.DATA_QUERY, min(0.5 + data_score * 0.1, 0.95)
        elif data_score == 1:
            return IntentType.DATA_QUERY, 0.6
        
        # Default: if message is long enough, assume it's a data query
        if len(text.split()) >= 4:
            return IntentType.DATA_QUERY, 0.5
        
        return IntentType.UNKNOWN, 0.3
    
    @classmethod
    def get_response(cls, intent: IntentType) -> Optional[str]:
        """
        Get a conversational response for non-data intents
        
        Args:
            intent: Classified intent type
            
        Returns:
            Response string or None if should proceed to SQL generation
        """
        import random
        
        responses = cls.RESPONSES.get(intent)
        if responses:
            return random.choice(responses)
        return None
    
    @classmethod
    def route(cls, user_input: str, conversation_history: Optional[list] = None) -> Dict[str, Any]:
        """
        Route user input to appropriate handler
        
        Args:
            user_input: User's message
            conversation_history: Previous messages for context
            
        Returns:
            Dict with routing decision and optional response
        """
        intent, confidence = cls.classify_intent(user_input, conversation_history)
        
        result = {
            "intent": intent.value,
            "confidence": confidence,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requires_sql": False,
            "response": None,
        }
        
        # Conversational intents - return direct response
        if intent in [IntentType.GREETING, IntentType.FAREWELL, IntentType.THANKS, IntentType.HELP]:
            result["response"] = cls.get_response(intent)
            result["requires_sql"] = False
            
        # Meta questions - return info response
        elif intent == IntentType.META_QUESTION:
            result["response"] = cls.get_response(intent)
            result["requires_sql"] = False
            
        # Data queries and refinements - need SQL generation
        elif intent in [IntentType.DATA_QUERY, IntentType.REFINEMENT]:
            result["requires_sql"] = True
            result["is_refinement"] = intent == IntentType.REFINEMENT
            
        # Unknown - default to SQL generation if confident enough
        else:
            result["requires_sql"] = confidence >= 0.4
        
        logger.info(f"Routed input to {intent.value} (confidence: {confidence:.2f}, requires_sql: {result['requires_sql']})")
        
        return result
    
    @classmethod
    def enhance_refinement_query(
        cls,
        current_query: str,
        previous_query: str,
        previous_sql: Optional[str] = None
    ) -> str:
        """
        Enhance a refinement query with context from previous query
        
        Args:
            current_query: Current user input (refinement)
            previous_query: Previous user query
            previous_sql: Previous generated SQL (optional)
            
        Returns:
            Enhanced query string with context
        """
        # Build context-aware query
        enhanced = f"Based on the previous query '{previous_query}', {current_query}"
        
        if previous_sql:
            enhanced += f"\n\nPrevious SQL for reference:\n{previous_sql}"
        
        return enhanced
    
    @classmethod
    def fuzzy_match_table(cls, input_text: str, known_tables: List[str], threshold: float = 0.6) -> Optional[str]:
        """
        Fuzzy match user input against known table names
        Handles typos like "employe" -> "EMPLOYEES"
        
        Args:
            input_text: User's potentially misspelled table reference
            known_tables: List of actual table names in the schema
            threshold: Minimum similarity ratio (0-1) to consider a match
            
        Returns:
            Best matching table name or None if no good match
        """
        if not input_text or not known_tables:
            return None
        
        input_lower = input_text.lower().strip()
        best_match = None
        best_ratio = 0.0
        
        for table in known_tables:
            table_lower = table.lower()
            # Check exact match first
            if input_lower == table_lower:
                return table
            
            # Check if input is a substring
            if input_lower in table_lower or table_lower in input_lower:
                ratio = len(input_lower) / len(table_lower) if len(table_lower) > 0 else 0
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = table
                continue
            
            # Use sequence matcher for fuzzy matching
            ratio = SequenceMatcher(None, input_lower, table_lower).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = table
        
        return best_match
    
    @classmethod
    def suggest_table_correction(cls, user_query: str, known_tables: List[str]) -> Optional[Dict[str, Any]]:
        """
        Suggest table name corrections for misspellings
        
        Args:
            user_query: User's natural language query
            known_tables: List of known table names
            
        Returns:
            Dict with suggestion info or None
        """
        # Extract potential table references from query
        words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', user_query)
        
        for word in words:
            if len(word) < 4:  # Skip short words
                continue
            
            # Check if word looks like a table reference but isn't exact
            word_upper = word.upper()
            if word_upper in [t.upper() for t in known_tables]:
                continue  # Exact match, no suggestion needed
            
            # Try fuzzy match
            match = cls.fuzzy_match_table(word, known_tables, threshold=0.7)
            if match and match.upper() != word_upper:
                return {
                    "original": word,
                    "suggested": match,
                    "message": f"Did you mean '{match}' instead of '{word}'?"
                }
        
        return None
    
    @classmethod
    async def classify_with_llm(
        cls,
        user_input: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        schema_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[IntentType, float, Optional[str]]:
        """
        LLM-based intent classification for better accuracy
        
        Args:
            user_input: User's message
            conversation_history: Previous messages for context
            schema_context: Available schema info for grounding
            
        Returns:
            Tuple of (IntentType, confidence, optional_response)
        """
        try:
            from app.orchestrator.llm_config import get_llm
            
            llm = get_llm()
            if not llm:
                logger.warning("LLM not available for intent classification, falling back to patterns")
                intent, confidence = cls.classify_intent(user_input, conversation_history)
                return intent, confidence, None
            
            # Build context string
            history_str = ""
            if conversation_history:
                recent = conversation_history[-3:]  # Last 3 messages
                history_str = "\n".join([
                    f"- {msg.get('role', 'user')}: {msg.get('content', '')[:100]}"
                    for msg in recent
                ])
            
            schema_str = ""
            if schema_context and schema_context.get("tables"):
                tables = list(schema_context["tables"].keys())[:10]
                schema_str = f"Available tables: {', '.join(tables)}"
            
            prompt = f"""Classify the following user input into one of these categories:
A) GREETING - Hello, hi, hey, good morning, etc.
B) FAREWELL - Goodbye, bye, see you, etc.
C) THANKS - Thank you, thanks, appreciate it, etc.
D) HELP - Questions about how to use the system
E) META_QUESTION - Questions about system capabilities
F) DATA_QUERY - Request for data that needs SQL
G) REFINEMENT - Modification of a previous query (e.g., "now filter by...", "also show...")
H) UNKNOWN - Cannot determine

User input: "{user_input}"

{f"Recent conversation:{chr(10)}{history_str}" if history_str else ""}
{schema_str}

Respond with ONLY the letter (A-H) and confidence (0-100), like: "F 85"
"""
            
            response = await llm.ainvoke(prompt)
            response_text = response.content.strip() if hasattr(response, 'content') else str(response).strip()
            
            # Parse response
            parts = response_text.split()
            if len(parts) >= 2:
                letter = parts[0].upper()
                try:
                    confidence = int(parts[1]) / 100.0
                except ValueError:
                    confidence = 0.7
                
                intent_map = {
                    'A': IntentType.GREETING,
                    'B': IntentType.FAREWELL,
                    'C': IntentType.THANKS,
                    'D': IntentType.HELP,
                    'E': IntentType.META_QUESTION,
                    'F': IntentType.DATA_QUERY,
                    'G': IntentType.REFINEMENT,
                    'H': IntentType.UNKNOWN,
                }
                
                intent = intent_map.get(letter, IntentType.UNKNOWN)
                logger.info(f"LLM classified intent as {intent.value} with confidence {confidence:.2f}")
                return intent, confidence, None
            
            # Fallback to pattern matching
            intent, confidence = cls.classify_intent(user_input, conversation_history)
            return intent, confidence, None
            
        except Exception as e:
            logger.warning(f"LLM intent classification failed: {e}, falling back to patterns")
            intent, confidence = cls.classify_intent(user_input, conversation_history)
            return intent, confidence, None
    
    @classmethod
    async def route_with_context(
        cls,
        user_input: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        schema_context: Optional[Dict[str, Any]] = None,
        use_llm: bool = True
    ) -> Dict[str, Any]:
        """
        Enhanced routing with LLM classification and context awareness
        
        Args:
            user_input: User's message
            conversation_history: Previous messages for context
            schema_context: Available schema info
            use_llm: Whether to use LLM for classification
            
        Returns:
            Dict with routing decision, response, and suggestions
        """
        result = {
            "intent": IntentType.UNKNOWN.value,
            "confidence": 0.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requires_sql": False,
            "response": None,
            "suggestion": None,
        }
        
        # Try LLM classification first if enabled
        if use_llm:
            intent, confidence, llm_response = await cls.classify_with_llm(
                user_input, conversation_history, schema_context
            )
        else:
            intent, confidence = cls.classify_intent(user_input, conversation_history)
            llm_response = None
        
        result["intent"] = intent.value
        result["confidence"] = confidence
        
        # Check for table name suggestions
        if schema_context and schema_context.get("tables"):
            known_tables = list(schema_context["tables"].keys())
            suggestion = cls.suggest_table_correction(user_input, known_tables)
            if suggestion:
                result["suggestion"] = suggestion
        
        # Handle different intents
        if intent in [IntentType.GREETING, IntentType.FAREWELL, IntentType.THANKS, IntentType.HELP]:
            result["response"] = llm_response or cls.get_response(intent)
            result["requires_sql"] = False
        elif intent == IntentType.META_QUESTION:
            result["response"] = llm_response or cls.get_response(intent)
            result["requires_sql"] = False
        elif intent in [IntentType.DATA_QUERY, IntentType.REFINEMENT]:
            result["requires_sql"] = True
            result["is_refinement"] = intent == IntentType.REFINEMENT
        else:
            # Unknown - default to SQL if confident enough
            result["requires_sql"] = confidence >= 0.4
        
        logger.info(f"Routed input to {intent.value} (confidence: {confidence:.2f}, requires_sql: {result['requires_sql']})")
        
        return result
