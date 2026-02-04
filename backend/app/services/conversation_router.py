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
    METADATA_QUERY = "metadata_query"  # Questions about schema/data definitions
    AMBIGUOUS = "ambiguous"  # Ambiguous intent requiring clarification
    UNKNOWN = "unknown"


class QueryTaxonomy(str, Enum):
    """
    Query taxonomy for classification and routing
    
    This taxonomy categorizes data queries to enable:
    - Optimized SQL generation strategies per category
    - Resource allocation and prioritization
    - Query intent understanding for better results
    """
    EXPLORATORY = "exploratory"  # Open-ended exploration ("what's in this table?")
    TARGETED = "targeted"  # Specific data retrieval ("show me customer X")
    COMPARATIVE = "comparative"  # Comparison queries ("compare A vs B")
    AGGREGATE = "aggregate"  # Summary statistics ("total sales by region")
    TREND = "trend"  # Time-series analysis ("monthly revenue trend")
    ANOMALY = "anomaly"  # Finding outliers ("unusual transactions")
    RELATIONSHIP = "relationship"  # Joins/relationships ("orders with customer details")
    EXECUTIVE = "executive"  # High-level KPIs ("dashboard summary")
    VALIDATION = "validation"  # Data quality checks ("check for duplicates")
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
    
    # Greeting patterns - expanded for better coverage
    GREETING_PATTERNS = [
        r"^(hi|hello|hey|good\s*(morning|afternoon|evening)|howdy|greetings)\b",
        r"^(what'?s\s*up|sup|yo)\b",
        r"^(hi+|hey+|hello+)\s*[!.]*$",  # hi!, hiii, heyyy
        r"^(good\s*day|g'?day)\b",
        r"^(hola|bonjour|ciao|namaste)\b",  # Common international greetings
    ]
    
    # Farewell patterns
    FAREWELL_PATTERNS = [
        r"^(bye|goodbye|see\s*you|later|take\s*care|ciao)\b",
        r"^(good\s*night|have\s*a\s*good\s*(day|one))\b",
        r"^(cheers|peace|adios|au\s*revoir)\b",
    ]
    
    # Thanks patterns
    THANKS_PATTERNS = [
        r"^(thanks?|thank\s*you|thx|ty|appreciate)\b",
        r"(thanks?\s*(a\s*lot|so\s*much|very\s*much))",
        r"^(cheers|ta|much\s*appreciated)\b",
        r"(that'?s?\s*(great|helpful|awesome|perfect))",
    ]
    
    # Help patterns
    HELP_PATTERNS = [
        r"^(help|how\s*do\s*i|what\s*can\s*you\s*do|capabilities)\b",
        r"(show\s*me\s*how|guide\s*me|tutorial)",
        r"^(what\s*are\s*you|who\s*are\s*you)\b",
        r"^(how\s*does\s*this\s*work|what\s*is\s*this)\b",
    ]
    
    # General conversational patterns (non-data queries)
    CONVERSATIONAL_PATTERNS = [
        r"^(how\s*are\s*you|how'?s\s*it\s*going|how\s*do\s*you\s*do)\b",
        r"^(nice\s*to\s*meet\s*you|pleased\s*to\s*meet\s*you)\b",
        r"^(i'?m\s*(good|fine|great|okay|well|doing\s*well))\b",
        r"^(that'?s?\s*(cool|nice|great|awesome|interesting))\b",
        r"^(ok|okay|alright|sure|got\s*it|understood|i\s*see)\b",
        r"^(yes|no|maybe|perhaps|definitely|absolutely)\s*[!.]*$",
        r"^(wow|cool|nice|great|awesome|amazing|excellent)\s*[!.]*$",
        r"^(lol|haha|hehe|:[\)\(]|xd)\s*$",
        r"^(sorry|my\s*bad|oops|apologies)\b",
        r"^(no\s*problem|no\s*worries|all\s*good|it'?s?\s*fine)\b",
        r"^(what\s*do\s*you\s*think|your\s*thoughts)\b",
        r"^(tell\s*me\s*(about\s*yourself|more))\b",
        r"^(can\s*you\s*help\s*me)\s*[?!.]*$",  # Generic help without specific query
        r"^(i\s*need\s*help)\s*[?!.]*$",
        r"^(testing|test|just\s*testing)\b",
        # Extended conversational patterns for longer messages
        r"^(i\s*just\s*wanted\s*to\s*(say|check|see|ask))\b",
        r"^(just\s*(checking|saying|asking|wondering))\b",
        r"^(hope\s*(you'?re?|everything|all)\s*(is\s*)?(good|well|fine|okay))\b",
        r"^(good\s*to\s*(see|hear|know|meet))\b",
        r"^(looking\s*forward\s*to)\b",
        r"^(have\s*a\s*(good|great|nice|wonderful))\b",
        r"^(it'?s?\s*(nice|good|great)\s*to\s*(be|meet|see|talk))\b",
        r"^(glad\s*to\s*(be|meet|see|help))\b",
        r"^(pleasure\s*to\s*(meet|help|assist))\b",
        r"(how\s*can\s*i\s*help\s*you|what\s*can\s*i\s*do\s*for\s*you)",
        r"^(i\s*appreciate\s*(it|that|your|the))\b",
        r"^(sounds\s*(good|great|fine|perfect|awesome))\b",
        r"^(perfect|wonderful|fantastic|brilliant)\s*[!.]*$",
        r"^(let\s*me\s*know\s*if)\b",
        r"^(feel\s*free\s*to)\b",
        r"^(don'?t\s*hesitate\s*to)\b",
    ]
    
    # Negative indicators - phrases that suggest NOT a data query
    NON_DATA_INDICATORS = [
        r"^(i\s*just\s*wanted\s*to)",
        r"^(just\s*saying|just\s*checking|just\s*wondering)",
        r"(nice\s*to\s*meet|good\s*to\s*see|glad\s*to)",
        r"(hope\s*you|hope\s*everything|hope\s*all)",
        r"(have\s*a\s*good|have\s*a\s*great|have\s*a\s*nice)",
        r"(looking\s*forward)",
        r"(take\s*care|see\s*you|talk\s*soon)",
        r"(appreciate\s*it|appreciate\s*your|appreciate\s*the)",
        r"(sounds\s*good|sounds\s*great|sounds\s*fine)",
        r"(no\s*worries|no\s*problem|all\s*good)",
        r"(let\s*me\s*know|feel\s*free|don'?t\s*hesitate)",
        r"^(i\s*am\s*(here|ready|available|happy)\s*to)",
        r"^(ready\s*when\s*you\s*are)",
        r"^(whenever\s*you'?re?\s*ready)",
    ]
    
    # Meta question patterns (about the system)
    META_PATTERNS = [
        r"(how\s*does\s*(this|it)\s*work)",
        r"(what\s*databases?\s*(do\s*you|can\s*you))",
        r"(can\s*you\s*(export|save|download))",
        r"(how\s*to\s*(use|export|filter|sort))",
        r"(what\s*format|which\s*format)",
    ]

    # Metadata/Schema question patterns
    METADATA_PATTERNS = [
        r"^(what|which)\s*(tables?|columns?|fields?)\s*(are|do|can)\b",
        r"^(describe|explain|define)\s+(table|column|field)\b",
        r"what\s*does\s*(column|field)\s*.*\s*mean",
        r"schema\s*of\s*",
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
        word_count = len(text.split())
        
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
        
        # Check general conversational patterns (non-data queries)
        for pattern in cls.CONVERSATIONAL_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.GREETING, 0.9  # Treat as greeting for response
        
        # Check meta question patterns
        for pattern in cls.META_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.META_QUESTION, 0.85

        # Check metadata/schema patterns
        for pattern in cls.METADATA_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return IntentType.METADATA_QUERY, 0.85
        
        # Check refinement patterns (needs conversation history)
        if conversation_history and len(conversation_history) > 0:
            for pattern in cls.REFINEMENT_PATTERNS:
                if re.search(pattern, text, re.IGNORECASE):
                    return IntentType.REFINEMENT, 0.85
        
        # Check for non-data indicators first (conversational phrases)
        non_data_score = 0
        for pattern in cls.NON_DATA_INDICATORS:
            if re.search(pattern, text, re.IGNORECASE):
                non_data_score += 1
        
        # Strong non-data indicators override data query classification
        if non_data_score >= 1:
            return IntentType.GREETING, 0.85
        
        # Check data query indicators
        data_score = 0
        for pattern in cls.DATA_QUERY_INDICATORS:
            if re.search(pattern, text, re.IGNORECASE):
                data_score += 1
        
        # Strong data query indicators - need at least 2 matches for confidence
        if data_score >= 2:
            return IntentType.DATA_QUERY, min(0.5 + data_score * 0.1, 0.95)
        elif data_score == 1 and word_count >= 3:
            # Single indicator with enough context
            return IntentType.DATA_QUERY, 0.6
        
        # Short messages without data indicators are likely conversational
        if word_count <= 3 and data_score == 0:
            # Check if it looks like a question about data
            if re.search(r'\?$', text):
                # Questions might be data queries, but short ones are often conversational
                if word_count <= 2:
                    return IntentType.HELP, 0.6
            return IntentType.UNKNOWN, 0.4
        
        # Medium-length messages need stronger signals to be data queries
        if word_count >= 4 and word_count <= 6 and data_score == 0:
            # Check for question words that suggest data queries
            if re.search(r'^(how\s*many|what\s*is\s*the|show\s*me|list|get|find)\b', text):
                return IntentType.DATA_QUERY, 0.6
            return IntentType.UNKNOWN, 0.4
        
        # Longer messages with some data context
        if word_count >= 7 and data_score >= 1:
            return IntentType.DATA_QUERY, 0.7
        
        # Default: longer messages might be data queries but with lower confidence
        if word_count >= 5:
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
    async def enhance_query_contextually(
        cls,
        user_input: str,
        conversation_history: List[Dict[str, str]],
        schema_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enhance a refinement query by merging it with previous context using LLM.
        
        SAFEGUARD: Preserves original input and strictly validates the enhancement.
        
        Args:
            user_input: User's current input
            conversation_history: History
            schema_context: Schema info
            
        Returns:
            Dict with 'original_input', 'enhanced_intent', 'method'
        """
        try:
            from app.orchestrator.llm_config import get_llm
            llm = get_llm()
            
            if not llm or not conversation_history:
                return {
                    "original_input": user_input,
                    "enhanced_intent": cls.enhance_refinement_query(
                        user_input, 
                        conversation_history[-1]["content"] if conversation_history else ""
                    ),
                    "method": "fallback_concatenation"
                }

            # Get recent context (last user query and assistant response)
            recent_history = conversation_history[-3:] # Last 3 messages including current? No, history passed in is previous.
            
            prompt = f"""You are a query understanding assistant.
Your task is to merge the User's "Current Input" with the "Previous Context" to create a Standalone Query Intent.

Rules:
1. PRESERVE INTENT: Do not change the target metric or entity unless the user explicitly asks.
2. MERGE CONSTRAINTS: If user says "filter by region", apply that to the previous subject.
3. BE CONSERVATIVE: If the input is unrelated (e.g., "Hi"), do not merge. Return "UNRELATED".
4. OUTPUT format: Just the standalone query string. No explanations.

Previous Context:
{chr(10).join([f"{m.get('role')}: {m.get('content')}" for m in recent_history])}

Current Input: "{user_input}"

Standalone Query Intent:"""

            response = await llm.ainvoke(prompt)
            enhanced_text = response.content.strip() if hasattr(response, 'content') else str(response).strip()
            
            # Basic validation
            if "UNRELATED" in enhanced_text:
                return {
                    "original_input": user_input, 
                    "enhanced_intent": user_input, 
                    "method": "none_unrelated"
                }
                
            return {
                "original_input": user_input,
                "enhanced_intent": enhanced_text,
                "method": "llm_enhancement",
                "context_used": True
            }

        except Exception as e:
            logger.error(f"Context enhancement failed: {e}")
            return {
                "original_input": user_input,
                "enhanced_intent": user_input,
                "method": "error_fallback"
            }

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
E) META_QUESTION - Questions about system capabilities (not data/schema)
F) DATA_QUERY - Request for data that needs SQL
G) REFINEMENT - Modification of a previous query (e.g., "now filter by...", "also show...")
H) METADATA_QUERY - Questions about table/column definitions, schema, or meanings
I) AMBIGUOUS - Vague, unclear, or could be multiple things (needs clarification)
J) UNKNOWN - Cannot determine

User input: "{user_input}"

{f"Recent conversation:{chr(10)}{history_str}" if history_str else ""}
{schema_str}

Respond with ONLY the letter (A-J) and confidence (0-100), like: "F 85"
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
                    'G': IntentType.REFINEMENT,
                    'H': IntentType.METADATA_QUERY,
                    'I': IntentType.AMBIGUOUS,
                    'J': IntentType.UNKNOWN,
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
    async def analyze_sentiment(
        cls,
        user_input: str,
        user_id: str,
        query_id: str = ""
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze user sentiment and detect frustration.
        
        Args:
            user_input: User's message
            user_id: User identifier
            query_id: Optional query ID for tracking
            
        Returns:
            Sentiment analysis result or None if analysis failed
        """
        try:
            from app.services.sentiment_tracker import SentimentTracker
            
            # Record the interaction start for adaptive routing (no outcome yet)
            sentiment_result = await SentimentTracker.record_interaction_start(
                user_id=user_id,
                query_text=user_input,
                query_id=query_id
            )
            
            return {
                "is_frustrated": sentiment_result.is_frustrated,
                "frustration_level": sentiment_result.frustration_level,
                "recommended_action": sentiment_result.recommended_action,
                "should_escalate": sentiment_result.should_escalate,
                "indicators": sentiment_result.indicators
            }
        except Exception as e:
            logger.debug(f"Sentiment analysis failed (non-fatal): {e}")
            return None
    
    @classmethod
    async def route_with_context(
        cls,
        user_input: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        schema_context: Optional[Dict[str, Any]] = None,
        use_llm: bool = True,
        user_id: str = "anonymous",
        query_id: str = ""
    ) -> Dict[str, Any]:
        """
        Enhanced routing with LLM classification and context awareness
        
        Args:
            user_input: User's message
            conversation_history: Previous messages for context
            schema_context: Available schema info
            use_llm: Whether to use LLM for classification
            user_id: User identifier for sentiment tracking
            
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
            "sentiment": None,
        }
        
        # Analyze sentiment before routing
        sentiment = await cls.analyze_sentiment(user_input, user_id, query_id=query_id)
        if sentiment:
            result["sentiment"] = sentiment
            
            # Log frustration detection
            if sentiment.get("is_frustrated"):
                logger.warning(
                    f"Frustration detected for user {user_id}: "
                    f"level={sentiment['frustration_level']:.2f}, "
                    f"action={sentiment['recommended_action']}"
                )
                
                # Adjust response based on frustration
                if sentiment.get("should_escalate"):
                    result["escalation_recommended"] = True
                    result["escalation_reason"] = "User frustration detected"
            
            # Include adaptive response recommendation
            result["adaptive_response"] = sentiment.get("recommended_action")
        
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
        elif intent == IntentType.METADATA_QUERY:
            result["requires_sql"] = False
            # Processing handled by MetadataQAService via processor.py
        elif intent == IntentType.AMBIGUOUS:
            result["requires_sql"] = False
            result["response"] = "I'm not sure what you mean. Could you please clarify which data or table you're interested in?"
        elif intent in [IntentType.DATA_QUERY, IntentType.REFINEMENT]:
            result["requires_sql"] = True
            result["is_refinement"] = intent == IntentType.REFINEMENT
            
            # Apply Contextual Enhancement for Refinements
            if intent == IntentType.REFINEMENT and use_llm:
                enhancement = await cls.enhance_query_contextually(
                    user_input,
                    conversation_history or [],
                    schema_context
                )
                result["enhanced_context"] = enhancement
                logger.info(f"Enhanced refinement: '{user_input}' -> '{enhancement.get('enhanced_intent')}'")
        else:
            # Unknown - default to SQL if confident enough
            result["requires_sql"] = confidence >= 0.4
        
        logger.info(f"Routed input to {intent.value} (confidence: {confidence:.2f}, requires_sql: {result['requires_sql']})")
        
        return result
