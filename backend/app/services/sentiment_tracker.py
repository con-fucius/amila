"""
User Sentiment & Frustration Tracker

Tracks user sentiment during query interactions to detect frustration.
Adapts system behavior when frustration is detected:
- Switches to simpler explanations
- Offers to connect with human support
- Reduces approval thresholds
- Escalates response priority

Detection methods:
1. Keyword-based sentiment scoring
2. Query reformulation patterns (repeated similar queries)
3. Explicit negative feedback
4. Abandonment indicators
"""

import re
import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class SentimentLevel(Enum):
    """Sentiment classification levels"""
    VERY_POSITIVE = 5
    POSITIVE = 4
    NEUTRAL = 3
    NEGATIVE = 2
    FRUSTRATED = 1
    VERY_FRUSTRATED = 0


class FrustrationIndicator(Enum):
    """Types of frustration indicators"""
    NEGATIVE_KEYWORDS = "negative_keywords"
    QUERY_REFORMULATION = "query_reformulation"
    REPEATED_FAILURES = "repeated_failures"
    EXPLICIT_COMPLAINT = "explicit_complaint"
    LONG_WAIT_TIME = "long_wait_time"
    ABANDONMENT = "abandonment"


# Frustration keywords (lowercase)
FRUSTRATION_KEYWORDS = {
    "very_frustrated": [
        "useless", "garbage", "broken", "hate", "terrible", "awful",
        "worst", "junk", "trash", "incompetent", "stupid", "dumb",
        "ridiculous", "pathetic", "crap", "bullshit", "damn"
    ],
    "frustrated": [
        "frustrating", "annoying", "confusing", "disappointing",
        "not working", "doesn't work", "not helpful", "waste of time",
        "useless", "pointless", "difficult", "complicated", "struggling"
    ],
    "concerned": [
        "confused", "unsure", "unclear", "hard to", "can't understand",
        "not clear", "wrong", "incorrect", "error", "problem",
        "issue", "fail", "failed"
    ],
    "positive": [
        "good", "great", "excellent", "helpful", "perfect", "awesome",
        "amazing", "love", "fantastic", "brilliant", "thanks", "thank you"
    ],
}

# Query patterns that indicate reformulation/frustration
REFORMULATION_PATTERNS = [
    r"(?:no|not|wrong|different|other|another|try|instead)",
    r"(?:give me|show me|i want|i need|actual)"
]


@dataclass
class SentimentScore:
    """Sentiment analysis result"""
    score: float  # 0.0 to 1.0
    level: SentimentLevel
    indicators: List[FrustrationIndicator]
    keywords_found: List[str]
    confidence: float


@dataclass
class UserSentimentProfile:
    """User's sentiment history and current state"""
    user_id: str
    current_sentiment: SentimentLevel
    sentiment_history: List[Dict[str, Any]] = field(default_factory=list)
    frustration_count: int = 0
    consecutive_failures: int = 0
    last_interaction: str = ""
    escalation_recommended: bool = False


@dataclass
class FrustrationResult:
    """Result of frustration detection"""
    is_frustrated: bool
    frustration_level: float  # 0.0 to 1.0
    indicators: List[Dict[str, Any]]
    recommended_action: str
    should_escalate: bool


class SentimentTracker:
    """
    Service for tracking user sentiment and detecting frustration.
    
    Features:
    - Real-time sentiment analysis of user messages
    - Query pattern tracking for reformulation detection
    - Consecutive failure tracking
    - Adaptive response recommendations
    - Alert escalation for very frustrated users
    """
    
    # Redis key prefixes
    SENTIMENT_PREFIX = "sentiment:user:"
    HISTORY_PREFIX = "sentiment:history:"
    INTERACTION_PREFIX = "sentiment:interaction:"
    
    # Thresholds
    FRUSTRATION_THRESHOLD = 0.4
    VERY_FRUSTRATED_THRESHOLD = 0.2
    ESCALATION_THRESHOLD = 3  # Number of frustration indicators
    MAX_HISTORY = 50
    
    # TTL settings
    SENTIMENT_TTL = 86400 * 7  # 7 days
    HISTORY_TTL = 86400 * 30  # 30 days
    INTERACTION_TTL = 86400 * 7  # 7 days
    FEEDBACK_TTL = 86400 * 30  # 30 days
    
    @classmethod
    def analyze_text(cls, text: str) -> SentimentScore:
        """
        Analyze sentiment of user text input.
        
        Args:
            text: User's text (query, feedback, etc.)
            
        Returns:
            SentimentScore with analysis results
        """
        if not text or not isinstance(text, str):
            return SentimentScore(
                score=0.5,
                level=SentimentLevel.NEUTRAL,
                indicators=[],
                keywords_found=[],
                confidence=0.0
            )
        
        text_lower = text.lower()
        keywords_found = []
        indicators = []
        
        # Check for frustration keywords
        for level, keywords in FRUSTRATION_KEYWORDS.items():
            found = [kw for kw in keywords if kw in text_lower]
            if found:
                keywords_found.extend(found)
                if level == "very_frustrated":
                    indicators.append(FrustrationIndicator.EXPLICIT_COMPLAINT)
                elif level in ["frustrated", "concerned"]:
                    indicators.append(FrustrationIndicator.NEGATIVE_KEYWORDS)
        
        # Calculate base score
        # Start neutral (0.5)
        score = 0.5
        
        # Adjust based on keywords found
        very_frustrated_count = len([kw for kw in keywords_found if kw in FRUSTRATION_KEYWORDS["very_frustrated"]])
        frustrated_count = len([kw for kw in keywords_found if kw in FRUSTRATION_KEYWORDS["frustrated"]])
        concerned_count = len([kw for kw in keywords_found if kw in FRUSTRATION_KEYWORDS["concerned"]])
        positive_count = len([kw for kw in keywords_found if kw in FRUSTRATION_KEYWORDS["positive"]])
        
        # Penalize for frustration, reward for positive
        score -= very_frustrated_count * 0.3
        score -= frustrated_count * 0.15
        score -= concerned_count * 0.05
        score += positive_count * 0.1
        
        # Clamp to 0-1 range
        score = max(0.0, min(1.0, score))
        
        # Determine sentiment level
        if score >= 0.9:
            level = SentimentLevel.VERY_POSITIVE
        elif score >= 0.7:
            level = SentimentLevel.POSITIVE
        elif score >= 0.5:
            level = SentimentLevel.NEUTRAL
        elif score >= 0.3:
            level = SentimentLevel.NEGATIVE
        elif score >= 0.1:
            level = SentimentLevel.FRUSTRATED
        else:
            level = SentimentLevel.VERY_FRUSTRATED
        
        # Calculate confidence based on text length and keyword density
        confidence = min(1.0, len(keywords_found) / max(1, len(text.split())) + 0.3)
        
        return SentimentScore(
            score=score,
            level=level,
            indicators=list(set(indicators)),
            keywords_found=keywords_found,
            confidence=confidence
        )
    
    @classmethod
    async def record_interaction(
        cls,
        user_id: str,
        query_text: str,
        success: bool = True,
        response_time_ms: int = 0,
        query_id: str = ""
    ) -> FrustrationResult:
        """
        Record a user interaction and detect frustration.
        
        Returns:
            FrustrationResult with detection outcome and recommendations
        """
        # Analyze sentiment
        sentiment = cls.analyze_text(query_text)
        
        # Get current profile
        profile = await cls._get_user_profile(user_id)
        
        # Track consecutive failures
        if not success:
            profile.consecutive_failures += 1
        else:
            profile.consecutive_failures = 0
        
        # Check for reformulation (similar recent query)
        is_reformulation = await cls._detect_reformulation(user_id, query_text)
        indicators = []
        
        # Add reformulation indicator
        if is_reformulation:
            indicators.append({
                "type": FrustrationIndicator.QUERY_REFORMULATION.value,
                "confidence": 0.7,
                "details": "Query reformulation detected"
            })
        
        # Add sentiment indicators
        if sentiment.level == SentimentLevel.VERY_FRUSTRATED or sentiment.level == SentimentLevel.FRUSTRATED:
            indicators.append({
                "type": FrustrationIndicator.NEGATIVE_KEYWORDS.value,
                "confidence": sentiment.confidence,
                "details": f"Keywords: {', '.join(sentiment.keywords_found[:3])}"
            })
        
        # Add failure indicator
        if profile.consecutive_failures >= 2:
            indicators.append({
                "type": FrustrationIndicator.REPEATED_FAILURES.value,
                "confidence": min(1.0, profile.consecutive_failures * 0.3),
                "details": f"{profile.consecutive_failures} consecutive failures"
            })
        
        # Add wait time indicator
        if response_time_ms > 30000:  # 30 seconds
            indicators.append({
                "type": FrustrationIndicator.LONG_WAIT_TIME.value,
                "confidence": 0.5,
                "details": f"Response time: {response_time_ms}ms"
            })
        
        # Calculate frustration level
        frustration_level = cls._calculate_frustration_level(sentiment, indicators, profile)
        is_frustrated = frustration_level >= cls.FRUSTRATION_THRESHOLD
        
        # Update profile
        profile.current_sentiment = sentiment.level
        profile.last_interaction = datetime.now(timezone.utc).isoformat()
        if is_frustrated:
            profile.frustration_count += 1
        
        # Determine if escalation is needed
        should_escalate = (
            frustration_level < cls.VERY_FRUSTRATED_THRESHOLD or
            len(indicators) >= cls.ESCALATION_THRESHOLD or
            profile.frustration_count >= 3
        )
        profile.escalation_recommended = should_escalate
        
        # Determine recommended action
        recommended_action = cls._get_recommended_action(frustration_level, indicators, profile)
        
        # Save history
        await cls._save_interaction(user_id, {
            "timestamp": profile.last_interaction,
            "query": query_text[:200],
            "sentiment_score": sentiment.score,
            "sentiment_level": sentiment.level.value,
            "success": success,
            "frustration_level": frustration_level,
            "indicators": indicators,
            "query_id": query_id,
            "phase": "single"
        })

        # Store interaction by query_id for later outcome updates
        if query_id:
            await cls._save_interaction_record(query_id, {
                "user_id": user_id,
                "query": query_text[:500],
                "sentiment_score": sentiment.score,
                "sentiment_level": sentiment.level.value,
                "keywords_found": sentiment.keywords_found[:10],
                "indicators": indicators,
                "started_at": profile.last_interaction,
                "success": success,
                "response_time_ms": response_time_ms,
            })
        
        # Save updated profile
        await cls._save_user_profile(user_id, profile)
        
        if is_frustrated:
            logger.warning(
                f"Frustration detected for user {user_id}: level={frustration_level:.2f}, "
                f"indicators={len(indicators)}"
            )
        
        return FrustrationResult(
            is_frustrated=is_frustrated,
            frustration_level=frustration_level,
            indicators=indicators,
            recommended_action=recommended_action,
            should_escalate=should_escalate
        )

    @classmethod
    async def record_interaction_start(
        cls,
        user_id: str,
        query_text: str,
        query_id: str = ""
    ) -> FrustrationResult:
        """
        Record the start of an interaction without assuming success.
        This is used at routing time to enable adaptive behavior.
        """
        sentiment = cls.analyze_text(query_text)
        profile = await cls._get_user_profile(user_id)

        # Detect reformulation (based on recent history)
        is_reformulation = await cls._detect_reformulation(user_id, query_text)
        indicators = []
        if is_reformulation:
            indicators.append({
                "type": FrustrationIndicator.QUERY_REFORMULATION.value,
                "confidence": 0.7,
                "details": "Query reformulation detected"
            })

        # Add sentiment indicators (no failure or wait-time indicators here)
        if sentiment.level in [SentimentLevel.VERY_FRUSTRATED, SentimentLevel.FRUSTRATED]:
            indicators.append({
                "type": FrustrationIndicator.NEGATIVE_KEYWORDS.value,
                "confidence": sentiment.confidence,
                "details": f"Keywords: {', '.join(sentiment.keywords_found[:3])}"
            })

        frustration_level = cls._calculate_frustration_level(sentiment, indicators, profile)
        is_frustrated = frustration_level >= cls.FRUSTRATION_THRESHOLD

        profile.current_sentiment = sentiment.level
        profile.last_interaction = datetime.now(timezone.utc).isoformat()
        if is_frustrated:
            profile.frustration_count += 1

        should_escalate = (
            frustration_level < cls.VERY_FRUSTRATED_THRESHOLD or
            len(indicators) >= cls.ESCALATION_THRESHOLD or
            profile.frustration_count >= 3
        )
        profile.escalation_recommended = should_escalate

        recommended_action = cls._get_recommended_action(frustration_level, indicators, profile)

        # Save start record
        await cls._save_interaction(user_id, {
            "timestamp": profile.last_interaction,
            "query": query_text[:200],
            "sentiment_score": sentiment.score,
            "sentiment_level": sentiment.level.value,
            "success": None,
            "frustration_level": frustration_level,
            "indicators": indicators,
            "query_id": query_id,
            "phase": "start"
        })

        if query_id:
            await cls._save_interaction_record(query_id, {
                "user_id": user_id,
                "query": query_text[:500],
                "sentiment_score": sentiment.score,
                "sentiment_level": sentiment.level.value,
                "keywords_found": sentiment.keywords_found[:10],
                "indicators": indicators,
                "started_at": profile.last_interaction,
                "success": None,
                "response_time_ms": None,
            })

        await cls._save_user_profile(user_id, profile)

        return FrustrationResult(
            is_frustrated=is_frustrated,
            frustration_level=frustration_level,
            indicators=indicators,
            recommended_action=recommended_action,
            should_escalate=should_escalate
        )

    @classmethod
    async def finalize_interaction(
        cls,
        user_id: str,
        query_id: str,
        success: bool,
        response_time_ms: int = 0,
        error_message: str | None = None
    ) -> Optional[FrustrationResult]:
        """
        Finalize an interaction outcome after execution.
        Updates failure counters and adds wait-time indicators.
        """
        if not query_id:
            return None

        profile = await cls._get_user_profile(user_id)
        record = await cls._get_interaction_record(query_id)

        # Base sentiment from start record if available
        if record:
            sentiment_score = float(record.get("sentiment_score", 0.5))
            try:
                sentiment_level = SentimentLevel(record.get("sentiment_level", 3))
            except Exception:
                sentiment_level = SentimentLevel.NEUTRAL
            sentiment = SentimentScore(
                score=sentiment_score,
                level=sentiment_level,
                indicators=[],
                keywords_found=record.get("keywords_found", []),
                confidence=0.5
            )
            indicators = record.get("indicators", [])
        else:
            sentiment = SentimentScore(
                score=0.5,
                level=SentimentLevel.NEUTRAL,
                indicators=[],
                keywords_found=[],
                confidence=0.0
            )
            indicators = []

        # Update failure counters
        if not success:
            profile.consecutive_failures += 1
        else:
            profile.consecutive_failures = 0

        # Add failure indicator
        if profile.consecutive_failures >= 2:
            indicators.append({
                "type": FrustrationIndicator.REPEATED_FAILURES.value,
                "confidence": min(1.0, profile.consecutive_failures * 0.3),
                "details": f"{profile.consecutive_failures} consecutive failures"
            })

        # Add wait time indicator
        if response_time_ms and response_time_ms > 30000:
            indicators.append({
                "type": FrustrationIndicator.LONG_WAIT_TIME.value,
                "confidence": 0.5,
                "details": f"Response time: {response_time_ms}ms"
            })

        frustration_level = cls._calculate_frustration_level(sentiment, indicators, profile)
        is_frustrated = frustration_level >= cls.FRUSTRATION_THRESHOLD

        profile.current_sentiment = sentiment.level
        profile.last_interaction = datetime.now(timezone.utc).isoformat()
        if is_frustrated:
            profile.frustration_count += 1

        should_escalate = (
            frustration_level < cls.VERY_FRUSTRATED_THRESHOLD or
            len(indicators) >= cls.ESCALATION_THRESHOLD or
            profile.frustration_count >= 3
        )
        profile.escalation_recommended = should_escalate

        recommended_action = cls._get_recommended_action(frustration_level, indicators, profile)

        await cls._save_interaction(user_id, {
            "timestamp": profile.last_interaction,
            "query": record.get("query", "")[:200] if record else "",
            "sentiment_score": sentiment.score,
            "sentiment_level": sentiment.level.value,
            "success": success,
            "frustration_level": frustration_level,
            "indicators": indicators,
            "query_id": query_id,
            "phase": "final",
            "response_time_ms": response_time_ms,
            "error_message": (error_message or "")[:200]
        })

        await cls._save_user_profile(user_id, profile)
        await cls._save_interaction_record(query_id, {
            **(record or {}),
            "user_id": user_id,
            "success": success,
            "response_time_ms": response_time_ms,
            "finalized_at": profile.last_interaction,
            "error_message": (error_message or "")[:200],
        })

        return FrustrationResult(
            is_frustrated=is_frustrated,
            frustration_level=frustration_level,
            indicators=indicators,
            recommended_action=recommended_action,
            should_escalate=should_escalate
        )

    @classmethod
    async def record_feedback(
        cls,
        user_id: str,
        rating: int,
        comment: Optional[str] = None,
        query_id: str | None = None
    ) -> Dict[str, Any]:
        """
        Record explicit user feedback and update sentiment profile.
        rating: +1 (positive) or -1 (negative)
        """
        now = datetime.now(timezone.utc).isoformat()
        profile = await cls._get_user_profile(user_id)

        # Basic sentiment impact from rating and optional comment
        indicators = []
        if rating < 0:
            indicators.append({
                "type": FrustrationIndicator.EXPLICIT_COMPLAINT.value,
                "confidence": 0.9,
                "details": "Explicit negative feedback"
            })
            profile.frustration_count += 1
            profile.current_sentiment = SentimentLevel.NEGATIVE
        elif rating > 0:
            profile.current_sentiment = SentimentLevel.POSITIVE

        if comment:
            sentiment = cls.analyze_text(comment)
            if sentiment.level in [SentimentLevel.FRUSTRATED, SentimentLevel.VERY_FRUSTRATED]:
                indicators.append({
                    "type": FrustrationIndicator.NEGATIVE_KEYWORDS.value,
                    "confidence": sentiment.confidence,
                    "details": f"Keywords: {', '.join(sentiment.keywords_found[:3])}"
                })
                profile.frustration_count += 1

        profile.last_interaction = now
        await cls._save_user_profile(user_id, profile)

        feedback_entry = {
            "timestamp": now,
            "user_id": user_id,
            "rating": rating,
            "comment": (comment or "")[:500],
            "query_id": query_id
        }

        # Store feedback history (per user)
        key = f"sentiment:feedback:{user_id}"
        try:
            await redis_client._client.lpush(key, json.dumps(feedback_entry))
            await redis_client._client.ltrim(key, 0, cls.MAX_HISTORY - 1)
            await redis_client._client.expire(key, cls.FEEDBACK_TTL)
        except Exception as e:
            logger.error(f"Failed to store feedback: {e}")

        return {
            "status": "success",
            "recorded_at": now,
            "indicators": indicators
        }
    
    @classmethod
    def _calculate_frustration_level(
        cls,
        sentiment: SentimentScore,
        indicators: List[Dict],
        profile: UserSentimentProfile
    ) -> float:
        """Calculate overall frustration level"""
        # Base from sentiment score (inverted, 0 = frustrated)
        base_level = 1.0 - sentiment.score
        
        # Add indicator weight
        indicator_weight = len(indicators) * 0.1
        
        # Add historical frustration weight
        history_weight = min(0.3, profile.frustration_count * 0.05)
        
        # Combine
        frustration_level = base_level + indicator_weight + history_weight
        
        return min(1.0, frustration_level)
    
    @classmethod
    def _get_recommended_action(
        cls,
        frustration_level: float,
        indicators: List[Dict],
        profile: UserSentimentProfile
    ) -> str:
        """Get recommended action based on frustration state"""
        if frustration_level < cls.VERY_FRUSTRATED_THRESHOLD:
            return "immediate_escalation"  # Connect to human support
        
        if frustration_level < cls.FRUSTRATION_THRESHOLD:
            return "simplify_response"  # Use simpler explanations, offer help
        
        if FrustrationIndicator.REPEATED_FAILURES.value in [i["type"] for i in indicators]:
            return "offer_alternatives"  # Offer different query approaches
        
        if FrustrationIndicator.QUERY_REFORMULATION.value in [i["type"] for i in indicators]:
            return "clarification_dialog"  # Ask clarifying questions
        
        if profile.frustration_count >= 2:
            return "proactive_help"  # Offer help before asked
        
        return "standard_response"
    
    @classmethod
    async def _detect_reformulation(cls, user_id: str, query_text: str) -> bool:
        """Detect if query is a reformulation of recent query"""
        try:
            # Get recent queries
            recent = await cls._get_recent_queries(user_id, limit=3)
            if not recent:
                return False
            
            query_lower = query_text.lower()
            
            # Check for explicit reformulation keywords
            reformulation_keywords = ["no", "not", "instead", "actually", "rather", "other"]
            if any(kw in query_lower.split()[:5] for kw in reformulation_keywords):
                return True
            
            # Calculate similarity with recent queries
            for recent_query in recent:
                similarity = cls._calculate_similarity(query_lower, recent_query.lower())
                if 0.3 < similarity < 0.8:  # Partial match indicates reformulation
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Failed to detect reformulation: {e}")
            return False
    
    @classmethod
    def _calculate_similarity(cls, text1: str, text2: str) -> float:
        """Calculate simple word overlap similarity"""
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union) if union else 0.0
    
    @classmethod
    async def _get_user_profile(cls, user_id: str) -> UserSentimentProfile:
        """Get user's sentiment profile from Redis"""
        key = f"{cls.SENTIMENT_PREFIX}{user_id}"
        
        try:
            data = await redis_client.get(key)
            if data:
                return UserSentimentProfile(
                    user_id=data.get("user_id", user_id),
                    current_sentiment=SentimentLevel(data.get("current_sentiment", 3)),
                    sentiment_history=data.get("sentiment_history", []),
                    frustration_count=data.get("frustration_count", 0),
                    consecutive_failures=data.get("consecutive_failures", 0),
                    last_interaction=data.get("last_interaction", ""),
                    escalation_recommended=data.get("escalation_recommended", False)
                )
        except Exception as e:
            logger.error(f"Failed to get user profile: {e}")
        
        return UserSentimentProfile(user_id=user_id, current_sentiment=SentimentLevel.NEUTRAL)
    
    @classmethod
    async def _save_user_profile(cls, user_id: str, profile: UserSentimentProfile):
        """Save user's sentiment profile to Redis"""
        key = f"{cls.SENTIMENT_PREFIX}{user_id}"
        
        data = {
            "user_id": profile.user_id,
            "current_sentiment": profile.current_sentiment.value,
            "sentiment_history": profile.sentiment_history[-10:],  # Keep last 10
            "frustration_count": profile.frustration_count,
            "consecutive_failures": profile.consecutive_failures,
            "last_interaction": profile.last_interaction,
            "escalation_recommended": profile.escalation_recommended
        }
        
        try:
            await redis_client.set(key, data, ttl=cls.SENTIMENT_TTL)
        except Exception as e:
            logger.error(f"Failed to save user profile: {e}")
    
    @classmethod
    async def _save_interaction(cls, user_id: str, interaction: Dict):
        """Save interaction to history"""
        key = f"{cls.HISTORY_PREFIX}{user_id}"
        
        try:
            await redis_client._client.lpush(key, json.dumps(interaction))
            await redis_client._client.ltrim(key, 0, cls.MAX_HISTORY - 1)
            await redis_client._client.expire(key, cls.HISTORY_TTL)
        except Exception as e:
            logger.error(f"Failed to save interaction: {e}")

    @classmethod
    async def _save_interaction_record(cls, query_id: str, record: Dict[str, Any]):
        """Persist interaction record by query_id for outcome updates."""
        if not query_id:
            return
        key = f"{cls.INTERACTION_PREFIX}{query_id}"
        try:
            await redis_client.set(key, record, ttl=cls.INTERACTION_TTL)
        except Exception as e:
            logger.error(f"Failed to save interaction record: {e}")

    @classmethod
    async def _get_interaction_record(cls, query_id: str) -> Optional[Dict[str, Any]]:
        """Get interaction record for query_id."""
        if not query_id:
            return None
        key = f"{cls.INTERACTION_PREFIX}{query_id}"
        try:
            return await redis_client.get(key)
        except Exception as e:
            logger.error(f"Failed to read interaction record: {e}")
            return None
    
    @classmethod
    async def _get_recent_queries(cls, user_id: str, limit: int = 5) -> List[str]:
        """Get recent query texts"""
        key = f"{cls.HISTORY_PREFIX}{user_id}"
        
        try:
            items = await redis_client._client.lrange(key, 0, limit - 1)
            queries = []
            for item in items:
                data = json.loads(item)
                if "query" in data:
                    queries.append(data["query"])
            return queries
        except Exception as e:
            logger.error(f"Failed to get recent queries: {e}")
            return []
    
    @classmethod
    async def get_user_sentiment_summary(cls, user_id: str) -> Dict[str, Any]:
        """Get sentiment summary for a user"""
        profile = await cls._get_user_profile(user_id)
        history = await cls._get_recent_queries(user_id, 10)
        
        return {
            "user_id": user_id,
            "current_sentiment": profile.current_sentiment.name,
            "frustration_count": profile.frustration_count,
            "consecutive_failures": profile.consecutive_failures,
            "escalation_recommended": profile.escalation_recommended,
            "recent_interactions": len(history),
            "last_interaction": profile.last_interaction
        }
    
    @classmethod
    async def reset_frustration_counter(cls, user_id: str):
        """Reset frustration counter (e.g., after successful resolution)"""
        profile = await cls._get_user_profile(user_id)
        profile.frustration_count = 0
        profile.escalation_recommended = False
        profile.consecutive_failures = 0
        await cls._save_user_profile(user_id, profile)
        logger.info(f"Reset frustration counter for user {user_id}")


# Global instance
sentiment_tracker = SentimentTracker()


# Convenience functions

async def analyze_sentiment(text: str) -> SentimentScore:
    """Analyze text sentiment"""
    return SentimentTracker.analyze_text(text)


async def record_user_interaction(
    user_id: str,
    query_text: str,
    success: bool = True,
    response_time_ms: int = 0,
    query_id: str = ""
) -> FrustrationResult:
    """Record user interaction and check for frustration"""
    return await SentimentTracker.record_interaction(
        user_id, query_text, success, response_time_ms, query_id
    )


async def check_user_frustration(user_id: str) -> Dict[str, Any]:
    """Check if user is frustrated"""
    return await SentimentTracker.get_user_sentiment_summary(user_id)
