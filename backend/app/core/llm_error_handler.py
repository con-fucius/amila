"""
LLM Error Handler with Quota and Rate Limit Management
Provides comprehensive error handling for LLM providers with retry logic and fallback
"""

import logging
import time
from typing import Optional, Dict, Any, Callable, TypeVar, Awaitable
from functools import wraps
import asyncio

logger = logging.getLogger(__name__)

T = TypeVar('T')


class LLMQuotaError(Exception):
    """Raised when LLM provider quota is exhausted"""
    def __init__(self, provider: str, message: str, retry_after: Optional[int] = None):
        self.provider = provider
        self.retry_after = retry_after
        super().__init__(f"[{provider}] Quota exhausted: {message}")


class LLMRateLimitError(Exception):
    """Raised when LLM provider rate limit is hit"""
    def __init__(self, provider: str, message: str, retry_after: Optional[int] = None):
        self.provider = provider
        self.retry_after = retry_after
        super().__init__(f"[{provider}] Rate limit exceeded: {message}")


class LLMProviderError(Exception):
    """Generic LLM provider error"""
    def __init__(self, provider: str, message: str, original_error: Optional[Exception] = None):
        self.provider = provider
        self.original_error = original_error
        super().__init__(f"[{provider}] Provider error: {message}")


def classify_llm_error(error: Exception, provider: str) -> Exception:
    """
    Classify LLM errors into specific error types for better handling
    
    Args:
        error: Original exception from LLM provider
        provider: Provider name (gemini, bedrock, qwen, openrouter)
    
    Returns:
        Classified exception (LLMQuotaError, LLMRateLimitError, or LLMProviderError)
    """
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # Extract retry-after if available
    retry_after = None
    if hasattr(error, 'response') and hasattr(error.response, 'headers'):
        retry_after_header = error.response.headers.get('Retry-After')
        if retry_after_header:
            try:
                retry_after = int(retry_after_header)
            except ValueError:
                pass
    
    # Gemini-specific errors
    if provider == 'gemini':
        if 'quota' in error_str or 'resource_exhausted' in error_str or '429' in error_str:
            if 'rate limit' in error_str or 'requests per minute' in error_str:
                return LLMRateLimitError(provider, str(error), retry_after or 60)
            return LLMQuotaError(provider, str(error), retry_after)
        if 'rate limit' in error_str or '429' in error_str:
            return LLMRateLimitError(provider, str(error), retry_after or 60)
    
    # Bedrock-specific errors
    elif provider == 'bedrock':
        if 'throttlingexception' in error_str or 'throttled' in error_str:
            return LLMRateLimitError(provider, str(error), retry_after or 60)
        if 'servicequotaexceeded' in error_str or 'quota' in error_str:
            return LLMQuotaError(provider, str(error), retry_after)
    
    # OpenRouter-specific errors
    elif provider == 'openrouter':
        if '429' in error_str or 'rate limit' in error_str:
            return LLMRateLimitError(provider, str(error), retry_after or 60)
        if 'quota' in error_str or 'insufficient credits' in error_str:
            return LLMQuotaError(provider, str(error), retry_after)
    
    # Qwen-specific errors
    elif provider == 'qwen':
        if '429' in error_str or 'rate limit' in error_str or 'throttle' in error_str:
            return LLMRateLimitError(provider, str(error), retry_after or 60)
        if 'quota' in error_str or 'insufficient' in error_str:
            return LLMQuotaError(provider, str(error), retry_after)
    
    # Mistral-specific errors
    elif provider == 'mistral':
        if '429' in error_str or 'rate limit' in error_str:
            return LLMRateLimitError(provider, str(error), retry_after or 60)
        if 'quota' in error_str or 'insufficient' in error_str:
            return LLMQuotaError(provider, str(error), retry_after)
    
    # Generic HTTP 429 detection
    if '429' in error_str or 'too many requests' in error_str:
        return LLMRateLimitError(provider, str(error), retry_after or 60)
    
    # Generic quota detection
    if 'quota' in error_str or 'resource_exhausted' in error_str:
        return LLMQuotaError(provider, str(error), retry_after)
    
    # Default to generic provider error
    return LLMProviderError(provider, str(error), error)


async def retry_with_exponential_backoff(
    func: Callable[..., Awaitable[T]],
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    provider: str = "unknown",
    *args,
    **kwargs
) -> T:
    """
    Retry async function with exponential backoff for rate limits
    
    Args:
        func: Async function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff calculation
        provider: LLM provider name for logging
        *args, **kwargs: Arguments to pass to func
    
    Returns:
        Result from successful function call
    
    Raises:
        Last exception if all retries fail
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            classified_error = classify_llm_error(e, provider)
            last_exception = classified_error
            
            # Don't retry quota errors - they won't resolve with retries
            if isinstance(classified_error, LLMQuotaError):
                logger.error(f"[{provider}] Quota exhausted - no retry: {classified_error}")
                raise classified_error
            
            # Retry rate limit errors with backoff
            if isinstance(classified_error, LLMRateLimitError):
                if attempt < max_retries:
                    # Use retry_after from error if available, otherwise exponential backoff
                    if classified_error.retry_after:
                        delay = min(classified_error.retry_after, max_delay)
                    else:
                        delay = min(initial_delay * (exponential_base ** attempt), max_delay)
                    
                    logger.warning(
                        f"[{provider}] Rate limit hit (attempt {attempt + 1}/{max_retries + 1}). "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"[{provider}] Rate limit - max retries exceeded")
                    raise classified_error
            
            # For other errors, retry with backoff
            if attempt < max_retries:
                delay = min(initial_delay * (exponential_base ** attempt), max_delay)
                logger.warning(
                    f"[{provider}] Error (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
                continue
            else:
                logger.error(f"[{provider}] Max retries exceeded")
                raise classified_error
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected retry loop exit")


def get_fallback_providers(current_provider: str) -> list[str]:
    """
    Get ordered list of fallback providers
    
    Args:
        current_provider: Current provider that failed
    
    Returns:
        List of alternative providers to try
    """
    # Define provider priority order
    all_providers = ['mistral', 'gemini', 'openrouter', 'qwen', 'bedrock']
    
    # Remove current provider and return remaining in priority order
    fallback = [p for p in all_providers if p != current_provider]
    
    logger.info(f"Fallback providers for {current_provider}: {fallback}")
    return fallback


async def call_llm_with_fallback(
    llm_func: Callable[[str], Awaitable[T]],
    current_provider: str,
    enable_fallback: bool = True,
    max_retries_per_provider: int = 2,
    *args,
    **kwargs
) -> tuple[T, str]:
    """
    Call LLM with automatic fallback to alternative providers on quota/rate limit errors
    
    Args:
        llm_func: Function that takes provider name and returns LLM response
        current_provider: Current LLM provider
        enable_fallback: Whether to try fallback providers
        max_retries_per_provider: Max retries per provider for rate limits
        *args, **kwargs: Arguments to pass to llm_func
    
    Returns:
        Tuple of (result, provider_used)
    
    Raises:
        Exception if all providers fail
    """
    providers_to_try = [current_provider]
    
    if enable_fallback:
        providers_to_try.extend(get_fallback_providers(current_provider))
    
    last_exception = None
    
    for provider in providers_to_try:
        try:
            logger.info(f"Attempting LLM call with provider: {provider}")
            
            # Try with retry logic for rate limits
            result = await retry_with_exponential_backoff(
                llm_func,
                max_retries_per_provider,
                1.0,  # initial_delay
                60.0,  # max_delay
                2.0,  # exponential_base
                provider,
                provider,  # Pass provider as first arg to llm_func
                *args,
                **kwargs
            )
            
            if provider != current_provider:
                logger.info(f"Successfully failed over to {provider} from {current_provider}")
            
            return result, provider
            
        except LLMQuotaError as e:
            logger.warning(f"[{provider}] Quota exhausted, trying next provider...")
            last_exception = e
            continue
            
        except LLMRateLimitError as e:
            logger.warning(f"[{provider}] Rate limit exceeded after retries, trying next provider...")
            last_exception = e
            continue
            
        except Exception as e:
            logger.error(f"[{provider}] Unexpected error: {e}")
            last_exception = e
            # For unexpected errors, still try next provider
            continue
    
    # All providers failed
    error_msg = f"All LLM providers failed. Last error: {last_exception}"
    logger.error(error_msg)
    
    if last_exception:
        raise last_exception
    raise RuntimeError(error_msg)


def format_user_friendly_error(error: Exception, provider: str) -> str:
    """
    Format LLM error into user-friendly message
    
    Args:
        error: Exception from LLM provider
        provider: Provider name
    
    Returns:
        User-friendly error message
    """
    if isinstance(error, LLMQuotaError):
        return (
            f"The {provider} AI service has reached its usage quota. "
            f"Please try again later or contact support to increase quota limits."
        )
    
    if isinstance(error, LLMRateLimitError):
        retry_msg = ""
        if error.retry_after:
            retry_msg = f" Please try again in {error.retry_after} seconds."
        return (
            f"The {provider} AI service is currently experiencing high demand. "
            f"{retry_msg}"
        )
    
    if isinstance(error, LLMProviderError):
        return (
            f"The {provider} AI service encountered an error. "
            f"Please try again or contact support if the issue persists."
        )
    
    return (
        f"An unexpected error occurred with the AI service. "
        f"Please try again or contact support if the issue persists."
    )
