"""
LLM Configuration for Query Orchestrator
"""

import logging
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Awaitable

from app.core.config import settings
from app.core.llm_error_handler import (
    call_llm_with_fallback,
    retry_with_exponential_backoff,
    classify_llm_error,
    format_user_friendly_error,
    LLMQuotaError,
    LLMRateLimitError,
    LLMProviderError,
)

if TYPE_CHECKING:
    from langchain_aws import ChatBedrock
    from langchain_google_genai import ChatGoogleGenerativeAI

logger = logging.getLogger(__name__)


def get_query_llm_provider() -> str:
    """Resolve configured provider for query orchestration."""
    # Highest precedence: explicit environment override
    env_provider = os.getenv('QUERY_LLM_PROVIDER')
    if env_provider:
        return env_provider.lower()

    # Settings-provided provider
    provider = getattr(settings, 'QUERY_LLM_PROVIDER', None)
    if provider:
        return provider.lower()

    # Heuristic: if Qwen credentials/base url are present, prefer Qwen for testing
    try:
        if (
            getattr(settings, 'QWEN_CODE_CREDENTIALS_PATH', None)
            or os.getenv('QWEN_CODE_CREDENTIALS_PATH')
            or (os.getenv('OPENAI_BASE_URL') or '').lower().find('dashscope') != -1
            or (os.getenv('OPENAI_MODEL') or '').lower().startswith('qwen')
        ):
            return 'qwen'
    except Exception:
        # Fall through to default if any attribute missing
        pass

    # Fallback to Graphiti LLM provider (defaults to gemini)
    return getattr(settings, 'GRAPHITI_LLM_PROVIDER', 'gemini').lower()


def get_query_llm_model(default: str = 'unknown', for_provider: Optional[str] = None) -> str:
    """Resolve model name for query orchestration."""
    # Environment override (supports OpenAI-compatible providers like Qwen)
    env_model = os.getenv('OPENAI_MODEL')
    if env_model:
        return env_model

    # Check for QUERY_LLM_MODEL specifically
    model = getattr(settings, 'QUERY_LLM_MODEL', None) or os.getenv('QUERY_LLM_MODEL')

    # If we are looking for a specific provider, check if the override is valid for it
    if for_provider:
        current_provider = get_query_llm_provider()
        if for_provider != current_provider:
            # We are likely in a fallback or specific provider instantiation
            # Do NOT use the universal QUERY_LLM_MODEL if it doesn't match the for_provider's characteristic
            if model:
                # Heuristic: if model contains 'flash' it's likely gemini, if 'devstral' it's mistral
                if for_provider == 'mistral' and 'devstral' in model:
                    return model
                if for_provider == 'gemini' and 'flash' in model:
                    return model
            return default
            
    if model:
        return model
        
    # Check if the current provider matches the Graphiti provider
    # If so, it's safe to fallback to GRAPHITI_LLM_MODEL
    llm_provider = get_query_llm_provider()
    graphiti_provider = getattr(settings, 'GRAPHITI_LLM_PROVIDER', 'gemini').lower()
    
    if llm_provider == graphiti_provider:
        graphiti_model = getattr(settings, 'GRAPHITI_LLM_MODEL', None)
        if graphiti_model:
            return graphiti_model
            
    return default


def load_qwen_credentials() -> tuple[str, str]:
    """Load Qwen access token and base URL from configured credentials path."""
    credentials_path = (
        getattr(settings, 'QWEN_CODE_CREDENTIALS_PATH', None)
        or os.getenv('QWEN_CODE_CREDENTIALS_PATH')
    )
    if not credentials_path:
        raise ValueError(
            "Qwen provider selected but QWEN_CODE_CREDENTIALS_PATH is not configured. "
            "Set this path to the Qwen CLI credentials JSON (e.g., C:/Users/<user>/.qwen/credentials.json)."
        )

    path = Path(credentials_path).expanduser()
    if not path.exists():
        raise ValueError(f"Qwen credentials file not found at {path}")

    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse Qwen credentials JSON at {path}: {exc}") from exc

    access_token = data.get('access_token')
    if not access_token:
        raise ValueError(
            f"Qwen credentials at {path} do not contain an 'access_token'. "
            "Ensure you have authenticated with `qwen-code auth`."
        )
    
    # Construct base_url from resource_url if present
    resource_url = data.get('resource_url')  # e.g., "portal.qwen.ai"
    
    base_url = (
        getattr(settings, 'QWEN_API_BASE_URL', None)
        or data.get('api_base')
        or data.get('base_url')
        or os.getenv('OPENAI_BASE_URL')
        or (f"https://{resource_url}/api/v1" if resource_url else None)
        or 'https://dashscope.aliyuncs.com/compatible-mode/v1'
    )
    
    logger.info(f"Qwen config: base_url={base_url}, token_type={data.get('token_type', 'Bearer')}")
    
    return access_token, base_url


def get_llm():
    """Initialize LLM client based on configured provider."""
    llm_provider = get_query_llm_provider()

    if llm_provider == 'gemini':
        from langchain_google_genai import ChatGoogleGenerativeAI

        model_name = get_query_llm_model("gemini-2.0-flash", for_provider='gemini')
        logger.debug(f"Initializing Google Gemini LLM ({model_name}) for query orchestration")
        return ChatGoogleGenerativeAI(
            model=model_name,
            google_api_key=settings.GOOGLE_API_KEY,
            temperature=0.0,
            max_output_tokens=4096,
            top_p=0.95,
        )

    if llm_provider == 'bedrock':
        from langchain_aws import ChatBedrock

        model_name = get_query_llm_model("anthropic.claude-3-5-sonnet-20241022-v2:0")
        logger.debug("Initializing AWS Bedrock LLM for query orchestration")
        return ChatBedrock(
            model_id=model_name,
            region_name=settings.AWS_REGION,
            model_kwargs={
                "temperature": 0.0,
                "max_tokens": 4096,
                "top_p": 0.95,
            },
        )

    if llm_provider == 'mistral':
        from langchain_mistralai import ChatMistralAI
        
        # Use specific model from settings or default
        model_name = get_query_llm_model(settings.MISTRAL_MODEL)
        api_key = os.getenv('MISTRAL_API_KEY') or settings.MISTRAL_API_KEY
        
        if not api_key:
            logger.warning("Mistral API key not found, falling back to OpenRouter for devstral")
            llm_provider = 'openrouter' # Run-time fallback
        else:
            logger.info(f"Initializing Mistral LLM: model={model_name}")
            return ChatMistralAI(
                model=model_name,
                mistral_api_key=api_key,
                temperature=0.0,
                max_tokens=4096,
            )

    if llm_provider == 'qwen':
        access_token, base_url = load_qwen_credentials()
        model_name = get_query_llm_model('qwen3-coder-plus')

        params = {
            "model": model_name,
            "temperature": 0.0,
            "max_tokens": 4096,
        }

        try:
            from langchain_openai import ChatOpenAI as _ChatOpenAI
            params.update({
                "api_key": access_token,
                "base_url": base_url,
            })
            logger.debug("Initializing Qwen (OpenAI-compatible) LLM via langchain_openai")
            return _ChatOpenAI(**params)
        except ImportError:
            from langchain_community.chat_models import ChatOpenAI as _ChatOpenAI
            params.update({
                "openai_api_key": access_token,
                "openai_api_base": base_url,
            })
            logger.debug("Initializing Qwen (OpenAI-compatible) LLM via langchain_community")
            return _ChatOpenAI(**params)

    if llm_provider == 'openrouter':
        # OpenRouter API - OpenAI-compatible endpoint
        api_key = os.getenv('OPENROUTER_API_KEY')
        if not api_key:
            raise ValueError(
                "OpenRouter provider selected but OPENROUTER_API_KEY is not set. "
                "Get your API key from https://openrouter.ai/keys"
            )
        
        model_name = get_query_llm_model('mistralai/devstral-small-latest:free', for_provider='openrouter')
        base_url = os.getenv('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1')
        
        logger.info(f"Initializing OpenRouter LLM: model={model_name}")
        
        try:
            from langchain_openai import ChatOpenAI as _ChatOpenAI
            # Avoid http_client wrapper issue by not passing custom headers via default_headers
            # Use model_kwargs for extra params instead
            return _ChatOpenAI(
                model=model_name,
                api_key=api_key,
                base_url=base_url,
                temperature=0.0,
                max_tokens=4096,
                http_client=None,  # Explicitly disable custom http_client
                http_async_client=None,
            )
        except (ImportError, TypeError) as e:
            logger.warning(f"langchain_openai ChatOpenAI failed: {e}, trying alternative")
            try:
                from langchain_community.chat_models import ChatOpenAI as _ChatOpenAI
                return _ChatOpenAI(
                    model=model_name,
                    openai_api_key=api_key,
                    openai_api_base=base_url,
                    temperature=0.0,
                    max_tokens=4096,
                )
            except Exception as e2:
                logger.error(f"All OpenRouter init methods failed: {e2}")
                raise ValueError(f"Failed to initialize OpenRouter LLM: {e2}")

    if llm_provider == 'mistral':
        from langchain_mistralai import ChatMistralAI
        
        # Use specific model from settings or default
        model_name = get_query_llm_model(settings.MISTRAL_MODEL)
        api_key = os.getenv('MISTRAL_API_KEY') or settings.MISTRAL_API_KEY
        
        if not api_key:
            logger.warning("Mistral API key not found, falling back to OpenRouter for devstral")
            llm_provider = 'openrouter' # Continue to openrouter block
        else:
            logger.info(f"Initializing Mistral LLM: model={model_name}")
            return ChatMistralAI(
                model=model_name,
                mistral_api_key=api_key,
                temperature=0.0,
                max_tokens=4096,
            )

    raise ValueError(
        f"Unsupported LLM provider: {llm_provider}. "
        f"Supported providers: gemini, bedrock, qwen, openrouter, mistral"
    )



async def invoke_llm_with_retry(
    llm: Any,
    messages: list,
    provider: str,
    enable_fallback: bool = True,
    max_retries: int = 2,
) -> Any:
    """
    Invoke LLM with automatic retry and fallback handling
    
    Args:
        llm: LangChain LLM instance
        messages: List of messages to send
        provider: Current provider name
        enable_fallback: Whether to enable fallback to other providers
        max_retries: Max retries per provider
    
    Returns:
        LLM response
    
    Raises:
        LLMQuotaError, LLMRateLimitError, or LLMProviderError
    """
    async def _invoke(provider_name: str) -> Any:
        """Inner function to invoke LLM for specific provider"""
        # Get fresh LLM instance for the provider
        if provider_name != provider:
            logger.info(f"Switching from {provider} to {provider_name}")
            current_llm = get_llm_for_provider(provider_name)
        else:
            current_llm = llm
        
        try:
            # Invoke LLM
            response = await current_llm.ainvoke(messages)
            return response
        except Exception as e:
            # Classify and re-raise
            classified = classify_llm_error(e, provider_name)
            raise classified
    
    # Use fallback mechanism
    result, used_provider = await call_llm_with_fallback(
        _invoke,
        provider,
        enable_fallback=enable_fallback,
        max_retries_per_provider=max_retries,
    )
    
    if used_provider != provider:
        logger.info(f"LLM call succeeded with fallback provider: {used_provider}")
    
    return result


def get_llm_for_provider(provider: str) -> Any:
    """
    Get LLM instance for specific provider
    
    Args:
        provider: Provider name (gemini, bedrock, qwen, openrouter)
    
    Returns:
        LangChain LLM instance
    """
    if provider == 'gemini':
        from langchain_google_genai import ChatGoogleGenerativeAI
        model_name = get_query_llm_model("gemini-2.0-flash", for_provider='gemini')
        return ChatGoogleGenerativeAI(
            model=model_name,
            google_api_key=settings.GOOGLE_API_KEY,
            temperature=0.0,
            max_output_tokens=4096,
            top_p=0.95,
        )
    
    if provider == 'bedrock':
        from langchain_aws import ChatBedrock
        model_name = get_query_llm_model("anthropic.claude-3-5-sonnet-20241022-v2:0")
        return ChatBedrock(
            model_id=model_name,
            region_name=settings.AWS_REGION,
            model_kwargs={
                "temperature": 0.0,
                "max_tokens": 4096,
                "top_p": 0.95,
            },
        )
    
    if provider == 'qwen':
        access_token, base_url = load_qwen_credentials()
        model_name = get_query_llm_model('qwen3-coder-plus')
        
        try:
            from langchain_openai import ChatOpenAI as _ChatOpenAI
            return _ChatOpenAI(
                model=model_name,
                api_key=access_token,
                base_url=base_url,
                temperature=0.0,
                max_tokens=4096,
            )
        except ImportError:
            from langchain_community.chat_models import ChatOpenAI as _ChatOpenAI
            return _ChatOpenAI(
                model=model_name,
                openai_api_key=access_token,
                openai_api_base=base_url,
                temperature=0.0,
                max_tokens=4096,
            )
    
    if provider == 'openrouter':
        api_key = os.getenv('OPENROUTER_API_KEY')
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY not set")
        
        model_name = get_query_llm_model('mistralai/devstral-small-latest:free', for_provider='openrouter')
        base_url = os.getenv('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1')
        
        try:
            from langchain_openai import ChatOpenAI as _ChatOpenAI
            return _ChatOpenAI(
                model=model_name,
                api_key=api_key,
                base_url=base_url,
                temperature=0.0,
                max_tokens=4096,
                http_client=None,
                http_async_client=None,
            )
        except (ImportError, TypeError):
            from langchain_community.chat_models import ChatOpenAI as _ChatOpenAI
            return _ChatOpenAI(
                model=model_name,
                openai_api_key=api_key,
                openai_api_base=base_url,
                temperature=0.0,
                max_tokens=4096,
            )
    
    if provider == 'mistral':
        from langchain_mistralai import ChatMistralAI
        model_name = get_query_llm_model(settings.MISTRAL_MODEL)
        api_key = os.getenv('MISTRAL_API_KEY') or settings.MISTRAL_API_KEY
        return ChatMistralAI(
            model=model_name,
            mistral_api_key=api_key,
            temperature=0.0,
            max_tokens=4096,
        )
    
    raise ValueError(f"Unsupported provider: {provider}")
