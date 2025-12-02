"""
LLM Configuration for Query Orchestrator
"""

import logging
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from app.core.config import settings

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


def get_query_llm_model(default: str = 'unknown') -> str:
    """Resolve model name for query orchestration."""
    # Environment override (supports OpenAI-compatible providers like Qwen)
    env_model = os.getenv('OPENAI_MODEL')
    if env_model:
        return env_model

    model = getattr(settings, 'QUERY_LLM_MODEL', None)
    if model:
        return model
    return getattr(settings, 'GRAPHITI_LLM_MODEL', default)


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

        model_name = get_query_llm_model(settings.GRAPHITI_LLM_MODEL)
        logger.debug("Initializing Google Gemini LLM for query orchestration")
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

    raise ValueError(
        f"Unsupported LLM provider: {llm_provider}. "
        f"Supported providers: gemini, bedrock, qwen"
    )
