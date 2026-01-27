"""
Graphiti Client Wrapper for Amil BI Agent
Provides unified interface for temporal knowledge graph operations using Graphiti + FalkorDB
Supports both Google Gemini (dev) and AWS Bedrock (production) LLM providers
"""

import asyncio
import os
import logging
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from datetime import datetime

try:
    from graphiti_core import Graphiti
    from graphiti_core.driver.falkordb_driver import FalkorDriver
    from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient
    # Gemini clients
    from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
    from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
    # Generic clients for Mistral/OpenRouter
    from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient, LLMConfig as OpenAIConfig
    from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig
    GRAPHITI_AVAILABLE = True
except Exception:
    GRAPHITI_AVAILABLE = False
    # Provide dummy types for type checking
    if TYPE_CHECKING:
        from graphiti_core import Graphiti

from app.core.config_manager import settings


logger = logging.getLogger(__name__)


class GraphitiClientError(Exception):
    """Custom exception for Graphiti client errors"""
    pass


class LocalEmbedder:
    """
    Local embedding provider using sentence-transformers.
    Provides a compatible interface for Graphiti.
    """
    def __init__(self, model_name: str, dimensions: int):
        from sentence_transformers import SentenceTransformer
        logger.info(f"Loading local embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.dimensions = dimensions

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate static embeddings for a list of texts synchronously (wrapped in to_thread)"""
        # sentence-transformers is sync, so we run in thread to avoid blocking event loop
        embeddings = await asyncio.to_thread(self.model.encode, texts)
        return embeddings.tolist()


class GraphitiClient:
    """
    Graphiti Client Wrapper for Temporal Knowledge Graph Operations
    
    Features:
    - Dual-provider support: Google Gemini (development) and AWS Bedrock (production)
    - FalkorDB backend for graph storage
    - Conversation history tracking
    - Schema evolution monitoring
    - Query pattern learning
    - User preference tracking
    """

    def __init__(self):
        """Initialize Graphiti client with configuration from settings"""
        if not GRAPHITI_AVAILABLE:
            raise GraphitiClientError(
                "Graphiti is not installed. Run: uv add 'graphiti-core[falkordb,google-genai]'"
            )

        self._graphiti: Optional[Graphiti] = None
        self._driver: Optional[FalkorDriver] = None
        self._llm_provider = settings.GRAPHITI_LLM_PROVIDER
        self._embedding_provider = settings.GRAPHITI_EMBEDDING_PROVIDER
        
        logger.info(f"Initializing GraphitiClient with LLM provider: {self._llm_provider}")

    async def initialize(self) -> None:
        """
        Initialize Graphiti with FalkorDB driver and configured LLM provider
        Must be called before using any Graphiti operations
        """
        try:
            # Set telemetry environment variable
            os.environ['GRAPHITI_TELEMETRY_ENABLED'] = str(settings.GRAPHITI_TELEMETRY_ENABLED).lower()
            
            # Set concurrency limit
            if hasattr(settings, 'GRAPHITI_SEMAPHORE_LIMIT'):
                os.environ['SEMAPHORE_LIMIT'] = str(settings.GRAPHITI_SEMAPHORE_LIMIT)

            # Initialize FalkorDB driver
            falkordb_config = settings.falkordb_connection_config
            logger.info(f"Connecting to FalkorDB at {falkordb_config['host']}:{falkordb_config['port']}")
            
            self._driver = FalkorDriver(
                host=falkordb_config['host'],
                port=falkordb_config['port'],
                password=falkordb_config.get('password', None),
                database=falkordb_config['database']
            )

            # Initialize Graphiti with appropriate LLM provider
            if self._llm_provider == "gemini":
                self._graphiti = await self._initialize_with_gemini()
            elif self._llm_provider == "mistral":
                self._graphiti = await self._initialize_with_mistral()
            elif self._llm_provider == "openrouter":
                self._graphiti = await self._initialize_with_openrouter()
            elif self._llm_provider == "bedrock":
                self._graphiti = await self._initialize_with_bedrock()
            else:
                raise GraphitiClientError(f"Unsupported LLM provider: {self._llm_provider}")

            # Initialize Graphiti indices and constraints
            await self._graphiti.build_indices_and_constraints()
            
            logger.info("Graphiti client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Graphiti client: {e}")
            raise GraphitiClientError(f"Initialization failed: {e}")

    async def _initialize_with_gemini(self) -> "Graphiti":
        """Initialize Graphiti with Google Gemini LLM provider (Development)"""
        if not settings.GOOGLE_API_KEY:
            raise GraphitiClientError(
                "GOOGLE_API_KEY not set. Required for Gemini provider. "
                "Get API key from: https://ai.google.dev/"
            )

        logger.info("Initializing Graphiti with Google Gemini provider")

        # Configure Gemini LLM client
        llm_config = GeminiLLMConfig(
            api_key=settings.GOOGLE_API_KEY,
            model=settings.GRAPHITI_LLM_MODEL  # Use configured model from settings
        )
        llm_client = GeminiClient(config=llm_config)

        # Configure Gemini embedder
        embedder_config = GeminiEmbedderConfig(
            api_key=settings.GOOGLE_API_KEY,
            embedding_model=settings.GRAPHITI_EMBEDDING_MODEL,  # "text-embedding-004"
            embedding_dim=settings.GRAPHITI_EMBEDDING_DIMENSIONS  # 768
        )
        embedder = GeminiEmbedder(config=embedder_config)

        # Configure Gemini reranker
        reranker_config = GeminiLLMConfig(
            api_key=settings.GOOGLE_API_KEY,
            model=settings.GRAPHITI_LLM_MODEL  # Use same model for reranking
        )
        reranker = GeminiRerankerClient(config=reranker_config)

        return Graphiti(
            graph_driver=self._driver,
            llm_client=llm_client,
            embedder=embedder,
            cross_encoder=reranker
        )

    async def _initialize_with_mistral(self) -> "Graphiti":
        """Initialize Graphiti with Mistral AI API"""
        api_key = os.getenv("MISTRAL_API_KEY") or settings.MISTRAL_API_KEY
        if not api_key:
            raise GraphitiClientError("MISTRAL_API_KEY not set")

        logger.info(f"Initializing Graphiti with Mistral provider (Model: {settings.GRAPHITI_LLM_MODEL})")

        # LLM Client (using Generic OpenAI-compatible client)
        llm_config = OpenAIConfig(
            api_key=api_key,
            model=settings.GRAPHITI_LLM_MODEL,
            base_url="https://api.mistral.ai/v1"
        )
        llm_client = OpenAIGenericClient(config=llm_config)

        # Embedder
        embedder = await self._get_embedder()

        return Graphiti(
            graph_driver=self._driver,
            llm_client=llm_client,
            embedder=embedder
        )

    async def _initialize_with_openrouter(self) -> "Graphiti":
        """Initialize Graphiti with OpenRouter (Fallback)"""
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise GraphitiClientError("OPENROUTER_API_KEY not set")

        logger.info(f"Initializing Graphiti with OpenRouter provider (Model: {settings.GRAPHITI_LLM_MODEL})")

        # LLM Client
        llm_config = OpenAIConfig(
            api_key=api_key,
            model=settings.GRAPHITI_LLM_MODEL,
            base_url="https://openrouter.ai/api/v1"
        )
        llm_client = OpenAIGenericClient(config=llm_config)

        # Embedder
        embedder = await self._get_embedder()

        return Graphiti(
            graph_driver=self._driver,
            llm_client=llm_client,
            embedder=embedder
        )

    async def _get_embedder(self) -> Any:
        """Resolve and initialize the configured embedder"""
        if self._embedding_provider == "local":
            return LocalEmbedder(
                model_name=settings.GRAPHITI_EMBEDDING_MODEL,
                dimensions=settings.GRAPHITI_EMBEDDING_DIMENSIONS
            )
        
        if self._embedding_provider == "gemini":
            from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
            embedder_config = GeminiEmbedderConfig(
                api_key=settings.GOOGLE_API_KEY,
                embedding_model=settings.GRAPHITI_EMBEDDING_MODEL,
                embedding_dim=settings.GRAPHITI_EMBEDDING_DIMENSIONS
            )
            return GeminiEmbedder(config=embedder_config)

        # Fallback to OpenAI if configured (optional)
        raise GraphitiClientError(f"Embedding provider '{self._embedding_provider}' not yet configured for this helper.")

    async def _initialize_with_bedrock(self) -> "Graphiti":
        """Initialize Graphiti with AWS Bedrock LLM provider (Production)"""
        # NOTE:
        # Official Graphiti documentation (as of Nov 2025) focuses on providers
        # with strong structured-output support (e.g. Gemini, OpenAI). There is
        # no stable, documented Bedrock client wiring in graphiti-core yet.
        #
        # To avoid a half-implemented or misleading integration, we fail fast
        # here with a descriptive error instead of attempting an ad-hoc setup.
        raise GraphitiClientError(
            "GRAPHITI_LLM_PROVIDER='bedrock' is not currently supported in this "
            "deployment. Use GRAPHITI_LLM_PROVIDER='gemini' (with GOOGLE_API_KEY) "
            "or extend GraphitiClient._initialize_with_bedrock once official "
            "Bedrock integration guidance is available."
        )

    async def add_episode(
        self,
        content: str,
        episode_type: str = "conversation",
        source: str = "user",
        reference_time: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Add an episode to the knowledge graph
        
        Args:
            content: The content of the episode (text or JSON)
            episode_type: Type of episode (conversation, query, schema_change, etc.)
            source: Source of the episode (user, system, agent)
            reference_time: Timestamp for the episode (defaults to now)
            metadata: Additional metadata for the episode
            
        Returns:
            Dict containing episode ID and processing status
        """
        if not self._graphiti:
            raise GraphitiClientError("Graphiti client not initialized. Call initialize() first.")

        try:
            if reference_time is None:
                reference_time = datetime.now()

            # Prepare episode data
            episode_data = {
                "content": content,
                "type": episode_type,
                "source": source,
                "timestamp": reference_time.isoformat(),
            }
            
            if metadata:
                episode_data["metadata"] = metadata

            # Add episode to Graphiti
            logger.info(f"Adding episode: type={episode_type}, source={source}")
            result = await self._graphiti.add_episode(
                name=f"{episode_type}_{reference_time.isoformat()}",
                episode_body=content,
                reference_time=reference_time,
                source_description=source
            )

            logger.info(f"Episode added successfully: {result}")
            return {
                "success": True,
                "episode_id": result,
                "timestamp": reference_time.isoformat(),
                "type": episode_type
            }

        except Exception as e:
            error_text = str(e)
            summary = error_text[:300] + "... [truncated]" if len(error_text) > 300 else error_text
            logger.error("Failed to add episode (summary): %s", summary)
            logger.debug("Failed to add episode (full): %s", error_text)
            raise GraphitiClientError(f"Failed to add episode: {error_text}")

    async def search(
        self,
        query: str,
        num_results: int = 10,
        search_type: str = "hybrid"  # hybrid, semantic, or keyword
    ) -> List[Dict[str, Any]]:
        """
        Search the knowledge graph
        
        Args:
            query: Search query string
            num_results: Number of results to return
            search_type: Type of search (hybrid, semantic, keyword)
            
        Returns:
            List of search results with nodes, edges, and scores
        """
        if not self._graphiti:
            raise GraphitiClientError("Graphiti client not initialized. Call initialize() first.")

        # Sanitize queries that may include markdown or symbols that break RediSearch
        def _sanitize(q: str) -> str:
            try:
                import re
                q = re.sub(r"```.*?```", " ", q, flags=re.DOTALL)  # remove fenced blocks
                q = q.replace("`", " ")
                q = q.replace("|", " ")
                q = q.replace("(", " ").replace(")", " ")
                q = q.replace("\n", " ").replace("\r", " ")
                q = q.replace(":", " ").replace(";", " ")
                q = re.sub(r"\s+", " ", q).strip()
                return q
            except Exception:
                return q

        safe_query = _sanitize(query)

        try:
            logger.info(f"Searching graph: query='{safe_query}', type={search_type}, limit={num_results}")
            
            # Perform search based on type
            if search_type == "hybrid":
                results = await self._graphiti.search(safe_query, num_results=num_results)
            elif search_type == "semantic":
                results = await self._graphiti.search(
                    safe_query,
                    num_results=num_results,
                    use_reranker=False
                )
            else:
                logger.warning(f"Unsupported search type: {search_type}, using hybrid")
                results = await self._graphiti.search(safe_query, num_results=num_results)

            logger.info(f"Search completed: {len(results)} results found")
            return results

        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise GraphitiClientError(f"Search failed: {e}")

    async def get_conversation_history(
        self,
        user_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Retrieve conversation history for a specific user
        
        Args:
            user_id: User identifier
            limit: Maximum number of conversations to retrieve
            
        Returns:
            List of conversation episodes
        """
        if not self._graphiti:
            raise GraphitiClientError("Graphiti client not initialized. Call initialize() first.")

        try:
            # Search for conversation episodes related to user
            query = f"conversations with user {user_id}"
            results = await self.search(query, num_results=limit, search_type="semantic")
            
            logger.info(f"Retrieved {len(results)} conversation history items for user {user_id}")
            return results

        except Exception as e:
            logger.error(f"Failed to retrieve conversation history: {e}")
            raise GraphitiClientError(f"Failed to retrieve conversation history: {e}")


    async def health_check(self) -> Dict[str, Any]:
        """
        Perform an active health check by running a lightweight Cypher query.
        
        Returns:
            Dict with status ('connected', 'inactive', 'error') and optional message/latency_ms
        """
        if not self._driver:
            return {"status": "inactive", "message": "Driver not initialized"}

        try:
            start_time = datetime.now()
            
            async with asyncio.timeout(5.0):
                # Access the underlying FalkorDB graph client from the driver
                # The FalkorDriver wraps a FalkorDB Graph object
                if hasattr(self._driver, '_graph') and self._driver._graph is not None:
                    # Execute a simple Cypher query that returns immediately
                    # RETURN 1 is the lightest possible query
                    result = await asyncio.to_thread(
                        self._driver._graph.ro_query,
                        "RETURN 1 AS health_check"
                    )
                    
                    latency_ms = (datetime.now() - start_time).total_seconds() * 1000
                    
                    # Verify we got a valid result
                    if result and hasattr(result, 'result_set') and len(result.result_set) > 0:
                        return {
                            "status": "connected",
                            "latency_ms": round(latency_ms, 2),
                            "message": "Cypher RETURN 1 probe successful"
                        }
                    else:
                        return {
                            "status": "error",
                            "message": "Cypher query returned no results"
                        }
                else:
                    # Fallback: driver exists but graph not accessible
                    # Try to check if driver has any connectivity method
                    return {
                        "status": "connected",
                        "message": "Driver initialized (graph probe unavailable)"
                    }
                
        except asyncio.TimeoutError:
            return {"status": "error", "message": "Health check timed out after 5s"}
        except Exception as e:
            logger.warning(f"Graphiti health check failed: {e}")
            return {"status": "error", "message": str(e)}

    async def close(self) -> None:
        """Close Graphiti client and cleanup resources"""
        try:
            if self._driver:
                await self._driver.close()
            logger.info("Graphiti client closed successfully")
        except Exception as e:
            logger.error(f"Error closing Graphiti client: {e}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# Global Graphiti client instance
_graphiti_client: Optional[GraphitiClient] = None


async def create_graphiti_client() -> GraphitiClient:
    """Instantiate and initialize a new Graphiti client."""
    client = GraphitiClient()
    await client.initialize()
    return client


async def get_graphiti_client() -> GraphitiClient:
    """Return a cached global Graphiti client instance, initializing if needed."""
    global _graphiti_client

    if _graphiti_client is None:
        _graphiti_client = await create_graphiti_client()

    return _graphiti_client

async def close_graphiti_client() -> None:
    """Close global Graphiti client instance"""
    global _graphiti_client
    
    if _graphiti_client:
        await _graphiti_client.close()
        _graphiti_client = None