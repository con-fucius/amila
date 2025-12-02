"""
Centralized Configuration Manager
Provides unified configuration management with validation, environment overrides, and secure storage support
"""

import os
import json
import logging
from typing import Optional, List, Union
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator, ValidationError


class AppSettings(BaseSettings):
    """Application settings with comprehensive validation"""

    # Application Settings
    app_name: str = Field(default="Amila", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    environment: str = Field(default="development", description="Runtime environment")
    debug: bool = Field(default=True, description="Debug mode")
    log_level: str = Field(default="INFO", description="Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)")

    # Server Configuration
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, ge=1, le=65535, description="Server port")

    # Security Settings
    jwt_secret_key: str = Field(..., min_length=32, description="JWT secret key (min 32 characters)")
    jwt_refresh_secret_key: str = Field(..., min_length=32, description="JWT refresh token secret key (min 32 characters)")
    jwt_algorithm: str = Field(default="HS256", pattern=r"^(HS256|HS384|HS512|RS256|RS384|RS512)$", description="JWT algorithm")
    jwt_expire_hours: int = Field(default=8, ge=1, le=168, description="JWT expiration hours (1-168)")
    refresh_token_expire_days: int = Field(default=30, ge=1, le=365, description="Refresh token expiration days (1-365)")

    # Authentication Rate Limiting
    auth_rate_limit_max_attempts: int = Field(default=5, ge=1, le=20, description="Max authentication attempts per window")
    auth_rate_limit_window_seconds: int = Field(default=300, ge=60, le=3600, description="Rate limit window in seconds")
    auth_rate_limit_block_duration: int = Field(default=900, ge=300, le=3600, description="Block duration after max attempts")

    # AWS Configuration
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    aws_region: str = Field(default="us-east-1", pattern=r"^[a-z0-9-]+$", description="AWS region")
    aws_default_region: str = Field(default="us-east-1", pattern=r"^[a-z0-9-]+$", description="AWS default region")
    aws_bedrock_model_id: str = Field(default="anthropic.claude-3-5-sonnet-20240620-v1:0", description="AWS Bedrock model ID for production")

    # Oracle Database Configuration
    ORACLE_HOST: str = Field(default="localhost", min_length=1, max_length=255, description="Oracle host")
    ORACLE_PORT: int = Field(default=1521, ge=1, le=65535, description="Oracle port")
    ORACLE_SERVICE_NAME: str = Field(default="XEPDB1", min_length=1, max_length=128, description="Oracle service name")
    ORACLE_USERNAME: Optional[str] = Field(default=None, min_length=1, max_length=128, description="Oracle username")
    ORACLE_PASSWORD: Optional[str] = Field(default=None, min_length=1, max_length=128, description="Oracle password")

    # MCP Server Configuration
    mcp_server_host: str = Field(default="localhost", min_length=1, max_length=255, description="MCP server host")
    mcp_server_port: int = Field(default=3001, ge=1, le=65535, description="MCP server port")
    mcp_connection_timeout: int = Field(default=30, ge=5, le=300, description="MCP connection timeout")
    mcp_request_timeout: int = Field(default=600, ge=30, le=3600, description="MCP request timeout")
    mcp_retry_attempts: int = Field(default=3, ge=1, le=10, description="MCP retry attempts")
    mcp_retry_backoff: float = Field(default=1.5, ge=1.0, le=10.0, description="MCP retry backoff")

    # Doris MCP Configuration
    DORIS_MCP_ENABLED: bool = Field(default=False, description="Enable Doris MCP Server integration")
    DORIS_MCP_REPO_URL: str = Field(default="https://github.com/apache/doris-mcp-server.git", description="Doris MCP Server Git Repo")
    DORIS_MCP_SERVER_PATH: Optional[str] = Field(
        default=None,
        description="Local path to Doris MCP Server (overrides git clone if set)"
    )
    DORIS_MCP_HOST: str = Field(default="127.0.0.1", description="Host for Doris MCP Server subprocess")
    DORIS_MCP_PORT: int = Field(default=8808, ge=1024, le=65535, description="Port for Doris MCP Server subprocess")
    DORIS_DB_HOST: str = Field(default="localhost", description="Doris Database Host")
    DORIS_DB_PORT: int = Field(default=9030, description="Doris Database Port")
    DORIS_DB_USER: str = Field(default="root", description="Doris Database User")
    DORIS_DB_PASSWORD: str = Field(default="", description="Doris Database Password")
    DORIS_DB_DATABASE: str = Field(default="demo", description="Doris Database Name")
    
    # SQLcl Configuration for STDIO MCP
    sqlcl_path: str = Field(default="sql", min_length=1, max_length=255, description="Path to SQLcl executable")
    sqlcl_args: List[str] = Field(default=["-mcp"], description="SQLcl command line arguments for MCP mode")
    sqlcl_timeout: int = Field(default=600, ge=30, le=3600, description="SQLcl subprocess timeout")
    sqlcl_max_processes: int = Field(default=2, ge=1, le=20, description="Maximum SQLcl processes in pool")
    oracle_default_connection: str = Field(default="TestUserCSV", description="Default Oracle SQLcl connection name to use for executions")
    # Redis Configuration
    REDIS_HOST: str = Field(default="localhost", description="Redis host")
    REDIS_PORT: int = Field(default=6379, ge=1, le=65535, description="Redis port")
    REDIS_PASSWORD: str = Field(default="", description="Redis password (load from .env)")
    REDIS_SESSION_DB: int = Field(default=0, ge=0, le=15, description="Redis database for sessions")
    REDIS_CACHE_DB: int = Field(default=1, ge=0, le=15, description="Redis database for caching")
    REDIS_CELERY_DB: int = Field(default=2, ge=0, le=15, description="Redis database for Celery")

    # Celery Configuration (URLs constructed at runtime via properties)
    CELERY_BROKER_DB: int = Field(default=0, ge=0, le=15, description="Redis database for Celery broker")
    CELERY_RESULT_DB: int = Field(default=1, ge=0, le=15, description="Redis database for Celery results")

    # LangGraph Configuration
    LANGGRAPH_CHECKPOINT_DB: str = Field(default="/app/data/checkpoints.db", description="LangGraph checkpoint database path")
    LANGGRAPH_API_KEY: Optional[str] = Field(default=None, description="LangGraph API key")

    # OpenTelemetry Configuration
    otel_exporter_otlp_endpoint: str = Field(default="http://localhost:4318", description="OTLP endpoint")
    otel_service_name: str = Field(default="bi-agent-backend", description="Service name for tracing")
    otel_service_version: str = Field(default="1.0.0", description="Service version")

    # CORS Configuration
    cors_origins_raw: Union[str, List[str]] = Field(
        default=['http://localhost:5173', 'http://127.0.0.1:5173', 'http://localhost:3000', 'http://127.0.0.1:3000'],
        alias="cors_origins",
        description="CORS origins (comma separated string or JSON list)",
    )
    cors_allow_credentials: bool = Field(default=True, description="Allow CORS credentials")
    cors_allow_methods: List[str] = Field(default=["*"], description="Allowed CORS methods")
    cors_allow_headers: List[str] = Field(default=["*"], description="Allowed CORS headers")

    # Secrets Manager Configuration (for future AWS Secrets Manager integration)
    use_secrets_manager: bool = Field(default=False, description="Enable AWS Secrets Manager")
    secrets_manager_prefix: str = Field(default="/bi-agent/", description="Secrets Manager prefix")

    # Google Gemini Configuration (Development/Testing)
    GOOGLE_API_KEY: Optional[str] = Field(default=None, description="Google Gemini API key for development")

    # FalkorDB Configuration (Graph Database for Graphiti)
    FALKORDB_HOST: str = Field(default="localhost", description="FalkorDB host")
    FALKORDB_PORT: int = Field(default=6380, ge=1, le=65535, description="FalkorDB port (mapped from 6379)")
    FALKORDB_PASSWORD: str = Field(default="", description="FalkorDB password (optional)")
    FALKORDB_DATABASE: str = Field(default="amil_knowledge_graph", description="FalkorDB database name")

    # Graphiti Configuration (Temporal Knowledge Graph)
    GRAPHITI_TELEMETRY_ENABLED: bool = Field(default=False, description="Enable Graphiti telemetry")
    GRAPHITI_SEMAPHORE_LIMIT: int = Field(default=10, ge=1, le=100, description="Graphiti concurrency limit")
    GRAPHITI_LLM_PROVIDER: str = Field(default="gemini", pattern=r"^(gemini|bedrock)$", description="Graphiti LLM provider")
    GRAPHITI_LLM_MODEL: str = Field(default="gemini-2.5-flash-lite", description="Graphiti LLM model name")
    GRAPHITI_EMBEDDING_PROVIDER: str = Field(default="gemini", pattern=r"^(gemini|bedrock)$", description="Graphiti embedding provider")
    GRAPHITI_EMBEDDING_MODEL: str = Field(default="gemini-embedding-001", description="Graphiti embedding model")
    GRAPHITI_EMBEDDING_DIMENSIONS: int = Field(default=768, ge=128, le=3072, description="Graphiti embedding dimensions")

    # Query Orchestrator Configuration (separate from Graphiti)
    QUERY_LLM_PROVIDER: str = Field(
        default="gemini",
        pattern=r"^(gemini|bedrock|qwen)$",
        description="LLM provider for the query orchestrator"
    )
    QUERY_LLM_MODEL: Optional[str] = Field(
        default=None,
        description="Override LLM model for the query orchestrator"
    )
    # Skills feature flag (alpha safe default: disabled)
    QUERY_SQL_SKILLS_ENABLED: bool = Field(
        default=False,
        description="Enable skills-based column mapping and prompt enhancement for SQL generation"
    )
    
    # Middleware feature flags (all disabled by default for safety)
    MIDDLEWARE_TRACING_ENABLED: bool = Field(
        default=False,
        description="Enable LLM tracing middleware for observability"
    )
    MIDDLEWARE_CACHING_ENABLED: bool = Field(
        default=False,
        description="Enable LLM response caching middleware"
    )
    MIDDLEWARE_VALIDATION_ENABLED: bool = Field(
        default=False,
        description="Enable LLM output validation middleware with auto-retry"
    )
    MIDDLEWARE_COST_CONTROL_ENABLED: bool = Field(
        default=False,
        description="Enable LLM cost control middleware for rate limiting"
    )

    # Qwen Code CLI Integration (OpenAI-compatible)
    QWEN_CODE_CREDENTIALS_PATH: Optional[str] = Field(
        default=None,
        description="Path to Qwen Code CLI credentials JSON (e.g., C:/Users/<user>/.qwen/credentials.json)"
    )
    QWEN_API_BASE_URL: Optional[str] = Field(
        default=None,
        description="Override for Qwen OpenAI-compatible base URL"
    )

    # Langfuse Configuration (Observability & Tracing)
    LANGFUSE_PUBLIC_KEY: Optional[str] = Field(default=None, description="Langfuse public API key")
    LANGFUSE_SECRET_KEY: Optional[str] = Field(default=None, description="Langfuse secret API key")
    LANGFUSE_HOST: str = Field(default="https://cloud.langfuse.com", description="Langfuse host URL")
    LANGFUSE_ENABLED: bool = Field(default=False, description="Enable Langfuse tracing")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra environment variables
    )

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment setting"""
        valid_environments = ["development", "staging", "production", "test"]
        if v not in valid_environments:
            raise ValueError(f"Environment must be one of: {valid_environments}")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level setting"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    @property
    def cors_origins(self) -> List[str]:
        """Return normalized list of CORS origins."""
        value = self.cors_origins_raw

        if isinstance(value, list):
            return [origin.strip() for origin in value if isinstance(origin, str) and origin.strip()]

        if isinstance(value, str):
            raw_value = value.strip()
            if not raw_value:
                return []

            if raw_value.startswith("[") and raw_value.endswith("]"):
                try:
                    parsed = json.loads(raw_value)
                    if isinstance(parsed, list):
                        return [origin.strip() for origin in parsed if isinstance(origin, str) and origin.strip()]
                except json.JSONDecodeError:
                    pass

            origins = [origin.strip().strip("'\"") for origin in raw_value.split(",")]
            return [origin for origin in origins if origin]

        return []
    
    @field_validator("sqlcl_args", mode="before")
    @classmethod
    def validate_sqlcl_args(cls, v) -> List[str]:
        """Validate SQLcl arguments"""
        if isinstance(v, str):
            # Parse string like '"-mcp", "-timeout", "300"' or "-mcp -timeout 300"
            import shlex
            try:
                return shlex.split(v)
            except ValueError:
                # Fallback to simple split
                return [arg.strip().strip('"\'') for arg in v.split(",") if arg.strip()]
        return v if v else ["-mcp"]

    @field_validator("jwt_secret_key", "jwt_refresh_secret_key")
    @classmethod
    def validate_jwt_secret_key(cls, v: str) -> str:
        """Ensure JWT secret keys are strong enough"""
        if len(v) < 32:
            raise ValueError("JWT secret key must be at least 32 characters long")
        return v

    def __init__(self, **data):
        super().__init__(**data)
        # Perform additional validation after initialization
        self._validate_configuration()

    def _validate_configuration(self):
        """Perform comprehensive configuration validation"""
        # Check for port conflicts
        self._validate_port_conflicts()

        # Validate required secrets based on environment
        self._validate_secrets()

        # Environment-specific validations
        self._validate_environment_specific()

    def _validate_port_conflicts(self):
        """Check for port conflicts between services"""
        ports = [self.port, self.ORACLE_PORT, self.mcp_server_port, self.REDIS_PORT, self.DORIS_MCP_PORT]
        unique_ports = set(ports)
        if len(ports) != len(unique_ports):
            duplicates = [port for port in ports if ports.count(port) > 1]
            raise ValueError(f"Port conflicts detected: {duplicates}. Each service must use a unique port.")

    def _validate_secrets(self):
        """Validate required secrets are present"""
        if self.environment in ["staging", "production"]:
            required_secrets = ["jwt_secret_key", "oracle_username", "oracle_password"]
            if self.use_secrets_manager:
                # In production, secrets should come from Secrets Manager
                pass  # Placeholder for future Secrets Manager validation
            else:
                missing = []
                if not self.jwt_secret_key:
                    missing.append("jwt_secret_key")
                if not self.jwt_refresh_secret_key:
                    missing.append("jwt_refresh_secret_key")
                if not self.ORACLE_USERNAME:
                    missing.append("ORACLE_USERNAME")
                if not self.ORACLE_PASSWORD:
                    missing.append("ORACLE_PASSWORD")
                if missing:
                    raise ValueError(f"Missing required secrets in {self.environment} environment: {missing}")

    def _validate_environment_specific(self):
        """Environment-specific validation rules"""
        if self.environment == "production":
            if self.debug:
                raise ValueError("Debug mode must be disabled in production")
            if self.log_level.upper() not in ["WARNING", "ERROR", "CRITICAL"]:
                raise ValueError("Production environment requires log level WARNING or higher")

    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.environment == "production"

    @property
    def REDIS_URL(self) -> str:
        """Generate Redis URL dynamically from components"""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_SESSION_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_SESSION_DB}"

    @property
    def CELERY_BROKER_URL(self) -> str:
        """Generate Celery broker URL dynamically"""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_BROKER_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_BROKER_DB}"

    @property
    def CELERY_RESULT_BACKEND(self) -> str:
        """Generate Celery result backend URL dynamically"""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_RESULT_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_RESULT_DB}"

    @property
    def oracle_connection_string(self) -> str:
        """Generate Oracle connection string"""
        if not self.ORACLE_USERNAME or not self.ORACLE_PASSWORD:
            raise ValueError("Oracle username and password must be configured")
        return f"oracle://{self.ORACLE_USERNAME}:{self.ORACLE_PASSWORD}@{self.ORACLE_HOST}:{self.ORACLE_PORT}/{self.ORACLE_SERVICE_NAME}"

    @property
    def mcp_server_url(self) -> str:
        """Generate MCP server URL"""
        return f"http://{self.mcp_server_host}:{self.mcp_server_port}"

    @property
    def database_url(self) -> str:
        """Alias for Oracle connection string"""
        return self.oracle_connection_string

    @property
    def falkordb_connection_config(self) -> dict:
        """Generate FalkorDB connection configuration"""
        config = {
            "host": self.FALKORDB_HOST,
            "port": self.FALKORDB_PORT,
            "database": self.FALKORDB_DATABASE,
        }
        if self.FALKORDB_PASSWORD:
            config["password"] = self.FALKORDB_PASSWORD
        return config


class ConfigurationManager:
    """Centralized configuration manager"""

    _instance: Optional[AppSettings] = None
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_settings(cls) -> AppSettings:
        """Get the global settings instance with singleton pattern"""
        if cls._instance is None:
            try:
                cls._instance = AppSettings()
                cls._logger.info(f"Configuration loaded successfully for environment: {cls._instance.environment}")
            except ValidationError as e:
                cls._logger.error(f"Configuration validation failed: {e}")
                raise
            except Exception as e:
                cls._logger.error(f"Failed to load configuration: {e}")
                cls._logger.error("Please check your .env file and ensure all required variables are set")
                raise
        return cls._instance

    @classmethod
    def reload_settings(cls) -> AppSettings:
        """Reload configuration (useful for testing or dynamic config changes)"""
        cls._instance = None
        return cls.get_settings()

    @classmethod
    def validate_configuration(cls) -> bool:
        """Validate current configuration without loading"""
        try:
            settings = cls.get_settings()
            # Additional runtime validations can be added here
            return True
        except Exception as e:
            cls._logger.error(f"Configuration validation failed: {e}")
            return False


# Global settings instance
settings = ConfigurationManager.get_settings()

# Convenience function
def get_settings() -> AppSettings:
    """Get the global settings instance"""
    return ConfigurationManager.get_settings()