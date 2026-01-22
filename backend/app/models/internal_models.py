"""
Internal Data Models for JSON Deserialization

Pydantic models for validating data structures stored in Redis and other
internal data sources. These models ensure data integrity and prevent
parsing errors from corrupted or malicious data.

Security: All json.loads() calls should use these models for validation
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, ConfigDict

logger = logging.getLogger(__name__)


# ==================== Redis Session Models ====================

class SessionData(BaseModel):
    """User session data stored in Redis"""
    model_config = ConfigDict(extra='allow')  # Allow extra fields for flexibility
    
    user_id: str
    username: str
    email: Optional[str] = None
    role: str
    is_active: bool = True
    created_at: Optional[str] = None
    last_activity: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    
    @field_validator('role')
    @classmethod
    def validate_role(cls, v: str) -> str:
        """Validate role is one of the expected values"""
        valid_roles = {'admin', 'analyst', 'viewer', 'guest', 'developer'}
        if v.lower() not in valid_roles:
            logger.warning(f"Unexpected role value: {v}")
        return v


class RefreshTokenData(BaseModel):
    """Refresh token data stored in Redis"""
    model_config = ConfigDict(extra='forbid')
    
    jti: str
    user_id: str
    created_at: str
    expires_at: str
    is_active: bool = True
    ip_address: Optional[str] = None


# ==================== Redis Cache Models ====================

class SchemaMetadata(BaseModel):
    """Database schema metadata cached in Redis"""
    model_config = ConfigDict(extra='allow')
    
    tables: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None
    columns: Optional[Dict[str, List[Dict[str, Any]]]] = None
    relationships: Optional[List[Dict[str, Any]]] = None
    cached_at: Optional[str] = None
    database_type: Optional[str] = None
    
    @field_validator('tables', mode='before')
    @classmethod
    def normalize_tables(cls, v: Any) -> Optional[List[Dict[str, Any]]]:
        """
        Normalize tables field to always be a list.
        Handles both dict format (table_name -> columns) and list format.
        """
        if v is None:
            return None
        
        # If it's already a list, return as-is
        if isinstance(v, list):
            return v
        
        # If it's a dict, convert to list format
        if isinstance(v, dict):
            # Convert dict format to list format
            tables_list = []
            for table_name, columns in v.items():
                if isinstance(columns, list):
                    tables_list.append({
                        "name": table_name,
                        "columns": columns
                    })
            return tables_list
        
        logger.warning(f"Unexpected tables format: {type(v)}, returning None")
        return None


class SampleData(BaseModel):
    """Sample table data cached in Redis"""
    model_config = ConfigDict(extra='allow')
    
    rows: List[Dict[str, Any]]
    row_count: int
    cached_at: Optional[str] = None
    
    @field_validator('rows')
    @classmethod
    def validate_rows(cls, v: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Ensure rows is a list"""
        if not isinstance(v, list):
            raise ValueError("rows must be a list")
        return v


class QueryCacheEntry(BaseModel):
    """Query result cache entry"""
    model_config = ConfigDict(extra='allow')
    
    result: Dict[str, Any]
    cached_at: str
    result_size: int = 0
    ttl: int = 300
    
    @field_validator('result')
    @classmethod
    def validate_result(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure result has expected structure"""
        if not isinstance(v, dict):
            raise ValueError("result must be a dictionary")
        return v


# ==================== Audit Log Models ====================

class AuditEntryData(BaseModel):
    """Audit trail entry from Redis"""
    model_config = ConfigDict(extra='allow')
    
    timestamp: str
    action: str
    user: str
    user_role: Optional[str] = None
    severity: str
    success: bool
    resource: Optional[str] = None
    resource_id: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    correlation_id: Optional[str] = None
    
    @field_validator('severity')
    @classmethod
    def validate_severity(cls, v: str) -> str:
        """Validate severity level"""
        valid_severities = {'info', 'warning', 'error', 'critical'}
        if v.lower() not in valid_severities:
            logger.warning(f"Unexpected severity value: {v}")
        return v


# ==================== LLM Cache Models ====================

class LLMCacheEntry(BaseModel):
    """LLM response cache entry"""
    model_config = ConfigDict(extra='allow')
    
    response: Union[str, Dict[str, Any]]
    cached_at: Optional[str] = None
    model: Optional[str] = None
    tokens: Optional[int] = None


# ==================== MCP Response Models ====================

class MCPResult(BaseModel):
    """MCP JSON-RPC result"""
    model_config = ConfigDict(extra='allow')
    
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[Union[str, int]] = None


class MCPQueryResult(BaseModel):
    """MCP query execution result"""
    model_config = ConfigDict(extra='allow')
    
    status: str
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    row_count: Optional[int] = None
    execution_time_ms: Optional[float] = None
    results: Optional[Dict[str, Any]] = None
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status value"""
        valid_statuses = {'success', 'error', 'pending', 'cancelled'}
        if v.lower() not in valid_statuses:
            logger.warning(f"Unexpected status value: {v}")
        return v


# ==================== Configuration Models ====================

class CORSOriginsList(BaseModel):
    """CORS origins list from config"""
    model_config = ConfigDict(extra='forbid')
    
    origins: List[str]
    
    @field_validator('origins')
    @classmethod
    def validate_origins(cls, v: List[str]) -> List[str]:
        """Validate and clean origins"""
        return [origin.strip() for origin in v if isinstance(origin, str) and origin.strip()]


class QwenCredentials(BaseModel):
    """Qwen API credentials from JSON file"""
    model_config = ConfigDict(extra='allow')
    
    api_key: str
    base_url: Optional[str] = None
    model: Optional[str] = None
    
    @field_validator('api_key')
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        """Ensure API key is not empty"""
        if not v or not v.strip():
            raise ValueError("api_key cannot be empty")
        return v.strip()


# ==================== Insights Models ====================

class InsightsData(BaseModel):
    """LLM-generated insights data"""
    model_config = ConfigDict(extra='allow')
    
    insights: List[str] = Field(default_factory=list)
    suggested_queries: List[str] = Field(default_factory=list)
    summary: Optional[str] = None


# ==================== Utility Functions ====================

def safe_parse_json(
    json_str: str,
    model: type[BaseModel],
    default: Optional[Any] = None,
    log_errors: bool = True
) -> Optional[Any]:
    """
    Safely parse JSON string with Pydantic validation
    
    Args:
        json_str: JSON string to parse
        model: Pydantic model class for validation
        default: Default value if parsing fails
        log_errors: Whether to log parsing errors
        
    Returns:
        Validated model instance or default value
    """
    import json
    
    try:
        data = json.loads(json_str)
        return model.model_validate(data)
    except json.JSONDecodeError as e:
        if log_errors:
            logger.error(f"JSON decode error: {e}")
        return default
    except Exception as e:
        if log_errors:
            logger.error(f"Validation error for {model.__name__}: {e}")
        return default


def safe_parse_json_dict(
    json_str: str,
    default: Optional[Dict[str, Any]] = None,
    log_errors: bool = True
) -> Dict[str, Any]:
    """
    Safely parse JSON string to dictionary without strict validation
    
    Args:
        json_str: JSON string to parse
        default: Default value if parsing fails
        log_errors: Whether to log parsing errors
        
    Returns:
        Parsed dictionary or default value
    """
    import json
    
    try:
        data = json.loads(json_str)
        if not isinstance(data, dict):
            if log_errors:
                logger.warning(f"Expected dict but got {type(data)}")
            return default or {}
        return data
    except json.JSONDecodeError as e:
        if log_errors:
            logger.error(f"JSON decode error: {e}")
        return default or {}
    except Exception as e:
        if log_errors:
            logger.error(f"Unexpected error parsing JSON: {e}")
        return default or {}


def safe_parse_json_list(
    json_str: str,
    default: Optional[List[Any]] = None,
    log_errors: bool = True
) -> List[Any]:
    """
    Safely parse JSON string to list without strict validation
    
    Args:
        json_str: JSON string to parse
        default: Default value if parsing fails
        log_errors: Whether to log parsing errors
        
    Returns:
        Parsed list or default value
    """
    import json
    
    try:
        data = json.loads(json_str)
        if not isinstance(data, list):
            if log_errors:
                logger.warning(f"Expected list but got {type(data)}")
            return default or []
        return data
    except json.JSONDecodeError as e:
        if log_errors:
            logger.error(f"JSON decode error: {e}")
        return default or []
    except Exception as e:
        if log_errors:
            logger.error(f"Unexpected error parsing JSON: {e}")
        return default or []
