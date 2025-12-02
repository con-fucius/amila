"""
Langfuse Client for Observability & Tracing
Provides centralized tracing for query orchestration pipeline
"""

import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from app.core.config import settings

logger = logging.getLogger(__name__)

# Global Langfuse client instance
_langfuse_client: Optional[Any] = None


def get_langfuse_client():
    """
    Get or create Langfuse client singleton
    
    Returns:
        Langfuse client instance or None if disabled/not configured
    """
    global _langfuse_client
    
    if not settings.LANGFUSE_ENABLED:
        return None
    
    if _langfuse_client is None:
        try:
            from langfuse import Langfuse
            
            if not settings.LANGFUSE_PUBLIC_KEY or not settings.LANGFUSE_SECRET_KEY:
                logger.warning(f"Langfuse enabled but credentials not configured")
                return None
            
            _langfuse_client = Langfuse(
                public_key=settings.LANGFUSE_PUBLIC_KEY,
                secret_key=settings.LANGFUSE_SECRET_KEY,
                host=settings.LANGFUSE_HOST,
            )
            
            logger.info(f"Langfuse client initialized (host: {settings.LANGFUSE_HOST})")
            
        except ImportError:
            logger.warning(f"Langfuse package not installed, tracing disabled")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Langfuse client: {e}")
            return None
    
    return _langfuse_client


def create_trace(
    query_id: str,
    user_id: str,
    user_query: str,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Create a new Langfuse trace for a query
    
    Args:
        query_id: Unique query identifier
        user_id: User identifier
        user_query: Natural language query
        metadata: Additional metadata
        
    Returns:
        Trace ID string or None
    """
    client = get_langfuse_client()
    if not client:
        return None
    
    try:
        # Langfuse v2+ uses trace() method
        client.trace(
            id=query_id,
            name="query_orchestration",
            user_id=user_id,
            input={"query": user_query},
            metadata=metadata or {},
            session_id=query_id,
        )
        return query_id
    except Exception as e:
        logger.error(f"Failed to create Langfuse trace: {e}")
        return None


def create_span(
    trace_id: str,
    span_name: str,
    input_data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Create a span within a trace
    
    Args:
        trace_id: Parent trace ID
        span_name: Name of the span (e.g., "understand_query", "generate_sql")
        input_data: Input data for the span
        metadata: Additional metadata
        
    Returns:
        Span object or None
    """
    client = get_langfuse_client()
    if not client:
        return None
    
    try:
        # Langfuse v2+ uses trace.span() method
        span = client.span(
            name=span_name,
            input=input_data or {},
            metadata=metadata or {},
            trace_id=trace_id,
        )
        return span
    except Exception as e:
        logger.error(f"Failed to create Langfuse span: {e}")
        return None


def update_span(
    span_obj,
    output_data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    level: str = "DEFAULT",
    status_message: Optional[str] = None
):
    """
    Update a span with output and completion status, then end it
    
    Args:
        span_obj: Span object from create_span
        output_data: Output data from the span
        metadata: Additional metadata
        level: Log level (DEFAULT, WARNING, ERROR)
        status_message: Status_message
    """
    if not span_obj:
        return
    
    try:
        span_obj.update(
            output=output_data or {},
            metadata=metadata or {},
            level=level,
            status_message=status_message,
        )
        span_obj.end()
    except Exception as e:
        logger.error(f"Failed to update Langfuse span: {e}")


def log_generation(
    trace_id: str,
    name: str,
    model: str,
    input_data: Dict[str, Any],
    output_data: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
    usage: Optional[Dict[str, Any]] = None
):
    """
    Log an LLM generation event
    
    Args:
        trace_id: Parent trace ID
        name: Generation name (e.g., "sql_generation", "intent_classification")
        model: Model identifier
        input_data: Input to the LLM
        output_data: Output from the LLM
        metadata: Additional metadata
        usage: Token usage information
    """
    client = get_langfuse_client()
    if not client:
        return
    
    try:
        # Langfuse v2+ uses generation() method
        client.generation(
            name=name,
            model=model,
            input=input_data,
            output=output_data,
            metadata=metadata or {},
            usage=usage if usage else None,
            trace_id=trace_id,
        )
    except Exception as e:
        logger.error(f"Failed to log Langfuse generation: {e}")


def log_event(
    trace_id: str,
    name: str,
    input_data: Optional[Dict[str, Any]] = None,
    output_data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    level: str = "DEFAULT"
):
    """
    Log a custom event
    
    Args:
        trace_id: Parent trace ID
        name: Event name
        input_data: Event input
        output_data: Event output
        metadata: Additional metadata
        level: Log level
    """
    client = get_langfuse_client()
    if not client:
        return
    
    try:
        # Langfuse v2+ uses event() method
        client.event(
            name=name,
            input=input_data or {},
            output=output_data or {},
            metadata=metadata or {},
            level=level,
            trace_id=trace_id,
        )
    except Exception as e:
        logger.error(f"Failed to log Langfuse event: {e}")


def update_trace(
    trace_id: str,
    output_data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[list] = None
):
    """
    Update trace with final output and metadata
    
    Args:
        trace_id: Trace identifier
        output_data: Final output data
        metadata: Additional metadata
        tags: Tags for categorization
    """
    client = get_langfuse_client()
    if not client:
        return
    
    try:
        # Langfuse v2+ uses trace() method with same ID to update
        client.trace(
            id=trace_id,
            output=output_data or {},
            metadata=metadata or {},
            tags=tags or [],
        )
    except Exception as e:
        logger.error(f"Failed to update Langfuse trace: {e}")


def flush():
    """Flush pending Langfuse events"""
    client = get_langfuse_client()
    if client:
        try:
            client.flush()
        except Exception as e:
            logger.error(f"Failed to flush Langfuse: {e}")

def flush_langfuse():
    """Alias for flush() - used in application shutdown"""
    flush()


@asynccontextmanager
async def trace_span(
    trace_id: str,
    span_name: str,
    input_data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Context manager for tracing a span
    
    Usage:
        async with trace_span(query_id, "generate_sql", {"query": query}) as span:
            # Your code here
            result = await generate_sql(query)
            span["output"] = {"sql": result}
    """
    span_data = {"output": {}, "metadata": metadata or {}, "level": "DEFAULT"}
    
    span = create_span(trace_id, span_name, input_data, metadata)
    
    try:
        yield span_data
        
        if span:
            update_span(
                span,
                output_data=span_data.get("output"),
                metadata=span_data.get("metadata"),
                level=span_data.get("level", "DEFAULT"),
                status_message=span_data.get("status_message")
            )
    except Exception as e:
        if span:
            update_span(
                span,
                output_data={"error": str(e)},
                level="ERROR",
                status_message=f"Span failed: {str(e)}"
            )
        raise
