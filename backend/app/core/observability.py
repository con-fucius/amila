"""
OpenTelemetry Observability Setup
Configures tracing, metrics, and logging instrumentation
"""

import logging
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource

from app.core.config import settings


def setup_observability(app=None) -> None:
    """
    Configure OpenTelemetry instrumentation for traces, metrics, and logs
    
    Args:
        app: FastAPI app instance for instrumentation (optional)
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Create resource with service information
        resource = Resource.create({
            "service.name": settings.otel_service_name,
            "service.version": settings.otel_service_version,
            "service.namespace": "amila-mvp",
            "deployment.environment": settings.environment,
        })
        
        # Setup tracing
        _setup_tracing(resource)
        
        # Setup metrics
        _setup_metrics(resource)
        
        # Setup automatic instrumentation
        _setup_instrumentation(app)
        
        logger.info("OpenTelemetry observability configured successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup observability: {e}")
        raise


def _setup_tracing(resource: Resource) -> None:
    """Configure distributed tracing"""
    
    # Create tracer provider
    tracer_provider = TracerProvider(resource=resource)
    
    # Only setup OTLP exporter in production or when explicitly enabled
    if settings.environment == "production" or settings.otel_exporter_otlp_endpoint != "http://localhost:4318":
        try:
            # Create OTLP span exporter
            otlp_exporter = OTLPSpanExporter(
                endpoint=f"{settings.otel_exporter_otlp_endpoint}/v1/traces",
                headers={}
            )
            
            # Create batch span processor
            span_processor = BatchSpanProcessor(
                otlp_exporter,
                max_queue_size=2048,
                max_export_batch_size=512,
                schedule_delay_millis=5
            )
            
            # Add processor to tracer provider
            tracer_provider.add_span_processor(span_processor)
            
        except Exception as e:
            logging.getLogger(__name__).warning(f"OTLP trace exporter not available: {e}")
    
    # Set global tracer provider
    trace.set_tracer_provider(tracer_provider)


def _setup_metrics(resource: Resource) -> None:
    """Configure metrics collection"""
    
    metric_readers = []
    
    # Only setup OTLP exporter in production or when explicitly enabled
    if settings.environment == "production" or settings.otel_exporter_otlp_endpoint != "http://localhost:4318":
        try:
            # Create OTLP metric exporter
            metric_exporter = OTLPMetricExporter(
                endpoint=f"{settings.otel_exporter_otlp_endpoint}/v1/metrics",
                headers={}
            )
            
            # Create periodic exporting metric reader
            metric_reader = PeriodicExportingMetricReader(
                exporter=metric_exporter,
                export_interval_millis=10000,  # 10 seconds
                export_timeout_millis=30000    # 30 seconds
            )
            
            metric_readers.append(metric_reader)
            
        except Exception as e:
            logging.getLogger(__name__).warning(f"OTLP metric exporter not available: {e}")
    
    # Create meter provider
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=metric_readers
    )
    
    # Set global meter provider
    metrics.set_meter_provider(meter_provider)


def _setup_instrumentation(app=None) -> None:
    """Setup automatic instrumentation for frameworks"""
    
    # FastAPI instrumentation
    if app:
        FastAPIInstrumentor().instrument_app(app)
    
    # HTTP client instrumentation
    HTTPXClientInstrumentor().instrument()
    
    # Logging instrumentation
    LoggingInstrumentor().instrument(set_logging_format=True)


def get_tracer(name: str):
    """Get a tracer instance"""
    return trace.get_tracer(name)


def get_meter(name: str):
    """Get a meter instance"""
    return metrics.get_meter(name)