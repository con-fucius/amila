# Langfuse Observability Setup Guide

## Overview

Langfuse provides comprehensive LLM observability for the Amila system, tracking:
- Query orchestration traces
- LLM generation events (SQL generation, intent classification)
- Agent decision paths
- Token usage and costs
- Performance metrics

## Prerequisites

1. Langfuse account (cloud or self-hosted)
2. API credentials (public key and secret key)
3. `langfuse` Python package installed (already in requirements)

## Configuration

### 1. Enable Langfuse

Edit `.env` file:

```bash
# Enable Langfuse tracing
LANGFUSE_ENABLED=true

# Langfuse credentials (from https://cloud.langfuse.com)
LANGFUSE_PUBLIC_KEY=pk-lf-your-public-key
LANGFUSE_SECRET_KEY=sk-lf-your-secret-key

# Langfuse host (cloud or self-hosted)
LANGFUSE_HOST=https://cloud.langfuse.com
```

### 2. Verify Installation

Check that langfuse package is installed:

```bash
cd backend
.\.venv\Scripts\Activate.ps1
python -c "import langfuse; print(f'Langfuse version: {langfuse.__version__}')"
```

### 3. Test Connection

Run the verification script:

```bash
python scripts/verify_langfuse.py
```

## What Gets Traced

### 1. Query Orchestration
- **Trace Name**: `query_orchestration`
- **Trace ID**: Query ID (e.g., `q_20260118_143022`)
- **User ID**: Authenticated user
- **Session ID**: Query ID
- **Metadata**: Session, role, database type, frontend surface

### 2. Orchestrator Nodes (Spans)
- `orchestrator.understand` - Intent classification
- `orchestrator.retrieve_context` - Graphiti context retrieval
- `orchestrator.decompose` - Query decomposition
- `orchestrator.hypothesis` - Query hypothesis generation
- `orchestrator.generate_sql` - SQL generation with skills
- `orchestrator.validate` - SQL validation and security checks
- `orchestrator.await_approval` - HITL approval gate
- `orchestrator.probe_sql` - SQL probing
- `orchestrator.execute` - Query execution
- `orchestrator.validate_results` - Result validation
- `orchestrator.format_results` - Result formatting
- `orchestrator.repair_sql` - SQL repair attempts
- `orchestrator.fallback_sql` - Fallback SQL generation

### 3. LLM Generations
- **SQL Generation**: Model, prompt, output, token usage
- **Intent Classification**: Input query, classified intent
- **Hypothesis Generation**: Query understanding
- **Column Mapping**: Semantic matching results

### 4. Custom Events
- State transitions (RECEIVED, PREPARED, EXECUTING, etc.)
- Approval decisions
- Validation results
- Error events

## Viewing Traces

### Langfuse Cloud Dashboard

1. Go to https://cloud.langfuse.com
2. Navigate to **Traces** tab
3. Filter by:
   - User ID
   - Session ID
   - Tags (e.g., `sql_generation`, `validation`)
   - Time range

### Key Metrics to Monitor

1. **Trace Duration**: End-to-end query processing time
2. **LLM Latency**: Time spent in LLM calls
3. **Token Usage**: Input/output tokens per generation
4. **Error Rate**: Failed traces vs successful
5. **Approval Rate**: Queries requiring HITL approval

## Troubleshooting

### Traces Not Appearing

1. **Check Langfuse is enabled**:
   ```bash
   grep LANGFUSE_ENABLED .env
   # Should show: LANGFUSE_ENABLED=true
   ```

2. **Verify credentials**:
   ```bash
   python scripts/verify_langfuse.py
   ```

3. **Check logs for errors**:
   ```bash
   docker logs bi-agent-backend | grep -i langfuse
   ```

4. **Ensure flush is called**:
   - Traces are batched and flushed periodically
   - Manual flush: `langfuse_client.flush()`
   - Automatic flush on shutdown

### Incomplete Traces

1. **Check span completion**: Ensure all spans call `span.end()`
2. **Verify error handling**: Exceptions should still complete spans
3. **Check trace updates**: Final trace update with output/tags

### High Latency

1. **Async flushing**: Langfuse uses background threads
2. **Batch size**: Adjust flush interval if needed
3. **Network latency**: Check connection to Langfuse host

## Best Practices

### 1. Trace Naming
- Use consistent trace names: `query_orchestration`, `report_generation`
- Include context in metadata, not in trace name

### 2. Span Granularity
- Create spans for logical operations (nodes, LLM calls)
- Don't create spans for trivial operations (<10ms)
- Group related operations under parent spans

### 3. Metadata
- Include relevant context: user role, database type, query intent
- Avoid PII in metadata (use user IDs, not names/emails)
- Keep metadata concise (<1KB per trace)

### 4. Error Handling
- Always complete spans, even on error
- Set span level to ERROR for failures
- Include error message in span output

### 5. Token Tracking
- Log token usage for all LLM calls
- Track costs using Langfuse's cost calculation
- Monitor token usage trends

## Integration Points

### Orchestrator Processor
- Creates trace at query start
- Updates trace with final output
- Flushes on completion

### Orchestrator Nodes
- Each node creates a span
- Spans include input/output data
- Errors are captured and logged

### LLM Middleware
- Wraps all LLM calls
- Logs generation events
- Tracks token usage

### Query State Manager
- Logs state transitions as events
- Tracks query lifecycle

## Performance Impact

- **Overhead**: <5ms per trace (async batching)
- **Network**: Batched uploads every 5 seconds
- **Memory**: ~1MB per 1000 traces (before flush)
- **CPU**: Negligible (<0.1% on background thread)

## Security Considerations

1. **API Keys**: Store in `.env`, never commit to git
2. **PII**: Redact sensitive data before logging
3. **Query Content**: SQL queries are logged (ensure compliance)
4. **User Data**: Use user IDs, not personal information

## Self-Hosted Langfuse

To use self-hosted Langfuse:

1. Deploy Langfuse instance (Docker/Kubernetes)
2. Update `LANGFUSE_HOST` to your instance URL
3. Generate API keys from your instance
4. Test connection with verification script

Example docker-compose for self-hosted:

```yaml
services:
  langfuse:
    image: langfuse/langfuse:latest
    ports:
      - "3000:3000"
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/langfuse
      - NEXTAUTH_SECRET=your-secret
      - NEXTAUTH_URL=http://localhost:3000
```

## Monitoring Langfuse Health

Add to your monitoring:

```python
from app.core.langfuse_client import get_langfuse_client

def check_langfuse_health():
    client = get_langfuse_client()
    if not client:
        return {"status": "disabled"}
    
    try:
        # Test trace creation
        client.trace(
            id="health_check",
            name="health_check",
            input={"test": True}
        )
        client.flush()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## Support

- Langfuse Docs: https://langfuse.com/docs
- Langfuse Discord: https://discord.gg/7NXusRtqYU
- GitHub Issues: https://github.com/langfuse/langfuse
