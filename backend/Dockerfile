# Stage 1: Builder - Install dependencies using uv
# Using Docker Hub (more accessible in corporate networks than ghcr.io)
FROM python:3.12-slim-bookworm AS builder

# Install uv via pip to avoid ghcr.io DNS issues
RUN pip install uv


# Set environment variables for uv with retry settings for unreliable networks
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_HTTP_TIMEOUT=600 \
    UV_REQUEST_TIMEOUT=300 \
    UV_CONCURRENT_DOWNLOADS=4

WORKDIR /app

# Copy dependency files first for better Docker layer caching
COPY pyproject.toml ./
COPY uv.lock* ./

# Install dependencies with retry logic for unreliable proxy/network
# Clear cache to avoid corrupted downloads, retry up to 3 times
RUN rm -rf /root/.cache/uv && \
    for i in 1 2 3; do \
        echo "Attempt $i: Installing dependencies..." && \
        uv sync --locked --no-install-project --no-dev && break || \
        (echo "Attempt $i failed, clearing cache and retrying..." && rm -rf /root/.cache/uv && sleep 5); \
    done

# Copy application code
COPY . /app

# Install the project and doris-mcp-server with retry logic
RUN rm -rf /root/.cache/uv && \
    for i in 1 2 3; do \
        echo "Attempt $i: Installing project..." && \
        uv sync --locked --no-dev && \
        uv pip install doris-mcp-server && break || \
        (echo "Attempt $i failed, clearing cache and retrying..." && rm -rf /root/.cache/uv && sleep 5); \
    done

# Stage 2: Runtime - Minimal image with Java for SQLcl
FROM python:3.12-slim-bookworm AS runtime

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PATH="/app/.venv/bin:${PATH}"

# Install system dependencies including Java for SQLcl and curl for healthcheck
# Configure apt to bypass proxy caching issues (Hash Sum mismatch workaround)
RUN echo 'Acquire::http::Pipeline-Depth "0";' > /etc/apt/apt.conf.d/99fixbadproxy && \
    echo 'Acquire::http::No-Cache "true";' >> /etc/apt/apt.conf.d/99fixbadproxy && \
    echo 'Acquire::BrokenProxy "true";' >> /etc/apt/apt.conf.d/99fixbadproxy && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Download and install SQLcl (latest version with MCP support)
RUN mkdir -p /opt/sqlcl && \
    curl -L -o /tmp/sqlcl.zip "https://download.oracle.com/otn_software/java/sqldeveloper/sqlcl-latest.zip" && \
    unzip -q /tmp/sqlcl.zip -d /opt && \
    rm /tmp/sqlcl.zip && \
    chmod +x /opt/sqlcl/bin/sql

ENV SQLCL_PATH=/opt/sqlcl/bin/sql
ENV PATH="/opt/sqlcl/bin:${PATH}"

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash --uid 1000 app

# Copy the virtual environment from builder
COPY --from=builder --chown=app:app /app/.venv /app/.venv

# Copy application code from builder
COPY --from=builder --chown=app:app /app /app

# Create necessary directories
RUN mkdir -p /app/data /app/logs && \
    chown -R app:app /app /opt/sqlcl

# Create SQLcl config directory for the app user
RUN mkdir -p /home/app/.dbtools && \
    chown -R app:app /home/app/.dbtools

# Copy entrypoint script
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Switch to non-root user
USER app

# Set home directory for SQLcl connections
ENV HOME=/home/app

# Expose port
EXPOSE 8000

# Health check with longer start period for MCP initialization
HEALTHCHECK --interval=30s --timeout=30s --start-period=120s --retries=5 \
    CMD curl -f http://localhost:8000/health || exit 1

# Command to run the application
ENTRYPOINT ["/docker-entrypoint.sh"]
