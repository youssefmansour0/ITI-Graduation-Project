# Use multi-stage build
FROM python:3.11-slim-bullseye AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    librdkafka-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv

# Make sure we use venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip first
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Production stage
FROM python:3.11-slim-bullseye AS production

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r dataeng && useradd -r -g dataeng -s /bin/bash dataeng

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /app

# Create logs directory structure for Filebeat BEFORE switching user
# This creates the exact directory structure that Filebeat expects
RUN mkdir -p /home/ubuntu/kafka-logs && \
    for i in {0..9}; do mkdir -p /home/ubuntu/kafka-logs/ecommerce-events-$i; done && \
    mkdir -p /app/logs && \
    chown -R dataeng:dataeng /app && \
    chown -R dataeng:dataeng /home/ubuntu/kafka-logs

# Create the ubuntu user directory if it doesn't exist (for compatibility)
RUN mkdir -p /home/ubuntu && chown -R dataeng:dataeng /home/ubuntu

# Copy application files with proper ownership
COPY --chown=dataeng:dataeng data_generator.py /app/
COPY --chown=dataeng:dataeng transaction.avsc /app/

# Switch to non-root user
USER dataeng

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Environment variable for log level (can be overridden at runtime)
ENV LOG_LEVEL=INFO

# Health check - Updated to also check if log directories are writable
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import confluent_kafka; import os; assert os.access('/home/ubuntu/kafka-logs', os.W_OK); print('OK')" || exit 1

# Labels
LABEL maintainer="data-engineering-team" \
      description="E-commerce Data Generator with Confluent Kafka Avro + Filebeat Integration" \
      version="2.2.0" \
      filebeat.enabled="true" \
      filebeat.log.path="/home/ubuntu/kafka-logs"

# Default command
CMD ["python", "data_generator.py"]