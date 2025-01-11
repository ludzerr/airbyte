FROM python:3.9-slim

# Maintainer info
LABEL maintainer="Ludovic Berset <ludo.berset@bluewin.ch>"

# Set environment variables
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENV PYTHONUNBUFFERED=1
ENV KAGGLE_CONFIG_DIR=/airbyte/integration_code/secrets

# Create directory for the app
WORKDIR /airbyte/integration_code

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry==1.7.1

# Copy Poetry files
COPY pyproject.toml poetry.lock ./

# Configure Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy source code
COPY . .

# Install the connector
RUN poetry install --no-interaction --no-ansi

# Create directory for Kaggle config and set permissions
RUN mkdir -p /airbyte/integration_code/secrets && \
    chmod 700 /airbyte/integration_code/secrets

# Set the entrypoint
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]