FROM ubuntu:24.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install system dependencies and Python 3
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Copy dependency files from workspace root
# Build context should be workspace root (nurion/), not aether/
COPY pyproject.toml uv.lock ./
COPY aether/pyproject.toml ./aether/

# Create venv and install dependencies (without workspace package)
RUN uv sync --package aether --no-dev --no-install-workspace

# Copy aether source code
COPY aether/ ./aether/

# Install aether package
RUN uv sync --package aether --no-dev

WORKDIR /app/aether

# Use venv directly to avoid uv sync on every startup
ENV PATH="/app/.venv/bin:$PATH"

# Copy and make entrypoint executable
COPY aether/scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
