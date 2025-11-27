FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --upgrade pip && pip install uv

# Copy dependency files from workspace root
# Build context should be workspace root (nurion/), not aether/
COPY pyproject.toml uv.lock ./
COPY aether/pyproject.toml ./aether/

# Create minimal workspace structure for uv to resolve aether
RUN uv sync --package aether --no-dev --no-install-workspace

# Copy aether source code
COPY aether/ ./aether/

# Install aether package
RUN uv sync --package aether --no-dev

WORKDIR /app/aether

CMD ["uv", "run", "uvicorn", "aether.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
