FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --upgrade pip && pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install dependencies
RUN uv sync --dev

# Copy source code
COPY . .

CMD ["uv", "run", "uvicorn", "aether.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

