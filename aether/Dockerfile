FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --upgrade pip

COPY . .

RUN pip install .[dev] pytest httpx pytest-asyncio

CMD ["uvicorn", "aether.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

