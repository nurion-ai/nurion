#!/bin/bash
# Local CI script to run the same checks as GitHub Actions

set -e

echo "🔍 Running ruff linting..."
cd aether
uv run ruff check .

echo "🎨 Running ruff formatting check..."
uv run ruff format --check .

echo "🐳 Building Docker images..."
docker-compose build

echo "🚀 Running integration tests with docker-compose..."
docker-compose up --abort-on-container-exit --exit-code-from tester

echo "🧹 Cleaning up..."
docker-compose down

echo "✅ All checks passed!"
