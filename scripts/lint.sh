#!/bin/bash
# Local linting script - runs code quality checks

set -e

echo "🔍 Running ruff linting..."
cd aether
uv run ruff check .

echo "🎨 Running ruff formatting check..."
uv run ruff format --check .

echo "✅ Code quality checks passed!"
