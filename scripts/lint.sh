#!/bin/bash
# Local linting script - runs code quality checks

set -e

echo "📜 Checking license headers..."
python3 scripts/check_license_headers.py

echo "🔍 Running ruff linting..."
cd aether
uv run ruff check .

echo "🎨 Running ruff formatting check..."
uv run ruff format --check .

echo "✅ Code quality checks passed!"
