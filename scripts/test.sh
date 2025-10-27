#!/bin/bash
# Local testing script - runs unit tests

set -e

echo "🧪 Running unit tests..."
cd aether
uv run pytest tests/ -v --cov=aether --cov-report=term-missing

echo "✅ Unit tests passed!"
