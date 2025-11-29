#!/bin/bash
set -e

echo "Running database migrations..."
alembic upgrade head

echo "Starting uvicorn server..."
exec uvicorn aether.app:create_app --factory --host 0.0.0.0 --port 8000

