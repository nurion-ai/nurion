# Aether

FastAPI-based service powering the Nurion data processing platform. Provides task management, Kubernetes integration, and data lake catalog APIs.

## Name origins

The name *Aether* nods to the classical concept of a medium connecting realms—mirroring this service's role as the orchestration layer connecting tasks, infrastructure, and data products across the platform.

## Development setup

1. [Install `uv`](https://docs.astral.sh/uv/getting-started/installation/) if you don't already have it.
2. From the project root, create the local virtual environment (uses the version pinned in `.python-version`):

   ```bash
   uv venv
   ```

3. Activate the environment:

   ```bash
   source .venv/bin/activate
   ```

4. Install all project dependencies:

   ```bash
   uv sync
   ```

5. (Optional) Run the API locally:

   ```bash
   uv run uvicorn aether.app:app --reload
   ```
