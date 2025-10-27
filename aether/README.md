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

## CI/CD

This project uses GitHub Actions for continuous integration with three separate jobs:

### PR Title Check (`pr-title-check` job)
- **Title format validation**: Ensures PR titles follow [Conventional Commits](https://conventionalcommits.org/) specification
- **Required format**: `<type>: <description>` (e.g., `feat: add user authentication`)
- **Standard types**: feat, fix, docs, style, refactor, perf, test, build, ci, chore, revert

### Code Quality Check (`lint` job)
- **Ruff linting**: Code quality and style enforcement
- **Ruff formatting**: Code formatting consistency

### Unit Tests (`test` job)
- **Unit tests**: Test execution with coverage reporting
- **Coverage upload**: Automatic coverage reporting to Codecov

### Running CI checks locally

You can run the same checks locally using the provided scripts:

```bash
# Run all checks (equivalent to both GitHub Actions jobs)
./scripts/ci.sh

# Run only code quality checks
./scripts/lint.sh

# Run only unit tests
./scripts/test.sh
```

Or run individual commands:

```bash
# Linting only
cd aether && uv run ruff check .

# Formatting check only
cd aether && uv run ruff format --check .

# Tests with coverage only
cd aether && uv run pytest tests/ -v --cov=aether --cov-report=term-missing
```

## Pull Request Guidelines

### PR Title Format

All pull requests must follow the [Conventional Commits](https://conventionalcommits.org/) specification:

**Format**: `<type>: <description>`

**Standard Types** (as per Conventional Commits spec):
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies
- `ci`: Changes to our CI configuration files and scripts
- `chore`: Other changes that don't modify src or test files
- `revert`: Reverts a previous commit

**Examples**:
- ✅ `feat: add user authentication system`
- ✅ `fix: resolve memory leak in data processing`
- ✅ `docs: update API documentation`
- ✅ `refactor: simplify database connection logic`
- ❌ `Add user authentication` (missing type)
- ❌ `FEAT: Add user authentication` (uppercase type)
- ❌ `feat:Add user authentication` (missing space)

**Note**: The description should be lowercase and descriptive. Avoid starting with uppercase letters.
