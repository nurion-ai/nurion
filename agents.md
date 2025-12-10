# Nurion - AI Agent Guidelines

This document provides project context and development guidelines for AI coding assistants, helping agents understand and contribute to the codebase more effectively.

## Project Overview

**Nurion** is a modern data platform workspace combining orchestration and multimodal data processing capabilities. The name draws from Norse mythology, representing the god of light and wisdom.

### Core Components

| Component | Path | Description |
|-----------|------|-------------|
| **Aether** | `/aether` | FastAPI-driven orchestration service connecting tasks, infrastructure, and data products |
| **Solstice** | `/solstice` | Ray + Spark multimodal data processing framework with streaming and exactly-once semantics |

## Tech Stack

- **Languages**: Python 3.13+, Scala (Spark integration)
- **Runtime**: Ray (distributed computing), Apache Spark
- **API Framework**: FastAPI (Aether)
- **Package Manager**: uv
- **Code Quality**: Ruff (linting + formatting)
- **Testing**: pytest
- **CI/CD**: GitHub Actions

## Project Structure

```
nurion/
├── aether/                  # Orchestration service
│   ├── aether/
│   │   ├── api/routes/      # API routes
│   │   ├── core/            # Core configuration
│   │   ├── models/          # SQLAlchemy models
│   │   ├── schemas/         # Pydantic schemas
│   │   └── services/        # Business logic
│   ├── alembic/             # Database migrations
│   └── tests/
│
├── solstice/                # Data processing framework
│   ├── solstice/
│   │   ├── core/            # Core abstractions (Job, Stage, Operator)
│   │   ├── operators/       # Built-in operators
│   │   │   ├── sources/     # Data sources (Lance, Iceberg, Spark, File)
│   │   │   ├── sinks/       # Data sinks (Lance, File, Print)
│   │   │   ├── map.py       # Transform operators
│   │   │   └── filter.py    # Filter operators
│   │   ├── queue/           # Queue backends (Tansu, Memory)
│   │   └── runtime/         # Ray runtime and autoscaling
│   ├── raydp/               # Spark on Ray integration
│   ├── java/                # Spark Java/Scala components
│   ├── workflows/           # Example workflows
│   ├── tests/
│   └── design-docs/         # Design documents
│
└── scripts/                 # CI/dev scripts
```

## Architecture Core Concepts

### Solstice Streaming Architecture

```
                 +--------------------+
                 |    RayJobRunner    |  Job orchestrator
                 +---------+----------+
                           |
        +------------------+------------------+
        |                  |                  |
+-------v------+   +-------v------+   +-------v------+
| StageMaster  |   | StageMaster  |   | StageMaster  |
| (Source)     |-->| (Transform)  |-->| (Sink)       |
+------+-------+   +------+-------+   +------+-------+
       |                  |                  |
  StageWorkers       StageWorkers       StageWorkers
```

**Key Components**:

1. **Job**: DAG pipeline definition containing multiple Stages
2. **Stage**: Processing step wrapping an Operator with parallelism config
3. **StageMaster**: Manages output queue and worker pool
4. **StageWorker**: Stateless Ray Actor executing Operator logic
5. **Operator**: Data processing logic (Source/Transform/Sink)
6. **Split**: Metadata record representing a unit of work

**Data Flow Model**: Pull-based
- Downstream stages actively pull data from upstream
- Natural backpressure mechanism
- Cursor-based consumption

## Development Guidelines

### Environment Setup

```bash
# Aether (Python 3.13)
cd aether
uv sync --dev

# Solstice (Python 3.12)
cd solstice
uv sync --dev --python 3.12
```

### Code Standards

1. **Commit Messages**: Follow [Conventional Commits](https://conventionalcommits.org/)
   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation update
   - `refactor:` Code refactoring
   - `test:` Test related

2. **Python Style**: Use Ruff for formatting and linting
   ```bash
   # Aether
   cd aether && uv run ruff check .
   cd aether && uv run ruff format --check .

   # Solstice
   cd solstice && uv run ruff check solstice/
   cd solstice && uv run ruff format --check solstice/
   ```

3. **Type Annotations**: Use Python type hints; project is `py.typed`

### Testing

```bash
# Aether tests
cd aether && uv run pytest tests/ -v

# Solstice unit tests (no external dependencies)
cd solstice && uv run pytest tests/ -v --tb=short -m "not integration"

# Solstice integration tests (requires Java 11, Tansu, Aether services)
cd solstice && uv run pytest tests/ -v --tb=short -m "integration"
```

#### Integration Test Prerequisites

For Solstice integration tests, you need:
1. **Java 11**: For Spark components
2. **Tansu**: Message broker (`curl -fsSL https://pub-8bc1f1d3d1984bdfb056d0bc0bf97c3d.r2.dev/tansu/tansu -o /usr/local/bin/tansu && chmod +x /usr/local/bin/tansu`)
3. **Aether services**: `cd aether && docker compose up -d`
4. **RayDP JARs**: `cd solstice/java && mvn clean package -DskipTests -q`

## Agent Working Tips

### When Understanding Code

1. **Design Docs**: Check `/solstice/design-docs/` for architecture decisions
2. **Core Abstractions**: Start with `solstice/core/` to understand the framework
3. **Example Workflows**: Reference `solstice/workflows/` and `quickstart.py`

### When Adding Features

1. **New Operator**: 
   - Inherit from `solstice.core.operator.Operator`
   - Implement `process_split()` method
   - Optionally implement `checkpoint()` and `restore()` for fault tolerance

2. **New Data Source**:
   - Inherit from `solstice.operators.sources.source.SourceOperator`
   - Implement `plan_splits()` to generate initial Splits
   - Register export in `__init__.py`

3. **New API Endpoint** (Aether):
   - Routes go in `aether/api/routes/`
   - Schemas go in `aether/schemas/`
   - Service logic goes in `aether/services/`

### When Debugging

1. **Logging**: Use `create_ray_logger()` to create loggers
2. **Testing**: Prefer writing unit tests to verify logic
3. **Ray Dashboard**: Use Ray Dashboard to monitor Actor states

### Patterns to Avoid

1. **Don't over-engineer**: Keep it simple, only implement current requirements
2. **Don't break existing APIs**: Maintain backward compatibility
3. **Don't skip types**: Add appropriate type annotations
4. **Don't hardcode config**: Use config classes and environment variables

## Key Files Reference

| Purpose | File Path |
|---------|-----------|
| Solstice entry point | `solstice/solstice/main.py` |
| Job definition | `solstice/solstice/core/job.py` |
| Stage definition | `solstice/solstice/core/stage.py` |
| Operator base class | `solstice/solstice/core/operator.py` |
| Stage Master | `solstice/solstice/core/stage_master.py` |
| Stage Worker | `solstice/solstice/core/worker.py` |
| Ray Runner | `solstice/solstice/runtime/ray_runner.py` |
| Queue backends | `solstice/solstice/queue/` |
| Built-in Sources | `solstice/solstice/operators/sources/` |
| Built-in Sinks | `solstice/solstice/operators/sinks/` |
| Aether App | `aether/aether/app.py` |
| Aether Routes | `aether/aether/api/routes/` |

## Common Task Examples

### Creating a Simple Pipeline

```python
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.sources import LanceTableSource
from solstice.operators.map import MapOperator
from solstice.operators.sinks import FileSink

job = Job(job_id='my_pipeline')

job.add_stage(Stage(
    'source',
    LanceTableSource,
    {'table_path': '/data/input'},
    parallelism=1,
))

job.add_stage(Stage(
    'transform',
    MapOperator,
    {'map_fn': lambda x: x.upper()},
    parallelism=(2, 8),  # Auto-scale 2-8 workers
), upstream_stages=['source'])

job.add_stage(Stage(
    'sink',
    FileSink,
    {'output_path': '/data/output.json'},
    parallelism=1,
), upstream_stages=['transform'])

runner = job.create_ray_runner()
runner.run()
```

### Custom Operator

```python
from solstice.core.operator import Operator
from solstice.core.models import Record

class MyOperator(Operator):
    def process(self, record: Record):
        result = transform(record.value)
        return [Record(key=record.key, value=result)]
    
    def checkpoint(self):
        return {'state': self.internal_state}
    
    def restore(self, state):
        self.internal_state = state['state']
```

## Resources

- **Design Documents**: `solstice/design-docs/`
- **README Files**: Root directory and each subproject's README.md
- **Examples**: `solstice/workflows/`, `solstice/quickstart.py`

---

*Last updated: 2025-12-10*
