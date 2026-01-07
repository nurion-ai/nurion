# Nurion - AI Agent Guidelines

This document provides project context and development guidelines for AI coding assistants, helping agents understand and contribute to the codebase more effectively.

## Project Overview

**Nurion** is a modern data platform workspace combining orchestration and multimodal data processing capabilities. The name draws from Norse mythology, representing the god of light and wisdom.

### Core Components

| Component | Path | Description |
|-----------|------|-------------|
| **Aether** | `/aether` | FastAPI-driven orchestration service connecting tasks, infrastructure, and data products |
| **Solstice** | `/solstice` | Ray + Spark multimodal data processing framework with high-throughput batch processing and streaming-style execution |

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
3. **Example Workflows**: Reference `solstice/workflows/`

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
2. **Don't create unused APIs**: Only implement endpoints that have actual callers
   - Example: Don't add batch endpoints if the caller only sends single requests
   - Example: Don't add "nice-to-have" endpoints without confirmed use cases
3. **Don't worry about backward compatibility (pre-1.0)**: Before version 1.0, breaking changes are acceptable
   - Focus on getting the design right, not maintaining compatibility
   - Document breaking changes in commit messages
   - After 1.0, maintain backward compatibility
4. **Don't skip types**: Add appropriate type annotations
5. **Don't hardcode config**: Use config classes and environment variables

### Preferred Patterns

1. **Use Protocols over Abstract Classes**: Prefer `typing.Protocol` for structural subtyping
   ```python
   # Good: Protocol (structural)
   from typing import Protocol
   
   class Storage(Protocol):
       def store(self, key: str, value: bytes) -> None: ...
       def get(self, key: str) -> Optional[bytes]: ...
   
   # Avoid: ABC (nominal)
   from abc import ABC, abstractmethod
   
   class Storage(ABC):
       @abstractmethod
       def store(self, key: str, value: bytes) -> None: pass
   ```

2. **Exception Handling**: The lower the layer, the less you should catch exceptions
   ```python
   # Good: Low-level storage - NEVER swallow exceptions
   class JobStorage:
       def get(self, key: str) -> bytes:
           return self.db.get(key)  # Let exceptions propagate
       
       def put(self, key: str, value: bytes) -> None:
           self.db.put(key, value)  # No try/except here
   
   # Good: API layer - let exceptions propagate to FastAPI
   @router.get("/data/{id}")
   async def get_data(id: str):
       data = storage.get(id)  # Let exceptions bubble up
       if not data:
           raise HTTPException(status_code=404, detail="Not found")
       return data
   
   # Good: Background task (top-level loop) - log and continue
   async def background_loop():
       while running:
           try:
               await collect_metrics()
           except Exception:
               logger.exception("Failed to collect metrics")
           await asyncio.sleep(1)
   
   # Avoid: Swallowing exceptions in low-level code
   class BadStorage:
       def get(self, key: str) -> Optional[bytes]:
           try:
               return self.db.get(key)
           except Exception:
               return None  # Bad: Caller can't tell error from missing key
   ```
   
   **Rule of thumb**: Only catch exceptions at the boundary where you can meaningfully handle them (e.g., top-level loops, HTTP handlers). Low-level code should let errors propagate.

3. **Dataclasses over Plain Dicts**: Use `@dataclass` for structured data
4. **Type Hints**: Always include type annotations for better IDE support
5. **Async by Default**: Use async/await for I/O operations

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
| **WebUI Portal** | `solstice/solstice/webui/portal.py` |
| **WebUI Storage** | `solstice/solstice/webui/storage/` |
| **WebUI Collectors** | `solstice/solstice/webui/collectors/` |
| **WebUI API** | `solstice/solstice/webui/api/` |
| **WebUI Templates** | `solstice/solstice/webui/templates/` |
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
from typing import Optional

from solstice.core.operator import Operator
from solstice.core.models import Split, SplitPayload


class MyOperator(Operator):
    def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        # Implement your transform here using the Arrow payload
        if payload is None:
            return None

        table = payload.to_table()
        # TODO: apply transformations on `table`
        return SplitPayload(data=table, split_id=split.split_id)
```

## WebUI - Debugging Interface

Solstice includes a web-based debugging interface for monitoring and analyzing jobs.

### Key Features

1. **Real-Time Monitoring**: Live metrics, progress tracking, resource usage
2. **History Server**: View completed jobs for post-mortem analysis
3. **Multi-Job Support**: Monitor multiple jobs in the same Ray cluster
4. **Comprehensive Metrics**:
   - Stage progress, ETA, throughput, queue lag
   - Partition-level offsets and skew detection
   - Worker resource usage (CPU/Memory/GPU)
   - Split lineage and data flow
   - Exception tracking with root cause hints

### Architecture

**Dual-Mode Design:**
- **Embedded Mode**: WebUI runs with job via Ray Serve (port 8000)
- **History Server**: Standalone service for archived jobs

**Storage Strategy:**
- **Prometheus**: Real-time metrics (1s granularity)
- **SlateDB**: Historical archives (30s snapshots + events)

**Multi-Job Routing:**
```
http://localhost:8000/solstice/          ← Portal (all jobs)
└── /jobs/{job_id}/                      ← Specific job
    ├── /stages/{stage_id}
    ├── /workers/{worker_id}
    └── /lineage
```

### Usage

```python
from solstice.core.job import Job, JobConfig, WebUIConfig

job = Job(
    job_id="my_job",
    config=JobConfig(
        webui=WebUIConfig(
            enabled=True,
            storage_path="s3://bucket/solstice-history/",
            prometheus_enabled=True,
        ),
    ),
)

# Add stages...
runner = job.create_ray_runner()
await runner.run()

# Access: http://localhost:8000/solstice/jobs/my_job/
```

**History Server:**
```bash
solstice history-server -s s3://bucket/solstice-history/ -p 8080
```

### Adding WebUI Features

1. **New API Endpoint**:
   - Routes go in `solstice/webui/api/`
   - Use mode-aware pattern (embedded vs history)
   - Return lightweight data for large datasets

2. **New Collector**:
   - Inherit from base patterns in `solstice/webui/collectors/`
   - Store to SlateDB for history
   - Update at appropriate intervals

3. **New Template**:
   - Extend `base.html` in `solstice/webui/templates/`
   - Use HTMX for dynamic updates
   - Use Alpine.js for interactivity

### Tech Stack

- **Backend**: FastAPI + Ray Serve
- **Frontend**: HTMX + Alpine.js + Jinja2
- **Styling**: Pico CSS (10KB, semantic)
- **Real-Time**: Server-Sent Events (SSE)
- **Storage**: SlateDB (S3-backed) + Prometheus
- **Charts**: Chart.js

### Design Principles

- **Simple & Professional**: No flashy animations
- **High Information Density**: Compact layout for developers
- **Large Dataset Friendly**: Pagination, fixed headers, virtual scrolling
- **Easy to Maintain**: Minimal JavaScript, mostly server-side rendering

## Resources

- **Design Documents**: `solstice/design-docs/`
- **WebUI Design**: `solstice/design-docs/webui.md`
- **WebUI Guide**: `solstice/webui/README.md`
- **README Files**: Root directory and each subproject's README.md
- **Examples**: `solstice/workflows/`, `solstice/examples/webui_demo.py`

---

*Last updated: 2025-01-05*
