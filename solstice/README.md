# Solstice

A unified platform for Apache Spark on Ray, data processing, and distributed streaming.

## Components

- **solstice/**: Core streaming framework - Ray-based distributed streaming with exactly-once semantics
- **raydp/**: Run Spark on Ray with distributed execution  
- **java/**: Java components for Spark integration
- **workflows/**: Example streaming workflows (ETL, video processing, etc.)

## Quick Start

### Prerequisites

```bash
# Install from pyproject.toml
cd /path/to/nurion/solstice
pip install -e .

# Or install dependencies directly
pip install "ray[default]" pyarrow click "fsspec[s3]"

# Optional: Lance table support
pip install pylance
```

### Running from the solstice/ Directory

**Important**: All commands should be run from the `solstice/` directory:

```bash
cd /path/to/nurion/solstice
```

### Quickstart Example

```bash
python quickstart.py
```

This runs a simple number processing pipeline demonstrating all core features.

### Run Workflows with CLI Parameters

All workflow configuration is in Python code with sensible defaults.
Override any parameter via CLI:

```bash
# Simple ETL workflow
python -m solstice.main \
  --workflow workflows.simple_etl \
  --job-id etl_001 \
  --input /data/lance_table \
  --output /data/output.json \
  --transform-parallelism 4

# Video processing workflow
python -m solstice.main \
  --workflow workflows.video_processing \
  --job-id video_001 \
  --input /data/video_metadata \
  --output /data/processed.json \
  --classify-parallelism 8 \
  --scenes-parallelism 16 \
  --features-parallelism 4 \
  --features-gpus 1
```

### Python API

```python
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.source import LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sink import FileSink

# Create a job
job = Job(job_id='my_pipeline')

# Add stages with parallelism configuration
job.add_stage(Stage(
    'source',
    LanceTableSource,
    {'table_path': '/data/input'},
    parallelism=1,  # Fixed 1 worker
))

job.add_stage(Stage(
    'transform',
    MapOperator,
    {'map_fn': my_transform},
    parallelism=(2, 8),  # Auto-scale 2-8 workers
), upstream_stages=['source'])

job.add_stage(Stage(
    'sink',
    FileSink,
    {'output_path': '/data/output.json'},
    parallelism=1,
), upstream_stages=['transform'])

# Run
job.initialize()
job.start()
```

## Key Features

✅ **Exactly-Once Semantics**: Checkpoint-based fault tolerance  
✅ **Elastic Scaling**: Auto-scale workers based on load  
✅ **Backpressure**: Automatic rate adaptation  
✅ **Remote State**: S3 backend (no local disk required)  
✅ **DAG Pipelines**: Complex multi-stage workflows  
✅ **Zero Config Files**: All configuration in Python code  
✅ **CLI Override**: Override any parameter from command line

## Configuration Philosophy

**No YAML files needed!** All configuration is in your workflow Python code with sensible defaults:

```python
def create_job(job_id, config, state_backend):
    # Defaults defined here
    input_path = config.get('input')
    parallelism = config.get('parallelism', (2, 8))
    
    # Build job with config
    job = Job(job_id=job_id, ...)
    job.add_stage(Stage('process', Op, parallelism=parallelism))
    return job
```

Override from CLI:
```bash
python -m solstice.main \
  --workflow workflows.my_workflow \
  --input /my/data \
  --parallelism 16
```

## State Backends

### Local (for testing)

```bash
python -m solstice.main \
  --workflow workflows.simple_etl \
  --state-backend local \
  --state-path /tmp/checkpoints \
  --input /data/input \
  --output /data/output.json
```

### S3 (production)

```bash
python -m solstice.main \
  --workflow workflows.video_processing \
  --state-backend s3 \
  --state-path my-bucket \
  --state-prefix checkpoints/video \
  --input s3://data/videos \
  --output s3://data/processed
```

## Operators

### Built-in Operators

#### Sources
- `LanceTableSource`: Read from Lance tables
- `FileSource`: Read from JSON/Parquet/CSV files

#### Transformations
- `MapOperator`: 1-to-1 transformations
- `FlatMapOperator`: 1-to-N transformations  
- `FilterOperator`: Filter records
- `KeyByOperator`: Extract/assign keys

#### Sinks
- `FileSink`: Write to JSON/Parquet/CSV
- `LanceSink`: Write to Lance tables
- `PrintSink`: Print to stdout (for debugging)

### Custom Operators

```python
from solstice.core.operator import Operator
from solstice.core.models import Record

class MyOperator(Operator):
    def process(self, record: Record):
        # Transform record
        result = do_something(record.value)
        return [Record(key=record.key, value=result)]
    
    def checkpoint(self):
        # Return state for checkpointing
        return {'my_state': self.state}
    
    def restore(self, state):
        # Restore from checkpoint
        self.state = state['my_state']
```

## Documentation

- `solstice/README.md` - Framework architecture
- `EXAMPLES.md` - Usage examples
- `PROJECT_OVERVIEW.md` - Project overview
- `COMPLETION_REPORT.md` - Implementation details

## Architecture

```
┌────────────────────────────────┐
│       Meta Service             │  Job coordinator
└────────────┬───────────────────┘
             │
    ┌────────┼────────┐
    ▼        ▼        ▼
┌────────┐┌────────┐┌────────┐
│ Stage  ││ Stage  ││ Stage  │  Stage managers
│ Master ││ Master ││ Master │
└────┬───┘└────┬───┘└────┬───┘
     │         │         │
   Workers   Workers   Workers  Data processing
     │         │         │
     └─────────┴─────────┘
             │
    ┌────────┴────────┐
    │ State Backend   │  S3 storage
    └─────────────────┘
```

## Examples

See `workflows/` directory:

1. **simple_etl.py**: Basic ETL pipeline
2. **video_processing.py**: Complex video processing pipeline
3. **quickstart.py**: Minimal example

## License

Apache License 2.0
