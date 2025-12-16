# Solstice

Solstice is a **high-throughput batch processing framework** with a **streaming-style execution model** and built-in multimodal operators.

It is designed for large-scale, production pipelines where:
- You want **streaming-style execution** (no stage-wide barriers, no long tails).
- You need to **fully utilise CPU + GPU** on heterogeneous workloads.
- You process **multimodal data** (video, images, embeddings, text, binary blobs) rather than only tabular data.

## Positioning

Conceptually, Solstice is a **batch processing engine**: jobs are finite DAGs processing finite input data sets.  
Implementation-wise, it uses a **streaming-style, pull-based execution model** inside the job to minimise stage barriers and long-tail latency.

Solstice focuses on **simple, elastic, and observable high-throughput pipelines**, not on being a full analytics platform.

- **Built-in checkpointing & recovery**:  
  - Checkpoints track split state and upstream cursors.  
  - On failure, Solstice restores from checkpoint and continues processing without user-visible complexity.

- **Extreme elasticity with stateless workers**:  
  - All workers are stateless Ray actors; only stage masters own state and checkpoint metadata.  
  - Worker pools can scale up/down dynamically at runtime without stopping the job.  
  - Combined with backpressure, the system can find a good stage-by-stage resource mix automatically, reducing manual tuning.

- **Backpressure by design**:  
  - Downstream stages pull from upstream, so slow consumers naturally throttle producers.  
  - The runtime uses queue metrics and buffer sizes to adapt throughput and avoid overload.

- **Minimal dependencies**:  
  - Runtime only requires **Ray** and an **S3-like object storage** for state and shuffle.  
  - No heavy external services are required to start a pipeline.

- **Streaming-style execution model**:  
  - Stages do not wait for each other to complete; data flows continuously through the DAG.  
  - This avoids classic batch-style stage barriers and long-tail stragglers.

- **External shuffle over high-performance storage**:  
  - Shuffle is implemented over object storage / external systems instead of only Ray’s in-memory object store.  
  - This makes large, multimodal shuffles practical and transparent.

## How Solstice compares

### vs Apache Spark

- Spark is fundamentally a **batch-oriented** system with stage barriers; even in streaming mode, many workloads suffer from **stage wait and long tails**.  
- Solstice is a **batch engine with a streaming-style execution model**:
  - No global stage barriers between operators.
  - Continuous pulling between stages keeps data flowing and avoids long-tail tasks.
  - Better at **saturating CPU + GPU** on pipelines that mix heavy compute with I/O.

### vs Ray Data

- Ray Data is primarily built around **in-memory object store shuffle**:
  - Great for smaller tabular workloads, but costly for **huge multimodal binaries** (e.g. video frames, model inputs).  
- Solstice:
  - Uses **external high-performance storage** (e.g. S3-like) for shuffle and state, not just the Ray object store.  
  - Offers a **transparent, explicit runtime model** (stages, splits, queues, backpressure) instead of opaque auto-tuning knobs.  
  - Works better when your data is large, binary, and long-lived.

### vs Daft

- Daft provides a **DataFrame API** optimised for analytics and table-centric workloads.  
- Solstice intentionally **does not** expose a DataFrame-centric interface:
  - Large-scale, multimodal pipelines do not always benefit from DataFrame abstractions.  
  - Operator-based DAGs (sources / transforms / sinks) map more directly to multimodal processing graphs and model serving pipelines.  
  - This keeps the core minimal while still allowing you to build higher-level APIs on top if needed.

## Non-goals

Solstice is **not** trying to be:

- A **full SQL engine** with complete SQL coverage.
- A **general-purpose DataFrame platform** (like Spark SQL / Pandas / Daft) for interactive analytics.
- A BI / ad-hoc analytics tool.

Instead, it is focused on:

- High-throughput, long-running **batch jobs with streaming-style dataflow**.
- **Multimodal data processing** pipelines.
- Operational simplicity with strong runtime guarantees (checkpointing, backpressure, elasticity).

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
from solstice.operators.sources import LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sinks import FileSink

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
runner = job.create_ray_runner()
runner.run()
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

## Documentation

- `README.md` - High-level overview and usage
- `PROJECT_OVERVIEW.md` - Extended project overview
- `design-docs/` - Architecture and design documents

## Architecture

Solstice is a **batch engine with a streaming-style, pull-based execution model**:
- **StageMaster + stateless workers**: Stage state and checkpoint metadata live in the master; workers are disposable and can scale elastically.
- **Pull-based flow + backpressure**: Downstream stages fetch splits from upstream, naturally throttling producers and avoiding long-tail barriers.
- **Checkpointed recovery**: Checkpoints capture split state and upstream cursors; restore resumes from the last safe point.
- **External shuffle/state**: S3-like object storage carries state and shuffle payloads, not just the Ray object store.

```
┌────────────────────────────────┐
│       Meta Service             │  Job coordinator
└────────────┬───────────────────┘
             │
    ┌────────┼────────┐
    ▼        ▼        ▼
┌────────┐┌────────┐┌────────┐
│ Stage  ││ Stage  ││ Stage  │  Stage masters
│ Master ││ Master ││ Master │
└────┬───┘└────┬───┘└────┬───┘
     │         │         │
   Workers   Workers   Workers  Stateless, elastic
     │         │         │
     └─────────┴─────────┘
             │
    ┌────────┴────────┐
    │ State Backend   │  S3-like / external storage
    └─────────────────┘
```

## Examples

See `workflows/` directory for end-to-end examples:

1. **simple_etl.py**: Basic ETL pipeline
2. **video_slice_workflow.py**: Video processing pipeline

## License

Apache License 2.0
