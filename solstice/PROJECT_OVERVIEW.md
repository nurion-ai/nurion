# Solstice - Project Overview

## What is Solstice?

Solstice is a Ray-based **high-throughput batch processing framework** whose internal execution model is **streaming-style and pull-based**, featuring exactly-once semantics, elastic scaling, and fault tolerance.

## Key Characteristics

- **Hybrid Model**: Batch + Streaming execution
- **Exactly-Once**: Checkpoint-based recovery
- **Elastic**: Dynamic worker scaling
- **Fault-Tolerant**: Automatic recovery from failures
- **Remote State**: S3/DFS/HDFS backends
- **Zero Dependencies**: No Kafka, RocksDB, or ZooKeeper

## Directory Structure

```
solstice/
├── solstice/              # Framework implementation
│   ├── core/              # Job, Stage, Operator abstractions
│   ├── actors/            # Ray actors (Meta Service, Stage Master, Worker)
│   ├── state/             # State management & checkpointing
│   ├── operators/         # Built-in operators
│   └── main.py            # CLI entry point
│
├── workflows/             # Example workflows
│   ├── simple_etl.py      # Basic ETL pipeline
│   └── video_slice_workflow.py  # Video processing pipeline
│
└── design-docs/           # Architecture and design docs
```

## Core Concepts

### 1. Job
A complete processing pipeline with a DAG of stages.

```python
job = Job(job_id='my_pipeline')
```

### 2. Stage
A processing step with an operator and parallelism configuration.

```python
Stage('transform', MapOperator, {...}, parallelism=4)
Stage('scale', MapOperator, {...}, parallelism=(2, 10))
```

### 3. Operator
The logic that processes data for a single split.

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

### 4. State Backend
Where checkpoints are stored (see state backends configured in your Job/runner).

## Built-in Operators

| Operator | Type | Description |
|----------|------|-------------|
| LanceTableSource | Source | Read from Lance tables |
| FileSource | Source | Read from JSON/Parquet/CSV |
| MapOperator | Transform | 1-to-1 transformation |
| FlatMapOperator | Transform | 1-to-N transformation |
| FilterOperator | Transform | Filter records |
| KeyByOperator | Transform | Extract keys |
| FileSink | Sink | Write to files |
| LanceSink | Sink | Write to Lance |
| PrintSink | Sink | Print to stdout |

## Parallelism Modes

### Fixed Parallelism
```python
parallelism=4  # Always 4 workers
```

Use for:
- Source/Sink operations
- Predictable workloads
- When you want exact resource control

### Auto-Scaling Parallelism
```python
parallelism=(2, 10)  # Scale between 2 and 10 workers
```

Use for:
- Variable workloads
- CPU/GPU intensive operations
- When you want optimal resource utilization

## Usage Patterns

### Pattern 1: Simple ETL
```
Source → Transform → Filter → Sink
```

### Pattern 2: Fan-Out
```
Source → FlatMap → [Multiple Workers] → Aggregate → Sink
```

### Pattern 3: Complex Pipeline
```
Source → Preprocess → Classify → Filter → 
  Detect → Extract → PostProcess → Sink
```

## Running the Framework

### On Ray Cluster
```bash
python -m solstice.main \
  --workflow workflows.simple_etl \
  --job-id my_job \
  --ray-address ray://head-node:10001
```

## Checkpointing

### Automatic
```python
job = Job(
    job_id='my_job',
    checkpoint_interval_secs=300,      # Every 5 minutes
    checkpoint_interval_records=10000,  # Or 10k records
)
```

### Manual
```python
runner = job.create_ray_runner()
runner.initialize()
checkpoint_id = runner.trigger_checkpoint()
runner.restore_from_checkpoint(checkpoint_id)
```

## Implementation Stats

- **Python Files**: 22 core + 3 workflows = 25 total
- **Documentation**: 12 markdown files
- **Examples**: 3 (quickstart + 2 workflows)
- **Configurations**: 2 YAML files
- **Lines of Code**: ~4,000+

## Architecture Layers

```
┌─────────────────────────────────────────┐
│ Layer 4: User API                       │
│ (Job, Stage, Operator)                  │
├─────────────────────────────────────────┤
│ Layer 3: Coordination                   │
│ (Meta Service, Global State Master)     │
├─────────────────────────────────────────┤
│ Layer 2: Execution                      │
│ (Stage Master, Workers)                 │
├─────────────────────────────────────────┤
│ Layer 1: State Management               │
│ (State Manager, Checkpoint Coordinator) │
├─────────────────────────────────────────┤
│ Layer 0: Storage                        │
│ (State Backend: S3/DFS/Local)           │
└─────────────────────────────────────────┘
```

## Feature Comparison

| Feature | Solstice | Flink | Spark Streaming |
|---------|----------|-------|-----------------|
| Exactly-Once | ✅ | ✅ | ✅ |
| Dynamic Scaling | ✅ | Limited | Limited |
| No External Deps | ✅ | ❌ | ❌ |
| Lance Integration | ✅ | ❌ | ❌ |
| Python-First | ✅ | ❌ | ✅ |
| State Backend | S3/DFS | RocksDB | HDFS |

## Next Steps

1. **Read**: `README.md`
2. **Explore**: `workflows/` for complete workflows
3. **Dive deeper**: `design-docs/` for architecture details

## Support & Documentation

- **Overview & API**: `README.md`
- **Extended Overview**: `PROJECT_OVERVIEW.md`
- **Architecture**: `design-docs/`

