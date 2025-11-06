# Solstice Streaming Examples

This directory contains example workflows and configurations for Solstice Streaming.

## Examples

### 1. Simple ETL Pipeline

A basic ETL (Extract, Transform, Load) pipeline demonstrating:
- Reading from Lance tables
- Transforming records
- Filtering data
- Writing to output files

**Workflow**: `workflows/simple_etl.py`
**Config**: `configs/simple_etl.yaml`

**Run it:**
```bash
python -m solstice.solstice.main \
  --config solstice/solstice/examples/configs/simple_etl.yaml \
  --workflow solstice.solstice.examples.workflows.simple_etl \
  --job-id my_etl_job_001
```

### 2. Video Processing Pipeline

A more complex pipeline inspired by the fusionflow blueprint, demonstrating:
- Reading video metadata from Lance tables
- Metadata classification and filtering
- Scene detection (one-to-many transformation)
- Feature extraction with GPU support
- Writing results to Lance tables

**Workflow**: `workflows/video_processing.py`
**Config**: `configs/video_processing.yaml`

**Run it:**
```bash
python -m solstice.solstice.main \
  --config solstice/solstice/examples/configs/video_processing.yaml \
  --workflow solstice.solstice.examples.workflows.video_processing \
  --job-id video_processing_001
```

## Key Features Demonstrated

### Checkpointing and Fault Tolerance
```bash
# Run with automatic checkpointing (configured in YAML)
python -m solstice.solstice.main --config config.yaml --workflow my_workflow

# Restore from a checkpoint
python -m solstice.solstice.main \
  --config config.yaml \
  --workflow my_workflow \
  --restore-from checkpoint_1234567890_abcd1234
```

### State Backends

**Local (for testing):**
```yaml
state_backend:
  type: "local"
  base_path: "/tmp/solstice/checkpoints"
```

**S3 (for production):**
```yaml
state_backend:
  type: "s3"
  bucket: "my-bucket"
  prefix: "checkpoints"
```

**DFS/HDFS:**
```yaml
state_backend:
  type: "dfs"
  base_path: "hdfs://namenode:9000/solstice/checkpoints"
```

### Dynamic Scaling

Configure min/max parallelism for each stage:
```python
stage = Stage(
    stage_id='process',
    operator_class=MyOperator,
    parallelism=4,        # Initial workers
    min_parallelism=2,    # Minimum workers
    max_parallelism=10,   # Maximum workers
)
```

### Resource Requirements

Specify CPU, GPU, and memory for workers:
```python
stage = Stage(
    stage_id='gpu_inference',
    operator_class=InferenceOperator,
    worker_resources={
        'num_cpus': 2,
        'num_gpus': 1,
        'memory': 8 * 1024**3,  # 8GB
    },
)
```

## Creating Custom Workflows

To create your own workflow:

1. Create a new Python file in `workflows/`
2. Define your operators (or use built-in ones)
3. Implement a `create_job()` function:

```python
from solstice.core.job import Job
from solstice.core.stage import Stage

def create_job(job_id, config, state_backend):
    job = Job(
        job_id=job_id,
        state_backend=state_backend,
        checkpoint_interval_secs=300,
    )
    
    # Add stages
    source_stage = Stage(...)
    transform_stage = Stage(...)
    sink_stage = Stage(...)
    
    # Build DAG
    job.add_stage(source_stage)
    job.add_stage(transform_stage, upstream_stages=['source'])
    job.add_stage(sink_stage, upstream_stages=['transform'])
    
    return job
```

4. Create a configuration YAML file in `configs/`
5. Run your workflow:

```bash
python -m solstice.solstice.main \
  --config your_config.yaml \
  --workflow your_workflow \
  --job-id your_job_id
```

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                        Meta Service                          │
│  (Job DAG Management, Global Coordination)                   │
└──────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│ Stage Master  │     │ Stage Master  │     │ Stage Master  │
│   (Stage 1)   │────▶│   (Stage 2)   │────▶│   (Stage 3)   │
└───────────────┘     └───────────────┘     └───────────────┘
        │                     │                     │
   ┌────┴────┐           ┌────┴────┐           ┌────┴────┐
   ▼         ▼           ▼         ▼           ▼         ▼
┌──────┐ ┌──────┐    ┌──────┐ ┌──────┐    ┌──────┐ ┌──────┐
│Worker│ │Worker│    │Worker│ │Worker│    │Worker│ │Worker│
└──────┘ └──────┘    └──────┘ └──────┘    └──────┘ └──────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │ Global State Master    │
                  │ (Checkpoint Coordinator)│
                  └────────────────────────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │   State Backend        │
                  │   (S3 / DFS / Local)   │
                  └────────────────────────┘
```

## Monitoring and Debugging

Enable debug logging:
```bash
python -m solstice.solstice.main \
  --config config.yaml \
  --workflow my_workflow \
  --log-level DEBUG
```

Check job status programmatically:
```python
status = job.get_status()
metrics = job.get_metrics()
checkpoints = job.list_checkpoints()
```

## Performance Tuning

1. **Batch Size**: Adjust `batch_size` in source and sink operators
2. **Parallelism**: Tune worker counts per stage based on bottlenecks
3. **Checkpoint Interval**: Balance recovery time vs. overhead
4. **Buffer Size**: Adjust queue sizes for backpressure management
5. **Resource Allocation**: Match worker resources to operator needs

## Best Practices

1. **Idempotent Operations**: Design operators to be idempotent for exactly-once semantics
2. **Error Handling**: Use `skip_on_error` option or implement proper error handling
3. **Checkpointing**: Checkpoint frequently enough for recovery but not so often it impacts performance
4. **Monitoring**: Track metrics to identify bottlenecks and optimize
5. **Testing**: Start with local backend and small data before scaling up

