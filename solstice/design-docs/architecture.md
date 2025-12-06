# Solstice Runtime Architecture

## Overview
Solstice implements a distributed, streaming dataflow engine on top of Ray actors. Workflows are expressed as directed acyclic graphs (DAGs) of stages. Each stage owns a user-defined operator and a pool of stateless `StageWorker` actors, while the stage master manages split-scoped state and checkpointing. Stages exchange *Splits*, which are metadata records describing batches of data pointed to by Ray object references. This orchestration keeps hot data off the control plane and allows the pipeline to scale horizontally across workers while maintaining exactly-once semantics.

```
       +-------------+       +-------------+       +-------------+
       | SourceStage |  -->  |  MapStage   |  -->  | SinkStage   |
       +-------------+       +-------------+       +-------------+
             |                     |                      |
   (read, produce batches)   (process splits)      (consume splits)
       Split + BatchRef         Split + BatchRef        Split + BatchRef
```

## Data Flow Model: Pull-Based Architecture

Solstice uses a **Pull-based** data flow model where downstream stages actively pull data from upstream stages. This design provides natural backpressure and reduces coupling between stages.

### Key Characteristics

1. **Downstream pulls from upstream**: Each stage maintains an `OutputBuffer` containing completed splits. Downstream stages call `fetch_splits()` to retrieve data.

2. **Natural backpressure**: If a downstream stage is slow, it simply pulls less frequently. The upstream's output buffer fills up, and the upstream stage naturally slows down when its buffer is full.

3. **Single-direction dependency**: Downstream stages know about their upstreams (to pull from them), but upstream stages don't need to know about their downstreams. This simplifies DAG modifications.

4. **Cursor-based consumption**: Each consumer maintains a cursor tracking its read position, enabling multiple downstreams to consume at different rates.

```
Pull-Based Data Flow:

Source.output_buffer <── fetch_splits() ── Processor.output_buffer <── fetch_splits() ── Sink
                     (cursor-based pull)                          (cursor-based pull)
```

## Components

### Job Definition
* `Job`: Declarative DAG specification. Tracks stages, edges, and state backend configuration.
* `Stage`: Wraps an operator class, parallelism configuration, and resource requirements.
* `Split`: Control-plane record representing a unit of work (batch metadata, lineage, status).

### Runtime
* `RayJobRunner`: Orchestrates the execution lifecycle. Responsibilities:
  - Initialise Ray services (`MetaService`, `StageMasterActor`).
  - Configure upstream references for each stage (enabling pull-based data flow).
  - Monitor stage counters to detect when the DAG is quiescent, trigger checkpoints, and collect metrics.

* `StageMasterActor`: Manages the split queues, per-split state, output buffer, and a pool of StageWorkers. Functions:
  - **Pull from upstream**: Actively fetches splits from upstream stages via `fetch_splits()`.
  - **Process splits**: Schedule splits on available workers (`process_split`) and track inflight work.
  - **Buffer outputs**: Write completed splits to `OutputBuffer` for downstream consumption.
  - **Serve downstream pulls**: Expose `fetch_splits()` for downstream stages to pull data.
  - Provide per-stage metrics and queue counters for lifecycle decisions.

* `OutputBuffer`: Thread-safe buffer for completed splits with cursor-based consumption:
  - Bounded size with configurable max capacity.
  - Multiple consumers with independent cursors.
  - Slow consumer detection.
  - Automatic GC of consumed splits.

* `StageWorker`: Executes the user operator over batches without retaining persistent state. Responsibilities:
  - Materialise Ray batch references and invoke `process_split`.
  - Produce output batches and return Ray references alongside operator metrics.

### State & Checkpointing
* `CheckpointManager`: Coordinates checkpoint triggers, collects stage checkpoint data, and orchestrates restore.
* `StageCheckpointTracker`: Lives with each stage master, tracking completed/inflight splits.
* `StageCheckpointData`: Captures completed splits, inflight splits, and upstream cursor positions for restoration.
* Checkpoints include upstream cursors, enabling precise resume from the last processed position.

### Control Plane Services
* `MetaService`: Maintains the DAG topology, stage metadata, and global job status. Handles stage registration and metrics aggregation.

## Dataflow

### 1. Source Ingestion
- Source stages generate splits internally (via `SourceOperator.plan_splits()` or similar).
- Splits are enqueued to the stage's pending queue and processed by workers.
- Completed splits are written to the source stage's `OutputBuffer`.

### 2. Stage Processing (Pull-Based)
- Each stage's run loop actively pulls from its upstream stages:
  ```python
  # Pseudocode for stage run loop
  while running:
      # Pull from all upstream stages
      for upstream in upstream_stage_refs:
          splits, cursor, finished = upstream.fetch_splits(cursor)
          pending_splits.extend(splits)
      
      # Schedule pending splits to workers
      schedule_pending_splits()
      
      # Drain completed results into output buffer
      drain_completed_results()
  ```
- Workers process splits and return results with output payload references.
- Completed splits are buffered in `OutputBuffer` for downstream consumption.

### 3. Output Buffering & Consumption
- Each stage maintains an `OutputBuffer` containing completed splits.
- Downstream stages call `fetch_splits(consumer_id, cursor)` to retrieve new splits.
- The buffer tracks each consumer's cursor independently, supporting fan-out to multiple downstreams.
- Splits are GC'd only after all registered consumers have fetched them.

### 4. Completion Detection
- A stage is idle when:
  - All upstreams have marked themselves as finished.
  - No pending splits remain.
  - No inflight results remain.
- When all stages are idle, the runner stops the job.

## Scheduling & Backpressure

### Natural Backpressure (Pull Model)
* **Buffer-based throttling**: When a stage's output buffer is full, `append()` returns false, and the stage waits for downstream to consume.
* **No explicit backpressure signals needed**: Downstream controls the flow rate by its pull frequency.
* **Per-consumer tracking**: Slow consumers can be detected and handled (warning, disconnection, etc.).

### Worker Scheduling
* Each stage master enforces per-worker concurrency limits (`max_active_splits_per_worker`).
* Workers are selected based on current load (least-loaded first).
* `max_queue_size` controls the input pending queue size.

## Elasticity
* `StageMasterActor.scale_workers()` adjusts worker pool size according to load.
* Workers can be added/removed without stopping the job; new workers immediately begin processing enqueued splits.
* When scaling in, idle workers are shut down gracefully; inflight splits are re-queued if necessary.

## Fault Tolerance
1. Runner triggers checkpoint (periodic or manual).
2. Each stage prepares checkpoint data including:
   - Completed splits
   - Inflight splits
   - Upstream cursor positions
3. Checkpoint manifest persisted via the configured backend (e.g., SlateDB, S3).
4. On failure, the runner:
   - Recreates stage masters and workers.
   - Restores checkpointed state including cursor positions.
   - Resumes pulling from the last checkpointed cursor position.

## CLI Lifecycle
1. Create `Job` via workflow module.
2. Build `RayJobRunner`.
3. `runner.run()`:
   - Initialize stages and configure upstream references.
   - Start stage run loops (each stage pulls from its upstreams).
   - Monitor until all stages are idle.
   - Stop job and report status/metrics.
4. `runner.shutdown()` cleans up actors and Ray services.

## ASCII Architecture Diagram
```
             +--------------------+
             |    RayJobRunner    |
             |--------------------|
             | - MetaService      |
             | - CheckpointMgr    |
             +---------+----------+
                       |
        configure_upstream (downward arrows show pull direction)
                       |
      +----------------+----------------+
      |                                 |
+-----v----+                      +-----v----+
| Stage A  |                      | Stage B  |
| Master   |<─── fetch_splits ────| Master   |
| (Source) |                      | (Map)    |
+----+-----+                      +----+-----+
     |                                 |
  Workers                        fetch_splits
     |                                 |
+----v----------+               +------v---------+
| StageWorker A |               | Stage C Master |<── fetch_splits ── Sink
+---------------+               +----------------+
     |                                 |
output_buffer                    output_buffer
```

## Configuration

### StageMasterConfig Options
* `max_split_attempts`: Maximum retry attempts for failed splits (default: 3)
* `max_active_splits_per_worker`: Concurrency limit per worker (default: 100)
* `max_queue_size`: Maximum pending split queue size (default: 1000)
* `max_output_buffer_size`: Maximum output buffer size (default: 1000)
* `max_consumer_lag`: Maximum allowed lag before slow consumer warning (default: 500)
* `fetch_batch_size`: Number of splits to fetch per pull request (default: 100)
* `fetch_timeout`: Timeout for upstream fetch calls (default: 1.0s)
* `fail_fast`: Stop immediately on exception (default: true)

## Known Gaps & Issues
* **Iceberg ingestion is not streaming**: `IcebergSource.read()` materialises `scan.to_arrow()` up front, so multi-billion-row tables will not fit in memory and cannot be processed incrementally without configuring `batch_size` or refactoring to iterate scan tasks.
* **Split metadata lacks resume coordinates**: `Split.data_range` contains only upstream stage and batch IDs. Without file paths, fragment IDs, or offsets, precise replay/checkpoint alignment is difficult after failure.
* **Buffer persistence**: Output buffers are in-memory; if a stage restarts, buffered splits are lost. Downstream stages need to re-pull from source or checkpoint.

## Future Improvements
* Add long-polling support for reduced latency (upstream waits briefly for new data before returning empty).
* Implement buffer persistence for fault tolerance.
* Add adaptive batch sizing based on throughput metrics.
* Explore auto-scaling policies driven by observed processing rates or backlog sizes.
* Extend checkpointing to support partial DAG snapshots and rolling restores.

---
*Last updated: 2025-12-05*
