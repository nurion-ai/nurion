# Solstice Runtime Architecture

## Overview
Solstice implements a distributed, streaming dataflow engine on top of Ray actors. Workflows are expressed as directed acyclic graphs (DAGs) of stages. Each stage owns a user-defined operator and a pool of stateful worker actors. Stages exchange *Splits*, which are metadata records describing batches of data pointed to by Ray object references. This orchestration keeps hot data off the control plane and allows the pipeline to scale horizontally across workers while maintaining exactly-once semantics.

```
       +-------------+       +-------------+       +-------------+
       | SourceStage |  -->  |  MapStage   |  -->  | SinkStage   |
       +-------------+       +-------------+       +-------------+
             |                     |                      |
   (read, produce batches)   (process splits)      (consume splits)
       Split + BatchRef         Split + BatchRef        Split + BatchRef
```

## Components
### Job Definition
* `Job`: Declarative DAG specification. Tracks stages, edges, and state backend configuration.
* `Stage`: Wraps an operator class, parallelism configuration, and resource requirements.
* `Split`: Control-plane record representing a unit of work (batch metadata, lineage, status).

### Runtime
* `RayJobRunner`: Orchestrates the execution lifecycle. Responsibilities:
  - Initialise Ray services (`MetaService`, `GlobalStateMaster`, `StageMasterActor`).
  - Seed source data by streaming records from source operators and enqueuing splits.
  - Drive the pipeline by pulling output splits from upstream stages and pushing to downstream stages.
  - Monitor stage counters to detect when the DAG is quiescent, trigger checkpoints, and collect metrics.
  - Apply backpressure to source ingestion via a configurable `source_pending_limit`, ensuring the Ray object store is not overrun.
* `StageMasterActor`: Manages a stage-local queue of splits and a pool of worker actors. Functions:
  - Schedule splits on available workers (`process_split`) and track inflight work.
  - Fan-out completed splits directly to downstream stage masters using shared Ray object references, buffering locally only for sink stages.
  - Provide per-stage metrics and queue counters for backpressure and lifecycle decisions.
* `WorkerActor`: Executes the user operator over batches. Responsibilities:
  - Materialise Ray batch references and invoke `process_batch`.
  - Maintain operator state via `StateManager`.
  - Produce output batches and return Ray references alongside operator metrics.

### State & Checkpointing
* `GlobalStateMaster`: Coordinates checkpoint barriers, collects split-level handles, and orchestrates restore.
* `StateManager`: Maintains per-split and operator state within worker actors, emitting checkpoint handles on demand.
* Checkpoints capture lineage and offsets per split. Restoration rehydrates stage masters and workers using these handles.

### Control Plane Services
* `MetaService`: Maintains the DAG topology, stage metadata, and global job status. Handles stage registration and metrics aggregation.
* `GlobalStateMaster`: (As above) orchestrates checkpoint lifecycle.

## Dataflow
1. **Source ingestion**
   - `RayJobRunner` iterates source operators and enqueues splits with batch references onto downstream stage masters.
   - Splits carry only metadata; batches remain in Ray’s object store, referenced by ID, preventing large-scale copies.

2. **Stage processing**
   - Stage masters maintain pending split queues. Workers pull splits, materialise the batch via Ray, and run operators.
   - Output batches are `with_split`-tagged with downstream split IDs and re-put into the object store.
   - Stage masters push completion records (split + batch ref) to downstream stages or the runner.

3. **Fan-out / Shuffle**
   - When a stage completes a split, it clones metadata per downstream edge while *reusing the same batch reference*; this minimises duplication and allows Ray to handle zero-copy broadcast to multiple stages.
   - Downstream stage masters enqueue the split ref and mutate only metadata, keeping network cost low.

4. **Completion detection**
   - Stage masters expose queue counters (pending, active, inflight, output). The runner polls these; when all are zero and no workers are active, the pipeline is idle.
   - The runner then stops the job, gathers final metrics, and returns control to the CLI.

## Large Table Ingestion (Lance/Iceberg)
1. **Source scanning**
   - `LanceTableSource.read()` opens a `LanceDataset` scanner and streams `pyarrow.RecordBatch` objects. Each batch is converted to a `Batch` via `ArrowStreamingSource._emit_table()`, which preserves Arrow payloads and optionally re-chunks them using the configured `batch_size`.
   - `IcebergSource.read()` evaluates `table.scan().to_arrow()`, producing a single `pyarrow.Table`. `_emit_table()` slices this table into batches; without an explicit `batch_size`, the full snapshot becomes one batch, which is unsafe for very large tables.
2. **Split creation**
   - The driver-side `RayJobRunner._seed_sources()` consumes each emitted `Batch`, waits on `_source_pending_limit` (`64` default) to avoid flooding, and stores the payload once in the Ray object store via `ray.put`.
   - For every downstream stage, the runner instantiates a `Split` referencing the upstream stage and batch metadata (`batch_id`, `record_count`) and pushes it to the target `StageMasterActor.enqueue_split()`. The `data_range` currently holds only coarse identifiers, not dataset offsets.
3. **Stage scheduling**
   - Each stage master maintains queues (`pending_splits`, `active_splits`, `inflight_results`) and uses `assign_work()` to dispatch to workers with spare capacity (`max_active_splits_per_worker` default `2`).
   - The payload reference is reused across downstream stages; Ray ensures deduplicated transfer while stage masters track logical ownership.
4. **Worker execution**
   - `WorkerActor.process_split()` dereferences the batch, normalises metadata (`batch_id`, `split_id`), activates state via `StateManager`, and runs `operator.process_batch()`. Operators return a `Batch` (or `None`), which is re-materialised into Ray’s object store when present.
5. **Downstream propagation**
   - The stage master run loop (`StageMasterActor.run()`) continuously waits for completion, increments metrics, and clones metadata per downstream edge via `Split.with_output()`, forwarding the same `payload_ref` downstream.
   - Stages without downstream edges buffer outputs in `output_queue` for sinks, test harnesses, or external consumers to retrieve.
6. **Object lifecycle & throttling**
   - Completed splits release their local references (`_release_split()`), allowing Ray to reclaim payloads once all consumers finish.
   - Backpressure flips `backpressure_active` when `pending_splits` exceeds `max_queue_size` (`1000`), but upstream throttling currently depends on the runner’s `_source_pending_limit` loop.

## Scheduling & Backpressure
* Each stage master enforces per-worker concurrency limits and tracks occupancy.
* Pending queue size triggers backpressure flags; `MetaService` may propagate slow-down signals upstream (hook for future dynamic throttling).
* `StageMasterActor.run()` is an asynchronous loop (`max_concurrency=16`) that assigns pending splits, awaits completions via `collect_ready_results()`, and immediately forwards payload refs downstream.
* `RayJobRunner` now seeds sources, monitors stage health, and detects completion; fine-grained scheduling and fan-out happen inside the per-stage run loops, eliminating the central polling bottleneck.

## Elasticity
* `StageMasterActor.scale_workers()` adjusts worker pool size according to load.
* Workers can be added/removed without stopping the job; new workers immediately begin processing enqueued splits.
* When scaling in, idle workers are shut down gracefully; inflight splits are re-queued if necessary.

## Fault Tolerance
1. Runner triggers checkpoint (periodic or manual).
2. `GlobalStateMaster` sends barriers to stage masters; workers snapshot state and return handles.
3. Checkpoint manifest persisted via the configured backend (e.g. S3).
4. On failure, the runner:
   - Recreates stage masters and workers.
   - Restores checkpointed splits via `restore_from_checkpoint`.
   - Rehydrates operator state before resuming from the last consistent point.

## CLI Lifecycle
1. Create `Job` via workflow module.
2. Build `RayJobRunner`.
3. `runner.run()`:
   - Initialise + start job.
   - Seed sources and execute until idle.
   - Stop job and report status/metrics.
4. `runner.shutdown()` cleans up actors and Ray services.

## ASCII Architecture Diagram
```
             +--------------------+
             |      RayJobRunner |
             |--------------------|
             | - MetaService      |
             | - GlobalStateMaster|
             +---------+----------+
                       |
      +----------------+----------------+
      |                                 |
+-----v----+                      +-----v----+
| Stage A  |                      | Stage B  |
| Master   |                      | Master   |
| (Source) |                      | (Map)    |
+----+-----+                      +----+-----+
     |                                 |
  enqueue_split                 enqueue_split
     |                                 |
+----v-----+                      +-----v----+
| Worker A |  process_split       | Worker B |
+----------+  (with state)        +----------+
          batch_ref                          batch_ref
               \                           /
                \----> Stage C Master <---/
```

## Known Gaps & Issues
* **Iceberg ingestion is not streaming**: `IcebergSource.read()` materialises `scan.to_arrow()` up front, so multi-billion-row tables will not fit in memory and cannot be processed incrementally without configuring `batch_size` or refactoring to iterate scan tasks.
* **Source stages run on the driver**: `RayJobRunner._seed_sources()` owns the read loop and pushes splits directly to downstream masters, keeping ingestion single-threaded and bypassing worker scaling for very large tables.
* **Split metadata lacks resume coordinates**: `_seed_sources()` populates `Split.data_range` with only upstream stage and batch IDs. Without file paths, fragment IDs, or offsets, precise replay/checkpoint alignment is difficult after failure.
* **Backpressure propagation is stubbed**: `StageMasterActor` can mark `backpressure_active`, but `MetaService.propagate_backpressure()` only logs; there is no control loop slowing upstream sources beyond the driver’s polling.
* **Queue sizing is static**: `StageMasterActor` hard-codes `max_queue_size=1000` and `max_active_splits_per_worker=2`; large-table workloads may require per-stage tuning, but no configuration surface exists yet.

## Future Improvements
* Implement asynchronous shuffle directly between stage masters to remove the runner from the hot path.
* Add adaptive backpressure handling (e.g. slow-down factors) based on queue depths and worker metrics.
* Explore auto-scaling policies driven by observed processing rates or backlog sizes.
* Extend checkpointing to support partial DAG snapshots and rolling restores.

---
*Last updated: 2025-11-14*

