# Solstice Debug WebUI Design

## Overview

The Solstice Debug WebUI provides a web-based interface for monitoring, debugging, and analyzing streaming data pipelines. It supports both real-time monitoring during job execution and historical analysis through a History Server.

## Design Goals

1. **Comprehensive Monitoring**: Track all aspects of job execution
2. **Post-Mortem Analysis**: Archive jobs for later investigation
3. **Multi-Job Support**: Monitor multiple jobs in the same Ray cluster
4. **Zero New Ports**: Reuse Ray Serve port
5. **Easy Maintenance**: Simple tech stack (HTMX + Alpine.js + Pico CSS)
6. **High Information Density**: Optimized for developers and data engineers

## Architecture

### Unified Read-Only Architecture

Portal and History Server share the **same read-only logic**. Both read from JobStorage (SlateDB).

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ray Cluster                               │
│                                                                   │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     │
│  │  JobRunner   │     │  JobRunner   │     │  JobRunner   │     │
│  │  (Job A)     │     │  (Job B)     │     │  (Job C)     │     │
│  │              │     │              │     │              │     │
│  │ StateManager │     │ StateManager │     │ StateManager │     │
│  │      ↓       │     │      ↓       │     │      ↓       │     │
│  │ JobStorage   │     │ JobStorage   │     │ JobStorage   │     │
│  │  (write)     │     │  (write)     │     │  (write)     │     │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘     │
│         │                    │                    │              │
│         └────────────────────┼────────────────────┘              │
│                              ↓                                   │
│                    ┌─────────────────┐                           │
│                    │   SlateDB (S3)  │                           │
│                    └────────┬────────┘                           │
│                             ↓                                    │
│                    ┌─────────────────┐                           │
│                    │     Portal      │  ← Ray Serve (singleton)  │
│                    │   (read-only)   │                           │
│                    └─────────────────┘                           │
└─────────────────────────────────────────────────────────────────┘

History Server (standalone):
  └── Also read-only SlateDB - SAME CODE as Portal
```

**Key Design Principles:**

1. **JobRunner is the ONLY writer** - StateManager consumes Tansu, writes to JobStorage
2. **Portal is read-only** - Just reads from JobStorage (SlateDB)
3. **History Server is read-only** - Same code as Portal
4. **No cross-process state sharing** - Each process only reads/writes its own storage
5. **Unified code path** - Running and completed jobs use the same read logic

### Multi-Job Routing

```
Ray Serve (port 8000)
│
├── Portal (singleton, read-only)
│   └── /solstice/                    ← Entry point
│       ├── /                         ← List all jobs (from JobStorage)
│       ├── /running                  ← Running jobs (status=RUNNING in storage)
│       ├── /completed                ← Completed jobs (status=COMPLETED/FAILED)
│       └── /jobs/{job_id}/           ← Job details (from JobStorage)
│
└── JobStorage (read-only access to SlateDB)
    └── Queries job data written by JobRunners
```

Note: No JobRegistry needed. Portal reads all job info from storage.

## Storage Strategy

### Prometheus (Real-Time Metrics)

**Stored:**
- Stage throughput (records/s)
- Queue lag and size
- Partition-level lag
- Backpressure status
- Data skew ratio
- Worker count

**Pros:**
- Standard monitoring solution
- Grafana integration
- Alerting support
- Ray already exports metrics

**Cons:**
- Limited retention (typically 15 days)
- Not suitable for long-term history

### SlateDB (Historical Data)

**Stored:**
- Job archives (complete final state)
- Metrics snapshots (every 30s)
- Worker lifecycle events
- Exceptions with stacktraces
- Split lineage
- Timeline events

**Pros:**
- S3-backed, unlimited retention
- Supports History Server
- Complex queries (lineage graphs)
- Stores structured data (JSON)

**Cons:**
- Not real-time
- No alerting
- **Single writer only** (see architecture note below)

#### SlateDB Single Writer Architecture

SlateDB only supports **one writer process** at a time. This constraint shapes our architecture:

```
┌─────────────────────────────────────────────────────────────┐
│  Writer: JobStateManager (inside JobRunner process)         │
│                                                             │
│  JobRunner                                                  │
│    └── StatePushManager                                     │
│          └── JobStateManager                                │
│                └── Consumes from Tansu topic                │
│                └── Aggregates state                         │
│                └── Writes to SlateDB                        │
│                                                             │
│  Each job has its own SlateDB path:                         │
│    storage_path = f"{base_path}/{job_id}/{attempt_id}/"     │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Readers: Portal and History Server (SAME CODE)             │
│                                                             │
│  Portal (Ray Serve)                                         │
│    └── JobStorage (read-only)                               │
│          └── Reads from SlateDB                             │
│          └── Lists jobs, gets details, queries metrics      │
│                                                             │
│  History Server (standalone)                                │
│    └── JobStorage (read-only)                               │
│          └── SAME code as Portal                            │
│          └── Just different deployment                      │
└─────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**

1. **JobStateManager is the only writer** - runs inside JobRunner process
2. **Portal is read-only** - no cross-process state sharing needed
3. **History Server is read-only** - same code as Portal
4. **Each job has its own SlateDB path** to avoid writer conflicts

**Attempt Tracking:**

Since the same `job_id` can run multiple times, we track attempts internally:
- `attempt_id`: UUID generated for each job run
- Stored in SlateDB, not exposed in UI (user sees `job_id` only)
- Storage path: `{base_path}/{job_id}/{attempt_id}/`
- Allows querying historical runs of the same job

**Storage Directory Structure:**

```
{base_path}/
├── job_a/
│   ├── abc123/   ← attempt 1 (SlateDB instance)
│   └── def456/   ← attempt 2 (SlateDB instance)
└── job_b/
    └── ghi789/   ← attempt 1
```

Portal/History Server reads by:
1. Listing job directories in base_path
2. Opening the most recent attempt's SlateDB (read-only)
3. Querying and returning data

This avoids writer conflicts while supporting multi-attempt history

### Hybrid Strategy

| Data Type | Prometheus | SlateDB |
|-----------|------------|---------|
| Real-time metrics | ✅ Primary | Snapshot backup |
| Resource usage | ✅ (via Ray) | Snapshot backup |
| Job metadata | - | ✅ Primary |
| Exceptions | - | ✅ Primary |
| Lineage | - | ✅ Primary |
| Timeline events | - | ✅ Primary |

## Component Details

### Portal

Ray Serve deployment providing global entry point. **Read-only** access to JobStorage.

- **Route Prefix**: `/solstice`
- **Resources**: 0.1 CPU (lightweight)
- **Functions**: List jobs, job details, stage/worker info
- **Data Source**: JobStorage (SlateDB) - read only
- **Same code as History Server** - just different deployment

### StatePushManager

Encapsulates push-based state infrastructure inside JobRunner.

- **Lifecycle**: Starts with job, stops when job completes
- **Components**: Tansu broker, StateProducer, JobStateManager
- **Fire-and-forget**: Producers don't wait for produce() completion

### JobStateManager

Consumes state messages from Tansu, maintains aggregated state, writes to SlateDB.

- **Input**: Tansu topic (push-based messages from workers/masters)
- **Processing**: Time-window aggregation, deduplication
- **Output**: Periodic writes to JobStorage (SlateDB)
- **Single writer**: Only component that writes to SlateDB for this job

### StateProducer

Helper for producing state messages (used by workers, masters, runner).

- **Rate-limited**: Workers emit at most once per 500ms
- **Async**: Fire-and-forget pattern, doesn't block caller
- **Messages**: WORKER_METRICS, STAGE_METRICS, EXCEPTION, etc.

## UI Design

### Tech Stack

- **Backend**: FastAPI + Ray Serve
- **Frontend**: HTMX + Alpine.js + Jinja2
- **Styling**: Pico CSS (10KB, semantic)
- **Charts**: Chart.js
- **DAG**: Dagre + D3.js

### Design Principles

- **Simple & Professional**: No flashy animations
- **High Information Density**: Compact spacing, readable fonts
- **Large Dataset Friendly**: Pagination, fixed headers, virtual scrolling
- **Stateless API Design**: Minimize `ray.get()` calls in API handlers
- **Single Writer per Storage**: SlateDB only supports one writer process

### Performance Optimizations

| Problem | Solution |
|---------|----------|
| Large lists | Server-side pagination (max 1000 items) |
| Wide tables | Fixed first column (`sticky-col`) |
| Scrolling headers | Fixed table headers (`position: sticky`) |
| Log overflow | Limit to 5000 lines in memory |
| Input lag | 300ms debounce on filters |
| Page freezing | Fixed container heights, internal scrolling |

## API Design

### Core Principles

**1. Read-Only Portal Design:**

Portal only reads from JobStorage. No cross-process state sharing.

```python
# ✅ GOOD: Read from storage
class Portal:
    async def list_jobs(self, request: Request):
        storage = request.app.state.storage
        return storage.list_jobs()

# ❌ BAD: Try to query actors or cross-process state
class Portal:
    async def list_jobs(self):
        return ray.get(some_actor.list_jobs.remote())  # Cross-process!
```

**2. No Cross-Process State Sharing:**

Different jobs run in different processes. Module-level variables don't work.

```python
# ❌ BAD: Module-level registry (only visible in one process)
_state_managers: Dict[str, Any] = {}  # Other processes can't see this!

# ✅ GOOD: Write to storage, read from storage
# JobRunner writes → SlateDB ← Portal reads
```

**3. Minimize `ray.get()` Calls:**

`ray.get()` is blocking and unpredictable. Only use for debugging tools (logs, stacktrace).

```python
# ❌ BAD: ray.get for regular data
async def get_stage(job_id, stage_id):
    return ray.get(runner.get_stages.remote())  # Blocking!

# ✅ GOOD: Read from storage
async def get_stage(job_id, stage_id, request: Request):
    storage = request.app.state.storage
    job = storage.get_job(job_id)
    return next((s for s in job['stages'] if s['stage_id'] == stage_id), None)

# ✅ OK: ray.get only for live debugging (logs, stacktrace)
async def get_worker_logs(worker_id):
    from ray.util.state import list_actors, get_log
    actors = list_actors(filters=[("name", "=", worker_id)])
    # This is for live debugging, acceptable to use Ray State API
```

### Standard Patterns

**Pagination:**

```python
@router.get("/jobs/{job_id}/splits")
async def list_splits(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=1000),
) -> PagedResponse[SplitInfo]:
    ...
```

**Unified Read Pattern:**

Portal and History Server use the same code - no mode checking needed:

```python
@router.get("/jobs/{job_id}/stages/{stage_id}")
async def get_stage(job_id: str, stage_id: str, request: Request):
    # Same code for running and completed jobs
    storage = request.app.state.storage
    job = storage.get_job(job_id)
    if job:
        return next((s for s in job['stages'] if s['stage_id'] == stage_id), None)
    raise HTTPException(404, "Job not found")
```

**Real-Time Updates:**

```python
@router.get("/sse/metrics")
async def stream_metrics() -> EventSourceResponse:
    async def generator():
        while running:
            yield {"event": "metrics", "data": {...}}
            await asyncio.sleep(2)
    return EventSourceResponse(generator())
```

## Integration Points

### Ray Dashboard

WebUI provides links to Ray Dashboard for:
- Actor details (by actor ID)
- Node monitoring
- Task execution details

### Grafana

Pre-built dashboards for:
- Job overview (throughput, lag, workers)
- Stage details (partition metrics, backpressure)
- Worker details (CPU, memory, GPU)

Users need to:
1. Deploy Prometheus + Grafana
2. Configure `SOLSTICE_GRAFANA_URL`
3. Import dashboard JSON from `solstice/webui/grafana/`

### Ray Event Exporter

EventCollector integrates with Ray's event system:
- Uses `ray.util.state.list_cluster_events()` (Ray 2.x)
- Filters and stores relevant events
- Provides timeline visualization

## Deployment

### Production Recommendations

```python
job_config = JobConfig(
    webui=WebUIConfig(
        enabled=True,
        storage_path="s3://prod-bucket/solstice-history/",
        prometheus_enabled=True,
        prometheus_pushgateway="http://pushgateway:9091",  # For batch jobs
        metrics_snapshot_interval_s=30.0,
        archive_on_completion=True,
    ),
)
```

### History Server Deployment

```bash
# Run as systemd service or K8s deployment
solstice history-server \
    --storage-path s3://prod-bucket/solstice-history/ \
    --host 0.0.0.0 \
    --port 8080
```

## Monitoring Checklist

What you can monitor:

**Job Level:**
- ✅ Progress and ETA
- ✅ Overall throughput
- ✅ Stage DAG with status
- ✅ Timeline of events
- ✅ Exception count

**Stage Level:**
- ✅ Worker count (current/min/max)
- ✅ Input/output throughput
- ✅ Queue lag and backpressure
- ✅ Partition-level offsets
- ✅ Data skew detection

**Worker Level:**
- ✅ Resource usage (CPU/Memory/GPU)
- ✅ Processing statistics
- ✅ Assigned partitions
- ✅ Real-time logs
- ✅ Stacktrace (py-spy)

**Data Flow:**
- ✅ Split lineage graph
- ✅ Parent-child relationships
- ✅ Processing worker mapping

**Debugging:**
- ✅ Exception aggregation
- ✅ Root cause hints
- ✅ Worker event history (created/destroyed/scaled)
- ✅ Checkpoint history

## Push-Based Metrics Architecture

### Motivation

The original pull-based metrics collection has scalability issues:

1. **Ray Remote Overhead**: Frequent `ray.get()` calls for metrics create pressure on GCS, metadata, and network
2. **Worker Interference**: `get_metrics()` calls may block worker processing
3. **Unpredictable Latency**: Timeouts cause missing data; busy workers cause delays
4. **Linear Scaling**: O(N) complexity with number of workers

### Event-Driven Architecture

We use Tansu (embedded Kafka) for push-based state management:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Push-Based State Architecture                          │
│                                                                                  │
│   PRODUCERS (fire-and-forget)                  TANSU TOPICS                     │
│   ──────────────────────────                   ────────────                     │
│                                                                                  │
│   ┌─────────────┐                              ┌──────────────────┐             │
│   │ RayJobRunner│ ──JOB_STARTED─────────────▶ │                  │             │
│   │             │ ──JOB_COMPLETED───────────▶ │  {job}_state     │             │
│   └─────────────┘                              │                  │             │
│                                                └────────┬─────────┘             │
│   ┌─────────────┐                                       │                       │
│   │ StageMaster │ ──STAGE_STARTED────────────▶        │                       │
│   │             │ ──STAGE_METRICS────────────▶        │                       │
│   │             │ ──BACKPRESSURE─────────────▶        │                       │
│   └─────────────┘                                       │                       │
│                                                         │                       │
│   ┌─────────────┐                                       │                       │
│   │ StageWorker │ ──WORKER_METRICS───────────▶        │                       │
│   │             │ ──WORKER_STARTED───────────▶        │                       │
│   │             │ ──WORKER_STOPPED───────────▶        │                       │
│   │             │ ──EXCEPTION────────────────▶        │                       │
│   └─────────────┘                                       │                       │
│                                                         ▼                       │
│                                                ┌────────────────────┐           │
│                                                │   JobStateManager  │           │
│                                                │   (per job)        │           │
│                                                │                    │           │
│                                                │ - Consume state    │           │
│                                                │   topic            │           │
│                                                │ - Time-window      │           │
│                                                │   aggregation      │           │
│                                                │ - In-memory state  │           │
│                                                │ - Snapshot to      │           │
│                                                │   SlateDB          │           │
│                                                │ - Export to        │           │
│                                                │   Prometheus       │           │
│                                                └──────────┬─────────┘           │
│                                                           │                     │
│                                    ┌──────────────────────┼────────────┐        │
│                                    │                      │            │        │
│                                    ▼                      ▼            ▼        │
│                            ┌────────────┐        ┌─────────────┐ ┌──────────┐  │
│                            │  REST API  │        │  SSE Stream │ │ SlateDB  │  │
│                            │ (query)    │        │  (push)     │ │ (history)│  │
│                            └────────────┘        └─────────────┘ └──────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### State Message Types

```python
class StateMessageType(str, Enum):
    # Job lifecycle
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    
    # Stage lifecycle
    STAGE_STARTED = "stage_started"
    STAGE_COMPLETED = "stage_completed"
    
    # Worker lifecycle
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"
    
    # Metrics (periodic, rate-limited)
    STAGE_METRICS = "stage_metrics"
    WORKER_METRICS = "worker_metrics"
    
    # Events
    EXCEPTION = "exception"
    BACKPRESSURE = "backpressure"
```

### Time-Window Aggregation

Since push-based metrics have non-aligned timestamps, we use time-window aggregation:

```python
class TimeWindowAggregator:
    """Aggregate metrics into fixed time windows.
    
    - window_size: 1 second (configurable)
    - max_lag: 3 seconds (wait for late arrivals)
    - Strategy: Take latest value per source within window
    """
    
    def _get_window_start(self, timestamp: float) -> float:
        return math.floor(timestamp / self.window_size) * self.window_size
    
    def process_message(self, msg: StateMessage) -> None:
        window = self._get_window_start(msg.timestamp)
        # Keep latest value per source_id within window
        if msg.timestamp > existing.timestamp:
            self._windows[window][msg.source_id] = msg
```

### Benefits vs Trade-offs

| Aspect | Pull-Based (Old) | Push-Based (New) |
|--------|-----------------|------------------|
| Ray GCS Load | High (N×2 calls/s) | Near zero |
| Worker Impact | Blocks processing | No impact (async) |
| Latency | Unpredictable (1-30s) | Predictable (~1.5s) |
| Consistency | Strong (point-in-time) | Eventual (windowed) |
| Fault Tolerance | Poor (timeouts) | Good (replay from Tansu) |
| Scalability | O(N) workers | O(1) consumer |
| Complexity | Simple | Medium |

### Design Principles

1. **SplitPayloadStore stays pure**: Only stores payloads, no job metadata
2. **Fire-and-forget producers**: Workers don't wait for produce() completion
3. **Rate limiting**: Workers emit at most once per 500ms
4. **Adaptive sampling**: When consumer lags, skip to latest
5. **Event sourcing**: Can rebuild state by replaying messages

## Future Work

### Phase 2 (Post-MVP)
- Grafana dashboard templates
- Alert rule examples
- Query builder for splits
- Job comparison tool
- Resource recommendations

### Phase 3 (Advanced)
- Flame graphs for performance analysis
- Cost analysis (based on resource usage)
- Anomaly detection
- Auto-remediation suggestions

## References

Design inspired by:
- Apache Flink Web UI
- Apache Spark Web UI & History Server
- Ray Dashboard
- Prometheus + Grafana ecosystem

