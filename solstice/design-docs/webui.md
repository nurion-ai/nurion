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

### Dual-Mode Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Embedded Mode                            │
│  (Runs with Job)                                            │
│                                                             │
│  RayJobRunner → JobWebUI → [MetricsCollector]              │
│                         → [EventCollector]                 │
│                         → [LineageTracker]                 │
│                         → [ExceptionAggregator]            │
│                                                             │
│  Ray Serve (port 8000)                                      │
│    └── Portal → JobRegistry (tracks running jobs)           │
│                                                             │
│  Storage:                                                   │
│    - Prometheus (real-time metrics)                         │
│    - SlateDB (snapshots, events, lineage)                   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    History Server Mode                       │
│  (Standalone Service)                                       │
│                                                             │
│  History Server (port 8080)                                 │
│    └── Read-only SlateDB access                             │
│                                                             │
│  Storage:                                                   │
│    - SlateDB (archived jobs, metrics snapshots)             │
└─────────────────────────────────────────────────────────────┘
```

### Multi-Job Routing

```
Ray Serve (port 8000)
│
├── Portal (singleton)
│   └── /solstice/                    ← Entry point
│       ├── /                         ← List all jobs
│       ├── /running                  ← Running jobs
│       ├── /completed                ← Completed jobs
│       └── /jobs/{job_id}/           ← Route to specific job
│
├── JobRegistry (singleton Ray Actor)
│   └── Tracks all running jobs
│
├── Job A WebUI (RayJobRunner component)
├── Job B WebUI (RayJobRunner component)
└── Job C WebUI (RayJobRunner component)
```

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
│  Embedded Mode: JobWebUI is the ONLY writer                 │
│                                                             │
│  JobWebUI (per job)                                         │
│    └── SlateDBStorage                                       │
│          └── Writes: metrics snapshots, events, archives    │
│                                                             │
│  Portal (Ray Serve)                                         │
│    └── DO NOT write to SlateDB (read-only mode)             │
│    └── For running jobs: read from JobRegistry              │
│    └── For completed jobs: read from SlateDB                │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  History Server Mode: Read-Only Access                      │
│                                                             │
│  History Server                                             │
│    └── SlateDBStorage (read-only)                           │
│          └── Reads archives written by JobWebUI             │
│                                                             │
│  Note: Original writes come from JobWebUI during execution  │
└─────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**

1. **Portal reads from JobRegistry, not SlateDB** for running jobs
2. **JobWebUI is the only writer** during job execution
3. **History Server is read-only** - it only reads archives written by JobWebUI
4. **Each job has its own SlateDB path** to avoid writer conflicts:
   ```python
   storage_path = f"{base_path}/{job_id}/{attempt_id}/"
   ```

**Attempt Tracking:**

Since the same `job_id` can run multiple times, we track attempts internally:
- `attempt_id`: UUID generated for each job run
- Stored in SlateDB, not exposed in UI (user sees `job_id` only)
- Storage path: `{base_path}/{job_id}/{attempt_id}/`
- Allows querying historical runs of the same job

**Portal History Access:**

Portal reads historical data by scanning the storage directory:
```
{base_path}/
├── job_a/
│   ├── abc123/   ← attempt 1 (SlateDB instance)
│   └── def456/   ← attempt 2 (SlateDB instance)
└── job_b/
    └── ghi789/   ← attempt 1
```

For each historical query, Portal:
1. Lists job directories in base_path
2. Opens the most recent attempt's SlateDB (read-only)
3. Queries and returns data

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

### JobRegistry

Global singleton Ray Actor that tracks all running jobs.

- **Lifetime**: Detached (persists across jobs)
- **Concurrency**: High (100 concurrent calls)
- **Operations**: register, unregister, update, list, get

### Portal

Ray Serve deployment providing global entry point.

- **Route Prefix**: `/solstice`
- **Resources**: 0.1 CPU (lightweight)
- **Functions**: List jobs, route to job WebUI, external links

### JobWebUI

Per-job component (not a Ray Serve deployment).

- **Lifecycle**: Starts with job, stops when job completes
- **Collectors**: Metrics, Events, Lineage, Exceptions
- **Archiver**: Archives complete state on completion

### MetricsCollector

Background task collecting metrics every second.

- **Prometheus**: Exports metrics immediately
- **SlateDB**: Snapshots every 30 seconds
- **Derived Metrics**: Calculates rates, ETA

### JobArchiver

Archives complete job state when job finishes.

- **Triggered**: Automatically on job completion
- **Stores**: Config, stages, final metrics, exceptions summary
- **Indexed**: By status and time for efficient queries

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

**1. Stateless API Design:**

API handlers should be stateless and avoid caching references to Ray actors:

```python
# ❌ BAD: Caching actor reference in __init__
class Portal:
    def __init__(self):
        self.registry = get_or_create_registry()  # May become stale
    
    async def list_jobs(self):
        return ray.get(self.registry.list_jobs.remote())  # May fail

# ✅ GOOD: Get fresh reference each time
class Portal:
    async def list_jobs(self):
        registry = get_or_create_registry()  # Always fresh
        return ray.get(registry.list_jobs.remote())
```

**2. Minimize `ray.get()` Calls:**

`ray.get()` is blocking and unpredictable - the target actor may be busy.
Prefer pushing data to storage rather than pulling from actors.

```python
# ❌ BAD: Multiple ray.get calls in API handler
async def get_stage(job_id, stage_id):
    runner = ray.get(registry.get_runner.remote(job_id))
    stages = ray.get(runner.get_stages.remote())  # Blocking!
    return stages[stage_id]

# ✅ GOOD: Data pushed to Registry, read from there
async def get_stage(job_id, stage_id):
    registry = get_or_create_registry()
    job = ray.get(registry.get_job.remote(job_id), timeout=2)
    return next((s for s in job.stages if s['stage_id'] == stage_id), None)
```

**3. Short Timeouts:**

Always use timeouts when calling Ray actors to prevent hangs:

```python
ray.get(registry.list_jobs.remote(), timeout=2)  # 2 second timeout
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

**Mode-Aware:**

```python
@router.get("/jobs/{job_id}/stages/{stage_id}")
async def get_stage(job_id: str, stage_id: str, request: Request):
    if request.app.state.mode == "embedded":
        # Get real-time data from runner
        runner = request.app.state.job_runner
        ...
    else:
        # Get historical data from storage
        storage = request.app.state.storage
        ...
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

