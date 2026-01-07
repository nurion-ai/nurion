# WebUI Feature Tracking

Track implementation status of WebUI features against `design-docs/webui.md`.

> **Last Updated**: 2025-01-07

---

## ✅ Completed

### Core Architecture

- [x] **Portal Service** - Ray Serve deployment with `/solstice` route prefix
- [x] **JobWebUI** - Per-job WebUI instance
- [x] **Dual-Mode Architecture** - Embedded Mode + History Server Mode
- [x] **No JobRegistry Design** - Uses Ray State API to query actors directly (simpler than design doc)

### Storage

- [x] **JobStorage (Writer)** - Per-job write protocol
- [x] **PortalStorage (Reader)** - Cross-job read protocol
- [x] **SlateDB Storage** - Basic implementation
- [x] **Prometheus Exporter** - Real-time metrics export

### Collectors

- [x] **MetricsCollector** - 1s polling, 30s snapshots
- [x] **LineageTracker** - Basic implementation
- [x] **ExceptionAggregator** - Basic implementation
- [x] **JobArchiver** - Archives job on completion

### API Endpoints

- [x] `GET /api/jobs` - List all jobs
- [x] `GET /api/jobs/{job_id}` - Job details
- [x] `GET /api/jobs/{job_id}/stages` - Stage list
- [x] `GET /health` - Health check

### Pages

- [x] **Portal Home** - Shows running/completed jobs
- [x] **Running Jobs Page** - Running jobs list
- [x] **Completed Jobs Page** - Completed jobs list
- [x] **Job Detail Page** - Job details, stages list
- [x] **Stage Detail Page** - Stage details, workers, partition metrics
- [x] **Workers Page** - All workers for a job
- [x] **Worker Detail Page** - Worker details, event history
- [x] **Exceptions Page** - Exception list
- [x] **Configuration Page** - Job configuration display

### Tech Stack

- [x] **FastAPI + Ray Serve** - Backend
- [x] **HTMX + Jinja2** - Frontend
- [x] **Pico CSS** - Styling

---

## 🚧 In Progress

*None*

---

## 📋 TODO

### High Priority

- [ ] **SSE Real-Time Updates** - `/sse/metrics` endpoint from design doc
  - Implement `EventSourceResponse` push
  - For real-time refresh on Job/Stage pages

- [ ] **Checkpoints Page** - Currently returns empty data
  - Implement CheckpointCollector
  - Store and query checkpoint history

- [ ] **Lineage Page** - Currently returns empty data
  - Implement lineage graph visualization
  - Use Dagre + D3.js for DAG rendering

### Medium Priority

- [ ] **Stage DAG Visualization** - DAG graph on Job Detail page
  - Design doc mentions Dagre + D3.js
  - Currently only shows stages list, no graphical display

- [ ] **Timeline Events** - Timeline visualization
  - Design doc mentions TimelineEvent
  - EventCollector not implemented

- [ ] **Worker Resource Monitoring** - CPU/Memory/GPU usage
  - Integrate Ray resource metrics
  - Chart.js visualization

- [ ] **Backpressure Visualization** - Backpressure status display
  - Time-series chart for backpressure ratio
  - Bottleneck stage analysis

- [ ] **Data Skew Detection** - Data skew display
  - Show skew ratio in Stage Detail
  - Partition-level lag comparison

### Low Priority

- [ ] **Grafana Dashboard Templates** - `solstice/webui/grafana/`
  - Design doc Phase 2
  - Create JSON templates

- [ ] **Alert Rule Examples** - Prometheus alerting
  - Design doc Phase 2

- [ ] **Worker Stacktrace** - py-spy integration
  - Mentioned in design doc
  - Implement `stacktrace_url`

- [ ] **Worker Real-Time Logs** - Log streaming
  - Design doc mentions 5000 line limit

- [ ] **Query Builder for Splits** - Design doc Phase 2

- [ ] **Job Comparison Tool** - Design doc Phase 2

- [ ] **Resource Recommendations** - Design doc Phase 2

---

## 🔄 Design Changes

Differences from `design-docs/webui.md`:

### 1. JobRegistry Removed

**Design Doc**:
```
JobRegistry (singleton Ray Actor)
└── Tracks all running jobs
```

**Actual Implementation**:
- No JobRegistry
- Uses Ray State API (`ray.util.state.list_actors`) for direct queries
- Identifies running jobs via `_RaySplitPayloadStoreActor`
- Gets stage/worker info from `StageWorker` actor names

**Reason**: 
- Avoids single point of failure
- No additional actor maintenance
- Simpler design

**Update design doc**: ✅ Yes

### 2. EventCollector Not Implemented

**Design Doc**:
```
JobWebUI → [EventCollector]
         → [LineageTracker]
         → [ExceptionAggregator]
```

**Actual Implementation**:
- `job_webui.py` only initializes: MetricsCollector, LineageTracker, ExceptionAggregator, JobArchiver
- No EventCollector

**Action Needed**:
- Decide if EventCollector is needed
- Or update design doc to remove it

### 3. Alpine.js Not Actually Used

**Design Doc**:
- Frontend: HTMX + Alpine.js + Jinja2

**Actual Implementation**:
- Templates don't use Alpine.js
- Primarily HTMX + Jinja2

**Action Needed**:
- Decide if Alpine.js is needed
- If needed, identify use cases (dropdowns, modals)

### 4. Chart.js Not Integrated

**Design Doc**:
- Charts: Chart.js

**Actual Implementation**:
- `static/js/` directory exists but no Chart.js integration
- No time-series charts

**Action Needed**:
- Add Chart.js vendor files
- Implement throughput/lag time-series charts

---

## 📝 Next Iteration Suggestions

1. **Prioritize SSE** - Key for improving user experience
2. **Complete Stage DAG Visualization** - Important for understanding pipeline structure
3. **Update design-docs/webui.md** - Reflect actual changes like JobRegistry removal
4. **Add Chart.js** - Prepare for throughput/lag charts

---

## References

- Design Doc: `design-docs/webui.md`
- WebUI Code: `solstice/webui/`
- Example: `examples/webui_demo.py`
