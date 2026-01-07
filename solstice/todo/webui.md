# WebUI Feature Tracking

Track implementation status of WebUI features against `design-docs/webui.md`.

> **Last Updated**: 2025-01-07

---

## 🚀 Unified Read-Only Architecture

Major architectural simplification: Portal and History Server use the same read-only code.

> **Status**: Complete (2025-01-07)

### Key Design Decisions ✅
- [x] **Portal is read-only** - Only reads from JobStorage (SlateDB)
- [x] **History Server is read-only** - Same code as Portal
- [x] **JobRunner is the only writer** - StateManager writes to SlateDB
- [x] **No cross-process state sharing** - Registry pattern removed
- [x] **Unified code path** - Running and completed jobs use same logic

### Implementation ✅
- [x] **Removed registry.py** - Cross-process state doesn't work
- [x] **Updated API handlers** - Read from `request.app.state.storage` only
  - `jobs.py`, `stages.py`, `workers.py`
- [x] **Updated state_push.py** - Removed registry calls
- [x] **Updated design-docs/webui.md** - Documented unified architecture

### Push-Based Metrics ✅
- [x] **StateMessage definitions** - `webui/state/messages.py`
- [x] **JobStateManager** - Consumes Tansu, writes to SlateDB
- [x] **StateProducer** - Fire-and-forget produce
- [x] **StatePushManager** - Encapsulates state infrastructure in runner

### Producer Integration ✅
- [x] **Worker metrics push** - Modified StageWorker
- [x] **Job lifecycle events** - Modified RayJobRunner
- [x] **Stage metrics push** - Modified StageMaster

---

## ✅ Completed

### Core Architecture

- [x] **Portal Service** - Ray Serve deployment with `/solstice` route prefix (read-only)
- [x] **StatePushManager** - Encapsulates state infrastructure in JobRunner
- [x] **Unified Architecture** - Portal and History Server use same read-only code
- [x] **No cross-process state** - Removed broken registry pattern

### Storage

- [x] **JobStorage (Writer)** - Per-job write protocol
- [x] **PortalStorage (Reader)** - Cross-job read protocol
- [x] **SlateDB Storage** - Basic implementation
- [x] **Prometheus Exporter** - Real-time metrics export

### Collectors

- [x] **MetricsCollector** - 1s polling, 30s snapshots (to be replaced by push-based)
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

Differences from original `design-docs/webui.md`:

### 1. Unified Read-Only Architecture ✅

**Original Design**:
- Portal queries running jobs via JobRegistry/StateManager
- History Server reads from SlateDB
- Different code paths for running vs completed

**Current Implementation**:
- Portal reads from JobStorage (SlateDB) only
- History Server uses same code
- Unified code path for all jobs
- No cross-process state sharing

**Reason**: 
- Cross-process state doesn't work (module-level dicts are process-local)
- Simpler architecture
- Better reliability

### 2. No Cross-Process Registry ✅

**Original Design**:
- register_state_manager() / get_state_manager()
- Module-level dict to track managers

**Current Implementation**:
- Removed registry.py
- JobRunner writes to storage
- Portal reads from storage

**Reason**:
- Different jobs run in different processes
- Module-level variables aren't shared

### 3. EventCollector Not Implemented

**Action Needed**:
- Decide if EventCollector is needed
- Or update design doc to remove it

### 4. Chart.js Not Integrated

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
