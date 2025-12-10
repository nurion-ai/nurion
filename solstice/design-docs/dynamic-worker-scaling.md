# Dynamic Worker Scaling Design

_Design document for Solstice auto-scaling feature_
_Created: December 2025_

## 1. Overview

This document describes the design for dynamic worker scaling in Solstice, a batch/offline data processing framework. The design prioritizes simplicity over complexity, recognizing that offline processing has different requirements than real-time streaming.

### 1.1 Goals

1. **Balanced throughput**: Prevent stages from becoming bottlenecks or starving
2. **Resource efficiency**: Scale workers up/down based on actual load
3. **Fault tolerance**: Handle worker failures gracefully
4. **Manual intervention**: Allow operators to override automatic decisions
5. **Simplicity**: Minimize code complexity and external dependencies

### 1.2 Non-Goals

- Sub-second scaling decisions (offline processing tolerates delays)
- Complex distributed consensus (single coordinator is sufficient)
- Persistent scaling state (can be reconstructed on restart)
- Predictive scaling (reactive is sufficient for batch workloads)

## 2. Context: Offline vs Real-Time

Solstice is an **offline/batch processing** framework, not a real-time streaming system. This distinction is crucial for design decisions:

| Dimension | Real-Time Streaming | Offline Batch (Solstice) |
|-----------|--------------------|-----------------------|
| Data source | Unbounded, continuous | **Bounded, controllable rate** |
| Latency requirement | Milliseconds~seconds | **Minutes~hours acceptable** |
| Fault tolerance | Must recover precisely | **Can re-run stages** |
| Backpressure | Critical, upstream uncontrollable | **Source rate controllable** |
| Scaling decisions | Must be instant | **10-30 second delay acceptable** |

**Key insight**: Since the source rate is controllable and latency requirements are relaxed, we can use a much simpler architecture than real-time systems like Flink or Kafka Streams.

## 3. Architecture

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RayJobRunner                                    │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      SimpleAutoscaler                               │ │
│  │                                                                     │ │
│  │  • In-memory state only (no persistence needed)                     │ │
│  │  • 15-30 second decision interval                                   │ │
│  │  • Simple threshold-based rules                                     │ │
│  │  • Manual override via configuration                                │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ StageMaster  │──│ StageMaster  │──│ StageMaster  │                   │
│  │   (Source)   │  │  (Process)   │  │    (Sink)    │                   │
│  │              │  │              │  │              │                   │
│  │ Workers[1-3] │  │ Workers[1-8] │  │ Workers[1-2] │                   │
│  └──────────────┘  └──────────────┘  └──────────────┘                   │
│         │                 │                 │                            │
│         └─────────────────┴─────────────────┘                            │
│                           │                                              │
│                    Tansu Queues (S3-backed)                              │
│                    • Data flow between stages                            │
│                    • Offset persistence (exactly-once)                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Design Principles

1. **Single coordinator**: The `SimpleAutoscaler` runs within `RayJobRunner`, not as a separate distributed component. This eliminates distributed consensus complexity.

2. **In-memory state**: Scaling decisions and worker counts are kept in memory. On restart, state is reconstructed from actual `StageMaster` status.

3. **Slow-paced decisions**: Scaling decisions are made every 15-30 seconds, not continuously. This is sufficient for batch workloads and reduces system overhead.

4. **Direct method calls**: Since `StageMaster` instances are Python objects (not Ray actors), metrics collection is synchronous and fast.

## 4. Detailed Design

### 4.1 Configuration

```python
@dataclass
class AutoscaleConfig:
    """Autoscaling configuration."""
    
    enabled: bool = True
    check_interval_s: float = 15.0  # Decision interval
    
    # Scaling thresholds
    scale_up_lag_threshold: int = 1000    # Scale up if queue lag > threshold
    scale_down_lag_threshold: int = 100   # Scale down if lag < threshold
    scale_down_utilization: float = 0.3   # Scale down if utilization < 30%
    
    # Damping
    cooldown_s: float = 60.0              # Cooldown after scaling
    max_scale_step: int = 2               # Max workers to add/remove per decision
    
    # Manual overrides
    fixed_workers: Optional[Dict[str, int]] = None  # {"stage_id": count}
    frozen_stages: Set[str] = field(default_factory=set)  # Stages to skip
```

### 4.2 Metrics Collection

Metrics are collected directly from `StageMaster` instances via synchronous method calls:

```python
@dataclass
class StageMetrics:
    stage_id: str
    worker_count: int
    input_queue_lag: int      # Messages pending in input queue
    output_queue_size: int    # Messages in output queue
    is_finished: bool
    config: StageConfig       # min_workers, max_workers, etc.
```

**Why not Ray RPC or message queues for metrics?**

- `StageMaster` is a regular Python object in the same process as `RayJobRunner`
- Direct method calls are fast and simple
- No serialization overhead or network latency
- No additional dependencies

### 4.3 Scaling Algorithm

The algorithm uses simple threshold-based rules:

```python
def compute_desired_workers(metrics: StageMetrics) -> int:
    """
    Compute desired worker count based on queue lag.
    
    Rules:
    1. Manual override has highest priority
    2. Scale up if input queue lag > threshold
    3. Scale down if lag is small and workers > min
    4. Otherwise maintain current count
    """
    config = metrics.config
    current = metrics.worker_count
    
    # Rule 1: Manual override
    if stage_id in fixed_workers:
        return fixed_workers[stage_id]
    
    # Rule 2: Scale up on high lag
    if metrics.input_queue_lag > scale_up_lag_threshold:
        return min(current + max_scale_step, config.max_workers)
    
    # Rule 3: Scale down on low lag
    if metrics.input_queue_lag < scale_down_lag_threshold:
        if current > config.min_workers:
            return max(current - 1, config.min_workers)
    
    # Rule 4: Maintain
    return current
```

### 4.4 Cooldown and Damping

To prevent thrashing (rapid scale up/down cycles):

1. **Cooldown period**: After scaling a stage, wait `cooldown_s` seconds before scaling it again
2. **Max step size**: Scale at most `max_scale_step` workers per decision
3. **Hysteresis**: Different thresholds for scale-up vs scale-down

### 4.5 Manual Intervention

Operators can intervene through the `RayJobRunner` API:

```python
# Set fixed worker count for a stage
runner.set_stage_workers("gpu_inference", 10)

# Freeze a stage (disable autoscaling)
runner.freeze_stage("gpu_inference")

# Unfreeze (re-enable autoscaling)  
runner.unfreeze_stage("gpu_inference")

# Pause all autoscaling
runner.pause_autoscaling()

# Resume autoscaling
runner.resume_autoscaling()

# Get current status
status = runner.get_autoscale_status()
```

## 5. Fault Tolerance

### 5.1 Worker Failure

When a worker fails (Ray actor dies):

1. `StageMaster` detects the failure via `ray.wait()` on worker tasks
2. Failed worker is removed from the worker pool
3. If `worker_count < min_workers`, a new worker is spawned immediately
4. Unprocessed messages are re-consumed from the queue (offset not committed)

```python
# In StageMaster.run()
for worker_id, task in list(self._worker_tasks.items()):
    ready, _ = ray.wait([task], timeout=0.1)
    if ready:
        try:
            ray.get(ready[0])
        except Exception as e:
            logger.warning(f"Worker {worker_id} failed: {e}")
            self._workers.pop(worker_id, None)
            self._worker_tasks.pop(worker_id, None)
            
            # Auto-replenish if below minimum
            if len(self._workers) < self.config.min_workers:
                await self._spawn_worker()
```

### 5.2 StageMaster Failure

If a `StageMaster` fails, the entire stage is restarted by `RayJobRunner`. The stage resumes from the last committed offset in Tansu.

### 5.3 Coordinator Failure

If `RayJobRunner` (and thus `SimpleAutoscaler`) fails:

1. The job restarts from scratch
2. `SimpleAutoscaler` reconstructs state from current `StageMaster` status
3. No persistent state to recover - decisions are recomputed

**Why is this acceptable?**

- Batch jobs are expected to run for minutes/hours
- Re-running scaling decisions is cheap
- Critical data (offsets) is persisted in Tansu

## 6. Resource Management

### 6.1 Resource Vectors

Different stages may have different resource requirements:

```python
@dataclass
class StageConfig:
    # Worker resource requirements
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0
    
    # Scaling bounds
    min_workers: int = 1
    max_workers: int = 4
```

### 6.2 Global Resource Constraints

When cluster resources are limited, the autoscaler respects Ray's resource constraints:

```python
def can_spawn_worker(config: StageConfig) -> bool:
    """Check if resources are available for a new worker."""
    available = ray.available_resources()
    
    if config.num_cpus > available.get("CPU", 0):
        return False
    if config.num_gpus > 0 and config.num_gpus > available.get("GPU", 0):
        return False
    
    return True
```

### 6.3 Bottleneck Prioritization

When resources are scarce, prioritize stages that are bottlenecks:

```python
def prioritize_stages(metrics: Dict[str, StageMetrics]) -> List[str]:
    """
    Return stages sorted by scaling priority.
    
    Bottleneck indicators:
    - High input queue lag
    - High worker utilization
    - Many downstream stages affected
    """
    def priority(m: StageMetrics) -> float:
        lag_score = m.input_queue_lag / 1000  # Normalize
        downstream_factor = 1 + len(m.downstream_stages) * 0.2
        return lag_score * downstream_factor
    
    return sorted(metrics.keys(), key=lambda s: priority(metrics[s]), reverse=True)
```

## 7. Observability

### 7.1 Logging

All scaling decisions are logged:

```python
logger.info(f"Scaling {stage_id}: {current} -> {target} workers "
            f"(lag={lag}, reason={reason})")
```

### 7.2 Metrics Export

Metrics can be exported for monitoring dashboards:

```python
def get_autoscale_status() -> Dict[str, Any]:
    return {
        "enabled": self.config.enabled,
        "stages": {
            stage_id: {
                "current_workers": metrics.worker_count,
                "desired_workers": self._compute_desired(metrics),
                "input_queue_lag": metrics.input_queue_lag,
                "last_scale_time": self._last_scale_time.get(stage_id),
                "is_frozen": stage_id in self.config.frozen_stages,
            }
            for stage_id, metrics in self._current_metrics.items()
        }
    }
```

## 8. Implementation Plan

### Phase 1: Core Autoscaler (MVP)

1. Add `AutoscaleConfig` dataclass
2. Implement `SimpleAutoscaler` class (~100 lines)
3. Integrate into `RayJobRunner`
4. Add basic logging

**Estimated effort**: 1-2 days

### Phase 2: Manual Intervention API

1. Add `set_stage_workers()`, `freeze_stage()`, etc. to `RayJobRunner`
2. Add CLI commands (optional)

**Estimated effort**: 0.5-1 day

### Phase 3: Resource-Aware Scaling

1. Add resource availability checks
2. Implement bottleneck prioritization

**Estimated effort**: 1 day

### Phase 4: Observability

1. Add structured logging for scaling events
2. Add metrics export endpoint

**Estimated effort**: 0.5-1 day

## 9. Testing Strategy

### 9.1 Unit Tests

- `test_scaling_decision`: Verify threshold-based decisions
- `test_cooldown`: Verify cooldown period is respected
- `test_manual_override`: Verify manual settings take priority
- `test_resource_check`: Verify resource availability checks

### 9.2 Integration Tests

- `test_scale_up_on_lag`: Create backlog, verify workers increase
- `test_scale_down_on_idle`: Clear backlog, verify workers decrease
- `test_worker_failure_recovery`: Kill worker, verify replenishment
- `test_frozen_stage`: Freeze stage, verify no scaling

### 9.3 End-to-End Tests

- Run multi-stage pipeline with autoscaling enabled
- Verify all data processed correctly
- Verify scaling events in logs

## 10. Future Considerations

### When to Revisit This Design

The simple design should be revisited if Solstice evolves to support:

1. **Real-time streaming**: Sub-second latency requirements
2. **Long-running jobs**: 24/7 operation requiring better state persistence
3. **Multi-tenant clusters**: Complex resource isolation needs
4. **Large-scale clusters**: 100+ stages requiring more sophisticated scheduling

### Potential Enhancements

- **Predictive scaling**: Use historical data to anticipate load
- **Cost optimization**: Prefer spot instances when possible
- **SLA-aware scheduling**: Priority levels for different jobs

## 11. References

- [Checkpoint and Recovery Design](checkpoint-and-recovery.md)
- [Architecture Overview](architecture.md)
- [Tansu Queue Backend](../solstice/queue/tansu.py)

---

_Last updated: December 2025_
