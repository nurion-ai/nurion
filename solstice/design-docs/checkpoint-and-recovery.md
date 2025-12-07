# Checkpoint, Recovery, and Stream-Based Architecture Design

_Design discussion summary - December 5-6, 2025_

## Problem Statement

### Core Issue: Split Determinism

When a job partially completes and restarts, the current checkpoint mechanism fails because:

1. **Spark Source**: Each execution may produce splits in different order (distributed execution non-determinism)
2. **Split ID mismatch**: `split_id=f"{stage_id}_split_{idx}"` - same index may refer to different data after restart
3. **Data loss/duplication**: Checkpoint records `completed_splits = {split_0, split_1, split_2}`, but after restart these IDs may map to completely different data

**Example scenario**:
```
First run:
  split_0 (users 1-100)   ✓ completed
  split_1 (users 101-200) ✓ completed
  split_2 (users 201-300) ✓ completed
  split_3 (users 301-400) ✗ not completed
  split_4 (users 401-500) ✗ not completed

Checkpoint: completed_splits = {split_0, split_1, split_2}

Second run (Spark order changed):
  split_0 (users 401-500) ← skipped (checkpoint says completed) ❌ DATA LOST
  split_1 (users 301-400) ← skipped ❌ DATA LOST
  split_2 (users 201-300) ← skipped ❌
  split_3 (users 101-200) ← executed ❌ DUPLICATE
  split_4 (users 1-100)   ← executed ❌ DUPLICATE
```

### Additional Challenges

1. **Data source changes**: What if data source is modified between runs?
2. **Config changes**: What if user changes filter/columns/parameters?
3. **Shuffle stages**: Non-source stages with repartition also have ordering issues

---

## Solution Options Evaluated

### Option A: Split Plan Persistence
- Save complete split plan on first run
- Restore from saved plan on restart
- **Problem**: Still need to handle data source/config changes

### Option B: Materialize Points
- Insert explicit materialization at key stages
- Recovery granularity: stage level, not split level
- **Pros**: Simple, reliable
- **Cons**: Storage overhead, coarser recovery

### Option C: Stream-Based Architecture
- Use persistent stream storage between stages
- Each record has unique, monotonic offset (SeqNum)
- Recovery: continue from last committed offset
- **Pros**: Deterministic, no split ordering issues
- **Cons**: Additional dependency

### Option D: At-Least-Once + Idempotent Sink
- Accept possible duplicate processing
- Sink uses upsert (not insert)
- **Pros**: Simple implementation
- **Cons**: Not suitable for expensive operations (GPU, paid APIs)

**Conclusion**: Option D is NOT suitable. Need precise recovery → **Option C (Stream-Based)**.

---

## Key Requirements

| Requirement | Priority | Notes |
|-------------|----------|-------|
| **No duplicate GPU inference** | 🔴 Critical | GPU compute is expensive |
| **No duplicate API calls** | 🔴 Critical | Pay-per-call billing |
| **No data loss** | 🔴 Critical | Data completeness |
| **High resource utilization** | 🔴 Critical | Time = money |
| **Handle large binaries** | 🟡 Important | Video/image multimodal data |
| **Embeddable queue** | 🟡 Important | Minimize external dependencies |
| **Extensible design** | 🟡 Important | Easy to swap implementations |

---

## Design Evolution and Rationale

### Why Not PyO3 Bindings for Tansu?

Initial plan was to create PyO3 bindings for `tansu-storage` Rust crate. This was rejected because:

1. **Tansu is designed as standalone broker**: The `tansu-storage` crate is the storage layer, but Tansu's queue semantics (offset management, consumer groups, etc.) are in the broker layer.

2. **Complex binding work**: Would need to bind not just storage, but the entire broker logic including:
   - Topic/partition management
   - Consumer group coordination
   - Offset commit/fetch semantics
   - Message serialization

3. **Maintenance burden**: Fork and maintain a complex binding layer.

### Why Not Fork Tansu?

Considered forking Tansu to make it embeddable. Rejected because:

1. **Large codebase**: Tansu is a full Kafka-compatible broker
2. **Divergence risk**: Hard to keep in sync with upstream
3. **Overkill**: We don't need Kafka compatibility

### Selected Approach: Tansu as Subprocess

**Decision**: Start Tansu binary as subprocess managed by master actor.

**Rationale**:
1. **Zero modification**: Use Tansu as-is, benefit from upstream improvements
2. **Clean separation**: Queue is external service, clear API boundary
3. **Extensible**: Easy to swap for other implementations later
4. **Production-ready**: Tansu is mature, supports S3/PostgreSQL/SQLite backends

### Architecture Change: Worker Pull Model

**Previous design** (master-master scheduling):
```
Worker(N-1) → Master(N-1) → Master(N) → Worker(N)
                  ↓
           Master schedules splits to downstream master
```

**Problems with previous design**:
- Master-to-master communication overhead
- Master becomes bottleneck for scheduling
- Complex coordination logic

**New design** (worker direct pull):
```
Worker(N-1) → Master(N-1).output_queue ← Worker(N) directly pulls
                  ↓
           Master only manages its own workers' output
```

**Benefits**:
1. **Simpler master role**: Only maintains output queue, no downstream scheduling
2. **Direct data flow**: Workers pull directly from upstream queue
3. **Better scalability**: No master bottleneck
4. **Natural backpressure**: Workers pull at their own pace

---

## Final Architecture

### Component Roles

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              STAGE N-1                                           │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        Master(N-1)                                       │    │
│  │                                                                          │    │
│  │  Responsibilities:                                                       │    │
│  │  - Manage Tansu subprocess lifecycle                                     │    │
│  │  - Maintain output queue (topic: stage_N-1_output)                       │    │
│  │  - Track worker status                                                   │    │
│  │  - NO downstream scheduling (workers pull directly)                      │    │
│  │                                                                          │    │
│  │  ┌──────────────────────────────────────────────────────────────────┐   │    │
│  │  │  Tansu Broker (subprocess)                                        │   │    │
│  │  │  - Storage: S3 / SQLite / Memory                                  │   │    │
│  │  │  - Kafka-compatible protocol                                       │   │    │
│  │  │  - Offset tracking built-in                                        │   │    │
│  │  │  - ID generation built-in (offset = message ID)                   │   │    │
│  │  └──────────────────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│        ↑ produce                                    ↓ fetch (downstream workers)│
│        │                                            │                            │
│  ┌─────┴─────┐  ┌───────────┐  ┌───────────┐       │                            │
│  │ Worker 1  │  │ Worker 2  │  │ Worker K  │       │                            │
│  │ (produce) │  │ (produce) │  │ (produce) │       │                            │
│  └───────────┘  └───────────┘  └───────────┘       │                            │
│                                                     │                            │
└─────────────────────────────────────────────────────┼────────────────────────────┘
                                                      │
                                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              STAGE N                                             │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        Master(N)                                         │    │
│  │  - Manages its own Tansu subprocess                                      │    │
│  │  - Workers PULL from upstream Master(N-1)'s queue                        │    │
│  │  - Workers PRODUCE to this master's queue                                │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐                                    │
│  │ Worker 1  │  │ Worker 2  │  │ Worker M  │                                    │
│  │           │  │           │  │           │                                    │
│  │ 1. Pull from Master(N-1) queue (input)                                       │
│  │ 2. Process data                                                              │
│  │ 3. Produce to Master(N) queue (output)                                       │
│  │ 4. Commit offset to upstream                                                 │
│  └───────────┘  └───────────┘  └───────────┘                                    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. Worker(N) requests batch from Master(N-1).queue
   - Uses Kafka client (aiokafka) to fetch from Tansu
   - Offset tracked per consumer group

2. Worker(N) processes data
   - GPU inference / API call / transformation

3. Worker(N) produces output to Master(N).queue
   - Write to local master's Tansu topic

4. Worker(N) commits input offset
   - Only after output is persisted
   - Guarantees exactly-once (with idempotent downstream)

5. On crash recovery:
   - Worker restarts, reads committed offset
   - Continues from last committed position
   - Output already in downstream queue (not lost)
```

### Message Content

Messages only contain **references**, not actual data:

```python
@dataclass
class QueueMessage:
    """Message stored in Tansu queue"""
    message_id: int          # Auto-generated by Tansu (offset)
    split_id: str            # Unique identifier for the data unit
    data_ref: str            # S3 URI or Ray ObjectRef
    metadata: dict           # Additional info (timestamps, etc.)
    
    # Actual payload is in S3/ObjectStore, NOT in queue
```

**Rationale**:
- Queue stores millions of messages → must be lightweight
- Actual data (video, images) in S3
- Queue only coordinates, doesn't transfer bulk data

---

## Extensibility Design

### Queue Backend Interface

```python
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class Record:
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp: int

class QueueBackend(ABC):
    """Abstract interface for message queue backends.
    
    Implementations:
    - TansuBackend: Tansu subprocess (current)
    - MemoryBackend: In-memory for lightweight stages
    - Future: tansu-py bindings, custom Rust queue, etc.
    """
    
    @abstractmethod
    async def start(self) -> None:
        """Start the queue backend"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the queue backend"""
        pass
    
    @abstractmethod
    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic"""
        pass
    
    @abstractmethod
    async def produce(self, topic: str, value: bytes, key: Optional[bytes] = None) -> int:
        """Produce a message, return offset"""
        pass
    
    @abstractmethod
    async def fetch(self, topic: str, offset: int, max_records: int = 100) -> List[Record]:
        """Fetch records from offset"""
        pass
    
    @abstractmethod
    async def commit_offset(self, group: str, topic: str, offset: int) -> None:
        """Commit consumer offset"""
        pass
    
    @abstractmethod
    async def get_committed_offset(self, group: str, topic: str) -> Optional[int]:
        """Get last committed offset"""
        pass
```

### Tansu Backend Implementation

```python
import asyncio
import subprocess
from pathlib import Path

class TansuBackend(QueueBackend):
    """Tansu subprocess-based queue backend"""
    
    def __init__(
        self,
        storage_url: str = "memory://",  # or "s3://bucket/", "sqlite://path"
        port: int = 9092,
        data_dir: Optional[Path] = None,
    ):
        self.storage_url = storage_url
        self.port = port
        self.data_dir = data_dir
        self._process: Optional[subprocess.Popen] = None
        self._client = None  # aiokafka client
    
    async def start(self) -> None:
        """Start Tansu broker subprocess"""
        cmd = [
            "tansu", "broker",
            "--storage", self.storage_url,
            "--port", str(self.port),
        ]
        if self.data_dir:
            cmd.extend(["--data-dir", str(self.data_dir)])
        
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        
        # Wait for broker to be ready
        await self._wait_for_ready()
        
        # Initialize Kafka client
        from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
        self._producer = AIOKafkaProducer(
            bootstrap_servers=f"localhost:{self.port}"
        )
        await self._producer.start()
    
    async def stop(self) -> None:
        """Stop Tansu broker"""
        if self._producer:
            await self._producer.stop()
        if self._process:
            self._process.terminate()
            self._process.wait()
    
    async def produce(self, topic: str, value: bytes, key: Optional[bytes] = None) -> int:
        """Produce message via Kafka protocol"""
        result = await self._producer.send_and_wait(topic, value, key=key)
        return result.offset
    
    # ... other methods using aiokafka
```

### Future Extension Options

```python
# Option 1: In-memory backend for lightweight stages
class MemoryBackend(QueueBackend):
    """Fast in-memory queue, no persistence"""
    pass

# Option 2: Future tansu-py bindings (if available)
class TansuPyBackend(QueueBackend):
    """Direct Rust bindings to tansu-storage"""
    pass

# Option 3: Custom lightweight queue
class SlateDBQueueBackend(QueueBackend):
    """Custom queue built on SlateDB (S3-native KV store)"""
    pass

# Option 4: Other message queues
class RedpandaBackend(QueueBackend):
    """Redpanda subprocess"""
    pass
```

### Stage Configuration

```python
@dataclass
class StageConfig:
    """Configuration for a pipeline stage"""
    
    # Queue backend selection
    queue_backend: Literal["tansu", "memory", "tansu-py", "custom"] = "tansu"
    
    # Tansu-specific options
    tansu_storage_url: str = "memory://"  # "s3://bucket/", "sqlite://..."
    tansu_port: int = 9092
    
    # Performance tuning
    batch_size: int = 100
    fetch_timeout_ms: int = 1000
    
    # Recovery options
    enable_persistence: bool = True  # False for lightweight stages
```

---

## Exactly-Once Semantics

### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                    EXACTLY-ONCE PROCESSING                          │
│                                                                     │
│  Input Queue (Tansu)              Output Queue (Tansu)              │
│  ┌─────────────────┐              ┌─────────────────┐               │
│  │ offset=0: msg_a │              │ offset=0: out_a │               │
│  │ offset=1: msg_b │  ──process──>│ offset=1: out_b │               │
│  │ offset=2: msg_c │              │ offset=2: out_c │               │
│  │ offset=3: msg_d │              │                 │               │
│  └─────────────────┘              └─────────────────┘               │
│         ▲                                                           │
│         │                                                           │
│  committed_offset = 2  (persisted in Tansu)                         │
│                                                                     │
│  On restart:                                                        │
│  1. Read committed_offset = 2                                       │
│  2. Start consuming from offset 3 (msg_d)                           │
│  3. Output out_a, out_b, out_c already in output queue              │
│  4. No duplicate processing!                                        │
│                                                                     │
│  Key: Output is persisted BEFORE input offset is committed          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Worker Processing Loop

```python
async def worker_loop(self):
    """Worker main loop with exactly-once semantics"""
    
    # Get last committed offset
    offset = await self.input_queue.get_committed_offset(
        group=self.consumer_group,
        topic=self.input_topic
    ) or 0
    
    while True:
        # 1. Fetch batch from input queue
        records = await self.input_queue.fetch(
            topic=self.input_topic,
            offset=offset,
            max_records=self.batch_size
        )
        
        if not records:
            await asyncio.sleep(0.1)
            continue
        
        # 2. Process each record
        for record in records:
            result = await self.process(record)
            
            # 3. Write output FIRST (must be durable)
            await self.output_queue.produce(
                topic=self.output_topic,
                value=result
            )
        
        # 4. Commit input offset AFTER output is persisted
        last_offset = records[-1].offset + 1
        await self.input_queue.commit_offset(
            group=self.consumer_group,
            topic=self.input_topic,
            offset=last_offset
        )
        
        offset = last_offset
```

---

## Hybrid Mode: Memory + Persistent Queues

### Stage Classification

| Stage Type | Queue Backend | Recovery Behavior |
|------------|--------------|-------------------|
| **Source** | Tansu | Resume from offset |
| **Expensive** (GPU/API) | Tansu | Resume from offset |
| **Lightweight** (filter/format) | Memory | Re-process from upstream |
| **Sink** | - | Resume from offset |

### Why Hybrid?

1. **Lightweight stages** (filter, format): Processing cost < 1ms per record
   - Memory queue is faster (no disk I/O)
   - Re-processing on crash is acceptable
   - No need to pay persistence overhead

2. **Expensive stages** (GPU, API): Processing cost = seconds to minutes
   - MUST persist output before commit
   - Cannot afford re-processing
   - Worth the persistence overhead

---

## Implementation Roadmap

### Phase 1: Tansu Subprocess Integration
1. Create `TansuBackend` class wrapping subprocess + aiokafka
2. Implement `QueueBackend` interface
3. Add lifecycle management (start/stop with master)

### Phase 2: Worker Pull Model
1. Modify workers to pull directly from upstream master's queue
2. Remove master-to-master scheduling logic
3. Simplify master to only manage output queue

### Phase 3: Hybrid Mode
1. Add `MemoryBackend` for lightweight stages
2. Per-stage queue backend configuration
3. Recovery logic aware of backend type

### Phase 4: Future Extensions (as needed)
1. tansu-py bindings (if Tansu provides)
2. Custom SlateDB-based queue
3. Other backends

---

## Test Plan

### 1. Correctness Tests

#### 1.1 Unit Tests

| Test Case | Description | Expected Result |
|-----------|-------------|-----------------|
| `test_queue_backend_interface` | Verify interface contract | All methods callable |
| `test_tansu_backend_lifecycle` | Start/stop subprocess | Clean start/stop, no zombies |
| `test_produce_fetch_roundtrip` | Produce N messages, fetch all | All messages retrieved in order |
| `test_offset_commit_persist` | Commit offset, restart, verify | Offset persists across restarts |
| `test_memory_backend_basic` | In-memory queue operations | FIFO order maintained |

#### 1.2 Exactly-Once Semantics Tests

| Test Case | Description | Expected Result |
|-----------|-------------|-----------------|
| `test_crash_before_commit` | Kill worker before offset commit | Message reprocessed on restart |
| `test_crash_after_produce` | Kill after output, before commit | Output exists, will reprocess (idempotent) |
| `test_duplicate_detection` | Same message processed twice | Downstream handles idempotently |
| `test_offset_recovery` | Restart from committed offset | No messages skipped or duplicated |

#### 1.3 Integration Tests

| Test Case | Description | Expected Result |
|-----------|-------------|-----------------|
| `test_two_stage_pipeline` | Source → GPU → Sink | All records processed exactly once |
| `test_hybrid_pipeline` | Tansu → Memory → Tansu | Lightweight stages work correctly |
| `test_worker_pull_model` | Workers pull from upstream master | Data flows correctly |
| `test_master_crash_recovery` | Kill master, restart | Queue state preserved |

### 2. Performance Specifications

#### 2.1 Target Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| **Throughput** | ≥10K msg/s (small messages) | Benchmark with 1KB payloads |
| **Latency (p50)** | ≤10ms (memory), ≤100ms (S3) | End-to-end timing |
| **Latency (p99)** | ≤50ms (memory), ≤500ms (S3) | Percentile measurement |
| **Recovery time** | ≤5s for 1M messages | Time from crash to resume |
| **Memory overhead** | ≤100MB per stage | RSS measurement |

#### 2.2 Benchmark Scenarios

```python
# Benchmark configuration
BENCHMARK_CONFIGS = [
    # Small messages, high throughput
    {"msg_size": 1024, "batch_size": 100, "num_messages": 100_000},
    
    # Large references (typical use case)
    {"msg_size": 256, "batch_size": 50, "num_messages": 1_000_000},
    
    # Stress test
    {"msg_size": 4096, "batch_size": 200, "num_messages": 10_000_000},
]
```

### 3. Performance Validation

#### 3.1 Micro-benchmarks

```python
@pytest.mark.benchmark
async def test_produce_throughput(benchmark, backend):
    """Measure raw produce throughput"""
    async def produce_batch():
        for _ in range(1000):
            await backend.produce("test", b"x" * 1024)
    
    result = benchmark(produce_batch)
    assert result.stats.mean < 1.0  # < 1s for 1000 messages

@pytest.mark.benchmark
async def test_fetch_throughput(benchmark, backend):
    """Measure raw fetch throughput"""
    # Pre-populate
    for i in range(10000):
        await backend.produce("test", b"x" * 1024)
    
    async def fetch_all():
        offset = 0
        while True:
            records = await backend.fetch("test", offset, 100)
            if not records:
                break
            offset = records[-1].offset + 1
    
    result = benchmark(fetch_all)
    assert result.stats.mean < 2.0  # < 2s for 10K messages
```

#### 3.2 End-to-End Pipeline Benchmark

```python
async def benchmark_pipeline(num_stages: int, num_workers: int, num_messages: int):
    """
    Benchmark complete pipeline performance.
    
    Metrics collected:
    - Total processing time
    - Throughput (messages/second)
    - Per-stage latency distribution
    - Memory usage
    - CPU utilization
    """
    pipeline = create_benchmark_pipeline(num_stages, num_workers)
    
    start = time.time()
    await pipeline.run(num_messages)
    elapsed = time.time() - start
    
    return {
        "total_time": elapsed,
        "throughput": num_messages / elapsed,
        "messages": num_messages,
    }
```

### 4. Distributed Scenario Tests

#### 4.1 Multi-Node Setup

```
┌─────────────────────────────────────────────────────────────────┐
│                    TEST CLUSTER TOPOLOGY                         │
│                                                                  │
│  Node 1 (Head)              Node 2                 Node 3        │
│  ┌───────────────┐          ┌───────────────┐      ┌───────────┐ │
│  │ Ray Head      │          │ Ray Worker    │      │ Ray Worker│ │
│  │ Master(0)     │ ◄──────► │ Workers(0)    │      │ Workers(0)│ │
│  │ Tansu(0)      │          │               │      │           │ │
│  └───────────────┘          └───────────────┘      └───────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.2 Distributed Test Cases

| Test Case | Setup | Description |
|-----------|-------|-------------|
| `test_cross_node_fetch` | 2 nodes | Worker on node B fetches from master on node A |
| `test_node_failure` | 3 nodes | Kill one worker node, verify recovery |
| `test_master_failover` | 2 nodes | Kill master node, restart on different node |
| `test_network_partition` | 3 nodes | Simulate network split, verify consistency |

#### 4.3 Chaos Testing

```python
class ChaosTest:
    """Chaos engineering tests for distributed scenarios"""
    
    async def test_random_worker_kills(self):
        """Randomly kill workers during processing"""
        pipeline = create_pipeline()
        
        async def chaos_monkey():
            while not pipeline.done:
                await asyncio.sleep(random.uniform(1, 5))
                worker = random.choice(pipeline.workers)
                await worker.kill()
                await asyncio.sleep(1)
                await worker.restart()
        
        asyncio.create_task(chaos_monkey())
        await pipeline.run(100_000)
        
        # Verify: all messages processed exactly once
        assert pipeline.output_count == 100_000
    
    async def test_tansu_restart_during_processing(self):
        """Restart Tansu broker mid-processing"""
        # Verify: no data loss, workers reconnect
        pass
    
    async def test_s3_latency_spike(self):
        """Inject S3 latency, verify backpressure works"""
        pass
```

### 5. Test Infrastructure

#### 5.1 Test Fixtures

```python
@pytest.fixture
async def tansu_backend():
    """Provide a fresh Tansu backend for each test"""
    backend = TansuBackend(storage_url="memory://", port=19092)
    await backend.start()
    yield backend
    await backend.stop()

@pytest.fixture
async def memory_backend():
    """Provide a fresh memory backend for each test"""
    backend = MemoryBackend()
    await backend.start()
    yield backend
    await backend.stop()

@pytest.fixture
def ray_cluster():
    """Provide a Ray cluster for distributed tests"""
    ray.init(num_cpus=4)
    yield
    ray.shutdown()
```

#### 5.2 CI Pipeline

```yaml
# .github/workflows/test.yml
test-queue-backends:
  runs-on: ubuntu-latest
  steps:
    - name: Unit tests
      run: pytest tests/queue/ -v
    
    - name: Integration tests
      run: pytest tests/integration/ -v --timeout=300
    
    - name: Benchmark tests
      run: pytest tests/benchmark/ -v --benchmark-json=results.json
    
    - name: Check performance regression
      run: python scripts/check_benchmark.py results.json
```

---

## Open Questions

1. **Tansu S3 Performance**: What's the latency with S3 backend?
2. **Topic Cleanup**: How to GC old messages after processing?
3. **Multi-partition**: Need partitions for parallelism?
4. **Cross-node**: How do workers connect to remote master's Tansu?

---

## Appendix: Design Discussion Summary

### Why Stream-Based Over Checkpoint-Based?

The original checkpoint design tracked `completed_splits` by split_id. This fails because:
- Split IDs are generated at runtime
- Non-deterministic sources (Spark) produce different orders on restart
- Checkpoint says "split_0 done" but split_0 is different data!

Stream-based design solves this:
- Messages have monotonic offsets (assigned by queue)
- Offsets are stable across restarts
- "Resume from offset 42" always means the same thing

### Why Not Pure In-Memory Queue?

Considered using Ray's built-in queues or Python queues. Problems:
- No persistence: crash = data loss
- No offset tracking: can't resume
- Ray remote calls: high overhead for millions of messages

### Why Subprocess Over Embedded?

Options considered:
1. **PyO3 bindings**: Complex, high maintenance
2. **Fork Tansu**: Divergence risk, overkill
3. **Subprocess**: Clean, production-ready, upgradeable

Subprocess wins because:
- Zero code changes to Tansu
- Benefit from Tansu upstream improvements
- Easy to swap for other brokers later
- Clear process boundary and failure isolation

### Why Worker Pull Over Master Push?

Previous design had master-to-master coordination. Problems:
- Master becomes scheduling bottleneck
- Complex coordination logic
- Extra hop in data path

Worker pull model:
- Workers directly connect to upstream queue
- Master only manages local output queue
- Simpler, more scalable

---

## Implementation Progress (December 6, 2025)

### Completed Work

#### 1. Queue Backend Infrastructure (`solstice/queue/`)

| File | Description | Status |
|------|-------------|--------|
| `backend.py` | `QueueBackend` abstract interface | ✅ Done |
| `memory.py` | `MemoryBackend` - in-process queue | ✅ Done, 23 tests |
| `ray_backend.py` | `RayBackend` - shared queue via Ray actor | ✅ Done |
| `tansu.py` | `TansuBackend` - subprocess broker | ✅ Done (memory mode) |

**Key interfaces:**
```python
class QueueBackend(ABC):
    async def produce(topic, value, key) -> int  # Returns offset
    async def fetch(topic, offset, max_records) -> List[Record]
    async def commit_offset(group, topic, offset)
    async def get_committed_offset(group, topic) -> Optional[int]
```

#### 2. Stage Master V2 (`solstice/core/stage_master_v2.py`)

| Component | Description | Status |
|-----------|-------------|--------|
| `StageMasterV2` | Simplified master, manages output queue only | ✅ Done |
| `StageWorkerV2` | Self-scheduling worker, pulls from upstream | ✅ Done |
| `StageConfigV2` | Config with queue_type selection | ✅ Done |
| `QueueEndpoint` | Serializable endpoint for workers | ✅ Done |

**Architecture change:**
```
V1 (Old): Master-to-Master Push
  [Master1] --push--> [Master2] --push--> [Master3]

V2 (New): Worker Pull
  [Queue1] <--pull-- [Workers] --produce--> [Queue2]
```

#### 3. Runner V2 (`solstice/runtime/ray_runner_v2.py`)

| Component | Description | Status |
|-----------|-------------|--------|
| `RayJobRunnerV2` | Async runner using V2 architecture | ✅ Done |
| `run_pipeline()` | Convenience function | ✅ Done |
| Topological ordering | Stage initialization order | ✅ Done |

#### 4. Legacy Deprecation

| File | Status | Migration Target |
|------|--------|------------------|
| `stage_master.py` | ⚠️ Deprecated | `stage_master_v2.py` |
| `worker.py` | ⚠️ Deprecated | `StageWorkerV2` |
| `output_buffer.py` | ⚠️ Deprecated | `solstice.queue` |

### Test Coverage

| Test File | Tests | Status |
|-----------|-------|--------|
| `test_queue_backend.py` | 25 | ✅ All pass |
| `test_stage_master_v2.py` | 13 | ✅ All pass |
| `test_pipeline_v2.py` | 7 | ✅ All pass |
| `test_tansu_s3.py` | 1 memory, 3 S3 skipped | ✅ Memory pass |

### Git Commits (feat-backpressure branch)

```
6d93893 Fix TansuBackend and add S3 test infrastructure
d260ce7 Mark legacy modules as deprecated, add v2 exports
ff95f12 Add RayJobRunnerV2 and end-to-end pipeline tests
7c6d0f1 Add stage_master_v2 with worker pull model architecture
f5ec91b Add queue backend infrastructure for stream-based architecture
```

---

## Remaining Work

### High Priority

1. **Tansu S3 Backend** ✅ RESOLVED
   - **Root cause**: Volcengine TOS requires virtual-hosted style S3 access, but Tansu's object_store uses path-style
   - **Solution**: Use MinIO or other path-style compatible S3 services
   - **Configuration**: Use `TansuBackend(s3_endpoint="http://minio:9000", s3_access_key=..., s3_secret_key=...)`
   - Tested and working with MinIO Docker container

2. **Integrate V2 with Existing Workflows** ✅ DONE
   - Updated `quickstart.py` to use `RayJobRunnerV2`
   - Updated `workflows/simple_etl.py` to use new Job API
   - Updated `examples/test_video_slice.py` to use new create_job signature

3. **Exactly-Once Processing Loop** ✅ DONE
   - Verified: output is persisted BEFORE input offset is committed
   - Added `test_crash_recovery.py` with 5 tests for:
     - Offset tracking, resume from committed offset
     - Crash before commit causes reprocessing (at-least-once)
     - Idempotent processing achieves exactly-once

### Medium Priority

4. **Multi-Stage Pipeline Integration Test** ✅ DONE
   - Added `TestMultiStagePipeline` class with 4 tests
   - Verified DAG structure, queue topology, parallel workers
   - Total: 11 pipeline tests pass

5. **Performance Benchmarks** ✅ DONE
   - MemoryBackend: **2M msg/s** produce, 125K batch, 40K fetch
   - RayBackend: **2K msg/s** produce (remote overhead), **100K msg/s** fetch
   - Exceeds 10K msg/s target for batch operations

6. **Topic Cleanup / GC** ✅ DONE
   - Added `truncate_before(topic, offset)` - delete records before offset
   - Added `get_min_committed_offset(topic)` - find safe GC point
   - Implemented in MemoryBackend and RayBackend

7. **Remove V2 Suffix and Replace Legacy Code**
   - After V2 is validated stable, perform cleanup:
   - **Files to delete**:
     - `solstice/core/stage_master.py` (legacy master)
     - `solstice/core/worker.py` (legacy worker)
     - `solstice/core/output_buffer.py` (legacy buffer)
     - `solstice/runtime/ray_runner.py` (legacy runner)
   - **Files to rename**:
     - `stage_master_v2.py` → `stage_master.py`
     - `ray_runner_v2.py` → `ray_runner.py`
   - **Classes to rename**:
     - `StageMasterV2` → `StageMaster`
     - `StageWorkerV2` → `StageWorker`
     - `StageConfigV2` → `StageConfig`
     - `RayJobRunnerV2` → `RayJobRunner`
   - Update all imports and references across codebase

### Low Priority

8. **Multi-Partition Support**
   - Current: single partition (partition=0)
   - Future: parallel partitions for higher throughput

9. **Cross-Node Queue Access**
   - TansuBackend: works (network broker)
   - RayBackend: works (Ray actor serializable)
   - MemoryBackend: single-process only

---

## Key Code Locations

### New V2 Architecture

```
solstice/
├── queue/
│   ├── __init__.py          # Exports: QueueBackend, MemoryBackend, RayBackend, TansuBackend
│   ├── backend.py            # Abstract interface
│   ├── memory.py             # In-process queue
│   ├── ray_backend.py        # Ray actor-based shared queue
│   └── tansu.py              # Tansu subprocess broker
├── core/
│   ├── __init__.py           # Exports V2 classes
│   └── stage_master_v2.py    # StageMasterV2, StageWorkerV2, StageConfigV2
└── runtime/
    ├── __init__.py           # Exports RayJobRunnerV2
    └── ray_runner_v2.py      # Async runner
```

### Usage Example

```python
from solstice.core import StageMasterV2, StageConfigV2, QueueType
from solstice.runtime import RayJobRunnerV2, run_pipeline
from solstice.queue import RayBackend, TansuBackend

# Create job
job = Job(job_id="my_pipeline")
job.add_stage(Stage(stage_id="source", operator_config=SourceConfig()))
job.add_stage(Stage(stage_id="transform", operator_config=TransformConfig()),
              upstream_stages=["source"])

# Run with V2 architecture
status = await run_pipeline(job, queue_type=QueueType.RAY)
print(f"Completed in {status.elapsed_time:.2f}s")
```

### Tansu Configuration

```python
# Memory storage (for testing)
backend = TansuBackend(storage_url="memory://", port=9092)

# MinIO S3 storage (production)
# NOTE: Tansu requires path-style S3 access. Use MinIO, Ceph, or AWS with path-style.
# Virtual-hosted style S3 services (like Volcengine TOS) are NOT supported.
backend = TansuBackend(
    storage_url="s3://bucket-name/",
    port=9092,
    s3_endpoint="http://minio:9000",  # MinIO endpoint
    s3_region="us-east-1",
    s3_access_key="minioadmin",
    s3_secret_key="minioadmin",
)

# Alternative: Use environment variables
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT, AWS_REGION
```

### RayBackend for Distributed Testing

```python
# Master creates backend
backend = RayBackend()
await backend.start()
actor_ref = backend.get_actor_ref()

# Worker connects via actor ref (serializable)
worker_backend = RayBackend.from_actor_ref(actor_ref)
await worker_backend.start()

# Both share same queue state
```

---

## Troubleshooting

### Tansu S3 Issues

1. **Feature not enabled**:
   ```
   FeatureNotEnabled { feature: "dynostore", message: "s3://..." }
   ```
   Solution: Build Tansu with `--all-features` or `--features tansu-broker/dynostore`

2. **InvalidPathAccess / 403 Forbidden** (e.g., Volcengine TOS):
   ```
   PermissionDenied { path: "clusters/...", source: Status { status: 403, body: "InvalidPathAccess" } }
   ```
   - Cause: S3 service requires virtual-hosted style, but Tansu uses path-style
   - Solution: Use MinIO or other path-style compatible S3 service
   ```bash
   # Start MinIO with Docker
   docker run -d --name minio -p 9000:9000 -p 9001:9001 \
     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
     minio/minio server /data --console-address ":9001"
   ```

3. **Connection closed after port opens**:
   - Tansu accepts TCP connection but Kafka protocol fails
   - Check S3 credentials and endpoint configuration
   - Ensure bucket exists before starting Tansu

### Queue Tests Hanging

1. Kill any zombie tansu processes: `pkill -9 tansu`
2. Use different port for each test
3. Check `startup_timeout` is sufficient (60s for S3)

---

## Next Steps for New Agent

1. **Read this document** for context
2. **Run tests** to verify current state:
   ```bash
   cd /root/workspace/nurion/solstice
   pytest tests/test_queue_backend.py tests/test_stage_master_v2.py tests/test_pipeline_v2.py -v
   ```
3. **Debug Tansu S3** if needed:
   - Check `/root/workspace/tansu/` for Tansu source
   - Tansu built with `--all-features`
   - Binary at `/usr/local/bin/tansu`
4. **Integrate V2 with workflows** or continue with remaining work

---

_This document summarizes the design evolution for checkpoint, recovery, and stream-based architecture in Solstice._
_Last updated: December 7, 2025_
