# Queue Implementation Issues To Resolve

_Analysis Date: December 10, 2025_

## Executive Summary

The current queue-based architecture has several critical issues that prevent achieving the exactly-once semantics described in `checkpoint-and-recovery.md`. The most severe problems are:

1. **Offset stored in local memory** - not persisted to Tansu/Kafka
2. **Multiple workers process the same messages** - no coordination mechanism
3. **Data stored in Ray Object Store** - memory only, lost on crash

## Issue Severity Overview

| # | Issue | Severity | Impact |
|---|-------|----------|--------|
| 1 | Offset not persisted | 🔴 Critical | Restart from offset 0 after crash |
| 2 | Data in Ray Object Store (memory) | 🔴 Critical | Data lost on crash |
| 3 | Consumer Group offset not shared | 🔴 Critical | All workers process same messages N times |
| 4 | Multi-worker no coordination | 🔴 Critical | Massive duplicate processing |
| 5 | Worker failure no restart | 🟡 High | Messages may not be fully processed |
| 6 | Exception skips message | 🟡 High | Data loss (At-Most-Once) |
| 7 | Single partition can't parallelize | 🟡 Medium | Multi-worker is meaningless |
| 8 | Payload deletion timing issue | 🟡 Medium | May cause KeyError or orphan data |
| 9 | Lag calculation incorrect | 🟢 Low | Monitoring inaccurate |

---

## Critical Issues

### Issue 1: Offset Not Persisted to Tansu/Kafka

**Design Requirement** (from checkpoint-and-recovery.md):
```
committed_offset = 2  (persisted in Tansu)
```

**Current Implementation** (`tansu.py:554-575`):
```python
async def commit_offset(
    self,
    group: str,
    topic: str,
    offset: int,
) -> None:
    """Commit the consumer offset for a consumer group."""
    # For now, store locally (Tansu supports consumer groups but
    # we use a simpler approach for single-partition topics)
    self._committed_offsets[(group, topic)] = offset  # ← Local dict!

    # TODO: Use Tansu's native consumer group support when needed
```

**Problem**: `_committed_offsets` is an in-memory dict in each process.

**Impact**:
- Worker crash → offset lost
- New worker starts from offset 0
- **All messages reprocessed from beginning**

**Fix**:
```python
async def commit_offset(self, group: str, topic: str, offset: int) -> None:
    consumer = await self._get_consumer_for_group(group, topic)
    tp = TopicPartition(topic, 0)
    await consumer.commit({tp: OffsetAndMetadata(offset, "")})
```

---

### Issue 2: Data Stored in Ray Object Store (Memory Only)

**Design Requirement** (from checkpoint-and-recovery.md):
```
data_ref: str  # S3 URI or Ray ObjectRef
```

**Current Implementation** (`split_payload_store.py:169-174`):
```python
def store(self, key: str, payload: SplitPayload) -> str:
    # Put directly to object store with actor as owner
    ref = ray.put(payload, _owner=self._actor)  # ← Memory storage!
```

**Problem**: Ray Object Store is in-memory, not persisted.

**Impact**:
- Worker crash → data in Object Store may be lost
- Queue message exists but `payload_store.get(key)` returns None
- **Downstream cannot process the message**

**Fix**: For expensive stages (GPU/API), support S3 storage:
```python
# Option: S3-backed payload store for expensive stages
async def store(self, key: str, payload: SplitPayload) -> str:
    if self.persist_to_s3:
        s3_key = await self._upload_to_s3(payload)
        return f"s3://{self.bucket}/{s3_key}"
    else:
        ref = ray.put(payload, _owner=self._actor)
        return f"ray://{ref.hex()}"
```

---

### Issue 3: Consumer Group Offset Not Shared Between Workers

**Design Intent**: Same `consumer_group` workers should coordinate consumption.

**Current Implementation** (`stage_master.py:587-602`):
```python
async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint) -> QueueBackend:
    if endpoint.queue_type == QueueType.TANSU:
        queue = TansuBackend(
            storage_url=endpoint.storage_url,
            port=endpoint.port,
            client_only=True,  # Each worker creates its own instance
        )
```

Each worker has its own `TansuBackend` instance with independent `_committed_offsets`:
```
Worker A: TansuBackend → _committed_offsets = {}
Worker B: TansuBackend → _committed_offsets = {}
Worker C: TansuBackend → _committed_offsets = {}
```

**Impact**:
- All workers call `get_committed_offset()` → all return `None` (or 0)
- **All workers start from offset 0**
- **Every message processed N times** (N = number of workers)

---

### Issue 4: Multi-Worker No Coordination Mechanism

Even if Issue 3 is fixed (shared offset storage), there's still no coordination:

```
Topic: job1_stage1_output (1 partition)

Worker A: fetch offset 0-100
Worker B: fetch offset 0-100  ← Same range!
```

**Problem**:
- Single partition cannot support parallel consumption in Kafka model
- Multiple workers fetching same offset range
- No partition assignment or locking

**Possible Fixes**:
- **Option A**: Use multiple partitions + partition assignment
- **Option B**: Single worker with multiple coroutines
- **Option C**: Implement fetch-level locking (custom solution)
- **Option D**: Use Kafka consumer group protocol properly

---

## High Severity Issues

### Issue 5: Worker Failure No Restart

**Current Implementation** (`stage_master.py:331-355`):
```python
# Check worker status
done_tasks = []
for worker_id, task in list(self._worker_tasks.items()):
    try:
        ready, _ = ray.wait([task], timeout=0.1)
        if ready:
            try:
                result = ray.get(ready[0])
                self.logger.info(f"Worker {worker_id} completed: {result}")
            except Exception as e:
                self.logger.error(f"Worker {worker_id} failed: {e}")
                self._failed = True
            done_tasks.append(worker_id)

# Remove completed workers
for worker_id in done_tasks:
    self._workers.pop(worker_id, None)  # ← Just removed, not restarted!
    self._worker_tasks.pop(worker_id, None)
```

**Problem**:
- Failed worker is removed, not restarted
- If all workers fail, stage ends
- Unprocessed messages may be lost

**Fix**: Add automatic worker restart:
```python
for worker_id in done_tasks:
    if worker_failed[worker_id] and self._running:
        # Restart failed worker
        await self._spawn_worker()
```

---

### Issue 6: Exception Skips Message (At-Most-Once)

**Current Implementation** (`stage_master.py:728-744`):
```python
for record in records:
    try:
        message = QueueMessage.from_bytes(record.value)
        await self._process_message(message)
        self._processed_count += 1
    except Exception as e:
        self._error_count += 1
        # Continue processing - don't block on single errors  ← Skipped!

    offset = record.offset + 1  # ← Offset still increments!

# Later: commit includes the failed message's offset
await self.upstream_queue.commit_offset(...)
```

**Problem**:
- Failed message is skipped
- Offset continues to increment
- Final commit includes failed message offset
- **Failed message is permanently lost**

**Fix Options**:
- **Option A**: Retry N times before skip
- **Option B**: Send to DLQ (Dead Letter Queue)
- **Option C**: Fail entire batch on error
- **Option D**: Track failed offsets separately

---

## Medium Severity Issues

### Issue 7: Single Partition Cannot Truly Parallelize

**Current Implementation**:
```python
await queue.create_topic(self._output_topic)  # Default partitions=1
```

**Problem**:
- Kafka/Tansu consumer model: each partition consumed by one consumer
- Single partition → only one consumer can consume
- Multiple workers with single partition is problematic

**Fix**: Use multiple partitions for parallel consumption:
```python
await queue.create_topic(
    self._output_topic, 
    partitions=self.config.max_workers
)
```

---

### Issue 8: Payload Deletion Timing Issue

**Current Implementation** (`stage_master.py:796-824`):
```python
# 1. Process and store output
output_payload = self.operator.process_split(split, payload)
self.payload_store.store(payload_key, output_payload)

# 2. Send output message
await self.output_queue.produce(self.output_topic, output_message.to_bytes())

# 3. Delete input payload
self.payload_store.delete(message.payload_key)  # ← Deleted here
```

**Problem Scenarios**:

**Scenario A** (with Issue 3 - duplicate processing):
1. Worker A processes msg_1, stores output, sends to queue, deletes input
2. Worker B (also from offset 0) tries to process msg_1
3. Worker B calls `payload_store.get(input_key)` → **KeyError** (deleted by A)

**Scenario B** (crash before commit):
1. Worker stores output payload
2. Worker sends message to queue
3. Worker crashes before commit
4. On restart, message reprocessed
5. Output payload becomes orphan (memory leak)

---

### Issue 9: Lag Calculation Incorrect

**Current Implementation** (`stage_master.py:456-476`):
```python
async def get_input_queue_lag(self) -> int:
    # Create NEW TansuBackend instance each time
    queue = TansuBackend(
        storage_url=self.upstream_endpoint.storage_url,
        port=self.upstream_endpoint.port,
        client_only=True,
    )
    await queue.start()
    
    try:
        latest = await queue.get_latest_offset(self.upstream_topic)
        committed = await queue.get_committed_offset(
            self._consumer_group, self.upstream_topic
        )  # ← Gets from NEW instance's empty dict!
```

**Problem**:
- New TansuBackend instance has empty `_committed_offsets`
- `get_committed_offset` always returns `None`
- Lag = latest - 0 = latest (always full queue size)

---

## Root Cause Analysis

**The core issue**: Offset management is entirely in local memory, not using Tansu/Kafka's Consumer Group functionality.

Design doc states:
> Offset tracking built-in (Tansu)

But implementation has:
```python
# TODO: Use Tansu's native consumer group support when needed
self._committed_offsets[(group, topic)] = offset  # Local memory!
```

This TODO is the root cause of Issues 1, 3, 4, and 9.

---

## Current Semantic Guarantees

| Scenario | Current Behavior | Actual Semantics |
|----------|-----------------|------------------|
| Normal operation | Process → periodic commit | At-Least-Once |
| Exception during processing | Skip message, continue | At-Most-Once (data loss) |
| Worker crash | New worker from offset 0 | Massive duplication |
| Master crash | Queue may persist, Object Store lost | Data incomplete |
| Full job restart | Everything from scratch | No recovery capability |

**Conclusion**: Current implementation is somewhere between At-Least-Once and At-Most-Once, with no crash recovery capability.

---

## Fix Priority

### Priority 1: Critical (Must Fix)

1. **Implement real Kafka offset commit** - Remove the TODO, use consumer.commit()
2. **Fix multi-worker consumption model**:
   - Option A: Multi-partition + partition assignment
   - Option B: Single worker per stage
   - Option C: Centralized offset management

### Priority 2: High

3. **Exception retry mechanism** - Don't skip on first failure
4. **Worker failure restart** - Auto-restart failed workers
5. **Consider S3 payload storage** - For expensive stages

### Priority 3: Medium

6. **Fix payload deletion timing** - Delete after downstream ack
7. **Fix lag calculation** - Use shared offset storage
8. **Add DLQ support** - For permanently failed messages

---

## References

- Design doc: `checkpoint-and-recovery.md`
- Stage master: `solstice/core/stage_master.py`
- Tansu backend: `solstice/queue/tansu.py`
- Payload store: `solstice/core/split_payload_store.py`

---

_This document should be updated as issues are resolved._
