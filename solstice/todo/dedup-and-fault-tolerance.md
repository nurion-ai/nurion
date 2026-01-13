# Deduplication & Fault Tolerance TODO

Track implementation status of deduplication operators and fault tolerance features.

> **Last Updated**: 2025-01-12

---

## ✅ Completed

### Shuffle Framework

- [x] **ShuffleOperator base class** - `operators/shuffle.py`
  - Computes `__target_partition` column
  - Split by partition utility function
  - ✅ Fixed: Removes existing `__target_partition` before adding (2025-01-12)
- [x] **RepartitionOperator** - Basic repartition by hash

### Deduplication Operators

- [x] **HashDedupeOperator** - Exact deduplication by key columns
  - Uses DuckDB for batch-level dedup
  - SlateDB for cross-batch state (partition-scoped)
  - Stateless design - no in-memory cache

### MinHash Operators

- [x] **MinHashComputeOperator** - Compute MinHash signatures
- [x] **CandidatePairOperator** - Generate candidate pairs from LSH bands
  - Stateless design

### Connected Components (Label Propagation)

- [x] **CCInitOperator** - Initialize labels from candidate pairs
- [x] **CCIterateOperator** - One iteration of label propagation
- [x] **CCMessageOperator** - Generate messages for next iteration
- [x] **DedupeByClusterOperator** - Keep one doc per cluster
- [x] **CCIterateMaster** - Self-contained iterative master
  - ⚠️ **Iteration NOT implemented** - currently runs single pass
  - See TODO below for full iteration implementation

### State Management

- [x] **PartitionStateStore protocol** - Synchronous interface
- [x] **SlateDBPartitionStateStore** - SlateDB-backed implementation
  - Per-partition isolation
  - Built-in fencing (single writer)
  - Synchronous API (no async wrappers)

### DuckDB Integration

- [x] **DuckDBEngine** - Vectorized operations
  - `hash_partition`, `aggregate`, `join`, `filter`, `dedupe`

---

## 🚧 In Progress

*None*

---

## 📋 TODO

### High Priority

- [ ] **CCIterateMaster Full Iteration (NOT IMPLEMENTED)**
  - Current status: Runs single pass, no actual iteration
  - ❌ `StageWorker` missing `start_iteration()` method
  - ❌ `StageWorker` missing `output_final_labels()` method
  - ❌ Workers don't call `report_partition_changes()` back to master
  
  **Required for full MinHash dedup:**
  1. Add iteration methods to StageWorker
  2. CCIterateOperator reports changes per partition
  3. Master collects changes, decides convergence
  4. Master triggers next iteration if not converged

- [ ] **Checkpoint Recovery (NOT IMPLEMENTED)**
  - Current status: Scaffolding exists but doesn't work
  - `FsspecCheckpointStorage` can read/write files
  - `recover_from_checkpoint()` loads checkpoint
  - ❌ No code saves checkpoints during execution
  - ❌ Recovered offsets not passed to workers
  - ❌ Workers don't seek to recovered offset
  
  **Options:**
  1. Implement fully (significant work)
  2. Remove scaffolding, implement later when needed

- [ ] **Worker State Store Integration**
  - Workers need to create/acquire state stores
  - Pass state store to operators via `set_state_store()`
  - Release state store on worker shutdown

### Medium Priority

- [ ] **Shuffle Integration in StageWorker**
  - Use `__target_partition` column to route payloads
  - Call `produce(partition=N)` on Tansu queue

- [ ] **Data Skew Detection**
  - Monitor partition sizes during shuffle
  - Alert on significant skew (> 10x difference)
  - Initial draft, refine with production experience

- [ ] **Payload GC**
  - Clean up orphaned payloads in Ray Object Store
  - Track references across stages
  - Initial draft, needs more design

### Low Priority

- [ ] **GroupBy Operator**
  - Build on shuffle framework
  - Support incremental aggregation

- [ ] **Join Operator**
  - Hash join with shuffle
  - Broadcast join for small tables
  - Co-partitioned join optimization

- [ ] **Vector Deduplication**
  - Similar to MinHash but with vector embeddings
  - FAISS or similar for ANN search

---

## 🔄 Design Changes

### 1. Stateless Operators ✅

**Original idea**: Operators maintain in-memory caches

**Current implementation**: 
- Operators are fully stateless
- All state in SlateDB (partition-scoped)
- Enables fault tolerance and elastic scaling

### 2. Synchronous State Store ✅

**Original idea**: Async interface for state store

**Current implementation**:
- Synchronous interface
- SlateDB is embedded, no need for async
- Simpler code in operators

### 3. Self-Contained Iterative Stages ✅

**Original idea**: RayJobRunner orchestrates iterations

**Current implementation**:
- `CCIterateMaster` handles iteration internally
- Each iterative stage is self-contained
- Supports multiple iterative groups in one pipeline

### 4. Single Checkpoint File ✅

**Original idea**: Keep checkpoint history

**Current implementation**:
- Only one `checkpoint.json` file
- Atomic overwrite on save
- Simpler, sufficient for recovery

---

## 📝 Notes

### Checkpoint Recovery Strategy (When Implemented)

```
1. Job starts
2. Load checkpoint from storage
3. For each stage:
   - Get partition offsets from checkpoint
   - Pass offsets to workers
4. Workers:
   - Acquire partition from SlateDB (fencing)
   - Seek queue consumer to offset
   - Resume processing
5. Periodic checkpoint:
   - Collect offsets from all workers
   - Save to checkpoint storage
```

### State vs Checkpoint Distinction

| Aspect | State (SlateDB) | Checkpoint (fsspec) |
|--------|-----------------|---------------------|
| Purpose | Runtime business data | Recovery metadata |
| Data | Seen keys, labels | Offsets, snapshot IDs |
| Access | Random read/write | Sequential write, rare read |
| Volume | High (millions of keys) | Low (KB-MB) |
| Location | Per-partition | Per-job |

---

## References

- Design Docs: `design-docs/`
- Operators: `solstice/operators/`
- State: `solstice/state/`
- Checkpoint: `solstice/checkpoint/`
- Tests: `tests/test_*_operator.py`, `tests/test_connected_components.py`
