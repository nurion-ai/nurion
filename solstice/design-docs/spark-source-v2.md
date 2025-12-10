# Spark Source V2: Direct Queue Integration

_Design document for optimized Spark-to-Solstice data pipeline_
_Created: December 2025_

## 1. Overview

This document describes the design for Spark Source V2 (`sparkv2.py`), an optimized implementation that reduces data transfer overhead by having JVM-side Spark executors write directly to both `SplitPayloadStore` and the Tansu Queue.

### 1.1 Goals

1. **Reduce data path**: Eliminate Python-side intermediary steps in `plan_splits()`
2. **Maintain compatibility**: Work with existing `stage_master.py` without modifications
3. **Single serialization**: Data is serialized once (Spark → Arrow) and stored directly
4. **Leverage Kafka protocol**: Use standard Kafka Java Client to write to Tansu

### 1.2 Non-Goals

- Solving Object Store memory pressure (addressed separately by streaming/spilling)
- Replacing the existing `spark.py` implementation (V1 remains for compatibility)
- Modifying `stage_master.py` worker logic

## 2. Problem Analysis

### 2.1 Current Data Flow (V1)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Current Spark Source V1                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Spark DataFrame (JVM)                                                       │
│       │                                                                      │
│       │ Step 1: _save_spark_df_to_object_store() [JVM]                      │
│       │         - Convert to Arrow IPC bytes                                 │
│       │         - Ray.put() to Object Store                                  │
│       ▼                                                                      │
│  Ray Object Store (ObjectRef[])                                              │
│       │                                                                      │
│       │ Step 2: plan_splits() [Python]                                      │
│       │         - Iterate ObjectRef list                                     │
│       │         - Serialize ObjectRef (cloudpickle + base64)                │
│       │         - Write Split metadata to Queue                              │
│       ▼                                                                      │
│  Source Queue (Split metadata with serialized ObjectRef)                     │
│       │                                                                      │
│       │ Step 3: Worker consumes [Python]                                    │
│       │         - SparkSource.read()                                         │
│       │         - Deserialize ObjectRef                                      │
│       │         - ray.get() from Object Store                               │
│       │         - Convert to SplitPayload                                    │
│       ▼                                                                      │
│  SplitPayload → Downstream                                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Issues with V1

| Issue | Description |
|-------|-------------|
| **Python intermediary** | `plan_splits()` iterates all ObjectRefs, adding latency |
| **Double Object Store access** | JVM writes, Python reads, then stores to `SplitPayloadStore` |
| **Complex serialization** | ObjectRef requires cloudpickle + base64 for JSON transport |
| **Sequential processing** | Python `plan_splits()` is a bottleneck for large DataFrames |

### 2.3 V2 Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Spark Source V2 (Proposed)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Spark DataFrame (JVM)                                                       │
│       │                                                                      │
│       │ Step 1: JVM Executor processes partition                            │
│       │         - Convert to Arrow IPC bytes                                 │
│       │         - Ray.put(bytes, owner=storeActor)                          │
│       │         - Call storeActor.register(key, {ref})  ◄── Cross-language  │
│       │         - Kafka produce to Queue                                     │
│       ▼                                                                      │
│  ┌─────────────────────────┐     ┌─────────────────────────┐               │
│  │  SplitPayloadStore      │     │  Source Queue           │               │
│  │  (Arrow bytes stored)   │     │  (Regular message)      │               │
│  │                         │     │  payload_key: "..."     │               │
│  └─────────────────────────┘     └─────────────────────────┘               │
│       │                                   │                                  │
│       │                                   │                                  │
│       │ Step 2: Worker consumes (unchanged!)                                │
│       │         - payload_store.get(payload_key)                            │
│       │         - Auto-convert Arrow bytes → SplitPayload                   │
│       ▼                                   │                                  │
│  SplitPayload → Downstream ◄──────────────┘                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. Architecture

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Ray Cluster                                     │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     Spark Executor (JVM)                                │ │
│  │                                                                          │ │
│  │   ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │   │              SplitPayloadStoreWriter (NEW)                       │  │ │
│  │   │                                                                  │  │ │
│  │   │   1. ArrowWriter: DataFrame → Arrow IPC bytes                   │  │ │
│  │   │   2. Ray.put(bytes, owner=storeActor)                           │  │ │
│  │   │   3. PyActorHandle.task("register", key, {ref})                 │  │ │
│  │   │   4. KafkaProducer.send(topic, QueueMessage)                    │  │ │
│  │   └─────────────────────────────────────────────────────────────────┘  │ │
│  │               │                              │                          │ │
│  └───────────────┼──────────────────────────────┼──────────────────────────┘ │
│                  │                              │                            │
│                  ▼                              ▼                            │
│  ┌──────────────────────────┐    ┌──────────────────────────────────────┐  │
│  │   SplitPayloadStore      │    │   Tansu Queue (Kafka Protocol)       │  │
│  │   (Ray Actor)            │    │                                      │  │
│  │                          │    │   QueueMessage {                     │  │
│  │   key → ObjectRef        │    │     message_id,                      │  │
│  │   (Arrow bytes)          │    │     split_id,                        │  │
│  │                          │    │     payload_key: "...",  ← non-empty │  │
│  │                          │    │     metadata                         │  │
│  │                          │    │   }                                  │  │
│  └──────────────────────────┘    └──────────────────────────────────────┘  │
│                  │                              │                            │
│                  └──────────────┬───────────────┘                            │
│                                 │                                            │
│                                 ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    Worker (stage_master.py - UNCHANGED)              │  │
│  │                                                                       │  │
│  │   payload_key non-empty → Regular message path                       │  │
│  │   payload = payload_store.get(payload_key)                           │  │
│  │   operator.process_split(split, payload)                             │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Use Regular message path** | `payload_key` is non-empty, worker fetches from `SplitPayloadStore` directly |
| **Store Arrow bytes (not SplitPayload)** | JVM cannot create Python objects; store raw Arrow bytes instead |
| **Auto-convert in `get()`** | `SplitPayloadStore.get()` detects Arrow bytes and converts to `SplitPayload` |
| **Kafka Java Client** | Tansu is Kafka-compatible; use mature Kafka client library |
| **Cross-language actor call** | Ray supports JVM calling Python actor methods via `PyActorHandle.task()` |

## 4. Detailed Design

### 4.1 Message Format Compatibility

The JVM must produce `QueueMessage` JSON identical to Python:

```json
{
  "message_id": "spark_stage_0",
  "split_id": "spark_p0_b0",
  "payload_key": "spark_stage_spark_p0_b0",
  "metadata": {
    "source_stage": "spark_stage",
    "num_records": 1000
  },
  "timestamp": 1702234567.123
}
```

Key difference from V1:
- V1: `payload_key` is empty (Source message), worker calls `SparkSource.read()`
- V2: `payload_key` is non-empty (Regular message), worker calls `payload_store.get()`

### 4.2 JVM Implementation

#### 4.2.1 SplitPayloadStoreWriter.scala

```scala
package org.apache.spark.sql.raydp

import io.ray.api.{ObjectRef, PyActorHandle, Ray}
import io.ray.api.function.PyActorMethod
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.google.gson.Gson
import java.util.{HashMap => JHashMap, Properties}

/**
 * Writes Arrow data directly to SplitPayloadStore and Queue.
 * 
 * This writer:
 * 1. Puts Arrow bytes to Object Store with storeActor as owner
 * 2. Calls storeActor.register() to register the ObjectRef
 * 3. Sends QueueMessage to Tansu via Kafka protocol
 */
class SplitPayloadStoreWriter(
    storeActorName: String,
    queueBootstrapServers: String,
    queueTopic: String,
    stageId: String
) extends Serializable {

  @transient private var storeActor: PyActorHandle = _
  @transient private var kafkaProducer: KafkaProducer[String, Array[Byte]] = _
  @transient private val gson = new Gson()
  
  private var messageCounter = 0

  def start(): Unit = {
    // Get SplitPayloadStore actor handle
    storeActor = Ray.getActor(storeActorName).get().asInstanceOf[PyActorHandle]
    
    // Initialize Kafka producer for Tansu
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, queueBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
              "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
              "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    
    kafkaProducer = new KafkaProducer[String, Array[Byte]](props)
  }

  /**
   * Store Arrow data and send message to queue.
   * 
   * @param arrowBytes Arrow IPC format bytes
   * @param splitId Unique split identifier
   * @param numRecords Number of records in this batch
   * @return Queue offset
   */
  def storeAndSend(
      arrowBytes: Array[Byte],
      splitId: String,
      numRecords: Int
  ): Long = {
    
    // 1. Put to Object Store with storeActor as owner
    val objectRef: ObjectRef[Array[Byte]] = Ray.put(arrowBytes, storeActor)
    
    // 2. Register with SplitPayloadStore actor
    val payloadKey = s"${stageId}_${splitId}"
    val refWrapper = new JHashMap[String, Any]()
    refWrapper.put("ref", objectRef)
    
    // Cross-language actor method call
    val registerResult = storeActor.task(
      PyActorMethod.of("register", classOf[String]),
      payloadKey,
      refWrapper
    ).remote()
    Ray.get(registerResult)  // Wait for registration
    
    // 3. Send QueueMessage (Regular message format)
    val metadata = new JHashMap[String, Any]()
    metadata.put("source_stage", stageId)
    metadata.put("num_records", Integer.valueOf(numRecords))
    
    val message = new JHashMap[String, Any]()
    message.put("message_id", s"${stageId}_${messageCounter}")
    message.put("split_id", splitId)
    message.put("payload_key", payloadKey)  // Non-empty!
    message.put("metadata", metadata)
    message.put("timestamp", java.lang.Double.valueOf(
      System.currentTimeMillis() / 1000.0))
    
    val jsonBytes = gson.toJson(message).getBytes("UTF-8")
    val record = new ProducerRecord[String, Array[Byte]](
      queueTopic, splitId, jsonBytes)
    
    val future = kafkaProducer.send(record)
    val result = future.get()
    
    messageCounter += 1
    result.offset()
  }

  def flush(): Unit = if (kafkaProducer != null) kafkaProducer.flush()
  
  def close(): Unit = {
    if (kafkaProducer != null) {
      kafkaProducer.flush()
      kafkaProducer.close()
    }
  }
  
  def getMessageCount: Int = messageCounter
}
```

#### 4.2.2 Integration with ObjectStoreWriter

Add a new method to `ObjectStoreWriter.scala`:

```scala
/**
 * Save DataFrame to SplitPayloadStore and Queue directly.
 * 
 * This is the V2 entry point that bypasses Python-side plan_splits().
 */
def saveToStoreAndQueue(
    useBatch: Boolean,
    storeActorName: String,
    queueBootstrapServers: String,
    queueTopic: String,
    stageId: String
): Int = {
  // Implementation details in Section 4.2.1
  // Returns total number of messages sent
}
```

### 4.3 Python Implementation

#### 4.3.1 Enhanced SplitPayloadStore.get()

Modify `split_payload_store.py` to auto-convert Arrow bytes:

```python
class RaySplitPayloadStore(SplitPayloadStore):
    
    def get(self, key: str) -> Optional[SplitPayload]:
        ref_wrapper = ray.get(self._actor.get_ref.remote(key))
        if ref_wrapper is None:
            return None
        
        data = ray.get(ref_wrapper["ref"])
        
        # Already a SplitPayload (from Python writers)
        if isinstance(data, SplitPayload):
            return data
        
        # Arrow IPC bytes (from JVM writers)
        if isinstance(data, bytes):
            import pyarrow.ipc as ipc
            import io
            table = ipc.open_stream(io.BytesIO(data)).read_all()
            return SplitPayload.from_arrow(table, split_id=key)
        
        # Arrow Table (direct)
        if isinstance(data, pa.Table):
            return SplitPayload.from_arrow(data, split_id=key)
        
        raise ValueError(f"Unsupported data type in store: {type(data)}")
```

#### 4.3.2 SparkSourceV2 Master (sparkv2.py)

```python
class SparkSourceV2Master(SourceMaster):
    """
    Spark Source V2: JVM writes directly to Store and Queue.
    
    Unlike V1, this master:
    - Does NOT iterate ObjectRefs in plan_splits()
    - Delegates all data writing to JVM-side SplitPayloadStoreWriter
    - Only responsible for Spark initialization and Queue setup
    """
    
    async def start(self) -> None:
        """Start the source master."""
        if self._running:
            return
        
        self.logger.info(f"Starting SparkSourceV2 {self.stage_id}")
        self._start_time = time.time()
        self._running = True
        
        # 1. Create source queue (Tansu)
        self._source_queue = await self._create_source_queue()
        
        # 2. Execute Spark write (JVM writes to Store + Queue)
        splits_count = await self._execute_spark_write()
        self._splits_produced = splits_count
        
        # 3. Create output queue (for downstream)
        self._output_queue = await self._create_queue()
        
        # 4. Set upstream to source queue
        self.upstream_endpoint = self._source_endpoint
        self.upstream_topic = self._source_topic
        
        # 5. Spawn workers
        for i in range(self.config.min_workers):
            await self._spawn_worker()
        
        self.logger.info(
            f"SparkSourceV2 {self.stage_id} started: "
            f"{splits_count} splits, {len(self._workers)} workers"
        )
        
        # 6. Notify workers that source is complete
        self._notify_splits_complete()
    
    async def _execute_spark_write(self) -> int:
        """Execute Spark write via JVM."""
        import raydp
        
        # Initialize Spark
        self._spark = raydp.init_spark(
            app_name=self._config.app_name,
            num_executors=self._config.num_executors,
            executor_cores=self._config.executor_cores,
            executor_memory=self._config.executor_memory,
            configs=self._config.spark_configs,
        )
        
        df = self._config.dataframe_fn(self._spark)
        if self._config.parallelism:
            df = df.repartition(self._config.parallelism)
        
        # Get store actor name
        store_actor_name = self.payload_store._actor._ray_actor_name
        
        # Queue connection info
        queue_bootstrap = f"localhost:{self._source_endpoint.port}"
        
        # Call JVM method
        jvm = df.sql_ctx.sparkSession.sparkContext._jvm
        writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(df._jdf)
        
        count = writer.saveToStoreAndQueue(
            False,  # useBatch
            store_actor_name,
            queue_bootstrap,
            self._source_topic,
            self.stage_id,
        )
        
        self.logger.info(f"JVM write completed: {count} splits")
        return count
    
    def plan_splits(self) -> Iterator[Split]:
        """Not used in V2 - JVM writes directly to queue."""
        raise NotImplementedError(
            "V2 does not use plan_splits(). "
            "JVM writes directly to Store and Queue."
        )
```

### 4.4 Configuration

```python
@dataclass
class SparkSourceV2Config(OperatorConfig):
    """Configuration for Spark Source V2."""
    
    # Spark configuration
    app_name: str = "solstice-spark-v2"
    num_executors: int = 1
    executor_cores: int = 2
    executor_memory: str = "1g"
    spark_configs: Dict[str, str] = field(default_factory=dict)
    dataframe_fn: Optional[DataFrameFactory] = None
    parallelism: Optional[int] = None
    
    # Queue configuration
    tansu_storage_url: str = "memory://"
```

## 5. Cross-Language Actor Communication

### 5.1 Ray Java API for Python Actors

Ray supports calling Python actor methods from Java:

```java
// Get Python actor handle
PyActorHandle actor = (PyActorHandle) Ray.getActor(actorName).get();

// Call Python method
ObjectRef<Object> result = actor.task(
    PyActorMethod.of("method_name", ReturnType.class),
    arg1, arg2, ...
).remote();

// Wait for result
Object value = Ray.get(result);
```

### 5.2 ObjectRef Cross-Language Passing

When passing `ObjectRef` to a Python actor:
- JVM creates `ObjectRef` via `Ray.put(data, owner)`
- The `ObjectRef` is serialized by Ray's internal mechanism
- Python actor receives the same `ObjectRef` reference

This is crucial for the `storeActor.register(key, {"ref": objectRef})` call.

### 5.3 Verification Required

Before implementation, verify:

1. **PyActorMethod invocation**: Can JVM call `_RaySplitPayloadStoreActor.register()`?
2. **ObjectRef passing**: Does the Python actor receive a valid `ObjectRef`?
3. **Owner semantics**: Is `owner=storeActor` properly respected?

Suggested POC test:

```python
# Python side
@ray.remote
class TestActor:
    def __init__(self):
        self.refs = {}
    
    def register(self, key: str, ref_wrapper: dict) -> str:
        self.refs[key] = ref_wrapper["ref"]
        return key
    
    def get(self, key: str):
        return ray.get(self.refs[key])

# JVM side test
val actor = Ray.getActor("test_actor").get().asInstanceOf[PyActorHandle]
val data = "test data".getBytes()
val ref = Ray.put(data, actor)
val wrapper = Map("ref" -> ref).asJava
actor.task(PyActorMethod.of("register", classOf[String]), "key1", wrapper).remote()
```

## 6. Maven Dependencies

Add to `pom.xml`:

```xml
<dependencies>
    <!-- Kafka Client for Tansu -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.6.0</version>
    </dependency>
    
    <!-- JSON serialization -->
    <dependency>
        <groupId>com.google.code.gson</groupId>
        <artifactId>gson</artifactId>
        <version>2.10.1</version>
    </dependency>
</dependencies>
```

## 7. Migration Path

### 7.1 File Structure

```
solstice/operators/sources/
├── spark.py          # V1 (unchanged, for compatibility)
├── sparkv2.py        # V2 (new implementation)
└── source.py         # Base SourceMaster (unchanged)

java/raydp-main/src/main/scala/org/apache/spark/sql/raydp/
├── ObjectStoreWriter.scala        # Add saveToStoreAndQueue()
└── SplitPayloadStoreWriter.scala  # New file
```

### 7.2 Backward Compatibility

- `spark.py` (V1) remains unchanged
- Users can choose V2 by using `SparkSourceV2Config`
- `stage_master.py` works with both V1 and V2

### 7.3 Usage Example

```python
# V1 (existing)
from solstice.operators.sources.spark import SparkSourceConfig

config_v1 = SparkSourceConfig(
    dataframe_fn=lambda spark: spark.read.parquet("/data"),
)

# V2 (new)
from solstice.operators.sources.sparkv2 import SparkSourceV2Config

config_v2 = SparkSourceV2Config(
    dataframe_fn=lambda spark: spark.read.parquet("/data"),
)
```

## 8. Work Estimate

| Task | Estimate | Complexity |
|------|----------|------------|
| **JVM Side** | | |
| SplitPayloadStoreWriter.scala | 1.5 days | Medium |
| Modify ObjectStoreWriter.scala | 0.5 days | Low |
| POC: Cross-language actor call | 1 day | High |
| **Python Side** | | |
| Enhance SplitPayloadStore.get() | 0.5 days | Low |
| SparkSourceV2Master (sparkv2.py) | 0.5 days | Low |
| **Testing** | | |
| Unit tests | 1 day | Medium |
| Integration tests | 1 day | Medium |
| **Benchmark** | | |
| Benchmark setup and V1 baseline | 1 day | Medium |
| V2 benchmark and analysis | 1 day | Medium |

**Total: ~8 days**

## 9. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Cross-language actor call instability | POC test before full implementation |
| ObjectRef ownership issues | Verify `owner=storeActor` works correctly |
| Kafka client version conflicts | Use shaded JAR or version alignment |
| Message format incompatibility | Comprehensive JSON format testing |

## 10. Future Improvements

1. **Streaming mode**: Process partitions as they complete, not wait for all
2. **Backpressure**: JVM-side rate limiting based on queue lag
3. **Arrow Flight alternative**: For scenarios requiring lower latency (see separate design doc)

## Appendix A: Message Flow Comparison

### V1 Flow
```
JVM: DataFrame → Arrow → Object Store (ObjectRef[])
         ↓
Python: plan_splits() iterates ObjectRef[]
         ↓
Python: for each ref: serialize(cloudpickle+base64) → Queue
         ↓
Worker: consume → deserialize → ray.get() → SparkSource.read() → SplitPayload
```

### V2 Flow
```
JVM: DataFrame → Arrow → Object Store (with store actor owner)
                       → storeActor.register(key, ref)
                       → Kafka produce to Queue
         ↓
Worker: consume → payload_store.get(key) → SplitPayload
```

**Eliminated steps in V2:**
- Python `plan_splits()` iteration
- cloudpickle + base64 serialization
- `SparkSource.read()` invocation

## Appendix B: Benchmark Plan

### B.1 Test Environment

| Component | Specification |
|-----------|---------------|
| Cluster | Ray cluster with 4 nodes |
| CPU | 8 cores per node |
| Memory | 32 GB per node |
| Storage | S3 (for Tansu persistence) |
| Spark | 2 executors, 4 cores each, 8GB memory |

### B.2 Test Datasets

| Dataset | Size | Records | Columns | Description |
|---------|------|---------|---------|-------------|
| Small | 100 MB | 1M | 10 | Baseline test |
| Medium | 1 GB | 10M | 20 | Typical workload |
| Large | 10 GB | 100M | 30 | Stress test |
| Wide | 1 GB | 1M | 200 | Column-heavy schema |

Data generation:

```python
def generate_test_data(spark, num_records, num_columns):
    from pyspark.sql.functions import rand, expr
    
    df = spark.range(num_records)
    for i in range(num_columns):
        df = df.withColumn(f"col_{i}", rand() * 1000)
    return df
```

### B.3 Metrics

| Metric | Description | How to Measure |
|--------|-------------|----------------|
| **End-to-end latency** | Time from Spark job start to last worker consuming | `time.time()` around full pipeline |
| **Source stage latency** | Time for source stage to produce all splits | `SparkSourceMaster` timing |
| **Throughput (records/sec)** | Records processed per second | `total_records / elapsed_time` |
| **Throughput (MB/sec)** | Data processed per second | `data_size_mb / elapsed_time` |
| **Object Store memory** | Peak memory usage in Ray Object Store | `ray.cluster_resources()` |
| **CPU utilization** | CPU usage during pipeline | `ray.available_resources()` sampling |
| **GC overhead** | Python garbage collection time | `gc.get_stats()` |

### B.4 Test Scenarios

#### Scenario 1: Source Stage Performance

Measure time for source stage only (no downstream processing):

```python
# V1
start = time.time()
for split in spark_source_master.plan_splits():
    pass
v1_source_time = time.time() - start

# V2
start = time.time()
await spark_source_v2_master._execute_spark_write()
v2_source_time = time.time() - start
```

**Expected improvement**: V2 should be 30-50% faster (no Python iteration overhead)

#### Scenario 2: End-to-End Pipeline

Full pipeline with downstream map operator:

```python
job = Job(
    stages=[
        Stage("spark_source", SparkSourceConfig(...)),  # or V2Config
        Stage("map", MapConfig(fn=lambda x: x)),
        Stage("sink", PrintSinkConfig()),
    ]
)

start = time.time()
await runner.run(job)
elapsed = time.time() - start
```

**Expected improvement**: V2 should be 10-20% faster overall

#### Scenario 3: Memory Pressure

Monitor Object Store memory with large dataset:

```python
import ray

def get_object_store_memory():
    resources = ray.cluster_resources()
    used = ray.available_resources()
    return resources.get("object_store_memory", 0) - used.get("object_store_memory", 0)

# Sample every 1 second during pipeline execution
memory_samples = []
```

**Expected result**: V2 should have similar or slightly lower peak memory

#### Scenario 4: Scalability

Test with varying number of partitions:

| Partitions | V1 Time | V2 Time | Improvement |
|------------|---------|---------|-------------|
| 10 | TBD | TBD | TBD |
| 50 | TBD | TBD | TBD |
| 100 | TBD | TBD | TBD |
| 500 | TBD | TBD | TBD |

**Expected**: V2 improvement should increase with partition count

### B.5 Benchmark Script

```python
# benchmarks/spark_source_benchmark.py

import asyncio
import time
import ray
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class BenchmarkResult:
    version: str
    dataset: str
    partitions: int
    source_time_sec: float
    total_time_sec: float
    records_per_sec: float
    mb_per_sec: float
    peak_memory_mb: float

async def run_benchmark(
    version: str,  # "v1" or "v2"
    num_records: int,
    num_columns: int,
    num_partitions: int,
) -> BenchmarkResult:
    """Run a single benchmark iteration."""
    
    # Setup
    if version == "v1":
        from solstice.operators.sources.spark import SparkSourceConfig
        config_class = SparkSourceConfig
    else:
        from solstice.operators.sources.sparkv2 import SparkSourceV2Config
        config_class = SparkSourceV2Config
    
    config = config_class(
        app_name=f"benchmark-{version}",
        num_executors=2,
        executor_cores=4,
        executor_memory="8g",
        dataframe_fn=lambda spark: generate_test_data(spark, num_records, num_columns),
        parallelism=num_partitions,
    )
    
    # Run pipeline and measure
    start = time.time()
    # ... run pipeline ...
    elapsed = time.time() - start
    
    data_size_mb = estimate_data_size(num_records, num_columns)
    
    return BenchmarkResult(
        version=version,
        dataset=f"{num_records}x{num_columns}",
        partitions=num_partitions,
        source_time_sec=source_elapsed,
        total_time_sec=elapsed,
        records_per_sec=num_records / elapsed,
        mb_per_sec=data_size_mb / elapsed,
        peak_memory_mb=peak_memory,
    )

async def run_all_benchmarks():
    """Run complete benchmark suite."""
    results: List[BenchmarkResult] = []
    
    test_configs = [
        # (records, columns, partitions)
        (1_000_000, 10, 10),
        (1_000_000, 10, 50),
        (10_000_000, 20, 50),
        (10_000_000, 20, 100),
        (100_000_000, 30, 100),
    ]
    
    for records, columns, partitions in test_configs:
        for version in ["v1", "v2"]:
            print(f"Running {version} with {records} records, {partitions} partitions...")
            result = await run_benchmark(version, records, columns, partitions)
            results.append(result)
            
            # Cleanup between runs
            ray.shutdown()
            ray.init()
    
    return results

def print_results(results: List[BenchmarkResult]):
    """Print benchmark results as markdown table."""
    print("| Version | Dataset | Partitions | Source Time | Total Time | Records/sec | MB/sec | Peak Memory |")
    print("|---------|---------|------------|-------------|------------|-------------|--------|-------------|")
    for r in results:
        print(f"| {r.version} | {r.dataset} | {r.partitions} | "
              f"{r.source_time_sec:.2f}s | {r.total_time_sec:.2f}s | "
              f"{r.records_per_sec:,.0f} | {r.mb_per_sec:.1f} | {r.peak_memory_mb:.0f} MB |")

if __name__ == "__main__":
    ray.init()
    results = asyncio.run(run_all_benchmarks())
    print_results(results)
```

### B.6 Expected Results

Based on the eliminated steps, we expect:

| Metric | V1 Baseline | V2 Expected | Improvement |
|--------|-------------|-------------|-------------|
| Source stage latency | 100% | 50-70% | 30-50% faster |
| End-to-end latency | 100% | 80-90% | 10-20% faster |
| Peak Object Store memory | 100% | 95-100% | Similar |
| CPU utilization (source) | 100% | 60-80% | Lower Python overhead |

### B.7 Acceptance Criteria

V2 is considered successful if:

1. **Source stage latency** is at least **25% faster** than V1
2. **End-to-end latency** is at least **10% faster** than V1
3. **No regression** in memory usage or correctness
4. **Scales better** with partition count (improvement increases with more partitions)

### B.8 Benchmark Schedule

| Phase | Duration | Description |
|-------|----------|-------------|
| Setup | 0.5 days | Prepare test environment and datasets |
| V1 Baseline | 0.5 days | Run all scenarios with V1 |
| V2 Implementation | Per schedule | Implement V2 |
| V2 Benchmark | 0.5 days | Run all scenarios with V2 |
| Analysis | 0.5 days | Compare results, identify issues |

**Total benchmark effort: 2 days**

---

## Appendix C: Implementation Status

_Last updated: December 10, 2025_

### C.1 Completed Tasks

| Task | Status | Notes |
|------|--------|-------|
| **Python Side** | | |
| Enhance `SplitPayloadStore.get()` | ✅ Done | Auto-converts Arrow bytes, handles `_v2arrow:` prefix |
| Create `sparkv2.py` | ✅ Done | SparkSourceV2Config, SparkSourceV2Master |
| Update `__init__.py` exports | ✅ Done | V2 classes exported |
| Update `stage_master.py` | ✅ Done | Added host to QueueEndpoint |
| **JVM Side** | | |
| Create `SplitPayloadStoreWriter.scala` | ✅ Done | Direct Arrow data in Kafka message |
| Modify `ObjectStoreWriter.scala` | ✅ Done | Added `saveToStoreAndQueue()` |
| Add Maven dependencies | ✅ Done | kafka-clients 3.6.0, gson 2.10.1 |
| **Testing** | | |
| Unit tests for V2 Arrow data | ✅ Done | 2 tests passing |
| Config/Master unit tests | ✅ Done | 5 tests passing |
| Integration tests | ✅ Done | 3 tests passing |
| All tests | ✅ Done | **11 tests passing** |

### C.2 Architecture Decision

**Original design**: JVM uses `Ray.put()` with ObjectRef ID embedded in messages.
**Issue**: Python's `ray.ObjectRef()` requires 28-byte format (ObjectId + metadata),
but Java's `ObjectId.getBytes()` only provides 20 bytes. Cross-language serialization
is not directly compatible.

**Final design**: JVM embeds Arrow IPC data directly in Kafka message (base64 encoded).
- `_v2arrow:{base64_arrow_ipc}` format in `payload_key`
- Python `SplitPayloadStore.get()` detects prefix and decodes inline
- Simple, reliable, no ObjectRef serialization issues
- Suitable for typical partition sizes (< 16MB)

### C.3 Files Modified/Created

```
solstice/solstice/core/split_payload_store.py          # Modified: _v2arrow: prefix handling
solstice/solstice/core/stage_master.py                 # Modified: host in QueueEndpoint
solstice/solstice/operators/sources/sparkv2.py         # New: V2 implementation
solstice/solstice/operators/sources/__init__.py        # Modified: V2 exports

solstice/java/raydp-main/src/main/scala/org/apache/spark/sql/raydp/
├── SplitPayloadStoreWriter.scala                      # New: Direct Arrow data writer
└── ObjectStoreWriter.scala                            # Modified: saveToStoreAndQueue()

solstice/java/raydp-main/pom.xml                       # Modified: Kafka + Gson deps

solstice/tests/test_spark_source_v2.py                 # New: V2 tests
```

### C.4 Usage

```python
from solstice.operators.sources.sparkv2 import SparkSourceV2Config

config = SparkSourceV2Config(
    dataframe_fn=lambda spark: spark.read.parquet("/data"),
    num_executors=2,
)
```

### C.5 Next Steps

1. **Benchmark**: Compare V1 vs V2 performance
2. **Large data optimization**: Consider chunking for partitions > 16MB
3. **Production testing**: Validate with real workloads
