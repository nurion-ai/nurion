# Tansu PyO3 Binding - Embedded Broker Architecture

## Overview

This document describes the design and implementation of `tansu-py`, a PyO3-based Python binding for the Tansu message broker, and the queue abstraction layer that uses it.

**Status**: ✅ COMPLETE - Full implementation with real Tansu broker using `tansu-broker` v0.5.9 from [tansu-io/tansu](https://github.com/tansu-io/tansu).

## Design Principles

### Interface Segregation Principle (ISP)

The queue layer follows the Interface Segregation Principle, providing small, focused protocols instead of one monolithic interface:

```
┌─────────────────────────────────────────────────────────────┐
│                     Protocol Layer                          │
├─────────────────┬─────────────────┬─────────────────────────┤
│  QueueProducer  │  QueueConsumer  │  QueueAdmin             │
│  - produce()    │  - fetch()      │  - create_topic()       │
│  - produce_batch│  - commit_offset│  - delete_topic()       │
│                 │  - get_latest   │  - health_check()       │
└─────────────────┴─────────────────┴─────────────────────────┘
          │                │                    │
          └────────────────┼────────────────────┘
                           │
                  ┌────────▼────────┐
                  │   QueueClient   │  Combined interface
                  │   (all above)   │
                  └─────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    QueueBroker                              │
│  - start()  - stop()  - get_broker_url()  - is_running()   │
└─────────────────────────────────────────────────────────────┘
```

**Benefits:**
- Components implement only what they need
- Clear separation of concerns
- Easier testing and mocking
- Better maintainability

### Component Separation

The implementation separates broker management from client operations:

```
┌──────────────────────────────────────────────────────────────┐
│                     StageMaster Node                         │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐│
│  │   TansuBrokerManager    │  │     TansuQueueClient        ││
│  │   (QueueBroker impl)    │  │     (QueueClient impl)      ││
│  │                         │  │                             ││
│  │   - Starts broker       │  │   - produce/fetch           ││
│  │   - Manages lifecycle   │──┤   - topic management        ││
│  │   - Provides broker_url │  │   - offset tracking         ││
│  └─────────────────────────┘  └─────────────────────────────┘│
└──────────────────────────────────────────────────────────────┘
                                       │ broker_url
                    ┌──────────────────┘
                    ▼
┌──────────────────────────────────────────────────────────────┐
│                     StageWorker Node                         │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              TansuQueueClient                           │ │
│  │              (QueueClient impl)                         │ │
│  │                                                          │ │
│  │   - Connects to remote broker                           │ │
│  │   - produce/fetch messages                              │ │
│  │   - No broker management overhead                       │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

## Architecture

### Component Layers

```
┌──────────────────────────────────────────────────────────────┐
│  Application Layer                                           │
│  ├── StageMaster: TansuBrokerManager + TansuQueueClient     │
│  └── StageWorker: TansuQueueClient (connects to master)     │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  Queue Abstraction Layer (solstice.queue)                    │
│  ├── TansuBrokerManager - Broker lifecycle (QueueBroker)    │
│  ├── TansuQueueClient - Kafka operations (QueueClient)      │
│  └── MemoryBackend - In-memory testing (QueueClient)        │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  Python Binding Layer (tansu_py)                             │
│  ├── TansuBroker - Embedded broker wrapper                  │
│  ├── BrokerConfig - Configuration dataclass                 │
│  └── BrokerEventHandler - Lifecycle callbacks               │
└───────────────────────────────┬──────────────────────────────┘
                                │ PyO3 FFI
┌───────────────────────────────▼──────────────────────────────┐
│  Rust Layer (tansu-py/src)                                   │
│  ├── Tokio runtime management                               │
│  ├── Thread lifecycle (non-blocking start)                  │
│  └── GIL-safe callback invocation                           │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  Tansu Broker Core (tansu-io/tansu v0.5.9)                  │
│  ├── Kafka protocol implementation                          │
│  ├── Storage backends (memory, S3, PostgreSQL)              │
│  └── Topic/partition management                             │
└──────────────────────────────────────────────────────────────┘
```

## Implementation

### 1. Protocol Definitions (`protocols.py`)

```python
@runtime_checkable
class QueueProducer(Protocol):
    async def produce(self, topic: str, value: bytes, ...) -> int: ...
    async def produce_batch(self, topic: str, values: List[bytes], ...) -> List[int]: ...

@runtime_checkable
class QueueConsumer(Protocol):
    async def fetch(self, topic: str, offset: int, ...) -> List[Record]: ...
    async def commit_offset(self, group: str, topic: str, offset: int, ...) -> None: ...
    async def get_committed_offset(self, group: str, topic: str, ...) -> Optional[int]: ...
    async def get_latest_offset(self, topic: str, ...) -> int: ...

@runtime_checkable
class QueueAdmin(Protocol):
    async def create_topic(self, topic: str, partitions: int = 1) -> None: ...
    async def delete_topic(self, topic: str) -> None: ...
    async def health_check(self) -> bool: ...

@runtime_checkable
class QueueBroker(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def get_broker_url(self) -> str: ...
    def is_running(self) -> bool: ...

@runtime_checkable
class QueueClient(QueueProducer, QueueConsumer, QueueAdmin, Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

### 2. TansuBrokerManager (Broker Lifecycle)

```python
class TansuBrokerManager:
    """Manages embedded Tansu broker lifecycle. Implements QueueBroker."""
    
    def __init__(
        self,
        storage_url: str = "memory://tansu/",
        port: Optional[int] = None,       # Auto-select if None
        host: str = "localhost",
        startup_timeout: float = 30.0,
    ): ...
    
    async def start(self) -> None:
        """Start embedded broker in background thread."""
        config = BrokerConfig(...)
        handler = _BrokerEventHandler(self)
        self._broker = TansuBroker(config, event_handler=handler)
        self._broker.start()
        await asyncio.wait_for(self._ready_event.wait(), timeout=self.startup_timeout)
    
    async def stop(self) -> None:
        """Stop the broker."""
        self._broker.stop()
    
    def get_broker_url(self) -> str:
        """Get broker URL for clients (e.g., 'localhost:9092')."""
        return f"{self.host}:{self.port}"
```

### 3. TansuQueueClient (Kafka Operations)

```python
class TansuQueueClient:
    """Kafka client for Tansu. Implements QueueClient."""
    
    def __init__(self, broker_url: str):
        self.broker_url = broker_url
        self._producer: Optional[AIOKafkaProducer] = None
        self._admin_client: Optional[AIOKafkaAdminClient] = None
        self._consumers: Dict[tuple, AIOKafkaConsumer] = {}
    
    async def start(self) -> None:
        """Connect to broker."""
        self._producer = AIOKafkaProducer(bootstrap_servers=self.broker_url)
        await self._producer.start()
        self._admin_client = AIOKafkaAdminClient(bootstrap_servers=self.broker_url)
        await self._admin_client.start()
    
    async def produce(self, topic: str, value: bytes, ...) -> int:
        result = await self._producer.send_and_wait(topic, value=value)
        return result.offset
    
    async def fetch(self, topic: str, offset: int, ...) -> List[Record]:
        consumer = await self._get_consumer(topic, partition=partition)
        consumer.seek(TopicPartition(topic, partition), offset)
        batch = await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        return [Record(...) for record in batch]
```

### 4. Usage Patterns

#### StageMaster (Broker + Client)
```python
# Start broker
broker = TansuBrokerManager(storage_url="memory://tansu/")
await broker.start()

# Create client
client = TansuQueueClient(broker.get_broker_url())
await client.start()

# Use client
await client.create_topic("stage-output")
await client.produce("stage-output", data)

# Cleanup
await client.stop()
await broker.stop()
```

#### StageWorker (Client Only)
```python
# Connect to master's broker
client = TansuQueueClient(broker_url="master-host:9092")
await client.start()

# Produce/consume
await client.produce("stage-output", data)
records = await client.fetch("stage-input", offset=0)

# Cleanup
await client.stop()
```

## Event Callback System

The PyO3 binding uses callbacks to notify Python of broker lifecycle events:

```python
class _BrokerEventHandler(BrokerEventHandler):
    def __init__(self, manager: TansuBrokerManager):
        self.manager = manager

    def on_started(self, port: int) -> None:
        """Called when broker is ready to accept connections."""
        self.manager._actual_port = port
        self.manager._running = True
        self.manager._ready_event.set()

    def on_stopped(self) -> None:
        """Called when broker stops."""
        self.manager._running = False

    def on_error(self, error: BrokerError) -> None:
        """Called on recoverable errors."""
        logger.warning(f"Broker error: {error.message}")

    def on_fatal(self, error: BrokerError) -> None:
        """Called on fatal errors."""
        logger.error(f"Fatal broker error: {error.message}")
        self.manager._running = False
        self.manager._ready_event.set()  # Unblock waiters
```

## Testing

### Test Results

All tests pass:

```
tests/test_queue_backend.py::TestTansuBrokerManager::test_start_stop PASSED
tests/test_queue_backend.py::TestTansuBrokerManager::test_get_broker_url PASSED
tests/test_queue_backend.py::TestTansuQueueClient::test_health_check PASSED
tests/test_queue_backend.py::TestTansuQueueClient::test_create_topic PASSED
tests/test_queue_backend.py::TestTansuQueueClient::test_produce_fetch PASSED
tests/test_queue_backend.py::TestTansuQueueClient::test_produce_batch PASSED
tests/test_queue_backend.py::TestTansuQueueClient::test_get_latest_offset PASSED
tests/test_queue_backend.py::TestTansuQueueClient::test_commit_and_get_offset PASSED
tests/test_queue_backend.py::TestTansuMultiClient::test_two_clients_communication PASSED
```

### Test Fixture

```python
@pytest_asyncio.fixture
async def tansu_broker_and_client():
    """Provide a Tansu broker and client pair."""
    broker = TansuBrokerManager(storage_url="memory://tansu/")
    await broker.start()
    
    client = TansuQueueClient(broker.get_broker_url())
    await client.start()
    
    yield broker, client
    
    await client.stop()
    await broker.stop()
```

## Performance Considerations

### Startup Time
- Broker startup: ~30s (first time, includes storage initialization)
- Client connection: <1s
- Topic creation: <100ms

### Timeouts
All async operations have timeouts to prevent hangs:
- Broker startup: 30s (configurable)
- Client operations: 10s
- Fetch operations: configurable via `timeout_ms`

### Resource Cleanup
- Consumers are cached per (topic, partition, group_id)
- All clients properly stopped on cleanup
- Port automatically released on broker stop

## Migration from Old Design

### Before (Monolithic TansuBackend)
```python
backend = TansuBackend(
    storage_url="memory://tansu/",
    port=9092,
    blocking=False,
)
await backend.start()  # Starts broker + client
await backend.produce("topic", data)
await backend.stop()  # Stops both
```

### After (Separated Components)
```python
# Master node
broker = TansuBrokerManager(storage_url="memory://tansu/")
await broker.start()
client = TansuQueueClient(broker.get_broker_url())
await client.start()
await client.produce("topic", data)

# Worker node (only needs broker_url)
client = TansuQueueClient(broker_url="master:9092")
await client.start()
records = await client.fetch("topic", offset=0)
```

## Changelog

- **2026-01-02**: Architecture redesign with ISP
  - Created protocol layer (QueueProducer, QueueConsumer, QueueAdmin, QueueBroker)
  - Separated TansuBrokerManager and TansuQueueClient
  - All 9 Tansu tests passing
  
- **2025-12-29**: Initial implementation
  - PyO3 binding project structure
  - Basic broker lifecycle management
  - Event callback system

---

*Last Updated: 2026-01-02*
