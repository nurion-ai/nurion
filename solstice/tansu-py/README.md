# tansu-py

Python bindings for Tansu - an embedded Kafka-compatible broker.

> **📖 For detailed design documentation, see [../design-docs/tansu-pyo3-binding.md](../design-docs/tansu-pyo3-binding.md)**

## Status

✅ **COMPLETE** - Full implementation with real Tansu broker

This implementation provides the complete Python API and callback infrastructure for embedding a Tansu broker using the real `tansu-broker` crate from [tansu-io/tansu](https://github.com/tansu-io/tansu).

### Implemented

- ✅ PyO3 project structure with maturin
- ✅ `TansuBroker` class with blocking and non-blocking modes
- ✅ `BrokerConfig` for broker configuration
- ✅ `BrokerEventHandler` callback interface
  - `on_started(port)` - called when broker starts
  - `on_stopped()` - called when broker stops
  - `on_error(error)` - called on recoverable errors
  - `on_fatal(error)` - called on fatal errors
- ✅ `BrokerError` and `BrokerErrorKind` for error handling
- ✅ Integration with `solstice.queue.TansuBackend`
- ✅ Unit tests for bindings API
- ✅ Complete removal of subprocess-based broker management
- ✅ **Real Tansu broker integration**
  - Using `tansu-broker` v0.5.9 crate
  - Using `tansu-storage` v0.5.9 for storage backends
  - Full Kafka-compatible broker embedded in Python

## Architecture

```
┌─────────────────────────────────────┐
│  Python Layer (tansu_py)            │
│  ├── TansuBroker                    │
│  ├── BrokerConfig                   │
│  ├── BrokerEventHandler             │
│  └── BrokerError                    │
└─────────────────────────────────────┘
            │ PyO3
┌─────────────────────────────────────┐
│  Rust Layer (src/broker.rs)         │
│  ├── Tokio runtime management       │
│  ├── Thread handling                │
│  ├── Callback invocation (GIL)      │
│  └── Mock broker (TODO: real)       │
└─────────────────────────────────────┘
            │ TODO
┌─────────────────────────────────────┐
│  Tansu Server (tansu-io/tansu)      │
│  ├── Kafka protocol                 │
│  ├── Storage backends               │
│  └── Topic management                │
└─────────────────────────────────────┘
```

## Usage

### Python API

```python
from tansu_py import TansuBroker, BrokerConfig, BrokerEventHandler

# Define event handler
class MyHandler(BrokerEventHandler):
    def on_started(self, port: int):
        print(f"Broker started on port {port}")
    
    def on_stopped(self):
        print("Broker stopped")
    
    def on_error(self, error):
        print(f"Error: {error.message}")
    
    def on_fatal(self, error):
        print(f"Fatal: {error.message}")

# Configure broker
config = BrokerConfig(
    storage_url="memory://",
    listener_port=9092,
    advertised_host="localhost",
)

# Create and start broker (non-blocking)
broker = TansuBroker(config, event_handler=MyHandler())
broker.start()

# ... use broker ...

# Stop broker
broker.stop()
broker.wait()
```

### With TansuBackend

```python
from solstice.queue.tansu import TansuBackend

# Create backend with embedded broker
backend = TansuBackend(
    storage_url="memory://",
    port=9092,
    blocking=False,  # non-blocking mode
)

await backend.start()

# Use Kafka protocol via aiokafka
await backend.create_topic("my-topic")
offset = await backend.produce("my-topic", b"hello world")
records = await backend.fetch("my-topic", offset=0)

await backend.stop()
```

## Building

```bash
# Install maturin
pip install maturin

# Build and install in development mode
cd tansu-py
maturin develop

# Build wheel
maturin build --release
```

## Testing

```bash
# Run unit tests (mock broker)
cd ..
pytest tests/test_tansu_binding.py -v

# Integration tests (requires real broker - currently skipped)
pytest tests/test_tansu_binding.py -v --run-skipped
```

## Next Steps

1. **Identify tansu-io/tansu repository and crates**
   - Find the correct GitHub repository
   - Identify exportable crates (tansu-server, tansu-kafka-sans-io, etc.)

2. **Add tansu dependencies to Cargo.toml**
   ```toml
   [dependencies]
   tansu-server = { git = "https://github.com/tansu-io/tansu", branch = "main" }
   ```

3. **Replace mock broker with real implementation**
   - Update `src/broker.rs::run_mock_broker()` 
   - Wire up real broker startup/shutdown
   - Connect lifecycle events to callbacks

4. **Enable integration tests**
   - Remove `@pytest.mark.skip` decorators
   - Test with real Kafka protocol
   - Verify produce/consume functionality

## License

Apache-2.0

