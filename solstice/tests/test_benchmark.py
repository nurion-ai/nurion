"""Performance benchmark tests for queue backends.

Target metrics:
- Throughput: ≥10K msg/s (small messages)
- Latency (p50): ≤10ms (memory), ≤100ms (S3)
- Latency (p99): ≤50ms (memory), ≤500ms (S3)
"""

import asyncio
import time
import statistics
import pytest
import ray

from solstice.queue import MemoryBackend, RayBackend
from solstice.core.stage_master import QueueMessage


pytestmark = pytest.mark.asyncio(loop_scope="function")


class BenchmarkMetrics:
    """Collect and report benchmark metrics."""
    
    def __init__(self, name: str):
        self.name = name
        self.latencies: list[float] = []
        self.start_time: float = 0
        self.end_time: float = 0
        self.message_count: int = 0
    
    def record_latency(self, latency_ms: float):
        self.latencies.append(latency_ms)
    
    def start(self):
        self.start_time = time.time()
    
    def stop(self, count: int):
        self.end_time = time.time()
        self.message_count = count
    
    @property
    def elapsed_seconds(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def throughput(self) -> float:
        """Messages per second."""
        if self.elapsed_seconds > 0:
            return self.message_count / self.elapsed_seconds
        return 0
    
    @property
    def p50_latency(self) -> float:
        """50th percentile latency in ms."""
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.5)
            return sorted_latencies[idx]
        return 0
    
    @property
    def p99_latency(self) -> float:
        """99th percentile latency in ms."""
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.99)
            return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
        return 0
    
    @property
    def avg_latency(self) -> float:
        """Average latency in ms."""
        if self.latencies:
            return statistics.mean(self.latencies)
        return 0
    
    def report(self) -> str:
        return (
            f"\n{'='*60}\n"
            f"Benchmark: {self.name}\n"
            f"{'='*60}\n"
            f"  Messages:    {self.message_count:,}\n"
            f"  Duration:    {self.elapsed_seconds:.2f}s\n"
            f"  Throughput:  {self.throughput:,.0f} msg/s\n"
            f"  Latency p50: {self.p50_latency:.2f}ms\n"
            f"  Latency p99: {self.p99_latency:.2f}ms\n"
            f"  Latency avg: {self.avg_latency:.2f}ms\n"
            f"{'='*60}"
        )


class TestMemoryBackendBenchmark:
    """Benchmark tests for MemoryBackend."""
    
    @pytest.mark.asyncio
    async def test_produce_throughput_1kb(self):
        """Measure produce throughput with 1KB messages."""
        backend = MemoryBackend()
        await backend.start()
        
        topic = "bench-produce"
        await backend.create_topic(topic)
        
        num_messages = 10_000
        message_size = 1024  # 1KB
        
        # Create test message
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * message_size,
            metadata={},
        )
        msg_bytes = msg.to_bytes()
        
        metrics = BenchmarkMetrics("MemoryBackend Produce (1KB)")
        metrics.start()
        
        for i in range(num_messages):
            start = time.time()
            await backend.produce(topic, msg_bytes)
            latency_ms = (time.time() - start) * 1000
            metrics.record_latency(latency_ms)
        
        metrics.stop(num_messages)
        print(metrics.report())
        
        # Assertions
        assert metrics.throughput >= 5000, f"Throughput {metrics.throughput:.0f} < 5000 msg/s"
        assert metrics.p99_latency < 50, f"P99 latency {metrics.p99_latency:.2f}ms > 50ms"
        
        await backend.stop()
    
    @pytest.mark.asyncio
    async def test_produce_batch_throughput(self):
        """Measure batch produce throughput."""
        backend = MemoryBackend()
        await backend.start()
        
        topic = "bench-batch"
        await backend.create_topic(topic)
        
        num_batches = 100
        batch_size = 100
        total_messages = num_batches * batch_size
        
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * 256,
            metadata={},
        )
        msg_bytes = msg.to_bytes()
        batch = [msg_bytes] * batch_size
        
        metrics = BenchmarkMetrics("MemoryBackend Batch Produce")
        metrics.start()
        
        for i in range(num_batches):
            start = time.time()
            await backend.produce_batch(topic, batch)
            latency_ms = (time.time() - start) * 1000
            metrics.record_latency(latency_ms)
        
        metrics.stop(total_messages)
        print(metrics.report())
        
        assert metrics.throughput >= 10000, f"Throughput {metrics.throughput:.0f} < 10000 msg/s"
        
        await backend.stop()
    
    @pytest.mark.asyncio
    async def test_fetch_throughput(self):
        """Measure fetch throughput."""
        backend = MemoryBackend()
        await backend.start()
        
        topic = "bench-fetch"
        await backend.create_topic(topic)
        
        # Pre-populate
        num_messages = 10_000
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * 256,
            metadata={},
        )
        msg_bytes = msg.to_bytes()
        
        for i in range(num_messages):
            await backend.produce(topic, msg_bytes)
        
        # Benchmark fetch
        metrics = BenchmarkMetrics("MemoryBackend Fetch")
        metrics.start()
        
        offset = 0
        fetched = 0
        while fetched < num_messages:
            start = time.time()
            records = await backend.fetch(topic, offset=offset, max_records=100)
            latency_ms = (time.time() - start) * 1000
            metrics.record_latency(latency_ms)
            
            if not records:
                break
            
            fetched += len(records)
            offset = records[-1].offset + 1
        
        metrics.stop(fetched)
        print(metrics.report())
        
        assert metrics.throughput >= 10000, f"Throughput {metrics.throughput:.0f} < 10000 msg/s"
        
        await backend.stop()
    
    @pytest.mark.asyncio
    async def test_end_to_end_latency(self):
        """Measure end-to-end latency (produce + fetch)."""
        backend = MemoryBackend()
        await backend.start()
        
        topic = "bench-e2e"
        await backend.create_topic(topic)
        
        num_messages = 1000
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * 256,
            metadata={},
        )
        
        metrics = BenchmarkMetrics("MemoryBackend E2E Latency")
        metrics.start()
        
        for i in range(num_messages):
            start = time.time()
            
            # Produce
            msg.message_id = str(i)
            offset = await backend.produce(topic, msg.to_bytes())
            
            # Fetch
            records = await backend.fetch(topic, offset=offset, max_records=1)
            
            latency_ms = (time.time() - start) * 1000
            metrics.record_latency(latency_ms)
        
        metrics.stop(num_messages)
        print(metrics.report())
        
        assert metrics.p50_latency < 10, f"P50 latency {metrics.p50_latency:.2f}ms > 10ms"
        assert metrics.p99_latency < 50, f"P99 latency {metrics.p99_latency:.2f}ms > 50ms"
        
        await backend.stop()


class TestRayBackendBenchmark:
    """Benchmark tests for RayBackend."""
    
    @pytest.fixture(scope="class")
    def ray_cluster(self):
        if not ray.is_initialized():
            ray.init(num_cpus=4, ignore_reinit_error=True)
        yield
    
    @pytest.mark.asyncio
    async def test_produce_throughput(self, ray_cluster):
        """Measure RayBackend produce throughput."""
        backend = RayBackend()
        await backend.start()
        
        topic = "ray-bench-produce"
        await backend.create_topic(topic)
        
        num_messages = 5_000  # Fewer messages for Ray (remote calls)
        
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * 256,
            metadata={},
        )
        msg_bytes = msg.to_bytes()
        
        metrics = BenchmarkMetrics("RayBackend Produce")
        metrics.start()
        
        for i in range(num_messages):
            start = time.time()
            await backend.produce(topic, msg_bytes)
            latency_ms = (time.time() - start) * 1000
            metrics.record_latency(latency_ms)
        
        metrics.stop(num_messages)
        print(metrics.report())
        
        # Ray has higher overhead due to remote calls
        assert metrics.throughput >= 1000, f"Throughput {metrics.throughput:.0f} < 1000 msg/s"
        
        await backend.stop()
    
    @pytest.mark.asyncio
    async def test_fetch_throughput(self, ray_cluster):
        """Measure RayBackend fetch throughput."""
        backend = RayBackend()
        await backend.start()
        
        topic = "ray-bench-fetch"
        await backend.create_topic(topic)
        
        # Pre-populate
        num_messages = 5_000
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * 256,
            metadata={},
        )
        msg_bytes = msg.to_bytes()
        
        for i in range(num_messages):
            await backend.produce(topic, msg_bytes)
        
        # Benchmark fetch
        metrics = BenchmarkMetrics("RayBackend Fetch")
        metrics.start()
        
        offset = 0
        fetched = 0
        while fetched < num_messages:
            start = time.time()
            records = await backend.fetch(topic, offset=offset, max_records=100)
            latency_ms = (time.time() - start) * 1000
            metrics.record_latency(latency_ms)
            
            if not records:
                break
            
            fetched += len(records)
            offset = records[-1].offset + 1
        
        metrics.stop(fetched)
        print(metrics.report())
        
        assert metrics.throughput >= 5000, f"Throughput {metrics.throughput:.0f} < 5000 msg/s"
        
        await backend.stop()


class TestBackendComparison:
    """Compare performance across backends."""
    
    @pytest.fixture(scope="class")
    def ray_cluster(self):
        if not ray.is_initialized():
            ray.init(num_cpus=4, ignore_reinit_error=True)
        yield
    
    @pytest.mark.asyncio
    async def test_comparison_produce_1000(self, ray_cluster):
        """Compare produce performance across backends."""
        num_messages = 1000
        
        msg = QueueMessage(
            message_id="bench",
            split_id="split",
            data_ref="x" * 256,
            metadata={},
        )
        msg_bytes = msg.to_bytes()
        
        results = {}
        
        # MemoryBackend
        memory = MemoryBackend()
        await memory.start()
        await memory.create_topic("bench")
        
        start = time.time()
        for i in range(num_messages):
            await memory.produce("bench", msg_bytes)
        elapsed = time.time() - start
        results["MemoryBackend"] = num_messages / elapsed
        await memory.stop()
        
        # RayBackend
        ray_backend = RayBackend()
        await ray_backend.start()
        await ray_backend.create_topic("bench")
        
        start = time.time()
        for i in range(num_messages):
            await ray_backend.produce("bench", msg_bytes)
        elapsed = time.time() - start
        results["RayBackend"] = num_messages / elapsed
        await ray_backend.stop()
        
        print("\n" + "="*60)
        print("Backend Comparison: Produce 1000 messages")
        print("="*60)
        for name, throughput in results.items():
            print(f"  {name:20s}: {throughput:,.0f} msg/s")
        print("="*60)
        
        # Memory should be faster than Ray
        assert results["MemoryBackend"] > results["RayBackend"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

