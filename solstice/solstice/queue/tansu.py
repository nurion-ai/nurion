"""Tansu-based queue backend for persistent message queuing.

This backend uses Tansu (a Kafka-compatible broker) as a subprocess
to provide durable message queuing with S3/SQLite/PostgreSQL storage.

Features:
- Persistent storage (survives process restarts)
- Kafka-compatible protocol (uses aiokafka client)
- Multiple storage backends (memory, S3, SQLite, PostgreSQL)
- Offset tracking for exactly-once semantics
- Production-ready and actively maintained

Architecture:
    ┌─────────────────────────────────────────────┐
    │              Master Actor                   │
    │  ┌───────────────────────────────────────┐  │
    │  │  TansuBackend                          │  │
    │  │  - Manages Tansu subprocess            │  │
    │  │  - Provides produce/fetch APIs         │  │
    │  │                                        │  │
    │  │  ┌─────────────────────────────────┐  │  │
    │  │  │  Tansu Broker (subprocess)       │  │  │
    │  │  │  - Kafka protocol on port 9092   │  │  │
    │  │  │  - S3/SQLite storage backend     │  │  │
    │  │  └─────────────────────────────────┘  │  │
    │  └───────────────────────────────────────┘  │
    │                                             │
    │       ▲ produce              fetch ▼        │
    │       │                      │              │
    │  ┌────┴────┐            ┌────┴────┐        │
    │  │ Workers │            │ Workers │        │
    │  └─────────┘            └─────────┘        │
    └─────────────────────────────────────────────┘
"""

import asyncio
import atexit
import signal
import socket
import subprocess
import os
import time
import weakref
from pathlib import Path
from typing import Dict, List, Optional, Set

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from solstice.queue.backend import QueueBackend, Record
from solstice.utils.logging import create_ray_logger


# Global registry of used ports (to avoid conflicts)
_used_ports: Set[int] = set()

# Global registry of TansuBackend instances for cleanup
_instances: weakref.WeakSet = weakref.WeakSet()


def _cleanup_all_tansu():
    """Cleanup all Tansu processes on exit."""
    for instance in list(_instances):
        try:
            if instance._process and instance._process.poll() is None:
                os.killpg(os.getpgid(instance._process.pid), signal.SIGKILL)
                instance._process.wait(timeout=1)
        except Exception:
            pass


# Register cleanup on interpreter exit
atexit.register(_cleanup_all_tansu)


def _find_free_port(start: int = 10000, end: int = 60000) -> int:
    """Find a free port that is not in use."""
    import random

    # Try random ports first
    for _ in range(100):
        port = random.randint(start, end)
        if port in _used_ports:
            continue

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", port))
            sock.close()
            return port
        except OSError:
            continue

    raise RuntimeError(f"Could not find a free port in range {start}-{end}")


class TansuBackend(QueueBackend):
    """Tansu subprocess-based queue backend.

    This backend starts a Tansu broker as a subprocess and communicates
    with it using the Kafka protocol via aiokafka.

    The backend supports multiple storage backends:
    - memory:// - In-memory storage (for testing)
    - s3://bucket?endpoint=...&region=... - S3 storage (durable)

    S3 Configuration:
        S3 backends require path-style access. Use MinIO, Ceph, or AWS S3
        with path-style enabled. Virtual-hosted style S3 services (like
        Volcengine TOS) are NOT supported.

        Required environment variables:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_ALLOW_HTTP=true (for http endpoints)

        S3 URL format: s3://bucket?endpoint=http://host:port&region=us-east-1&allow_http=true

    Example:
        ```python
        # For testing (in-memory, auto-select port)
        backend = TansuBackend(storage_url="memory://")

        # For production (MinIO S3)
        backend = TansuBackend(
            storage_url="s3://tansu-data?endpoint=http://minio:9000&region=us-east-1&allow_http=true"
        )

        await backend.start()

        await backend.create_topic("my-topic")
        offset = await backend.produce("my-topic", b"data")
        records = await backend.fetch("my-topic", offset=0)

        await backend.stop()
        ```

    Prerequisites:
        - `tansu` binary must be in PATH
        - aiokafka must be installed: pip install aiokafka
    """

    def __init__(
        self,
        storage_url: str = "memory://",
        port: Optional[int] = None,
        data_dir: Optional[Path] = None,
        tansu_binary: str = "tansu",
        startup_timeout: float = 30.0,
        s3_endpoint: Optional[str] = None,
        s3_region: str = "us-east-1",
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        client_only: bool = False,
    ):
        """Initialize Tansu backend.

        Args:
            storage_url: Storage backend URL (memory://, s3://bucket/)
            port: Port for Kafka protocol. If None, auto-selects a free port.
            data_dir: Directory for Tansu data (optional)
            tansu_binary: Path to tansu binary (default: "tansu")
            startup_timeout: Timeout for Tansu startup in seconds
            s3_endpoint: S3 endpoint URL (e.g., http://localhost:9000 for MinIO)
            s3_region: S3 region (default: us-east-1)
            s3_access_key: S3 access key (can also use AWS_ACCESS_KEY_ID env var)
            s3_secret_key: S3 secret key (can also use AWS_SECRET_ACCESS_KEY env var)
            client_only: If True, only connect to existing Tansu server, don't start one
        """
        self.storage_url = storage_url
        self.data_dir = data_dir
        self.tansu_binary = tansu_binary
        self.startup_timeout = startup_timeout
        self.s3_endpoint = s3_endpoint
        self.s3_region = s3_region
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.client_only = client_only

        # Auto-select port if not specified
        if port is None and not client_only:
            self.port = _find_free_port()
        else:
            self.port = port or 9092

        # Get node IP for distributed access
        self.host = self._get_node_ip()

        # Mark port as used
        _used_ports.add(self.port)

        self._process: Optional[subprocess.Popen] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._admin_client: Optional[AIOKafkaAdminClient] = None
        self._consumers: Dict[tuple, AIOKafkaConsumer] = {}  # Consumer cache:
        # - For consumer groups: (topic, group_id) -> consumer (handles all assigned partitions)
        # - For manual assignment: (topic, None, partition) -> consumer (one per partition)
        self._committed_offsets: Dict[tuple, int] = {}  # (group, topic, partition) -> offset
        self._running = False

        self.logger = create_ray_logger(f"TansuBackend:{self.port}")

        # Register for cleanup
        _instances.add(self)

    def _get_node_ip(self) -> str:
        """Get the IP address of the current Ray node.

        Returns the node IP for distributed access. Falls back to localhost
        if Ray is not initialized or node info is unavailable.
        """
        try:
            import ray

            if ray.is_initialized():
                # Get current node's IP from Ray runtime context
                node_id = ray.get_runtime_context().get_node_id()
                nodes = ray.nodes()
                for node in nodes:
                    if node.get("NodeID") == node_id:
                        return node.get("NodeManagerAddress", "localhost")
        except Exception:
            pass
        return "localhost"

    def __del__(self):
        """Cleanup on garbage collection."""
        self._force_cleanup()

    def _force_cleanup(self):
        """Force cleanup of Tansu process."""
        # Release port
        _used_ports.discard(self.port)

        # Kill process if still running
        if self._process and self._process.poll() is None:
            try:
                os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                self._process.wait(timeout=1)
            except Exception:
                pass
            self._process = None

    async def start(self) -> None:
        """Start the Tansu broker subprocess and connect."""
        if self._running:
            return

        if not self.client_only:
            # Start Tansu subprocess
            await self._start_tansu_process()

            # Wait for broker to be ready
            await self._wait_for_ready()

        # Initialize Kafka clients
        await self._init_kafka_clients()

        self._running = True
        mode = "client-only" if self.client_only else "server"
        self.logger.info(f"TansuBackend started on port {self.port} ({mode})")

    async def _start_tansu_process(self) -> None:
        """Start the Tansu broker subprocess."""
        cmd = [
            self.tansu_binary,
            "broker",
            "--storage-engine",
            self.storage_url,
            "--listener-url",
            f"tcp://0.0.0.0:{self.port}",
            "--advertised-listener-url",
            f"tcp://localhost:{self.port}",
        ]

        if self.data_dir:
            cmd.extend(["--data-dir", str(self.data_dir)])

        # Build environment with S3 configuration
        env = os.environ.copy()

        if self.storage_url.startswith("s3://"):
            # S3 configuration via environment variables
            if self.s3_endpoint:
                env["AWS_ENDPOINT"] = self.s3_endpoint
                env["AWS_ENDPOINT_URL"] = self.s3_endpoint
                # Allow HTTP endpoints (like MinIO)
                if self.s3_endpoint.startswith("http://"):
                    env["AWS_ALLOW_HTTP"] = "true"
            if self.s3_region:
                env["AWS_REGION"] = self.s3_region
                env["AWS_DEFAULT_REGION"] = self.s3_region
            if self.s3_access_key:
                env["AWS_ACCESS_KEY_ID"] = self.s3_access_key
            if self.s3_secret_key:
                env["AWS_SECRET_ACCESS_KEY"] = self.s3_secret_key

        self.logger.info(f"Starting Tansu: {' '.join(cmd)}")

        # Start process
        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                preexec_fn=os.setsid,  # Create new process group for clean shutdown
            )
        except FileNotFoundError:
            _used_ports.discard(self.port)
            raise RuntimeError(
                f"Tansu binary not found: {self.tansu_binary}. "
                "Please install Tansu or provide the correct path."
            )

        # Check if process started successfully
        await asyncio.sleep(0.1)
        if self._process.poll() is not None:
            stderr = self._process.stderr.read().decode() if self._process.stderr else ""
            _used_ports.discard(self.port)
            raise RuntimeError(f"Tansu failed to start: {stderr}")

    async def _wait_for_ready(self) -> None:
        """Wait for Tansu broker to be ready."""
        start_time = time.time()
        connected = False

        while time.time() - start_time < self.startup_timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(("localhost", self.port))
                sock.close()
                if result == 0:
                    if not connected:
                        self.logger.info("Tansu broker port is open, waiting for initialization...")
                        connected = True
                        # Give Tansu a moment to fully initialize after port opens
                        await asyncio.sleep(2.0)
                        continue
                    self.logger.info("Tansu broker is ready")
                    return
            except Exception:
                pass

            # Check if process died
            if self._process and self._process.poll() is not None:
                stderr = self._process.stderr.read().decode() if self._process.stderr else ""
                _used_ports.discard(self.port)
                raise RuntimeError(f"Tansu process died: {stderr}")

            await asyncio.sleep(0.5)

        self._force_cleanup()
        raise RuntimeError(f"Tansu failed to start within {self.startup_timeout}s")

    async def _init_kafka_clients(self) -> None:
        """Initialize Kafka producer and admin client."""
        bootstrap_servers = f"localhost:{self.port}"

        # Initialize producer
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",  # Wait for all replicas
        )
        await self._producer.start()

        # Initialize admin client
        self._admin_client = AIOKafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
        )
        await self._admin_client.start()

    async def stop(self) -> None:
        """Stop the Tansu broker and cleanup."""
        self._running = False

        # Stop Kafka clients
        if self._producer:
            try:
                await self._producer.stop()
            except Exception:
                pass
            self._producer = None

        if self._admin_client:
            try:
                await self._admin_client.close()
            except Exception:
                pass
            self._admin_client = None

        for consumer in self._consumers.values():
            try:
                await consumer.stop()
            except Exception:
                pass
        self._consumers.clear()

        # Stop Tansu process
        if self._process:
            try:
                # Send SIGTERM to process group
                os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)

                # Wait for graceful shutdown
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill
                    os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                    self._process.wait()
            except ProcessLookupError:
                pass  # Process already dead
            except Exception as e:
                self.logger.warning(f"Error stopping Tansu process: {e}")

            self._process = None

        # Release port
        _used_ports.discard(self.port)

        self.logger.info("TansuBackend stopped")

    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic."""
        try:
            new_topic = NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=1,
            )
            await self._admin_client.create_topics([new_topic])
            self.logger.info(f"Created topic: {topic}")
        except Exception as e:
            # Topic may already exist
            if "TopicExistsError" not in str(e) and "TOPIC_ALREADY_EXISTS" not in str(e):
                raise RuntimeError(f"Failed to create topic {topic}: {e}")

    async def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        try:
            await self._admin_client.delete_topics([topic])
            self.logger.info(f"Deleted topic: {topic}")
        except Exception as e:
            # Topic may not exist
            if "UnknownTopicOrPartitionError" not in str(e):
                raise RuntimeError(f"Failed to delete topic {topic}: {e}")

    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message to the topic."""
        result = await self._producer.send_and_wait(topic, value, key=key)
        return result.offset

    async def produce_batch(
        self,
        topic: str,
        values: List[bytes],
        keys: Optional[List[Optional[bytes]]] = None,
    ) -> List[int]:
        """Produce multiple messages to the topic."""
        if keys is not None and len(keys) != len(values):
            raise ValueError(f"keys length ({len(keys)}) must match values length ({len(values)})")

        if not values:
            return []

        offsets = []
        # Send all messages
        futures = []
        for i, value in enumerate(values):
            key = keys[i] if keys else None
            future = await self._producer.send(topic, value, key=key)
            futures.append(future)

        # Wait for all to complete
        for future in futures:
            result = await future
            offsets.append(result.offset)

        return offsets

    async def _get_consumer(
        self, topic: str, group_id: Optional[str] = None, partition: Optional[int] = None
    ) -> AIOKafkaConsumer:
        """Get or create a consumer for the topic.

        This method implements proper consumer lifecycle management:
        - For consumer groups: One consumer per (topic, group_id) pair
        - For manual assignment: One consumer per (topic, partition) pair
        - Consumers are reused across multiple calls and live for the lifetime of TansuBackend

        Args:
            topic: Topic name
            group_id: Consumer group ID. If provided, uses consumer group protocol
                for automatic partition assignment. If None, uses manual assignment.
            partition: Specific partition to assign (only used if group_id is None).
                If None and group_id is None, defaults to partition 0.

        Returns:
            AIOKafkaConsumer instance (reused if already exists)
        """
        # Consumer key design:
        # - For consumer groups: (topic, group_id) - one consumer handles all assigned partitions
        # - For manual assignment: (topic, None, partition) - one consumer per partition
        if group_id:
            # Consumer group mode: one consumer per (topic, group_id)
            consumer_key = (topic, group_id)
        else:
            # Manual assignment mode: one consumer per (topic, partition)
            partition_id = partition if partition is not None else 0
            consumer_key = (topic, None, partition_id)

        if consumer_key not in self._consumers:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=f"localhost:{self.port}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=30000,
                group_id=group_id,  # Use consumer group for automatic partition assignment
            )
            await consumer.start()

            # Wait a bit for metadata to be available
            await asyncio.sleep(0.2)

            if group_id:
                # Use consumer group - partitions will be automatically assigned
                # Subscribe to topic and let Kafka handle partition assignment
                consumer.subscribe([topic])
                self.logger.debug(
                    f"Created consumer for topic {topic} with group {group_id} "
                    f"(automatic partition assignment, will be reused)"
                )
            else:
                # Manual partition assignment (for backward compatibility)
                partition_id = partition if partition is not None else 0
                tp = TopicPartition(topic, partition_id)
                consumer.assign([tp])
                self.logger.debug(
                    f"Created consumer for topic {topic} with manual assignment to partition {partition_id} "
                    f"(will be reused)"
                )

            # Wait for partition assignment to take effect
            await asyncio.sleep(0.1)

            self._consumers[consumer_key] = consumer

        return self._consumers[consumer_key]

    async def fetch(
        self,
        topic: str,
        offset: int = 0,
        max_records: int = 100,
        timeout_ms: int = 1000,
        group_id: Optional[str] = None,
        partition: Optional[int] = None,
    ) -> List[Record]:
        """Fetch records from the topic starting at the given offset.

        Args:
            topic: Topic name
            offset: Starting offset (only used for manual partition assignment)
            max_records: Maximum number of records to fetch
            timeout_ms: Timeout in milliseconds
            group_id: Consumer group ID for automatic partition assignment
            partition: Specific partition to fetch from (only used if group_id is None)

        Returns:
            List of records
        """
        consumer = await self._get_consumer(topic, group_id=group_id, partition=partition)

        # For consumer group, we don't seek - we rely on committed offsets
        # For manual assignment, seek to the desired offset
        if not group_id:
            partition_id = partition if partition is not None else 0
            tp = TopicPartition(topic, partition_id)
            consumer.seek(tp, offset)

        # Fetch records using getmany with proper timeout
        records = []
        try:
            # getmany returns {TopicPartition: [ConsumerRecord]}
            batch = await consumer.getmany(
                timeout_ms=timeout_ms,
                max_records=max_records,
            )

            for tp_key, tp_records in batch.items():
                for record in tp_records:
                    records.append(
                        Record(
                            offset=record.offset,
                            value=record.value,
                            key=record.key,
                            timestamp=record.timestamp or int(time.time() * 1000),
                        )
                    )
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            self.logger.warning(f"Fetch error: {e}")

        return records

    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
        partition: Optional[int] = None,
    ) -> None:
        """Commit the consumer offset for a consumer group.

        This method follows Kafka best practices:
        - Commits offsets for all partitions assigned to this consumer (group mode)
        - Or commits a specific partition when requested (manual path)
        - Uses Kafka's native offset commit mechanism (not local cache)
        - In consumer group mode, Kafka automatically manages partition assignment

        Args:
            group: Consumer group ID
            topic: Topic name
            offset: Offset to commit (next offset to consume)
            partition: Specific partition to commit. If None, commits all assigned partitions.

        Note:
            In Kafka consumer group mode, each consumer is assigned specific partitions.
            If you need per-partition offsets, pass partition explicitly or track them in
            the caller and commit with separate calls.
        """
        if partition is not None:
            # Commit a specific partition using a dedicated, manually assigned consumer
            tp = TopicPartition(topic, partition)
            commit_consumer = await self._get_consumer(topic, group_id=None, partition=partition)
            await commit_consumer.commit({tp: offset})
            self._committed_offsets[(group, topic, partition)] = offset
            return

        # Commit all assigned partitions for the consumer group
        consumer = await self._get_consumer(topic, group_id=group, partition=None)
        assigned = consumer.assignment()

        if not assigned:
            self.logger.warning(
                f"No partitions assigned for group {group}, topic {topic}. "
                "Cannot commit offsets. This may happen if the consumer hasn't "
                "joined the group yet or if there are no partitions in the topic."
            )
            return

        offsets_to_commit = {tp: offset for tp in assigned}
        await consumer.commit(offsets_to_commit)

        for tp in assigned:
            self._committed_offsets[(group, topic, tp.partition)] = offset

    async def get_committed_offset(
        self,
        group: str,
        topic: str,
        partition: Optional[int] = None,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group.

        Args:
            group: Consumer group ID
            topic: Topic name
            partition: Specific partition (if None, returns offset for partition 0 for backward compatibility)

        Returns:
            Committed offset, or None if not found
        """
        # First check memory cache
        if partition is not None:
            cached = self._committed_offsets.get((group, topic, partition))
            if cached is not None:
                return cached
        else:
            cached = self._committed_offsets.get((group, topic, 0)) or self._committed_offsets.get(
                (group, topic)
            )
            if cached is not None:
                return cached

        # If not in cache, read from Kafka/Tansu
        try:
            partition_id = partition if partition is not None else 0
            tp = TopicPartition(topic, partition_id)

            # Reuse the consumer group consumer if it exists
            # For consumer groups, we use the group consumer (one per topic+group)
            # committed() can read offsets for any partition in the group, even if not assigned
            consumer = await self._get_consumer(topic, group_id=group, partition=None)

            # Get committed offset - this works even if partition is not assigned to this consumer
            # Kafka stores committed offsets per group, not per consumer instance
            offset = await consumer.committed(tp)

            # committed() returns the offset directly (or None)
            if offset is not None:
                # Cache it
                self._committed_offsets[(group, topic, partition_id)] = offset
                self.logger.debug(
                    f"Read committed offset {offset} for group={group}, topic={topic}, partition={partition_id}"
                )
                return offset
            else:
                # Offset is None means no commit yet
                self.logger.debug(
                    f"No committed offset found (None) for group={group}, topic={topic}, partition={partition_id}"
                )
                return None
        except Exception as e:
            self.logger.warning(
                f"Error reading committed offset from Kafka/Tansu for group={group}, topic={topic}, partition={partition_id}: {e}"
            )
            import traceback

            self.logger.debug(f"Traceback: {traceback.format_exc()}")

        return None

    async def get_latest_offset(self, topic: str, partition: Optional[int] = None) -> int:
        """Get the latest offset in the topic.

        Args:
            topic: Topic name
            partition: Specific partition (if None, returns offset for partition 0 for backward compatibility)

        Returns:
            Latest offset (next offset that will be assigned)
        """
        partition_id = partition if partition is not None else 0
        tp = TopicPartition(topic, partition_id)

        # Reuse existing consumer or create one for this partition
        # For read-only operations like getting latest offset, we can use a consumer
        # without group_id (manual assignment)
        consumer = await self._get_consumer(topic, group_id=None, partition=partition_id)

        # Get end offset
        end_offsets = await consumer.end_offsets([tp])
        return end_offsets.get(tp, 0)

    async def get_all_partition_offsets(self, topic: str) -> Dict[int, int]:
        """Get the latest offset for all partitions in the topic.

        Args:
            topic: Topic name

        Returns:
            Dictionary mapping partition ID to latest offset
        """
        # First, we need to get the number of partitions
        # We'll try to get metadata from the admin client; let errors surface upstream
        metadata = await self._admin_client.describe_topics([topic])
        topic_metadata = metadata[0] if metadata else None

        partitions = None
        if isinstance(topic_metadata, dict):
            partitions = topic_metadata.get("partitions")
        elif topic_metadata is not None and hasattr(topic_metadata, "partitions"):
            partitions = topic_metadata.partitions

        num_partitions = len(partitions) if partitions else 1

        # Get offsets for all partitions
        partition_offsets = {}
        for p in range(num_partitions):
            offset = await self.get_latest_offset(topic, partition=p)
            partition_offsets[p] = offset

        return partition_offsets

    @property
    def is_persistent(self) -> bool:
        """Tansu backend persists data (depends on storage URL)."""
        # memory:// is not persistent, but s3:// is persistent
        return not self.storage_url.startswith("memory://")

    async def health_check(self) -> bool:
        """Check if the backend is healthy."""
        if not self._running:
            return False

        if self._process and self._process.poll() is not None:
            return False

        try:
            # Try to list topics as a health check
            await self._admin_client.list_topics()
            return True
        except Exception:
            return False

    def get_stats(self) -> Dict:
        """Get statistics about the backend."""
        return {
            "storage_url": self.storage_url,
            "port": self.port,
            "running": self._running,
            "process_alive": self._process.poll() is None if self._process else False,
            "topics": list(self._consumers.keys()),
            "committed_offsets": dict(self._committed_offsets),
        }
