"""Test TansuBackend with S3 storage configuration.

This test uses MinIO (via testcontainers) for S3-compatible storage to test:
1. Starting Tansu with S3 storage backend
2. Producing and fetching messages
3. Offset tracking for exactly-once semantics

NOTE: S3 backend requires path-style access. Virtual-hosted style S3 services
(like Volcengine TOS) are NOT supported by Tansu's object_store.
Use MinIO, Ceph, or AWS S3 with path-style enabled.
"""

import asyncio
import pytest

# Skip if tansu not available
import shutil

if not shutil.which("tansu"):
    pytest.skip("tansu binary not found", allow_module_level=True)

from solstice.queue import TansuBackend


pytestmark = pytest.mark.asyncio(loop_scope="function")


async def wait_for_topic_ready(backend, topic: str, timeout: float = 30.0) -> bool:
    """Wait for a topic to be ready for produce/fetch operations.

    S3 storage backends have slower metadata propagation, so we need to
    poll until the topic is actually available for BOTH consumer and producer.
    """
    import time

    start = time.time()
    while time.time() - start < timeout:
        consumer_ready = False
        producer_ready = False

        # Check consumer can see the topic
        try:
            await backend.fetch(topic, offset=0, max_records=1)
            consumer_ready = True
        except Exception as e:
            if "UnknownTopicOrPartitionError" not in str(e):
                # Other errors might indicate the topic is ready but empty
                consumer_ready = True

        # Check producer can see the topic
        try:
            if backend._producer:
                await backend._producer.client._wait_on_metadata(topic)
            producer_ready = True
        except Exception as e:
            if "UnknownTopicOrPartitionError" not in str(e):
                producer_ready = True

        if consumer_ready and producer_ready:
            return True

        await asyncio.sleep(1)

    return False


class TestTansuMinioS3:
    """Tests for TansuBackend with MinIO S3 storage via testcontainers."""

    @pytest.fixture(scope="class")
    def minio_for_tansu(self):
        """Start MinIO container and create tansu-test bucket."""
        from testcontainers.minio import MinioContainer
        from minio import Minio

        with MinioContainer() as minio:
            host_ip = minio.get_container_host_ip()
            exposed_port = minio.get_exposed_port(9000)
            minio_client = Minio(
                endpoint=f"{host_ip}:{exposed_port}",
                access_key=minio.access_key,
                secret_key=minio.secret_key,
                secure=False,
            )
            bucket_name = "tansu-test"
            if not minio_client.bucket_exists(bucket_name=bucket_name):
                minio_client.make_bucket(bucket_name=bucket_name)
            yield minio

    @pytest.fixture
    def minio_config(self, minio_for_tansu):
        """Get MinIO S3 configuration for Tansu."""
        host = minio_for_tansu.get_container_host_ip()
        port = minio_for_tansu.get_exposed_port(9000)
        return {
            "storage_url": "s3://tansu-test/",
            "s3_endpoint": f"http://{host}:{port}",
            "s3_region": "us-east-1",
            "s3_access_key": minio_for_tansu.access_key,
            "s3_secret_key": minio_for_tansu.secret_key,
        }

    @pytest.mark.asyncio
    async def test_tansu_start_with_minio(self, minio_config):
        """Test starting Tansu with MinIO S3 storage backend."""
        backend = TansuBackend(
            port=19092,
            startup_timeout=60.0,
            **minio_config,
        )

        try:
            await backend.start()
            assert await backend.health_check()
            print("Tansu started with MinIO S3")
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_produce_fetch_with_minio(self, minio_config):
        """Test produce and fetch operations with MinIO S3 backend."""
        import uuid

        backend = TansuBackend(
            port=19093,
            startup_timeout=60.0,
            **minio_config,
        )

        try:
            await backend.start()

            # Use unique topic name to avoid conflicts from previous runs
            topic = f"test-minio-topic-{uuid.uuid4().hex[:8]}"
            await backend.create_topic(topic)

            # Wait for topic to be ready (S3 backend has slower metadata propagation)
            assert await wait_for_topic_ready(backend, topic, timeout=30.0), (
                f"Topic {topic} not ready after 30s"
            )

            # Produce messages
            offsets = []
            for i in range(5):
                offset = await backend.produce(topic, f"message-{i}".encode())
                offsets.append(offset)
                print(f"Produced message {i} at offset {offset}")

            # Fetch messages
            records = await backend.fetch(topic, offset=0, max_records=10)
            print(f"Fetched {len(records)} records")

            assert len(records) == 5
            for i, record in enumerate(records):
                assert record.value == f"message-{i}".encode()

        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_offset_commit_with_minio(self, minio_config):
        """Test offset commit and recovery with MinIO S3 backend."""
        import uuid

        backend = TansuBackend(
            port=19094,
            startup_timeout=60.0,
            **minio_config,
        )

        try:
            await backend.start()

            # Use unique topic name to avoid conflicts from previous runs
            topic = f"test-minio-offset-topic-{uuid.uuid4().hex[:8]}"
            group = "test-consumer-group"

            await backend.create_topic(topic)

            # Wait for topic to be ready (S3 backend has slower metadata propagation)
            assert await wait_for_topic_ready(backend, topic, timeout=30.0), (
                f"Topic {topic} not ready after 30s"
            )

            # Produce messages
            for i in range(10):
                await backend.produce(topic, f"msg-{i}".encode())

            # Consume first half and commit
            records = await backend.fetch(topic, offset=0, max_records=5)
            assert len(records) == 5

            await backend.commit_offset(group, topic, 5)

            # Verify committed offset
            committed = await backend.get_committed_offset(group, topic)
            assert committed == 5

            # Resume from committed offset
            remaining = await backend.fetch(topic, offset=committed)
            assert len(remaining) == 5
            assert remaining[0].value == b"msg-5"

            print("Offset commit and recovery works correctly with MinIO S3!")

        finally:
            await backend.stop()


class TestTansuMemory:
    """Tests for TansuBackend with memory storage."""

    @pytest.mark.asyncio
    async def test_tansu_memory_backend(self):
        """Test Tansu with in-memory storage."""
        backend = TansuBackend(
            storage_url="memory://",
            port=19095,
            startup_timeout=30.0,
        )

        try:
            await backend.start()
            assert await backend.health_check()

            topic = "test-memory-topic"
            await backend.create_topic(topic)
            await asyncio.sleep(1)

            # Quick produce/fetch test
            await backend.produce(topic, b"hello")
            records = await backend.fetch(topic, offset=0)

            assert len(records) == 1
            assert records[0].value == b"hello"

            print("Tansu memory backend works!")

        finally:
            await backend.stop()
