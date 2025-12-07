"""Test TansuBackend with S3 storage configuration.

This test uses MinIO (Docker) for S3-compatible storage to test:
1. Starting Tansu with S3 storage backend
2. Producing and fetching messages
3. Offset tracking for exactly-once semantics

NOTE: S3 backend requires path-style access. Virtual-hosted style S3 services
(like Volcengine TOS) are NOT supported by Tansu's object_store.
Use MinIO, Ceph, or AWS S3 with path-style enabled.
"""

import asyncio
import os
import subprocess
import pytest

# Skip if tansu not available
import shutil
if not shutil.which("tansu"):
    pytest.skip("tansu binary not found", allow_module_level=True)

from solstice.queue import TansuBackend


def check_minio_available() -> bool:
    """Check if MinIO is running on localhost:9000."""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(("localhost", 9000))
        sock.close()
        return result == 0
    except Exception:
        return False


def start_minio_docker() -> bool:
    """Start MinIO using Docker if available."""
    try:
        # Check if docker is available
        if not shutil.which("docker"):
            return False
        
        # Remove existing container
        subprocess.run(["docker", "rm", "-f", "minio-test"], 
                      capture_output=True, timeout=10)
        
        # Start MinIO
        result = subprocess.run([
            "docker", "run", "-d", "--name", "minio-test",
            "-p", "9000:9000", "-p", "9001:9001",
            "-e", "MINIO_ROOT_USER=minioadmin",
            "-e", "MINIO_ROOT_PASSWORD=minioadmin",
            "minio/minio", "server", "/data", "--console-address", ":9001"
        ], capture_output=True, timeout=60)
        
        if result.returncode != 0:
            return False
        
        # Wait for MinIO to be ready
        import time
        for _ in range(30):
            if check_minio_available():
                # Create test bucket
                subprocess.run([
                    "docker", "exec", "minio-test",
                    "mc", "alias", "set", "local", "http://localhost:9000", 
                    "minioadmin", "minioadmin"
                ], capture_output=True, timeout=10)
                subprocess.run([
                    "docker", "exec", "minio-test",
                    "mc", "mb", "local/tansu-test"
                ], capture_output=True, timeout=10)
                return True
            time.sleep(1)
        
        return False
    except Exception:
        return False


def stop_minio_docker():
    """Stop MinIO Docker container."""
    try:
        subprocess.run(["docker", "rm", "-f", "minio-test"], 
                      capture_output=True, timeout=10)
    except Exception:
        pass


def get_minio_config() -> dict:
    """Get MinIO S3 configuration for Tansu."""
    return {
        "storage_url": "s3://tansu-test/",
        "s3_endpoint": "http://localhost:9000",
        "s3_region": "us-east-1",
        "s3_access_key": "minioadmin",
        "s3_secret_key": "minioadmin",
    }


pytestmark = pytest.mark.asyncio(loop_scope="function")


@pytest.mark.skipif(
    not check_minio_available() and not shutil.which("docker"),
    reason="MinIO not available and Docker not found"
)
class TestTansuMinioS3:
    """Tests for TansuBackend with MinIO S3 storage.
    
    These tests require MinIO running on localhost:9000.
    If MinIO is not available, it will try to start it via Docker.
    """
    
    @pytest.fixture(scope="class", autouse=True)
    def setup_minio(self):
        """Ensure MinIO is running."""
        if not check_minio_available():
            if not start_minio_docker():
                pytest.skip("Could not start MinIO")
        yield
        # Note: Don't stop MinIO here to allow reuse across test runs
    
    @pytest.fixture
    def minio_config(self):
        """Get MinIO S3 configuration."""
        return get_minio_config()
    
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
            print(f"Tansu started with MinIO S3")
        finally:
            await backend.stop()
    
    @pytest.mark.asyncio
    async def test_produce_fetch_with_minio(self, minio_config):
        """Test produce and fetch operations with MinIO S3 backend."""
        backend = TansuBackend(
            port=19093,
            startup_timeout=60.0,
            **minio_config,
        )
        
        try:
            await backend.start()
            
            topic = "test-minio-topic"
            await backend.create_topic(topic)
            
            # Wait for topic to be ready
            await asyncio.sleep(2)
            
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
        backend = TansuBackend(
            port=19094,
            startup_timeout=60.0,
            **minio_config,
        )
        
        try:
            await backend.start()
            
            topic = "test-minio-offset-topic"
            group = "test-consumer-group"
            
            await backend.create_topic(topic)
            await asyncio.sleep(2)
            
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


if __name__ == "__main__":
    # Run a quick manual test
    async def main():
        print("Testing TansuBackend with MinIO S3...")
        
        if check_minio_available():
            config = get_minio_config()
            print(f"MinIO S3 config: {config}")
        else:
            print("MinIO not available, using memory backend...")
            config = {"storage_url": "memory://"}
        
        backend = TansuBackend(
            port=19096,
            startup_timeout=60.0,
            **config,
        )
        
        try:
            print("Starting Tansu...")
            await backend.start()
            print("Tansu started!")
            
            topic = "test-topic"
            await backend.create_topic(topic)
            await asyncio.sleep(2)
            
            print("Producing messages...")
            for i in range(3):
                offset = await backend.produce(topic, f"test-{i}".encode())
                print(f"  Produced at offset {offset}")
            
            print("Fetching messages...")
            records = await backend.fetch(topic, offset=0)
            for r in records:
                print(f"  offset={r.offset}, value={r.value}")
            
            print("Test completed successfully!")
            
        except Exception as e:
            print(f"Test failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await backend.stop()
            print("Tansu stopped.")
    
    asyncio.run(main())

