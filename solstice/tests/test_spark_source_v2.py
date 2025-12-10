"""Integration tests for SparkSource V2 - Direct Queue Integration.

V2 bypasses source_queue and operators by having JVM write directly
to output_queue with Arrow data embedded in payload_key.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

import pytest
import ray

from solstice.core.models import SplitPayload
from solstice.core.split_payload_store import RaySplitPayloadStore
from solstice.core.stage import Stage
from solstice.operators.sources.sparkv2 import (
    SparkSourceV2Config,
    SparkSourceV2Master,
)


# Test data path
TESTDATA_DIR = Path(__file__).parent / "testdata" / "resources" / "spark"
TEST_DATA_100 = TESTDATA_DIR / "test_data_100.parquet"
TEST_DATA_1000 = TESTDATA_DIR / "test_data_1000.parquet"


def _check_raydp_jars_available():
    """Check if raydp JAR files are available."""
    try:
        from raydp.utils import code_search_path

        paths = code_search_path()
        for path in paths:
            jars = glob.glob(os.path.join(path, "*.jar"))
            # Check for raydp-specific jars (not just pyspark jars)
            raydp_jars = [j for j in jars if "raydp" in os.path.basename(j).lower()]
            if raydp_jars:
                return True
        return False
    except Exception:
        return False


RAYDP_JARS_AVAILABLE = _check_raydp_jars_available()
SKIP_RAYDP_REASON = "raydp JAR files not available (need to build java components)"


def _wait_for_actor(store: RaySplitPayloadStore, timeout: float = 5.0):
    """Wait for the store actor to be ready."""
    import time
    start = time.time()
    while time.time() - start < timeout:
        try:
            # Try a simple operation to ensure actor is ready
            ray.get(store._actor.clear.remote(), timeout=1)
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("Actor not ready within timeout")


@pytest.mark.integration
@pytest.mark.skipif(not RAYDP_JARS_AVAILABLE, reason=SKIP_RAYDP_REASON)
class TestSparkSourceV2Integration:
    """Integration tests for SparkSourceV2Master.

    V2 writes directly to output_queue, bypassing source_queue and operators.
    """

    @pytest.mark.asyncio
    async def test_v2_writes_to_output_queue(self, ray_cluster):
        """Test that V2 writes directly to output_queue."""
        test_path = str(TEST_DATA_100)

        source_stage = Stage(
            stage_id="spark_v2_source",
            operator_config=SparkSourceV2Config(
                app_name="test-v2-output-queue",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        payload_store = RaySplitPayloadStore(name="test_v2_output_store")
        _wait_for_actor(payload_store)

        master = SparkSourceV2Master(
            job_id="test-v2-output",
            stage=source_stage,
            payload_store=payload_store,
        )

        try:
            # Start V2 master
            await master.start()

            # Verify output_queue was created and has messages
            output_queue = master.get_output_queue()
            assert output_queue is not None
            assert await output_queue.health_check()

            # Check that messages were written
            latest_offset = await output_queue.get_latest_offset(master._output_topic)
            assert latest_offset > 0
            print(f"V2 wrote {latest_offset} messages to output_queue")

            # Verify we can consume and get data via payload_store
            messages = await output_queue.fetch(master._output_topic, offset=0, max_records=10)
            assert len(messages) > 0

            # Check message format (messages are Record objects with .value attribute)
            from solstice.core.stage_master import QueueMessage
            msg = QueueMessage.from_bytes(messages[0].value)
            assert msg.payload_key.startswith("_jvm_arrow:")

            # Verify payload_store can fetch data
            payload = payload_store.get(msg.payload_key)
            assert payload is not None
            assert isinstance(payload, SplitPayload)
            assert len(payload) > 0
            print(f"Retrieved {len(payload)} records via _jvm_arrow lookup")

        finally:
            await master.stop()

    @pytest.mark.asyncio
    async def test_v2_with_parallelism(self, ray_cluster):
        """Test V2 with custom parallelism."""
        test_path = str(TEST_DATA_100)

        source_stage = Stage(
            stage_id="spark_v2_parallel",
            operator_config=SparkSourceV2Config(
                app_name="test-v2-parallel",
                num_executors=1,
                executor_cores=2,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
                parallelism=4,
            ),
        )

        payload_store = RaySplitPayloadStore(name="test_v2_parallel_store")
        _wait_for_actor(payload_store)

        master = SparkSourceV2Master(
            job_id="test-v2-parallel",
            stage=source_stage,
            payload_store=payload_store,
        )

        try:
            await master.start()

            # Should have 4 messages due to parallelism setting
            output_queue = master.get_output_queue()
            latest_offset = await output_queue.get_latest_offset(master._output_topic)
            assert latest_offset == 4
            print(f"V2 with parallelism=4 wrote {latest_offset} messages")

        finally:
            await master.stop()

    @pytest.mark.asyncio
    async def test_v2_large_dataset(self, ray_cluster):
        """Test V2 with larger dataset."""
        test_path = str(TEST_DATA_1000)

        source_stage = Stage(
            stage_id="spark_v2_large",
            operator_config=SparkSourceV2Config(
                app_name="test-v2-large",
                num_executors=1,
                executor_cores=2,
                executor_memory="1g",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        payload_store = RaySplitPayloadStore(name="test_v2_large_store")
        _wait_for_actor(payload_store)

        master = SparkSourceV2Master(
            job_id="test-v2-large",
            stage=source_stage,
            payload_store=payload_store,
        )

        try:
            await master.start()

            status = master.get_status()
            splits_produced = status.metrics.get("splits_produced", 0)
            assert splits_produced > 0
            print(f"V2 processed 1000 records in {splits_produced} splits")

        finally:
            await master.stop()
