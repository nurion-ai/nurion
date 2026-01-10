# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end data consistency tests for distributed Solstice pipelines.

These are P0 (highest priority) tests that verify:
- No data loss in simple and complex pipelines
- No duplicate records
- Checksum/content integrity
- Data consistency under fault conditions
- Correctness with row-count-changing operators (filter, explode)

All tests use real Ray clusters and Tansu queues (no mocks).
Data volumes: 10,000+ records for realistic testing.
"""

import asyncio
import uuid

import pytest
import ray

from solstice.runtime.ray_runner import RayJobRunner

from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    FilterExplodeConfig,
    create_collector,
    create_multi_stage_pipeline,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    kill_random_worker,
    wait_for_progress,
)

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestBasicDataConsistency:
    """Basic data consistency tests without fault injection."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        # Use full UUID to ensure uniqueness across all runs
        self.collector_name = f"test_collector_{uuid.uuid4().hex}"

        create_collector(self.collector_name)
        yield
        # Cleanup
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_e2e_no_data_loss_simple(self, ray_cluster):
        """Basic scenario: verify simple pipeline has no data loss."""
        NUM_RECORDS = 10000
        validator = DataValidator()

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=2,
            max_workers=6,
            collector_name=self.collector_name,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=480)
        finally:
            await runner.stop()

        records = get_sink_records(self.collector_name)

        # Verify no data loss
        assert validator.verify_count(records, NUM_RECORDS), (
            f"Data loss detected: expected {NUM_RECORDS}, got {len(records)}"
        )
        assert validator.verify_no_duplicates(records), (
            f"Duplicates detected: {validator.get_duplicate_ids(records)}"
        )

    @pytest.mark.asyncio
    async def test_e2e_no_data_loss_multi_stage(self, ray_cluster):
        """Multi-stage pipeline: verify no data loss through multiple stages."""
        NUM_RECORDS = 10000
        validator = DataValidator()

        job = create_multi_stage_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            num_transform_stages=3,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=540)
        finally:
            await runner.stop()

        records = get_sink_records(self.collector_name)

        assert validator.verify_count(records, NUM_RECORDS), (
            f"Data loss in multi-stage pipeline: expected {NUM_RECORDS}, got {len(records)}"
        )
        assert validator.verify_no_duplicates(records)

    @pytest.mark.asyncio
    async def test_e2e_no_duplicates_simple(self, ray_cluster):
        """Verify no duplicate records in output."""
        NUM_RECORDS = 15000
        validator = DataValidator()

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=480)
        finally:
            await runner.stop()

        records = get_sink_records(self.collector_name)

        # Primary verification: no duplicates
        assert validator.verify_no_duplicates(records), (
            f"Duplicates found: {validator.get_duplicate_ids(records)}"
        )

        # Secondary verification: count is correct
        assert validator.verify_count(records, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_e2e_checksum_integrity(self, ray_cluster):
        """Verify data checksum integrity through pipeline."""
        NUM_RECORDS = 10000
        validator = DataValidator()

        # Generate source data with checksums
        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify checksums match
        assert validator.verify_checksums(source_data, sink_data), (
            "Checksum mismatch detected - data may be corrupted"
        )
        assert validator.verify_count(sink_data, NUM_RECORDS)

    @pytest.mark.asyncio
    async def test_e2e_content_correctness(self, ray_cluster):
        """Verify content is correctly transformed and preserved."""
        NUM_RECORDS = 10000
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_ids = {r["id"] for r in source_data}

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            collector_name=self.collector_name,
            source_data=source_data,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify all IDs are present
        assert validator.verify_all_ids_present(sink_data, expected_ids), (
            f"Missing IDs: {validator.get_missing_ids(sink_data, expected_ids)}"
        )


class TestFilterOperatorConsistency:
    """Data consistency tests with Filter operator (row count reduction)."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        self.collector_name = f"test_collector_{uuid.uuid4().hex}"

        create_collector(self.collector_name)
        yield
        # Cleanup
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_filter_50_percent(self, ray_cluster):
        """Filter 50% of data: verify correct row count and no data corruption."""
        NUM_RECORDS = 20000
        FILTER_MODULO = 2
        FILTER_REMAINDER = 0  # Keep even IDs
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=540)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify filter result
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        ), f"Filter result incorrect: got {len(sink_data)} records"

        # Verify checksums for filtered records
        assert validator.verify_checksums(source_data, sink_data), (
            "Checksum mismatch after filtering"
        )

    @pytest.mark.asyncio
    async def test_filter_20_percent(self, ray_cluster):
        """Filter to 20% of data: verify correct row count."""
        NUM_RECORDS = 25000
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0  # Keep ids divisible by 5
        validator = DataValidator()

        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=540)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Filter to 20%: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )


class TestExplodeOperatorConsistency:
    """Data consistency tests with Explode operator (row count increase)."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        self.collector_name = f"test_collector_{uuid.uuid4().hex}"

        create_collector(self.collector_name)
        yield
        # Cleanup
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_explode_3x(self, ray_cluster):
        """Explode 3x: verify correct row count and no data corruption."""
        NUM_RECORDS = 10000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=540)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify explode result
        assert validator.verify_count(sink_data, expected_count), (
            f"Explode 3x: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(
            sink_data, NUM_RECORDS, EXPLODE_FACTOR
        ), "Explode result incorrect"

        # Verify no duplicates with composite key (id, copy_idx)
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicate (id, copy_idx) found"

        # Verify checksums (each copy should have same checksum as source)
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_explode_5x(self, ray_cluster):
        """Explode 5x: verify large row count increase."""
        NUM_RECORDS = 8000
        EXPLODE_FACTOR = 5
        validator = DataValidator()

        expected_count = NUM_RECORDS * EXPLODE_FACTOR  # 40,000 records

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Explode 5x: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(
            sink_data, NUM_RECORDS, EXPLODE_FACTOR
        )


class TestFilterExplodeConsistency:
    """Data consistency tests with combined Filter+Explode (complex row changes)."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        self.collector_name = f"test_collector_{uuid.uuid4().hex}"

        create_collector(self.collector_name)
        yield
        # Cleanup
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_filter_then_explode(self, ray_cluster):
        """Filter 20% then explode 4x: verify complex row count changes."""
        NUM_RECORDS = 25000
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 4
        validator = DataValidator()

        # Expected: 25000 * 20% * 4 = 20000
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        source_data = generate_test_data_with_checksum(NUM_RECORDS)

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterExplodeConfig(
                filter_modulo=FILTER_MODULO,
                filter_remainder=FILTER_REMAINDER,
                explode_factor=EXPLODE_FACTOR,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify filter+explode result
        assert validator.verify_count(sink_data, expected_count), (
            f"Filter+Explode: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_explode_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        ), "Filter+Explode result incorrect"

        # Verify checksums
        assert validator.verify_checksums(source_data, sink_data)


class TestFaultScenarioConsistency:
    """Data consistency tests under fault conditions with complex operators."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        self.collector_name = f"test_collector_{uuid.uuid4().hex}"

        create_collector(self.collector_name)
        yield
        # Cleanup
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_e2e_consistency_with_worker_crash(self, ray_cluster):
        """Verify data consistency when a worker crashes mid-processing."""
        NUM_RECORDS = 15000
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=6,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(
                runner, min_processed=2000, timeout=60, collector_name=self.collector_name
            )

            # Kill a random worker
            killed = await kill_random_worker(runner, stage_id="transform")
            if killed:
                await asyncio.sleep(1)

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=360)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Core verifications
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after worker crash: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_e2e_consistency_with_scale_events(self, ray_cluster):
        """Verify data consistency during worker scaling events with explode."""
        NUM_RECORDS = 10000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=2,
            max_workers=8,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(
                runner, min_processed=2000, timeout=60, collector_name=self.collector_name
            )

            # Scale up: spawn additional workers
            master = runner._masters.get("transform")
            if master:
                for _ in range(3):
                    try:
                        await master._spawn_worker()
                    except Exception:
                        pass

            # Wait then scale down
            await asyncio.sleep(1)
            await wait_for_progress(
                runner, min_processed=10000, timeout=120, collector_name=self.collector_name
            )
            await kill_random_worker(runner, stage_id="transform")

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss during scaling: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_e2e_consistency_with_slow_worker(self, ray_cluster):
        """Verify data consistency with slow workers (filter + slow processing)."""
        NUM_RECORDS = 10000
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        # Combine filter with slow processing
        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=540)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss with slow worker: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_e2e_consistency_with_backpressure(self, ray_cluster):
        """Verify data consistency when backpressure is activated (filter+explode)."""
        NUM_RECORDS = 15000
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        # Small batch size + explode = more likely to trigger backpressure
        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=200,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterExplodeConfig(
                filter_modulo=FILTER_MODULO,
                filter_remainder=FILTER_REMAINDER,
                explode_factor=EXPLODE_FACTOR,
            ),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss under backpressure: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_explode_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_e2e_consistency_large_dataset(self, ray_cluster):
        """Verify data consistency with large dataset (50K+ output records)."""
        NUM_RECORDS = 20000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR  # 60,000 records

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=4,
            max_workers=8,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=540)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Full verification suite for large dataset
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in large dataset: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicates in large dataset"
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)
        assert validator.verify_checksums(source_data, sink_data)
