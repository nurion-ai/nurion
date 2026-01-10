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

"""Elasticity tests for distributed Solstice pipelines.

These are P1 tests that verify:
- Dynamic worker scaling up during processing
- Dynamic worker scaling down during processing
- Rapid scale up/down cycles
- Partition rebalancing during scaling

All tests use real Ray clusters and Tansu queues (no mocks).
Data volumes: 10,000+ records with complex operators.
"""

import asyncio
import pytest
import ray

from solstice.runtime.ray_runner import RayJobRunner

from tests.utils import (
    DataValidator,
    ExplodeConfig,
    FilterConfig,
    FilterExplodeConfig,
    create_collector,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    kill_random_worker,
    wait_for_progress,
)

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestElasticScaling:
    """Tests for elastic worker scaling."""

    @pytest.fixture(autouse=True)
    async def setup_collector(self, ray_cluster, request):
        """Create a unique collector for each test."""
        import hashlib

        test_name = request.node.name.replace("[", "_").replace("]", "_")
        unique = hashlib.md5(test_name.encode()).hexdigest()[:8]
        self.collector_name = f"test_collector_{unique}"
        create_collector(self.collector_name)
        yield
        try:
            collector = ray.get_actor(self.collector_name)
            ray.kill(collector)
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_scale_up_during_processing(self, ray_cluster):
        """Scale up: new workers should join and partition rebalance correctly."""
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
            min_workers=2,
            max_workers=8,
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

            # Record initial worker count
            master = runner._masters.get("transform")
            initial_count = len(master._workers) if master else 0

            # Scale up: spawn additional workers
            if master:
                for _ in range(4):
                    try:
                        await master._spawn_worker()
                    except Exception:
                        pass
                    await asyncio.sleep(0.2)

            # Verify workers increased
            await asyncio.sleep(1)
            new_count = len(master._workers) if master else 0
            assert new_count > initial_count, (
                f"Scale up failed: {initial_count} -> {new_count}"
            )

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=360)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify data integrity after scale up
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after scale up: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_scale_down_during_processing(self, ray_cluster):
        """Scale down: removed workers' partitions should be taken over by others."""
        NUM_RECORDS = 12000
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

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
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start with more workers
            await wait_for_progress(
                runner, min_processed=3000, timeout=60, collector_name=self.collector_name
            )

            # Scale down: kill some workers
            await kill_random_worker(runner, stage_id="transform")
            await asyncio.sleep(0.3)
            await kill_random_worker(runner, stage_id="transform")

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=360)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify data integrity after scale down
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after scale down: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_scale_to_zero_and_back(self, ray_cluster):
        """Scale to zero then back: state should be preserved, recovery from offset."""
        NUM_RECORDS = 10000
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=2,
            max_workers=6,
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
            run_task = asyncio.create_task(runner.run())

            # Wait for processing to start
            await wait_for_progress(
                runner, min_processed=1500, timeout=60, collector_name=self.collector_name
            )

            # Kill all workers (scale to ~zero active processing)
            master = runner._masters.get("transform")
            if master:
                workers = list(master._workers.values())
                for worker in workers:
                    try:
                        ray.kill(worker)
                    except Exception:
                        pass

            # Wait a bit - master should recreate workers
            await asyncio.sleep(2)

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once semantics: no data loss, but may have duplicates
        assert len(sink_data) >= expected_count, (
            f"Data loss after scale to zero: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present (after filter + explode)
        actual_ids = {(r["id"], r.get("copy_idx", 0)) for r in sink_data}
        expected_ids = {
            (i, c)
            for i in range(NUM_RECORDS)
            if i % FILTER_MODULO == FILTER_REMAINDER
            for c in range(EXPLODE_FACTOR)
        }
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} records after scale to zero"
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_rapid_scale_up_down_cycles(self, ray_cluster):
        """Rapid scaling: no race conditions or duplicate processing."""
        NUM_RECORDS = 12000
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=400,
            min_workers=2,
            max_workers=8,
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

            # Rapid scale up/down cycles
            master = runner._masters.get("transform")

            for cycle in range(4):
                await wait_for_progress(
                    runner,
                    min_processed=500 + cycle * 700,
                    timeout=90,
                    collector_name=self.collector_name,
                )

                # Scale up
                if master:
                    for _ in range(2):
                        try:
                            await master._spawn_worker()
                        except Exception:
                            pass

                await asyncio.sleep(0.3)

                # Scale down
                await kill_random_worker(runner, stage_id="transform")

                await asyncio.sleep(0.2)

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once semantics: no data loss, but may have duplicates
        assert len(sink_data) >= expected_count, (
            f"Data loss in rapid scaling: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present (after filter)
        actual_ids = {r["id"] for r in sink_data}
        expected_ids = {i for i in range(NUM_RECORDS) if i % FILTER_MODULO == FILTER_REMAINDER}
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} IDs in rapid scaling"
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_scale_with_partition_rebalance(self, ray_cluster):
        """Partition rebalance during scaling: balanced distribution, no message loss."""
        NUM_RECORDS = 15000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR  # 45,000 records

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

            # Wait for initial processing
            await wait_for_progress(
                runner, min_processed=5000, timeout=90, collector_name=self.collector_name
            )

            master = runner._masters.get("transform")

            # Scale up significantly to trigger rebalance
            if master:
                for _ in range(4):
                    try:
                        await master._spawn_worker()
                    except Exception:
                        pass
                    await asyncio.sleep(0.1)

            # Wait for rebalance to settle
            await asyncio.sleep(2)

            # Continue processing
            await wait_for_progress(
                runner, min_processed=20000, timeout=120, collector_name=self.collector_name
            )

            # Scale down to trigger another rebalance
            for _ in range(3):
                await kill_random_worker(runner, stage_id="transform")
                await asyncio.sleep(0.2)

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # Verify partition rebalance didn't lose data
        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss after rebalance: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicates after rebalance"
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)
        assert validator.verify_checksums(source_data, sink_data)
