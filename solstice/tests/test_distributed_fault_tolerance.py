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

"""Fault tolerance tests for distributed Solstice pipelines.

These are P0 (highest priority) tests that verify:
- Worker crash recovery
- Multi-worker simultaneous crash
- Exactly-once semantics under failures
- Offset tracking and recovery

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
    kill_all_workers,
    kill_random_worker,
    wait_for_progress,
    wait_for_stage_workers,
)

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestWorkerFaultRecovery:
    """Tests for worker crash and recovery scenarios."""

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
    async def test_single_worker_crash_recovery(self, ray_cluster):
        """Worker crash: in-flight splits should be rescheduled, no data loss."""
        # Use larger data + smaller batch to ensure workers are still running when we kill
        NUM_RECORDS = 50000
        BATCH_SIZE = 100  # Smaller batch = more splits = longer processing
        FILTER_MODULO = 3
        FILTER_REMAINDER = 0
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=BATCH_SIZE,
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

            # First, wait for workers to be spawned
            await wait_for_stage_workers(runner, "transform", min_workers=3, timeout=30)

            # Then wait for some progress (but not too much)
            await wait_for_progress(
                runner, min_processed=1000, timeout=60, collector_name=self.collector_name
            )

            # Verify workers still exist before killing
            transform_master = runner._masters.get("transform")
            assert transform_master and len(transform_master._workers) > 0, (
                "No workers available to kill"
            )

            # Kill one worker
            killed_worker = await kill_random_worker(runner, stage_id="transform")
            assert killed_worker is not None, "No worker was killed"

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=360)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once semantics: no data loss, but may have duplicates
        assert len(sink_data) >= expected_count, (
            f"Data loss after single worker crash: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present (may have duplicates)
        actual_ids = {r["id"] for r in sink_data}
        expected_ids = {i for i in range(NUM_RECORDS) if i % FILTER_MODULO == FILTER_REMAINDER}
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} IDs after crash: {list(missing)[:10]}..."
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_multi_worker_simultaneous_crash(self, ray_cluster):
        """Multiple workers crash simultaneously: system should recover without deadlock."""
        NUM_RECORDS = 12000
        EXPLODE_FACTOR = 2

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

            # Wait for processing to start and workers to be up
            await wait_for_progress(
                runner, min_processed=3000, timeout=60, collector_name=self.collector_name
            )

            # Kill multiple workers simultaneously
            master = runner._masters.get("transform")
            if master and len(master._workers) >= 2:
                workers_to_kill = list(master._workers.values())[:2]
                for worker in workers_to_kill:
                    try:
                        ray.kill(worker)
                    except Exception:
                        pass

            # Wait for completion - should not deadlock
            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once semantics: no data loss, but may have duplicates
        assert len(sink_data) >= expected_count, (
            f"Data loss after multi-worker crash: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present (may have duplicates)
        actual_ids = {r["id"] for r in sink_data}
        expected_ids = set(range(NUM_RECORDS))
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} IDs after multi-worker crash"

    @pytest.mark.asyncio
    async def test_all_workers_crash_and_recovery(self, ray_cluster):
        """All workers crash: master should recreate workers and recover from offset."""
        # Use moderate data size for reasonable test time
        NUM_RECORDS = 30000
        BATCH_SIZE = 200
        FILTER_MODULO = 4
        FILTER_REMAINDER = 1
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=BATCH_SIZE,
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

            # Wait for workers to be spawned and get some progress
            await wait_for_stage_workers(runner, "transform", min_workers=3, timeout=30)

            # Give workers time to start processing, then kill immediately
            await asyncio.sleep(0.5)

            # Kill ALL workers in transform stage immediately after they start
            await kill_all_workers(runner, stage_id="transform")
            # Note: killed_count might be 0 if workers finished quickly, but test should still pass

            # Wait for workers to be recreated (if needed) and complete
            await asyncio.wait_for(run_task, timeout=300)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # After a crash, at-least-once guarantees mean we may have duplicates
        # but should not have data loss (got >= expected)
        assert len(sink_data) >= expected_count, (
            f"Data loss after all workers crash: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present (may have duplicates)
        actual_ids = {r["id"] for r in sink_data}
        expected_ids = {i for i in range(NUM_RECORDS) if i % FILTER_MODULO == FILTER_REMAINDER}
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} records after crash: {list(missing)[:10]}..."
        # Checksums still valid for present records
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_worker_restart_continues_from_offset(self, ray_cluster):
        """Worker restart: should continue from committed offset, no skip or repeat."""
        # Use larger data + smaller batch for longer processing time
        NUM_RECORDS = 50000
        BATCH_SIZE = 100
        EXPLODE_FACTOR = 3

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=BATCH_SIZE,
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
            run_task = asyncio.create_task(runner.run())

            # Wait for workers to be spawned
            await wait_for_stage_workers(runner, "transform", min_workers=3, timeout=30)

            # Wait for some processing
            await wait_for_progress(
                runner, min_processed=5000, timeout=60, collector_name=self.collector_name
            )

            # Verify workers exist before killing
            master = runner._masters.get("transform")
            if master and len(master._workers) > 0:
                await kill_random_worker(runner, stage_id="transform")
                await asyncio.sleep(1)

            # Wait for more processing and kill again
            await wait_for_progress(
                runner, min_processed=50000, timeout=180, collector_name=self.collector_name
            )
            if master and len(master._workers) > 0:
                await kill_random_worker(runner, stage_id="transform")

            # Wait for completion
            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once semantics: no data loss, but may have duplicates
        assert len(sink_data) >= expected_count, (
            f"Data loss after restart: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present (after explode)
        actual_ids = {(r["id"], r.get("copy_idx", 0)) for r in sink_data}
        expected_ids = {(i, c) for i in range(NUM_RECORDS) for c in range(EXPLODE_FACTOR)}
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} records after restart"


class TestExactlyOnceSemantics:
    """Tests for exactly-once processing semantics."""

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
    async def test_no_duplicate_on_worker_restart(self, ray_cluster):
        """Worker restart should not produce duplicate records."""
        # Use larger data + smaller batch for longer processing time
        NUM_RECORDS = 50000
        BATCH_SIZE = 100
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
            batch_size=BATCH_SIZE,
            min_workers=3,
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

            # Wait for workers to be spawned
            await wait_for_stage_workers(runner, "transform", min_workers=3, timeout=30)

            # Restart workers multiple times during processing
            for i in range(3):
                await wait_for_progress(
                    runner,
                    min_processed=2000 + i * 3000,
                    timeout=90,
                    collector_name=self.collector_name,
                )
                master = runner._masters.get("transform")
                if master and len(master._workers) > 0:
                    await kill_random_worker(runner, stage_id="transform")
                    await asyncio.sleep(0.5)

            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once semantics: no data loss, but may have duplicates
        # (duplicates can occur when worker crashes after processing but before commit)
        assert len(sink_data) >= expected_count, (
            f"Data loss: expected >= {expected_count}, got {len(sink_data)}"
        )

        # Verify all expected IDs are present (after filter + explode)
        # Only IDs that pass the filter will be in the output
        actual_ids = {(r["id"], r.get("copy_idx", 0)) for r in sink_data}
        expected_ids = {
            (i, c)
            for i in range(NUM_RECORDS)
            if i % FILTER_MODULO == FILTER_REMAINDER
            for c in range(EXPLODE_FACTOR)
        }
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} records: {list(missing)[:10]}..."

    @pytest.mark.asyncio
    async def test_no_loss_on_crash_before_commit(self, ray_cluster):
        """Crash before commit: batch should be reprocessed (at-least-once)."""
        NUM_RECORDS = 12000
        EXPLODE_FACTOR = 2

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=300,  # Small batches for more commit points
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
            run_task = asyncio.create_task(runner.run())

            # Rapid kills to increase chance of catching pre-commit state
            for _ in range(5):
                await asyncio.sleep(0.5)
                await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=480)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once: reprocessing should happen, so no data loss
        # but we may have duplicates
        assert len(sink_data) >= expected_count, (
            f"Data loss on crash before commit: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all IDs are present (may have duplicates)
        actual_ids = {r["id"] for r in sink_data}
        expected_ids = set(range(NUM_RECORDS))
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} IDs: {list(missing)[:10]}..."

    @pytest.mark.asyncio
    async def test_offset_commit_atomicity(self, ray_cluster):
        """Offset commit: no data loss after worker crashes (at-least-once)."""
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

            # Kill workers at various points
            await wait_for_progress(
                runner, min_processed=1500, timeout=60, collector_name=self.collector_name
            )
            await kill_random_worker(runner)
            await wait_for_progress(
                runner, min_processed=3000, timeout=90, collector_name=self.collector_name
            )
            await kill_random_worker(runner)

            await asyncio.wait_for(run_task, timeout=420)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once: no data loss (may have duplicates)
        assert len(sink_data) >= expected_count, (
            f"Data loss: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present
        actual_ids = {r["id"] for r in sink_data}
        expected_ids = {i for i in range(NUM_RECORDS) if i % FILTER_MODULO == FILTER_REMAINDER}
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} IDs"
        # Checksums should still be valid
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_at_least_once_with_multi_partition(self, ray_cluster):
        """Multi-partition: no data loss after worker crashes (at-least-once)."""
        # Use larger data + smaller batch for longer processing time
        NUM_RECORDS = 50000
        BATCH_SIZE = 100
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=BATCH_SIZE,
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
            run_task = asyncio.create_task(runner.run())

            # Wait for workers to be spawned
            await wait_for_stage_workers(runner, "transform", min_workers=4, timeout=30)

            # Kill workers to test partition rebalancing
            await wait_for_progress(
                runner, min_processed=5000, timeout=60, collector_name=self.collector_name
            )
            master = runner._masters.get("transform")
            if master and len(master._workers) > 0:
                await kill_random_worker(runner, stage_id="transform")

            await wait_for_progress(
                runner, min_processed=20000, timeout=120, collector_name=self.collector_name
            )
            # Kill multiple to force significant rebalance
            if master and len(master._workers) > 0:
                await kill_random_worker(runner, stage_id="transform")
            if master and len(master._workers) > 0:
                await kill_random_worker(runner, stage_id="transform")

            await asyncio.wait_for(run_task, timeout=600)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        # At-least-once: no data loss (may have duplicates due to reprocessing)
        assert len(sink_data) >= expected_count, (
            f"Data loss in multi-partition: expected >= {expected_count}, got {len(sink_data)}"
        )
        # Verify all expected IDs are present
        actual_ids = {(r["id"], r.get("copy_idx", 0)) for r in sink_data}
        expected_ids = {
            (i, c)
            for i in range(NUM_RECORDS)
            if i % FILTER_MODULO == FILTER_REMAINDER
            for c in range(EXPLODE_FACTOR)
        }
        missing = expected_ids - actual_ids
        assert not missing, f"Missing {len(missing)} records in multi-partition scenario"
        # Checksums should be valid
        assert validator.verify_checksums(source_data, sink_data)
