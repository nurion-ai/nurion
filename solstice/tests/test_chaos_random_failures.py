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

"""Chaos engineering tests with random failure injection.

These are P2 tests that:
- Inject random failures during processing
- Test combined failure scenarios
- Are NOT expected to be 100% stable
- Run separately from integration tests

Use `pytest -m chaos` to run these tests.
Data volumes: 20,000+ records with complex operators.
"""

import asyncio
import logging
import random
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
    is_runner_finished,
    kill_random_worker,
    wait_for_progress,
)

logger = logging.getLogger(__name__)

# Mark all tests in this module as chaos tests (NOT integration)
pytestmark = [pytest.mark.chaos, pytest.mark.slow]


class TestRandomFailureInjection:
    """Chaos tests with random failure injection."""

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
    async def test_random_worker_kills_continuous(self, ray_cluster):
        """Continuous random worker kills during processing.

        Note: This test may occasionally fail due to timing issues.
        It's designed to stress-test the system, not guarantee 100% pass rate.
        Uses Filter+Explode for complex row count changes.
        """
        NUM_RECORDS = 5000  # Reduced for faster test with per-message commits
        FILTER_MODULO = 5
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 2
        KILL_INTERVAL = (5.0, 10.0)  # Less aggressive killing for stability
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
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
        kills = 0
        killer_running = True

        async def chaos_killer():
            """Background task that randomly kills workers."""
            nonlocal kills
            while killer_running and not is_runner_finished(runner):
                await asyncio.sleep(random.uniform(*KILL_INTERVAL))
                if is_runner_finished(runner):
                    break
                try:
                    killed = await kill_random_worker(runner)
                    if killed:
                        kills += 1
                except Exception:
                    pass  # Ignore errors during chaos

        try:
            await runner.initialize()

            # Start chaos killer in background
            killer_task = asyncio.create_task(chaos_killer())

            try:
                # Run with generous timeout
                await asyncio.wait_for(runner.run(), timeout=240)
            finally:
                killer_running = False
                killer_task.cancel()
                try:
                    await killer_task
                except asyncio.CancelledError:
                    pass

        finally:
            await runner.stop()

        print(f"Total workers killed: {kills}")

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once semantics: no data loss AND no duplicates
        assert validator.verify_count(sink_data, expected_count), (
            f"Data count mismatch in chaos test: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        ), "Duplicates found in chaos test - exactly-once semantics violated"

    @pytest.mark.asyncio
    async def test_burst_kills(self, ray_cluster):
        """Burst of worker kills at random intervals.

        Simulates scenarios where multiple failures occur in quick succession.
        Uses Explode operator to increase output volume.
        """
        NUM_RECORDS = 3000  # Small for faster test with frequent commits
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=150,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        burst_count = 0

        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Perform burst kills at random intervals (reduced for stability)
            for _ in range(3):
                # Wait random interval
                await asyncio.sleep(random.uniform(3.0, 8.0))

                if run_task.done():
                    break

                # Burst: kill 1-2 workers quickly
                burst_size = random.randint(1, 2)
                for _ in range(burst_size):
                    try:
                        await kill_random_worker(runner)
                        burst_count += 1
                    except Exception:
                        pass
                    await asyncio.sleep(0.2)

            await asyncio.wait_for(run_task, timeout=180)
        finally:
            await runner.stop()

        print(f"Total burst kills: {burst_count}")

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in burst kill test: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)


class TestCombinedFailures:
    """Tests combining multiple failure types."""

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
    async def test_combined_failures(self, ray_cluster):
        """Combined failure scenario: kills + scaling + delays.

        Tests system stability under multiple concurrent failure modes.
        Uses Filter operator for deterministic row count verification.
        """
        NUM_RECORDS = 8000  # Reduced for faster test with per-message commits
        FILTER_MODULO = 4
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
            max_workers=10,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=FilterConfig(
                modulo=FILTER_MODULO,
                remainder=FILTER_REMAINDER,
            ),
        )

        runner = RayJobRunner(job)
        chaos_running = True

        async def combined_chaos():
            """Apply various chaos actions randomly."""
            actions_taken = 0
            MAX_ACTIONS = 5  # Limit total chaos actions
            while chaos_running and not is_runner_finished(runner) and actions_taken < MAX_ACTIONS:
                await asyncio.sleep(random.uniform(2.0, 5.0))  # Slower chaos

                if is_runner_finished(runner):
                    break

                # Random action (bias towards "nothing" for stability)
                action = random.choice(["kill", "scale_up", "nothing", "nothing"])

                try:
                    if action == "kill":
                        await kill_random_worker(runner, stage_id="transform")
                        actions_taken += 1
                    elif action == "scale_up":
                        master = runner._masters.get("transform")
                        if master and master._worker_manager and len(master._workers) < 6:
                            partition_count = master._partition_count
                            await master._worker_manager.spawn_worker(partition_count=partition_count)
                            actions_taken += 1
                    # "nothing" - just wait
                except Exception:
                    pass

        try:
            await runner.initialize()

            chaos_task = asyncio.create_task(combined_chaos())

            try:
                await asyncio.wait_for(runner.run(), timeout=420)
            finally:
                chaos_running = False
                chaos_task.cancel()
                try:
                    await chaos_task
                except asyncio.CancelledError:
                    pass

        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in combined chaos: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_cascading_failures(self, ray_cluster):
        """Cascading failures: kill workers in multiple stages.

        Tests that failures in one stage don't cascade to corrupt data
        in other stages. Uses Filter+Explode for complex verification.
        """
        NUM_RECORDS = 50000  # Large data to ensure workers are alive during kills
        FILTER_MODULO = 4
        FILTER_REMAINDER = 0
        EXPLODE_FACTOR = 2
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = validator.calculate_filter_explode_expected_count(
            NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=100,  # Small batches = more splits = longer processing
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
        kills_performed = 0

        try:
            await runner.initialize()
            run_task = asyncio.create_task(runner.run())

            # Wait for some progress before killing (but not too much)
            await wait_for_progress(
                runner, min_processed=500, timeout=60, collector_name=self.collector_name
            )

            # Kill workers in different stages
            for _ in range(3):
                if run_task.done():
                    break

                # Randomly pick a stage to kill from (including source)
                stage = random.choice(["source", "transform", "sink"])
                try:
                    killed = await kill_random_worker(runner, stage_id=stage)
                    if killed:
                        kills_performed += 1
                        logger.info(f"Killed worker in stage {stage}")
                except Exception as e:
                    logger.debug(f"Failed to kill worker in {stage}: {e}")

                await asyncio.sleep(random.uniform(0.5, 2.0))

            await asyncio.wait_for(run_task, timeout=180)
        finally:
            await runner.stop()

        # Verify failures were actually injected
        assert kills_performed > 0, "No workers were killed - test is not valid"
        logger.info(f"Test completed with {kills_performed} worker kills")

        sink_data = get_sink_records(self.collector_name)

        # Exactly-once semantics: no data loss AND no duplicates
        assert validator.verify_count(sink_data, expected_count), (
            f"Data count mismatch in cascading failures: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_explode_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )
        assert validator.verify_checksums(source_data, sink_data)
