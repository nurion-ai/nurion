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

"""Chaos engineering stress tests.

These are P2 tests that:
- Stress test the system under load
- Test long-running stability
- Test resource pressure scenarios
- Are NOT expected to be 100% stable
- Run separately from integration tests

Use `pytest -m chaos` to run these tests.
Data volumes: 30,000+ records with complex operators.
"""

import asyncio
import gc
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
    create_multi_stage_pipeline,
    create_test_pipeline,
    generate_test_data_with_checksum,
    get_sink_records,
    is_runner_finished,
    kill_random_worker,
)

# Mark all tests in this module as chaos tests (NOT integration)
pytestmark = [pytest.mark.chaos, pytest.mark.slow]


class TestStressScenarios:
    """Stress tests for system limits."""

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
    async def test_high_throughput_stress(self, ray_cluster):
        """High throughput stress test with many records and Explode.

        Tests system behavior under high data volume: 30K input -> 90K output.
        """
        NUM_RECORDS = 30000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR  # 90,000 records

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=4,
            max_workers=10,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=600)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in high throughput: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)

    @pytest.mark.asyncio
    async def test_many_small_batches_stress(self, ray_cluster):
        """Stress test with many small batches.

        Tests overhead of batch management with high batch count.
        Uses Filter to reduce output while maintaining batch count.
        """
        NUM_RECORDS = 30000
        BATCH_SIZE = 50  # Many small batches: 600 batches
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
            await asyncio.wait_for(runner.run(), timeout=600)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss with small batches: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER
        )

    @pytest.mark.asyncio
    async def test_deep_pipeline_stress(self, ray_cluster):
        """Stress test with deep pipeline (many stages).

        Tests system behavior with many sequential stages.
        Uses multi-stage pipeline with passthrough operators.
        """
        NUM_RECORDS = 20000
        NUM_STAGES = 5
        validator = DataValidator()

        job = create_multi_stage_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            num_transform_stages=NUM_STAGES,
            min_workers=2,
            max_workers=4,
            collector_name=self.collector_name,
            with_checksum=True,
        )

        runner = RayJobRunner(job)
        try:
            await runner.initialize()
            await asyncio.wait_for(runner.run(), timeout=600)
        finally:
            await runner.stop()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, NUM_RECORDS), (
            f"Data loss in deep pipeline: expected {NUM_RECORDS}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates(sink_data)


class TestLongRunningStability:
    """Tests for long-running stability."""

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
    async def test_long_running_stability(self, ray_cluster):
        """Long-running stability test.

        Tests for memory leaks and stability over extended processing.
        Uses Filter+Explode for complex row count tracking.
        """
        NUM_RECORDS = 25000
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
            batch_size=400,
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

        # Track memory before
        gc.collect()

        try:
            await runner.initialize()

            # Inject periodic chaos during long run
            chaos_running = True

            async def periodic_chaos():
                while chaos_running and not is_runner_finished(runner):
                    await asyncio.sleep(random.uniform(5.0, 15.0))
                    if is_runner_finished(runner):
                        break
                    try:
                        await kill_random_worker(runner)
                    except Exception:
                        pass

            chaos_task = asyncio.create_task(periodic_chaos())

            try:
                await asyncio.wait_for(runner.run(), timeout=600)
            finally:
                chaos_running = False
                chaos_task.cancel()
                try:
                    await chaos_task
                except asyncio.CancelledError:
                    pass

        finally:
            await runner.stop()

        # Force garbage collection
        gc.collect()

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in long run: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_filter_explode_result(
            sink_data, NUM_RECORDS, FILTER_MODULO, FILTER_REMAINDER, EXPLODE_FACTOR
        )
        assert validator.verify_checksums(source_data, sink_data)

    @pytest.mark.asyncio
    async def test_sustained_chaos(self, ray_cluster):
        """Sustained chaos over extended period.

        Continuous failure injection over a longer processing window.
        Uses Explode operator for high output volume.
        """
        NUM_RECORDS = 20000
        EXPLODE_FACTOR = 3
        validator = DataValidator()

        source_data = generate_test_data_with_checksum(NUM_RECORDS)
        expected_count = NUM_RECORDS * EXPLODE_FACTOR  # 60,000 records

        job = create_test_pipeline(
            num_records=NUM_RECORDS,
            batch_size=500,
            min_workers=3,
            max_workers=10,
            collector_name=self.collector_name,
            with_checksum=True,
            source_data=source_data,
            transform_config=ExplodeConfig(factor=EXPLODE_FACTOR),
        )

        runner = RayJobRunner(job)
        total_kills = 0
        chaos_running = True

        async def sustained_chaos():
            nonlocal total_kills
            while chaos_running and not is_runner_finished(runner):
                await asyncio.sleep(random.uniform(0.5, 3.0))
                if is_runner_finished(runner):
                    break

                # Random chaos action
                action = random.choice(["kill", "scale_up", "nothing", "nothing"])
                try:
                    if action == "kill":
                        killed = await kill_random_worker(runner)
                        if killed:
                            total_kills += 1
                    elif action == "scale_up":
                        master = runner._masters.get("transform")
                        if master and len(master._workers) < 10:
                            await master._spawn_worker()
                except Exception:
                    pass

        try:
            await runner.initialize()

            chaos_task = asyncio.create_task(sustained_chaos())

            try:
                await asyncio.wait_for(runner.run(), timeout=600)
            finally:
                chaos_running = False
                chaos_task.cancel()
                try:
                    await chaos_task
                except asyncio.CancelledError:
                    pass

        finally:
            await runner.stop()

        print(f"Total kills during sustained chaos: {total_kills}")

        sink_data = get_sink_records(self.collector_name)

        assert validator.verify_count(sink_data, expected_count), (
            f"Data loss in sustained chaos: expected {expected_count}, got {len(sink_data)}"
        )
        assert validator.verify_no_duplicates_composite(
            sink_data, ["id", "copy_idx"]
        )
        assert validator.verify_explode_result(sink_data, NUM_RECORDS, EXPLODE_FACTOR)
        assert validator.verify_checksums(source_data, sink_data)
