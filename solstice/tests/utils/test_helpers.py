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

"""Test helper functions for distributed correctness tests."""

import asyncio
import logging
import random
import time
from typing import Optional

import ray

from solstice.runtime.ray_runner import RayJobRunner

logger = logging.getLogger(__name__)


async def wait_for_progress(
    runner: RayJobRunner,
    min_processed: int,
    timeout: float = 60.0,
    poll_interval: float = 0.5,
    collector_name: Optional[str] = None,
) -> None:
    """Wait until at least min_processed records have been processed.

    If collector_name is provided, uses the CollectingSink to track actual output.
    Otherwise, waits a fixed amount of time proportional to min_processed.

    Args:
        runner: The RayJobRunner instance
        min_processed: Minimum number of records to wait for
        timeout: Maximum time to wait in seconds
        poll_interval: Time between status checks
        collector_name: Optional name of CollectingSink actor to check progress

    Raises:
        TimeoutError: If progress is not reached within timeout
    """
    start = time.time()

    if collector_name:
        # Use CollectingSink for accurate progress tracking
        try:
            collector = ray.get_actor(collector_name)
        except ValueError:
            logger.warning(f"Collector {collector_name} not found, using time-based wait")
            collector = None

        if collector:
            last_count = 0
            while time.time() - start < timeout:
                try:
                    count = ray.get(collector.count.remote())
                    if count != last_count:
                        logger.debug(f"Sink progress: {count}/{min_processed} records")
                        last_count = count
                    if count >= min_processed:
                        logger.info(f"Progress reached: {count} records in sink")
                        return
                except Exception as e:
                    logger.debug(f"Collector check error: {e}")

                await asyncio.sleep(poll_interval)

            raise TimeoutError(
                f"Progress not reached within {timeout}s: expected {min_processed} records, got {last_count}"
            )

    # Fallback: simple time-based wait (give pipeline time to start)
    # Wait at least 5 seconds or until timeout
    wait_time = min(5.0, timeout * 0.3)
    logger.debug(f"Time-based wait: {wait_time}s for pipeline startup")
    await asyncio.sleep(wait_time)


async def wait_for_stage_workers(
    runner: RayJobRunner,
    stage_id: str,
    min_workers: int,
    timeout: float = 30.0,
) -> None:
    """Wait until a stage has at least min_workers active workers.

    Args:
        runner: The RayJobRunner instance
        stage_id: ID of the stage to check
        min_workers: Minimum number of workers to wait for
        timeout: Maximum time to wait in seconds

    Raises:
        TimeoutError: If workers are not available within timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            master = runner._masters.get(stage_id)
            if master and len(master._workers) >= min_workers:
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)

    raise TimeoutError(
        f"Stage {stage_id} did not reach {min_workers} workers within {timeout}s"
    )


async def kill_random_worker(
    runner: RayJobRunner,
    stage_id: Optional[str] = None,
    wait_for_death: bool = True,
    timeout: float = 5.0,
) -> Optional[str]:
    """Kill a random worker from a stage.

    Args:
        runner: The RayJobRunner instance
        stage_id: Optional stage ID to target (random if not specified)
        wait_for_death: If True, wait for the worker to actually die
        timeout: Maximum time to wait for worker death (seconds)

    Returns:
        Worker ID that was killed, or None if no workers available
    """
    import os
    import signal

    if stage_id:
        masters = [runner._masters.get(stage_id)]
        masters = [m for m in masters if m is not None]
    else:
        masters = list(runner._masters.values())

    # Shuffle to randomize which stage we target
    random.shuffle(masters)

    for master in masters:
        if master._workers:
            worker_id = random.choice(list(master._workers.keys()))
            worker = master._workers[worker_id]

            # Try to get the worker's pid before killing (for fallback)
            worker_pid = None
            try:
                status = ray.get(worker.get_status.remote(), timeout=1.0)
                worker_pid = status.get("pid")
            except Exception:
                pass

            try:
                # ray.kill is async - it sends SIGKILL but doesn't wait
                ray.kill(worker)

                if wait_for_death:
                    # Wait for the actor to actually die by trying to call a method
                    # This will raise RayActorError once the actor is dead
                    deadline = asyncio.get_event_loop().time() + timeout
                    actor_dead = False

                    while asyncio.get_event_loop().time() < deadline:
                        try:
                            # Try to ping the worker - if it's dead this will fail
                            ray.get(worker.get_status.remote(), timeout=0.5)
                            await asyncio.sleep(0.1)
                        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
                            # Actor is confirmed dead or unreachable
                            actor_dead = True
                            break
                        except Exception:
                            # Any other error also means actor is likely dead
                            actor_dead = True
                            break

                    # Fallback: if actor still alive after timeout, force kill by pid
                    if not actor_dead and worker_pid:
                        try:
                            os.kill(worker_pid, signal.SIGKILL)
                            logger.warning(
                                f"Force killed worker {worker_id} (pid={worker_pid}) via SIGKILL"
                            )
                            # Wait a bit for the process to actually die
                            await asyncio.sleep(0.2)
                        except ProcessLookupError:
                            # Process already dead
                            pass
                        except Exception as e:
                            logger.warning(f"Failed to force kill pid {worker_pid}: {e}")

                return worker_id
            except Exception:
                # Worker might already be dead
                pass

    return None


async def kill_all_workers(
    runner: RayJobRunner,
    stage_id: Optional[str] = None,
) -> int:
    """Kill all workers from a stage.

    Args:
        runner: The RayJobRunner instance
        stage_id: Optional stage ID to target (all stages if not specified)

    Returns:
        Number of workers killed
    """
    killed = 0

    if stage_id:
        masters = [runner._masters.get(stage_id)]
        masters = [m for m in masters if m is not None]
    else:
        masters = list(runner._masters.values())

    for master in masters:
        for worker_id, worker in list(master._workers.items()):
            try:
                ray.kill(worker)
                killed += 1
            except Exception:
                pass

    return killed


async def scale_stage_workers(
    runner: RayJobRunner,
    stage_id: str,
    target_count: int,
) -> int:
    """Scale a stage to target worker count.

    Args:
        runner: The RayJobRunner instance
        stage_id: Stage ID to scale
        target_count: Target number of workers

    Returns:
        Actual worker count after scaling
    """
    master = runner._masters.get(stage_id)
    if master is None:
        raise ValueError(f"Stage {stage_id} not found")

    current = len(master._workers)

    if target_count > current:
        # Scale up using worker manager
        partition_count = master._partition_count
        for _ in range(target_count - current):
            await master._worker_manager.spawn_worker(partition_count=partition_count)
    elif target_count < current:
        # Scale down
        workers_to_remove = current - target_count
        for worker_id in list(master._workers.keys())[:workers_to_remove]:
            try:
                ray.kill(master._workers[worker_id])
            except Exception:
                pass

    return len(master._workers)


def is_runner_finished(runner: RayJobRunner) -> bool:
    """Check if the runner has finished processing.

    Args:
        runner: The RayJobRunner instance

    Returns:
        True if finished, False otherwise
    """
    try:
        # Runner is finished if:
        # 1. It's not running (completed or stopped), or
        # 2. It's been initialized and all master tasks are done
        if not runner._running:
            return True
        if runner._initialized and len(runner._master_tasks) == 0:
            return True
        return False
    except Exception:
        return False
