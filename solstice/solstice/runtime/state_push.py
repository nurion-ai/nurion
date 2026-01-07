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

"""State push manager for WebUI metrics.

Manages the push-based state infrastructure:
- Tansu broker and queue for state messages
- StateProducer for emitting job-level events
- JobStateManager for consuming and aggregating state

This is extracted from RayJobRunner to keep it focused on job execution.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from solstice.core.stage_master import QueueEndpoint

from solstice.utils.logging import create_ray_logger


@dataclass
class StatePushConfig:
    """Configuration for state push infrastructure."""

    enabled: bool = False
    storage_url: str = "memory://state/"
    webui_storage_path: Optional[str] = None


class StatePushManager:
    """Manages push-based state/metrics infrastructure.

    Encapsulates:
    - Tansu broker lifecycle
    - State topic creation
    - StateProducer for job events
    - JobStateManager for state aggregation
    - Registration with WebUI

    Usage:
        manager = StatePushManager(job_id, config)
        await manager.start()  # Sets up broker, producer, consumer

        # Get endpoint for stage configs
        endpoint = manager.endpoint

        # Emit events
        await manager.emit_job_started(dag_edges, stages)
        await manager.emit_job_completed("COMPLETED", duration_ms)

        # Cleanup
        await manager.stop()
    """

    def __init__(self, job_id: str, config: StatePushConfig):
        self.job_id = job_id
        self.config = config
        self.logger = create_ray_logger(f"StatePush-{job_id}")

        # Infrastructure (created in start())
        self._broker = None
        self._queue = None
        self._producer = None
        self._state_manager = None
        self._endpoint: Optional["QueueEndpoint"] = None

        self._started = False

    @property
    def topic(self) -> str:
        """State topic name."""
        return f"{self.job_id}_state"

    @property
    def endpoint(self) -> Optional["QueueEndpoint"]:
        """Queue endpoint for stage configs."""
        return self._endpoint

    @property
    def state_manager(self):
        """JobStateManager instance (for WebUI queries)."""
        return self._state_manager

    async def start(self) -> None:
        """Start state push infrastructure."""
        if not self.config.enabled:
            self.logger.debug("State push disabled")
            return

        if self._started:
            return

        try:
            from solstice.queue import TansuBrokerManager, TansuQueueClient, QueueType
            from solstice.core.stage_master import QueueEndpoint
            from solstice.webui.state.producer import StateProducer
            from solstice.webui.state.manager import JobStateManager

            # Create and start broker
            self._broker = TansuBrokerManager(storage_url=self.config.storage_url)
            await self._broker.start()

            broker_url = self._broker.get_broker_url()
            host, port_str = broker_url.split(":")

            self._endpoint = QueueEndpoint(
                queue_type=QueueType.TANSU,
                host=host,
                port=int(port_str),
                storage_url=self.config.storage_url,
            )

            # Create queue client
            self._queue = TansuQueueClient(broker_url)
            await self._queue.start()

            # Create state topic
            await self._queue.create_topic(self.topic, partitions=1)
            self.logger.info(f"Created state topic {self.topic}")

            # Create state producer
            self._producer = StateProducer(
                job_id=self.job_id,
                queue_client=self._queue,
                state_topic=self.topic,
            )
            await self._producer.start()

            # Create state manager (consumer)
            storage = None
            if self.config.webui_storage_path:
                from solstice.webui.storage import JobStorage

                storage = JobStorage(
                    base_path=self.config.webui_storage_path,
                    job_id=self.job_id,
                )

            self._state_manager = JobStateManager(
                job_id=self.job_id,
                queue_client=self._queue,
                state_topic=self.topic,
                storage=storage,
            )
            await self._state_manager.start()

            self._started = True
            self.logger.info("State push infrastructure started")

        except Exception as e:
            self.logger.warning(f"Failed to start state push: {e}")
            await self._cleanup()

    async def stop(self) -> None:
        """Stop state push infrastructure."""
        if not self._started:
            return

        await self._cleanup()
        self._started = False
        self.logger.info("State push infrastructure stopped")

    async def _cleanup(self) -> None:
        """Clean up all resources."""
        if self._state_manager:
            try:
                await self._state_manager.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state manager: {e}")
            self._state_manager = None

        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state producer: {e}")
            self._producer = None

        if self._queue:
            try:
                await self._queue.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state queue: {e}")
            self._queue = None

        if self._broker:
            try:
                await self._broker.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping state broker: {e}")
            self._broker = None

        self._endpoint = None

    async def emit_job_started(
        self,
        dag_edges: Dict[str, List[str]],
        stages: List[Dict[str, Any]],
    ) -> None:
        """Emit JOB_STARTED event."""
        if not self._producer:
            return

        try:
            from solstice.webui.state.messages import job_started_message

            msg = job_started_message(
                job_id=self.job_id,
                dag_edges=dag_edges,
                stages=stages,
            )
            await self._producer.produce(msg)
            self.logger.debug("Emitted JOB_STARTED event")
        except Exception as e:
            self.logger.warning(f"Failed to emit job started: {e}")

    async def emit_job_completed(
        self,
        status: str,
        start_time: Optional[float],
    ) -> None:
        """Emit JOB_COMPLETED or JOB_FAILED event."""
        if not self._producer:
            return

        try:
            from solstice.webui.state.messages import job_completed_message

            duration_ms = int((time.time() - (start_time or time.time())) * 1000)
            msg = job_completed_message(
                job_id=self.job_id,
                status=status,
                duration_ms=duration_ms,
            )
            await self._producer.produce(msg)
            self.logger.debug(f"Emitted JOB_{status} event")
        except Exception as e:
            self.logger.warning(f"Failed to emit job completed: {e}")
