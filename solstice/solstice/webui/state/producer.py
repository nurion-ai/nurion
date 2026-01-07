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

"""State producer for push-based metrics.

StateProducer provides a simple interface for producing state messages
to Tansu. It handles:
- Rate limiting (to avoid overwhelming the queue)
- Async fire-and-forget produce (doesn't block caller)
- Sequence number generation
- Graceful degradation on failures
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Optional

from solstice.webui.state.messages import StateMessage, StateMessageType
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.queue import QueueClient


class StateProducer:
    """Producer for state messages with rate limiting.

    Features:
    - Rate limiting per message type (configurable)
    - Fire-and-forget async produce
    - Automatic sequence numbering
    - Graceful failure handling (log and continue)

    Usage:
        producer = StateProducer(job_id, queue_client, state_topic)
        await producer.start()

        # Produce immediately
        await producer.produce(message)

        # Produce with rate limiting (skipped if too frequent)
        await producer.produce_rate_limited(message)

        await producer.stop()
    """

    # Default rate limits (minimum interval in seconds between messages)
    DEFAULT_RATE_LIMITS = {
        StateMessageType.WORKER_METRICS: 0.5,  # Max 2/s per worker
        StateMessageType.STAGE_METRICS: 1.0,  # Max 1/s per stage
        StateMessageType.BACKPRESSURE: 2.0,  # Max 1/2s
        # Lifecycle events are not rate limited
    }

    def __init__(
        self,
        job_id: str,
        queue_client: "QueueClient",
        state_topic: str,
        rate_limits: Optional[dict] = None,
    ):
        """Initialize state producer.

        Args:
            job_id: Job identifier
            queue_client: Tansu queue client
            state_topic: Topic name for state messages
            rate_limits: Optional custom rate limits per message type
        """
        self.job_id = job_id
        self.queue_client = queue_client
        self.state_topic = state_topic
        self.rate_limits = rate_limits or self.DEFAULT_RATE_LIMITS.copy()

        self.logger = create_ray_logger(f"StateProducer-{job_id}")

        # Rate limiting state: (message_type, source_id) -> last_emit_time
        self._last_emit: dict[tuple[StateMessageType, str], float] = {}

        # Sequence counter
        self._sequence = 0

        # Background task queue for fire-and-forget
        self._pending_produces: asyncio.Queue[StateMessage] = asyncio.Queue()
        self._background_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the background produce task."""
        if self._running:
            return

        self._running = True
        self._background_task = asyncio.create_task(self._produce_loop())
        self.logger.debug("StateProducer started")

    async def stop(self) -> None:
        """Stop the producer and flush pending messages."""
        self._running = False

        if self._background_task:
            # Allow some time for pending messages to flush
            try:
                await asyncio.wait_for(self._drain_pending(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("Timeout draining pending state messages")

            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None

        self.logger.debug("StateProducer stopped")

    async def produce(self, message: StateMessage) -> None:
        """Produce a message immediately (queued for async send).

        This is fire-and-forget - it doesn't wait for the message
        to be sent to Tansu. Failures are logged but not raised.
        """
        # Assign sequence number
        self._sequence += 1
        message.sequence = self._sequence

        # Queue for background produce
        await self._pending_produces.put(message)

    async def produce_rate_limited(self, message: StateMessage) -> bool:
        """Produce a message with rate limiting.

        Returns:
            True if message was queued, False if rate limited (skipped)
        """
        rate_limit = self.rate_limits.get(message.message_type)

        if rate_limit is not None:
            key = (message.message_type, message.source_id)
            last_time = self._last_emit.get(key, 0.0)
            now = time.time()

            if now - last_time < rate_limit:
                # Rate limited - skip this message
                return False

            self._last_emit[key] = now

        await self.produce(message)
        return True

    def should_emit(self, message_type: StateMessageType, source_id: str) -> bool:
        """Check if a message should be emitted (not rate limited).

        Useful for pre-checking before constructing expensive message payloads.
        """
        rate_limit = self.rate_limits.get(message_type)

        if rate_limit is None:
            return True

        key = (message_type, source_id)
        last_time = self._last_emit.get(key, 0.0)
        now = time.time()

        return now - last_time >= rate_limit

    async def _produce_loop(self) -> None:
        """Background loop that sends pending messages to Tansu."""
        while self._running or not self._pending_produces.empty():
            try:
                # Wait for a message with timeout
                try:
                    message = await asyncio.wait_for(
                        self._pending_produces.get(),
                        timeout=0.1,
                    )
                except asyncio.TimeoutError:
                    continue

                # Send to Tansu
                try:
                    await self.queue_client.produce(
                        self.state_topic,
                        message.to_bytes(),
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Failed to produce state message: {e}, "
                        f"type={message.message_type}, source={message.source_id}"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in produce loop: {e}")

    async def _drain_pending(self) -> None:
        """Drain all pending messages."""
        while not self._pending_produces.empty():
            try:
                message = self._pending_produces.get_nowait()
                await self.queue_client.produce(
                    self.state_topic,
                    message.to_bytes(),
                )
            except Exception as e:
                self.logger.warning(f"Failed to drain message: {e}")
