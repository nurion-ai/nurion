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

"""Common test utilities for Solstice tests."""

import asyncio
from typing import Callable, Union


async def wait_until(
    condition: Callable[[], Union[bool, "asyncio.Future[bool]"]],
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str = "Condition not met",
) -> None:
    """Wait for a condition to become True, with timeout (Awaitility-style).

    Similar to Java's Awaitility. Polls the condition at regular intervals
    until it returns True or timeout is reached.

    Args:
        condition: A callable returning bool (sync) or awaitable bool (async).
        timeout: Maximum time to wait in seconds.
        interval: Time between checks in seconds.
        message: Error message if timeout is reached.

    Raises:
        AssertionError: If condition is not met within timeout.

    Example:
        await wait_until(lambda: len(workers) == 2, timeout=5.0)
        await wait_until(lambda: master.is_ready, timeout=10.0, interval=0.2)
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = condition()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"{message} within {timeout}s")
