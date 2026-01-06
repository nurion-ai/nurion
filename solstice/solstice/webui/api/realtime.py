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

"""Real-time API endpoints (embedded mode only)."""

import asyncio
from typing import AsyncGenerator

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

router = APIRouter(tags=["realtime"])


@router.get("/jobs/{job_id}/sse/metrics")
async def stream_metrics(job_id: str, request: Request) -> EventSourceResponse:
    """Stream real-time metrics via Server-Sent Events.

    Args:
        job_id: Job identifier

    Returns:
        SSE event stream
    """

    async def event_generator() -> AsyncGenerator[dict, None]:
        """Generate SSE events."""
        runner = request.app.state.job_runner

        if not runner or runner.job.job_id != job_id:
            yield {"event": "error", "data": "Job not found"}
            return

        while runner.is_running:
            # Get current status
            status = await runner.get_status_async()

            # Send metrics
            yield {
                "event": "metrics",
                "data": {
                    "job_id": job_id,
                    "elapsed_time": status.elapsed_time,
                    "stages": status.stages,
                },
            }

            await asyncio.sleep(2)  # Update every 2 seconds

    return EventSourceResponse(event_generator())
