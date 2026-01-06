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

"""Lineage API - split lineage and data flow."""

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["lineage"])


@router.get("/jobs/{job_id}/lineage")
async def get_lineage_graph(job_id: str, request: Request) -> Dict[str, Any]:
    """Get complete lineage graph for a job.

    Args:
        job_id: Job identifier

    Returns:
        Graph data with nodes and edges
    """
    if request.app.state.storage:
        return request.app.state.storage.get_lineage_graph(job_id)

    return {"nodes": [], "edges": []}


@router.get("/jobs/{job_id}/lineage/{split_id}")
async def get_split_lineage(
    job_id: str,
    split_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get lineage for a specific split.

    Args:
        job_id: Job identifier
        split_id: Split identifier

    Returns:
        Lineage information
    """
    if request.app.state.storage:
        lineage = request.app.state.storage.get_split_lineage(job_id, split_id)
        if lineage:
            return lineage

    raise HTTPException(status_code=404, detail=f"Split {split_id} not found")
