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

"""Lineage API - split lineage and data flow.

4 core endpoints:
- GET /jobs/{job_id}/lineage/overview - stage-level aggregated stats
- GET /jobs/{job_id}/lineage/stages/{stage_id}/splits - paginated splits list
- GET /jobs/{job_id}/lineage/splits/{split_id} - single split details
- GET /jobs/{job_id}/lineage/splits/{split_id}/trace - complete trace
"""

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["lineage"])


@router.get("/jobs/{job_id}/lineage/overview")
async def get_lineage_overview(job_id: str, request: Request) -> Dict[str, Any]:
    """Get stage-level lineage overview with aggregated statistics.

    Returns:
        - stages: list of {stage_id, splits_count, total_output_rows, total_output_bytes}
        - edges: list of {from_stage, to_stage, splits_count, total_rows, total_bytes,
                         min/max rows, min/max bytes, min/max/avg processing_ms}
        - dag_edges: original DAG structure
    """
    if request.app.state.storage:
        return request.app.state.storage.get_lineage_overview(job_id)

    return {"stages": [], "edges": [], "dag_edges": {}}


@router.get("/jobs/{job_id}/lineage/stages/{stage_id}/splits")
async def list_stage_splits(
    job_id: str,
    stage_id: str,
    limit: int = Query(100, ge=10, le=1000),
    offset: int = Query(0, ge=0),
    request: Request = None,
) -> List[Dict[str, Any]]:
    """List splits for a stage with pagination.

    Returns:
        List of split lineage records (sorted by timestamp, newest first)
    """
    if request.app.state.storage:
        return request.app.state.storage.list_splits_by_stage(job_id, stage_id, limit, offset)

    return []


@router.get("/jobs/{job_id}/lineage/splits/{split_id}/trace")
async def get_split_trace(
    job_id: str,
    split_id: str,
    request: Request = None,
) -> Dict[str, Any]:
    """Get complete lineage trace for a split (both upstream and downstream).

    Returns:
        - splits: list of split details ordered by stage
        - edges: list of {source, target} relationships
        - root_split_id: the starting split
    """
    if request.app.state.storage:
        return request.app.state.storage.get_split_trace(job_id, split_id)

    return {"splits": [], "edges": [], "root_split_id": split_id}


@router.get("/jobs/{job_id}/lineage/splits/{split_id}")
async def get_split_lineage(
    job_id: str,
    split_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get single split's lineage details.

    Returns:
        Full lineage record including timing, sizes, parent IDs, etc.
    """
    if request.app.state.storage:
        lineage = request.app.state.storage.get_split_lineage(job_id, split_id)
        if lineage:
            return lineage

    raise HTTPException(status_code=404, detail=f"Split {split_id} not found")
