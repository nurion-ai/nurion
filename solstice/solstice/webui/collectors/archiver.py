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

"""Job archiver for storing complete job state after completion."""

import time
from typing import TYPE_CHECKING

from solstice.webui.storage import JobStorage
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner


class JobArchiver:
    """Archive complete job state when job finishes.

    Archives:
    - Job configuration and metadata
    - Stage definitions and final status
    - DAG structure
    - Final metrics snapshot
    - Exception summary
    - Worker event summary

    Usage:
        archiver = JobArchiver(storage)
        await archiver.archive_job(job_runner)
    """

    def __init__(self, storage: JobStorage):
        """Initialize archiver.

        Args:
            storage: SlateDB storage instance
        """
        self.storage = storage
        self.logger = create_ray_logger("JobArchiver")

    async def archive_job(self, job_runner: "RayJobRunner") -> None:
        """Archive complete job state.

        Args:
            job_runner: RayJobRunner instance to archive
        """
        job_id = job_runner.job.job_id
        self.logger.info(f"Archiving job {job_id}")

        try:
            # Get final status
            final_status = await job_runner.get_status_async()

            # Determine final job status
            if final_status.error:
                status = "FAILED"
            elif final_status.is_running:
                status = "CANCELLED"  # Should not happen, but handle it
            else:
                status = "COMPLETED"

            # Get previously stored job data to use as fallback for metrics
            # (workers may have already stopped by the time we archive)
            stored_job = self.storage.get_job_archive()
            stored_stages_by_id = {}
            if stored_job:
                for s in stored_job.get("stages", []):
                    stored_stages_by_id[s.get("stage_id")] = s

            # Collect stage information
            stages = []
            for stage_id, master in job_runner._masters.items():
                stage_status = await master.get_status_async()

                # Get final metrics from workers (may fail if workers stopped)
                metrics_dict = {}
                try:
                    stage_metrics = await master.collect_metrics()
                    metrics_dict = stage_metrics.to_dict()
                except Exception as e:
                    self.logger.debug(f"Could not collect metrics from workers for {stage_id}: {e}")

                # Use stored metrics as fallback if worker metrics are empty
                input_records = metrics_dict.get("input_records", 0)
                output_records = metrics_dict.get("output_records", 0)

                if input_records == 0 and output_records == 0:
                    # Try latest metrics snapshot first (most accurate)
                    latest_metrics = self.storage.get_latest_stage_metrics(stage_id)
                    if latest_metrics:
                        input_records = latest_metrics.get("input_records", 0)
                        output_records = latest_metrics.get("output_records", 0)

                    # If still 0, try from previously stored job data
                    if input_records == 0 and output_records == 0:
                        stored_stage = stored_stages_by_id.get(stage_id, {})
                        input_records = stored_stage.get("input_records", 0)
                        output_records = stored_stage.get("output_records", 0)

                    if input_records or output_records:
                        self.logger.debug(
                            f"Using stored metrics for {stage_id}: in={input_records}, out={output_records}"
                        )

                stages.append(
                    {
                        "stage_id": stage_id,
                        "operator_type": type(master.stage.operator_config).__name__,
                        "min_parallelism": master.config.min_workers,
                        "max_parallelism": master.config.max_workers,
                        "final_worker_count": stage_status.worker_count,
                        "is_finished": stage_status.is_finished,
                        "failed": stage_status.failed,
                        "failure_message": stage_status.failure_message,
                        "final_metrics": metrics_dict,
                        # Store at top level for template compatibility
                        "input_records": input_records,
                        "output_records": output_records,
                        "worker_count": metrics_dict.get("worker_count", stage_status.worker_count),
                    }
                )

            # Build archive data
            archive_data = {
                "job_id": job_id,
                "status": status,
                "start_time": final_status.start_time or time.time(),
                "end_time": time.time(),
                "duration_ms": int(final_status.elapsed_time * 1000),
                # Configuration
                "config": {
                    "queue_type": job_runner.queue_type.value,
                    "tansu_storage_url": job_runner.tansu_storage_url,
                },
                # Structure
                "stages": stages,
                "dag_edges": job_runner.job.dag_edges,
                # Final metrics
                "final_metrics": {
                    "stages": {s["stage_id"]: s["final_metrics"] for s in stages},
                },
                # Summary (input/output totals from final stage metrics)
                "total_input_records": sum(
                    s["final_metrics"].get("input_records", 0) for s in stages
                ),
                "total_output_records": sum(
                    s["final_metrics"].get("output_records", 0) for s in stages
                ),
                # Error
                "error": final_status.error,
            }

            # Store archive
            self.storage.store_job_archive(archive_data)
            self.logger.info(f"Archived job {job_id} with status {status}, {len(stages)} stages")

        except Exception as e:
            self.logger.error(f"Failed to archive job {job_id}: {e}")
