"""Runtime modules for executing Solstice jobs.

V2 Runtime (Recommended):
- RayJobRunnerV2: Async runner with queue-based communication
- run_pipeline: Convenience function for simple execution

Legacy Runtime:
- RayJobRunner: Original runner with master-to-master push
"""

# V2 Runtime (recommended)
from solstice.runtime.ray_runner_v2 import (
    RayJobRunnerV2,
    run_pipeline,
    PipelineStatus,
)

# Legacy Runtime
from solstice.runtime.ray_runner import RayJobRunner

__all__ = [
    # V2 (recommended)
    "RayJobRunnerV2",
    "run_pipeline",
    "PipelineStatus",
    
    # Legacy
    "RayJobRunner",
]
