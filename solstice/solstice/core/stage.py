"""Stage definition and management"""

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union
import logging

from solstice.core.operator import OperatorConfig

if TYPE_CHECKING:
    from solstice.core.stage_master import StageMasterConfig


class Stage:
    """Represents a stage in the processing pipeline"""

    def __init__(
        self,
        stage_id: str,
        operator_config: OperatorConfig,
        master_config: Optional["StageMasterConfig"] = None,
        parallelism: Union[int, Tuple[int, int]] = 1,
        worker_resources: Optional[Dict[str, float]] = None,
        skip_checkpoint: bool = False,
    ):
        """
        Initialize a stage.

        Args:
            stage_id: Unique identifier for the stage
            operator_config: Configuration for the operator (OperatorConfig subclass)
            master_config: Configuration for the stage master (StageMasterConfig subclass)
            parallelism: Number of workers. Can be:
                - int: Fixed number of workers (no auto-scaling)
                - Tuple[int, int]: (min_workers, max_workers) for auto-scaling
            worker_resources: Resource requirements per worker (num_cpus, num_gpus, memory)
            skip_checkpoint: If True, this stage will not participate in checkpoints.
                Use for lightweight stateless operators (filter, map) to reduce I/O.

        Examples:
            >>> # Fixed 4 workers, no scaling
            >>> Stage('process', MyOperatorConfig(param=value), parallelism=4)

            >>> # Auto-scaling between 2 and 10 workers
            >>> Stage('process', MyOperatorConfig(param=value), parallelism=(2, 10))

            >>> # Skip checkpoint for lightweight filter stage
            >>> Stage('filter', FilterConfig(...), skip_checkpoint=True)
        """
        self.stage_id = stage_id
        self.operator_config = operator_config
        self.skip_checkpoint = skip_checkpoint

        from solstice.core.stage_master import StageMasterConfig, DefaultStageMasterConfig

        self.master_config: StageMasterConfig = master_config or DefaultStageMasterConfig()

        # Parse parallelism parameter
        if isinstance(parallelism, int):
            # Fixed parallelism
            self.min_parallelism = parallelism
            self.max_parallelism = parallelism
        elif isinstance(parallelism, tuple) and len(parallelism) == 2:
            # Dynamic parallelism with (min, max)
            min_p, max_p = parallelism
            if min_p > max_p:
                raise ValueError(
                    f"min_parallelism ({min_p}) cannot be greater than max_parallelism ({max_p})"
                )
            self.min_parallelism = min_p
            self.max_parallelism = max_p
        else:
            raise ValueError(f"parallelism must be int or Tuple[int, int], got {type(parallelism)}")

        # Default worker resources
        self.worker_resources = worker_resources or {
            "num_cpus": 0.5,
            "num_gpus": 0,
            "memory": 500 * 1024**2,  # 500MB
        }

        self.logger = logging.getLogger(f"Stage-{stage_id}")

    @property
    def parallelism(self) -> Tuple[int, int]:
        """Get parallelism configuration"""
        return (self.min_parallelism, self.max_parallelism)

    def to_dict(self) -> Dict[str, Any]:
        """Convert stage to dictionary representation"""
        return {
            "stage_id": self.stage_id,
            "operator_config": self.operator_config.to_dict(),
            "master_config": self.master_config.to_dict(),
            "max_parallelism": self.max_parallelism,
            "min_parallelism": self.min_parallelism,
            "worker_resources": self.worker_resources,
            "skip_checkpoint": self.skip_checkpoint,
        }
