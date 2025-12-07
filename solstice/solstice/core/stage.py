"""Stage definition and management"""

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type, Union
import logging

from solstice.core.operator import OperatorConfig, Operator


class Stage:
    """Represents a stage in the processing pipeline.
    
    A stage consists of:
    - An operator class that processes data
    - Configuration for that operator
    - Parallelism settings (number of workers)
    - Resource requirements per worker
    
    Example:
        ```python
        # Simple source stage
        source = Stage(
            stage_id="source",
            operator_class=MySourceOperator,
            operator_config={"batch_size": 100},
            parallelism=1,
        )
        
        # Parallel transform stage
        transform = Stage(
            stage_id="transform",
            operator_class=MyTransformOperator,
            operator_config={},
            parallelism=4,  # 4 parallel workers
            worker_resources={"num_gpus": 1},
        )
        ```
    """

    def __init__(
        self,
        stage_id: str,
        operator_class: Type[Operator] = None,
        operator_config: Union[OperatorConfig, Dict[str, Any], None] = None,
        parallelism: Union[int, Tuple[int, int]] = 1,
        worker_resources: Optional[Dict[str, float]] = None,
        skip_checkpoint: bool = False,
        master_config: Any = None,  # Legacy parameter, ignored
    ):
        """
        Initialize a stage.

        Args:
            stage_id: Unique identifier for the stage
            operator_class: The operator class to use for processing
            operator_config: Configuration for the operator (dict or OperatorConfig)
            parallelism: Number of workers. Can be:
                - int: Fixed number of workers (no auto-scaling)
                - Tuple[int, int]: (min_workers, max_workers) for auto-scaling
            worker_resources: Resource requirements per worker (num_cpus, num_gpus, memory)
            skip_checkpoint: If True, this stage will not participate in checkpoints.
                Use for lightweight stateless operators (filter, map) to reduce I/O.

        Examples:
            >>> # Fixed 4 workers, no scaling
            >>> Stage('process', MyOperator, {'param': 'value'}, parallelism=4)

            >>> # Auto-scaling between 2 and 10 workers
            >>> Stage('process', MyOperator, {}, parallelism=(2, 10))

            >>> # Skip checkpoint for lightweight filter stage
            >>> Stage('filter', FilterOperator, {}, skip_checkpoint=True)
        """
        self.stage_id = stage_id
        self.operator_class = operator_class
        self.skip_checkpoint = skip_checkpoint
        
        # Store operator_config as-is (may be dict, OperatorConfig, or None)
        # Keep original object to preserve setup() method for legacy OperatorConfig
        self.operator_config = operator_config if operator_config is not None else {}

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
        # Convert operator_config to dict if it has a to_dict method
        if hasattr(self.operator_config, 'to_dict'):
            config_dict = self.operator_config.to_dict()
        elif isinstance(self.operator_config, dict):
            config_dict = self.operator_config
        else:
            config_dict = {}
            
        return {
            "stage_id": self.stage_id,
            "operator_class": self.operator_class.__name__ if self.operator_class else None,
            "operator_config": config_dict,
            "max_parallelism": self.max_parallelism,
            "min_parallelism": self.min_parallelism,
            "worker_resources": self.worker_resources,
            "skip_checkpoint": self.skip_checkpoint,
        }
