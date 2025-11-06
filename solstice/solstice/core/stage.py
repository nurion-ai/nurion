"""Stage definition and management"""

from typing import Any, Dict, List, Optional, Tuple, Type, Union
import logging

from solstice.core.operator import Operator


class Stage:
    """Represents a stage in the processing pipeline"""
    
    def __init__(
        self,
        stage_id: str,
        operator_class: Type[Operator],
        operator_config: Optional[Dict[str, Any]] = None,
        parallelism: Union[int, Tuple[int, int]] = 1,
        worker_resources: Optional[Dict[str, float]] = None,
    ):
        """
        Initialize a stage.
        
        Args:
            stage_id: Unique identifier for the stage
            operator_class: Class of the operator to execute
            operator_config: Configuration for the operator
            parallelism: Number of workers. Can be:
                - int: Fixed number of workers (no auto-scaling)
                - Tuple[int, int]: (min_workers, max_workers) for auto-scaling
            worker_resources: Resource requirements per worker (num_cpus, num_gpus, memory)
        
        Examples:
            >>> # Fixed 4 workers, no scaling
            >>> Stage('process', MyOp, parallelism=4)
            
            >>> # Auto-scaling between 2 and 10 workers
            >>> Stage('process', MyOp, parallelism=(2, 10))
        """
        self.stage_id = stage_id
        self.operator_class = operator_class
        self.operator_config = operator_config or {}
        
        # Parse parallelism parameter
        if isinstance(parallelism, int):
            # Fixed parallelism
            self.initial_parallelism = parallelism
            self.min_parallelism = parallelism
            self.max_parallelism = parallelism
            self.fixed_parallelism = True
        elif isinstance(parallelism, tuple) and len(parallelism) == 2:
            # Dynamic parallelism with (min, max)
            min_p, max_p = parallelism
            if min_p > max_p:
                raise ValueError(f"min_parallelism ({min_p}) cannot be greater than max_parallelism ({max_p})")
            self.min_parallelism = min_p
            self.max_parallelism = max_p
            self.initial_parallelism = min_p  # Start with minimum
            self.fixed_parallelism = False
        else:
            raise ValueError(
                f"parallelism must be int or Tuple[int, int], got {type(parallelism)}"
            )
        
        # Default worker resources
        self.worker_resources = worker_resources or {
            'num_cpus': 1,
            'num_gpus': 0,
            'memory': 2 * 1024**3,  # 2GB
        }
        
        self.logger = logging.getLogger(f"Stage-{stage_id}")
    
    @property
    def parallelism(self) -> Union[int, Tuple[int, int]]:
        """Get parallelism configuration"""
        if self.fixed_parallelism:
            return self.initial_parallelism
        else:
            return (self.min_parallelism, self.max_parallelism)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stage to dictionary representation"""
        return {
            'stage_id': self.stage_id,
            'operator_class': f"{self.operator_class.__module__}.{self.operator_class.__name__}",
            'operator_config': self.operator_config,
            'initial_parallelism': self.initial_parallelism,
            'max_parallelism': self.max_parallelism,
            'min_parallelism': self.min_parallelism,
            'fixed_parallelism': self.fixed_parallelism,
            'worker_resources': self.worker_resources,
        }


class StageMaster:
    """Wrapper for StageMasterActor to provide a cleaner API"""
    
    def __init__(self, stage: Stage, state_backend):
        self.stage = stage
        self.state_backend = state_backend
        self.actor_ref = None
    
    def start(self):
        """Start the stage master actor"""
        from solstice.actors.stage_master import StageMasterActor
        
        self.actor_ref = StageMasterActor.remote(
            stage_id=self.stage.stage_id,
            operator_class=self.stage.operator_class,
            operator_config=self.stage.operator_config,
            state_backend=self.state_backend,
            worker_resources=self.stage.worker_resources,
            initial_workers=self.stage.initial_parallelism,
            max_workers=self.stage.max_parallelism,
            min_workers=self.stage.min_parallelism,
        )
        
        return self.actor_ref
    
    def get_ref(self):
        """Get the actor reference"""
        return self.actor_ref

