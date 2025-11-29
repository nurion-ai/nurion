"""Base operator interface with EasyConfig pattern"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar
import logging

from solstice.core.models import SplitPayload, Split


T = TypeVar("T", bound="Operator")


@dataclass
class OperatorConfig(ABC):
    """Base configuration class for operators.
    
    Subclasses should define their configuration fields as dataclass fields,
    and set the `operator_class` class variable to the corresponding operator class.
    
    Example:
        @dataclass
        class MyOperatorConfig(OperatorConfig):
            operator_class = MyOperator
            
            param1: str
            param2: int = 10
            
        # Usage:
        config = MyOperatorConfig(param1="value")
        operator = config.setup(worker_id="worker_0")
    """
    
    operator_class: ClassVar[Type["Operator"]]
    
    def setup(self, worker_id: Optional[str] = None) -> "Operator":
        """Create and return an operator instance with this configuration.
        
        Args:
            worker_id: Optional worker ID to pass to the operator
            
        Returns:
            Configured operator instance
        """
        return self.operator_class(config=self, worker_id=worker_id)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary representation."""
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            # Handle nested configs
            if isinstance(value, OperatorConfig):
                result[f.name] = value.to_dict()
            else:
                result[f.name] = value
        return result

    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value by key, with optional default.
        
        This method provides dict-like access for backward compatibility.
        """
        return getattr(self, key, default)


class Operator(ABC):
    """Base class for all operators"""

    def __init__(
        self,
        config: OperatorConfig,
        worker_id: Optional[str] = None,
    ):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.worker_id = worker_id

    @abstractmethod
    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        pass

    def close(self) -> None:
        """Clean up operator resources"""
        pass


class SourceOperator(Operator):
    @abstractmethod
    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read data for a specific split.

        Args:
            split: Split object containing all metadata needed to read data
                  (data_range, metadata, etc.)

        Returns:
            SplitPayload containing the data, or None if no data available
        """
        pass

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split for source operators.

        For source operators, payload is None and split contains all metadata.
        This method calls read() with the split.
        """
        if payload is not None:
            raise ValueError("Source operators should not receive payload, only split")

        return self.read(split)


class SinkOperator(Operator):
    """Base class for sink operators"""
