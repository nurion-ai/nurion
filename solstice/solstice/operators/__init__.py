"""Built-in operators"""

from solstice.operators.source import LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sink import Sink

__all__ = ["LanceTableSource", "MapOperator", "FlatMapOperator", "FilterOperator", "Sink"]

