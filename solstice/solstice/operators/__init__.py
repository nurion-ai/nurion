"""Built-in operators"""

from solstice.operators.sources import FileSource, IcebergSource, LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator, MapBatchesOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sinks import FileSink, LanceSink, PrintSink
from solstice.operators.video import (
    FFmpegSceneDetectOperator,
    FFmpegSliceOperator,
)

__all__ = [
    "LanceTableSource",
    "IcebergSource",
    "FileSource",
    "MapOperator",
    "FlatMapOperator",
    "MapBatchesOperator",
    "FilterOperator",
    "FileSink",
    "LanceSink",
    "PrintSink",
    "FFmpegSceneDetectOperator",
    "FFmpegSliceOperator",
]
