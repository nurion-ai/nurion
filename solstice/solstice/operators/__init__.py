"""Built-in operators"""

from solstice.operators.sources import FileSource, IcebergSource, LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator, KeyByOperator
from solstice.operators.batch import MapBatchesOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sinks import FileSink, LanceSink, PrintSink, Sink
from solstice.operators.video import (
    FFmpegSceneDetectOperator,
    FFmpegSliceOperator,
    attach_slice_hash,
    keep_every_n,
)

__all__ = [
    "LanceTableSource",
    "IcebergSource",
    "FileSource",
    "MapOperator",
    "FlatMapOperator",
    "KeyByOperator",
    "MapBatchesOperator",
    "FilterOperator",
    "Sink",
    "FileSink",
    "LanceSink",
    "PrintSink",
    "FFmpegSceneDetectOperator",
    "FFmpegSliceOperator",
    "attach_slice_hash",
    "keep_every_n",
]
