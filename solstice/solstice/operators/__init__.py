"""Built-in operators"""

from solstice.operators.sources import (
    FileSource,
    FileSourceConfig,
    IcebergSource,
    IcebergSourceConfig,
    LanceTableSource,
    LanceTableSourceConfig,
)
from solstice.operators.map import (
    MapOperator,
    MapOperatorConfig,
    FlatMapOperator,
    FlatMapOperatorConfig,
    MapBatchesOperator,
    MapBatchesOperatorConfig,
)
from solstice.operators.filter import FilterOperator, FilterOperatorConfig
from solstice.operators.sinks import (
    FileSink,
    FileSinkConfig,
    LanceSink,
    LanceSinkConfig,
    PrintSink,
    PrintSinkConfig,
)
from solstice.operators.video import (
    FFmpegSceneDetectOperator,
    FFmpegSceneDetectConfig,
    FFmpegSliceOperator,
    FFmpegSliceConfig,
)

__all__ = [
    # Source operators and configs
    "LanceTableSource",
    "LanceTableSourceConfig",
    "IcebergSource",
    "IcebergSourceConfig",
    "FileSource",
    "FileSourceConfig",
    # Map operators and configs
    "MapOperator",
    "MapOperatorConfig",
    "FlatMapOperator",
    "FlatMapOperatorConfig",
    "MapBatchesOperator",
    "MapBatchesOperatorConfig",
    # Filter operator and config
    "FilterOperator",
    "FilterOperatorConfig",
    # Sink operators and configs
    "FileSink",
    "FileSinkConfig",
    "LanceSink",
    "LanceSinkConfig",
    "PrintSink",
    "PrintSinkConfig",
    # Video operators and configs
    "FFmpegSceneDetectOperator",
    "FFmpegSceneDetectConfig",
    "FFmpegSliceOperator",
    "FFmpegSliceConfig",
]
