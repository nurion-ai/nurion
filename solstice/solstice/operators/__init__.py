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
from solstice.operators.shuffle import (
    ShuffleOperator,
    ShuffleOperatorConfig,
    RepartitionOperator,
    RepartitionConfig,
    split_by_partition,
    is_shuffle_operator,
)
from solstice.operators.dedupe import (
    HashDedupeOperator,
    HashDedupeConfig,
)
from solstice.operators.minhash import (
    MinHashComputeConfig,
    MinHashComputeOperator,
    CandidatePairConfig,
    CandidatePairOperator,
)
from solstice.operators.connected_components import (
    CCInitConfig,
    CCInitOperator,
    CCIterateConfig,
    CCIterateOperator,
    CCMessageConfig,
    CCMessageOperator,
    DedupeByClusterConfig,
    DedupeByClusterOperator,
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
    # Shuffle operators and configs
    "ShuffleOperator",
    "ShuffleOperatorConfig",
    "RepartitionOperator",
    "RepartitionConfig",
    "split_by_partition",
    "is_shuffle_operator",
    # Dedupe operators and configs
    "HashDedupeOperator",
    "HashDedupeConfig",
    # MinHash operators and configs
    "MinHashComputeConfig",
    "MinHashComputeOperator",
    "CandidatePairConfig",
    "CandidatePairOperator",
    # Connected Components operators and configs
    "CCInitConfig",
    "CCInitOperator",
    "CCIterateConfig",
    "CCIterateOperator",
    "CCMessageConfig",
    "CCMessageOperator",
    "DedupeByClusterConfig",
    "DedupeByClusterOperator",
]
