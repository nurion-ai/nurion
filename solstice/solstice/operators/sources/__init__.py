"""Built-in source operators."""

from solstice.operators.sources.file import FileSource, FileSourceConfig
from solstice.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
from solstice.operators.sources.lance import (
    LanceTableSource,
    LanceTableSourceConfig,
    LanceSourceStageMaster,
    LanceSourceStageMasterConfig,
)
from solstice.operators.sources.source import SourceStageMaster
from solstice.operators.sources.spark import (
    SparkSource,
    SparkSourceConfig,
    SparkSourceStageMaster,
    SparkSourceStageMasterConfig,
)

__all__ = [
    "FileSource",
    "FileSourceConfig",
    "IcebergSource",
    "IcebergSourceConfig",
    "LanceTableSource",
    "LanceTableSourceConfig",
    "LanceSourceStageMaster",
    "LanceSourceStageMasterConfig",
    "SourceStageMaster",
    "SparkSource",
    "SparkSourceConfig",
    "SparkSourceStageMaster",
    "SparkSourceStageMasterConfig",
]
