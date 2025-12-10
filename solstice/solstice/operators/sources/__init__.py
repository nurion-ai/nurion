"""Built-in source operators."""

from solstice.operators.sources.file import FileSource, FileSourceConfig
from solstice.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
from solstice.operators.sources.lance import (
    LanceTableSource,
    LanceTableSourceConfig,
    LanceSourceMaster,
)
from solstice.operators.sources.source import (
    SourceMaster,
    SourceConfig,
)
from solstice.operators.sources.spark import (
    SparkSource,
    SparkSourceConfig,
    SparkSourceMaster,
)
from solstice.operators.sources.sparkv2 import (
    SparkSourceV2Config,
    SparkSourceV2Master,
)

__all__ = [
    # File source
    "FileSource",
    "FileSourceConfig",
    # Iceberg source
    "IcebergSource",
    "IcebergSourceConfig",
    # Lance source
    "LanceTableSource",
    "LanceTableSourceConfig",
    "LanceSourceMaster",
    # Source base
    "SourceMaster",
    "SourceConfig",
    # Spark source V1
    "SparkSource",
    "SparkSourceConfig",
    "SparkSourceMaster",
    # Spark source V2 (simplified - no operator needed)
    "SparkSourceV2Config",
    "SparkSourceV2Master",
]
