"""Built-in sink operators."""

from solstice.operators.sinks.file import FileSink, FileSinkConfig
from solstice.operators.sinks.lance import LanceSink, LanceSinkConfig
from solstice.operators.sinks.print import PrintSink, PrintSinkConfig

__all__ = [
    "FileSink",
    "FileSinkConfig",
    "LanceSink",
    "LanceSinkConfig",
    "PrintSink",
    "PrintSinkConfig",
]
