"""Built-in sink operators."""

from solstice.operators.sinks.base import Sink
from solstice.operators.sinks.file import FileSink
from solstice.operators.sinks.lance import LanceSink
from solstice.operators.sinks.print import PrintSink

__all__ = ["Sink", "FileSink", "LanceSink", "PrintSink"]

