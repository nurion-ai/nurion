"""File-based source operator emitting Arrow batches."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Literal, Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SourceOperator, OperatorConfig


@dataclass
class FileSourceConfig(OperatorConfig):
    """Configuration for FileSource operator."""
    
    file_paths: List[str] = field(default_factory=list)
    """List of file paths to read from."""
    
    format: Literal["json", "parquet", "csv"] = "json"
    """File format (json, parquet, or csv)."""


class FileSource(SourceOperator):
    """Source operator for reading from local files (JSON, Parquet, CSV)."""

    SUPPORTED_FORMATS = {"json", "parquet", "csv"}

    def __init__(self, config: FileSourceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self.file_paths = [str(path) for path in config.file_paths]
        self.file_format = config.format.lower()

        if self.file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.file_format}")

        self.current_file_idx = 0
        self.current_row_idx = 0

    def read(self, split: Split) -> Optional[SplitPayload]:
        file_path = split.data_range.get("file_path")
        if not file_path:
            raise ValueError("Split missing file_path for FileSource")

        table = self._load_table(file_path)
        if not table or table.num_rows == 0:
            return None

        return SplitPayload.from_arrow(
            table,
            split_id=split.split_id,
        )

    def _advance_file(self, file_idx: int) -> None:
        if file_idx >= self.current_file_idx:
            self.current_file_idx = file_idx + 1
            self.current_row_idx = 0
            self._resume_offset = 0
            if self._context:
                self._context.set_state("file_idx", self.current_file_idx)
                self._context.set_state("row_idx", 0)

    def _load_table(self, file_path: str) -> pa.Table:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if self.file_format == "json":
            rows = []
            with path.open("r") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))
            return pa.Table.from_pylist(rows)

        if self.file_format == "parquet":
            return pq.read_table(file_path)

        if self.file_format == "csv":
            return pacsv.read_csv(file_path)

        raise ValueError(f"Unsupported format: {self.file_format}")


# Set operator_class after class definition
FileSourceConfig.operator_class = FileSource
