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
    """Source operator for reading from local files (JSON, Parquet, CSV).

    Supports checkpoint/resume via offset tracking. The offset contains:
    - file_path: Current file being read
    - row_offset: Number of rows already read from the current file
    - completed_files: List of files that have been fully processed
    """

    SUPPORTED_FORMATS = {"json", "parquet", "csv"}

    def __init__(self, config: FileSourceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self.file_paths = [str(path) for path in config.file_paths]
        self.file_format = config.format.lower()

        if self.file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.file_format}")

        # Initialize offset tracking
        self._current_offset = {
            "completed_files": [],
            "current_file": None,
            "row_offset": 0,
        }

    def read(self, split: Split) -> Optional[SplitPayload]:
        file_path = split.data_range.get("file_path")
        if not file_path:
            raise ValueError("Split missing file_path for FileSource")

        # Check if this file was already completed (from checkpoint restore)
        if file_path in self._current_offset.get("completed_files", []):
            self.logger.debug(f"Skipping already completed file: {file_path}")
            return None

        table = self._load_table(file_path)
        if not table or table.num_rows == 0:
            # Mark file as completed
            self._mark_file_completed(file_path)
            return None

        # Apply row offset if resuming from checkpoint
        row_offset = 0
        if (
            self._current_offset.get("current_file") == file_path
            and self._current_offset.get("row_offset", 0) > 0
        ):
            row_offset = self._current_offset["row_offset"]
            if row_offset >= table.num_rows:
                # Already processed all rows in this file
                self._mark_file_completed(file_path)
                return None
            table = table.slice(row_offset)
            self.logger.info(f"Resuming {file_path} from row {row_offset}")

        # Update offset for checkpoint
        self.update_offset(
            {
                "current_file": file_path,
                "row_offset": row_offset + table.num_rows,
            }
        )

        # Mark file as completed after reading all rows
        self._mark_file_completed(file_path)

        return SplitPayload.from_arrow(
            table,
            split_id=split.split_id,
        )

    def _mark_file_completed(self, file_path: str) -> None:
        """Mark a file as fully processed."""
        completed = self._current_offset.get("completed_files", [])
        if file_path not in completed:
            completed.append(file_path)
            self.update_offset(
                {
                    "completed_files": completed,
                    "current_file": None,
                    "row_offset": 0,
                }
            )

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
