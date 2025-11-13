"""File-based source operator emitting Arrow batches."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from solstice.core.models import Batch
from solstice.operators.sources.base import ArrowStreamingSource


class FileSource(ArrowStreamingSource):
    """Source operator for reading from local files (JSON, Parquet, CSV)."""

    SUPPORTED_FORMATS = {"json", "parquet", "csv"}

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.file_paths = [str(path) for path in cfg.get("file_paths", [])]
        self.file_format = cfg.get("format", "json").lower()

        if self.file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.file_format}")

        self.current_file_idx = 0
        self.current_row_idx = 0

    def open(self, context) -> None:
        super().open(context)
        if self._context:
            self.current_file_idx = self._context.get_state("file_idx", 0)
            self.current_row_idx = self._context.get_state("row_idx", 0)
            self._resume_offset = self.current_row_idx
            self._emitted_offset = self.current_row_idx
        else:
            self.current_file_idx = 0
            self.current_row_idx = 0

    def restore(self, state: Dict[str, Any]) -> None:
        super().restore(state)
        self.current_file_idx = state.get("file_idx", self.current_file_idx)
        self.current_row_idx = state.get("row_idx", self.current_row_idx)
        self._resume_offset = self.current_row_idx
        self._emitted_offset = self.current_row_idx

    def read(self) -> Iterable[Batch]:
        for file_idx in range(self.current_file_idx, len(self.file_paths)):
            file_path = self.file_paths[file_idx]
            table = self._load_table(file_path)
            if not table or table.num_rows == 0:
                self._advance_file(file_idx)
                continue

            if file_idx == self.current_file_idx:
                self._resume_offset = self.current_row_idx
            else:
                self._resume_offset = 0

            metadata = {"file": file_path, "format": self.file_format}
            for batch in self._emit_table(table, metadata=metadata):
                self.current_row_idx += len(batch)
                if self._context:
                    self._context.set_state("file_idx", file_idx)
                    self._context.set_state("row_idx", self.current_row_idx)
                yield batch

            self._advance_file(file_idx)

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

