"""File-based source operator emitting Arrow batches."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from solstice.core.models import Batch, Split, SplitStatus
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

    def plan_splits(self) -> List[Split]:
        if not self.file_paths:
            raise ValueError("file_paths is required for FileSource")

        stage_id = (self.config or {}).get("stage_id", "file_source")
        splits: List[Split] = []
        for idx, file_path in enumerate(self.file_paths):
            splits.append(
                Split(
                    split_id=f"{stage_id}_file_{idx}",
                    stage_id=stage_id,
                    data_range={"file_path": file_path, "format": self.file_format},
                    metadata={"file_path": file_path},
                    status=SplitStatus.PENDING,
                )
            )
        return splits

    def read(self, split: Split) -> Optional[Batch]:
        file_path = split.data_range.get("file_path")
        if not file_path:
            raise ValueError("Split missing file_path for FileSource")

        table = self._load_table(file_path)
        if not table or table.num_rows == 0:
            return None

        metadata = dict(split.metadata)
        metadata.update({"file": file_path, "format": self.file_format})

        return Batch.from_arrow(
            table,
            batch_id=f"{split.stage_id}_batch_{split.split_id}",
            source_split=split.split_id,
            metadata=metadata,
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
