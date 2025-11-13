"""Shared Arrow-based source operator utilities."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, Optional, Union

import pyarrow as pa

from solstice.core.models import Batch
from solstice.core.operator import SourceOperator


class ArrowStreamingSource(SourceOperator):
    """Base class for sources that materialize Arrow batches."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.batch_size: Optional[int] = cfg.get("batch_size")
        self._resume_offset: int = 0
        self._emitted_offset: int = 0
        self._batch_counter: int = 0
        self._split_counter: int = 0

    def open(self, context=None) -> None:
        super().open(context)
        if self._context:
            offset = self._context.get_state("offset", 0)
            self._resume_offset = offset
            self._emitted_offset = offset
        else:
            self._resume_offset = 0
            self._emitted_offset = 0
        self._batch_counter = 0
        self._split_counter = 0

    def restore(self, state: Dict[str, Any]) -> None:
        super().restore(state)
        if self._context:
            offset = self._context.get_state("offset", 0)
            self._resume_offset = offset
            self._emitted_offset = offset

    # ------------------------------------------------------------------
    # Helpers for subclasses
    # ------------------------------------------------------------------
    def _batch_metadata(self) -> Dict[str, Any]:
        return {"source": self.__class__.__name__}

    def _next_batch_id(self) -> str:
        stage_prefix = self._context.stage_id if self._context and self._context.stage_id else "source"
        batch_id = f"{stage_prefix}_batch_{self._batch_counter}"
        self._batch_counter += 1
        return batch_id

    def _next_split_id(self) -> str:
        stage_prefix = self._context.stage_id if self._context and self._context.stage_id else "source"
        split_id = f"{stage_prefix}_split_{self._split_counter}"
        self._split_counter += 1
        return split_id

    def _update_offset(self, count: int) -> None:
        self._emitted_offset += count
        if self._context:
            self._context.set_state("offset", self._emitted_offset)

    def _apply_offset_to_table(self, table: pa.Table) -> pa.Table:
        if self._resume_offset <= 0 or table.num_rows == 0:
            return table

        if self._resume_offset >= table.num_rows:
            self._resume_offset -= table.num_rows
            return pa.table({})

        sliced = table.slice(self._resume_offset)
        self._resume_offset = 0
        return sliced

    def _emit_table(
        self,
        table: pa.Table,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Batch]:
        table = self._apply_offset_to_table(table)
        if table.num_rows == 0:
            return

        chunk_size = self.batch_size or table.num_rows
        combined_metadata = self._batch_metadata()
        if metadata:
            combined_metadata = {**combined_metadata, **metadata}

        for record_batch in table.to_batches(chunk_size):
            batch = Batch.from_arrow(
                record_batch,
                batch_id=self._next_batch_id(),
                source_split=self._next_split_id(),
                metadata=combined_metadata,
            )
            self._update_offset(len(batch))
            yield batch

    def _emit_arrow(
        self,
        data: Union[pa.Table, pa.RecordBatch, Iterable[pa.RecordBatch]],
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Batch]:
        if isinstance(data, pa.Table):
            yield from self._emit_table(data, metadata=metadata)
            return

        if isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
            yield from self._emit_table(table, metadata=metadata)
            return

        for record_batch in data:
            table = pa.Table.from_batches([record_batch])
            yield from self._emit_table(table, metadata=metadata)

