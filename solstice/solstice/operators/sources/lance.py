"""Lance table source operator and source master."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator, Optional

import lance

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SourceOperator, OperatorConfig
from solstice.operators.sources.source import SourceMaster

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@dataclass
class LanceTableSourceConfig(OperatorConfig):
    """Configuration for LanceTableSource operator and LanceSourceMaster.

    This unified config is used by both the operator (for reading splits)
    and the master (for planning splits).
    """

    dataset_uri: str
    """URI of the Lance dataset."""

    filter: Optional[str] = None
    """Filter expression to apply when reading."""

    columns: Optional[Iterable[str]] = None
    """Columns to read from the dataset."""

    split_size: int = 1024
    """Number of rows per split."""

    # SourceConfig fields for master
    tansu_storage_url: str = "memory://"
    """Tansu storage URL (s3://, sqlite://, memory://)."""


def _get_lance_storage_options(uri: str) -> Optional[dict]:
    """Get storage options for S3 URIs."""
    if uri.startswith("s3://"):
        from solstice.utils.remote import get_lance_storage_options

        bucket = uri[5:].split("/")[0]
        return get_lance_storage_options(bucket)
    return None


class LanceTableSource(SourceOperator):
    """Source operator for reading from Lance tables."""

    def __init__(self, config: LanceTableSourceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        if not config.dataset_uri:
            raise ValueError("dataset_uri is required for LanceTableSource")
        self.dataset_uri: str = config.dataset_uri
        self.storage_options = _get_lance_storage_options(self.dataset_uri)

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read data for a split from the Lance dataset."""
        dataset = lance.dataset(self.dataset_uri, storage_options=self.storage_options)

        # Get split metadata from data_range
        data_range = dict(split.data_range)  # Make a copy to avoid modifying original
        fragment_id = data_range.pop("fragment_id")

        fragment = dataset.get_fragment(fragment_id)
        fragment_scanner = fragment.scanner(
            **data_range,
            with_row_id=True,
        )
        table = fragment_scanner.to_table()
        if table.num_rows == 0:
            return SplitPayload.empty(split_id=f"{split.split_id}:{self.worker_id}")

        return SplitPayload.from_arrow(
            table,
            split_id=f"{split.split_id}:{self.worker_id}",
        )

    def close(self) -> None:
        self.dataset_uri = None


# Set operator_class after class definition
LanceTableSourceConfig.operator_class = LanceTableSource


class LanceSourceMaster(SourceMaster):
    """Source master for Lance tables.

    Generates splits based on Lance dataset fragments and writes
    split metadata to a persistent TansuBackend queue.

    Workers consume from the queue and use LanceTableSource operator
    to read actual data for each split.
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        **kwargs,
    ):
        # Get config from stage.operator_config
        operator_cfg = stage.operator_config
        if not isinstance(operator_cfg, LanceTableSourceConfig):
            raise TypeError(
                f"LanceSourceMaster requires LanceTableSourceConfig, got {type(operator_cfg)}"
            )

        super().__init__(job_id, stage, **kwargs)

        self.dataset_uri: str = operator_cfg.dataset_uri
        self.filter: Optional[str] = operator_cfg.filter
        self.columns: Optional[Iterable[str]] = operator_cfg.columns
        self.split_size: int = operator_cfg.split_size
        self.storage_options = _get_lance_storage_options(self.dataset_uri)

        # Load dataset for split planning
        self.dataset = lance.dataset(self.dataset_uri, storage_options=self.storage_options)
        self.logger.info(f"Loaded Lance dataset: {self.dataset_uri}")

    def plan_splits(self) -> Iterator[Split]:
        """Plan splits based on Lance dataset fragments.

        Generates one split per (fragment, offset) pair, ensuring
        deterministic split ordering based on fragment_id.
        """
        # Sort fragments by fragment_id for deterministic ordering
        sorted_fragments = sorted(self.dataset.get_fragments(), key=lambda x: x.fragment_id)

        split_idx = 0
        for frag in sorted_fragments:
            row_count = frag.count_rows()
            for offset in range(0, row_count, self.split_size):
                yield Split(
                    split_id=f"{self.stage.stage_id}_split_{split_idx}",
                    stage_id=self.stage.stage_id,
                    data_range={
                        "filter": self.filter,
                        "columns": list(self.columns) if self.columns else None,
                        "fragment_id": frag.fragment_id,
                        "offset": offset,
                        "limit": self.split_size,
                    },
                )
                split_idx += 1

        self.logger.info(f"Planned {split_idx} splits from {len(sorted_fragments)} fragments")


# Set master_class after class definition
LanceTableSourceConfig.master_class = LanceSourceMaster
