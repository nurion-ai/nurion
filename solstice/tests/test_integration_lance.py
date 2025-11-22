"""Integration tests for LanceTableSource using real fragments."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
import pyarrow as pa
from lance.dataset import write_dataset

from solstice.core.models import Split
from solstice.operators.sources import LanceTableSource


def build_lance_splits(dataset_uri: str, *, split_size: int) -> list[Split]:
    import lance

    dataset = lance.dataset(dataset_uri)
    splits: list[Split] = []
    for fragment in sorted(dataset.get_fragments(), key=lambda frag: frag.fragment_id):
        row_count = fragment.count_rows()
        for offset in range(0, row_count, split_size):
            splits.append(
                Split(
                    split_id=f"fragment_{fragment.fragment_id}_{offset}",
                    stage_id="source",
                    data_range={
                        "fragment_id": fragment.fragment_id,
                        "offset": offset,
                        "limit": split_size,
                    },
                )
            )
    return splits


@pytest.fixture
def lance_dataset_uri():
    tmpdir = tempfile.mkdtemp()
    table_path = Path(tmpdir) / "table.lance"

    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        }
    )
    write_dataset(data, str(table_path))

    try:
        yield str(table_path)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.integration
class TestLanceSource:
    def test_lance_source_reads_fragments(self, lance_dataset_uri):
        source = LanceTableSource({"dataset_uri": lance_dataset_uri, "split_size": 2})
        splits = build_lance_splits(lance_dataset_uri, split_size=2)

        batches = []
        for split in splits:
            batch = source.process_split(split)
            if batch:
                batches.append(batch)

        assert sum(len(batch) for batch in batches) == 5
        column_names = set(batches[0].column_names)
        assert {"id", "value", "name"}.issubset(column_names)

        source.close()

    def test_lance_source_respects_column_selection(self, lance_dataset_uri):
        source = LanceTableSource(
            {"dataset_uri": lance_dataset_uri, "split_size": 10, "columns": ["id", "name"]}
        )
        splits = build_lance_splits(lance_dataset_uri, split_size=10)
        for split in splits:
            split.data_range["columns"] = ["id", "name"]

        batches = [source.process_split(split) for split in splits]
        batches = [batch for batch in batches if batch]
        assert len(batches) == 1
        column_names = set(batches[0].column_names)
        assert {"id", "name"}.issubset(column_names)

        source.close()
