# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for MinHash operators."""

import pyarrow as pa
import pytest

from solstice.core.models import Split, SplitPayload
from solstice.operators.minhash import (
    MinHashComputeConfig,
    MinHashComputeOperator,
    CandidatePairConfig,
    CandidatePairOperator,
)
from solstice.operators.minhash.compute import jaccard_similarity


class TestMinHashComputeOperator:
    """Tests for MinHashComputeOperator."""

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="minhash", data_range={})

    def test_compute_basic(self, sample_split):
        """Test basic MinHash computation."""
        table = pa.table({
            "id": ["doc1", "doc2", "doc3"],
            "content": [
                "The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy cat",
                "A completely different document about something else",
            ],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashComputeConfig(
            content_column="content",
            id_column="id",
            num_hashes=64,
            num_bands=8,
        )
        operator = config.setup()
        operator.set_num_partitions(4)

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Should have 3 docs * 8 bands = 24 rows
        assert result_table.num_rows == 24

        # Check columns
        assert "doc_id" in result_table.column_names
        assert "band_id" in result_table.column_names
        assert "band_hash" in result_table.column_names
        assert "signature" in result_table.column_names

        operator.close()

    def test_compute_similar_docs_share_bands(self, sample_split):
        """Test that similar documents share some band hashes."""
        # Two very similar documents
        table = pa.table({
            "id": ["doc1", "doc2"],
            "content": [
                "The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy cat",
            ],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashComputeConfig(
            content_column="content",
            id_column="id",
            num_hashes=128,
            num_bands=16,
            seed=42,
        )
        operator = config.setup()
        operator.set_num_partitions(4)

        result = operator.process_split(sample_split, payload)
        result_table = result.to_table()

        # Group by band_id and check for shared band_hashes
        doc1_bands = {}
        doc2_bands = {}

        for i in range(result_table.num_rows):
            doc_id = result_table.column("doc_id")[i].as_py()
            band_id = result_table.column("band_id")[i].as_py()
            band_hash = result_table.column("band_hash")[i].as_py()

            if doc_id == "doc1":
                doc1_bands[band_id] = band_hash
            else:
                doc2_bands[band_id] = band_hash

        # Similar docs should share at least some band hashes
        shared_bands = sum(
            1 for band_id in doc1_bands
            if doc1_bands[band_id] == doc2_bands.get(band_id)
        )

        # With high similarity, we expect at least a few shared bands
        assert shared_bands > 0

        operator.close()

    def test_compute_empty_content(self, sample_split):
        """Test handling of empty content."""
        table = pa.table({
            "id": ["doc1", "doc2"],
            "content": ["Some content", ""],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashComputeConfig(
            content_column="content",
            id_column="id",
            num_hashes=64,
            num_bands=8,
        )
        operator = config.setup()
        operator.set_num_partitions(4)

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Only doc1 should produce output (8 bands)
        assert result_table.num_rows == 8

        operator.close()

    def test_compute_deterministic(self, sample_split):
        """Test that MinHash computation is deterministic."""
        table = pa.table({
            "id": ["doc1"],
            "content": ["The quick brown fox"],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = MinHashComputeConfig(
            content_column="content",
            id_column="id",
            num_hashes=64,
            num_bands=8,
            seed=42,
        )

        operator1 = config.setup()
        operator1.set_num_partitions(4)
        result1 = operator1.process_split(sample_split, payload)

        operator2 = config.setup()
        operator2.set_num_partitions(4)
        result2 = operator2.process_split(sample_split, payload)

        # Signatures should be identical
        sig1 = result1.to_table().column("signature")[0].as_py()
        sig2 = result2.to_table().column("signature")[0].as_py()
        assert sig1 == sig2

        operator1.close()
        operator2.close()


class TestCandidatePairOperator:
    """Tests for CandidatePairOperator."""

    @pytest.fixture
    def sample_split(self):
        """Create a sample split."""
        return Split(split_id="test", stage_id="candidates", data_range={})

    def test_generate_pairs_basic(self, sample_split):
        """Test basic candidate pair generation."""
        # Create fake MinHash output with same band_hash for two docs
        # (simulating similar documents)
        import numpy as np

        sig1 = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()
        sig2 = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()  # Identical
        sig3 = np.array([5, 6, 7, 8], dtype=np.uint64).tobytes()  # Different

        table = pa.table({
            "doc_id": ["doc1", "doc2", "doc3"],
            "band_hash": [100, 100, 200],  # doc1 and doc2 share band_hash
            "signature": [sig1, sig2, sig3],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = CandidatePairConfig(similarity_threshold=0.5)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Should have one pair (doc1, doc2) with similarity 1.0
        assert result_table.num_rows == 1
        assert result_table.column("similarity")[0].as_py() == 1.0

        operator.close()

    def test_generate_pairs_threshold(self, sample_split):
        """Test that pairs below threshold are filtered."""
        import numpy as np

        # Create signatures with low similarity
        sig1 = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()
        sig2 = np.array([5, 6, 7, 8], dtype=np.uint64).tobytes()  # All different

        table = pa.table({
            "doc_id": ["doc1", "doc2"],
            "band_hash": [100, 100],  # Same band_hash
            "signature": [sig1, sig2],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = CandidatePairConfig(similarity_threshold=0.5)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        # Similarity is 0.0, below threshold, so no pairs
        assert result is None

        operator.close()

    def test_generate_pairs_no_duplicates_within_batch(self, sample_split):
        """Test that duplicate pairs are not generated within same batch.

        Note: The operator is stateless, so it only deduplicates within
        each batch. Cross-batch duplicates are handled by downstream
        stages (CC algorithm naturally handles duplicate edges).
        """
        import numpy as np

        sig = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()

        # Same pair appears in same batch via different bands
        table = pa.table({
            "doc_id": ["doc1", "doc2", "doc1", "doc2"],
            "band_hash": [100, 100, 200, 200],  # Two bands, same docs
            "signature": [sig, sig, sig, sig],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = CandidatePairConfig(similarity_threshold=0.5)
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        # Should produce the pair only once
        assert result is not None
        assert result.to_table().num_rows == 1

        operator.close()

    def test_generate_pairs_batch_level_stateless(self, sample_split):
        """Test that operator is stateless across batches.

        Each batch is processed independently. Cross-batch duplicate
        handling is done downstream by the CC algorithm.
        """
        import numpy as np

        sig = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()

        # Same pair in two separate batches
        table1 = pa.table({
            "doc_id": ["doc1", "doc2"],
            "band_hash": [100, 100],
            "signature": [sig, sig],
        })
        payload1 = SplitPayload(data=table1, split_id="test1")

        table2 = pa.table({
            "doc_id": ["doc1", "doc2"],
            "band_hash": [200, 200],  # Different band, same docs
            "signature": [sig, sig],
        })
        payload2 = SplitPayload(data=table2, split_id="test2")

        config = CandidatePairConfig(similarity_threshold=0.5)
        operator = config.setup()

        result1 = operator.process_split(sample_split, payload1)
        result2 = operator.process_split(sample_split, payload2)

        # Both batches produce the pair (stateless)
        assert result1 is not None
        assert result1.to_table().num_rows == 1

        # Second batch also produces the pair (no cross-batch tracking)
        assert result2 is not None
        assert result2.to_table().num_rows == 1

        operator.close()

    def test_generate_pairs_large_bucket(self, sample_split):
        """Test handling of large buckets with sampling."""
        import numpy as np

        # Create a large bucket
        n_docs = 100
        sig = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()

        table = pa.table({
            "doc_id": [f"doc{i}" for i in range(n_docs)],
            "band_hash": [100] * n_docs,  # All same band_hash
            "signature": [sig] * n_docs,
        })
        payload = SplitPayload(data=table, split_id="test")

        config = CandidatePairConfig(
            similarity_threshold=0.5,
            max_pairs_per_bucket=50,  # Limit pairs
        )
        operator = config.setup()

        result = operator.process_split(sample_split, payload)

        assert result is not None
        result_table = result.to_table()

        # Should be limited by max_pairs_per_bucket
        assert result_table.num_rows <= 50

        operator.close()


class TestJaccardSimilarity:
    """Tests for jaccard_similarity function."""

    def test_identical_signatures(self):
        """Test identical signatures have similarity 1.0."""
        import numpy as np

        sig = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()
        assert jaccard_similarity(sig, sig) == 1.0

    def test_different_signatures(self):
        """Test completely different signatures have similarity 0.0."""
        import numpy as np

        sig1 = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()
        sig2 = np.array([5, 6, 7, 8], dtype=np.uint64).tobytes()
        assert jaccard_similarity(sig1, sig2) == 0.0

    def test_partial_similarity(self):
        """Test partially similar signatures."""
        import numpy as np

        sig1 = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()
        sig2 = np.array([1, 2, 5, 6], dtype=np.uint64).tobytes()  # 2/4 match
        assert jaccard_similarity(sig1, sig2) == 0.5

    def test_length_mismatch_error(self):
        """Test that mismatched lengths raise error."""
        import numpy as np

        sig1 = np.array([1, 2, 3, 4], dtype=np.uint64).tobytes()
        sig2 = np.array([1, 2, 3], dtype=np.uint64).tobytes()

        with pytest.raises(ValueError, match="same length"):
            jaccard_similarity(sig1, sig2)
