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

"""Candidate pair generation operator for MinHash LSH.

This operator takes MinHash band outputs and generates candidate pairs
of similar documents. Documents that share a band hash are considered
candidates for similarity comparison.

Algorithm:
1. Group documents by (band_id, band_hash)
2. For each group with multiple documents, generate pairs
3. Compute exact Jaccard similarity for each pair
4. Filter pairs above similarity threshold

Output schema:
    - doc_id_1: First document ID
    - doc_id_2: Second document ID
    - similarity: Jaccard similarity (0.0 to 1.0)

The output feeds into the Connected Components algorithm to cluster
similar documents.

This operator is STATELESS - it does not track seen pairs across batches.
Duplicate pairs from different batches are deduplicated downstream by
the Connected Components algorithm or can be handled via a separate
shuffle-dedupe step if needed.
"""

from dataclasses import dataclass
from typing import ClassVar, Dict, List, Optional, Set, Tuple, Type

import numpy as np
import pyarrow as pa

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig
from solstice.operators.minhash.compute import jaccard_similarity


@dataclass
class CandidatePairConfig(OperatorConfig):
    """Configuration for candidate pair generation.

    Attributes:
        similarity_threshold: Minimum Jaccard similarity for pairs
        max_pairs_per_bucket: Maximum pairs to generate per bucket
        doc_id_column: Column containing document ID
        band_hash_column: Column containing band hash
        signature_column: Column containing MinHash signature
    """

    similarity_threshold: float = 0.5
    max_pairs_per_bucket: int = 10000
    doc_id_column: str = "doc_id"
    band_hash_column: str = "band_hash"
    signature_column: str = "signature"

    operator_class: ClassVar[Type["CandidatePairOperator"]] = None  # type: ignore[assignment]  # Set below


class CandidatePairOperator(Operator):
    """Stateless operator for generating candidate pairs from MinHash bands.

    This operator:
    1. Groups documents by band_hash (same hash = potential duplicates)
    2. Generates all pairs within each bucket
    3. Computes exact Jaccard similarity
    4. Outputs pairs above the similarity threshold

    The operator is STATELESS - it does not maintain any in-memory state
    across batches. Each batch is processed independently.

    Note on duplicate pairs:
    - Pairs are deduplicated within each batch
    - Cross-batch duplicates may occur and are handled downstream
    - The CC algorithm naturally handles duplicate edges

    Example:
        config = CandidatePairConfig(similarity_threshold=0.8)
        stage = Stage("candidates", config, parallelism=8)

    Note: This operator receives data already shuffled by band_hash,
    so all documents with the same band_hash are in the same partition.
    """

    def __init__(
        self,
        config: CandidatePairConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.candidate_config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Generate candidate pairs from MinHash band data."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        config = self.candidate_config

        # Extract columns
        doc_ids = table.column(config.doc_id_column).to_pylist()
        band_hashes = table.column(config.band_hash_column).to_pylist()
        signatures = table.column(config.signature_column).to_pylist()

        # Group by band_hash
        buckets: Dict[int, List[Tuple[str, bytes]]] = {}
        for doc_id, band_hash, signature in zip(doc_ids, band_hashes, signatures):
            if band_hash not in buckets:
                buckets[band_hash] = []
            buckets[band_hash].append((doc_id, signature))

        # Generate candidate pairs (dedupe within batch only)
        pairs = []
        seen_in_batch: Set[Tuple[str, str]] = set()

        for band_hash, docs in buckets.items():
            if len(docs) < 2:
                continue

            # Generate pairs within bucket
            bucket_pairs = self._generate_pairs(docs, config.max_pairs_per_bucket)

            for doc1, sig1, doc2, sig2 in bucket_pairs:
                # Create canonical pair (smaller ID first)
                if str(doc1) > str(doc2):
                    doc1, sig1, doc2, sig2 = doc2, sig2, doc1, sig1

                pair_key = (str(doc1), str(doc2))
                if pair_key in seen_in_batch:
                    continue

                # Compute similarity
                sim = jaccard_similarity(sig1, sig2)
                if sim >= config.similarity_threshold:
                    pairs.append(
                        {
                            "doc_id_1": doc1,
                            "doc_id_2": doc2,
                            "similarity": sim,
                        }
                    )
                    seen_in_batch.add(pair_key)

        if not pairs:
            return None

        # Convert to Arrow table
        result = pa.table(
            {
                "doc_id_1": [p["doc_id_1"] for p in pairs],
                "doc_id_2": [p["doc_id_2"] for p in pairs],
                "similarity": [p["similarity"] for p in pairs],
            }
        )

        return SplitPayload(data=result, split_id=split.split_id)

    def _generate_pairs(
        self,
        docs: List[Tuple[str, bytes]],
        max_pairs: int,
    ) -> List[Tuple[str, bytes, str, bytes]]:
        """Generate pairs from a bucket of documents.

        If the bucket is too large, sample pairs randomly.
        """
        n = len(docs)
        total_pairs = n * (n - 1) // 2

        if total_pairs <= max_pairs:
            # Generate all pairs
            pairs = []
            for i in range(n):
                for j in range(i + 1, n):
                    doc1, sig1 = docs[i]
                    doc2, sig2 = docs[j]
                    pairs.append((doc1, sig1, doc2, sig2))
            return pairs
        else:
            # Sample pairs randomly
            pairs = []
            seen = set()
            attempts = 0
            max_attempts = max_pairs * 3

            while len(pairs) < max_pairs and attempts < max_attempts:
                i = np.random.randint(0, n)
                j = np.random.randint(0, n)
                if i != j and (i, j) not in seen and (j, i) not in seen:
                    seen.add((i, j))
                    doc1, sig1 = docs[i]
                    doc2, sig2 = docs[j]
                    pairs.append((doc1, sig1, doc2, sig2))
                attempts += 1

            return pairs


# Set the operator class reference
CandidatePairConfig.operator_class = CandidatePairOperator
