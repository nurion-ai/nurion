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

"""MinHash signature computation operator.

This operator computes MinHash signatures for text documents and expands
them into band hashes for LSH (Locality Sensitive Hashing).

Algorithm:
1. Tokenize text into k-shingles (character n-grams)
2. Hash each shingle using multiple hash functions
3. Take minimum hash for each function -> MinHash signature
4. Divide signature into bands
5. Hash each band -> band hash for LSH bucketing

Output schema:
    - doc_id: Original document ID
    - band_id: Band index (0 to num_bands-1)
    - band_hash: Hash of the band (for LSH bucketing)
    - signature: Full MinHash signature (for Jaccard computation)

The output is shuffled by band_hash so that similar documents
(with matching band hashes) end up in the same partition.
"""

from dataclasses import dataclass
from typing import ClassVar, Optional, Type

import numpy as np
import pyarrow as pa

from solstice.operators.shuffle import ShuffleOperator, ShuffleOperatorConfig


# Constants for MinHash
LARGE_PRIME = 2**61 - 1  # Mersenne prime for hash functions


@dataclass
class MinHashComputeConfig(ShuffleOperatorConfig):
    """Configuration for MinHash signature computation.

    Attributes:
        content_column: Column containing text to hash
        id_column: Column containing document ID
        num_hashes: Number of hash functions (signature length)
        num_bands: Number of bands for LSH
        shingle_size: Size of character shingles (k-grams)
        seed: Random seed for reproducibility
    """

    content_column: str = "content"
    id_column: str = "id"
    num_hashes: int = 128  # Signature length
    num_bands: int = 16  # Must divide num_hashes evenly
    shingle_size: int = 5  # Character n-gram size
    seed: int = 42

    operator_class: ClassVar[Type["MinHashComputeOperator"]] = None  # type: ignore[assignment]  # Set below

    def __post_init__(self):
        # Partition by band_hash for LSH bucketing
        self.partition_keys = ["band_hash"]

        # Validate configuration
        if self.num_hashes % self.num_bands != 0:
            raise ValueError(
                f"num_hashes ({self.num_hashes}) must be divisible by num_bands ({self.num_bands})"
            )


class MinHashComputeOperator(ShuffleOperator):
    """Operator for computing MinHash signatures.

    This operator:
    1. Tokenizes text into shingles
    2. Computes MinHash signatures
    3. Expands into band hashes for LSH
    4. Outputs one row per (document, band) pair

    Example:
        config = MinHashComputeConfig(
            content_column="text",
            id_column="doc_id",
            num_hashes=128,
            num_bands=16,
        )
        stage = Stage("minhash", config, parallelism=8)
    """

    def __init__(
        self,
        config: MinHashComputeConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.minhash_config = config

        # Pre-compute hash function parameters
        self._init_hash_params()

    def _init_hash_params(self) -> None:
        """Initialize hash function parameters."""
        np.random.seed(self.minhash_config.seed)

        # Generate random coefficients for hash functions
        # h(x) = (a * x + b) mod p
        self._hash_a = np.random.randint(
            1, LARGE_PRIME, size=self.minhash_config.num_hashes, dtype=np.uint64
        )
        self._hash_b = np.random.randint(
            0, LARGE_PRIME, size=self.minhash_config.num_hashes, dtype=np.uint64
        )

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Compute MinHash signatures and expand into bands."""
        config = self.minhash_config

        # Extract columns
        if config.content_column not in table.column_names:
            raise ValueError(f"Content column '{config.content_column}' not found")
        if config.id_column not in table.column_names:
            raise ValueError(f"ID column '{config.id_column}' not found")

        contents = table.column(config.content_column).to_pylist()
        doc_ids = table.column(config.id_column).to_pylist()

        # Compute MinHash for each document
        results = []
        for doc_id, content in zip(doc_ids, contents):
            if content is None or not content:
                continue

            # Compute signature
            signature = self._compute_signature(str(content))

            # Expand into bands
            rows_per_band = config.num_hashes // config.num_bands
            for band_id in range(config.num_bands):
                start_idx = band_id * rows_per_band
                end_idx = start_idx + rows_per_band
                band_values = signature[start_idx:end_idx]

                # Hash the band
                band_hash = self._hash_band(band_values)

                results.append(
                    {
                        "doc_id": doc_id,
                        "band_id": band_id,
                        "band_hash": band_hash,
                        "signature": signature.tobytes(),
                    }
                )

        if not results:
            return None

        # Convert to Arrow table
        return pa.table(
            {
                "doc_id": [r["doc_id"] for r in results],
                "band_id": [r["band_id"] for r in results],
                "band_hash": [r["band_hash"] for r in results],
                "signature": [r["signature"] for r in results],
            }
        )

    def _compute_signature(self, text: str) -> np.ndarray:
        """Compute MinHash signature for a text document."""
        config = self.minhash_config

        # Generate shingles
        shingles = self._get_shingles(text, config.shingle_size)
        if not shingles:
            # Return max values if no shingles
            return np.full(config.num_hashes, np.iinfo(np.uint64).max, dtype=np.uint64)

        # Hash each shingle
        shingle_hashes = np.array([hash(s) & 0xFFFFFFFFFFFFFFFF for s in shingles], dtype=np.uint64)

        # Compute MinHash signature
        signature = np.full(config.num_hashes, np.iinfo(np.uint64).max, dtype=np.uint64)

        for shingle_hash in shingle_hashes:
            # Apply all hash functions
            hashes = (self._hash_a * shingle_hash + self._hash_b) % LARGE_PRIME
            signature = np.minimum(signature, hashes)

        return signature

    def _get_shingles(self, text: str, k: int) -> set:
        """Generate k-shingles (character n-grams) from text."""
        text = text.lower().strip()
        if len(text) < k:
            return {text} if text else set()

        return {text[i : i + k] for i in range(len(text) - k + 1)}

    def _hash_band(self, band_values: np.ndarray) -> int:
        """Hash a band of signature values."""
        # Use a simple hash of the band values
        return hash(band_values.tobytes()) & 0x7FFFFFFFFFFFFFFF  # Positive int64


# Set the operator class reference
MinHashComputeConfig.operator_class = MinHashComputeOperator


def jaccard_similarity(sig1: bytes, sig2: bytes) -> float:
    """Compute Jaccard similarity from MinHash signatures.

    The Jaccard similarity is estimated as the fraction of
    hash values that are equal between the two signatures.

    Args:
        sig1: First signature (bytes)
        sig2: Second signature (bytes)

    Returns:
        Estimated Jaccard similarity (0.0 to 1.0)
    """
    arr1 = np.frombuffer(sig1, dtype=np.uint64)
    arr2 = np.frombuffer(sig2, dtype=np.uint64)

    if len(arr1) != len(arr2):
        raise ValueError("Signatures must have the same length")

    matches = np.sum(arr1 == arr2)
    return float(matches) / len(arr1)
