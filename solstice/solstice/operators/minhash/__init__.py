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

"""MinHash-based fuzzy deduplication operators.

This module provides operators for fuzzy deduplication using MinHash LSH
(Locality Sensitive Hashing). The process involves multiple stages:

1. **MinHashComputeOperator**: Compute MinHash signatures for documents
   - Tokenizes text into shingles
   - Computes MinHash signature
   - Expands into band hashes for LSH

2. **CandidatePairOperator**: Generate candidate pairs from LSH buckets
   - Groups documents by band hash
   - Generates candidate pairs within each bucket
   - Computes exact Jaccard similarity for candidates

3. **Connected Components**: Cluster similar documents (separate module)
   - Uses distributed label propagation
   - Groups documents into clusters

4. **DedupeByClusterOperator**: Keep one representative per cluster
   - Shuffles by cluster ID
   - Keeps first document in each cluster

Architecture:
    Input Documents
         |
         v
    MinHashCompute (Stage 1)
         |
         v
    Shuffle by band_hash
         |
         v
    CandidatePairs (Stage 2)
         |
         v
    Connected Components (Iterative, Stage 3)
         |
         v
    Shuffle by cluster_id
         |
         v
    DedupeByCluster (Stage 4)
         |
         v
    Deduplicated Output
"""

from solstice.operators.minhash.compute import (
    MinHashComputeConfig,
    MinHashComputeOperator,
)
from solstice.operators.minhash.candidates import (
    CandidatePairConfig,
    CandidatePairOperator,
)

__all__ = [
    "MinHashComputeConfig",
    "MinHashComputeOperator",
    "CandidatePairConfig",
    "CandidatePairOperator",
]
