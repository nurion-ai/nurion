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

"""MinHash-based fuzzy deduplication workflow.

This workflow removes near-duplicate documents using MinHash LSH
(Locality Sensitive Hashing) and Connected Components clustering.

SELF-CONTAINED ITERATION
========================
The cc_iterate stage uses CCIterateMaster which handles iteration internally:
- No special logic needed in RayJobRunner
- Iteration loop runs inside the stage master
- Multiple iterative stages can coexist in one pipeline
- Configure max_iterations via CCIterateConfig

Pipeline Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                     MinHash Deduplication                       │
    └─────────────────────────────────────────────────────────────────┘

    Input Documents
         │
         ▼
    ┌─────────────────┐
    │  MinHash Compute │  Compute signatures + expand to band hashes
    └────────┬────────┘
             │ shuffle by band_hash
             ▼
    ┌─────────────────┐
    │ Candidate Pairs  │  Find similar doc pairs (Jaccard > threshold)
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │   CC Init        │  Initialize labels (label = doc_id)
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  CC Iterate     │  Label propagation until convergence
    │  (iterative)    │  (requires iterative mode)
    └────────┬────────┘
             │ shuffle by cluster_id
             ▼
    ┌─────────────────┐
    │ Dedupe by Cluster│  Keep one doc per cluster
    └────────┬────────┘
             │
             ▼
    Deduplicated Output

Configuration:
    - content_column: Column containing text to hash (required)
    - id_column: Column containing document ID (required)
    - similarity_threshold: Jaccard similarity threshold (default: 0.8)
    - num_hashes: Number of MinHash permutations (default: 128)
    - num_bands: Number of LSH bands (default: 16)
    - max_iterations: Max CC iterations (default: 100)

Example:
    python -m solstice.main \\
        --workflow workflows.minhash_dedup \\
        --job-id dedup_001 \\
        --input /data/documents \\
        --output /data/deduplicated \\
        --content-column text \\
        --id-column doc_id \\
        --similarity-threshold 0.8
"""

import logging
from typing import Any, Dict

from solstice.core.job import Job, JobConfig
from solstice.core.stage import Stage
from solstice.queue import QueueType
from solstice.operators.sources import LanceTableSourceConfig
from solstice.operators.minhash import MinHashComputeConfig, CandidatePairConfig
from solstice.operators.connected_components import (
    CCInitConfig,
    CCIterateConfig,
    DedupeByClusterConfig,
)
from solstice.operators.sinks import FileSinkConfig, LanceSinkConfig


# Default parameters
DEFAULT_SIMILARITY_THRESHOLD = 0.8
DEFAULT_NUM_HASHES = 128
DEFAULT_NUM_BANDS = 16
DEFAULT_MAX_ITERATIONS = 100


def create_job(
    job_id: str,
    config: Dict[str, Any],
) -> Job:
    """Create a MinHash deduplication job.

    Required config:
        - input: Input Lance table path
        - output: Output path
        - content_column: Column containing text to hash
        - id_column: Column containing document ID

    Optional config:
        - similarity_threshold: Jaccard threshold (default: 0.8)
        - num_hashes: MinHash permutations (default: 128)
        - num_bands: LSH bands (default: 16)
        - max_iterations: Max CC iterations (default: 100)
        - queue_type: TANSU or MEMORY (default: TANSU)
        - output_format: json/lance (default: lance)

    Args:
        job_id: Unique job identifier
        config: Job configuration dictionary

    Returns:
        Configured Job instance
    """
    logger = logging.getLogger(__name__)
    logger.info("Creating MinHash deduplication workflow")

    # Validate required parameters
    input_path = config.get("input")
    output_path = config.get("output")
    content_column = config.get("content_column")
    id_column = config.get("id_column")

    if not input_path:
        raise ValueError("'input' parameter is required (Lance table path)")
    if not output_path:
        raise ValueError("'output' parameter is required")
    if not content_column:
        raise ValueError("'content_column' parameter is required")
    if not id_column:
        raise ValueError("'id_column' parameter is required")

    # Extract parameters with defaults
    similarity_threshold = float(config.get("similarity_threshold", DEFAULT_SIMILARITY_THRESHOLD))
    num_hashes = int(config.get("num_hashes", DEFAULT_NUM_HASHES))
    num_bands = int(config.get("num_bands", DEFAULT_NUM_BANDS))
    max_iterations = int(config.get("max_iterations", DEFAULT_MAX_ITERATIONS))

    # Queue configuration
    queue_type_str = config.get("queue_type", "TANSU")
    queue_type = QueueType[queue_type_str] if isinstance(queue_type_str, str) else queue_type_str
    tansu_storage_url = config.get("tansu_storage_url", "memory://")

    # Worker resources
    worker_resources = {
        "num_cpus": config.get("worker_num_cpus", 1.0),
        "num_gpus": config.get("worker_num_gpus", 0),
        "memory": int(config.get("worker_memory_mb", 2048)) * 1024**2,
    }

    # Parallelism settings
    minhash_parallelism = config.get("minhash_parallelism", (2, 8))
    candidate_parallelism = config.get("candidate_parallelism", (2, 8))
    cc_parallelism = config.get("cc_parallelism", (2, 8))
    dedupe_parallelism = config.get("dedupe_parallelism", (2, 4))

    # Create job config (iteration handled internally by CCIterateMaster)
    job_config = JobConfig(
        queue_type=queue_type,
        tansu_storage_url=tansu_storage_url,
    )

    job = Job(job_id=job_id, config=job_config)

    # =========================================================================
    # Stage 1: Source - Read documents from Lance table
    # =========================================================================
    source_stage = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=config.get("split_size", 1000),
            columns=[id_column, content_column],
        ),
        parallelism=1,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Stage 2: MinHash Compute - Generate signatures and band hashes
    # =========================================================================
    minhash_stage = Stage(
        stage_id="minhash",
        operator_config=MinHashComputeConfig(
            content_column=content_column,
            id_column=id_column,
            num_hashes=num_hashes,
            num_bands=num_bands,
            partition_keys=["band_hash"],  # Shuffle by band_hash
            num_partitions=config.get("num_partitions", 32),
        ),
        parallelism=minhash_parallelism,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Stage 3: Candidate Pairs - Find similar document pairs
    # =========================================================================
    candidate_stage = Stage(
        stage_id="candidates",
        operator_config=CandidatePairConfig(
            similarity_threshold=similarity_threshold,
            doc_id_column=id_column,
            band_hash_column="band_hash",
            signature_column="signature",
            max_pairs_per_bucket=config.get("max_pairs_per_bucket", 10000),
        ),
        parallelism=candidate_parallelism,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Stage 4: CC Init - Initialize labels for Connected Components
    # =========================================================================
    cc_init_stage = Stage(
        stage_id="cc_init",
        operator_config=CCInitConfig(
            doc_id_1_column="doc_id_1",
            doc_id_2_column="doc_id_2",
        ),
        parallelism=cc_parallelism,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Stage 5: CC Iterate - Label propagation (iterative)
    # =========================================================================
    cc_iterate_stage = Stage(
        stage_id="cc_iterate",
        operator_config=CCIterateConfig(
            doc_id_column="doc_id",
            neighbor_label_column="neighbor_label",
            partition_keys=["doc_id"],
            num_partitions=config.get("num_partitions", 32),
            max_iterations=max_iterations,  # Iteration handled by CCIterateMaster
        ),
        parallelism=cc_parallelism,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Stage 6: Dedupe by Cluster - Keep one document per cluster
    # =========================================================================
    dedupe_stage = Stage(
        stage_id="dedupe",
        operator_config=DedupeByClusterConfig(
            doc_id_column="doc_id",
            cluster_id_column="label",
            partition_keys=["label"],
            num_partitions=config.get("num_partitions", 32),
        ),
        parallelism=dedupe_parallelism,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Stage 7: Sink - Write deduplicated documents
    # =========================================================================
    output_format = config.get("output_format", "lance")
    if output_format == "lance":
        sink_config = LanceSinkConfig(
            table_path=output_path,
            mode="overwrite",
            buffer_size=config.get("sink_buffer_size", 1000),
        )
    else:
        sink_config = FileSinkConfig(
            output_path=output_path,
            format=output_format,
            buffer_size=config.get("sink_buffer_size", 1000),
        )

    sink_stage = Stage(
        stage_id="sink",
        operator_config=sink_config,
        parallelism=1,
        worker_resources=worker_resources,
    )

    # =========================================================================
    # Build DAG
    # =========================================================================
    job.add_stage(source_stage)
    job.add_stage(minhash_stage, upstream_stages=["source"])
    job.add_stage(candidate_stage, upstream_stages=["minhash"])
    job.add_stage(cc_init_stage, upstream_stages=["candidates"])
    job.add_stage(cc_iterate_stage, upstream_stages=["cc_init"])
    job.add_stage(dedupe_stage, upstream_stages=["cc_iterate"])
    job.add_stage(sink_stage, upstream_stages=["dedupe"])

    logger.info(
        f"MinHash dedup workflow created: {len(job.stages)} stages, "
        f"threshold={similarity_threshold}, bands={num_bands}, hashes={num_hashes}"
    )

    return job


# CLI usage:
# python -m solstice.main \
#   --workflow workflows.minhash_dedup \
#   --job-id dedup_001 \
#   --input /data/documents \
#   --output /data/deduplicated \
#   --content-column text \
#   --id-column doc_id \
#   --similarity-threshold 0.8
