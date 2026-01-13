#!/usr/bin/env python3
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

"""Example: MinHash deduplication workflow.

This example demonstrates:
1. Creating test documents with near-duplicates
2. Building a MinHash dedup pipeline
3. Running with iterative Connected Components

Self-contained Iteration:
- CCIterateMaster handles iteration internally
- No special logic needed in RayJobRunner
- Configure max_iterations via CCIterateConfig
- Multiple iterative stages can coexist in one pipeline

Run:
    cd solstice
    python examples/minhash_dedup_example.py
"""

import asyncio
import logging
import tempfile
from pathlib import Path

import pyarrow as pa
import lance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_test_data(path: str) -> int:
    """Create test documents with near-duplicates."""
    documents = [
        # Group 1: Near-duplicates (fox)
        {"doc_id": "doc_001", "text": "The quick brown fox jumps over the lazy dog. Classic pangram."},
        {"doc_id": "doc_002", "text": "The quick brown fox jumps over the lazy dog! A classic pangram."},
        # Group 2: Near-duplicates (ML)
        {"doc_id": "doc_003", "text": "Machine learning is AI that enables computers to learn from data."},
        {"doc_id": "doc_004", "text": "Machine learning is AI enabling computers to learn from data."},
        # Group 3: Unique
        {"doc_id": "doc_005", "text": "Python is a high-level programming language."},
        {"doc_id": "doc_006", "text": "Data engineering builds systems for data at scale."},
        {"doc_id": "doc_007", "text": "Cloud computing provides on-demand resources."},
    ]

    table = pa.Table.from_pylist(documents)
    lance.write_dataset(table, path, mode="overwrite")
    logger.info(f"Created {len(documents)} test documents at {path}")
    return len(documents)


async def run_example():
    """Run the MinHash dedup workflow."""
    logger.info("=" * 60)
    logger.info("MinHash Deduplication Example")
    logger.info("=" * 60)

    from workflows.minhash_dedup import create_job
    from solstice.operators.connected_components import CCIterateConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = str(Path(tmpdir) / "input.lance")
        output_path = str(Path(tmpdir) / "output.lance")

        # Step 1: Create test data
        logger.info("\n[Step 1] Creating test data with duplicates...")
        total_docs = create_test_data(input_path)

        # Step 2: Create job
        logger.info("\n[Step 2] Creating MinHash dedup job...")
        config = {
            "input": input_path,
            "output": output_path,
            "content_column": "text",
            "id_column": "doc_id",
            "similarity_threshold": 0.5,
            "num_hashes": 64,
            "num_bands": 8,
            "max_iterations": 10,
            "queue_type": "MEMORY",
            "output_format": "lance",
            "num_partitions": 4,
        }
        job = create_job("minhash_dedup_example", config)

        # Show pipeline structure
        logger.info(f"\nPipeline: {len(job.stages)} stages")
        for stage_id, stage in job.stages.items():
            config_type = type(stage.operator_config).__name__
            # Check if iterative stage (uses custom master)
            is_iterative = stage.operator_config.master_class is not None
            marker = " (iterative)" if is_iterative else ""
            logger.info(f"  - {stage_id}: {config_type}{marker}")

        # Step 3: Run pipeline
        logger.info("\n[Step 3] Running pipeline...")
        logger.info("Note: cc_iterate stage handles iteration internally")
        runner = job.create_ray_runner()

        try:
            status = await runner.run(timeout=300)
            logger.info(f"\nPipeline completed in {status.elapsed_time:.2f}s")
        finally:
            await runner.stop()

        # Step 4: Verify results
        logger.info("\n[Step 4] Verifying results...")
        if Path(output_path).exists():
            result_ds = lance.dataset(output_path)
            result_count = result_ds.count_rows()
            logger.info(f"Input: {total_docs} documents")
            logger.info(f"Output: {result_count} documents")

            # With near-duplicates, we expect fewer output docs
            # Group 1 (2 docs) -> 1, Group 2 (2 docs) -> 1, Unique (3 docs) -> 3
            # Expected: ~5 unique documents
            expected = 5
            if result_count <= expected + 1:
                logger.info(f"✓ Deduplication successful (expected ~{expected})")
            else:
                logger.warning(f"✗ More docs than expected ({result_count} > {expected})")
        else:
            logger.warning("Output not found - pipeline may have failed")

        logger.info("\n" + "=" * 60)
        logger.info("Example completed!")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
