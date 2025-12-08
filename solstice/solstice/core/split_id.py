"""Split ID generation utilities.

Split IDs are **purely content-based** (no counters) to ensure:
1. Same input always produces same split ID (deterministic)
2. Multiple workers won't generate conflicting IDs
3. Checkpoint recovery can match splits by ID

Format: {stage_id}:{content_hash}
- stage_id: The stage that produced this split
- content_hash: SHA256 hash of the split's defining content

For source splits: hash of data_range (file path, offset, etc.)
For derived splits: hash of parent_split_ids + sequence

Examples:
- Source split: "source:a1b2c3d4e5f6"  (hash of data_range)
- Derived split: "transform:f7g8h9i0j1k2"  (hash of parent IDs)
"""

import hashlib
from typing import Any, Dict, List


def _content_hash(content: str, length: int = 12) -> str:
    """Generate deterministic hash from content string."""
    return hashlib.sha256(content.encode()).hexdigest()[:length]

def generate_derived_split_id(
    stage_id: str,
    parent_split_ids: List[str],
    sequence_in_parent: int = 0,
) -> str:
    """Generate split ID for derived (non-source) split.

    The ID is based on parent lineage, so:
    - Same parents always produce same child ID
    - Deterministic across workers
    - Supports checkpoint recovery

    Args:
        stage_id: The stage producing this split
        parent_split_ids: IDs of parent splits (must be non-empty)
        sequence_in_parent: Index if parent produces multiple outputs

    Returns:
        Deterministic split ID: "{stage_id}:{hash}"

    Example:
        >>> generate_derived_split_id("transform", ["source:a1b2c3"], 0)
        "transform:d4e5f6g7h8i9"
    """
    # Sort parent IDs for determinism
    parents_str = ",".join(sorted(parent_split_ids))
    content = f"{parents_str}|{sequence_in_parent}"
    content_hash = _content_hash(content)
    return f"{stage_id}:{content_hash}"