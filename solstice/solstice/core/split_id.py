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


def _normalize_dict(data: Dict[str, Any]) -> str:
    """Convert dict to deterministic string for hashing.

    Handles nested dicts and sorts keys for consistency.
    """

    def _serialize(obj: Any) -> str:
        if isinstance(obj, dict):
            # Sort keys and recursively serialize
            items = sorted((k, _serialize(v)) for k, v in obj.items())
            return "{" + ",".join(f"{k}:{v}" for k, v in items) + "}"
        elif isinstance(obj, (list, tuple)):
            return "[" + ",".join(_serialize(x) for x in obj) + "]"
        else:
            return str(obj)

    return _serialize(data)


def generate_source_split_id(stage_id: str, data_range: Dict[str, Any]) -> str:
    """Generate split ID for source stage.

    The ID is purely based on the data_range content, so:
    - Same file + offset always produces same split ID
    - Any worker can generate the same ID for same input
    - Checkpoint recovery can match by ID

    Args:
        stage_id: The source stage ID
        data_range: Data range info (file path, offset, partition, etc.)

    Returns:
        Deterministic split ID: "{stage_id}:{hash}"

    Example:
        >>> generate_source_split_id("source", {"file": "data.json", "offset": 0})
        "source:a1b2c3d4e5f6"
    """
    content = _normalize_dict(data_range)
    content_hash = _content_hash(content)
    return f"{stage_id}:{content_hash}"


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


def generate_split_id_with_key(
    stage_id: str,
    key: Any,
    parent_split_ids: List[str],
) -> str:
    """Generate split ID for keyed/partitioned output.

    Used when an operator partitions output by key (e.g., group by).

    Args:
        stage_id: The stage producing this split
        key: The partition key
        parent_split_ids: IDs of parent splits

    Returns:
        Deterministic split ID including key hash
    """
    parents_str = ",".join(sorted(parent_split_ids))
    content = f"{parents_str}|key={key}"
    content_hash = _content_hash(content)
    return f"{stage_id}:{content_hash}"


def parse_split_id(split_id: str) -> Dict[str, str]:
    """Parse a split ID into components.

    Format: {stage_id}:{content_hash}

    Returns:
        Dict with keys: stage_id, hash, full_id
    """
    parts = split_id.split(":")
    if len(parts) >= 2:
        return {
            "stage_id": parts[0],
            "hash": parts[1],
            "full_id": split_id,
        }
    return {"full_id": split_id, "stage_id": split_id}


def get_stage_from_split_id(split_id: str) -> str:
    """Extract stage ID from split ID."""
    return split_id.split(":")[0]


def splits_from_same_source(split_id1: str, split_id2: str) -> bool:
    """Check if two splits are from the same source data.

    Since split IDs are content-based, same ID means same source.
    """
    return split_id1 == split_id2
