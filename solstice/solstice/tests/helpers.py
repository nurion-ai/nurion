"""Reusable helpers for test operators and fixtures."""

from __future__ import annotations

from typing import Any, Dict


def mark_seen(value: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``value`` with a ``seen`` flag set to ``True``."""
    result = dict(value)
    result["seen"] = True
    return result


def identity_transform(value: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of ``value``."""
    return dict(value)

