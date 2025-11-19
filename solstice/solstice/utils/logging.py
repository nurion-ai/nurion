"""Logging utilities tailored for Ray actors."""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def create_ray_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Create a logger configured for Ray worker processes.

    Ray runs actors in separate processes, so each actor should configure its own
    logger to ensure messages go to stdout/stderr for collection by Ray.
    """

    logger = logging.getLogger(name)

    if level is None:
        env_level = os.getenv("SOLSTICE_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, env_level, logging.INFO)

    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            os.getenv("SOLSTICE_LOG_FORMAT", DEFAULT_FORMAT)
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger

