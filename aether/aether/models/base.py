"""Declarative base for ORM models."""

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class BaseModel(DeclarativeBase):
    """Base class for SQLAlchemy models using dataclass integration."""

    metadata = MetaData()
    repr_cols_num = 3

    def __repr__(self) -> str:  # pragma: no cover - repr utility
        attrs = ", ".join(
            f"{key}={value!r}" for key, value in self.__dict__.items() if not key.startswith("_")
        )
        return f"{self.__class__.__name__}({attrs})"
