from datetime import datetime
from typing import Any, Dict, Optional

import sqlalchemy
from sqlalchemy import JSON, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.sqltypes import Boolean, DateTime, String

meta = sqlalchemy.MetaData(schema=None)


class Base(DeclarativeBase):
    """Base class for all DB models."""

    metadata = meta


class Results(Base):
    __tablename__ = "results"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    identifier: Mapped[str] = mapped_column(String(), nullable=False)
    data_description: Mapped[Dict[str, Any]] = mapped_column(
        type_=JSON, default={}, nullable=False
    )
    data: Mapped[str] = mapped_column(String(), nullable=True)
    archived: Mapped[bool] = mapped_column(Boolean(), default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("identifier", name="unique constraint for identifier"),
        # Index("")
    )

    @property
    def title(self) -> Optional[str]:
        return self.metadata.get("title", None)
