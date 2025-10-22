from __future__ import annotations

from sqlalchemy import Column, String, TIMESTAMP, text, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class _RowBase:
    row_hash = Column(String(32), primary_key=True)
    data = Column(Text, nullable=False)
    ingested_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )


class Competence(Base, _RowBase):
    __tablename__ = "competence"


class Pointage(Base, _RowBase):
    __tablename__ = "pointage"


class Priorite(Base, _RowBase):
    __tablename__ = "priorite"


class Prog(Base, _RowBase):
    __tablename__ = "prog"


class TachesLignes(Base, _RowBase):
    __tablename__ = "tacheslignes"


class TachesSepare(Base, _RowBase):
    __tablename__ = "tachessepare"


def init_db(engine) -> None:
    """Create tables for all ORM models if they do not exist."""
    Base.metadata.create_all(bind=engine)
