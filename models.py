from __future__ import annotations

from sqlalchemy import Column, String, TIMESTAMP, text, Text, Integer, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
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


# --- Users & Roles with per-department access ---
class Role(Base):
    __tablename__ = "roles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)
    description = Column(String(255), nullable=True)
    # Relations
    user_roles = relationship("UserRole", back_populates="role", cascade="all, delete-orphan")
    users = relationship("User", secondary="user_roles", back_populates="roles")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(80), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, nullable=False, server_default=text("1"))
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )
    # Relations
    user_roles = relationship("UserRole", back_populates="user", cascade="all, delete-orphan")
    roles = relationship("Role", secondary="user_roles", back_populates="users")
    department_links = relationship("UserDepartment", back_populates="user", cascade="all, delete-orphan")


class UserRole(Base):
    __tablename__ = "user_roles"
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    role_id = Column(Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True)
    # Relations
    user = relationship("User", back_populates="user_roles")
    role = relationship("Role", back_populates="user_roles")


class UserDepartment(Base):
    __tablename__ = "user_departments"
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    department = Column(String(64), primary_key=True)
    can_write = Column(Boolean, nullable=False, server_default=text("1"))
    __table_args__ = (
        UniqueConstraint('user_id', 'department', name='uq_user_dept'),
    )
    # Relations
    user = relationship("User", back_populates="department_links")


def init_db(engine) -> None:
    """Create tables for all ORM models if they do not exist."""
    Base.metadata.create_all(bind=engine)
