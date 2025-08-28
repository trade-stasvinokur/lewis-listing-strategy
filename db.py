# db.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

# Файл БД создастся в корне проекта
DATABASE_URL = "sqlite:///lewis.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class CoinEvent(Base):
    __tablename__ = "coins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    coin_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    coin_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    coin_symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    coin_fullname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    event_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    event_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("coin_id", "event_name", "event_date", name="uq_coin_event"),
    )


def init_db() -> None:
    """Создаёт файл lewis.db и таблицу coins (если их ещё нет)."""
    Base.metadata.create_all(engine)


def get_session():
    return SessionLocal()