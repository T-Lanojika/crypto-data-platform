from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base


class Candlestick(Base):
    """Raw OHLCV candlestick row collected from Binance."""

    __tablename__ = "candlesticks"
    __table_args__ = (
        UniqueConstraint("symbol", "interval", "open_time", name="uq_candlestick_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(10), nullable=False, index=True)

    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)

    close_time: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    quote_asset_volume: Mapped[float] = mapped_column(Float, nullable=False)
    number_of_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    taker_buy_base_asset_volume: Mapped[float] = mapped_column(Float, nullable=False)
    taker_buy_quote_asset_volume: Mapped[float] = mapped_column(Float, nullable=False)
    ignore_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    open_time_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    close_time_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), default=datetime.utcnow, nullable=False
    )