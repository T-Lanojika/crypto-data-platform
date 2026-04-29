from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd
from sqlalchemy import text

from app.config.database import engine
from app.services.gap_detection_service import INTERVAL_TO_MS


@dataclass
class SyntheticCandle:
	symbol: str
	interval: str
	open_time_ms: int
	open_time: datetime
	close_time_ms: int
	close_time: datetime
	open: float
	high: float
	low: float
	close: float
	volume: float
	quote_asset_volume: float
	number_of_trades: int
	taker_buy_base_asset_volume: float
	taker_buy_quote_asset_volume: float
	ignore_value: float
	is_synthetic: bool


class GapFillerService:
	"""Fills gaps using multi-timeframe backfill and synthetic candle generation."""

	@staticmethod
	def fetch_larger_timeframe_data(
		symbol: str, larger_interval: str, table_name: str, gap_start_ms: int, gap_end_ms: int
	) -> pd.DataFrame:
		query = text(
			f"""
			SELECT
				open_time_ms, close_time_ms, open, high, low, close, volume,
				quote_asset_volume, number_of_trades,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
				ignore_value, open_time, close_time
			FROM {table_name}
			WHERE open_time_ms >= :gap_start_ms AND close_time_ms <= :gap_end_ms
			ORDER BY open_time_ms
			"""
		)

		with engine.begin() as connection:
			rows = connection.execute(
				query, {"gap_start_ms": gap_start_ms, "gap_end_ms": gap_end_ms}
			).mappings().all()

		if not rows:
			return pd.DataFrame()

		data = []
		for row in rows:
			data.append(dict(row))

		return pd.DataFrame(data)

	@staticmethod
	def split_candle_into_smaller_intervals(
		symbol: str,
		larger_candle: dict,
		smaller_interval: str,
		larger_interval: str,
		ratio: int,
	) -> list[SyntheticCandle]:
		interval_ms = INTERVAL_TO_MS.get(smaller_interval, 0)
		if interval_ms == 0 or ratio == 0:
			return []

		open_price = float(larger_candle["open"])
		close_price = float(larger_candle["close"])
		high_price = float(larger_candle["high"])
		low_price = float(larger_candle["low"])
		base_volume = float(larger_candle["volume"]) / ratio
		base_quote_volume = float(larger_candle["quote_asset_volume"]) / ratio
		base_trades = max(1, int(larger_candle["number_of_trades"]) // ratio)
		base_taker_buy_base = float(larger_candle["taker_buy_base_asset_volume"]) / ratio
		base_taker_buy_quote = float(larger_candle["taker_buy_quote_asset_volume"]) / ratio

		synthetic_candles: list[SyntheticCandle] = []
		open_time_ms = int(larger_candle["open_time_ms"])

		for i in range(ratio):
			candle_open_ms = open_time_ms + (i * interval_ms)
			candle_close_ms = candle_open_ms + interval_ms - 1

			synthetic_open = open_price if i == 0 else (open_price + (close_price - open_price) * (i / ratio))
			synthetic_close = (open_price + (close_price - open_price) * ((i + 1) / ratio))
			synthetic_high = high_price
			synthetic_low = low_price

			candle_open_time = datetime.fromtimestamp(candle_open_ms / 1000, tz=UTC)
			candle_close_time = datetime.fromtimestamp(candle_close_ms / 1000, tz=UTC)

			synthetic_candles.append(
				SyntheticCandle(
					symbol=symbol,
					interval=smaller_interval,
					open_time_ms=candle_open_ms,
					open_time=candle_open_time,
					close_time_ms=candle_close_ms,
					close_time=candle_close_time,
					open=synthetic_open,
					high=synthetic_high,
					low=synthetic_low,
					close=synthetic_close,
					volume=base_volume,
					quote_asset_volume=base_quote_volume,
					number_of_trades=base_trades,
					taker_buy_base_asset_volume=base_taker_buy_base,
					taker_buy_quote_asset_volume=base_taker_buy_quote,
					ignore_value=0.0,
					is_synthetic=True,
				)
			)

		return synthetic_candles
	@staticmethod
	def upsert_synthetic_candles(table_name: str, synthetic_candles: list[SyntheticCandle]) -> int:
		if not synthetic_candles:
			return 0

		records: list[dict] = []
		for candle in synthetic_candles:
			records.append(
				{
					"open_time": candle.open_time,
					"open": candle.open,
					"high": candle.high,
					"low": candle.low,
					"close": candle.close,
					"volume": candle.volume,
					"close_time": candle.close_time,
					"quote_asset_volume": candle.quote_asset_volume,
					"number_of_trades": candle.number_of_trades,
					"taker_buy_base_asset_volume": candle.taker_buy_base_asset_volume,
					"taker_buy_quote_asset_volume": candle.taker_buy_quote_asset_volume,
					"ignore_value": candle.ignore_value,
					"open_time_ms": candle.open_time_ms,
					"close_time_ms": candle.close_time_ms,
					"is_synthetic": candle.is_synthetic,
				}
			)

		upsert_sql = text(
			f"""
			INSERT INTO {table_name} (
				open_time, open, high, low, close, volume, close_time,
				quote_asset_volume, number_of_trades,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
				ignore_value, open_time_ms, close_time_ms, is_synthetic
			)
			VALUES (
				:open_time, :open, :high, :low, :close, :volume, :close_time,
				:quote_asset_volume, :number_of_trades,
				:taker_buy_base_asset_volume, :taker_buy_quote_asset_volume,
				:ignore_value, :open_time_ms, :close_time_ms, :is_synthetic
			)
			ON CONFLICT (open_time_ms)
			DO NOTHING
			"""
		)

		with engine.begin() as connection:
			connection.execute(upsert_sql, records)

		return len(records)