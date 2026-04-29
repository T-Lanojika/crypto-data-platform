from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from app.config.settings import BINANCE_BASE_URL


class BinanceService:
	"""Service for fetching candlestick data from Binance public API."""

	BASE_URL = f"{BINANCE_BASE_URL.rstrip('/')}" + "/api/v3/klines"

	@staticmethod
	def fetch_klines(
		symbol: str,
		interval: str,
		limit: int = 500,
		start_time_ms: Optional[int] = None,
		end_time_ms: Optional[int] = None,
	) -> pd.DataFrame:
		params = {
			"symbol": symbol.upper(),
			"interval": interval,
			"limit": limit,
		}

		if start_time_ms is not None:
			params["startTime"] = start_time_ms
		if end_time_ms is not None:
			params["endTime"] = end_time_ms

		response = requests.get(BinanceService.BASE_URL, params=params, timeout=20)
		response.raise_for_status()
		rows = response.json()

		columns = [
			"open_time_ms",
			"open",
			"high",
			"low",
			"close",
			"volume",
			"close_time_ms",
			"quote_asset_volume",
			"number_of_trades",
			"taker_buy_base_asset_volume",
			"taker_buy_quote_asset_volume",
			"ignore_value",
		]

		df = pd.DataFrame(rows, columns=columns)
		if df.empty:
			return df

		numeric_cols = [
			"open",
			"high",
			"low",
			"close",
			"volume",
			"quote_asset_volume",
			"taker_buy_base_asset_volume",
			"taker_buy_quote_asset_volume",
			"ignore_value",
		]

		for col in numeric_cols:
			df[col] = pd.to_numeric(df[col], errors="coerce")

		df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce").fillna(0).astype(int)
		df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce").fillna(0).astype("int64")
		df["close_time_ms"] = pd.to_numeric(df["close_time_ms"], errors="coerce").fillna(0).astype("int64")

		df["open_time"] = df["open_time_ms"].apply(
			lambda x: datetime.utcfromtimestamp(x / 1000)
		)
		df["close_time"] = df["close_time_ms"].apply(
			lambda x: datetime.utcfromtimestamp(x / 1000)
		)

		# STRICT REQUIREMENT: Enforce ascending order by open_time_ms
		# This ensures all downstream calculations (EMA, RSI, MACD, patterns) work correctly
		df = df.sort_values("open_time_ms", ascending=True).reset_index(drop=True)

		return df
