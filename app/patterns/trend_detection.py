import numpy as np
import pandas as pd


def compute_trend_features(df: pd.DataFrame) -> pd.DataFrame:
	"""Compute trend, trend category, support, and resistance.

	Rules:
	- trend_pct_change: signed candle return in percent from open to close
	- trend: up if trend_pct_change > 0, down if trend_pct_change < 0, sideaway otherwise
	- trend_category: based on ADX bands for non-sideaway rows
	  - ADX > 25 => strong trend
	  - 20 <= ADX <= 25 => moderate trend
	  - ADX < 20 => temporary trend
	- support/resistance: rolling 20-candle min(low)/max(high)
	"""
	open_p = pd.to_numeric(df["open"], errors="coerce")
	close = pd.to_numeric(df["close"], errors="coerce")
	high = pd.to_numeric(df["high"], errors="coerce")
	low = pd.to_numeric(df["low"], errors="coerce")
	trend_pct_change = ((close - open_p) / open_p.replace(0, np.nan)) * 100.0

	ema_12 = pd.to_numeric(df.get("ema_12"), errors="coerce")
	ema_26 = pd.to_numeric(df.get("ema_26"), errors="coerce")
	adx_14 = pd.to_numeric(df.get("adx_14"), errors="coerce")

	support = low.rolling(window=20, min_periods=1).min()
	resistance = high.rolling(window=20, min_periods=1).max()

	base_trend = np.where(trend_pct_change > 0, "up", np.where(trend_pct_change < 0, "down", "sideaway"))

	# Optional EMA confirmation: if candle direction conflicts strongly with EMA direction,
	# downgrade to sideaway rather than forcing reversal.
	ema_slope = ema_12.diff()
	ema_up = (ema_12 > ema_26) & (ema_slope > 0)
	ema_down = (ema_12 < ema_26) & (ema_slope < 0)

	trend = np.where(
		(base_trend == "up") & (~ema_up.fillna(True)),
		"sideaway",
		np.where((base_trend == "down") & (~ema_down.fillna(True)), "sideaway", base_trend),
	)

	strong = adx_14 > 25
	moderate = (adx_14 >= 20) & (adx_14 <= 25)

	trend_category = np.where(
		trend == "up",
		np.where(strong, "strong up", np.where(moderate, "moderate up", "temporary up")),
		np.where(
			trend == "down",
			np.where(strong, "strong down", np.where(moderate, "moderate down", "temporary down")),
			"sideaway",
		),
	)

	out = pd.DataFrame(index=df.index)
	out["trend_pct_change"] = trend_pct_change
	out["trend"] = trend
	out["trend_category"] = trend_category
	out["support"] = support
	out["resistance"] = resistance

	return out
