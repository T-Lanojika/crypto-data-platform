import numpy as np
import pandas as pd


def compute_turning_points(df: pd.DataFrame) -> pd.DataFrame:
	"""Compute turning points with a 2-candle lookback/lookforward pivot rule.

	Score combines:
	- pivot move magnitude into next candles (percent change)
	- relative volume weight
	Then normalizes to 0..1.
	"""
	close = pd.to_numeric(df["close"], errors="coerce")
	high = pd.to_numeric(df["high"], errors="coerce")
	low = pd.to_numeric(df["low"], errors="coerce")
	volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

	# Local pivot with 2-candle window before and after.
	prev2_high_max = pd.concat([high.shift(1), high.shift(2)], axis=1).max(axis=1)
	next2_high_max = pd.concat([high.shift(-1), high.shift(-2)], axis=1).max(axis=1)
	prev2_low_min = pd.concat([low.shift(1), low.shift(2)], axis=1).min(axis=1)
	next2_low_min = pd.concat([low.shift(-1), low.shift(-2)], axis=1).min(axis=1)

	is_local_top = (high >= prev2_high_max) & (high >= next2_high_max)
	is_local_bottom = (low <= prev2_low_min) & (low <= next2_low_min)
	is_turning_point = (is_local_top | is_local_bottom).fillna(False)

	# Measure move from pivot to near-future candles.
	future_high_2 = pd.concat([high.shift(-1), high.shift(-2)], axis=1).max(axis=1)
	future_low_2 = pd.concat([low.shift(-1), low.shift(-2)], axis=1).min(axis=1)

	top_move_pct = ((high - future_low_2).abs() / high.replace(0, np.nan)).fillna(0.0)
	bottom_move_pct = ((future_high_2 - low).abs() / low.replace(0, np.nan)).fillna(0.0)
	pivot_move_pct = np.where(is_local_top, top_move_pct, np.where(is_local_bottom, bottom_move_pct, 0.0))

	# Weight by relative volume.
	vol_baseline = volume.rolling(window=20, min_periods=1).mean().replace(0, np.nan)
	rel_volume = (volume / vol_baseline).replace([np.inf, -np.inf], np.nan).fillna(0.0)
	raw_score = pd.Series(pivot_move_pct, index=df.index) * rel_volume

	# Robust normalization to 0..1.
	active_scores = raw_score[is_turning_point]
	if active_scores.empty:
		normalized = pd.Series(0.0, index=df.index)
	else:
		scale = float(active_scores.quantile(0.95))
		if scale <= 0:
			normalized = pd.Series(0.0, index=df.index)
		else:
			normalized = (raw_score / scale).clip(lower=0.0, upper=1.0)

	out = pd.DataFrame(index=df.index)
	out["is_turning_point"] = is_turning_point
	out["turning_point_score"] = np.where(is_turning_point, normalized.fillna(0.0), 0.0)

	return out
