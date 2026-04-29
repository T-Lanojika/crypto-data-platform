import numpy as np
import pandas as pd


def compute_volatility(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
	"""Compute Bollinger bands, ATR, ADX, and VIX proxy with progress logs."""

	def log(msg: str):
		if verbose:
			print(f"[compute_volatility] {msg}")

	log("Starting volatility computation...")

	# Step 1: Convert columns
	log("Converting columns to numeric...")
	close = pd.to_numeric(df["close"], errors="coerce")
	high = pd.to_numeric(df["high"], errors="coerce")
	low = pd.to_numeric(df["low"], errors="coerce")

	log(f"Total rows: {len(df)}")

	# Step 2: Bollinger Bands
	log("Calculating Bollinger Bands (20 period)...")
	ma20 = close.rolling(window=20, min_periods=1).mean()
	std20 = close.rolling(window=20, min_periods=1).std(ddof=0)

	# Step 3: ATR
	log("Calculating ATR (14 period)...")
	prev_close = close.shift(1)
	tr = pd.concat(
		[
			(high - low).abs(),
			(high - prev_close).abs(),
			(low - prev_close).abs(),
		],
		axis=1,
	).max(axis=1)

	atr = tr.ewm(alpha=1 / 14, adjust=False).mean()

	# Step 4: ADX
	log("Calculating ADX (14 period)...")
	plus_dm = high.diff()
	minus_dm = -low.diff()

	plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
	minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

	tr_smooth = tr.ewm(alpha=1 / 14, adjust=False).mean().replace(0, np.nan)

	plus_di = 100 * (plus_dm.ewm(alpha=1 / 14, adjust=False).mean() / tr_smooth)
	minus_di = 100 * (minus_dm.ewm(alpha=1 / 14, adjust=False).mean() / tr_smooth)

	di_sum = (plus_di + minus_di).replace(0, np.nan)
	dx = ((plus_di - minus_di).abs() / di_sum) * 100
	adx = dx.ewm(alpha=1 / 14, adjust=False).mean()

	# Step 5: VIX Proxy
	log("Calculating VIX proxy (30 period rolling std)...")
	ratio = (close / close.shift(1)).where((close / close.shift(1)) > 0)
	log_ret = np.log(ratio)

	vix_proxy = (
		log_ret.rolling(window=30, min_periods=10)
		.std(ddof=0) * (365**0.5) * 100
	)

	# Step 6: Combine results
	log("Combining results into output DataFrame...")
	out = pd.DataFrame(index=df.index)
	out["bollinger_mid_20"] = ma20
	out["bollinger_upper_20"] = ma20 + (2 * std20)
	out["bollinger_lower_20"] = ma20 - (2 * std20)
	out["atr_14"] = atr
	out["adx_14"] = adx
	out["vix"] = vix_proxy

	log("Computation completed successfully ✅")

	return out