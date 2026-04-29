import numpy as np
import pandas as pd


def compute_stochastic_cci(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
	"""Compute Stochastic (%K/%D) and CCI with progress logs."""

	def log(msg: str):
		if verbose:
			print(f"[compute_stochastic_cci] {msg}")

	log("Starting Stochastic + CCI computation...")

	# Step 1: Convert columns
	log("Converting 'close', 'high', 'low' columns to numeric...")
	close = pd.to_numeric(df["close"], errors="coerce")
	high = pd.to_numeric(df["high"], errors="coerce")
	low = pd.to_numeric(df["low"], errors="coerce")

	log(f"Total rows: {len(df)}")

	# Step 2: Stochastic %K
	log("Calculating Stochastic %K (14 period)...")
	lowest_low = low.rolling(window=14, min_periods=1).min()
	highest_high = high.rolling(window=14, min_periods=1).max()

	stoch_den = (highest_high - lowest_low).replace(0, np.nan)
	stoch_k = ((close - lowest_low) / stoch_den) * 100

	# Step 3: Stochastic %D
	log("Calculating Stochastic %D (3 period SMA of %K)...")
	stoch_d = stoch_k.rolling(window=3, min_periods=1).mean()

	# Step 4: Typical Price
	log("Calculating Typical Price...")
	typical_price = (high + low + close) / 3

	# Step 5: CCI
	log("Calculating CCI (20 period)...")
	sma_tp = typical_price.rolling(window=20, min_periods=1).mean()
	mad = (typical_price - sma_tp).abs().rolling(window=20, min_periods=1).mean()

	cci = (typical_price - sma_tp) / (0.015 * mad.replace(0, np.nan))

	# Step 6: Combine results
	log("Combining results into output DataFrame...")
	out = pd.DataFrame(index=df.index)
	out["stoch_k_14"] = stoch_k
	out["stoch_d_3"] = stoch_d
	out["cci_20"] = cci

	log("Computation completed successfully ✅")

	return out