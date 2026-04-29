import numpy as np
import pandas as pd


def compute_rsi(df: pd.DataFrame, period: int = 14, verbose: bool = True) -> pd.DataFrame:
	"""Compute RSI with progress logs."""

	def log(msg: str):
		if verbose:
			print(f"[compute_rsi] {msg}")

	log("Starting RSI computation...")

	# Step 1: Convert column
	log("Converting 'close' column to numeric...")
	close = pd.to_numeric(df["close"], errors="coerce")

	log(f"Total rows: {len(df)}")

	# Step 2: Price changes
	log("Calculating price differences (delta)...")
	delta = close.diff()

	# Step 3: Gains & losses
	log("Separating gains and losses...")
	gain = delta.clip(lower=0)
	loss = -delta.clip(upper=0)

	# Step 4: Smoothed averages
	log(f"Calculating exponential moving averages (period={period})...")
	avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
	avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

	# Step 5: Relative strength
	log("Calculating relative strength (RS)...")
	rs = avg_gain / avg_loss.replace(0, np.nan)

	# Step 6: RSI
	log("Calculating RSI values...")
	rsi = 100 - (100 / (1 + rs))

	# Step 7: Output
	log("Combining results into output DataFrame...")
	out = pd.DataFrame(index=df.index)
	out[f"rsi_{period}"] = rsi

	log("Computation completed successfully ✅")

	return out