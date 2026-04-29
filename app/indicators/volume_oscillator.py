import numpy as np
import pandas as pd


def compute_volume_indicators(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
	"""Compute volume-based indicators with progress logs."""

	def log(msg: str):
		if verbose:
			print(f"[compute_volume_indicators] {msg}")

	log("Starting volume indicators computation...")

	# Step 1: Convert columns
	log("Converting 'close', 'high', 'low', 'volume' columns to numeric...")
	close = pd.to_numeric(df["close"], errors="coerce")
	high = pd.to_numeric(df["high"], errors="coerce")
	low = pd.to_numeric(df["low"], errors="coerce")
	volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

	log(f"Total rows: {len(df)}")

	# Step 2: OBV
	log("Calculating On-Balance Volume (OBV)...")
	direction = close.diff().fillna(0)
	signed_volume = direction.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)) * volume
	obv = signed_volume.cumsum()

	# Step 3: VWAP
	log("Calculating VWAP...")
	typical_price = (high + low + close) / 3
	cum_v = volume.cumsum().replace(0, np.nan)
	vwap = (typical_price * volume).cumsum() / cum_v

	# Step 4: MFI
	log("Calculating Money Flow Index (MFI 14)...")
	raw_money_flow = typical_price * volume
	tp_delta = typical_price.diff()

	pos_flow = raw_money_flow.where(tp_delta > 0, 0.0)
	neg_flow = raw_money_flow.where(tp_delta < 0, 0.0).abs()

	pos_mf_14 = pos_flow.rolling(window=14, min_periods=1).sum()
	neg_mf_14 = neg_flow.rolling(window=14, min_periods=1).sum().replace(0, np.nan)

	mfi = 100 - (100 / (1 + (pos_mf_14 / neg_mf_14)))

	# Step 5: Accumulation/Distribution Line
	log("Calculating Accumulation/Distribution Line...")
	mfm_den = (high - low).replace(0, np.nan)
	mfm = ((close - low) - (high - close)) / mfm_den
	ad_line = (mfm.fillna(0.0) * volume).cumsum()

	# Step 6: Volume Oscillator
	log("Calculating Volume Oscillator (EMA 14 vs 28)...")
	vol_short = volume.ewm(span=14, adjust=False).mean()
	vol_long = volume.ewm(span=28, adjust=False).mean().replace(0, np.nan)

	volume_osc = ((vol_short - vol_long) / vol_long) * 100

	# Step 7: Combine results
	log("Combining results into output DataFrame...")
	out = pd.DataFrame(index=df.index)
	out["obv"] = obv
	out["vwap"] = vwap
	out["mfi_14"] = mfi
	out["ad_line"] = ad_line
	out["volume_oscillator"] = volume_osc

	log("Computation completed successfully ✅")

	return out