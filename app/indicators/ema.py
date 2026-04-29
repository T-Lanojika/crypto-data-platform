import pandas as pd


def compute_ma_ema(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
	"""Compute MA/EMA trend basics with progress logs."""

	def log(msg: str):
		if verbose:
			print(f"[compute_ma_ema] {msg}")

	log("Starting MA/EMA computation...")

	# Step 1: Convert column
	log("Converting 'close' column to numeric...")
	close = pd.to_numeric(df["close"], errors="coerce")

	log(f"Total rows: {len(df)}")

	# Step 2: Moving Average
	log("Calculating Moving Average (MA 20)...")
	ma_20 = close.rolling(window=20, min_periods=1).mean()

	# Step 3: Exponential Moving Averages
	log("Calculating Exponential Moving Average (EMA 12)...")
	ema_12 = close.ewm(span=12, adjust=False).mean()

	log("Calculating Exponential Moving Average (EMA 26)...")
	ema_26 = close.ewm(span=26, adjust=False).mean()

	# Step 4: Combine results
	log("Combining results into output DataFrame...")
	out = pd.DataFrame(index=df.index)
	out["ma_20"] = ma_20
	out["ema_12"] = ema_12
	out["ema_26"] = ema_26

	log("Computation completed successfully ✅")

	return out