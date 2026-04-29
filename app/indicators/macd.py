import pandas as pd


def compute_macd(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
	"""Compute MACD, signal, and histogram with progress logs."""

	def log(msg: str):
		if verbose:
			print(f"[compute_macd] {msg}")

	log("Starting MACD computation...")

	# Step 1: Convert column
	log("Converting 'close' column to numeric...")
	close = pd.to_numeric(df["close"], errors="coerce")

	log(f"Total rows: {len(df)}")

	# Step 2: Compute EMAs
	log("Calculating fast EMA (12)...")
	ema_fast = close.ewm(span=12, adjust=False).mean()

	log("Calculating slow EMA (26)...")
	ema_slow = close.ewm(span=26, adjust=False).mean()

	# Step 3: MACD Line
	log("Calculating MACD line...")
	macd = ema_fast - ema_slow

	# Step 4: Signal Line
	log("Calculating Signal line (EMA 9 of MACD)...")
	signal = macd.ewm(span=9, adjust=False).mean()

	# Step 5: Histogram
	log("Calculating MACD histogram...")
	macd_hist = macd - signal

	# Step 6: Combine results
	log("Combining results into output DataFrame...")
	out = pd.DataFrame(index=df.index)
	out["macd"] = macd
	out["macd_signal"] = signal
	out["macd_hist"] = macd_hist

	log("Computation completed successfully ✅")

	return out