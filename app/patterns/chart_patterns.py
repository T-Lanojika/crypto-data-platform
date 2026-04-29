import json
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class ChartPatternEvent:
	pattern: str
	start_idx: int
	end_idx: int
	direction: str
	confidence: float
	key_points: dict


def _linear_slope(series: pd.Series) -> float:
	vals = pd.to_numeric(series, errors="coerce").dropna()
	if len(vals) < 2:
		return 0.0
	x = np.arange(len(vals), dtype=float)
	slope, _ = np.polyfit(x, vals.values.astype(float), 1)
	return float(slope)


def _find_swings(
	close_smooth: pd.Series,
	min_distance: int,
	prominence_ratio: float,
) -> list[tuple[int, str, float]]:
	values = pd.to_numeric(close_smooth, errors="coerce").values
	n = len(values)
	if n < 5:
		return []

	baseline = float(np.nanmedian(np.abs(values))) if np.isfinite(np.nanmedian(np.abs(values))) else 1.0
	baseline = max(1e-9, baseline)
	prominence = baseline * prominence_ratio

	peaks: list[tuple[int, str, float]] = []
	troughs: list[tuple[int, str, float]] = []

	last_peak_idx = -10_000
	last_trough_idx = -10_000

	for i in range(2, n - 2):
		c = values[i]
		if not np.isfinite(c):
			continue

		left = values[i - 2 : i]
		right = values[i + 1 : i + 3]
		if np.any(~np.isfinite(left)) or np.any(~np.isfinite(right)):
			continue

		if c > left.max() and c > right.max():
			local_prom = c - max(left.min(), right.min())
			if local_prom >= prominence and (i - last_peak_idx) >= min_distance:
				peaks.append((i, "peak", float(c)))
				last_peak_idx = i

		if c < left.min() and c < right.min():
			local_prom = min(left.max(), right.max()) - c
			if local_prom >= prominence and (i - last_trough_idx) >= min_distance:
				troughs.append((i, "trough", float(c)))
				last_trough_idx = i

	swings = peaks + troughs
	swings.sort(key=lambda x: x[0])
	return swings


def _similar(a: float, b: float, tol: float) -> bool:
	denom = max(1e-9, (abs(a) + abs(b)) / 2.0)
	return abs(a - b) / denom <= tol


def _confidence(base: float, *penalties: float) -> float:
	score = base
	for p in penalties:
		score -= p
	return float(max(0.0, min(1.0, score)))


def _detect_peak_trough_patterns(swings: list[tuple[int, str, float]], tol: float) -> list[ChartPatternEvent]:
	events: list[ChartPatternEvent] = []

	for i in range(len(swings) - 2):
		a = swings[i]
		b = swings[i + 1]
		c = swings[i + 2]

		# Double Top: peak-trough-peak
		if a[1] == "peak" and b[1] == "trough" and c[1] == "peak":
			if _similar(a[2], c[2], tol) and b[2] < min(a[2], c[2]) * (1 - tol / 2):
				penalty = min(0.3, abs(a[2] - c[2]) / max(1e-9, (a[2] + c[2]) / 2))
				events.append(
					ChartPatternEvent(
						pattern="Double Top",
						start_idx=a[0],
						end_idx=c[0],
						direction="bearish",
						confidence=_confidence(0.9, penalty),
						key_points={
							"peak1": {"idx": a[0], "price": a[2]},
							"valley": {"idx": b[0], "price": b[2]},
							"peak2": {"idx": c[0], "price": c[2]},
						},
					)
				)

		# Double Bottom: trough-peak-trough
		if a[1] == "trough" and b[1] == "peak" and c[1] == "trough":
			if _similar(a[2], c[2], tol) and b[2] > max(a[2], c[2]) * (1 + tol / 2):
				penalty = min(0.3, abs(a[2] - c[2]) / max(1e-9, (a[2] + c[2]) / 2))
				events.append(
					ChartPatternEvent(
						pattern="Double Bottom",
						start_idx=a[0],
						end_idx=c[0],
						direction="bullish",
						confidence=_confidence(0.9, penalty),
						key_points={
							"trough1": {"idx": a[0], "price": a[2]},
							"peak": {"idx": b[0], "price": b[2]},
							"trough2": {"idx": c[0], "price": c[2]},
						},
					)
				)

	for i in range(len(swings) - 4):
		a, b, c, d, e = swings[i : i + 5]

		# Triple Top: peak-trough-peak-trough-peak
		if a[1] == "peak" and b[1] == "trough" and c[1] == "peak" and d[1] == "trough" and e[1] == "peak":
			if _similar(a[2], c[2], tol) and _similar(c[2], e[2], tol):
				events.append(
					ChartPatternEvent(
						pattern="Triple Top",
						start_idx=a[0],
						end_idx=e[0],
						direction="bearish",
						confidence=_confidence(0.88),
						key_points={
							"peak1": {"idx": a[0], "price": a[2]},
							"peak2": {"idx": c[0], "price": c[2]},
							"peak3": {"idx": e[0], "price": e[2]},
						},
					)
				)

		# Triple Bottom: trough-peak-trough-peak-trough
		if a[1] == "trough" and b[1] == "peak" and c[1] == "trough" and d[1] == "peak" and e[1] == "trough":
			if _similar(a[2], c[2], tol) and _similar(c[2], e[2], tol):
				events.append(
					ChartPatternEvent(
						pattern="Triple Bottom",
						start_idx=a[0],
						end_idx=e[0],
						direction="bullish",
						confidence=_confidence(0.88),
						key_points={
							"trough1": {"idx": a[0], "price": a[2]},
							"trough2": {"idx": c[0], "price": c[2]},
							"trough3": {"idx": e[0], "price": e[2]},
						},
					)
				)

		# Head & Shoulders: peak-trough-peak-trough-peak, middle peak highest, shoulders similar
		if a[1] == "peak" and b[1] == "trough" and c[1] == "peak" and d[1] == "trough" and e[1] == "peak":
			if c[2] > a[2] and c[2] > e[2] and _similar(a[2], e[2], tol * 1.2):
				neckline_slope = (d[2] - b[2]) / max(1, (d[0] - b[0]))
				events.append(
					ChartPatternEvent(
						pattern="Head & Shoulders",
						start_idx=a[0],
						end_idx=e[0],
						direction="bearish",
						confidence=_confidence(0.92, abs(neckline_slope) * 0.02),
						key_points={
							"left_shoulder": {"idx": a[0], "price": a[2]},
							"head": {"idx": c[0], "price": c[2]},
							"right_shoulder": {"idx": e[0], "price": e[2]},
							"neckline": {
								"p1": {"idx": b[0], "price": b[2]},
								"p2": {"idx": d[0], "price": d[2]},
								"slope": neckline_slope,
							},
						},
					)
				)

		# Inverse H&S: trough-peak-trough-peak-trough, middle trough lowest
		if a[1] == "trough" and b[1] == "peak" and c[1] == "trough" and d[1] == "peak" and e[1] == "trough":
			if c[2] < a[2] and c[2] < e[2] and _similar(a[2], e[2], tol * 1.2):
				neckline_slope = (d[2] - b[2]) / max(1, (d[0] - b[0]))
				events.append(
					ChartPatternEvent(
						pattern="Inverse Head & Shoulders",
						start_idx=a[0],
						end_idx=e[0],
						direction="bullish",
						confidence=_confidence(0.92, abs(neckline_slope) * 0.02),
						key_points={
							"left_shoulder": {"idx": a[0], "price": a[2]},
							"head": {"idx": c[0], "price": c[2]},
							"right_shoulder": {"idx": e[0], "price": e[2]},
							"neckline": {
								"p1": {"idx": b[0], "price": b[2]},
								"p2": {"idx": d[0], "price": d[2]},
								"slope": neckline_slope,
							},
						},
					)
				)

	return events


def _detect_window_patterns(df: pd.DataFrame, win: int = 40) -> list[ChartPatternEvent]:
	events: list[ChartPatternEvent] = []
	if len(df) < win:
		return events

	highs = pd.to_numeric(df["high"], errors="coerce")
	lows = pd.to_numeric(df["low"], errors="coerce")
	close = pd.to_numeric(df["close"], errors="coerce")

	for end in range(win, len(df) + 1):
		start = end - win
		w_high = highs.iloc[start:end]
		w_low = lows.iloc[start:end]
		w_close = close.iloc[start:end]

		high_slope = _linear_slope(w_high)
		low_slope = _linear_slope(w_low)
		close_slope = _linear_slope(w_close)

		hi_mean = float(w_high.mean()) if w_high.notna().any() else 1.0
		lo_mean = float(w_low.mean()) if w_low.notna().any() else 1.0
		hi_mean = max(1e-9, hi_mean)
		lo_mean = max(1e-9, lo_mean)

		high_flat = abs(high_slope / hi_mean) < 0.0005
		low_flat = abs(low_slope / lo_mean) < 0.0005
		high_up = high_slope > 0
		high_down = high_slope < 0
		low_up = low_slope > 0
		low_down = low_slope < 0

		# Ascending / Descending / Symmetrical triangles.
		if high_flat and low_up:
			events.append(
				ChartPatternEvent(
					pattern="Ascending Triangle",
					start_idx=start,
					end_idx=end - 1,
					direction="bullish",
					confidence=0.8,
					key_points={"upper_slope": high_slope, "lower_slope": low_slope},
				)
			)
		elif low_flat and high_down:
			events.append(
				ChartPatternEvent(
					pattern="Descending Triangle",
					start_idx=start,
					end_idx=end - 1,
					direction="bearish",
					confidence=0.8,
					key_points={"upper_slope": high_slope, "lower_slope": low_slope},
				)
			)
		elif high_down and low_up:
			events.append(
				ChartPatternEvent(
					pattern="Symmetrical Triangle",
					start_idx=start,
					end_idx=end - 1,
					direction="neutral",
					confidence=0.76,
					key_points={"upper_slope": high_slope, "lower_slope": low_slope},
				)
			)

		# Flag/Pennant approximation using prior sharp trend + short consolidation.
		prior_len = min(12, win // 2)
		prior_start = max(0, start - prior_len)
		prior_close = close.iloc[prior_start:start]
		prior_slope = _linear_slope(prior_close)
		strong_prior = abs(prior_slope) / max(1e-9, float(prior_close.mean() if len(prior_close) else 1.0)) > 0.0018

		# Flag: channel-like consolidation opposite prior trend.
		if strong_prior and ((prior_slope > 0 and close_slope < 0) or (prior_slope < 0 and close_slope > 0)):
			if (high_up and low_up) or (high_down and low_down):
				events.append(
					ChartPatternEvent(
						pattern="Flag",
						start_idx=start,
						end_idx=end - 1,
						direction="bullish" if prior_slope > 0 else "bearish",
						confidence=0.72,
						key_points={"prior_slope": prior_slope, "channel_slope": close_slope},
					)
				)

		# Pennant: strong prior move + converging lines in consolidation.
		if strong_prior and high_down and low_up:
			events.append(
				ChartPatternEvent(
					pattern="Pennant",
					start_idx=start,
					end_idx=end - 1,
					direction="bullish" if prior_slope > 0 else "bearish",
					confidence=0.75,
					key_points={"prior_slope": prior_slope, "upper_slope": high_slope, "lower_slope": low_slope},
				)
			)

	return events


def compute_chart_patterns(
	df: pd.DataFrame,
	smooth_window: int = 3,
	min_peak_distance: int = 3,
	prominence_ratio: float = 0.002,
	similarity_tolerance: float = 0.015,
) -> pd.DataFrame:
	"""Detect geometric chart patterns and map them into row-level annotations.

	Returns columns:
	- chart_pattern_type
	- chart_pattern_confidence
	- chart_pattern_direction
	- chart_pattern_start_time_ms
	- chart_pattern_end_time_ms
	- chart_pattern_key_points
	"""
	if df.empty:
		return pd.DataFrame(
			columns=[
				"chart_pattern_type",
				"chart_pattern_confidence",
				"chart_pattern_direction",
				"chart_pattern_start_time_ms",
				"chart_pattern_end_time_ms",
				"chart_pattern_key_points",
			]
		)

	work = df.copy().sort_values("open_time_ms").reset_index(drop=True)
	close = pd.to_numeric(work["close"], errors="coerce")
	close_smooth = close.rolling(window=max(1, smooth_window), min_periods=1).mean()

	swings = _find_swings(
		close_smooth=close_smooth,
		min_distance=max(1, int(min_peak_distance)),
		prominence_ratio=max(1e-6, float(prominence_ratio)),
	)

	events = _detect_peak_trough_patterns(swings=swings, tol=max(1e-6, similarity_tolerance))
	events.extend(_detect_window_patterns(work, win=40))

	n = len(work)
	best_conf = np.zeros(n, dtype=float)
	pattern_type = np.array([None] * n, dtype=object)
	pattern_dir = np.array([None] * n, dtype=object)
	start_ms = np.array([None] * n, dtype=object)
	end_ms = np.array([None] * n, dtype=object)
	key_points = np.array([None] * n, dtype=object)

	for event in events:
		s = max(0, int(event.start_idx))
		e = min(n - 1, int(event.end_idx))
		if e < s:
			continue

		s_ms = int(work.loc[s, "open_time_ms"])
		e_ms = int(work.loc[e, "open_time_ms"])
		payload = json.dumps(event.key_points, separators=(",", ":"))

		for idx in range(s, e + 1):
			if event.confidence >= best_conf[idx]:
				best_conf[idx] = event.confidence
				pattern_type[idx] = event.pattern
				pattern_dir[idx] = event.direction
				start_ms[idx] = s_ms
				end_ms[idx] = e_ms
				key_points[idx] = payload

	out = pd.DataFrame(index=work.index)
	out["chart_pattern_type"] = pattern_type
	out["chart_pattern_confidence"] = np.where(best_conf > 0, best_conf, np.nan)
	out["chart_pattern_direction"] = pattern_dir
	out["chart_pattern_start_time_ms"] = start_ms
	out["chart_pattern_end_time_ms"] = end_ms
	out["chart_pattern_key_points"] = key_points

	# Return aligned to original order of df.
	out.index = df.sort_values("open_time_ms").index
	out = out.reindex(df.index)
	return out
