from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import text

from app.config.database import engine


INTERVAL_TO_MS = {
	"1m": 60_000,
	"3m": 180_000,
	"5m": 300_000,
	"15m": 900_000,
	"30m": 1_800_000,
	"1h": 3_600_000,
	"2h": 7_200_000,
	"4h": 14_400_000,
	"6h": 21_600_000,
	"8h": 28_800_000,
	"12h": 43_200_000,
	"1d": 86_400_000,
	"3d": 259_200_000,
	"1w": 604_800_000,
	"1M": 2_592_000_000,
}

TIMEFRAME_HIERARCHY = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
COLLECTED_INTERVALS = {"15m", "1h", "4h", "1d", "1w", "1M"}

def _ms_to_utc(ms: int) -> str:
	"""Convert milliseconds to UTC datetime string."""
	return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


@dataclass
class Gap:
	start_open_time_ms: int
	end_open_time_ms: int
	expected_candle_count: int
	missing_candle_count: int
	start_time_utc: str = ""
	end_time_utc: str = ""

	def __post_init__(self) -> None:
		if not self.start_time_utc:
			self.start_time_utc = _ms_to_utc(self.start_open_time_ms)
		if not self.end_time_utc:
			self.end_time_utc = _ms_to_utc(self.end_open_time_ms)


class GapDetectionService:
	"""Detects gaps and missing candles in raw data."""

	@staticmethod
	def find_gaps(table_name: str, interval: str) -> list[Gap]:
		interval_ms = INTERVAL_TO_MS.get(interval)
		if interval_ms is None:
			return []

		query = text(
			f"""
			WITH ordered AS (
				SELECT
					open_time_ms,
					LAG(open_time_ms) OVER (ORDER BY open_time_ms) AS prev_open_time_ms
				FROM {table_name}
				ORDER BY open_time_ms
			)
			SELECT
				(prev_open_time_ms + :interval_ms)::bigint AS gap_start_ms,
				open_time_ms::bigint AS gap_next_open_ms,
				((open_time_ms - (prev_open_time_ms + :interval_ms)) / :interval_ms)::bigint AS missing_count
			FROM ordered
			WHERE prev_open_time_ms IS NOT NULL
			AND open_time_ms > (prev_open_time_ms + :interval_ms)
			ORDER BY open_time_ms
			"""
		)

		gaps: list[Gap] = []
		with engine.begin() as connection:
			rows = connection.execute(query, {"interval_ms": interval_ms}).mappings().all()

		for item in rows:
			gap_start = int(item["gap_start_ms"])
			missing_count = int(item["missing_count"])
			expected_count = missing_count

			if missing_count > 0:
				gaps.append(
					Gap(
						start_open_time_ms=gap_start,
						end_open_time_ms=gap_start + (missing_count - 1) * interval_ms,
						expected_candle_count=expected_count,
						missing_candle_count=missing_count,
					)
				)

		return gaps

	@staticmethod
	def get_larger_timeframe_for_backfill(current_interval: str) -> Optional[str]:
		try:
			current_idx = TIMEFRAME_HIERARCHY.index(current_interval)
			for i in range(current_idx + 1, len(TIMEFRAME_HIERARCHY)):
				candidate = TIMEFRAME_HIERARCHY[i]
				if candidate in COLLECTED_INTERVALS:
					return candidate
		except ValueError:
			pass
		return None

	@staticmethod
	def get_ratio(smaller_interval: str, larger_interval: str) -> int:
		smaller_ms = INTERVAL_TO_MS.get(smaller_interval, 0)
		larger_ms = INTERVAL_TO_MS.get(larger_interval, 0)
		if smaller_ms > 0 and larger_ms % smaller_ms == 0:
			return larger_ms // smaller_ms
		return 0
