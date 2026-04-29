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
}


@dataclass
class TimeSeriesAuditResult:
	symbol: str
	interval: str
	table_name: str
	row_count: int
	start_open_time_ms: Optional[int]
	start_open_time_utc: Optional[str]
	end_open_time_ms: Optional[int]
	end_open_time_utc: Optional[str]
	gap_event_count: int
	estimated_missing_candles: Optional[int]
	missing_ranges_utc: list[str]
	status: str
	message: str


class PreprocessingService:
	"""Checks raw table time-series integrity before feature preprocessing."""

	@staticmethod
	def _ms_to_utc(ms: Optional[int]) -> Optional[str]:
		if ms is None:
			return None
		return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

	@staticmethod
	def _table_exists(table_name: str) -> bool:
		query = text(
			"""
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = 'public'
				AND table_name = :table_name
			)
			"""
		)
		with engine.begin() as connection:
			return bool(connection.execute(query, {"table_name": table_name}).scalar())

	@staticmethod
	def _get_basic_stats(table_name: str) -> tuple[int, Optional[int], Optional[int]]:
		query = text(
			f"""
			SELECT
				COUNT(*)::bigint AS row_count,
				MIN(open_time_ms)::bigint AS start_open_time_ms,
				MAX(open_time_ms)::bigint AS end_open_time_ms
			FROM {table_name}
			"""
		)
		with engine.begin() as connection:
			row = connection.execute(query).mappings().first()

		if row is None:
			return 0, None, None

		return int(row["row_count"] or 0), row["start_open_time_ms"], row["end_open_time_ms"]

	@staticmethod
	def _find_gap_deltas(table_name: str) -> list[int]:
		query = text(
			f"""
			WITH ordered AS (
				SELECT
					open_time_ms,
					LAG(close_time_ms) OVER (ORDER BY open_time_ms) AS prev_close_time_ms
				FROM {table_name}
			)
			SELECT
				(open_time_ms - (prev_close_time_ms + 1))::bigint AS gap_delta_ms
			FROM ordered
			WHERE prev_close_time_ms IS NOT NULL
			AND open_time_ms <> (prev_close_time_ms + 1)
			ORDER BY open_time_ms
			"""
		)

		with engine.begin() as connection:
			rows = connection.execute(query).mappings().all()

		return [int(item["gap_delta_ms"]) for item in rows if item["gap_delta_ms"] is not None]

	@staticmethod
	def _find_missing_ranges_utc(table_name: str, interval: str) -> list[str]:
		interval_ms = INTERVAL_TO_MS.get(interval)
		if interval_ms is None:
			return []

		query = text(
			f"""
			WITH ordered AS (
				SELECT
					open_time_ms,
					LAG(close_time_ms) OVER (ORDER BY open_time_ms) AS prev_close_time_ms
				FROM {table_name}
			)
			SELECT
				(prev_close_time_ms + 1)::bigint AS expected_open_time_ms,
				open_time_ms::bigint AS actual_open_time_ms
			FROM ordered
			WHERE prev_close_time_ms IS NOT NULL
			AND open_time_ms <> (prev_close_time_ms + 1)
			ORDER BY open_time_ms
			"""
		)

		with engine.begin() as connection:
			rows = connection.execute(query).mappings().all()

		missing_ranges: list[str] = []
		for item in rows:
			expected_open_ms = int(item["expected_open_time_ms"])
			actual_open_ms = int(item["actual_open_time_ms"])
			missing_end_ms = actual_open_ms - interval_ms
			if missing_end_ms < expected_open_ms:
				missing_ranges.append(
					f"{PreprocessingService._ms_to_utc(expected_open_ms)} -> "
					f"{PreprocessingService._ms_to_utc(actual_open_ms)} "
					"(gap anomaly: less than one full candle)"
				)
				continue
			missing_ranges.append(
				f"{PreprocessingService._ms_to_utc(expected_open_ms)} -> "
				f"{PreprocessingService._ms_to_utc(missing_end_ms)}"
			)

		return missing_ranges

	@staticmethod
	def audit_time_series(symbol: str, interval: str, table_name: str) -> TimeSeriesAuditResult:
		if not PreprocessingService._table_exists(table_name):
			return TimeSeriesAuditResult(
				symbol=symbol,
				interval=interval,
				table_name=table_name,
				row_count=0,
				start_open_time_ms=None,
				start_open_time_utc=None,
				end_open_time_ms=None,
				end_open_time_utc=None,
				gap_event_count=0,
				estimated_missing_candles=None,
				missing_ranges_utc=[],
				status="MISSING_TABLE",
				message="Raw table not found. Run collector first.",
			)

		row_count, start_ms, end_ms = PreprocessingService._get_basic_stats(table_name)
		if row_count == 0:
			return TimeSeriesAuditResult(
				symbol=symbol,
				interval=interval,
				table_name=table_name,
				row_count=0,
				start_open_time_ms=start_ms,
				start_open_time_utc=PreprocessingService._ms_to_utc(start_ms),
				end_open_time_ms=end_ms,
				end_open_time_utc=PreprocessingService._ms_to_utc(end_ms),
				gap_event_count=0,
				estimated_missing_candles=0,
				missing_ranges_utc=[],
				status="EMPTY",
				message="Table exists but has no rows.",
			)

		gap_deltas = PreprocessingService._find_gap_deltas(table_name)
		missing_ranges_utc = PreprocessingService._find_missing_ranges_utc(table_name, interval)
		gap_event_count = len(gap_deltas)
		interval_ms = INTERVAL_TO_MS.get(interval)

		estimated_missing: Optional[int] = None
		if interval_ms is not None:
			estimated_missing = sum(delta // interval_ms for delta in gap_deltas if delta > 0)

		if gap_event_count == 0:
			return TimeSeriesAuditResult(
				symbol=symbol,
				interval=interval,
				table_name=table_name,
				row_count=row_count,
				start_open_time_ms=start_ms,
				start_open_time_utc=PreprocessingService._ms_to_utc(start_ms),
				end_open_time_ms=end_ms,
				end_open_time_utc=PreprocessingService._ms_to_utc(end_ms),
				gap_event_count=0,
				estimated_missing_candles=0,
				missing_ranges_utc=[],
				status="OK",
				message="Time series continuity check passed.",
			)

		missing_info = (
			f"estimated_missing_candles={estimated_missing}"
			if estimated_missing is not None
			else "estimated_missing_candles=unknown(interval has variable length)"
		)
		return TimeSeriesAuditResult(
			symbol=symbol,
			interval=interval,
			table_name=table_name,
			row_count=row_count,
			start_open_time_ms=start_ms,
			start_open_time_utc=PreprocessingService._ms_to_utc(start_ms),
			end_open_time_ms=end_ms,
			end_open_time_utc=PreprocessingService._ms_to_utc(end_ms),
			gap_event_count=gap_event_count,
			estimated_missing_candles=estimated_missing,
			missing_ranges_utc=missing_ranges_utc,
			status="ISSUES",
			message=f"Detected {gap_event_count} gap event(s), {missing_info}.",
		)
