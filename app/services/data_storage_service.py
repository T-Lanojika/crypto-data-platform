from typing import Optional

import pandas as pd
from sqlalchemy import text

from app.config.database import engine


class DataStorageService:
	"""Handles persistence of raw collected market data into dynamic tables."""

	@staticmethod
	def table_exists(table_name: str) -> bool:
		query = text(
			"""
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = :table_name
			)
			"""
		)
		with engine.begin() as connection:
			return bool(connection.execute(query, {"table_name": table_name}).scalar())

	@staticmethod
	def _normalize_interval_for_table(interval: str) -> str:
		if interval.endswith("M"):
			interval = f"{interval[:-1]}mo"
		return "".join(ch for ch in interval.lower() if ch.isalnum())

	@staticmethod
	def build_raw_table_name(symbol: str, interval: str) -> str:
		safe_symbol = "".join(ch for ch in symbol.lower() if ch.isalnum())
		safe_interval = DataStorageService._normalize_interval_for_table(interval)
		return f"{safe_symbol}_{safe_interval}_raw"

	@staticmethod
	def ensure_raw_table(table_name: str) -> None:
		create_table_sql = f"""
		CREATE TABLE IF NOT EXISTS {table_name} (
			id BIGSERIAL PRIMARY KEY,
			open_time TIMESTAMP NOT NULL,
			open DOUBLE PRECISION NOT NULL,
			high DOUBLE PRECISION NOT NULL,
			low DOUBLE PRECISION NOT NULL,
			close DOUBLE PRECISION NOT NULL,
			volume DOUBLE PRECISION NOT NULL,
			close_time TIMESTAMP NOT NULL,
			quote_asset_volume DOUBLE PRECISION NOT NULL,
			number_of_trades INTEGER NOT NULL,
			taker_buy_base_asset_volume DOUBLE PRECISION NOT NULL,
			taker_buy_quote_asset_volume DOUBLE PRECISION NOT NULL,
			ignore_value DOUBLE PRECISION NOT NULL,
			open_time_ms BIGINT NOT NULL UNIQUE,
			close_time_ms BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
		"""

		with engine.begin() as connection:
			connection.execute(text(create_table_sql))

	@staticmethod
	def get_latest_open_time_ms(table_name: str) -> Optional[int]:
		DataStorageService.ensure_raw_table(table_name)
		query = text(f"SELECT MAX(open_time_ms) FROM {table_name}")

		with engine.begin() as connection:
			result = connection.execute(query).scalar()

		return int(result) if result is not None else None

	@staticmethod
	def upsert_raw_klines(df: pd.DataFrame, table_name: str) -> int:
		if df.empty:
			return 0
		DataStorageService.ensure_raw_table(table_name)

		records: list[dict] = []
		for _, row in df.iterrows():
			records.append(
				{
					"open_time": row["open_time"],
					"open": float(row["open"]),
					"high": float(row["high"]),
					"low": float(row["low"]),
					"close": float(row["close"]),
					"volume": float(row["volume"]),
					"close_time": row["close_time"],
					"quote_asset_volume": float(row["quote_asset_volume"]),
					"number_of_trades": int(row["number_of_trades"]),
					"taker_buy_base_asset_volume": float(row["taker_buy_base_asset_volume"]),
					"taker_buy_quote_asset_volume": float(row["taker_buy_quote_asset_volume"]),
					"ignore_value": float(row["ignore_value"]),
					"open_time_ms": int(row["open_time_ms"]),
					"close_time_ms": int(row["close_time_ms"]),
				}
			)

		upsert_sql = text(
			f"""
			INSERT INTO {table_name} (
				open_time, open, high, low, close, volume,
				close_time, quote_asset_volume, number_of_trades,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
				ignore_value, open_time_ms, close_time_ms
			)
			VALUES (
				:open_time, :open, :high, :low, :close, :volume,
				:close_time, :quote_asset_volume, :number_of_trades,
				:taker_buy_base_asset_volume, :taker_buy_quote_asset_volume,
				:ignore_value, :open_time_ms, :close_time_ms
			)
			ON CONFLICT (open_time_ms)
			DO UPDATE SET
				open = EXCLUDED.open,
				high = EXCLUDED.high,
				low = EXCLUDED.low,
				close = EXCLUDED.close,
				volume = EXCLUDED.volume,
				close_time = EXCLUDED.close_time,
				quote_asset_volume = EXCLUDED.quote_asset_volume,
				number_of_trades = EXCLUDED.number_of_trades,
				taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
				taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
				ignore_value = EXCLUDED.ignore_value,
				close_time_ms = EXCLUDED.close_time_ms,
				updated_at = NOW()
			"""
		)

		with engine.begin() as connection:
			connection.execute(upsert_sql, records)

		return len(records)

	@staticmethod
	def build_processed_table_name(symbol: str, interval: str) -> str:
		safe_symbol = "".join(ch for ch in symbol.lower() if ch.isalnum())
		safe_interval = DataStorageService._normalize_interval_for_table(interval)
		return f"{safe_symbol}_{safe_interval}_processed"

	@staticmethod
	def create_processed_table(table_name: str, symbol: str, interval: str) -> None:
		create_table_sql = f"""
		CREATE TABLE IF NOT EXISTS {table_name} (
			id BIGSERIAL PRIMARY KEY,
			open_time TIMESTAMP NOT NULL,
			open DOUBLE PRECISION,
			high DOUBLE PRECISION,
			low DOUBLE PRECISION,
			close DOUBLE PRECISION,
			volume DOUBLE PRECISION,
			close_time TIMESTAMP NOT NULL,
			quote_asset_volume DOUBLE PRECISION,
			number_of_trades INTEGER,
			taker_buy_base_asset_volume DOUBLE PRECISION,
			taker_buy_quote_asset_volume DOUBLE PRECISION,
			ignore_value DOUBLE PRECISION,
			open_time_ms BIGINT NOT NULL UNIQUE,
			close_time_ms BIGINT NOT NULL,
			is_synthetic BOOLEAN NOT NULL DEFAULT FALSE,
			is_missing BOOLEAN NOT NULL DEFAULT FALSE,
			is_interpolated BOOLEAN NOT NULL DEFAULT FALSE,
			fill_source VARCHAR(64) NOT NULL DEFAULT 'raw_sync',
			fill_method VARCHAR(64) NOT NULL DEFAULT 'raw_sync',
			confidence_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
		"""

		with engine.begin() as connection:
			connection.execute(text(create_table_sql))

		# Ensure older processed tables can store placeholders and flags.
		alter_sql = [
			f"ALTER TABLE {table_name} ALTER COLUMN open DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN high DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN low DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN close DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN volume DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN quote_asset_volume DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN number_of_trades DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN taker_buy_base_asset_volume DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN taker_buy_quote_asset_volume DROP NOT NULL",
			f"ALTER TABLE {table_name} ALTER COLUMN ignore_value DROP NOT NULL",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS is_missing BOOLEAN NOT NULL DEFAULT FALSE",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS is_interpolated BOOLEAN NOT NULL DEFAULT FALSE",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS fill_source VARCHAR(64) NOT NULL DEFAULT 'raw_sync'",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS fill_method VARCHAR(64) NOT NULL DEFAULT 'raw_sync'",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS confidence_score DOUBLE PRECISION NOT NULL DEFAULT 1.0",
		]

		with engine.begin() as connection:
			for stmt in alter_sql:
				connection.execute(text(stmt))

	@staticmethod
	def merge_raw_to_processed(raw_table_name: str, processed_table_name: str) -> int:
		"""Sync all raw rows into processed table and overwrite placeholders if raw arrives later."""
		merge_sql = text(
			f"""
			INSERT INTO {processed_table_name} (
				open_time, open, high, low, close, volume, close_time,
				quote_asset_volume, number_of_trades,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
				ignore_value, open_time_ms, close_time_ms,
				is_synthetic, is_missing, is_interpolated, fill_source, fill_method,
				confidence_score
			)
			SELECT
				open_time, open, high, low, close, volume, close_time,
				quote_asset_volume, number_of_trades,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
				ignore_value, open_time_ms, close_time_ms,
				FALSE AS is_synthetic,
				FALSE AS is_missing,
				FALSE AS is_interpolated,
				'raw_sync' AS fill_source,
				'raw_sync' AS fill_method,
				1.0 AS confidence_score
			FROM {raw_table_name}
			ON CONFLICT (open_time_ms)
			DO UPDATE SET
				open_time = EXCLUDED.open_time,
				open = EXCLUDED.open,
				high = EXCLUDED.high,
				low = EXCLUDED.low,
				close = EXCLUDED.close,
				volume = EXCLUDED.volume,
				close_time = EXCLUDED.close_time,
				quote_asset_volume = EXCLUDED.quote_asset_volume,
				number_of_trades = EXCLUDED.number_of_trades,
				taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
				taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
				ignore_value = EXCLUDED.ignore_value,
				close_time_ms = EXCLUDED.close_time_ms,
				is_synthetic = FALSE,
				is_missing = FALSE,
				is_interpolated = FALSE,
				fill_source = 'raw_sync',
				fill_method = 'raw_sync',
				confidence_score = 1.0,
				updated_at = NOW()
			"""
		)

		with engine.begin() as connection:
			result = connection.execute(merge_sql)

		return int(result.rowcount or 0)

	@staticmethod
	def upsert_processed_rows(table_name: str, rows: list[dict]) -> int:
		"""Upsert synthetic/placeholder rows into processed table."""
		if not rows:
			return 0

		upsert_sql = text(
			f"""
			INSERT INTO {table_name} (
				open_time, open, high, low, close, volume, close_time,
				quote_asset_volume, number_of_trades,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
				ignore_value, open_time_ms, close_time_ms,
				is_synthetic, is_missing, is_interpolated, fill_source, fill_method,
				confidence_score
			)
			VALUES (
				:open_time, :open, :high, :low, :close, :volume, :close_time,
				:quote_asset_volume, :number_of_trades,
				:taker_buy_base_asset_volume, :taker_buy_quote_asset_volume,
				:ignore_value, :open_time_ms, :close_time_ms,
				:is_synthetic, :is_missing, :is_interpolated, :fill_source, :fill_method,
				:confidence_score
			)
			ON CONFLICT (open_time_ms)
			DO UPDATE SET
				open_time = EXCLUDED.open_time,
				open = EXCLUDED.open,
				high = EXCLUDED.high,
				low = EXCLUDED.low,
				close = EXCLUDED.close,
				volume = EXCLUDED.volume,
				close_time = EXCLUDED.close_time,
				quote_asset_volume = EXCLUDED.quote_asset_volume,
				number_of_trades = EXCLUDED.number_of_trades,
				taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
				taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
				ignore_value = EXCLUDED.ignore_value,
				close_time_ms = EXCLUDED.close_time_ms,
				is_synthetic = EXCLUDED.is_synthetic,
				is_missing = EXCLUDED.is_missing,
				is_interpolated = EXCLUDED.is_interpolated,
				fill_source = EXCLUDED.fill_source,
				fill_method = EXCLUDED.fill_method,
				confidence_score = EXCLUDED.confidence_score,
				updated_at = NOW()
			"""
		)

		with engine.begin() as connection:
			connection.execute(upsert_sql, rows)

		return len(rows)

	@staticmethod
	def reorder_raw_table_by_time(table_name: str) -> None:
		"""
		Reorder a raw table by open_time_ms to fix chronological inversions.
		
		CRITICAL: Some data sources may insert records out of order (late-arriving data).
		This method ensures IDs follow chronological order, maintaining time-series integrity.
		
		Preserves all existing columns including open_time, close_time, and all OHLCV data.
		"""
		with engine.begin() as connection:
			columns_query = text(
				"""
				SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = :table_name
				ORDER BY ordinal_position
				"""
			)
			column_rows = connection.execute(columns_query, {"table_name": table_name}).mappings().all()

			all_columns = [str(row["column_name"]) for row in column_rows]
			non_id_columns = [col for col in all_columns if col != "id"]
			if not non_id_columns:
				return

			column_list_sql = ", ".join(non_id_columns)

			snapshot_sql = text(
				f"""
				CREATE TEMP TABLE tmp_{table_name}_ordered AS
				SELECT
					ROW_NUMBER() OVER (ORDER BY open_time_ms) AS id,
					{column_list_sql}
				FROM {table_name}
				ORDER BY open_time_ms
				"""
			)

			truncate_sql = text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")

			reinsert_sql = text(
				f"""
				INSERT INTO {table_name} (id, {column_list_sql})
				SELECT id, {column_list_sql}
				FROM tmp_{table_name}_ordered
				ORDER BY id
				"""
			)

			set_sequence_sql = text(
				f"""
				SELECT setval(
					pg_get_serial_sequence('{table_name}', 'id'),
					COALESCE((SELECT MAX(id) FROM {table_name}), 1),
					TRUE
				)
				"""
			)

			drop_tmp_sql = text(f"DROP TABLE IF EXISTS tmp_{table_name}_ordered")

			connection.execute(snapshot_sql)
			connection.execute(truncate_sql)
			connection.execute(reinsert_sql)
			connection.execute(set_sequence_sql)
			connection.execute(drop_tmp_sql)

	@staticmethod
	def reorder_processed_table_by_time(table_name: str) -> None:
		"""Reinsert rows ordered by open_time_ms so IDs follow chronological order.

		Preserves all existing columns dynamically (including indicator columns).
		"""
		with engine.begin() as connection:
			columns_query = text(
				"""
				SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = :table_name
				ORDER BY ordinal_position
				"""
			)
			column_rows = connection.execute(columns_query, {"table_name": table_name}).mappings().all()

			all_columns = [str(row["column_name"]) for row in column_rows]
			non_id_columns = [col for col in all_columns if col != "id"]
			if not non_id_columns:
				return

			column_list_sql = ", ".join(non_id_columns)

			snapshot_sql = text(
				f"""
				CREATE TEMP TABLE tmp_{table_name}_ordered AS
				SELECT
					ROW_NUMBER() OVER (ORDER BY open_time_ms) AS id,
					{column_list_sql}
				FROM {table_name}
				ORDER BY open_time_ms
				"""
			)

			truncate_sql = text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")

			reinsert_sql = text(
				f"""
				INSERT INTO {table_name} (id, {column_list_sql})
				SELECT id, {column_list_sql}
				FROM tmp_{table_name}_ordered
				ORDER BY id
				"""
			)

			set_sequence_sql = text(
				f"""
				SELECT setval(
					pg_get_serial_sequence('{table_name}', 'id'),
					COALESCE((SELECT MAX(id) FROM {table_name}), 1),
					TRUE
				)
				"""
			)

			drop_tmp_sql = text(f"DROP TABLE IF EXISTS tmp_{table_name}_ordered")

			connection.execute(snapshot_sql)
			connection.execute(truncate_sql)
			connection.execute(reinsert_sql)
			connection.execute(set_sequence_sql)
			connection.execute(drop_tmp_sql)

	@staticmethod
	def ensure_indicator_columns(table_name: str) -> None:
		"""Ensure all requested indicator/fundamental/sentiment columns exist."""
		alter_sql = [
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ma_20 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ema_12 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ema_26 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS macd DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS macd_signal DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS macd_hist DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS adx_14 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS rsi_14 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS stoch_k_14 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS stoch_d_3 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS cci_20 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS bollinger_mid_20 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS bollinger_upper_20 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS bollinger_lower_20 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS atr_14 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS vix DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS obv DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS vwap DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS mfi_14 DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ad_line DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS volume_oscillator DOUBLE PRECISION",
		]

		with engine.begin() as connection:
			for stmt in alter_sql:
				connection.execute(text(stmt))

	@staticmethod
	def ensure_pattern_columns(table_name: str) -> None:
		"""Ensure candlestick pattern output columns exist on processed tables."""
		alter_sql = [
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS candle_pattern_1 VARCHAR(128)",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS candle_pattern_2 VARCHAR(128)",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS candle_pattern_3 VARCHAR(128)",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS candle_pattern_complex VARCHAR(128)",
		]

		with engine.begin() as connection:
			for stmt in alter_sql:
				connection.execute(text(stmt))

	@staticmethod
	def ensure_trend_columns(table_name: str) -> None:
		"""Ensure trend handling columns exist on processed tables."""
		alter_sql = [
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS support DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS resistance DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS trend_pct_change DOUBLE PRECISION",
		]

		with engine.begin() as connection:
			for stmt in alter_sql:
				connection.execute(text(stmt))

		# Remove legacy columns that are no longer part of the trend table contract.
		drop_sql = [
			f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS trend",
			f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS trend_category",
			f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS is_turning_point",
			f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS turning_point_score",
		]

		with engine.begin() as connection:
			for stmt in drop_sql:
				connection.execute(text(stmt))

	@staticmethod
	def ensure_chart_pattern_columns(table_name: str) -> None:
		"""Ensure geometric chart pattern columns exist on processed tables."""
		alter_sql = [
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS chart_pattern_type VARCHAR(128)",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS chart_pattern_confidence DOUBLE PRECISION",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS chart_pattern_direction VARCHAR(32)",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS chart_pattern_start_time_ms BIGINT",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS chart_pattern_end_time_ms BIGINT",
			f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS chart_pattern_key_points TEXT",
		]

		with engine.begin() as connection:
			for stmt in alter_sql:
				connection.execute(text(stmt))

	@staticmethod
	def count_time_sequence_violations(table_name: str) -> int:
		"""Return count of non-strict time steps when ordered by open_time_ms."""
		query = text(
			f"""
			SELECT COUNT(*)
			FROM (
				SELECT
					open_time_ms,
					LAG(open_time_ms) OVER (ORDER BY open_time_ms) AS prev_open_time_ms
				FROM {table_name}
			) ordered
			WHERE prev_open_time_ms IS NOT NULL AND open_time_ms <= prev_open_time_ms
			"""
		)

		with engine.begin() as connection:
			result = connection.execute(query).scalar() or 0

		return int(result)

	@staticmethod
	def count_id_inversions_by_time(table_name: str) -> int:
		"""Diagnostic only: count rows where id decreases when ordered by open_time_ms."""
		query = text(
			f"""
			SELECT COUNT(*)
			FROM (
				SELECT
					id,
					LAG(id) OVER (ORDER BY open_time_ms) AS prev_id
				FROM {table_name}
			) ordered
			WHERE prev_id IS NOT NULL AND id < prev_id
			"""
		)

		with engine.begin() as connection:
			result = connection.execute(query).scalar() or 0

		return int(result)

	@staticmethod
	def validate_ordering(table_name: str) -> tuple[bool, int, str]:
		"""
		Validate that a table maintains STRICT ascending order by open_time_ms.
		
		Returns:
			(is_ordered: bool, inversion_count: int, message: str)
		
		MANDATORY: All preprocessing, indicators, and pattern detection MUST run 
		on strictly ordered data. This check is NON-NEGOTIABLE.
		"""
		# Get row count first
		count_query = text(f"SELECT COUNT(*) FROM {table_name}")
		with engine.begin() as connection:
			row_count = connection.execute(count_query).scalar() or 0
		
		if row_count == 0:
			return True, 0, "Table is empty (no ordering issues)"
		
		if row_count == 1:
			return True, 0, "Single row (trivially ordered)"
		
		# Strict sequence check must be based only on open_time_ms.
		# Never rely on id ordering because ids reflect insertion timing, not market time.
		inversion_count = DataStorageService.count_time_sequence_violations(table_name)
		
		is_ordered = inversion_count == 0
		message = (
			f"✅ Strictly ordered: {row_count} rows, 0 inversions"
			if is_ordered
			else f"❌ ORDER VIOLATION: {inversion_count} inversions detected in {row_count} rows"
		)
		
		return is_ordered, int(inversion_count), message

	@staticmethod
	def create_time_series_indexes() -> dict[str, str]:
		"""
		Create critical indexes on all raw and processed tables.
		
		MANDATORY for time-series integrity:
		- Raw table: INDEX on (open_time_ms) for deterministic ordering during fetch
		- Processed tables: INDEX on (open_time_ms) for indicator calculations
		
		Returns:
			Dictionary with table name → status for each index created
		"""
		results = {}
		
		with engine.begin() as connection:
			# Get all raw and processed tables
			tables_query = text(
				"""
				SELECT table_name 
				FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND (table_name LIKE '%_raw' OR table_name LIKE '%_processed')
				ORDER BY table_name
				"""
			)
			tables = connection.execute(tables_query).scalars().all()
		
		for table_name in tables:
			try:
				index_name = f"idx_{table_name}_open_time_ms"
				create_index_sql = text(
					f"""
					CREATE INDEX IF NOT EXISTS {index_name}
					ON {table_name} (open_time_ms ASC)
					"""
				)
				
				with engine.begin() as connection:
					connection.execute(create_index_sql)
				
				results[table_name] = f"✅ Index {index_name} created/verified"
			except Exception as e:
				results[table_name] = f"❌ Failed: {str(e)}"
		
		return results

	@staticmethod
	def validate_all_processed_tables() -> dict[str, tuple[bool, int, str]]:
		"""
		Validate ordering across ALL processed tables.
		
		FAIL-SAFE: If ANY processed table is disordered, this returns diagnostics
		to halt the pipeline before downstream calculations corrupt.
		
		Returns:
			Dictionary with table_name → (is_ordered, inversion_count, message)
		"""
		results = {}
		
		with engine.begin() as connection:
			tables_query = text(
				"""
				SELECT table_name 
				FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name LIKE '%_processed'
				ORDER BY table_name
				"""
			)
			tables = connection.execute(tables_query).scalars().all()
		
		for table_name in tables:
			is_ordered, inversion_count, message = DataStorageService.validate_ordering(table_name)
			results[table_name] = (is_ordered, inversion_count, message)
		
		return results

	@staticmethod
	def enforce_configured_table_ordering(table_kind: str) -> dict[str, int]:
		"""
		Enforce ordering integrity for all configured symbol/interval tables.

		Checks at process start:
		1) strict open_time_ms sequence (source of truth)
		2) id inversions by time (diagnostic and realignment trigger)

		If either check fails, table is reordered and re-validated.
		Raises RuntimeError if any table remains invalid after repair.
		"""
		if table_kind not in {"raw", "processed"}:
			raise ValueError(f"Invalid table_kind={table_kind}. Use 'raw' or 'processed'.")

		from app.config.settings import BINANCE_INTERVALS, BINANCE_SYMBOLS

		stats = {
			"tables_seen": 0,
			"tables_checked": 0,
			"tables_reordered": 0,
			"time_violations_before": 0,
			"time_violations_after": 0,
			"id_inversions_before": 0,
			"id_inversions_after": 0,
			"failures": 0,
		}

		failures: list[str] = []

		for symbol in BINANCE_SYMBOLS:
			for interval in BINANCE_INTERVALS:
				stats["tables_seen"] += 1
				if table_kind == "raw":
					table_name = DataStorageService.build_raw_table_name(symbol=symbol, interval=interval)
				else:
					table_name = DataStorageService.build_processed_table_name(symbol=symbol, interval=interval)

				if not DataStorageService.table_exists(table_name):
					continue

				stats["tables_checked"] += 1
				time_before = DataStorageService.count_time_sequence_violations(table_name)
				id_before = DataStorageService.count_id_inversions_by_time(table_name)
				stats["time_violations_before"] += time_before
				stats["id_inversions_before"] += id_before

				needs_reorder = time_before > 0 or id_before > 0
				if needs_reorder:
					if table_kind == "raw":
						DataStorageService.reorder_raw_table_by_time(table_name)
					else:
						DataStorageService.reorder_processed_table_by_time(table_name)
					stats["tables_reordered"] += 1

				time_after = DataStorageService.count_time_sequence_violations(table_name)
				id_after = DataStorageService.count_id_inversions_by_time(table_name)
				stats["time_violations_after"] += time_after
				stats["id_inversions_after"] += id_after

				if time_after > 0 or id_after > 0:
					stats["failures"] += 1
					failures.append(
						f"{table_name}: time_after={time_after}, id_after={id_after}"
					)

		if failures:
			raise RuntimeError(
				"Ordering enforcement failed for table(s): " + "; ".join(failures)
			)

		return stats

	@staticmethod
	def ensure_table_ordering_for_calculation(table_name: str, table_kind: str) -> dict[str, int]:
		"""
		Per-table guard for calculation stages.

		This method is intended to run immediately before indicators/patterns/trend/chart
		computations so table ordering problems never propagate into calculations.
		"""
		if table_kind not in {"raw", "processed"}:
			raise ValueError(f"Invalid table_kind={table_kind}. Use 'raw' or 'processed'.")

		stats = {
			"time_before": 0,
			"id_before": 0,
			"reordered": 0,
			"time_after": 0,
			"id_after": 0,
		}

		if not DataStorageService.table_exists(table_name):
			return stats

		time_before = DataStorageService.count_time_sequence_violations(table_name)
		id_before = DataStorageService.count_id_inversions_by_time(table_name)
		stats["time_before"] = time_before
		stats["id_before"] = id_before

		if time_before > 0 or id_before > 0:
			if table_kind == "raw":
				DataStorageService.reorder_raw_table_by_time(table_name)
			else:
				DataStorageService.reorder_processed_table_by_time(table_name)
			stats["reordered"] = 1

		time_after = DataStorageService.count_time_sequence_violations(table_name)
		id_after = DataStorageService.count_id_inversions_by_time(table_name)
		stats["time_after"] = time_after
		stats["id_after"] = id_after

		if time_after > 0 or id_after > 0:
			raise RuntimeError(
				f"Ordering guard failed for {table_name}: "
				f"time_after={time_after}, id_after={id_after}"
			)

		return stats
