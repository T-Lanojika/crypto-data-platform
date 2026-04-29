import time

import pandas as pd
from sqlalchemy import text

from app.config.database import engine
from app.config.settings import BINANCE_INTERVALS, BINANCE_SYMBOLS
from app.patterns.candlestick_patterns import compute_candlestick_patterns
from app.services.data_storage_service import DataStorageService


class PatternPipeline:
    """Calculates and stores candlestick patterns for all processed tables."""

    def run(self) -> dict:
        stats = {
            "tables_processed": 0,
            "rows_updated": 0,
            "failures": [],
        }

        total_symbols = len(BINANCE_SYMBOLS)
        total_intervals = len(BINANCE_INTERVALS)
        total_tasks = total_symbols * total_intervals
        task_number = 0
        pipeline_start = time.time()

        print("\n" + "=" * 80)
        print("🕯️  CANDLESTICK PATTERN PIPELINE — STARTING")
        print(f"    Symbols   : {total_symbols}  ({', '.join(s.upper() for s in BINANCE_SYMBOLS)})")
        print(f"    Intervals : {total_intervals}  ({', '.join(BINANCE_INTERVALS)})")
        print(f"    Total jobs: {total_tasks}")
        print("=" * 80, flush=True)

        print("🔒 PRE-CHECK: Enforcing PROCESSED table ordering integrity ...", flush=True)
        ordering_stats = DataStorageService.enforce_configured_table_ordering("processed")
        print(
            "   ✔ PROCESSED ordering check complete | "
            f"checked={ordering_stats['tables_checked']} "
            f"reordered={ordering_stats['tables_reordered']} "
            f"time_after={ordering_stats['time_violations_after']} "
            f"id_after={ordering_stats['id_inversions_after']}",
            flush=True,
        )

        for sym_idx, symbol in enumerate(BINANCE_SYMBOLS, start=1):
            sym_start = time.time()
            print(f"\n{'━' * 80}")
            print(f"🎯  SYMBOL [{sym_idx}/{total_symbols}]: {symbol.upper()}")
            print(f"{'━' * 80}", flush=True)

            for interval in BINANCE_INTERVALS:
                task_number += 1
                task_start = time.time()

                table_name = DataStorageService.build_processed_table_name(
                    symbol=symbol,
                    interval=interval,
                )

                print(f"\n  ┌─ Job [{task_number}/{total_tasks}]  {symbol.upper()} | {interval}")
                print(f"  │  Table : {table_name}", flush=True)

                try:
                    print("  │  [PRE-CHECK] Validating ID/time ordering before calculation ...", flush=True)
                    ordering = DataStorageService.ensure_table_ordering_for_calculation(
                        table_name=table_name,
                        table_kind="processed",
                    )
                    print(
                        "  │        ✔ Ordering guard "
                        f"(time_before={ordering['time_before']}, "
                        f"id_before={ordering['id_before']}, "
                        f"reordered={ordering['reordered']}, "
                        f"time_after={ordering['time_after']}, "
                        f"id_after={ordering['id_after']})",
                        flush=True,
                    )

                    print("  │  [1/4] Ensuring pattern columns exist ...", flush=True)
                    DataStorageService.ensure_pattern_columns(table_name)
                    print("  │        ✔ Columns verified", flush=True)

                    print("  │  [2/4] Loading OHLC data ...", flush=True)
                    df = self._load_processed_data(table_name)
                    if df.empty:
                        elapsed = time.time() - task_start
                        print(f"  │        ⚠️  No rows found — skipping  ({elapsed:.2f}s)")
                        print("  └─ SKIPPED\n", flush=True)
                        continue

                    print("  │  [3/4] Computing candlestick patterns ...", flush=True)
                    pattern_df = compute_candlestick_patterns(df)
                    valid_count = int(pattern_df.notna().any(axis=1).sum())
                    print(f"  │        ✔ Valid pattern rows: {valid_count:,}", flush=True)

                    print("  │  [4/4] Writing patterns to database ...", flush=True)
                    updated = self._write_patterns(table_name, df, pattern_df)
                    print(f"  │        ✔ {updated:,} row(s) updated", flush=True)

                    DataStorageService.reorder_processed_table_by_time(table_name)

                    task_elapsed = time.time() - task_start
                    stats["tables_processed"] += 1
                    stats["rows_updated"] += updated
                    print(f"  └─ ✅  DONE  ({task_elapsed:.2f}s)\n", flush=True)

                except Exception as exc:
                    task_elapsed = time.time() - task_start
                    error_msg = f"{symbol}/{interval}: {str(exc)}"
                    stats["failures"].append(error_msg)
                    print(f"  │  ❌  ERROR: {exc}")
                    print(f"  └─ FAILED  ({task_elapsed:.2f}s)\n", flush=True)

            sym_elapsed = time.time() - sym_start
            print(f"  ⏱  Symbol {symbol.upper()} finished in {sym_elapsed:.2f}s", flush=True)

        pipeline_elapsed = time.time() - pipeline_start
        print("\n" + "=" * 80)
        print("📊  CANDLESTICK PATTERN PIPELINE — SUMMARY")
        print("=" * 80)
        print(f"  ⏱  Total runtime     : {pipeline_elapsed:.2f}s")
        print(f"  📦  Tables processed  : {stats['tables_processed']} / {total_tasks}")
        print(f"  🧮  Rows updated      : {stats['rows_updated']:,}")
        if stats["failures"]:
            print(f"  ❌  Failures          : {len(stats['failures'])}")
            for failure in stats["failures"]:
                print(f"       • {failure}")
        else:
            print("  ✅  All jobs completed with no failures")
        print("=" * 80 + "\n", flush=True)

        return stats

    @staticmethod
    def _load_processed_data(table_name: str) -> pd.DataFrame:
        query = text(
            f"""
            SELECT open_time_ms, open, high, low, close
            FROM {table_name}
            ORDER BY open_time_ms
            """
        )
        with engine.begin() as connection:
            rows = connection.execute(query).mappings().all()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([dict(row) for row in rows])

    @staticmethod
    def _write_patterns(table_name: str, base_df: pd.DataFrame, pattern_df: pd.DataFrame) -> int:
        update_sql = text(
            f"""
            UPDATE {table_name}
            SET
                candle_pattern_1 = :candle_pattern_1,
                candle_pattern_2 = :candle_pattern_2,
                candle_pattern_3 = :candle_pattern_3,
                candle_pattern_complex = :candle_pattern_complex,
                updated_at = NOW()
            WHERE open_time_ms = :open_time_ms
            """
        )

        merged = base_df[["open_time_ms"]].join(pattern_df)
        records = []
        for _, row in merged.iterrows():
            records.append(
                {
                    "open_time_ms": int(row["open_time_ms"]),
                    "candle_pattern_1": None if pd.isna(row["candle_pattern_1"]) else str(row["candle_pattern_1"]),
                    "candle_pattern_2": None if pd.isna(row["candle_pattern_2"]) else str(row["candle_pattern_2"]),
                    "candle_pattern_3": None if pd.isna(row["candle_pattern_3"]) else str(row["candle_pattern_3"]),
                    "candle_pattern_complex": None if pd.isna(row["candle_pattern_complex"]) else str(row["candle_pattern_complex"]),
                }
            )

        with engine.begin() as connection:
            connection.execute(update_sql, records)

        return len(records)
