import time

import pandas as pd
from sqlalchemy import text

from app.indicators.atr import compute_volatility
from app.indicators.ema import compute_ma_ema
from app.indicators.macd import compute_macd
from app.indicators.rsi import compute_rsi
from app.indicators.stochastic_rsi import compute_stochastic_cci
from app.indicators.volume_oscillator import compute_volume_indicators
from app.config.database import engine
from app.config.settings import BINANCE_INTERVALS, BINANCE_SYMBOLS
from app.services.data_storage_service import DataStorageService


class IndicatorPipeline:
    """Calculates and stores indicators for all processed tables."""

    def run(self) -> dict:
        stats = {
            "tables_processed": 0,
            "rows_updated": 0,
            "failures": [],
        }

        total_symbols   = len(BINANCE_SYMBOLS)
        total_intervals = len(BINANCE_INTERVALS)
        total_tasks     = total_symbols * total_intervals
        task_number     = 0
        pipeline_start  = time.time()

        print("\n" + "=" * 80)
        print("📐  INDICATOR ENRICHMENT PIPELINE — STARTING")
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
                task_start   = time.time()

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

                    # ── Step 1: Ensure columns exist ──────────────────────────────
                    print("  │  [1/5] Ensuring indicator columns exist ...", flush=True)
                    DataStorageService.ensure_indicator_columns(table_name)
                    print("  │        ✔ Columns verified", flush=True)

                    # ── Step 2: Load raw processed data ───────────────────────────
                    print("  │  [2/5] Loading processed OHLCV data ...", flush=True)
                    df = self._load_processed_data(table_name)

                    if df.empty:
                        elapsed = time.time() - task_start
                        print(f"  │        ⚠️  No rows found — skipping  ({elapsed:.2f}s)")
                        print("  └─ SKIPPED\n", flush=True)
                        continue

                    print(
                        f"  │        ✔ Loaded {len(df):,} row(s)  "
                        f"[open_time_ms {df['open_time_ms'].min()} → {df['open_time_ms'].max()}]",
                        flush=True,
                    )

                    # ── Step 3: Compute indicators ────────────────────────────────
                    print("  │  [3/5] Computing indicators ...", flush=True)
                    compute_start   = time.time()
                    enriched_df     = self._compute_indicators(df)
                    compute_elapsed = time.time() - compute_start

                    raw_cols = {
                        "open_time_ms", "close_time_ms", "open", "high", "low",
                        "close", "volume", "quote_asset_volume", "number_of_trades",
                        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                    }
                    indicator_cols  = [c for c in enriched_df.columns if c not in raw_cols]
                    non_null_counts = enriched_df[indicator_cols].notna().sum()

                    print(f"  │        ✔ All indicators computed in {compute_elapsed:.2f}s")
                    print(f"  │        ✔ Coverage per indicator ({len(df):,} rows total):")
                    for col, cnt in non_null_counts.items():
                        pct    = cnt / len(enriched_df) * 100
                        filled = int(pct / 5)
                        bar    = "█" * filled + "░" * (20 - filled)
                        print(f"  │           {col:<28} {bar}  {cnt:>6,}  ({pct:5.1f}%)")
                    print(flush=True)

                    # ── Step 4: Write indicators back to DB ───────────────────────
                    print("  │  [4/5] Writing indicators to database ...", flush=True)
                    write_start   = time.time()
                    updated       = self._write_indicators(table_name, enriched_df)
                    write_elapsed = time.time() - write_start
                    print(f"  │        ✔ {updated:,} row(s) updated in {write_elapsed:.2f}s", flush=True)

                    # ── Step 5: Reorder table ─────────────────────────────────────
                    print("  │  [5/5] Reordering table by open_time_ms ...", flush=True)
                    DataStorageService.reorder_processed_table_by_time(table_name)
                    print("  │        ✔ Table reordered", flush=True)

                    task_elapsed = time.time() - task_start
                    stats["tables_processed"] += 1
                    stats["rows_updated"]     += updated
                    print(f"  └─ ✅  DONE  ({task_elapsed:.2f}s)\n", flush=True)

                except Exception as exc:
                    task_elapsed = time.time() - task_start
                    error_msg    = f"{symbol}/{interval}: {str(exc)}"
                    stats["failures"].append(error_msg)
                    print(f"  │  ❌  ERROR: {exc}")
                    print(f"  └─ FAILED  ({task_elapsed:.2f}s)\n", flush=True)

            sym_elapsed = time.time() - sym_start
            print(f"  ⏱  Symbol {symbol.upper()} finished in {sym_elapsed:.2f}s", flush=True)

        pipeline_elapsed = time.time() - pipeline_start

        # ── Final summary ─────────────────────────────────────────────────────────
        print("\n" + "=" * 80)
        print("📊  INDICATOR PIPELINE — SUMMARY")
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
        print(f"  │        → Querying '{table_name}' ...", flush=True)
        query = text(
            f"""
            SELECT
                open_time_ms, close_time_ms,
                open, high, low, close, volume,
                quote_asset_volume, number_of_trades,
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume
            FROM {table_name}
            ORDER BY open_time_ms
            """
        )
        with engine.begin() as connection:
            rows = connection.execute(query).mappings().all()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        print(f"  │        → {len(df):,} row(s) fetched from DB", flush=True)
        return df

    @staticmethod
    def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        indicator_steps = [
            ("MA / EMA",          compute_ma_ema),
            ("MACD",              compute_macd),
            ("RSI",               compute_rsi),
            ("Stochastic / CCI",  compute_stochastic_cci),
            ("Volatility / ATR",  compute_volatility),
            ("Volume indicators", compute_volume_indicators),
        ]

        for label, fn in indicator_steps:
            step_start = time.time()
            frame      = fn(out)
            out        = out.join(frame)
            step_ms    = (time.time() - step_start) * 1000
            cols_str   = ", ".join(frame.columns.tolist())
            print(f"  │           → {label:<22} ✔  [{cols_str}]  ({step_ms:.1f}ms)", flush=True)

        return out

    @staticmethod
    def _to_none(value):
        if pd.isna(value):
            return None
        return float(value)

    def _write_indicators(self, table_name: str, df: pd.DataFrame) -> int:
        update_sql = text(
            f"""
            UPDATE {table_name}
            SET
                ma_20 = :ma_20,
                ema_12 = :ema_12,
                ema_26 = :ema_26,
                macd = :macd,
                macd_signal = :macd_signal,
                macd_hist = :macd_hist,
                adx_14 = :adx_14,
                rsi_14 = :rsi_14,
                stoch_k_14 = :stoch_k_14,
                stoch_d_3 = :stoch_d_3,
                cci_20 = :cci_20,
                bollinger_mid_20 = :bollinger_mid_20,
                bollinger_upper_20 = :bollinger_upper_20,
                bollinger_lower_20 = :bollinger_lower_20,
                atr_14 = :atr_14,
                vix = :vix,
                obv = :obv,
                vwap = :vwap,
                mfi_14 = :mfi_14,
                ad_line = :ad_line,
                volume_oscillator = :volume_oscillator,
                updated_at = NOW()
            WHERE open_time_ms = :open_time_ms
            """
        )

        print(f"  │        → Building {len(df):,} update record(s) ...", flush=True)
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "open_time_ms":       int(row["open_time_ms"]),
                    "ma_20":              self._to_none(row["ma_20"]),
                    "ema_12":             self._to_none(row["ema_12"]),
                    "ema_26":             self._to_none(row["ema_26"]),
                    "macd":               self._to_none(row["macd"]),
                    "macd_signal":        self._to_none(row["macd_signal"]),
                    "macd_hist":          self._to_none(row["macd_hist"]),
                    "adx_14":             self._to_none(row["adx_14"]),
                    "rsi_14":             self._to_none(row["rsi_14"]),
                    "stoch_k_14":         self._to_none(row["stoch_k_14"]),
                    "stoch_d_3":          self._to_none(row["stoch_d_3"]),
                    "cci_20":             self._to_none(row["cci_20"]),
                    "bollinger_mid_20":   self._to_none(row["bollinger_mid_20"]),
                    "bollinger_upper_20": self._to_none(row["bollinger_upper_20"]),
                    "bollinger_lower_20": self._to_none(row["bollinger_lower_20"]),
                    "atr_14":             self._to_none(row["atr_14"]),
                    "vix":                self._to_none(row["vix"]),
                    "obv":                self._to_none(row["obv"]),
                    "vwap":               self._to_none(row["vwap"]),
                    "mfi_14":             self._to_none(row["mfi_14"]),
                    "ad_line":            self._to_none(row["ad_line"]),
                    "volume_oscillator":  self._to_none(row["volume_oscillator"]),
                }
            )

        print(f"  │        → Executing batch UPDATE on '{table_name}' ...", flush=True)
        with engine.begin() as connection:
            connection.execute(update_sql, records)
        print("  │        → Batch commit successful", flush=True)

        return len(records)