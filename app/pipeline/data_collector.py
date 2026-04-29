from dataclasses import dataclass
from typing import Optional

from app.config.settings import (
    BINANCE_FULL_HISTORY_SYNC,
    BINANCE_INTERVALS,
    BINANCE_KLINE_LIMIT,
    BINANCE_SYMBOLS,
)
from app.services.binance_service import BinanceService
from app.services.data_storage_service import DataStorageService


@dataclass
class CollectionResult:
    symbol: str
    interval: str
    fetched_rows: int
    saved_rows: int
    table_name: str


class DataCollectorPipeline:
    """Collect raw klines and save them to the configured database."""

    def __init__(self) -> None:
        self.binance_service = BinanceService()
        self.storage_service = DataStorageService()

    def run(
        self,
        symbol: str,
        interval: str,
        limit: int = BINANCE_KLINE_LIMIT,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
    ) -> CollectionResult:

        table_name = self.storage_service.build_raw_table_name(symbol=symbol, interval=interval)
        latest_open_time_ms = self.storage_service.get_latest_open_time_ms(table_name)

        # 🔹 Determine mode
        if start_time_ms is not None:
            effective_start = start_time_ms
            mode = "MANUAL"
        elif BINANCE_FULL_HISTORY_SYNC:
            if latest_open_time_ms is None:
                effective_start = 0
                mode = "FULL_HISTORY"
            else:
                effective_start = latest_open_time_ms + 1
                mode = "INCREMENTAL"
        else:
            effective_start = latest_open_time_ms + 1 if latest_open_time_ms is not None else 0
            mode = "INCREMENTAL"

        print("\n" + "=" * 80)
        print(f"🚀 START SYNC  | {symbol.upper()} | {interval}")
        print("-" * 80)
        print(f"📦 Table        : {table_name}")
        print(f"⚙️  Mode         : {mode}")
        print(f"⏱️  Start Time   : {effective_start}")
        print(f"📄 Page Size    : {limit}")
        print("=" * 80, flush=True)

        total_fetched = 0
        total_saved = 0
        next_start = effective_start
        page_no = 0

        while True:
            page_no += 1

            print(f"\n🔄 FETCHING PAGE {page_no}...", flush=True)

            df = self.binance_service.fetch_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                start_time_ms=next_start,
                end_time_ms=end_time_ms,
            )

            if df.empty:
                print(f"⚠️  No new data found. Stopping.", flush=True)
                break

            print(f"📥 Fetched Rows : {len(df)}", flush=True)

            saved_rows = self.storage_service.upsert_raw_klines(df=df, table_name=table_name)

            total_fetched += len(df)
            total_saved += saved_rows

            first_ms = int(df["open_time_ms"].min())
            last_ms = int(df["open_time_ms"].max())

            print(f"💾 Saved Rows   : {saved_rows}")
            print(f"📊 Time Range   : {first_ms} → {last_ms}")
            print(f"📈 Total Fetched: {total_fetched}")
            print(f"📉 Total Saved  : {total_saved}", flush=True)

            # Stop if last page
            if len(df) < limit:
                print("✅ Last page reached.", flush=True)
                break

            next_start = last_ms + 1

        print("\n" + "-" * 80)
        print(f"✅ COMPLETED | {symbol.upper()} | {interval}")
        print(f"📈 Total Fetched : {total_fetched}")
        print(f"💾 Total Saved   : {total_saved}")
        print("=" * 80 + "\n", flush=True)

        return CollectionResult(
            symbol=symbol.upper(),
            interval=interval,
            fetched_rows=total_fetched,
            saved_rows=total_saved,
            table_name=table_name,
        )

    def run_for_symbol_all_intervals(self, symbol: str) -> list[CollectionResult]:
        print(f"\n🎯 START SYMBOL: {symbol.upper()}")
        print(f"🧩 Intervals: {', '.join(BINANCE_INTERVALS)}\n", flush=True)

        results: list[CollectionResult] = []

        for interval in BINANCE_INTERVALS:
            results.append(self.run(symbol=symbol, interval=interval, limit=BINANCE_KLINE_LIMIT))

        total_saved = sum(item.saved_rows for item in results)

        print(f"\n🏁 SYMBOL COMPLETE: {symbol.upper()}")
        print(f"💾 Total Saved Across Intervals: {total_saved}")
        print("=" * 80 + "\n", flush=True)

        return results

    def run_for_env_symbols_all_intervals(self) -> list[CollectionResult]:
        print("\n🌍 START FULL ENV SYNC\n", flush=True)

        print("🔒 PRE-CHECK: Enforcing RAW table ordering integrity ...", flush=True)
        ordering_stats = DataStorageService.enforce_configured_table_ordering("raw")
        print(
            "   ✔ RAW ordering check complete | "
            f"checked={ordering_stats['tables_checked']} "
            f"reordered={ordering_stats['tables_reordered']} "
            f"time_after={ordering_stats['time_violations_after']} "
            f"id_after={ordering_stats['id_inversions_after']}",
            flush=True,
        )

        all_results: list[CollectionResult] = []

        for symbol in BINANCE_SYMBOLS:
            all_results.extend(self.run_for_symbol_all_intervals(symbol))

        print("\n🎉 ALL SYMBOLS COMPLETED\n", flush=True)

        return all_results