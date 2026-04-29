import time

from app.config.settings import BINANCE_INTERVALS, BINANCE_SYMBOLS
from app.services.data_storage_service import DataStorageService


class SeriesOrderPipeline:
    """Final guard stage: enforce and verify strict time ordering by open_time_ms."""

    def run(self) -> dict:
        stats = {
            "tables_checked": 0,
            "tables_reordered": 0,
            "inversions_before": 0,
            "inversions_after": 0,
            "failures": [],
        }

        total_tasks = len(BINANCE_SYMBOLS) * len(BINANCE_INTERVALS)
        task_number = 0
        started = time.time()

        print("\n" + "=" * 80)
        print("🧭  SERIES ORDER INTEGRITY PIPELINE — STARTING")
        print(f"    Total jobs: {total_tasks}")
        print("=" * 80, flush=True)

        for symbol in BINANCE_SYMBOLS:
            for interval in BINANCE_INTERVALS:
                task_number += 1
                table_name = DataStorageService.build_processed_table_name(symbol=symbol, interval=interval)
                print(f"\n  ┌─ Job [{task_number}/{total_tasks}]  {symbol.upper()} | {interval}")
                print(f"  │  Table : {table_name}", flush=True)

                try:
                    before = DataStorageService.count_time_sequence_violations(table_name)
                    stats["inversions_before"] += before
                    print(f"  │  Time violations before : {before}", flush=True)

                    # Diagnostic only. ID order is not the source of truth.
                    id_before = DataStorageService.count_id_inversions_by_time(table_name)
                    print(f"  │  ID inversions (diagnostic) before : {id_before}", flush=True)

                    DataStorageService.reorder_processed_table_by_time(table_name)
                    stats["tables_reordered"] += 1

                    after = DataStorageService.count_time_sequence_violations(table_name)
                    stats["inversions_after"] += after
                    print(f"  │  Time violations after  : {after}", flush=True)

                    id_after = DataStorageService.count_id_inversions_by_time(table_name)
                    print(f"  │  ID inversions (diagnostic) after  : {id_after}", flush=True)

                    if after != 0:
                        raise RuntimeError(f"time ordering still inconsistent (after={after})")

                    stats["tables_checked"] += 1
                    print("  └─ ✅  VERIFIED", flush=True)

                except Exception as exc:
                    stats["failures"].append(f"{symbol}/{interval}: {str(exc)}")
                    print(f"  └─ ❌  FAILED: {exc}", flush=True)

        elapsed = time.time() - started
        print("\n" + "=" * 80)
        print("📊  SERIES ORDER INTEGRITY PIPELINE — SUMMARY")
        print("=" * 80)
        print(f"  ⏱  Total runtime      : {elapsed:.2f}s")
        print(f"  📦  Tables checked     : {stats['tables_checked']} / {total_tasks}")
        print(f"  🔁  Tables reordered   : {stats['tables_reordered']}")
        print(f"  ↘️  Time violations before : {stats['inversions_before']}")
        print(f"  ↗️  Time violations after  : {stats['inversions_after']}")
        if stats["failures"]:
            print(f"  ❌  Failures           : {len(stats['failures'])}")
            for failure in stats["failures"]:
                print(f"       • {failure}")
        else:
            print("  ✅  All tables verified with strict open_time_ms ordering")
        print("=" * 80 + "\n", flush=True)

        return stats
