import os
import sys
from datetime import UTC, datetime

if __package__ in (None, ""):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from app.pipeline.data_collector import DataCollectorPipeline
from app.pipeline.chart_pattern_pipeline import ChartPatternPipeline
from app.pipeline.indicator_pipeline import IndicatorPipeline
from app.pipeline.pattern_pipeline import PatternPipeline
from app.pipeline.preprocessing_pipeline import PreprocessingPipeline
from app.pipeline.series_order_pipeline import SeriesOrderPipeline
from app.pipeline.trend_pipeline import TrendPipeline
from app.services.data_storage_service import DataStorageService


def _print_section(title: str) -> None:
    line = "=" * 90
    print(f"\n{line}", flush=True)
    print(f"🚀 {title}", flush=True)
    print(line, flush=True)


def _print_stage_result(title: str, started_at: datetime, status: str) -> None:
    duration_sec = (datetime.now(UTC) - started_at).total_seconds()

    status_icon = "✅" if status == "SUCCESS" else "❌"

    print(
        f"{status_icon} {title}\n"
        f"⏱️  Duration: {duration_sec:.2f} sec\n"
        f"📌 Status   : {status}\n",
        flush=True,
    )


def run_startup_sync() -> None:
    print("\n📡 STARTING BINANCE DATA SYNC...\n", flush=True)

    pipeline = DataCollectorPipeline()
    results = pipeline.run_for_env_symbols_all_intervals()

    total_fetched = sum(item.fetched_rows for item in results)
    total_saved = sum(item.saved_rows for item in results)

    print("\n📊 SYNC SUMMARY")
    print("-" * 60)
    print(f"📦 Tables Processed : {len(results)}")
    print(f"📥 Total Fetched    : {total_fetched}")
    print(f"💾 Total Saved      : {total_saved}", flush=True)


def run_preprocessing_checks() -> None:
    print("\n🧪 STARTING PREPROCESSING CHECKS...\n", flush=True)

    pipeline = PreprocessingPipeline()
    results = pipeline.run_time_series_audit()

    ok_count = len([item for item in results if item.status == "OK"])
    issue_count = len([item for item in results if item.status == "ISSUES"])
    empty_count = len(
        [item for item in results if item.status in {"EMPTY", "MISSING_TABLE"}]
    )

    print("\n📊 PREPROCESSING SUMMARY")
    print("-" * 60)
    print(f"✅ OK               : {ok_count}")
    print(f"⚠️ Issues           : {issue_count}")
    print(f"❌ Empty/Missing    : {empty_count}")

    # 🔥 Final verdict
    print("\n🧠 FINAL DATA QUALITY VERDICT")
    print("-" * 60)

    if issue_count == 0 and empty_count == 0:
        print("✅ DATA IS CLEAN & COMPLETE TIME SERIES\n", flush=True)
    else:
        print(
            f"⚠️ DATA HAS ISSUES → "
            f"(issues={issue_count}, empty/missing={empty_count}, ok={ok_count})\n",
            flush=True,
        )


def run_gap_backfill() -> None:
    print("\n🔧 STARTING GAP BACKFILL & DATA QUALITY...\n", flush=True)

    pipeline = PreprocessingPipeline()
    stats = pipeline.run_gap_backfill()


def run_post_fill_datetime_order_validation() -> None:
    print("\n🗓️ STARTING POST-FILL DATE/TIME ORDER VALIDATION...\n", flush=True)

    raw_stats = DataStorageService.enforce_configured_table_ordering("raw")
    processed_stats = DataStorageService.enforce_configured_table_ordering("processed")

    print("\n📊 POST-FILL ORDER VALIDATION SUMMARY")
    print("-" * 60)
    print(
        "🧱 RAW       | "
        f"checked={raw_stats['tables_checked']} reordered={raw_stats['tables_reordered']} "
        f"time_after={raw_stats['time_violations_after']} id_after={raw_stats['id_inversions_after']}"
    )
    print(
        "🧾 PROCESSED | "
        f"checked={processed_stats['tables_checked']} reordered={processed_stats['tables_reordered']} "
        f"time_after={processed_stats['time_violations_after']} id_after={processed_stats['id_inversions_after']}",
        flush=True,
    )


def run_indicator_enrichment() -> None:
    print("\n📐 STARTING INDICATOR ENRICHMENT...\n", flush=True)

    pipeline = IndicatorPipeline()
    pipeline.run()


def run_pattern_enrichment() -> None:
    print("\n🕯️ STARTING CANDLESTICK PATTERN ENRICHMENT...\n", flush=True)

    pipeline = PatternPipeline()
    pipeline.run()


def run_trend_enrichment() -> None:
    print("\n📈 STARTING TREND HANDLING...\n", flush=True)

    pipeline = TrendPipeline()
    pipeline.run()


def run_chart_pattern_enrichment() -> None:
    print("\n📊 STARTING GEOMETRIC CHART PATTERN DETECTION...\n", flush=True)

    pipeline = ChartPatternPipeline()
    pipeline.run()


def run_series_order_integrity() -> None:
    print("\n🧭 STARTING FINAL SERIES ORDER INTEGRITY CHECK...\n", flush=True)

    pipeline = SeriesOrderPipeline()
    pipeline.run()


def initialize_database_infrastructure() -> None:
    """
    CRITICAL INITIALIZATION: Create database indexes for time-series ordering.
    
    MANDATORY: All raw and processed tables MUST have indexes on open_time_ms
    to guarantee deterministic ordering for downstream calculations.
    
    This runs once at pipeline start, before any data processing begins.
    """
    print("\n" + "=" * 90)
    print("🔧 INITIALIZING DATABASE INFRASTRUCTURE - TIME SERIES INDEXES")
    print("=" * 90, flush=True)
    
    try:
        results = DataStorageService.create_time_series_indexes()
        created_count = len([v for v in results.values() if "created" in v.lower()])
        print(f"\n✅ Index Creation Summary: {created_count} index(es) created/verified")
        for table_name, status in sorted(results.items()):
            if "created" in status.lower():
                print(f"  ✅ {table_name:30s} | {status[:50]}")
        print("=" * 90 + "\n", flush=True)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Failed to create indexes: {str(e)}", flush=True)
        raise


def validate_time_series_ordering_mandatory() -> None:
    """
    MANDATORY PRE-PROCESSING VALIDATION: Strict ordering check.
    
    FAIL-SAFE: If ANY table has ordering violations, pipeline STOPS immediately.
    All indicators (EMA, RSI, MACD, ATR, Bollinger Bands) and patterns depend
    on strict chronological sequence. A single out-of-order record corrupts
    all downstream calculations.
    """
    print("\n" + "=" * 90)
    print("⏱️  CRITICAL VALIDATION: TIME SERIES ORDERING CHECK")
    print("=" * 90, flush=True)
    
    try:
        pipeline = PreprocessingPipeline()
        pipeline.validate_time_series_ordering()
        print("✅ ALL TABLES PASS STRICT ORDERING VALIDATION\n", flush=True)
    except Exception as e:
        print(f"\n❌ FATAL: {str(e)}\n", flush=True)
        raise


def main() -> None:
    overall_started_at = datetime.now(UTC)

    # 🔹 INITIALIZATION: Database infrastructure
    try:
        initialize_database_infrastructure()
    except Exception as e:
        print(f"❌ INITIALIZATION FAILED: Pipeline cannot proceed: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 1
    _print_section("PIPELINE 1/9: RAW DATA COLLECTION SYNC")
    stage_started_at = datetime.now(UTC)

    try:
        run_startup_sync()
        _print_stage_result("RAW DATA COLLECTION SYNC", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("RAW DATA COLLECTION SYNC", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 INITIAL VALIDATION AFTER RAW DATA EXISTS
    try:
        validate_time_series_ordering_mandatory()
    except Exception as e:
        print(f"❌ POST-COLLECTION VALIDATION FAILED: Pipeline cannot proceed: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 2
    _print_section("PIPELINE 2/9: TIME-SERIES QUALITY PREPROCESSING")
    stage_started_at = datetime.now(UTC)

    try:
        run_preprocessing_checks()
        _print_stage_result("TIME-SERIES QUALITY PREPROCESSING", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("TIME-SERIES QUALITY PREPROCESSING", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 3
    _print_section("PIPELINE 3/9: GAP BACKFILL & DATA QUALITY")
    stage_started_at = datetime.now(UTC)

    try:
        run_gap_backfill()
        _print_stage_result("GAP BACKFILL & DATA QUALITY", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("GAP BACKFILL & DATA QUALITY", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 4
    _print_section("PIPELINE 4/9: POST-FILL DATE/TIME ORDER VALIDATION")
    stage_started_at = datetime.now(UTC)

    try:
        run_post_fill_datetime_order_validation()
        _print_stage_result("POST-FILL DATE/TIME ORDER VALIDATION", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("POST-FILL DATE/TIME ORDER VALIDATION", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 5
    _print_section("PIPELINE 5/9: INDICATOR ENRICHMENT")
    stage_started_at = datetime.now(UTC)

    try:
        run_indicator_enrichment()
        _print_stage_result("INDICATOR ENRICHMENT", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("INDICATOR ENRICHMENT", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 6
    _print_section("PIPELINE 6/9: CANDLESTICK PATTERN ENRICHMENT")
    stage_started_at = datetime.now(UTC)

    try:
        run_pattern_enrichment()
        _print_stage_result("CANDLESTICK PATTERN ENRICHMENT", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("CANDLESTICK PATTERN ENRICHMENT", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 7
    _print_section("PIPELINE 7/9: TREND HANDLING")
    stage_started_at = datetime.now(UTC)

    try:
        run_trend_enrichment()
        _print_stage_result("TREND HANDLING", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("TREND HANDLING", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 8
    _print_section("PIPELINE 8/9: GEOMETRIC CHART PATTERNS")
    stage_started_at = datetime.now(UTC)

    try:
        run_chart_pattern_enrichment()
        _print_stage_result("GEOMETRIC CHART PATTERNS", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("GEOMETRIC CHART PATTERNS", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 PIPELINE 9
    _print_section("PIPELINE 9/9: SERIES ORDER INTEGRITY")
    stage_started_at = datetime.now(UTC)

    try:
        run_series_order_integrity()
        _print_stage_result("SERIES ORDER INTEGRITY", stage_started_at, "SUCCESS")
    except Exception as e:
        _print_stage_result("SERIES ORDER INTEGRITY", stage_started_at, "FAILED")
        print(f"❌ ERROR: {str(e)}\n", flush=True)
        raise

    # 🔹 FINAL
    _print_section("🎉 ALL PIPELINES COMPLETED")
    _print_stage_result("FULL MAIN WORKFLOW", overall_started_at, "SUCCESS")


if __name__ == "__main__":
    main()