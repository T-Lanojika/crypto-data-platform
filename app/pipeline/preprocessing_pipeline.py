from datetime import UTC, datetime

from app.config.settings import (
    BINANCE_INTERVALS,
    BINANCE_SYMBOLS,
    GAP_UNRELIABLE_THRESHOLD_HOURS,
)
from app.services.data_storage_service import DataStorageService
from app.services.gap_classifier_service import GapClassifierService
from app.services.gap_detection_service import INTERVAL_TO_MS, GapDetectionService
from app.services.gap_filler_service import GapFillerService
from app.services.preprocessing_service import PreprocessingService, TimeSeriesAuditResult


class PreprocessingPipeline:
    """Runs preprocessing checks before feature engineering steps."""

    def validate_time_series_ordering(self) -> bool:
        """
        MANDATORY VALIDATION: Verify strict ascending order by open_time_ms
        for ALL raw tables before any preprocessing begins.
        
        AUTO-FIX: If a table has inversions, automatically reorder it.
        FAIL-SAFE: If reordering fails, raise exception and halt pipeline.
        
        Returns:
            True if all tables pass validation (after reordering if needed), 
            raises exception otherwise.
        """
        print("\n" + "=" * 80)
        print("⏱️  VALIDATING TIME SERIES ORDERING - CRITICAL CHECK")
        print("=" * 80, flush=True)
        
        has_failures = False
        total_checks = len(BINANCE_SYMBOLS) * len(BINANCE_INTERVALS)
        passed = 0
        failed = 0
        reordered = 0
        
        for symbol in BINANCE_SYMBOLS:
            for interval in BINANCE_INTERVALS:
                table_name = DataStorageService.build_raw_table_name(
                    symbol=symbol, interval=interval
                )
                
                is_ordered, inversion_count, message = DataStorageService.validate_ordering(table_name)
                
                if is_ordered:
                    status_icon = "✅"
                    print(f"{status_icon} {symbol.upper():8s} | {interval:5s} | {message}", flush=True)
                    passed += 1
                else:
                    # AUTO-FIX: Reorder the table
                    status_icon = "🔄"
                    print(f"{status_icon} {symbol.upper():8s} | {interval:5s} | REORDERING... ({inversion_count} inversions)", flush=True)
                    
                    try:
                        DataStorageService.reorder_raw_table_by_time(table_name)
                        
                        # Validate again after reordering
                        is_ordered_after, inv_after, msg_after = DataStorageService.validate_ordering(table_name)
                        
                        if is_ordered_after:
                            print(f"   ✅ Reorder successful | {msg_after}", flush=True)
                            passed += 1
                            reordered += 1
                        else:
                            print(f"   ❌ Reorder FAILED - still has {inv_after} inversions", flush=True)
                            failed += 1
                            has_failures = True
                    except Exception as e:
                        print(f"   ❌ Reorder ERROR: {str(e)[:60]}", flush=True)
                        failed += 1
                        has_failures = True
        
        print("-" * 80)
        print(f"✅ Passed: {passed}/{total_checks} | 🔄 Reordered: {reordered} | ❌ Failed: {failed}/{total_checks}")
        print("=" * 80, flush=True)
        
        if has_failures:
            raise RuntimeError(
                f"CRITICAL: {failed} table(s) have ordering violations that could not be fixed. "
                "Pipeline MUST NOT continue. All indicators and patterns depend on "
                "strict chronological ordering. Manual intervention required."
            )
        
        return True

    def ensure_processed_tables_ordered_before_indicators(self) -> bool:
        """
        CRITICAL PRE-INDICATOR ENFORCEMENT: Reorder all processed tables before 
        ANY indicators, patterns, or calculations are applied.
        
        This runs AFTER raw->processed sync but BEFORE indicator computation.
        All indicators (EMA, RSI, MACD, ATR) depend on strict chronological order.
        
        FAIL-SAFE: If ANY table cannot be reordered, pipeline halts.
        
        Returns:
            True if all processed tables are ordered, raises exception otherwise.
        """
        print("\n" + "=" * 80)
        print("🔒 CRITICAL: ENSURING PROCESSED TABLES ORDERED BEFORE INDICATORS")
        print("=" * 80, flush=True)
        
        has_failures = False
        total_tables = len(BINANCE_SYMBOLS) * len(BINANCE_INTERVALS)
        passed = 0
        reordered = 0
        failed = 0
        
        for symbol in BINANCE_SYMBOLS:
            for interval in BINANCE_INTERVALS:
                table_name = DataStorageService.build_processed_table_name(
                    symbol=symbol, interval=interval
                )
                
                # Check if table exists
                try:
                    is_ordered, inversion_count, message = DataStorageService.validate_ordering(table_name)
                    
                    if is_ordered:
                        status_icon = "✅"
                        print(f"{status_icon} {symbol.upper():8s} | {interval:5s} | {message}", flush=True)
                        passed += 1
                    else:
                        # AUTO-REORDER processed table
                        status_icon = "🔄"
                        print(f"{status_icon} {symbol.upper():8s} | {interval:5s} | REORDERING... ({inversion_count} inversions)", flush=True)
                        
                        try:
                            DataStorageService.reorder_processed_table_by_time(table_name)
                            
                            # Validate after reordering
                            is_ordered_after, inv_after, msg_after = DataStorageService.validate_ordering(table_name)
                            
                            if is_ordered_after:
                                print(f"   ✅ Reorder successful | {msg_after}", flush=True)
                                passed += 1
                                reordered += 1
                            else:
                                print(f"   ❌ Reorder FAILED - still has {inv_after} inversions", flush=True)
                                failed += 1
                                has_failures = True
                        except Exception as e:
                            print(f"   ❌ Reorder ERROR: {str(e)[:60]}", flush=True)
                            failed += 1
                            has_failures = True
                except Exception as e:
                    # Table may not exist yet - skip
                    pass
        
        print("-" * 80)
        print(f"✅ Passed: {passed} | 🔄 Reordered: {reordered} | ❌ Failed: {failed}")
        print("=" * 80, flush=True)
        
        if has_failures:
            raise RuntimeError(
                f"CRITICAL: {failed} processed table(s) could not be reordered before indicators. "
                "Pipeline MUST NOT continue. Indicators require strict chronological order."
            )
        
        return True

    def run_time_series_audit(self) -> list[TimeSeriesAuditResult]:
        results: list[TimeSeriesAuditResult] = []
        total_checks = len(BINANCE_SYMBOLS) * len(BINANCE_INTERVALS)
        current_check = 0

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

        print("\n" + "=" * 80)
        print("🧪 START PREPROCESSING - TIME SERIES AUDIT")
        print(f"🔢 Total Checks: {total_checks}")
        print("=" * 80, flush=True)

        for symbol in BINANCE_SYMBOLS:
            print(f"\n🎯 SYMBOL: {symbol.upper()}")
            print("-" * 80, flush=True)

            for interval in BINANCE_INTERVALS:
                current_check += 1
                table_name = DataStorageService.build_raw_table_name(
                    symbol=symbol, interval=interval
                )

                print(
                    f"\n🔍 CHECK {current_check}/{total_checks} | {symbol.upper()} | {interval}",
                    flush=True,
                )
                print(f"📦 Table: {table_name}", flush=True)

                result = PreprocessingService.audit_time_series(
                    symbol=symbol,
                    interval=interval,
                    table_name=table_name,
                )

                results.append(result)

                # Status indicator
                if result.status == "OK":
                    status_icon = "✅"
                elif result.status == "ISSUES":
                    status_icon = "⚠️"
                else:
                    status_icon = "❌"

                print(f"{status_icon} Status        : {result.status}")
                print(f"📊 Rows          : {result.row_count}")
                print(f"🕒 Start DateTime: {result.start_open_time_utc}")
                print(f"🕒 End DateTime  : {result.end_open_time_utc}")
                print(f"⚠️  Gap Events    : {result.gap_event_count}")
                print(f"📉 Missing Candles: {result.estimated_missing_candles}")
                if result.missing_ranges_utc:
                    print("🧩 Missing Ranges (UTC):")
                    max_ranges_to_print = len(result.missing_ranges_utc)
                    for idx, missing_range in enumerate(result.missing_ranges_utc[:max_ranges_to_print], start=1):
                        print(f"   {idx}. {missing_range}")
                    if len(result.missing_ranges_utc) > max_ranges_to_print:
                        remaining = len(result.missing_ranges_utc) - max_ranges_to_print
                        print(f"   ... and {remaining} more missing range(s)")
                print(f"📝 Message       : {result.message}", flush=True)

        # 🔹 Summary
        ok_count = len([item for item in results if item.status == "OK"])
        issue_count = len([item for item in results if item.status == "ISSUES"])
        empty_count = len(
            [item for item in results if item.status in {"EMPTY", "MISSING_TABLE"}]
        )

        print("\n" + "=" * 80)
        print("📊 AUDIT SUMMARY")
        print("-" * 80)
        print(f"🔢 Total Checks        : {len(results)}")
        print(f"✅ OK                 : {ok_count}")
        print(f"⚠️ Issues             : {issue_count}")
        print(f"❌ Empty/Missing      : {empty_count}")
        print("=" * 80 + "\n", flush=True)

        return results

    def run_gap_backfill(self) -> dict:
        """Fills gaps using multi-timeframe backfill and synthetic candle generation."""
        backfill_stats = {
            "total_gaps_detected": 0,
            "total_gaps_filled": 0,
            "total_synthetic_candles_generated": 0,
            "total_placeholder_rows_created": 0,
            "total_linear_interpolated_rows": 0,
            "symbols_processed": 0,
            "failures": [],
        }

        print("\n" + "=" * 80)
        print("🔧 START PREPROCESSING - GAP BACKFILL & SYNTHETIC GENERATION")
        print(f"🎯 Symbols: {', '.join([s.upper() for s in BINANCE_SYMBOLS])}")
        print("=" * 80, flush=True)

        print("🔒 PRE-CHECK: Enforcing RAW table ordering integrity ...", flush=True)
        raw_stats = DataStorageService.enforce_configured_table_ordering("raw")
        print(
            "   ✔ RAW ordering check complete | "
            f"checked={raw_stats['tables_checked']} "
            f"reordered={raw_stats['tables_reordered']} "
            f"time_after={raw_stats['time_violations_after']} "
            f"id_after={raw_stats['id_inversions_after']}",
            flush=True,
        )

        # 🔒 CRITICAL: Ensure all processed tables ordered BEFORE any processing
        try:
            self.ensure_processed_tables_ordered_before_indicators()
        except Exception as e:
            print(f"❌ PRE-INDICATOR ORDERING FAILED: {str(e)}\n", flush=True)
            raise

        for symbol in BINANCE_SYMBOLS:
            print(f"\n🎯 SYMBOL: {symbol.upper()}")
            print("-" * 80, flush=True)
            backfill_stats["symbols_processed"] += 1

            for interval in BINANCE_INTERVALS:
                raw_table_name = DataStorageService.build_raw_table_name(
                    symbol=symbol, interval=interval
                )
                processed_table_name = DataStorageService.build_processed_table_name(
                    symbol=symbol, interval=interval
                )

                print(f"\n🔍 Processing {symbol.upper()} | {interval}")
                print(f"📦 Raw Table      : {raw_table_name}")
                print(f"📦 Processed Table: {processed_table_name}")

                try:
                    if not DataStorageService.table_exists(raw_table_name):
                        print(
                            "⚠️  Raw table missing — downloading Binance data before sync ...",
                            flush=True,
                        )
                        from app.pipeline.data_collector import DataCollectorPipeline

                        DataCollectorPipeline().run(symbol=symbol, interval=interval)

                    # Ensure processed table exists
                    DataStorageService.create_processed_table(
                        table_name=processed_table_name,
                        symbol=symbol,
                        interval=interval,
                    )

                    # Always sync raw rows first so processed has all source data.
                    DataStorageService.merge_raw_to_processed(
                        raw_table_name=raw_table_name,
                        processed_table_name=processed_table_name,
                    )

                    # Detect gaps in raw data
                    gaps = GapDetectionService.find_gaps(
                        table_name=raw_table_name, interval=interval
                    )
                    backfill_stats["total_gaps_detected"] += len(gaps)

                    if not gaps:
                        print("✅ No gaps found - processed table synced from raw", flush=True)
                        interpolated_rows = self._interpolate_blank_missing_prices(
                            table_name=processed_table_name
                        )
                        if interpolated_rows > 0:
                            print(
                                f"📈 Linear interpolation filled {interpolated_rows} row(s)",
                                flush=True,
                            )
                        backfill_stats["total_linear_interpolated_rows"] += interpolated_rows
                        DataStorageService.reorder_processed_table_by_time(
                            table_name=processed_table_name
                        )
                        continue

                    print(f"⚠️  {len(gaps)} gap(s) detected - attempting backfill", flush=True)

                    gaps_filled = 0
                    for gap_idx, gap in enumerate(gaps, start=1):
                        print(f"\n   Gap {gap_idx}/{len(gaps)}")
                        print(f"   📍 Start: {gap.start_time_utc} | End: {gap.end_time_utc}")
                        print(f"   📊 Missing: {gap.missing_candle_count} candles")

                        missing_open_times = self._build_missing_open_time_ms(gap, interval)
                        if not missing_open_times:
                            continue

                        synthetic_by_open_time: dict[int, dict] = {}

                        # Get larger timeframe for backfill
                        larger_interval = GapDetectionService.get_larger_timeframe_for_backfill(interval)

                        if larger_interval:
                            larger_table_name = DataStorageService.build_raw_table_name(
                                symbol=symbol, interval=larger_interval
                            )
                            if self._table_exists(larger_table_name):
                                print(f"   🔄 Using {larger_interval} data for backfill")

                                larger_df = GapFillerService.fetch_larger_timeframe_data(
                                    symbol=symbol,
                                    larger_interval=larger_interval,
                                    table_name=larger_table_name,
                                    gap_start_ms=gap.start_open_time_ms,
                                    gap_end_ms=gap.end_open_time_ms,
                                )

                                if not larger_df.empty:
                                    print(f"   📥 Found {len(larger_df)} candle(s) in {larger_interval}")
                                    ratio = GapDetectionService.get_ratio(interval, larger_interval)
                                    if ratio > 0:
                                        for _, larger_row in larger_df.iterrows():
                                            split_candles = GapFillerService.split_candle_into_smaller_intervals(
                                                symbol=symbol,
                                                larger_candle=larger_row,
                                                smaller_interval=interval,
                                                larger_interval=larger_interval,
                                                ratio=ratio,
                                            )
                                            for candle in split_candles:
                                                if candle.open_time_ms in missing_open_times:
                                                    synthetic_by_open_time[candle.open_time_ms] = {
                                                        "open_time": candle.open_time,
                                                        "open": candle.open,
                                                        "high": candle.high,
                                                        "low": candle.low,
                                                        "close": candle.close,
                                                        "volume": candle.volume,
                                                        "close_time": candle.close_time,
                                                        "quote_asset_volume": candle.quote_asset_volume,
                                                        "number_of_trades": candle.number_of_trades,
                                                        "taker_buy_base_asset_volume": candle.taker_buy_base_asset_volume,
                                                        "taker_buy_quote_asset_volume": candle.taker_buy_quote_asset_volume,
                                                        "ignore_value": candle.ignore_value,
                                                        "open_time_ms": candle.open_time_ms,
                                                        "close_time_ms": candle.close_time_ms,
                                                        "is_synthetic": True,
                                                        "is_missing": False,
                                                        "is_interpolated": True,
                                                        "fill_source": f"synthetic_from_{larger_interval}",
                                                        "fill_method": f"synthetic_from_{larger_interval}",
                                                        "confidence_score": 0.7,
                                                    }
                            else:
                                print(f"   ⚠️  {larger_interval} table not collected - using blank rows")

                        upsert_rows: list[dict] = []
                        placeholder_count = 0
                        for open_time_ms in missing_open_times:
                            if open_time_ms in synthetic_by_open_time:
                                upsert_rows.append(synthetic_by_open_time[open_time_ms])
                                continue

                            close_time_ms = open_time_ms + INTERVAL_TO_MS[interval] - 1
                            upsert_rows.append(
                                {
                                    "open_time": datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
                                    "open": None,
                                    "high": None,
                                    "low": None,
                                    "close": None,
                                    "volume": 0.0,
                                    "close_time": datetime.fromtimestamp(close_time_ms / 1000, tz=UTC),
                                    "quote_asset_volume": 0.0,
                                    "number_of_trades": 0,
                                    "taker_buy_base_asset_volume": 0.0,
                                    "taker_buy_quote_asset_volume": 0.0,
                                    "ignore_value": 0.0,
                                    "open_time_ms": open_time_ms,
                                    "close_time_ms": close_time_ms,
                                    "is_synthetic": False,
                                    "is_missing": True,
                                    "is_interpolated": False,
                                    "fill_source": "blank_placeholder",
                                    "fill_method": "blank_placeholder",
                                    "confidence_score": 0.0,
                                }
                            )
                            placeholder_count += 1

                        upsert_rows.sort(key=lambda item: int(item["open_time_ms"]))

                        inserted_count = DataStorageService.upsert_processed_rows(
                            table_name=processed_table_name,
                            rows=upsert_rows,
                        )

                        synthetic_count = len(upsert_rows) - placeholder_count
                        backfill_stats["total_synthetic_candles_generated"] += synthetic_count
                        backfill_stats["total_placeholder_rows_created"] += placeholder_count

                        if inserted_count > 0:
                            gaps_filled += 1

                        print(
                            f"   ✅ Gap rows upserted: {inserted_count} (synthetic={synthetic_count}, blank={placeholder_count})",
                            flush=True,
                        )

                    print(f"✅ Filled {gaps_filled}/{len(gaps)} gaps")
                    backfill_stats["total_gaps_filled"] += gaps_filled

                    # Re-sync raw rows to ensure late-arriving raw data overwrites placeholders.
                    DataStorageService.merge_raw_to_processed(
                        raw_table_name=raw_table_name,
                        processed_table_name=processed_table_name,
                    )

                    interpolated_rows = self._interpolate_blank_missing_prices(
                        table_name=processed_table_name
                    )
                    backfill_stats["total_linear_interpolated_rows"] += interpolated_rows
                    if interpolated_rows > 0:
                        print(
                            f"📈 Linear interpolation filled {interpolated_rows} row(s)",
                            flush=True,
                        )

                    DataStorageService.reorder_processed_table_by_time(
                        table_name=processed_table_name
                    )

                except Exception as e:
                    error_msg = f"{symbol}/{interval}: {str(e)}"
                    print(f"❌ Error: {error_msg}", flush=True)
                    backfill_stats["failures"].append(error_msg)

        # Summary
        print("\n" + "=" * 80)
        print("📊 GAP BACKFILL SUMMARY")
        print("-" * 80)
        print(f"🔢 Symbols Processed        : {backfill_stats['symbols_processed']}")
        print(f"⚠️  Total Gaps Detected      : {backfill_stats['total_gaps_detected']}")
        print(f"✅ Gaps Filled              : {backfill_stats['total_gaps_filled']}")
        print(f"🔄 Synthetic Candles Created: {backfill_stats['total_synthetic_candles_generated']}")
        print(f"🏷️  Blank Placeholder Rows   : {backfill_stats['total_placeholder_rows_created']}")
        print(f"📈 Linearly Filled Rows     : {backfill_stats['total_linear_interpolated_rows']}")
        if backfill_stats["failures"]:
            print(f"❌ Failures                 : {len(backfill_stats['failures'])}")
            for failure in backfill_stats["failures"]:
                print(f"   - {failure}")
        print("=" * 80 + "\n", flush=True)

        return backfill_stats

    @staticmethod
    def _table_exists(table_name: str) -> bool:
        from sqlalchemy import text

        from app.config.database import engine

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
    def _build_missing_open_time_ms(gap, interval: str) -> list[int]:
        interval_ms = INTERVAL_TO_MS.get(interval, 0)
        if interval_ms <= 0:
            return []

        missing: list[int] = []
        for idx in range(max(0, int(gap.missing_candle_count))):
            missing.append(gap.start_open_time_ms + (idx * interval_ms))

        return missing

    @staticmethod
    def _interpolate_blank_missing_prices(table_name: str) -> int:
        """Fill blank missing rows linearly between known neighbor prices."""
        from sqlalchemy import text

        from app.config.database import engine

        fetch_sql = text(
            f"""
            SELECT open_time_ms, open, close, is_missing
            FROM {table_name}
            ORDER BY open_time_ms
            """
        )

        with engine.begin() as connection:
            rows = connection.execute(fetch_sql).mappings().all()

        if not rows:
            return 0

        values: list[dict] = [dict(row) for row in rows]
        update_payload: list[dict] = []

        idx = 0
        total_rows = len(values)
        while idx < total_rows:
            row = values[idx]
            is_blank_missing = (
                bool(row["is_missing"])
                and row["open"] is None
                and row["close"] is None
            )
            if not is_blank_missing:
                idx += 1
                continue

            block_start = idx
            while idx < total_rows:
                block_row = values[idx]
                if not (
                    bool(block_row["is_missing"])
                    and block_row["open"] is None
                    and block_row["close"] is None
                ):
                    break
                idx += 1
            block_end = idx - 1

            prev_idx = block_start - 1
            next_idx = block_end + 1
            if prev_idx < 0 or next_idx >= total_rows:
                continue

            prev_row = values[prev_idx]
            next_row = values[next_idx]

            left_price = prev_row["close"] if prev_row["close"] is not None else prev_row["open"]
            right_price = next_row["open"] if next_row["open"] is not None else next_row["close"]
            if left_price is None or right_price is None:
                continue

            missing_count = block_end - block_start + 1
            left_price = float(left_price)
            right_price = float(right_price)

            interval_ms = 0
            if missing_count > 0:
                next_open = int(values[next_idx]["open_time_ms"])
                prev_open = int(values[prev_idx]["open_time_ms"])
                interval_ms = max(1, (next_open - prev_open) // (missing_count + 1))

            classification = GapClassifierService.classify(
                start_price=left_price,
                end_price=right_price,
                missing_count=missing_count,
                interval_ms=interval_ms,
                unreliable_threshold_hours=GAP_UNRELIABLE_THRESHOLD_HOURS,
            )

            for pos in range(missing_count):
                step = pos + 1
                target_row = values[block_start + pos]
                if classification.fill_method == "unreliable_gap":
                    row_open = None
                    row_close = None
                    row_high = None
                    row_low = None
                elif classification.fill_method == "flat_fill":
                    row_open = left_price
                    row_close = left_price
                    row_high = left_price
                    row_low = left_price
                else:
                    prev_step = step - 1
                    row_open = left_price + ((right_price - left_price) * (prev_step / (missing_count + 1)))
                    row_close = left_price + ((right_price - left_price) * (step / (missing_count + 1)))
                    row_high = max(row_open, row_close)
                    row_low = min(row_open, row_close)

                update_payload.append(
                    {
                        "open_time_ms": int(target_row["open_time_ms"]),
                        "open": row_open,
                        "high": row_high,
                        "low": row_low,
                        "close": row_close,
                        "is_interpolated": classification.is_interpolated,
                        "fill_method": classification.fill_method,
                        "confidence_score": classification.confidence_score,
                    }
                )

        if not update_payload:
            return 0

        update_sql = text(
            f"""
            UPDATE {table_name}
            SET
                                open = :open,
                                high = :high,
                                low = :low,
                                close = :close,
                volume = 0.0,
                quote_asset_volume = 0.0,
                number_of_trades = 0,
                taker_buy_base_asset_volume = 0.0,
                taker_buy_quote_asset_volume = 0.0,
                ignore_value = 0.0,
                                is_interpolated = :is_interpolated,
                                fill_source = :fill_method,
                                fill_method = :fill_method,
                                confidence_score = :confidence_score,
                updated_at = NOW()
            WHERE open_time_ms = :open_time_ms
              AND is_missing = TRUE
            """
        )

        with engine.begin() as connection:
            connection.execute(update_sql, update_payload)

        return len(update_payload)