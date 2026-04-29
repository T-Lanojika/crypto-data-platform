import argparse
import os
import sys

if __package__ in (None, ""):
	project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	if project_root not in sys.path:
		sys.path.insert(0, project_root)

from app.config.settings import BINANCE_SYMBOLS, normalize_symbol
from app.pipeline.data_collector import DataCollectorPipeline


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Sync Binance raw data to Postgres for all configured timeframes")
	parser.add_argument(
		"--symbols",
		default=None,
		help="Optional comma separated symbols, e.g. BTC/USDT,ETH/USDT. If omitted, uses BINANCE_SYMBOLS from .env",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	pipeline = DataCollectorPipeline()

	if args.symbols:
		symbols = [normalize_symbol(item) for item in args.symbols.split(",") if item.strip()]
		results = []
		for symbol in symbols:
			results.extend(pipeline.run_for_symbol_all_intervals(symbol))
	else:
		results = pipeline.run_for_env_symbols_all_intervals()

	total_fetched = sum(item.fetched_rows for item in results)
	total_saved = sum(item.saved_rows for item in results)

	print(
		"Collection completed "
		f"symbols={','.join(BINANCE_SYMBOLS) if not args.symbols else args.symbols} "
		f"intervals_synced={len(results)} fetched_rows={total_fetched} saved_rows={total_saved}"
	)
	for item in results:
		print(
			f"table={item.table_name} symbol={item.symbol} interval={item.interval} "
			f"fetched_rows={item.fetched_rows} saved_rows={item.saved_rows}"
		)


if __name__ == "__main__":
	main()
