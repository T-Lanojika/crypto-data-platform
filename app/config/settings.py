import os
import re

from dotenv import load_dotenv

load_dotenv()


def _parse_csv_env(env_name: str, default: str) -> list[str]:
	value = os.getenv(env_name, default)
	return [item.strip() for item in value.split(",") if item.strip()]


def normalize_symbol(symbol: str) -> str:
	return re.sub(r"[^A-Za-z0-9]", "", symbol).upper()


BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
BINANCE_KLINE_LIMIT = int(os.getenv("BINANCE_KLINE_LIMIT", "1000"))
BINANCE_FULL_HISTORY_SYNC = os.getenv("BINANCE_FULL_HISTORY_SYNC", "true").lower() in {
	"1",
	"true",
	"yes",
	"y",
	"on",
}
BINANCE_SYMBOLS = [
	normalize_symbol(symbol)
	for symbol in _parse_csv_env("BINANCE_SYMBOLS", "BTC/USDT")
]
BINANCE_INTERVALS = _parse_csv_env(
	"BINANCE_INTERVALS",
	"1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M",
)

GAP_UNRELIABLE_THRESHOLD_HOURS = int(os.getenv("GAP_UNRELIABLE_THRESHOLD_HOURS", "24"))
