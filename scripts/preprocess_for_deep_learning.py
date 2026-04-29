from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.config.database import engine

TABLES: Dict[str, str] = {
    "15m": "btcusdt_15m_processed",
    "1h": "btcusdt_1h_processed",
    "4h": "btcusdt_4h_processed",
    "1d": "btcusdt_1d_processed",
    "1w": "btcusdt_1w_processed",
    "1mo": "btcusdt_1mo_processed",
}

DROP_COLUMNS = {
    "candle_pattern_1",
    "candle_pattern_2",
    "candle_pattern_3",
    "candle_pattern_complex",
    "chart_pattern_key_points",
}

KEEP_FEATURES = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "rsi_14",
    "macd",
    "macd_signal",
    "macd_hist",
    "ema_12",
    "ema_26",
    "atr_14",
    "adx_14",
    "mfi_14",
    "trend_pct_change",
    "vix",
    "volume_oscillator",
    "support",
    "resistance",
]

OUTPUT_MAP = {
    "15m": "cleaned_15m.csv",
    "1h": "cleaned_1h.csv",
    "4h": "cleaned_4h.csv",
}


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def load_table(table_name: str) -> pd.DataFrame:
    query = text(f"SELECT * FROM {quote_ident(table_name)}")
    return pd.read_sql(query, engine)


def ensure_open_time_ms(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "open_time_ms" in out.columns:
        out["open_time_ms"] = pd.to_numeric(out["open_time_ms"], errors="coerce")
    elif "open_time" in out.columns:
        ts = pd.to_datetime(out["open_time"], utc=True, errors="coerce")
        out["open_time_ms"] = (ts.view("int64") // 1_000_000).astype("float64")
    else:
        raise ValueError("No time column found. Expected open_time_ms or open_time.")

    out = out.dropna(subset=["open_time_ms"]).copy()
    out["open_time_ms"] = out["open_time_ms"].astype("int64")
    out = out.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="last")
    return out


def drop_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns], errors="ignore").copy()

    non_numeric_cols = [
        c
        for c in out.columns
        if c != "open_time_ms" and not pd.api.types.is_numeric_dtype(out[c])
    ]
    out = out.drop(columns=non_numeric_cols, errors="ignore")

    return out


def keep_relevant_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["open_time_ms"] + [c for c in KEEP_FEATURES if c in df.columns]
    return df.loc[:, cols].copy()


def apply_missing_value_policy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    numeric_cols = [c for c in out.columns if c != "open_time_ms"]
    drop_cols: List[str] = []

    for col in numeric_cols:
        missing_pct = float(out[col].isna().mean() * 100.0)

        if missing_pct > 40.0:
            drop_cols.append(col)
            continue

        if missing_pct < 5.0:
            out[col] = out[col].ffill()
        elif missing_pct <= 20.0:
            out[col] = out[col].interpolate(method="linear", limit_direction="both")
        else:
            # For 20-40%, apply interpolation plus edge filling.
            out[col] = out[col].interpolate(method="linear", limit_direction="both")
            out[col] = out[col].ffill().bfill()

    if drop_cols:
        out = out.drop(columns=drop_cols)

    return out


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["return"] = np.log(out["close"] / out["close"].shift(1))
    out["high_low_range"] = out["high"] - out["low"]
    out["body_size"] = (out["close"] - out["open"]).abs()

    return out


def finalize_numeric_dataset(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.sort_values("open_time_ms").reset_index(drop=True)
    out = out.ffill().bfill()
    out = out.dropna(axis=0, how="any")

    if out.empty:
        raise ValueError("Dataset became empty after cleaning. Check source data quality.")

    return out


def clean_single_timeframe(table_name: str) -> pd.DataFrame:
    df = load_table(table_name)
    df = ensure_open_time_ms(df)
    df = drop_unwanted_columns(df)
    df = keep_relevant_columns(df)
    df = apply_missing_value_policy(df)
    df = engineer_features(df)
    df = finalize_numeric_dataset(df)
    return df


def align_multi_timeframe(base_15m: pd.DataFrame, tf_1h: pd.DataFrame, tf_4h: pd.DataFrame) -> pd.DataFrame:
    base = base_15m.copy()
    one_h = tf_1h.copy()
    four_h = tf_4h.copy()

    base["_ts"] = pd.to_datetime(base["open_time_ms"], unit="ms", utc=True)
    one_h["_ts"] = pd.to_datetime(one_h["open_time_ms"], unit="ms", utc=True)
    four_h["_ts"] = pd.to_datetime(four_h["open_time_ms"], unit="ms", utc=True)

    one_h_cols = [c for c in one_h.columns if c not in {"open_time_ms", "_ts"}]
    four_h_cols = [c for c in four_h.columns if c not in {"open_time_ms", "_ts"}]

    one_h = one_h[["_ts"] + one_h_cols].rename(columns={c: f"{c}_1h" for c in one_h_cols})
    four_h = four_h[["_ts"] + four_h_cols].rename(columns={c: f"{c}_4h" for c in four_h_cols})

    merged = pd.merge_asof(base.sort_values("_ts"), one_h.sort_values("_ts"), on="_ts", direction="backward")
    merged = pd.merge_asof(merged.sort_values("_ts"), four_h.sort_values("_ts"), on="_ts", direction="backward")

    merged = merged.drop(columns=["_ts"]) 
    merged = merged.ffill().bfill()

    for col in merged.columns:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    merged = merged.dropna(axis=0, how="any").sort_values("open_time_ms").reset_index(drop=True)
    return merged


def export_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def run_pipeline(output_dir: Path | None = None) -> Dict[str, Path]:
    if output_dir is None:
        output_dir = Path("data/processed")

    cleaned: Dict[str, pd.DataFrame] = {}
    for tf, table in TABLES.items():
        cleaned[tf] = clean_single_timeframe(table)

    merged = align_multi_timeframe(cleaned["15m"], cleaned["1h"], cleaned["4h"])

    exported_paths: Dict[str, Path] = {}
    for tf, filename in OUTPUT_MAP.items():
        path = output_dir / filename
        export_csv(cleaned[tf], path)
        exported_paths[f"cleaned_{tf}"] = path

    merged_path = output_dir / "merged_multi_timeframe.csv"
    export_csv(merged, merged_path)
    exported_paths["merged_multi_timeframe"] = merged_path

    return exported_paths


if __name__ == "__main__":
    outputs = run_pipeline()
    print("Export completed:")
    for name, path in outputs.items():
        print(f"- {name}: {path}")
