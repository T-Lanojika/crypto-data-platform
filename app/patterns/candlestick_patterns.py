import pandas as pd
import numpy as np
import talib

# ==============================
# CONFIGURATION
# ==============================

CATEGORY_MAP = {
    "candle_pattern_1": [
        "CDLDOJI",
        "CDLHAMMER",
        "CDLINVERTEDHAMMER",
        "CDLLONGLEGGEDDOJI",
        "CDLSHOOTINGSTAR"
    ],
    "candle_pattern_2": [
        "CDLENGULFING",
        "CDLPIERCING",
        "CDLDARKCLOUDCOVER",
        "CDLMORNINGDOJISTAR"
    ],
    "candle_pattern_3": [
        "CDLMORNINGSTAR",
        "CDLEVENINGSTAR",
        "CDLTRISTAR"
    ],
    "candle_pattern_complex": [
        "CDL3WHITESOLDIERS",
        "CDL3BLACKCROWS"
    ],
}

# Only these patterns will include Bullish/Bearish prefix
IMPORTANT_PATTERNS = ["CDLENGULFING", "CDLMARUBOZU"]


# ==============================
# CORE FUNCTION
# ==============================

def compute_candlestick_patterns(df: pd.DataFrame, debug: bool = True) -> pd.DataFrame:
    """
    Compute candlestick patterns with:
    - Single best pattern per category
    - Selective Bullish/Bearish labeling
    - Optimized TA-Lib calls
    """

    open_p = pd.to_numeric(df["open"], errors="coerce").values
    high_p = pd.to_numeric(df["high"], errors="coerce").values
    low_p = pd.to_numeric(df["low"], errors="coerce").values
    close_p = pd.to_numeric(df["close"], errors="coerce").values

    out = pd.DataFrame(index=df.index)

    # ==============================
    # STEP 1: PRECOMPUTE ALL PATTERNS (FAST)
    # ==============================
    pattern_results = {}

    if debug:
        print("\n🚀 Precomputing TA-Lib patterns...\n")

    for patterns in CATEGORY_MAP.values():
        for pattern_name in patterns:
            if pattern_name not in pattern_results and hasattr(talib, pattern_name):
                func = getattr(talib, pattern_name)
                pattern_results[pattern_name] = func(open_p, high_p, low_p, close_p)

                if debug:
                    print(f"✔ Computed: {pattern_name}")

    # ==============================
    # STEP 2: FORMAT LABEL
    # ==============================
    def format_label(pattern_name, value):
        name = pattern_name[3:].replace("_", " ").title()

        # Only important patterns get Bullish/Bearish
        if pattern_name in IMPORTANT_PATTERNS:
            if value > 0:
                return f"Bullish {name}"
            elif value < 0:
                return f"Bearish {name}"

        # Others → just name
        if value != 0:
            return name

        return None

    # ==============================
    # STEP 3: CATEGORY PROCESSING
    # ==============================
    for category, patterns in CATEGORY_MAP.items():
        if debug:
            print(f"\n🔍 Processing category: {category}")

        category_result = []

        for i in range(len(df)):
            selected_signal = None

            for pattern_name in patterns:
                if pattern_name in pattern_results:
                    val = pattern_results[pattern_name][i]
                    label = format_label(pattern_name, val)

                    if label:
                        selected_signal = label

                        if debug:
                            print(f"Index {i} → {pattern_name} → {label}")

                        # ✅ STOP at first match (priority order)
                        break

            category_result.append(selected_signal)

        out[category] = category_result

    if debug:
        print("\n✅ Candlestick pattern computation completed\n")

    return out
