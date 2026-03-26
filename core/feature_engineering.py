# =============================================================================
# market_minds/core/feature_engineering.py
# -----------------------------------------------------------------------------
# Pure-pandas feature engineering applied to a single 1-minute OHLCV candle.
#
# Design principles
# -----------------
# • All functions are stateless and side-effect free.
# • Every function accepts a pandas DataFrame and returns a pandas DataFrame.
# • Numerical stability: safe division guards prevent ZeroDivisionError for
#   zero-range candles (e.g., circuit-breaker halts, auction snapshots).
# • The module is fully unit-testable without a Spark / Databricks context.
# =============================================================================

from __future__ import annotations

import logging
from typing import Final

import numpy as np
import pandas as pd

from config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EPSILON: Final[float] = 1e-9   # guard against division by zero

_MARKET_OPEN_HOUR:   Final[int] = settings.MARKET_OPEN_HOUR
_MARKET_OPEN_MINUTE: Final[int] = settings.MARKET_OPEN_MINUTE
_SESSION_MINUTES:    Final[int] = settings.SESSION_TOTAL_MINUTES
_SESSION_BUCKETS:    Final[int] = settings.SESSION_BUCKETS

# Minutes per bucket  (62.5 → Python floor truncates correctly)
_BUCKET_SIZE: Final[float] = _SESSION_MINUTES / _SESSION_BUCKETS


# ===========================================================================
# 1. TIME FEATURES
# ===========================================================================

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive temporal columns from *tick_ts* (timezone-aware timestamp).

    Added columns
    -------------
    year, month, day, date (YYYYMMDD INT), hour, minute,
    week_of_month, session_bucket, tick_time_str
    """
    if df.empty:
        return df

    ts = df["tick_ts"]

    # ── Basic calendar fields ────────────────────────────────────────────────
    df = df.copy()
    df["year"]   = ts.dt.year.astype("int32")
    df["month"]  = ts.dt.month.astype("int32")
    df["day"]    = ts.dt.day.astype("int32")
    df["hour"]   = ts.dt.hour.astype("int32")
    df["minute"] = ts.dt.minute.astype("int32")

    # YYYYMMDD as integer  (e.g. 20250114)
    df["date"] = (
        df["year"] * 10_000
        + df["month"] * 100
        + df["day"]
    ).astype("int32")

    # ── Week-of-month  (1–5)  ─────────────────────────────────────────────
    # Week 1 = days 1-7, Week 2 = days 8-14, …
    df["week_of_month"] = ((df["day"] - 1) // 7 + 1).astype("int32")

    # ── Human-readable tick string  ───────────────────────────────────────
    df["tick_time_str"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S IST")

    # ── Session bucket  ───────────────────────────────────────────────────
    df["session_bucket"] = _compute_session_bucket(df["hour"], df["minute"])

    logger.debug("Time features added for %d rows.", len(df))
    return df


def _compute_session_bucket(
    hour_series: pd.Series,
    minute_series: pd.Series,
) -> pd.Series:
    """
    Divide the NSE trading session (9:15–15:30, 375 min) into 6 equal buckets.

    Formula
    -------
    minutes_since_915 = (hour - 9) * 60 + minute - 15
    bucket            = floor(minutes_since_915 / (375/6)) + 1
    Clamped to [1, 6].
    """
    minutes_since_open = (hour_series - _MARKET_OPEN_HOUR) * 60 + (
        minute_series - _MARKET_OPEN_MINUTE
    )

    raw_bucket = np.floor(minutes_since_open / _BUCKET_SIZE) + 1
    clamped    = np.clip(raw_bucket, 1, _SESSION_BUCKETS)
    return clamped.astype("int32")


# ===========================================================================
# 2. PRICE FEATURES
# ===========================================================================

def add_price_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute candlestick-derived price features.

    Added columns
    -------------
    typical_price, price_range, return_pct, hl_ratio,
    body_pct, upper_shadow, lower_shadow
    """
    if df.empty:
        return df

    df = df.copy()

    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]

    # ── Typical price  (classic pivot-point numerator) ────────────────────
    df["typical_price"] = (h + l + c) / 3.0

    # ── Price range  ──────────────────────────────────────────────────────
    df["price_range"] = (h - l).clip(lower=0.0)

    # Guard: avoid 0-range division
    safe_range = df["price_range"].replace(0, np.nan)
    safe_low   = l.replace(0, np.nan)

    # ── Percentage return vs. previous close  (filled at candle level) ────
    #    previous_close is passed in as a column by the ingestion layer.
    if "previous_close" in df.columns:
        safe_prev = df["previous_close"].replace(0, np.nan)
        df["return_pct"] = ((c - safe_prev) / safe_prev).fillna(0.0)
    else:
        df["return_pct"] = 0.0

    # ── High / low ratio  ────────────────────────────────────────────────
    df["hl_ratio"] = (h / safe_low).fillna(1.0)

    # ── Body percentage  (fraction of range occupied by the real body) ───
    df["body_pct"] = (np.abs(c - o) / safe_range).fillna(0.0).clip(0.0, 1.0)

    # ── Shadows  ─────────────────────────────────────────────────────────
    candle_top    = np.maximum(o, c)
    candle_bottom = np.minimum(o, c)

    df["upper_shadow"] = (h - candle_top).clip(lower=0.0)
    df["lower_shadow"] = (candle_bottom - l).clip(lower=0.0)

    logger.debug("Price features computed for %d rows.", len(df))
    return df


# ===========================================================================
# 3. VWAP  (session-level rolling)
# ===========================================================================

def compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Approximate single-candle VWAP:  (typical_price × volume) / volume.

    For a single row this collapses to *typical_price*.
    When multiple rows for the same session are passed the function computes
    a proper running VWAP grouped by *date*.

    The column *typical_price* must already exist (call add_price_features first).
    """
    if df.empty:
        return df

    if "typical_price" not in df.columns:
        raise ValueError("compute_vwap requires 'typical_price'; call add_price_features first.")

    df = df.copy()

    safe_vol   = df["volume"].replace(0, np.nan)
    cum_tpv    = (df["typical_price"] * df["volume"]).groupby(df["date"]).cumsum()
    cum_vol    = df["volume"].groupby(df["date"]).cumsum().replace(0, np.nan)

    df["vwap"] = (cum_tpv / cum_vol).fillna(df["typical_price"])

    logger.debug("VWAP computed for %d rows.", len(df))
    return df


# ===========================================================================
# 4. PIPELINE  (convenience wrapper)
# ===========================================================================

def run_feature_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Execute the complete feature engineering pipeline in the correct order.

    Expected input columns
    ----------------------
    tick_ts, open, high, low, close, volume
    Optional: previous_close

    Returns
    -------
    DataFrame enriched with all feature columns ready for Delta ingestion.
    """
    if df.empty:
        logger.warning("Feature pipeline received an empty DataFrame – skipping.")
        return df

    logger.info("Running feature pipeline on %d row(s).", len(df))

    df = add_time_features(df)
    df = add_price_features(df)
    df = compute_vwap(df)

    return df