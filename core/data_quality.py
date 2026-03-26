# =============================================================================
# market_minds/core/data_quality.py
# -----------------------------------------------------------------------------
# Lightweight data-quality layer applied to every ingested candle batch.
#
# Quality labels
# --------------
# "good"      – row is unique, all mandatory fields populated, values sane.
# "duplicate" – identical tick_ts already exists in the batch or Delta table.
# "missing"   – one or more mandatory numeric columns is null / NaN / zero.
#
# The label is written to the *data_quality* column in the Delta table.
# Duplicate rows are removed before writing to prevent double-counting.
# =============================================================================

from __future__ import annotations

import logging
from typing import Final, List

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Columns that MUST contain non-null, positive values for a row to be "good"
# ---------------------------------------------------------------------------

MANDATORY_COLS: Final[List[str]] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "typical_price",
    "price_range",
    "tick_ts",
]


# ===========================================================================
# Public API
# ===========================================================================

def assign_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Evaluate each row and attach a *data_quality* label.

    Processing order (applied in sequence so each label is exclusive)
    -----------------------------------------------------------------
    1. Mark rows where any MANDATORY_COLS value is null/NaN/inf as "missing".
    2. Mark rows that are duplicates of an earlier row in the same batch as
       "duplicate"  (keeps first occurrence).
    3. All remaining rows → "good".

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain columns listed in *MANDATORY_COLS* plus *tick_ts*.

    Returns
    -------
    pandas.DataFrame
        Same rows, same order, with a new *data_quality* STRING column.
    """
    if df.empty:
        logger.warning("assign_data_quality received empty DataFrame.")
        return df

    df = df.copy()

    # ── 1. Detect missing / invalid values ──────────────────────────────────
    existing_mandatory = [c for c in MANDATORY_COLS if c in df.columns]
    missing_mask = _is_missing(df, existing_mandatory)

    # ── 2. Detect duplicates on tick_ts  (within this batch) ────────────────
    duplicate_mask = df.duplicated(subset=["tick_ts"], keep="first") & ~missing_mask

    # ── 3. Default label ─────────────────────────────────────────────────────
    df["data_quality"] = "good"
    df.loc[missing_mask,   "data_quality"] = "missing"
    df.loc[duplicate_mask, "data_quality"] = "duplicate"

    good_count      = (df["data_quality"] == "good").sum()
    missing_count   = (df["data_quality"] == "missing").sum()
    duplicate_count = (df["data_quality"] == "duplicate").sum()

    logger.info(
        "Data quality summary → good: %d | missing: %d | duplicate: %d",
        good_count, missing_count, duplicate_count,
    )

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows labelled "duplicate" (keeps the first occurrence already marked
    "good").  Rows labelled "missing" are retained so downstream monitoring
    can detect and alert on data gaps.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain a *data_quality* column (produced by assign_data_quality).

    Returns
    -------
    pandas.DataFrame
        Duplicate rows removed; index reset.
    """
    if df.empty:
        return df

    if "data_quality" not in df.columns:
        logger.warning(
            "remove_duplicates: 'data_quality' column not found – returning df unchanged."
        )
        return df

    before = len(df)
    df = df[df["data_quality"] != "duplicate"].reset_index(drop=True)
    removed = before - len(df)

    if removed:
        logger.info("Removed %d duplicate row(s).", removed)

    return df


def validate_ohlc_integrity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Additional sanity checks that go beyond null detection:

    • high  ≥ low               (violated during bad feed / bad alignment)
    • high  ≥ open  AND  close  (violated by some yfinance decimal issues)
    • low   ≤ open  AND  close
    • volume ≥ 0

    Rows failing any check are downgraded to "missing".
    """
    if df.empty:
        return df

    required = {"open", "high", "low", "close", "volume", "data_quality"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        logger.warning("validate_ohlc_integrity: missing columns %s – skipping OHLC checks.", missing_cols)
        return df

    df = df.copy()

    ohlc_bad_mask = (
        (df["high"] < df["low"])
        | (df["high"] < df["open"])
        | (df["high"] < df["close"])
        | (df["low"]  > df["open"])
        | (df["low"]  > df["close"])
        | (df["volume"] < 0)
    )

    # Only override rows currently labelled "good"
    downgrade_mask = ohlc_bad_mask & (df["data_quality"] == "good")
    df.loc[downgrade_mask, "data_quality"] = "missing"

    if downgrade_mask.any():
        logger.warning(
            "%d row(s) failed OHLC integrity checks and were downgraded to 'missing'.",
            downgrade_mask.sum(),
        )

    return df


# ===========================================================================
# Pipeline convenience wrapper
# ===========================================================================

def run_quality_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full data-quality pipeline:

    1. assign_data_quality  (null / duplicate detection)
    2. validate_ohlc_integrity  (OHLC sanity)
    3. remove_duplicates  (drop exact tick_ts dupes before writing to Delta)
    """
    df = assign_data_quality(df)
    df = validate_ohlc_integrity(df)
    df = remove_duplicates(df)
    return df


# ===========================================================================
# Private helpers
# ===========================================================================

def _is_missing(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    """
    Return a boolean Series that is True wherever a row has a problematic
    value in any of *cols*:
      • pandas NaN / None
      • numpy inf or -inf
      • Zero in OHLC / price columns (zero price = data gap, not valid trade)
    """
    price_cols = {"open", "high", "low", "close", "typical_price"}
    bad = pd.Series(False, index=df.index)

    for col in cols:
        if col not in df.columns:
            continue

        series = df[col]

        if pd.api.types.is_numeric_dtype(series):
            null_mask = series.isna() | np.isinf(series)
            if col in price_cols:
                null_mask = null_mask | (series <= 0)
            bad = bad | null_mask
        else:
            # Timestamp / string columns: just check for null
            bad = bad | series.isna()

    return bad