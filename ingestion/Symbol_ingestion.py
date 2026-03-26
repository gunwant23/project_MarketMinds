# =============================================================================
# market_minds/ingestion/symbol_ingestion.py
# -----------------------------------------------------------------------------
# Real-time 1-minute market candle ingestion loop.
#
# Responsibilities (single file, no sub-modules)
# -----------------------------------------------
#  1. Initialise SparkSession (or reuse active one on Databricks).
#  2. Continuously fetch 1-min OHLCV data from yfinance every N seconds.
#  3. Deduplicate ticks against an in-process seen-set.
#  4. Run the full feature-engineering pipeline (pandas).
#  5. Compute expiry metadata.
#  6. Assess data quality.
#  7. Convert to Spark DataFrame.
#  8. Write to the partitioned Delta table.
#  9. Emit structured log lines for every cycle.
#
# Databricks widget integration
# ------------------------------
# All runtime knobs (symbol, expiry_week, interval, etc.) are resolved from
# Databricks widgets via config/settings.py.  No code changes required to
# switch instruments from the notebook UI.
#
# Usage
# -----
# From a Databricks notebook:
#
#     from market_minds.config.settings import register_widgets
#     register_widgets()               # creates widget UI in the notebook
#
#     from market_minds.ingestion.symbol_ingestion import IngestionLoop
#     loop = IngestionLoop()
#     loop.run()                       # blocks; interrupt kernel to stop
#
# From the command line (local testing):
#
#     python -m market_minds.ingestion.symbol_ingestion
# =============================================================================

from __future__ import annotations

import logging
import math
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional, Set

import numpy as np
import pandas as pd
import yfinance as yf

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from config import settings
from core.data_quality import run_quality_pipeline
from core.feature_engineering import run_feature_pipeline
from storage.delta_writer import write_to_delta

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format=settings.LOG_FORMAT,
    datefmt=settings.LOG_DATE_FORMAT,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ===========================================================================
# Expiry helpers
# ===========================================================================

def _get_thursdays_in_month(year: int, month: int) -> list[date]:
    """Return all Thursdays in *month* of *year*, sorted ascending."""
    thursdays: list[date] = []
    d = date(year, month, 1)
    while d.month == month:
        if d.weekday() == 3:          # 3 = Thursday
            thursdays.append(d)
        d += timedelta(days=1)
    return thursdays


def _next_thursday(reference_date: date, expiry_week: str = "current") -> date:
    """
    Return the Thursday to treat as the active expiry.

    expiry_week = "current"  → nearest upcoming Thursday (including today if
                               today is Thursday)
    expiry_week = "next"     → skip the nearest Thursday, take the one after.
    """
    days_ahead = (3 - reference_date.weekday()) % 7    # 0 if today is Thursday
    nearest_thursday = reference_date + timedelta(days=days_ahead)

    if expiry_week == "next":
        nearest_thursday += timedelta(weeks=1)

    return nearest_thursday

def _is_market_open(ts: pd.Timestamp) -> bool:
    """Check if timestamp falls within NSE market hours."""
    if ts.tz is None:
        ts = ts.tz_localize("UTC")

    ist_ts = ts.tz_convert(settings.TIMEZONE_IST)

    open_time = ist_ts.replace(
        hour=settings.MARKET_OPEN_HOUR,
        minute=settings.MARKET_OPEN_MINUTE,
        second=0,
        microsecond=0,
    )

    close_time = ist_ts.replace(
        hour=settings.MARKET_CLOSE_HOUR,
        minute=settings.MARKET_CLOSE_MINUTE,
        second=0,
        microsecond=0,
    )

    return open_time <= ist_ts <= close_time

def compute_expiry_features(tick_ts: pd.Timestamp) -> dict:
    """
    Expiry logic with:
    - Market hour awareness
    - Post-close rollover
    """

    if tick_ts.tz is None:
        tick_ts = tick_ts.tz_localize("UTC")

    ist_ts = tick_ts.tz_convert(settings.TIMEZONE_IST)

    tick_date = ist_ts.date()
    tick_time = ist_ts.time()

    is_open = _is_market_open(tick_ts)

    # ── Find nearest Thursday ─────────────────────────────
    days_ahead = (3 - tick_date.weekday()) % 7
    nearest_thursday = tick_date + timedelta(days=days_ahead)

    # Handle "next" expiry setting
    if settings.EXPIRY_WEEK == "next":
        nearest_thursday += timedelta(weeks=1)

    # ── Expiry logic ──────────────────────────────────────
    expiry = 0

    if tick_date.weekday() == 3:  # Thursday
        if is_open:
            expiry = 1
            target_expiry = nearest_thursday
        else:
            # After market close → shift to next week
            target_expiry = nearest_thursday + timedelta(weeks=1)
    else:
        target_expiry = nearest_thursday

    # ── Days to expiry ────────────────────────────────────
    days_to_expiry = (target_expiry - tick_date).days

    # ── Monthly expiry check ──────────────────────────────
    thursdays = _get_thursdays_in_month(
        target_expiry.year,
        target_expiry.month
    )

    is_monthly_expiry = 1 if target_expiry == thursdays[-1] else 0

    return {
        "expiry": expiry,
        "days_to_expiry": max(days_to_expiry, 0),
        "is_monthly_expiry": is_monthly_expiry,
        "is_market_open": int(is_open),   # NEW useful feature
    }
# ===========================================================================
# yfinance fetch
# ===========================================================================

def fetch_candles(symbol: str) -> tuple[pd.DataFrame, float]:
    """
    Download the latest 1-day, 1-minute candles for *symbol*.

    Returns
    -------
    (df, latency_ms)
        df           : raw OHLCV DataFrame with a timezone-aware DatetimeIndex in IST.
        latency_ms   : wall-clock time of the yfinance round trip in milliseconds.
    """
    t_start = time.perf_counter()

    ticker = yf.Ticker(symbol)
    raw_df = ticker.history(
        period=settings.YFINANCE_PERIOD,
        interval=settings.YFINANCE_INTERVAL,
        auto_adjust=True,
        prepost=False,
    )

    latency_ms = (time.perf_counter() - t_start) * 1_000

    if raw_df.empty:
        logger.warning("yfinance returned empty data for symbol '%s'.", symbol)
        return pd.DataFrame(), latency_ms

    # ── Normalise index to IST ───────────────────────────────────────────────
    if raw_df.index.tz is None:
        raw_df.index = raw_df.index.tz_localize("UTC")
    raw_df.index = raw_df.index.tz_convert(settings.TIMEZONE_IST)

    # ── Standardise column names ─────────────────────────────────────────────
    raw_df = raw_df.rename(
        columns={
            "Open":   "open",
            "High":   "high",
            "Low":    "low",
            "Close":  "close",
            "Volume": "volume",
        }
    )

    # Keep only the columns we need
    raw_df = raw_df[["open", "high", "low", "close", "volume"]].copy()

    # tick_ts as a proper column (UTC-aware timestamp; Spark handles it correctly)
    raw_df["tick_ts"] = raw_df.index

    logger.debug(
        "Fetched %d candle(s) for '%s' in %.1f ms.", len(raw_df), symbol, latency_ms
    )

    return raw_df.reset_index(drop=True), latency_ms


def _extract_latest_unseen_candle(
    raw_df: pd.DataFrame,
    seen_timestamps: Set[pd.Timestamp],
) -> Optional[pd.DataFrame]:
    """
    Return a single-row DataFrame for the newest candle NOT in *seen_timestamps*.
    The very last candle is often still forming, so we take the second-to-last
    (the last completed bar).
    """
    if raw_df.empty or len(raw_df) < 2:
        return None

    # Last COMPLETED candle = second-to-last row in the 1-min history
    completed_df = raw_df.iloc[:-1]

    # Most recent completed candle
    latest = completed_df.iloc[[-1]].copy()    # single-row DataFrame
    ts = latest["tick_ts"].iloc[0]

    if ts in seen_timestamps:
        logger.debug("Candle at %s already ingested – skipping.", ts)
        return None

    return latest


# ===========================================================================
# Pandas → Spark conversion
# ===========================================================================

def to_spark_dataframe(
    pandas_df: pd.DataFrame,
    spark: SparkSession,
) -> SparkDataFrame:
    """
    Convert *pandas_df* to a Spark DataFrame using the canonical Delta schema.

    The function casts each column to the correct PySpark type to prevent
    type-mismatch errors on Delta write.
    """
    from market_minds.storage.delta_writer import DELTA_SCHEMA
    from pyspark.sql import functions as F

    # Ensure tick_ts is Python datetime (Spark fromPandas handles tz-aware)
    if pd.api.types.is_datetime64_any_dtype(pandas_df["tick_ts"]):
        # strip tz for Spark (Spark stores TIMESTAMP in UTC internally)
        pandas_df = pandas_df.copy()
        pandas_df["tick_ts"] = pandas_df["tick_ts"].dt.tz_localize(None) if pandas_df["tick_ts"].dt.tz is None else pandas_df["tick_ts"].dt.tz_convert("UTC").dt.tz_localize(None)

    spark_df = spark.createDataFrame(pandas_df, schema=DELTA_SCHEMA)
    return spark_df


# ===========================================================================
# Core ingestion cycle
# ===========================================================================

def run_single_cycle(
    symbol: str,
    spark: SparkSession,
    seen_timestamps: Set[pd.Timestamp],
    previous_close: Optional[float],
) -> tuple[Optional[float], float]:

    # ── 1. Fetch ─────────────────────────────────────────────
    raw_df, latency_ms = fetch_candles(symbol)

    if raw_df.empty:
        return previous_close, latency_ms

    # ── 2. Extract candle ────────────────────────────────────
    candle_df = _extract_latest_unseen_candle(raw_df, seen_timestamps)

    if candle_df is None:
        logger.info("No new candle this cycle.  Latency=%.1f ms.", latency_ms)
        return previous_close, latency_ms

    tick_ts = candle_df["tick_ts"].iloc[0]

    # 🚫 MARKET CLOSED CHECK (MAIN REQUIREMENT)
    if not _is_market_open(tick_ts):
        logger.info("No ingestion to process as market is closed │ %s", tick_ts)
        return previous_close, latency_ms

    logger.info(
        "New candle │ symbol=%s │ tick_ts=%s │ latency=%.1f ms",
        symbol, tick_ts, latency_ms,
    )

    # ── Continue normal pipeline ─────────────────────────────
    candle_df = candle_df.copy()
    candle_df["previous_close"] = previous_close if previous_close else candle_df["open"].iloc[0]

    enriched_df = run_feature_pipeline(candle_df)

    expiry_meta = compute_expiry_features(tick_ts)
    for col, val in expiry_meta.items():
        enriched_df[col] = val

    enriched_df["fetch_latency_ms"] = round(latency_ms, 3)

    enriched_df = run_quality_pipeline(enriched_df)

    if enriched_df.empty:
        logger.warning("All rows removed by quality pipeline – nothing to write.")
        return previous_close, latency_ms

    enriched_df = enriched_df.drop(columns=["previous_close"], errors="ignore")

    seen_timestamps.add(tick_ts)

    spark_df = to_spark_dataframe(enriched_df, spark)
    write_to_delta(spark_df, table_name=settings.DELTA_TABLE, spark=spark)

    new_previous_close = float(enriched_df["close"].iloc[0])
    return new_previous_close, latency_ms

    # ── 3. Attach previous_close for return_pct ───────────────────────────
    candle_df = candle_df.copy()
    candle_df["previous_close"] = previous_close if previous_close else candle_df["open"].iloc[0]

    # ── 4. Feature engineering ───────────────────────────────────────────────
    enriched_df = run_feature_pipeline(candle_df)

    # ── 5. Expiry metadata ───────────────────────────────────────────────────
    expiry_meta = compute_expiry_features(tick_ts)
    for col, val in expiry_meta.items():
        enriched_df[col] = val

    # ── 6. Fetch latency column ───────────────────────────────────────────────
    enriched_df["fetch_latency_ms"] = round(latency_ms, 3)

    # ── 7. Data quality ───────────────────────────────────────────────────────
    enriched_df = run_quality_pipeline(enriched_df)

    if enriched_df.empty:
        logger.warning("All rows removed by quality pipeline – nothing to write.")
        return previous_close, latency_ms

    # ── 8. Drop helper column not in Delta schema ─────────────────────────────
    enriched_df = enriched_df.drop(columns=["previous_close"], errors="ignore")

    # ── 9. Persist seen timestamp ────────────────────────────────────────────
    seen_timestamps.add(tick_ts)

    # ── 10. Convert to Spark and write ────────────────────────────────────────
    spark_df = to_spark
    write_to_delta(spark_df, table_name=settings.DELTA_TABLE, spark=spark)

    new_previous_close = float(enriched_df["close"].iloc[0])
    return new_previous_close, latency_ms


# ===========================================================================
# Ingestion loop orchestrator
# ===========================================================================

class IngestionLoop:
    """
    Orchestrates the continuous real-time ingestion loop.

    Instantiate once per notebook / process, then call `.run()`.
    The loop runs until interrupted (KeyboardInterrupt) or until
    `settings.MAX_CYCLES` cycles have completed (0 = infinite).

    Example (Databricks notebook)
    ------------------------------
        from market_minds.config.settings import register_widgets, print_config
        register_widgets()
        print_config()

        from market_minds.ingestion.symbol_ingestion import IngestionLoop
        loop = IngestionLoop()
        loop.run()
    """

    def __init__(
        self,
        symbol:   Optional[str]          = None,
        spark:    Optional[SparkSession]  = None,
    ) -> None:
        self.symbol   = symbol or settings.SYMBOL
        self.spark    = spark  or self._get_or_create_spark()
        self.seen_ts: Set[pd.Timestamp] = set()
        self._previous_close: Optional[float] = None
        self._cycle_count = 0

        logger.info(
            "IngestionLoop initialised │ symbol=%s │ table=%s │ interval=%ds",
            self.symbol, settings.DELTA_TABLE, settings.INGESTION_INTERVAL_SEC,
        )

    # ------------------------------------------------------------------
    def run(self) -> None:
        """
        Start the ingestion loop.  Blocks until interrupted or MAX_CYCLES reached.
        """
        max_cycles = settings.MAX_CYCLES
        interval   = settings.INGESTION_INTERVAL_SEC

        logger.info(
            "▶ Starting ingestion loop │ max_cycles=%s │ interval=%ds",
            max_cycles or "∞", interval,
        )
        settings.print_config()

        try:
            while True:
                self._cycle_count += 1
                cycle_label = f"Cycle #{self._cycle_count}"

                logger.info("── %s START ──────────────────────────────", cycle_label)
                cycle_start = time.perf_counter()

                try:
                    self._previous_close, latency = run_single_cycle(
                        symbol=self.symbol,
                        spark=self.spark,
                        seen_timestamps=self.seen_ts,
                        previous_close=self._previous_close,
                    )
                except Exception as exc:                        # noqa: BLE001
                    logger.exception("%s failed: %s", cycle_label, exc)

                elapsed = (time.perf_counter() - cycle_start) * 1_000
                logger.info(
                    "── %s END │ elapsed=%.0f ms ────────────────────",
                    cycle_label, elapsed,
                )

                # Stop condition
                if max_cycles and self._cycle_count >= max_cycles:
                    logger.info("MAX_CYCLES=%d reached – stopping loop.", max_cycles)
                    break

                # Sleep for remainder of interval
                sleep_sec = max(0.0, interval - elapsed / 1_000)
                logger.debug("Sleeping %.1f s …", sleep_sec)
                time.sleep(sleep_sec)

        except KeyboardInterrupt:
            logger.info("⏹  Ingestion loop interrupted by user after %d cycle(s).", self._cycle_count)

    # ------------------------------------------------------------------
    @staticmethod
    def _get_or_create_spark() -> SparkSession:
        """Return the active SparkSession or create a local one for testing."""
        active = SparkSession.getActiveSession()
        if active:
            logger.debug("Reusing active SparkSession.")
            return active

        logger.info("No active SparkSession found – creating a local one.")
        return (
            SparkSession.builder
            .appName("market_minds_ingestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )


# ===========================================================================
# CLI entry-point
# ===========================================================================

if __name__ == "__main__":
    loop = IngestionLoop()
    loop.run()