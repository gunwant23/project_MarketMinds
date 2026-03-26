# =============================================================================
# market_minds/monitoring/monitoring_notebook.py
# -----------------------------------------------------------------------------
# Databricks monitoring notebook (Python file format).
#
# Paste each section into a separate Databricks notebook cell, or run the
# script from the command line against a Delta table on the local filesystem.
#
# Sections
# --------
# 1. Widget setup & config print
# 2. Load Delta table as a Spark DataFrame
# 3. Display latest 50 rows
# 4. Display latest tick (single row)
# 5. Total row count + per-quality breakdown
# 6. Ingestion delay (current UTC time vs. latest tick)
# 7. Session bucket distribution
# 8. Fetch latency statistics
# 9. Daily volume summary
# =============================================================================

# ── CELL 1: Setup ──────────────────────────────────────────────────────────

from config.settings import register_widgets, print_config

# Register Databricks widgets (idempotent – safe to re-run)
register_widgets()
print_config()


# ── CELL 2: Load Delta table ────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import settings

spark = SparkSession.getActiveSession() or SparkSession.builder.appName("mm_monitoring").getOrCreate()

TABLE = settings.DELTA_TABLE

print(f"\n📂  Reading from Delta table: {TABLE}\n")

try:
    delta_df = spark.read.format("delta").table(TABLE)
    total_rows = delta_df.count()
    print(f"✅  Table loaded successfully  │  Total rows: {total_rows:,}")
except Exception as e:
    print(f"❌  Could not load table '{TABLE}': {e}")
    delta_df = None
    total_rows = 0


# ── CELL 3: Latest 50 rows ──────────────────────────────────────────────────

if delta_df is not None:
    print("\n📋  Latest 50 candles (most recent first)\n" + "─" * 70)
    (
        delta_df
        .orderBy(F.col("tick_ts").desc())
        .limit(50)
        .display()                     # Databricks display(); use .show() locally
    )


# ── CELL 4: Latest single tick ──────────────────────────────────────────────

if delta_df is not None:
    print("\n🕐  Latest ingested tick\n" + "─" * 70)
    latest_tick = (
        delta_df
        .orderBy(F.col("tick_ts").desc())
        .limit(1)
    )
    latest_tick.display()

    latest_row = latest_tick.collect()
    if latest_row:
        row = latest_row[0]
        print(f"\n  Symbol tick time  : {row['tick_time_str']}")
        print(f"  Close             : {row['close']:.2f}")
        print(f"  VWAP              : {row['vwap']:.2f}")
        print(f"  Session bucket    : {row['session_bucket']}")
        print(f"  Data quality      : {row['data_quality']}")
        print(f"  Fetch latency     : {row['fetch_latency_ms']:.1f} ms")
        print(f"  Expiry            : {'Yes' if row['expiry'] else 'No'}")
        print(f"  Days to expiry    : {row['days_to_expiry']}")


# ── CELL 5: Row count + quality breakdown ───────────────────────────────────

if delta_df is not None:
    print("\n📊  Data Quality Summary\n" + "─" * 70)

    quality_summary = (
        delta_df
        .groupBy("data_quality")
        .agg(
            F.count("*").alias("row_count"),
            F.round(F.count("*") / F.lit(total_rows) * 100, 2).alias("pct"),
        )
        .orderBy("data_quality")
    )
    quality_summary.display()

    print(f"\n  Total rows in table : {total_rows:,}")


# ── CELL 6: Ingestion delay ─────────────────────────────────────────────────

if delta_df is not None and latest_row:
    import pytz
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)

    # tick_ts is stored as UTC TIMESTAMP in Spark
    latest_ts_row = (
        delta_df
        .orderBy(F.col("tick_ts").desc())
        .select(F.col("tick_ts").cast("long").alias("epoch"))
        .limit(1)
        .collect()
    )

    if latest_ts_row:
        latest_epoch = latest_ts_row[0]["epoch"]
        latest_utc   = datetime.utcfromtimestamp(latest_epoch).replace(tzinfo=timezone.utc)
        delay_sec    = (now_utc - latest_utc).total_seconds()

        print("\n⏱   Ingestion Delay\n" + "─" * 70)
        print(f"  Current UTC time  : {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Latest tick (UTC) : {latest_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Ingestion delay   : {delay_sec:.0f} seconds  ({delay_sec / 60:.1f} minutes)")

        if delay_sec > 180:
            print("  ⚠️   WARNING: Ingestion delay > 3 minutes.  Check the ingestion loop.")
        else:
            print("  ✅  Delay within acceptable range.")


# ── CELL 7: Session bucket distribution ─────────────────────────────────────

if delta_df is not None:
    print("\n🪣   Session Bucket Distribution (today)\n" + "─" * 70)

    from datetime import date
    today_int = int(date.today().strftime("%Y%m%d"))

    (
        delta_df
        .filter(F.col("date") == today_int)
        .groupBy("session_bucket")
        .agg(
            F.count("*").alias("candles"),
            F.round(F.avg("close"), 2).alias("avg_close"),
            F.round(F.avg("volume"), 0).alias("avg_volume"),
        )
        .orderBy("session_bucket")
        .display()
    )


# ── CELL 8: Fetch latency statistics ────────────────────────────────────────

if delta_df is not None:
    print("\n📡  Fetch Latency Statistics (last 500 rows)\n" + "─" * 70)

    (
        delta_df
        .orderBy(F.col("tick_ts").desc())
        .limit(500)
        .agg(
            F.round(F.min("fetch_latency_ms"),  1).alias("min_ms"),
            F.round(F.avg("fetch_latency_ms"),  1).alias("avg_ms"),
            F.round(F.max("fetch_latency_ms"),  1).alias("max_ms"),
            F.round(
                F.percentile_approx("fetch_latency_ms", 0.95), 1
            ).alias("p95_ms"),
        )
        .display()
    )


# ── CELL 9: Daily candle & volume summary ───────────────────────────────────

if delta_df is not None:
    print("\n📅  Daily Summary (last 10 trading days)\n" + "─" * 70)

    (
        delta_df
        .filter(F.col("data_quality") == "good")
        .groupBy("date")
        .agg(
            F.count("*").alias("candle_count"),
            F.round(F.first("open"),  2).alias("day_open"),
            F.round(F.max("high"),    2).alias("day_high"),
            F.round(F.min("low"),     2).alias("day_low"),
            F.round(F.last("close"),  2).alias("day_close"),
            F.round(F.sum("volume"),  0).alias("total_volume"),
            F.round(F.avg("fetch_latency_ms"), 1).alias("avg_latency_ms"),
        )
        .orderBy(F.col("date").desc())
        .limit(10)
        .display()
    )

print("\n✅  Monitoring complete.")