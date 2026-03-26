# =============================================================================
# market_minds/storage/delta_writer.py
# -----------------------------------------------------------------------------
# Thin, opinionated wrapper around PySpark's Delta Lake writer.
#
# Design decisions
# ----------------
# • append mode only  – the ingestion loop never overwrites history.
# • Partition by *date* (YYYYMMDD INT) for efficient time-range queries.
# • No mergeSchema – schema is fixed; schema drift must be caught early.
# • Table DDL is idempotent: CREATE TABLE IF NOT EXISTS on first write.
# • Accepts a Spark DataFrame that already matches the target schema.
# =============================================================================

from __future__ import annotations

import logging
from textwrap import dedent
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import settings

logger = logging.getLogger(__name__)

# ===========================================================================
# Schema definition  (single source of truth – matches the DDL in the spec)
# ===========================================================================

DELTA_SCHEMA: StructType = StructType(
    [
        StructField("tick_ts",          TimestampType(), nullable=False),
        StructField("tick_time_str",    StringType(),    nullable=True),
        # ── Time dimensions ──────────────────────────────────────────────────
        StructField("date",             IntegerType(),   nullable=False),
        StructField("month",            IntegerType(),   nullable=True),
        StructField("year",             IntegerType(),   nullable=True),
        StructField("day",              IntegerType(),   nullable=True),
        StructField("hour",             IntegerType(),   nullable=True),
        StructField("minute",           IntegerType(),   nullable=True),
        StructField("week_of_month",    IntegerType(),   nullable=True),
        # ── OHLCV ────────────────────────────────────────────────────────────
        StructField("open",             DoubleType(),    nullable=True),
        StructField("high",             DoubleType(),    nullable=True),
        StructField("low",              DoubleType(),    nullable=True),
        StructField("close",            DoubleType(),    nullable=True),
        StructField("volume",           DoubleType(),    nullable=True),
        # ── Feature engineering ───────────────────────────────────────────────
        StructField("typical_price",    DoubleType(),    nullable=True),
        StructField("price_range",      DoubleType(),    nullable=True),
        StructField("return_pct",       DoubleType(),    nullable=True),
        StructField("vwap",             DoubleType(),    nullable=True),
        StructField("hl_ratio",         DoubleType(),    nullable=True),
        StructField("body_pct",         DoubleType(),    nullable=True),
        StructField("upper_shadow",     DoubleType(),    nullable=True),
        StructField("lower_shadow",     DoubleType(),    nullable=True),
        # ── Session ───────────────────────────────────────────────────────────
        StructField("session_bucket",   IntegerType(),   nullable=True),
        # ── Expiry ────────────────────────────────────────────────────────────
        StructField("expiry",           IntegerType(),   nullable=True),
        StructField("is_monthly_expiry",IntegerType(),   nullable=True),
        StructField("days_to_expiry",   IntegerType(),   nullable=True),
        # ── Metadata ──────────────────────────────────────────────────────────
        StructField("data_quality",     StringType(),    nullable=True),
        StructField("fetch_latency_ms", DoubleType(),    nullable=True),
    ]
)

# ===========================================================================
# DDL template
# ===========================================================================

_DDL_TEMPLATE = dedent(
    """\
    CREATE TABLE IF NOT EXISTS {table_name} (
        tick_ts             TIMESTAMP   NOT NULL,
        tick_time_str       STRING,

        date                INT         NOT NULL,
        month               INT,
        year                INT,
        day                 INT,
        hour                INT,
        minute              INT,
        week_of_month       INT,

        open                DOUBLE,
        high                DOUBLE,
        low                 DOUBLE,
        close               DOUBLE,
        volume              DOUBLE,

        typical_price       DOUBLE,
        price_range         DOUBLE,
        return_pct          DOUBLE,
        vwap                DOUBLE,
        hl_ratio            DOUBLE,
        body_pct            DOUBLE,
        upper_shadow        DOUBLE,
        lower_shadow        DOUBLE,

        session_bucket      INT,

        expiry              INT,
        is_monthly_expiry   INT,
        days_to_expiry      INT,

        data_quality        STRING,
        fetch_latency_ms    DOUBLE
    )
    USING DELTA
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'description' = 'Real-time 1-minute market candles – market_minds'
    )
    """
)


# ===========================================================================
# Public API
# ===========================================================================

def ensure_table_exists(spark: SparkSession, table_name: str) -> None:
    """
    Create the Delta table with the canonical schema if it does not yet exist.
    Safe to call on every ingestion cycle (idempotent).
    """
    ddl = _DDL_TEMPLATE.format(table_name=table_name)
    spark.sql(ddl)
    logger.info("Table '%s' ensured (CREATE IF NOT EXISTS).", table_name)


def write_to_delta(
    spark_df: DataFrame,
    table_name: Optional[str] = None,
    *,
    spark: Optional[SparkSession] = None,
) -> int:
    """
    Append *spark_df* to the Delta table.

    Parameters
    ----------
    spark_df   : pyspark.sql.DataFrame
        Fully-featured candle row(s) conforming to DELTA_SCHEMA.
    table_name : str, optional
        Fully qualified table name.  Defaults to settings.DELTA_TABLE.
    spark      : SparkSession, optional
        Active SparkSession.  Auto-resolved if not provided.

    Returns
    -------
    int
        Number of rows written in this batch.
    """
    table_name = table_name or settings.DELTA_TABLE
    spark      = spark or SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active SparkSession.  Create one before calling write_to_delta."
        )

    if spark_df is None or spark_df.rdd.isEmpty():
        logger.warning("write_to_delta: received empty DataFrame – nothing written.")
        return 0

    # Ensure table exists (idempotent)
    ensure_table_exists(spark, table_name)

    # Column-order alignment  (guard against upstream column reordering)
    expected_cols = [f.name for f in DELTA_SCHEMA.fields]
    available     = set(spark_df.columns)
    missing       = set(expected_cols) - available

    if missing:
        logger.warning(
            "write_to_delta: DataFrame is missing columns %s – filling with NULL.", missing
        )
        for col in missing:
            dtype = DELTA_SCHEMA[col].dataType
            spark_df = spark_df.withColumn(col, F.lit(None).cast(dtype))

    spark_df = spark_df.select(expected_cols)

    # Count before write  (materialises the plan)
    row_count = spark_df.count()

    (
        spark_df.write
        .format("delta")
        .mode(settings.DELTA_WRITE_MODE)
        .option("mergeSchema", "false")
        .partitionBy("date")
        .saveAsTable(table_name)
    )

    logger.info(
        "✅ Wrote %d row(s) → %s  [mode=%s]",
        row_count, table_name, settings.DELTA_WRITE_MODE,
    )
    return row_count