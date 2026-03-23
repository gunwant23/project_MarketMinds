# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "yfinance",
# ]
# ///
# MAGIC %md
# MAGIC # Nifty Minute Price Pipeline
# MAGIC Fetches Nifty 50 price every minute and appends to a Delta table.

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

"""
Nifty 50 Live Fetcher
- Checks if NSE market is currently open (IST)
- If open: fetches latest price via yfinance and saves to a DataFrame
- If closed: prints a message and exits
"""

import datetime
import zoneinfo
import yfinance as yf
import pandas as pd

# ── Config ───────────────────────────────────────────────────────────────────
SYMBOL        = "^NSEI"
SESSION_START = datetime.time(9, 15)
SESSION_END   = datetime.time(15, 30)
IST           = zoneinfo.ZoneInfo("Asia/Kolkata")

# ── Market check ─────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    now_ist = datetime.datetime.now(IST)
    # Skip weekends (Saturday=5, Sunday=6)
    if now_ist.weekday() >= 5:
        return False
    return SESSION_START <= now_ist.time() <= SESSION_END

# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_nifty() -> pd.DataFrame:
    ticker = yf.Ticker(SYMBOL)
    # Pull last 1-day 1-min bars for OHLCV
    df = ticker.history(period="1d", interval="1m")
    if df.empty:
        raise ValueError("yfinance returned no data for ^NSEI")
    # Keep the most recent bar
    latest = df.iloc[[-1]].copy()
    latest.index = latest.index.tz_convert(IST)          # convert to IST
    latest.index.name = "datetime_ist"
    latest = latest[["Open", "High", "Low", "Close"]]
    latest.columns = ["open", "high", "low", "close"]
    return latest

def program() -> pd.DataFrame | None:
    """Single fetch — returns one-row DataFrame or None if market closed."""
    if not is_market_open():
        return None
    return fetch_nifty()

if __name__ == "__main__":
    import time

    INTERVAL_SEC = 60
    session_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")

    now_ist = datetime.datetime.now(IST)
    market_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end   = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

    if now_ist >= market_end or now_ist.weekday() >= 5:
        print("❌  Market is CLOSED.")
    else:
        print("⏳  Waiting for market open (09:15 IST) …")

        # ── Wait until market opens ───────────────────────────────────────────────
        while True:
            now_ist = datetime.datetime.now(IST)
            market_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
            market_end   = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

            if now_ist >= market_start:
                break
            sleep_secs = (market_start - now_ist).total_seconds()
            print(f"  Market opens in {int(sleep_secs//60)}m {int(sleep_secs%60)}s — sleeping …")
            time.sleep(min(sleep_secs, 60))   # wake up every 60s to re-check

        print("✅  Market is OPEN. Starting data collection every 60s …\n")

        # ── Collection loop ───────────────────────────────────────────────────────
        nifty_df = pd.DataFrame()

        while True:
            now_ist    = datetime.datetime.now(IST)
            market_end = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

            # Stop at 15:30
            if now_ist >= market_end:
                print(f"\n🔔  15:30 IST reached — stopping collection.")
                print("❌  Market is CLOSED.")
                break

            tick = program()
            if tick is not None:
                nifty_df = pd.concat([nifty_df, tick])
                ts = now_ist.strftime("%H:%M:%S")
                print(f"[{ts}]  ✔  close={tick['close'].iloc[0]:.2f}  rows_collected={len(nifty_df)}")

                # Save each tick to Delta table with proper naming
                spark_df = spark.createDataFrame(tick.reset_index())
                table_name = f"nifty_live.ticks_{session_date}"
                spark_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_name)

                # Read the Delta table as a streaming DataFrame and display
                df_live = spark.readStream.table(table_name)
                display(df_live)
            else:
                print(f"[{now_ist.strftime('%H:%M:%S')}]  ⚠  No data returned — skipping tick.")

            time.sleep(INTERVAL_SEC)

        # ── Save to Delta + CSV ───────────────────────────────────────────────────
        if not nifty_df.empty:
            # 1. Save CSV with date in name (local / DBFS fallback)
            csv_path = f"/Workspace/Users/hada.jai.hind@gmail.com/project_MarketMinds/Nifty_data_collection{session_date}.csv"
            nifty_df.to_csv(csv_path)
            print(f"\n💾  CSV saved → {csv_path}")
            # Save Parquet for pipeline use
            parquet_path = f"/Workspace/Users/hada.jai.hind@gmail.com/project_MarketMinds/Nifty_data_collection{session_date}.parquet"
            nifty_df.to_parquet(parquet_path)
            print(f"\n💾  Parquet saved → {parquet_path}")

            # 2. Write to Delta table (runs inside Databricks)
            spark_df = spark.createDataFrame(nifty_df.reset_index())
            spark_df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("nifty_live.session_ticks")
            print(f"✅  Delta write OK → nifty_live.session_ticks ({len(nifty_df)} rows)")

            display(spark.read.table("nifty_live.session_ticks"))

        else:
            print("⚠  No data was collected.")
