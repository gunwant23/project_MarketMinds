# =============================================================================
# market_minds/config/settings.py
# -----------------------------------------------------------------------------
# Centralised configuration with Databricks widget-based runtime overrides.
#
# How widgets work
# ----------------
# When this module is imported inside a Databricks notebook the try/except
# block silently captures dbutils.widgets and exposes every configurable knob
# as a Python constant.  Outside Databricks (unit tests, local runs) the
# except branch falls back to the sensible defaults defined below so the rest
# of the codebase never has to care about the execution environment.
# =============================================================================

from __future__ import annotations

import os
from datetime import date
from typing import Any

# ---------------------------------------------------------------------------
# Helper – safe widget read
# ---------------------------------------------------------------------------

def _get_widget(widget_name: str, default: Any) -> Any:
    """Return the current widget value or *default* if widgets are unavailable."""
    try:
        # dbutils is injected by Databricks; it is never imported explicitly.
        value = dbutils.widgets.get(widget_name)  # type: ignore[name-defined]  # noqa: F821
        return value if value not in ("", None) else default
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Widget registration  (only executed inside a Databricks notebook)
# ---------------------------------------------------------------------------

def register_widgets() -> None:
    """
    Call this once at the top of your Databricks notebook BEFORE importing
    any other market_minds module.  It creates all interactive widgets so the
    analyst can change parameters from the notebook UI without touching code.

    Usage (in a Databricks notebook cell):
        from market_minds.config.settings import register_widgets
        register_widgets()
    """
    try:
        # ── Symbol selector ──────────────────────────────────────────────────
        dbutils.widgets.dropdown(                        # type: ignore[name-defined]  # noqa: F821
            "symbol",
            "^NSEI",                                     # default  → NIFTY 50
            ["^NSEI", "^NSEBANK", "^INDIAVIX"],
            "📈 Symbol",
        )

        # ── Expiry week selector ─────────────────────────────────────────────
        dbutils.widgets.dropdown(                        # type: ignore[name-defined]  # noqa: F821
            "expiry_week",
            "current",
            ["current", "next"],
            "📅 Expiry Week",
        )

        # ── Ingestion interval ───────────────────────────────────────────────
        dbutils.widgets.text(                            # type: ignore[name-defined]  # noqa: F821
            "ingestion_interval_sec",
            "60",
            "⏱ Ingestion Interval (sec)",
        )

        # ── Delta table override ─────────────────────────────────────────────
        dbutils.widgets.text(                            # type: ignore[name-defined]  # noqa: F821
            "delta_table",
            "",                                          # empty → derived automatically
            "🗄 Delta Table (leave blank for auto)",
        )

        # ── Max run cycles (0 = infinite) ────────────────────────────────────
        dbutils.widgets.text(                            # type: ignore[name-defined]  # noqa: F821
            "max_cycles",
            "0",
            "🔁 Max Run Cycles (0 = ∞)",
        )

        # ── Data quality threshold ───────────────────────────────────────────
        dbutils.widgets.text(                            # type: ignore[name-defined]  # noqa: F821
            "quality_threshold",
            "0.95",
            "✅ Quality Threshold (0-1)",
        )

        print("✅ Databricks widgets registered successfully.")
    except Exception:
        # Not inside Databricks – ignore silently.
        pass


# ===========================================================================
# SYMBOL CONFIGURATION
# ===========================================================================

#: Active trading symbol (overridable via widget)
SYMBOL: str = _get_widget("symbol", os.getenv("MM_SYMBOL", "^NSEI"))

#: Human-readable name map – used in table names and log messages
SYMBOL_DISPLAY_NAMES: dict[str, str] = {
    "^NSEI":     "NIFTY50",
    "^NSEBANK":  "BANKNIFTY",
    "^INDIAVIX": "INDIAVIX",
}

SYMBOL_DISPLAY_NAME: str = SYMBOL_DISPLAY_NAMES.get(SYMBOL, SYMBOL.replace("^", ""))

# ===========================================================================
# EXPIRY CONFIGURATION
# ===========================================================================

#: "current" uses the next / this-week Thursday; "next" skips one week.
EXPIRY_WEEK: str = _get_widget("expiry_week", os.getenv("MM_EXPIRY_WEEK", "current"))

# ===========================================================================
# INGESTION CONFIGURATION
# ===========================================================================

#: Seconds between each ingestion cycle
INGESTION_INTERVAL_SEC: int = int(
    _get_widget("ingestion_interval_sec", os.getenv("MM_INTERVAL", "60"))
)

#: 0 = run forever; positive integer = stop after N cycles
MAX_CYCLES: int = int(
    _get_widget("max_cycles", os.getenv("MM_MAX_CYCLES", "0"))
)

#: yfinance download period for each fetch
YFINANCE_PERIOD: str = "1d"

#: yfinance interval granularity
YFINANCE_INTERVAL: str = "1m"

# ===========================================================================
# TIMEZONE
# ===========================================================================

TIMEZONE_IST: str = "Asia/Kolkata"

# ===========================================================================
# MARKET SESSION
# ===========================================================================

#: Market open – NSE equity derivatives
MARKET_OPEN_HOUR: int = 9
MARKET_OPEN_MINUTE: int = 15

#: Market close
MARKET_CLOSE_HOUR: int = 15
MARKET_CLOSE_MINUTE: int = 30

#: Total session length in minutes  (9:15 → 15:30 = 375 min)
SESSION_TOTAL_MINUTES: int = 375

#: Number of equal-length session buckets
SESSION_BUCKETS: int = 6

# ===========================================================================
# DELTA LAKE
# ===========================================================================

#: Databricks catalog / schema prefix  (edit to match your Unity Catalog setup)
DELTA_CATALOG: str = os.getenv("MM_CATALOG", "market_minds")
DELTA_SCHEMA: str = os.getenv("MM_SCHEMA", "raw_market_data")

def _build_table_name() -> str:
    """Derive table name from symbol unless the widget provides an explicit one."""
    widget_override: str = _get_widget("delta_table", "")
    if widget_override:
        return widget_override
    safe_name = SYMBOL_DISPLAY_NAME.lower()
    return f"{DELTA_CATALOG}.{DELTA_SCHEMA}.{safe_name}_1min_candles"

DELTA_TABLE: str = _build_table_name()

#: Write mode for the Delta writer
DELTA_WRITE_MODE: str = "append"

# ===========================================================================
# DATA QUALITY
# ===========================================================================

#: Minimum fraction of non-null columns required for a row to be "good"
QUALITY_THRESHOLD: float = float(
    _get_widget("quality_threshold", os.getenv("MM_QUALITY_THRESHOLD", "0.95"))
)

# ===========================================================================
# LOGGING
# ===========================================================================

LOG_LEVEL: str = os.getenv("MM_LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"

# ===========================================================================
# QUICK SANITY PRINT  (shown when the module is imported interactively)
# ===========================================================================

def print_config() -> None:
    """Pretty-print the active configuration – useful at notebook startup."""
    divider = "─" * 60
    print(divider)
    print("  market_minds  |  Active Configuration")
    print(divider)
    print(f"  Symbol              : {SYMBOL}  ({SYMBOL_DISPLAY_NAME})")
    print(f"  Expiry week         : {EXPIRY_WEEK}")
    print(f"  Ingestion interval  : {INGESTION_INTERVAL_SEC}s")
    print(f"  Max cycles          : {MAX_CYCLES or '∞'}")
    print(f"  Delta table         : {DELTA_TABLE}")
    print(f"  Quality threshold   : {QUALITY_THRESHOLD:.0%}")
    print(f"  Log level           : {LOG_LEVEL}")
    print(divider)