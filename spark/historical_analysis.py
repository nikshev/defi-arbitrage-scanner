"""
PySpark-based historical analysis for the DeFi Arbitrage Scanner.

Loads Parquet snapshots produced by DEXFetcher.save_snapshot(),
computes aggregate statistics, and persists reports.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("PySpark not installed. HistoricalAnalyzer will use pandas fallback.")

import pandas as pd


# ---------------------------------------------------------------------------
# Demo / fallback data generator
# ---------------------------------------------------------------------------

def _generate_demo_dataframe() -> pd.DataFrame:
    """Generate a synthetic snapshot DataFrame for demo purposes."""
    import random

    pairs = ["ETH/USDC", "ETH/DAI", "ETH/WBTC", "USDC/DAI", "WBTC/USDC"]
    dexes = ["uniswap_v2", "uniswap_v3", "sushiswap"]
    base_prices = {
        "ETH/USDC": 3500.0,
        "ETH/DAI": 3500.0,
        "ETH/WBTC": 0.0555,
        "USDC/DAI": 1.0,
        "WBTC/USDC": 63000.0,
    }

    records = []
    now = datetime.now(timezone.utc)
    for hours_back in range(48, 0, -1):
        ts = now - timedelta(hours=hours_back)
        for pair in pairs:
            for dex in dexes:
                spread = random.uniform(-0.005, 0.005)
                records.append(
                    {
                        "timestamp": ts,
                        "pair": pair,
                        "dex": dex,
                        "price": round(base_prices[pair] * (1 + spread), 6),
                    }
                )

    return pd.DataFrame(records)


class HistoricalAnalyzer:
    """
    Analyse historical price snapshots to find recurring arbitrage patterns.

    Parameters
    ----------
    config : dict
        Parsed YAML configuration.  Uses the ``spark`` section.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        spark_conf = config.get("spark", {})
        self.app_name: str = spark_conf.get("app_name", "DeFiArbitrageAnalysis")
        self.snapshots_path = Path(
            spark_conf.get("snapshots_path", "data/snapshots")
        )
        self.report_path = Path(
            spark_conf.get("report_path", "data/reports")
        )
        self._spark: Optional[Any] = None
        self._use_pandas = not SPARK_AVAILABLE

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _get_spark(self) -> Any:
        """Lazily initialise SparkSession."""
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.memory", "1g")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
        return self._spark

    def stop(self) -> None:
        """Stop the SparkSession."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_snapshots(self, path: Optional[Path] = None) -> pd.DataFrame:
        """
        Load all Parquet snapshots from the snapshots directory.

        Falls back to demo data if no snapshots exist.

        Parameters
        ----------
        path : Path, optional  Override the default snapshots directory.

        Returns
        -------
        pd.DataFrame  Combined snapshot data.
        """
        snap_dir = path or self.snapshots_path
        snap_dir = Path(snap_dir)
        parquet_files = list(snap_dir.glob("snapshot_*.parquet"))

        if not parquet_files:
            logger.warning(
                "No snapshots found in %s — using demo data.", snap_dir
            )
            return _generate_demo_dataframe()

        if self._use_pandas:
            frames = [pd.read_parquet(f) for f in parquet_files]
            df = pd.concat(frames, ignore_index=True)
        else:
            try:
                spark = self._get_spark()
                sdf = spark.read.parquet(str(snap_dir / "snapshot_*.parquet"))
                df = sdf.toPandas()
            except Exception as exc:
                logger.warning("Spark unavailable (%s). Falling back to pandas.", exc)
                self._use_pandas = True
                frames = [pd.read_parquet(f) for f in parquet_files]
                df = pd.concat(frames, ignore_index=True)

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        logger.info(
            "Loaded %d rows from %d snapshot files.", len(df), len(parquet_files)
        )
        return df

    def analyze_opportunities(self) -> Dict[str, pd.DataFrame]:
        """
        Run full analysis pipeline.

        Returns
        -------
        dict with keys: ``by_pair``, ``by_hour``, ``spread_summary``
        """
        df = self.load_snapshots()
        return {
            "by_pair": self.aggregate_by_pair(df),
            "by_hour": self.aggregate_by_hour(df),
            "spread_summary": self._compute_spread_summary(df),
        }

    def aggregate_by_pair(
        self, df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Compute per-pair statistics: mean price spread, observation count.

        Parameters
        ----------
        df : pd.DataFrame, optional  Pre-loaded data (loads from disk if None).

        Returns
        -------
        pd.DataFrame  Columns: pair, mean_spread_pct, observation_count.
        """
        if df is None:
            df = self.load_snapshots()

        # Compute max-min spread per snapshot timestamp per pair
        grouped = (
            df.groupby(["timestamp", "pair"])["price"]
            .agg(["max", "min"])
            .reset_index()
        )
        grouped["spread_pct"] = (
            (grouped["max"] - grouped["min"]) / grouped["min"] * 100
        ).round(4)

        result = (
            grouped.groupby("pair")
            .agg(
                mean_spread_pct=("spread_pct", "mean"),
                max_spread_pct=("spread_pct", "max"),
                observation_count=("spread_pct", "count"),
            )
            .reset_index()
            .sort_values("mean_spread_pct", ascending=False)
        )
        return result

    def aggregate_by_hour(
        self, df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Aggregate average spread by hour of day (UTC) to identify best times.

        Parameters
        ----------
        df : pd.DataFrame, optional

        Returns
        -------
        pd.DataFrame  Columns: hour, mean_spread_pct, opportunity_count.
        """
        if df is None:
            df = self.load_snapshots()

        df = df.copy()
        df["hour"] = pd.to_datetime(df["timestamp"], utc=True).dt.hour

        grouped = (
            df.groupby(["hour", "timestamp", "pair"])["price"]
            .agg(["max", "min"])
            .reset_index()
        )
        grouped["spread_pct"] = (
            (grouped["max"] - grouped["min"]) / grouped["min"] * 100
        ).round(4)

        result = (
            grouped.groupby("hour")
            .agg(
                mean_spread_pct=("spread_pct", "mean"),
                opportunity_count=("spread_pct", "count"),
            )
            .reset_index()
            .sort_values("hour")
        )
        return result

    def save_report(
        self, df: pd.DataFrame, path: Optional[Path] = None, name: str = "report"
    ) -> Path:
        """
        Save a report DataFrame as Parquet.

        Parameters
        ----------
        df : pd.DataFrame  Report data.
        path : Path, optional  Override the default report directory.
        name : str  Base filename (without extension).

        Returns
        -------
        Path  Path to the saved report.
        """
        report_dir = path or self.report_path
        report_dir = Path(report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        out_path = report_dir / f"{name}_{ts}.parquet"
        df.to_parquet(out_path, index=False)
        logger.info("Saved report to %s (%d rows)", out_path, len(df))
        return out_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_spread_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute overall spread summary across all pairs and DEXes."""
        pivot = df.pivot_table(
            index=["timestamp", "pair"],
            columns="dex",
            values="price",
            aggfunc="mean",
        ).reset_index()

        dex_cols = [c for c in pivot.columns if c not in ("timestamp", "pair")]
        if len(dex_cols) < 2:
            return pd.DataFrame()

        pivot["max_price"] = pivot[dex_cols].max(axis=1)
        pivot["min_price"] = pivot[dex_cols].min(axis=1)
        pivot["spread_pct"] = (
            (pivot["max_price"] - pivot["min_price"]) / pivot["min_price"] * 100
        ).round(4)

        return pivot[["timestamp", "pair", "spread_pct"] + dex_cols].copy()
