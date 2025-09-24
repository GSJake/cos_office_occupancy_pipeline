#!/usr/bin/env python3
"""
Publish pipeline CSV outputs to Delta tables in Databricks.

Default target:
- dev.jb_off_occ.fact_occupancy_aggregated

Usage examples (Databricks):
- python publish_to_delta.py                                  # publish aggregated to default table
- python publish_to_delta.py --table dev.jb_off_occ.fact_occupancy_aggregated --mode overwrite
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def _abs_file_uri(p: Path) -> str:
    ap = p.resolve()
    # Use file: URI so Spark reads local Workspace file system
    return f"file:{ap}"


def _get_base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        # Databricks/IPython Run File
        return Path.cwd()


def publish_fact_occupancy_aggregated(table: str, mode: str = "overwrite") -> None:
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # Ensure database exists
    db = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    base = _get_base_dir()
    csv_path = base / "facts" / "FactOccupancyAggregated.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}. Run the pipeline first (stages 1-9).")

    df = (
        spark.read.option("header", True).option("inferSchema", True).csv(_abs_file_uri(csv_path))
    )

    # Cast columns explicitly for consistency
    df = (
        df
        .withColumn("date_key", F.col("date_key").cast("int"))
        .withColumn("location_key", F.col("location_key").cast("int"))
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("month", F.col("month").cast("int"))
        .withColumn("is_weekend", F.col("is_weekend").cast("boolean"))
        .withColumn("attendance_count", F.col("attendance_count").cast("int"))
        .withColumn("deskcount", F.col("deskcount").cast("int"))
        .withColumn("occupancy_rate", F.col("occupancy_rate").cast("double"))
        .withColumn("is_hybrid_day", F.col("is_hybrid_day").cast("boolean"))
    )

    # Force a clean overwrite to avoid any stale data issues
    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    (df.write.mode("overwrite" if mode == "overwrite" else mode)
        .format("delta").option("overwriteSchema", "true").saveAsTable(table))

    # Refresh table metadata and caches
    spark.sql(f"REFRESH TABLE {table}")

    print(f"Published {df.count():,} rows to {table} (mode={mode})")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish CSV outputs to Delta tables")
    p.add_argument("--table", default="dev.jb_off_occ.fact_occupancy_aggregated", help="Target table name")
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Write mode")
    # Tolerate IPython/Databricks injected args like '-f <json>'
    args, _ = p.parse_known_args()
    return args


def main() -> int:
    args = parse_args()
    publish_fact_occupancy_aggregated(args.table, args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
