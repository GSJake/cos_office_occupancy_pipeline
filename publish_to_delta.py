#!/usr/bin/env python3
"""
Publish pipeline CSV outputs to Delta tables in Databricks.

Default targets:
- dev.jb_off_occ.fact_occupancy_aggregated
- dev.jb_off_occ.dim_location

Usage examples (Databricks):
- python publish_to_delta.py                                  # publish all tables
- python publish_to_delta.py --table dev.jb_off_occ.fact_occupancy_aggregated --mode overwrite
- python publish_to_delta.py --mode append
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.utils import AnalysisException

# -----------------------------------------------------------------------------
# Canonical schema for dev.jb_off_occ.fact_occupancy_aggregated
# -----------------------------------------------------------------------------
FACT_SCHEMA = T.StructType([
    T.StructField("date_key", T.IntegerType(), False),
    T.StructField("location_key", T.IntegerType(), False),
    T.StructField("date", T.DateType(), True),
    T.StructField("office_location", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True),
    T.StructField("month", T.IntegerType(), True),
    T.StructField("is_weekend", T.BooleanType(), True),
    T.StructField("attendance_count", T.IntegerType(), True),
    T.StructField("deskcount", T.IntegerType(), True),
    T.StructField("occupancy_rate", T.DoubleType(), True),
    T.StructField("is_hybrid_day", T.BooleanType(), True),
])

CANONICAL_COLUMNS = [f.name for f in FACT_SCHEMA]

# -----------------------------------------------------------------------------
# Canonical schema for dev.jb_off_occ.dim_location
# -----------------------------------------------------------------------------
DIM_LOCATION_SCHEMA = T.StructType([
    T.StructField("location_key", T.IntegerType(), False),
    T.StructField("office_location", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("RSF", T.IntegerType(), True),
    T.StructField("date", T.DateType(), True),
])

DIM_LOCATION_COLUMNS = [f.name for f in DIM_LOCATION_SCHEMA]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _abs_file_uri(p: Path) -> str:
    ap = p.resolve()
    return f"file:{ap}"


def _get_base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:  # Databricks/IPython Run File
        return Path.cwd()


def _table_exists(spark: SparkSession, full_name: str) -> bool:
    try:
        spark.table(full_name).limit(1).collect()
        return True
    except AnalysisException:
        return False


def _normalize_bool(col: F.Column) -> F.Column:
    """Robust boolean parser: accepts true/false/1/0/yes/no (case-insensitive)."""
    c = F.lower(F.trim(col))
    return F.when(c.isNull(), None) \
            .when(c.isin("true", "t", "1", "yes", "y"), F.lit(True)) \
            .when(c.isin("false", "f", "0", "no", "n"), F.lit(False)) \
            .otherwise(None)


def _align_to_fact_schema_from_strings(df: DataFrame) -> DataFrame:
    """Cast/select columns from a DataFrame whose columns are read as strings.

    This handles common CSV scenarios where booleans and numbers arrive as text.
    If `is_weekend` is missing, derive it from `date`.
    """
    cols = df.columns
    tmp = df

    # Integers
    for c in ["date_key", "location_key", "year", "month", "attendance_count", "deskcount"]:
        if c in cols:
            tmp = tmp.withColumn(c, F.col(c).cast("int"))
        else:
            tmp = tmp.withColumn(c, F.lit(None).cast("int"))

    # Date
    if "date" in cols:
        iso = F.to_date(F.col("date"))
        alt1 = F.to_date(F.col("date"), "yyyy-MM-dd")
        alt2 = F.to_date(F.col("date"), "MM/dd/yyyy")
        tmp = tmp.withColumn("date", F.coalesce(iso, alt1, alt2))
    else:
        tmp = tmp.withColumn("date", F.lit(None).cast("date"))

    # Strings
    if "office_location" not in cols:
        tmp = tmp.withColumn("office_location", F.lit(None).cast("string"))
    else:
        tmp = tmp.withColumn("office_location", F.col("office_location").cast("string"))

    # Doubles
    if "occupancy_rate" in cols:
        tmp = tmp.withColumn(
            "occupancy_rate",
            F.regexp_replace(F.col("occupancy_rate").cast("string"), "%", "").cast("double")
        )
    else:
        tmp = tmp.withColumn("occupancy_rate", F.lit(None).cast("double"))

    # Booleans: is_weekend & is_hybrid_day
    # Robust bool normalizer
    def _norm_bool(cname: str) -> F.Column:
        c = F.lower(F.trim(F.col(cname).cast("string")))
        return F.when(F.col(cname).isNull(), None) \
                .when(c.isin("true", "t", "1", "yes", "y"), F.lit(True)) \
                .when(c.isin("false", "f", "0", "no", "n"), F.lit(False)) \
                .otherwise(None)

    # is_hybrid_day: parse if present, else null
    if "is_hybrid_day" in cols:
        tmp = tmp.withColumn("is_hybrid_day", _norm_bool("is_hybrid_day"))
    else:
        tmp = tmp.withColumn("is_hybrid_day", F.lit(None).cast("boolean"))

    # is_weekend: parse if present; otherwise derive from date if available
    if "is_weekend" in cols:
        tmp = tmp.withColumn("is_weekend", _norm_bool("is_weekend"))
    else:
        # Spark dayofweek: 1=Sunday ... 7=Saturday
        tmp = tmp.withColumn(
            "is_weekend",
            F.when(F.col("date").isNull(), F.lit(None).cast("boolean"))
             .otherwise(F.dayofweek(F.col("date")).isin(1, 7))
        )

    return tmp.select(CANONICAL_COLUMNS)


def _align_to_existing_table_schema(df: DataFrame, target_table: str, spark: SparkSession) -> DataFrame:
    target_schema = spark.table(target_table).schema
    select_expr = []
    for f in target_schema:
        if f.name in df.columns:
            select_expr.append(F.col(f.name).cast(f.dataType).alias(f.name))
        else:
            select_expr.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.select(*select_expr)


def _align_to_dim_location_schema(df: DataFrame) -> DataFrame:
    """Cast/select columns for DimLocation table."""
    cols = df.columns
    tmp = df

    # Integers
    for c in ["location_key", "RSF"]:
        if c in cols:
            tmp = tmp.withColumn(c, F.col(c).cast("int"))
        else:
            tmp = tmp.withColumn(c, F.lit(None).cast("int"))

    # Strings
    for c in ["office_location", "city", "state", "country", "region"]:
        if c in cols:
            tmp = tmp.withColumn(c, F.col(c).cast("string"))
        else:
            tmp = tmp.withColumn(c, F.lit(None).cast("string"))

    # Date
    if "date" in cols:
        iso = F.to_date(F.col("date"))
        alt1 = F.to_date(F.col("date"), "yyyy-MM-dd")
        alt2 = F.to_date(F.col("date"), "MM/dd/yyyy")
        tmp = tmp.withColumn("date", F.coalesce(iso, alt1, alt2))
    else:
        tmp = tmp.withColumn("date", F.lit(None).cast("date"))

    return tmp.select(DIM_LOCATION_COLUMNS)


# -----------------------------------------------------------------------------
# Publisher
# -----------------------------------------------------------------------------

def publish_fact_occupancy_aggregated(table: str, mode: str = "overwrite") -> None:
    spark = SparkSession.getActiveSession() or SparkSession.builder.enableHiveSupport().getOrCreate()

    # Ensure database (catalog.schema) exists
    if "." in table:
        db = table.rsplit(".", 1)[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    base = _get_base_dir()
    csv_path = base / "facts" / "FactOccupancyAggregated.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}. Run the pipeline first (stages 1-9).")

    # Read CSV as strings to normalize/parse ourselves
    # Use inferSchema to read actual CSV columns by name (not position)
    # This handles cases where CSV may be missing columns like is_weekend
    raw = (
        spark.read
             .option("header", True)
             .option("inferSchema", False)  # Keep all as strings for custom parsing
             .csv(_abs_file_uri(csv_path))
    )

    df = _align_to_fact_schema_from_strings(raw)

    if mode == "overwrite":
        # Clean replacement with schema overwrite
        (df.write
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .format("delta")
           .saveAsTable(table))
    else:  # append
        if _table_exists(spark, table):
            df = _align_to_existing_table_schema(df, table, spark)
            df.write.mode("append").format("delta").saveAsTable(table)
        else:
            # First write creates the table
            (df.write
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .format("delta")
               .saveAsTable(table))

    spark.sql(f"REFRESH TABLE {table}")

    rows = df.count()
    print(f"Published {rows:,} rows to {table} (mode={mode})")


def publish_dim_location(table: str = "dev.jb_off_occ.dim_location", mode: str = "overwrite") -> None:
    """Publish DimLocation.csv to a Delta table."""
    spark = SparkSession.getActiveSession() or SparkSession.builder.enableHiveSupport().getOrCreate()

    # Ensure database (catalog.schema) exists
    if "." in table:
        db = table.rsplit(".", 1)[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    base = _get_base_dir()
    csv_path = base / "dimensions" / "DimLocation.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}. Run the pipeline first (stages 1-6).")

    raw = (
        spark.read
             .option("header", True)
             .option("inferSchema", False)
             .csv(_abs_file_uri(csv_path))
    )

    df = _align_to_dim_location_schema(raw)

    if mode == "overwrite":
        (df.write
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .format("delta")
           .saveAsTable(table))
    else:  # append
        if _table_exists(spark, table):
            df = _align_to_existing_table_schema(df, table, spark)
            df.write.mode("append").format("delta").saveAsTable(table)
        else:
            (df.write
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .format("delta")
               .saveAsTable(table))

    spark.sql(f"REFRESH TABLE {table}")

    rows = df.count()
    print(f"Published {rows:,} rows to {table} (mode={mode})")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish CSV outputs to Delta tables")
    p.add_argument("--table", default="dev.jb_off_occ.fact_occupancy_aggregated", help="Target table name")
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Write mode")
    args, _ = p.parse_known_args()
    return args


def main() -> int:
    args = parse_args()
    publish_fact_occupancy_aggregated(args.table, args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
