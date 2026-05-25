#!/usr/bin/env python3
"""
Bronze Layer for NewsPulse Data Lakehouse

Ingest raw JSON data from HDFS (or local fallback) into Delta Lake format.
Adds metadata columns: _ingested_at, _source

Schema enforcement happens at ingestion point.
Raw data is preserved without transformation (except adding metadata).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType

try:
    from delta import configure_spark_with_delta_pip
except ImportError:
    print("ERROR: delta-spark not installed. Run: pip install delta-spark")
    sys.exit(1)


# Paths
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_BRONZE_OUTPUT = os.path.join(ROOT_DIR, "lakehouse_data", "bronze", "news")
DEFAULT_HDFS_BASE = os.getenv("NEWS_HDFS_BASE", "hdfs://localhost:8020/data/news")
DEFAULT_SPARK_MASTER = os.getenv("NEWS_SPARK_MASTER", "local[1]")

# Schema definition (same as ETS)
NEWS_SCHEMA = StructType(
    [
        StructField("judul", StringType(), True),
        StructField("sumber", StringType(), True),
        StructField("url", StringType(), True),
        StructField("deskripsi", StringType(), True),
        StructField("image", StringType(), True),
        StructField("thumbnail", StringType(), True),
        StructField("waktu_terbit", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def build_spark_session(use_local: bool = False, hdfs_base: str = DEFAULT_HDFS_BASE) -> SparkSession:
    """Initialize Spark session with Delta Lake support.

    In local mode we avoid forcing an HDFS defaultFS so file paths are resolved
    to the local filesystem instead of hdfs://localhost:8020.
    """
    try:
        builder = SparkSession.builder.appName("Bronze-NewsPulse") \
            .master(DEFAULT_SPARK_MASTER) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", 
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.ansi.enabled", "false")

        if not use_local:
            builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_base.split("/data/news")[0])
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        return spark
    except Exception as e:
        print(f"ERROR initializing Spark: {e}")
        sys.exit(1)


def read_hdfs_json(spark: SparkSession, path: str, use_local: bool = False) -> any:
    """
    Read JSON from HDFS with fallback to empty dataframe.
    If use_local=True, attempt to read from local filesystem instead.
    """
    try:
        if use_local:
            # Try reading from local path
            if os.path.exists(path):
                return spark.read.option("multiLine", "true").json(path)
            else:
                print(f"WARNING: Local path {path} not found, returning empty dataframe")
                return spark.createDataFrame([], NEWS_SCHEMA)
        else:
            # Try HDFS
            return spark.read.option("multiLine", "true").json(path)
    except Exception as e:
        print(f"WARNING: Could not read {path}: {e}")
        print("         Returning empty dataframe for this source")
        return spark.createDataFrame([], NEWS_SCHEMA)


def ingest_to_bronze(
    spark: SparkSession,
    hdfs_base: str,
    output_path: str,
    use_local: bool = False,
):
    """
    Ingest API and RSS data from HDFS into Bronze Delta Lake.
    
    Process:
    1. Read API JSON from HDFS
    2. Read RSS JSON from HDFS
    3. Union them together
    4. Add metadata columns: _ingested_at, _source
    5. Write to Delta Lake format
    """
    
    print("[BRONZE] Starting ingestion...")
    print(f"[BRONZE] HDFS Base: {hdfs_base}")
    print(f"[BRONZE] Output: {output_path}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Read from HDFS (API)
    print("[BRONZE] Reading API data...")
    api_path = f"{hdfs_base}/api"
    api_df = read_hdfs_json(spark, api_path, use_local)
    api_count = api_df.count()
    print(f"[BRONZE] API records: {api_count}")
    
    # Add metadata for API
    api_bronze = api_df.withColumn("_ingested_at", current_timestamp()) \
                       .withColumn("_source", lit("api"))
    
    # Read from HDFS (RSS)
    print("[BRONZE] Reading RSS data...")
    rss_path = f"{hdfs_base}/rss"
    rss_df = read_hdfs_json(spark, rss_path, use_local)
    rss_count = rss_df.count()
    print(f"[BRONZE] RSS records: {rss_count}")
    
    # Add metadata for RSS
    rss_bronze = rss_df.withColumn("_ingested_at", current_timestamp()) \
                       .withColumn("_source", lit("rss"))
    
    # Union both sources
    print("[BRONZE] Combining API and RSS data...")
    combined = api_bronze.unionByName(rss_bronze, allowMissingColumns=True)
    total_count = combined.count()
    print(f"[BRONZE] Total records before write: {total_count}")
    
    # Ensure all required columns exist
    for column_name in NEWS_SCHEMA.fieldNames():
        if column_name not in combined.columns:
            combined = combined.withColumn(column_name, lit(None).cast(StringType()))
    
    # Write to Delta Lake (Bronze layer)
    print("[BRONZE] Writing to Delta Lake...")
    try:
        combined.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        print(f"[DONE] Bronze layer created successfully at {output_path}")
        print(f"[DONE] Total records ingested: {total_count}")
        
        # Show schema
        print("\n[BRONZE] Schema:")
        combined.printSchema()
        
        # Show sample data
        print("\n[BRONZE] Sample data (first 3 records):")
        combined.limit(3).show(truncate=False)
        
        return True
    except Exception as e:
        print(f"[ERROR] Failed to write Bronze layer: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Ingest raw JSON from HDFS to Bronze Delta Layer"
    )
    parser.add_argument(
        "--hdfs-base",
        default=DEFAULT_HDFS_BASE,
        help="Base HDFS path for raw data (default: hdfs://localhost:8020/data/news)"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_BRONZE_OUTPUT,
        help=f"Output path for Bronze Delta table (default: {DEFAULT_BRONZE_OUTPUT})"
    )
    parser.add_argument(
        "--use-local",
        action="store_true",
        help="Read from local filesystem instead of HDFS (for testing)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Spark logging level"
    )
    
    args = parser.parse_args()
    
    # Build Spark session
    spark = build_spark_session(use_local=args.use_local, hdfs_base=args.hdfs_base)
    spark.sparkContext.setLogLevel(args.log_level)
    
    print(f"\n[SPARK] PySpark version: {spark.version}")
    print(f"[SPARK] Session initialized at {datetime.utcnow().isoformat()}Z\n")
    
    # Run ingestion
    success = ingest_to_bronze(
        spark,
        hdfs_base=args.hdfs_base,
        output_path=args.output,
        use_local=args.use_local,
    )
    
    # Cleanup
    spark.stop()
    print("\n[SPARK] Session stopped")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
