#!/usr/bin/env python3
"""
Silver Layer for NewsPulse Data Lakehouse

Transform raw Bronze data into cleaned, quality-assured Silver layer.
Performs:
1. Deduplication (by URL)
2. Type casting & Timestamp parsing
3. Text normalization & null handling

This layer is the source of truth for clean analytics data.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    hour,
    lit,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import StringType

try:
    from delta import configure_spark_with_delta_pip
except ImportError:
    print("ERROR: delta-spark not installed. Run: pip install delta-spark")
    sys.exit(1)


# Paths
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_BRONZE_PATH = os.path.join(ROOT_DIR, "lakehouse_data", "bronze", "news")
DEFAULT_SILVER_OUTPUT = os.path.join(ROOT_DIR, "lakehouse_data", "silver", "news")
DEFAULT_SPARK_MASTER = os.getenv("NEWS_SPARK_MASTER", "local[1]")

# Timestamp parsing patterns (from original spark_analysis.py)
TIMESTAMP_PATTERNS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd HH:mm:ss",
    "EEE, dd MMM yyyy HH:mm:ss z",
    "EEE, dd MMM yyyy HH:mm:ss Z",
]


def build_spark_session() -> SparkSession:
    """Initialize Spark session with Delta Lake support."""
    try:
        builder = SparkSession.builder.appName("Silver-NewsPulse") \
            .master(DEFAULT_SPARK_MASTER) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", 
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.ansi.enabled", "false")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        return spark
    except Exception as e:
        print(f"ERROR initializing Spark: {e}")
        sys.exit(1)


def parse_timestamp_column(df):
    """
    Parse timestamp column into proper TimestampType.
    Tries multiple patterns and takes first successful parse.
    """
    from pyspark.sql.functions import coalesce
    
    parsed_candidates = []
    
    # Try parsing timestamp column
    for pattern in TIMESTAMP_PATTERNS:
        try:
            parsed_candidates.append(to_timestamp(col("timestamp"), pattern))
        except:
            pass
    
    # Try parsing waktu_terbit column
    for pattern in TIMESTAMP_PATTERNS:
        try:
            parsed_candidates.append(to_timestamp(col("waktu_terbit"), pattern))
        except:
            pass
    
    if not parsed_candidates:
        print("WARNING: Could not create any timestamp parsers, using null")
        return lit(None)
    
    return coalesce(*parsed_candidates)


def clean_silver(
    spark: SparkSession,
    bronze_path: str,
    output_path: str,
):
    """
    Clean and transform Bronze layer data into Silver layer.
    
    Transformations:
    1. DEDUPLICATE - Remove duplicate records (by URL)
    2. TYPE_CASTING - Parse timestamps to proper TimestampType
    3. NORMALIZATION - Trim whitespace, fill nulls, extract hour
    """
    
    print("[SILVER] Starting cleaning transformations...")
    print(f"[SILVER] Input: {bronze_path}")
    print(f"[SILVER] Output: {output_path}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Read Bronze layer
    print("[SILVER] Reading Bronze data...")
    bronze = spark.read.format("delta").load(bronze_path)
    bronze_count = bronze.count()
    print(f"[SILVER] Bronze records: {bronze_count}")
    
    # TRANSFORMATION 1: DEDUPLICATE
    print("\n[SILVER] Transformation 1: Deduplication")
    print("[SILVER] Strategy: dropDuplicates(['url'])")
    print("[SILVER] Reason: URL is unique identifier for news article")
    
    dedup = bronze.dropDuplicates(["url"])
    dedup_count = dedup.count()
    duplicates_removed = bronze_count - dedup_count
    
    print(f"[SILVER] Records removed: {duplicates_removed} ({duplicates_removed/bronze_count*100:.1f}%)")
    print(f"[SILVER] Records after dedup: {dedup_count}")
    
    # TRANSFORMATION 2: TYPE CASTING & TIMESTAMP PARSING
    print("\n[SILVER] Transformation 2: Type Casting & Timestamp Parsing")
    print("[SILVER] Strategy: Parse 'timestamp' and 'waktu_terbit' columns")
    print("[SILVER] Reason: Enable temporal analysis (hourly aggregation, window functions)")
    
    dedup_parsed = dedup.withColumn("parsed_timestamp", parse_timestamp_column(dedup))
    
    # Count how many timestamps were successfully parsed
    parsed_count = dedup_parsed.filter(col("parsed_timestamp").isNotNull()).count()
    null_timestamps = dedup_count - parsed_count
    
    print(f"[SILVER] Timestamps parsed successfully: {parsed_count}/{dedup_count}")
    if null_timestamps > 0:
        print(f"[SILVER] WARNING: {null_timestamps} records have NULL timestamp")
    
    # Extract hour from parsed timestamp
    dedup_parsed = dedup_parsed.withColumn("jam", hour(col("parsed_timestamp")))
    
    # TRANSFORMATION 3: TEXT NORMALIZATION & NULL HANDLING
    print("\n[SILVER] Transformation 3: Text Normalization & Null Handling")
    print("[SILVER] Strategy: trim() + fillna()")
    print("[SILVER] Reason: Ensure consistent string values for grouping/join operations")
    
    normalized = dedup_parsed \
        .withColumn("judul", trim(col("judul"))) \
        .withColumn("sumber", trim(col("sumber"))) \
        .withColumn("url", trim(col("url"))) \
        .withColumn("deskripsi", trim(col("deskripsi"))) \
        .withColumn("image", trim(col("image"))) \
        .withColumn("thumbnail", trim(col("thumbnail"))) \
        .withColumn("waktu_terbit", trim(col("waktu_terbit"))) \
        .withColumn("timestamp", trim(col("timestamp")))
    
    # Fill nulls in sumber with "Unknown"
    null_sumber_before = normalized.filter(col("sumber").isNull()).count()
    normalized = normalized.withColumn("sumber", 
                                       coalesce(col("sumber"), lit("Unknown")))
    
    print(f"[SILVER] Null values in 'sumber' field: {null_sumber_before}")
    print(f"[SILVER] Replaced with 'Unknown': {null_sumber_before}")
    
    # Final count
    final_count = normalized.count()
    print(f"\n[SILVER] Final record count: {final_count}")
    
    # Write to Delta Lake (Silver layer)
    print("\n[SILVER] Writing to Delta Lake...")
    try:
        normalized.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        print(f"[DONE] Silver layer created successfully at {output_path}")
        
        # Show schema
        print("\n[SILVER] Schema:")
        normalized.printSchema()
        
        # Show sample data
        print("\n[SILVER] Sample data (first 3 records):")
        normalized.select("judul", "sumber", "url", "jam", "parsed_timestamp", "_ingested_at", "_source") \
                 .limit(3) \
                 .show(truncate=False)
        
        # Data Quality Report
        print("\n[SILVER] === DATA QUALITY REPORT ===")
        print(f"Bronze Input:        {bronze_count} records")
        print(f"After Dedup:         {dedup_count} records (-{duplicates_removed})")
        print(f"After Parse:         {parsed_count} with valid timestamp")
        print(f"After Normalize:     {final_count} records")
        print(f"Data Loss:           {(bronze_count - final_count) / bronze_count * 100:.1f}%")
        print(f"  - Duplicates:      {duplicates_removed}")
        print(f"  - Null sumber:     {null_sumber_before} (replaced, not dropped)")
        
        return True
    except Exception as e:
        print(f"[ERROR] Failed to write Silver layer: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Clean Bronze layer data into Silver Delta Layer"
    )
    parser.add_argument(
        "--bronze-path",
        default=DEFAULT_BRONZE_PATH,
        help=f"Input path for Bronze Delta table (default: {DEFAULT_BRONZE_PATH})"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_SILVER_OUTPUT,
        help=f"Output path for Silver Delta table (default: {DEFAULT_SILVER_OUTPUT})"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Spark logging level"
    )
    
    args = parser.parse_args()
    
    # Verify Bronze path exists
    if not os.path.exists(args.bronze_path):
        print(f"ERROR: Bronze path does not exist: {args.bronze_path}")
        print("Please run 01_bronze.py first")
        sys.exit(1)
    
    # Build Spark session
    spark = build_spark_session()
    spark.sparkContext.setLogLevel(args.log_level)
    
    print(f"\n[SPARK] PySpark version: {spark.version}")
    print(f"[SPARK] Session initialized at {datetime.utcnow().isoformat()}Z\n")
    
    # Run cleaning
    success = clean_silver(
        spark,
        bronze_path=args.bronze_path,
        output_path=args.output,
    )
    
    # Cleanup
    spark.stop()
    print("\n[SPARK] Session stopped")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
