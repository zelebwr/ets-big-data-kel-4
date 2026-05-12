#!/usr/bin/env python3
"""PySpark analysis pipeline for NewsPulse.

The job reads raw Kafka ingestion output directly from HDFS, computes the
aggregations used by the dashboard, and writes a processed JSON snapshot to
dashboard/data/spark_results.json.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    asc,
    col,
    coalesce,
    concat_ws,
    count,
    desc,
    explode,
    hour,
    length,
    lit,
    lower,
    regexp_replace,
    row_number,
    split,
    trim,
    to_timestamp,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_OUTPUT_PATH = os.path.join(ROOT_DIR, "dashboard", "data", "spark_results.json")
DEFAULT_HDFS_BASE = os.getenv("NEWS_HDFS_BASE", "hdfs://localhost:8020/data/news")

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

STOPWORDS = {
    "dan",
    "yang",
    "untuk",
    "dari",
    "pada",
    "dengan",
    "ke",
    "di",
    "atau",
    "akan",
    "jadi",
    "para",
    "the",
    "of",
    "in",
    "to",
}


def build_spark_session() -> SparkSession:
    hdfs_root = DEFAULT_HDFS_BASE.split("/data/news")[0] or "hdfs://localhost:8020"
    return (
        SparkSession.builder.appName("NewsPulse Analysis")
        .master("local[*]")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.fs.defaultFS", hdfs_root)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def read_hdfs_json(spark: SparkSession, path: str):
    try:
        return spark.read.schema(NEWS_SCHEMA).option("multiLine", "true").json(path)
    except Exception:
        return spark.createDataFrame([], NEWS_SCHEMA)


def ensure_columns(df, columns):
    for column_name in columns:
        if column_name not in df.columns:
            df = df.withColumn(column_name, lit(None).cast(StringType()))
    return df


def parse_timestamp_column(df):
    timestamp_patterns = [
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "yyyy-MM-dd'T'HH:mm:ssXXX",
        "yyyy-MM-dd HH:mm:ss",
        "EEE, dd MMM yyyy HH:mm:ss z",
        "EEE, dd MMM yyyy HH:mm:ss Z",
    ]

    parsed_candidates = []
    for column_name in ("timestamp", "waktu_terbit"):
        for pattern in timestamp_patterns:
            parsed_candidates.append(to_timestamp(col(column_name), pattern))

    return coalesce(*parsed_candidates)


def normalize_news(df, channel_name):
    base = ensure_columns(df, NEWS_SCHEMA.fieldNames())
    normalized = base.select(*NEWS_SCHEMA.fieldNames()).withColumn("channel", lit(channel_name))
    normalized = normalized.withColumn("judul", trim(col("judul")))
    normalized = normalized.withColumn("sumber", trim(col("sumber")))
    normalized = normalized.withColumn("url", trim(col("url")))
    normalized = normalized.withColumn("deskripsi", trim(col("deskripsi")))
    normalized = normalized.withColumn("image", trim(col("image")))
    normalized = normalized.withColumn("thumbnail", trim(col("thumbnail")))
    normalized = normalized.withColumn("waktu_terbit", trim(col("waktu_terbit")))
    normalized = normalized.withColumn("timestamp", trim(col("timestamp")))
    normalized = normalized.withColumn("parsed_timestamp", parse_timestamp_column(normalized))
    return normalized


def build_combined_news(api_df, rss_df):
    combined = api_df.unionByName(rss_df, allowMissingColumns=True)
    dedup_key = when(
        length(trim(col("url"))) > 0,
        trim(col("url")),
    ).otherwise(
        concat_ws(
            "||",
            coalesce(col("judul"), lit("")),
            coalesce(col("sumber"), lit("")),
            coalesce(col("waktu_terbit"), lit("")),
            coalesce(col("timestamp"), lit("")),
        )
    )
    window_spec = Window.partitionBy(dedup_key).orderBy(
        col("parsed_timestamp").desc_nulls_last(),
        col("timestamp").desc_nulls_last(),
        col("url").desc_nulls_last(),
    )

    return (
        combined.withColumn("dedup_key", dedup_key)
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number", "dedup_key")
    )


def compute_trending(news_df, top_n=15):
    tokenized = (
        news_df.select(
            explode(
                split(
                    regexp_replace(lower(coalesce(col("judul"), lit(""))), r"[^A-Za-z0-9\s]+", " "),
                    r"\s+",
                )
            ).alias("kata")
        )
        .filter(col("kata") != "")
        .filter(~col("kata").isin(sorted(STOPWORDS)))
        .filter(length(col("kata")) > 1)
    )

    return (
        tokenized.groupBy("kata")
        .agg(count("*").alias("frekuensi"))
        .orderBy(desc("frekuensi"), asc("kata"))
        .limit(top_n)
    )


def compute_source_distribution(news_df):
    return (
        news_df.groupBy(coalesce(col("sumber"), lit("Unknown")).alias("sumber"))
        .agg(count("*").alias("jumlah"))
        .orderBy(desc("jumlah"), asc("sumber"))
    )


def compute_hourly_volume(news_df):
    hourly = news_df.withColumn("jam", hour(col("parsed_timestamp")))
    return (
        hourly.filter(col("jam").isNotNull())
        .groupBy("jam")
        .agg(count("*").alias("jumlah_berita"))
        .orderBy("jam")
    )


def collect_rows(df):
    return [row.asDict(recursive=True) for row in df.collect()]


def build_payload(spark, hdfs_base, max_news):
    api_df = normalize_news(read_hdfs_json(spark, f"{hdfs_base}/api"), "api")
    rss_df = normalize_news(read_hdfs_json(spark, f"{hdfs_base}/rss"), "rss")
    combined_news = build_combined_news(api_df, rss_df)

    processed_news = (
        combined_news.orderBy(
            col("parsed_timestamp").desc_nulls_last(),
            col("timestamp").desc_nulls_last(),
            col("url").desc_nulls_last(),
        )
        .limit(max_news)
        .select(
            "judul",
            "sumber",
            "url",
            "deskripsi",
            "image",
            "thumbnail",
            "waktu_terbit",
            "timestamp",
            "channel",
        )
    )

    trending = compute_trending(combined_news)
    sources = compute_source_distribution(combined_news)
    volume = compute_hourly_volume(combined_news)
    hourly_lookup = {row["jam"]: row["jumlah_berita"] for row in collect_rows(volume)}

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "kata_trending": collect_rows(trending),
        "distribusi_sumber": collect_rows(sources),
        "volume_per_jam": [
            {"jam": hour_index, "jumlah_berita": int(hourly_lookup.get(hour_index, 0))}
            for hour_index in range(24)
        ],
        "live_news": collect_rows(processed_news),
        "total_api": int(api_df.count()),
        "total_rss": int(rss_df.count()),
    }


def write_payload(output_path, payload):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def run_once(hdfs_base=DEFAULT_HDFS_BASE, output_path=DEFAULT_OUTPUT_PATH, max_news=50):
    spark = build_spark_session()
    try:
        payload = build_payload(spark, hdfs_base, max_news)
        write_payload(output_path, payload)
        print(f"[DONE] Wrote processed dashboard data to {output_path}")
    finally:
        spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Run NewsPulse Spark analysis.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path untuk output JSON dashboard")
    parser.add_argument("--hdfs-base", default=DEFAULT_HDFS_BASE, help="Base HDFS path untuk data mentah")
    parser.add_argument("--max-news", type=int, default=50, help="Maksimal berita terbaru yang disimpan")
    parser.add_argument("--interval", type=int, default=120, help="Interval detik saat mode watch aktif")
    parser.add_argument("--watch", action="store_true", help="Jalankan terus-menerus dengan interval tertentu")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.watch:
        run_once(args.hdfs_base, args.output, args.max_news)
        return

    print(f"[WATCH] Spark analysis berjalan setiap {args.interval} detik.")
    while True:
        started_at = datetime.utcnow().isoformat() + "Z"
        print(f"[WATCH] Mulai run pada {started_at}")
        try:
            run_once(args.hdfs_base, args.output, args.max_news)
        except Exception as exc:
            print(f"[WATCH] Run gagal: {exc}")
        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
