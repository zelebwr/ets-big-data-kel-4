#!/usr/bin/env python3
"""
Gold Layer for NewsPulse Data Lakehouse

Create aggregated tables for dashboard and analytics:

Reproducing ETS Analysis:
1. word_frequency - Top 15 trending words (from titles)
2. news_per_source - News distribution by source

Enhanced Analysis (NEW - not in ETS):
3. word_velocity - Words trending upward by velocity (rapid frequency increase)
4. cross_source_topics - Topics appearing in both API and RSS with correlation

This layer enables advanced analytics impossible with raw JSON ingestion.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    asc,
    coalesce,
    col,
    count,
    desc,
    explode,
    lag,
    length,
    lit,
    lower,
    regexp_replace,
    split,
    trim,
)

try:
    from delta import configure_spark_with_delta_pip
except ImportError:
    print("ERROR: delta-spark not installed. Run: pip install delta-spark")
    sys.exit(1)


# Paths
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_SILVER_PATH = os.path.join(ROOT_DIR, "lakehouse_data", "silver", "news")
DEFAULT_GOLD_OUTPUT = os.path.join(ROOT_DIR, "lakehouse_data", "gold")
DEFAULT_SPARK_MASTER = os.getenv("NEWS_SPARK_MASTER", "local[1]")

# Try to import Sastrawi for additional stopwords
try:
    from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
    HAS_SASTRAWI = True
except ImportError:
    HAS_SASTRAWI = False

# Base stopwords (Indonesian + English)
BASE_STOPWORDS = {
    # Indonesian stopwords - kata sambung dasar
    "dan", "yang", "untuk", "dari", "pada", "dengan", "ke", "di", "atau",
    "akan", "jadi", "para", "ini", "itu", "ada", "tak", "tidak", "bukan",
    "agar", "karena", "sebab", "saat", "hingga", "dalam", "oleh", "sebagai",
    "juga", "masih", "saja", "lebih", "sudah", "belum", "pun", "lah", "nya",
    "per", "bagi", "tanpa", "atas", "bawah", "antara", "selain",
    # Indonesian stopwords - kata sambung/keterangan tambahan
    "besar", "kecil", "banyak", "sedikit", "baru", "lama", "tahun", "orang",
    "hari", "waktu", "tempat", "hal", "cara", "bisa", "dapat", "harus",
    "ialah", "yaitu", "yakni", "adalah", "merupakan", "tersebut", "demikian",
    "begitu", "apa", "bagaimana", "berapa", "dimana", "kapan", "siapa",
    "kenapa", "mengapa", "kalau", "jika", "bila", "andai", "apabila",
    "ketika", "sebelum", "sesudah", "setelah", "selama", "sementara",
    "hanya", "sangat", "terlalu", "paling", "lagi", "namun", "tetapi",
    "melainkan", "kecuali", "bahkan", "malah", "justru", "cuma", "tadi",
    "nanti", "kemudian", "lalu", "maka", "akibat", "hasil", "tujuan",
    "guna", "serta", "maupun", "hingga", "bahwa", "karena", "maka",
    "biar", "supaya", "agar", "sehingga", "sebab", "oleh", "tentang",
    "kepada", "terhadap", "mengenai", "menurut", "seperti", "ibarat",
    "bak", "laksana", "bagai", "ibarat", "daripada", "alihalih",
    "melainkan", "hanyalah", "adalah", "ialah", "yakni", "yaitu",
    # Numbers and common words yang sering muncul
    "satu", "dua", "tiga", "empat", "lima", "enam", "tujuh", "delapan",
    "sembilan", "sepuluh", "pertama", "kedua", "ketiga", "tersebut",
    # Indonesian common words
    "ia", "dia", "mereka", "kami", "kita", "kamu", "anda", "saya", "aku",
    "kita", "diri", "sendiri", "sini", "situ", "sana", "mana", "sana",
    "situ", "kini", "nanti", "dulu", "dahulu", "tadi", "barusan",
    "sesuatu", "seseorang", "beberapa", "berbagai", "macam", "jenis",
    "buah", "ekor", "orang", "lembar", "helai", "batang", "pucuk",
    # Common verbs/adjectives
    "buat", "lakukan", "ambil", "beri", "tahu", "lihat", "dengar",
    "bilang", "kata", "ucap", "tanya", "jawab", "pikir", "rasa",
    "ingin", "mau", "perlu", "harus", "dapat", "bisa", "boleh",
    "baik", "buruk", "benar", "salah", "tinggi", "rendah", "panjang",
    "pendek", "lebar", "sempit", "tebal", "tipis", "berat", "ringan",
    # English stopwords
    "the", "of", "in", "to", "a", "is", "was", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "must", "can", "are", "am", "as", "at",
    "by", "for", "it", "on", "or", "this", "that", "these", "those",
    "i", "you", "he", "she", "we", "they", "what", "which", "who", "where",
    "when", "why", "how", "all", "each", "every", "both", "any", "some",
    "an", "and", "but", "if", "not", "no", "yes", "so", "than", "too",
    "very", "just", "only", "also", "now", "here", "there", "then", "up",
    "out", "about", "into", "over", "after", "before", "between", "under",
    "again", "further", "once", "here", "there", "when", "where", "why",
    "how", "all", "any", "both", "each", "few", "more", "most", "other",
    "some", "such", "no", "nor", "not", "only", "own", "same", "than",
    "too", "very", "s", "t", "can", "will", "just", "don", "should", "now",
}


def get_stopwords():
    """Get combined stopwords from base list and Sastrawi (if available)."""
    stopwords = set(BASE_STOPWORDS)
    
    if HAS_SASTRAWI:
        try:
            factory = StopWordRemoverFactory()
            sastrawi_stopwords = set(factory.get_stop_words())
            stopwords.update(sastrawi_stopwords)
            print(f"[GOLD] Combined {len(BASE_STOPWORDS)} base + {len(sastrawi_stopwords)} Sastrawi stopwords = {len(stopwords)} total")
        except Exception as e:
            print(f"[GOLD] Could not load Sastrawi stopwords: {e}")
            print(f"[GOLD] Using {len(stopwords)} base stopwords only")
    else:
        print(f"[GOLD] Sastrawi not available, using {len(stopwords)} built-in stopwords")
    
    return stopwords


# Final stopwords set
STOPWORDS = get_stopwords()


def build_spark_session() -> SparkSession:
    """Initialize Spark session with Delta Lake support."""
    try:
        builder = SparkSession.builder.appName("Gold-NewsPulse") \
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


def compute_word_frequency(silver_df, top_n=15):
    """
    REPRODUCING ETS: word_frequency table
    
    Extract words from titles, remove stopwords, count frequency.
    This is the same logic as spark_analysis.py but reading from Silver (clean) data.
    """
    print("[GOLD] Computing word_frequency (Reproducing ETS)...")
    
    tokenized = (
        silver_df.select(
            explode(
                split(
                    regexp_replace(
                        lower(coalesce(col("judul"), lit(""))),
                        r"[^A-Za-z0-9\s]+",
                        " "
                    ),
                    r"\s+"
                )
            ).alias("kata")
        )
        .filter(col("kata") != "")
        .filter(~col("kata").isin(sorted(STOPWORDS)))
        .filter(length(col("kata")) > 1)
    )
    
    result = (
        tokenized.groupBy("kata")
        .agg(count("*").alias("frekuensi"))
        .orderBy(desc("frekuensi"), asc("kata"))
        .limit(top_n)
    )
    
    count_result = result.count()
    print(f"[GOLD] word_frequency: {count_result} unique words")
    return result


def compute_news_per_source(silver_df):
    """
    REPRODUCING ETS: news_per_source table
    
    Count news articles per source (news agency).
    This is the same as spark_analysis.py but from clean Silver data.
    """
    print("[GOLD] Computing news_per_source (Reproducing ETS)...")
    
    result = (
        silver_df.groupBy(coalesce(col("sumber"), lit("Unknown")).alias("sumber"))
        .agg(count("*").alias("jumlah"))
        .orderBy(desc("jumlah"), asc("sumber"))
    )
    
    count_result = result.count()
    print(f"[GOLD] news_per_source: {count_result} sources")
    return result


def compute_word_velocity(silver_df):
    """
    ENHANCED ANALYSIS: word_velocity table
    
    Detect words that are trending upward (velocity = frequency change per hour).
    This is NEW - not possible with raw JSON in ETS.
    
    Window Function approach:
    - Group words by hour
    - Calculate frequency per hour
    - Compare with previous hour using LAG()
    - Calculate velocity (delta per hour)
    - Top 10 words with highest positive velocity
    """
    print("[GOLD] Computing word_velocity (ENHANCED - New Analysis)...")
    
    # Extract words and hour
    words_per_hour = (
        silver_df
        .select(
            col("jam"),
            explode(
                split(
                    regexp_replace(
                        lower(coalesce(col("judul"), lit(""))),
                        r"[^A-Za-z0-9\s]+",
                        " "
                    ),
                    r"\s+"
                )
            ).alias("kata"),
            col("_ingested_at")
        )
        .filter(col("kata") != "")
        .filter(~col("kata").isin(sorted(STOPWORDS)))
        .filter(length(col("kata")) > 1)
        .filter(col("jam").isNotNull())
    )
    
    # Count per word per hour
    freq_by_hour = (
        words_per_hour
        .groupBy("kata", "jam")
        .agg(count("*").alias("freq_current"))
        .orderBy("kata", "jam")
    )
    
    # Window function: compare with previous hour
    window_spec = Window.partitionBy("kata").orderBy("jam")
    
    velocity = (
        freq_by_hour
        .withColumn("freq_previous", lag("freq_current", 1).over(window_spec))
        .withColumn(
            "velocity",
            col("freq_current") - coalesce(col("freq_previous"), lit(0))
        )
        .filter(col("velocity") > 0)  # Only uptrending
        .select("kata", "jam", "freq_current", "freq_previous", "velocity")
        .orderBy(desc("velocity"), desc("jam"), asc("kata"))
    )
    
    count_result = velocity.count()
    print(f"[GOLD] word_velocity: {count_result} trending words detected")
    return velocity


def compute_cross_source_topics(silver_df):
    """
    ENHANCED ANALYSIS: cross_source_topics table
    
    Join API and RSS data to find topics appearing in both sources.
    Validates if topics are truly trending (appear in multiple sources) vs. single-source noise.
    
    This is NEW - requires joining clean Silver data from two sources.
    """
    print("[GOLD] Computing cross_source_topics (ENHANCED - New Analysis)...")
    
    # Separate API and RSS
    api_df = silver_df.filter(col("_source") == "api")
    rss_df = silver_df.filter(col("_source") == "rss")
    
    # Extract topics (words) from each source
    api_topics = (
        api_df.select(
            explode(
                split(
                    regexp_replace(
                        lower(coalesce(col("judul"), lit(""))),
                        r"[^A-Za-z0-9\s]+",
                        " "
                    ),
                    r"\s+"
                )
            ).alias("topik"),
            lit("api").alias("sumber")
        )
        .filter(col("topik") != "")
        .filter(~col("topik").isin(sorted(STOPWORDS)))
        .filter(length(col("topik")) > 1)
    )
    
    rss_topics = (
        rss_df.select(
            explode(
                split(
                    regexp_replace(
                        lower(coalesce(col("judul"), lit(""))),
                        r"[^A-Za-z0-9\s]+",
                        " "
                    ),
                    r"\s+"
                )
            ).alias("topik"),
            lit("rss").alias("sumber")
        )
        .filter(col("topik") != "")
        .filter(~col("topik").isin(sorted(STOPWORDS)))
        .filter(length(col("topik")) > 1)
    )
    
    # Count topics in API
    api_count = (
        api_topics.groupBy("topik")
        .agg(count("*").alias("api_count"))
    )
    
    # Count topics in RSS
    rss_count = (
        rss_topics.groupBy("topik")
        .agg(count("*").alias("rss_count"))
    )
    
    # Join to find cross-source topics
    cross_source = (
        api_count.join(rss_count, "topik", "inner")
        .withColumn(
            "co_occurrence_ratio",
            (col("api_count") + col("rss_count")) / 
            (col("api_count") * col("rss_count")).cast("float")
        )
        .orderBy(
            desc(col("api_count") + col("rss_count")),
            asc("topik")
        )
    )
    
    count_result = cross_source.count()
    print(f"[GOLD] cross_source_topics: {count_result} topics in both API and RSS")
    return cross_source


def demonstrate_time_travel(spark: SparkSession, silver_path: str):
    """
    Demonstrate Delta Lake Time Travel capabilities.
    
    This is required by the assignment - shows ability to query old versions.
    """
    print("\n[TIME TRAVEL] Starting Time Travel Demonstration...\n")
    
    try:
        from delta.tables import DeltaTable
        
        delta_table = DeltaTable.forPath(spark, silver_path)
        
        # Show history
        print("[TIME TRAVEL] === Delta Table Version History ===")
        history_df = delta_table.history()
        preferred_cols = ["version", "timestamp", "operation", "numAddedFiles", "numRemovedFiles"]
        selected_cols = [column_name for column_name in preferred_cols if column_name in history_df.columns]
        history = history_df.select(*selected_cols)
        history.show(10)
        
        # Read current version
        print("\n[TIME TRAVEL] Current version (latest):")
        current = spark.read.format("delta").load(silver_path)
        print(f"  Records: {current.count()}")
        
        # Try to read version 0 if it exists
        try:
            v0 = spark.read.format("delta") \
                .option("versionAsOf", 0) \
                .load(silver_path)
            print(f"\n[TIME TRAVEL] Version 0 (original):")
            print(f"  Records: {v0.count()}")
            
            # Compare
            diff = current.count() - v0.count()
            if diff != 0:
                print(f"\n[TIME TRAVEL] Change between v0 and latest: {diff} records")
        except:
            print("[TIME TRAVEL] Version 0 not available (normal if only written once)")
        
        print("\n[TIME TRAVEL] Time Travel Demonstration Complete ✓")
        return True
        
    except Exception as e:
        print(f"[TIME TRAVEL] Demo skipped: {e}")
        return False


def write_gold_tables(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
):
    """
    Read Silver layer and create all Gold tables.
    """
    
    print("[GOLD] Starting Gold layer transformation...")
    print(f"[GOLD] Input: {silver_path}")
    print(f"[GOLD] Output: {output_path}")
    
    # Clean Gold output directory to avoid schema conflicts
    import shutil
    if os.path.exists(output_path):
        print(f"[GOLD] Cleaning existing Gold output directory: {output_path}")
        shutil.rmtree(output_path)
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    # Read Silver
    print("\n[GOLD] Reading Silver data...")
    silver = spark.read.format("delta").load(silver_path)
    silver_count = silver.count()
    print(f"[GOLD] Silver records: {silver_count}")
    
    # Compute all Gold tables
    print("\n" + "="*60)
    print("REPRODUCING ETS ANALYSIS")
    print("="*60)
    
    # Gold 1: word_frequency
    word_freq = compute_word_frequency(silver)
    
    # Gold 2: news_per_source
    news_source = compute_news_per_source(silver)
    
    print("\n" + "="*60)
    print("ENHANCED ANALYSIS (NEW)")
    print("="*60)
    
    # Gold 3: word_velocity (Enhanced)
    word_vel = compute_word_velocity(silver)
    
    # Gold 4: cross_source_topics (Enhanced)
    cross_src = compute_cross_source_topics(silver)
    
    # Write all tables
    print("\n" + "="*60)
    print("WRITING GOLD TABLES TO DELTA")
    print("="*60)
    
    try:
        # Write word_frequency
        print("\n[GOLD] Writing word_frequency table...")
        word_freq.write \
            .format("delta") \
            .mode("overwrite") \
            .save(os.path.join(output_path, "word_frequency"))
        print("[DONE] word_frequency saved")
        
        # Write news_per_source
        print("[GOLD] Writing news_per_source table...")
        news_source.write \
            .format("delta") \
            .mode("overwrite") \
            .save(os.path.join(output_path, "news_per_source"))
        print("[DONE] news_per_source saved")
        
        # Write word_velocity
        print("[GOLD] Writing word_velocity table...")
        word_vel.write \
            .format("delta") \
            .mode("overwrite") \
            .save(os.path.join(output_path, "word_velocity"))
        print("[DONE] word_velocity saved")
        
        # Write cross_source_topics
        print("[GOLD] Writing cross_source_topics table...")
        cross_src.write \
            .format("delta") \
            .mode("overwrite") \
            .save(os.path.join(output_path, "cross_source_topics"))
        print("[DONE] cross_source_topics saved")
        
        # Demo Time Travel
        print("\n")
        demonstrate_time_travel(spark, silver_path)
        
        # Show sample outputs
        print("\n" + "="*60)
        print("SAMPLE OUTPUT FROM EACH GOLD TABLE")
        print("="*60)
        
        print("\n[GOLD] Top 15 Trending Words:")
        word_freq.show(15)
        
        print("\n[GOLD] News Distribution by Source:")
        news_source.show(10)
        
        if word_vel.count() > 0:
            print("\n[GOLD] Top 10 Trending Words (by velocity):")
            word_vel.limit(10).show()
        else:
            print("\n[GOLD] word_velocity: No trending words detected (need hourly data)")
        
        if cross_src.count() > 0:
            print("\n[GOLD] Cross-Source Topics (API + RSS):")
            cross_src.limit(10).show()
        else:
            print("\n[GOLD] cross_source_topics: No cross-source overlap (need both API and RSS data)")
        
        print("\n[DONE] All Gold tables created successfully!")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to write Gold tables: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create Gold layer aggregations from Silver Delta Layer"
    )
    parser.add_argument(
        "--silver-path",
        default=DEFAULT_SILVER_PATH,
        help=f"Input path for Silver Delta table (default: {DEFAULT_SILVER_PATH})"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_GOLD_OUTPUT,
        help=f"Output path for Gold Delta tables (default: {DEFAULT_GOLD_OUTPUT})"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Spark logging level"
    )
    
    args = parser.parse_args()
    
    # Verify Silver path exists
    if not os.path.exists(args.silver_path):
        print(f"ERROR: Silver path does not exist: {args.silver_path}")
        print("Please run 02_silver.py first")
        sys.exit(1)
    
    # Build Spark session
    spark = build_spark_session()
    spark.sparkContext.setLogLevel(args.log_level)
    
    print(f"\n[SPARK] PySpark version: {spark.version}")
    print(f"[SPARK] Session initialized at {datetime.utcnow().isoformat()}Z\n")
    
    # Run Gold layer creation
    success = write_gold_tables(
        spark,
        silver_path=args.silver_path,
        output_path=args.output,
    )
    
    # Cleanup
    spark.stop()
    print("\n[SPARK] Session stopped")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
