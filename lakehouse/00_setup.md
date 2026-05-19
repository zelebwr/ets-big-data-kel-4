# Setup Guide untuk Data Lakehouse Pipeline NewsPulse

## 📋 Persyaratan Awal

### Periksa Data di HDFS
Pastikan Anda sudah menjalankan producer dan consumer dari ETS:

```bash
# Di container/terminal dengan Hadoop aktif
hdfs dfs -ls /data/news/api/
hdfs dfs -ls /data/news/rss/

# Lihat sample 1 file
hdfs dfs -cat /data/news/api/part-*.json | head -n 100
```

**Expected Output:**
```json
{
  "judul": "Breaking News Title",
  "sumber": "CNN",
  "url": "https://...",
  "deskripsi": "Article description",
  "image": "image_url",
  "thumbnail": "thumb_url",
  "waktu_terbit": "2024-05-19T10:30:45Z",
  "timestamp": "2024-05-19T10:30:45Z"
}
```

---

## 🔧 Instalasi Dependensi

### 1. Install PySpark + Delta Lake

```bash
# Jika menggunakan pip
pip install pyspark delta-spark pyarrow

# Atau jika menggunakan conda
conda install -c conda-forge pyspark delta-spark pyarrow
```

### 2. Verify Instalasi

```bash
python -c "import pyspark; print(f'PySpark: {pyspark.__version__}')"
python -c "import delta; print(f'Delta: {delta.__version__}')"
```

**Expected Output:**
```
PySpark: 3.4.x or higher
Delta: 3.0.x or higher
```

---

## 🌍 Environment Variables (HDFS)

Jika menggunakan HDFS:

```bash
# Linux/Mac
export HADOOP_HOME=/path/to/hadoop
export JAVA_HOME=/path/to/java
export PATH=$HADOOP_HOME/bin:$PATH

# Atau Windows (PowerShell)
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME = "C:\java"
```

**Verify:**
```bash
hadoop version
java -version
```

---

## 🚀 Jalankan Pipeline

### Quick Start (All-in-One)

```bash
cd lakehouse

# 1. Bronze Layer
python 01_bronze.py --hdfs-base "hdfs://localhost:8020/data/news" --output "./lakehouse_data/bronze/news"

# 2. Silver Layer
python 02_silver.py --bronze-path "./lakehouse_data/bronze/news" --output "./lakehouse_data/silver/news"

# 3. Gold Layer
python 03_gold.py --silver-path "./lakehouse_data/silver/news" --output "./lakehouse_data/gold"
```

### Advanced: Custom Parameters

```bash
# Bronze: Ubah HDFS base
python 01_bronze.py \
  --hdfs-base "hdfs://namenode:8020/data/news" \
  --output "./lakehouse_data/bronze/news" \
  --timestamp $(date +%s)

# Silver: Dengan mode strict
python 02_silver.py \
  --bronze-path "./lakehouse_data/bronze/news" \
  --output "./lakehouse_data/silver/news" \
  --mode strict

# Gold: Hanya API
python 03_gold.py \
  --silver-path "./lakehouse_data/silver/news" \
  --output "./lakehouse_data/gold" \
  --channel api
```

---

## 🔍 Inspeksi Hasil

### Cek Bronze Layer

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("Inspector")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Baca Bronze
bronze = spark.read.format("delta").load("./lakehouse_data/bronze/news")
print(f"Bronze Records: {bronze.count()}")
print("Bronze Schema:")
bronze.printSchema()
print("Bronze Sample:")
bronze.limit(3).show()
```

### Cek Silver Layer

```python
# Baca Silver
silver = spark.read.format("delta").load("./lakehouse_data/silver/news")
print(f"Silver Records: {silver.count()}")
print("Transformasi dari Bronze ke Silver:")
print(f"  Duplikat removed: {bronze.count() - silver.count()}")
print("Silver Schema:")
silver.printSchema()
```

### Cek Gold Layer

```python
# Word Frequency
gold_freq = spark.read.format("delta").load("./lakehouse_data/gold/word_frequency")
print("Top 15 Trending Words:")
gold_freq.show(15)

# News per Source
gold_source = spark.read.format("delta").load("./lakehouse_data/gold/news_per_source")
print("News Distribution by Source:")
gold_source.show()

# Word Velocity (Enhanced)
gold_velocity = spark.read.format("delta").load("./lakehouse_data/gold/word_velocity")
print("Top 10 Trending Velocity:")
gold_velocity.limit(10).show()

# Cross Source Topics (Enhanced)
gold_cross = spark.read.format("delta").load("./lakehouse_data/gold/cross_source_topics")
print("Cross-Source Topic Analysis:")
gold_cross.show()
```

---

## ⏮️ Time Travel Demo

### Demonstrasi Versi Lama

```python
from delta.tables import DeltaTable

silver_path = "./lakehouse_data/silver/news"
delta_table = DeltaTable.forPath(spark, silver_path)

# 1. Lihat history tabel
print("=== Delta Table History ===")
history = delta_table.history().select("version", "timestamp", "operation")
history.show(10)

# 2. Baca data versi 0 (asli)
print("=== Data Version 0 (Original) ===")
v0_data = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(silver_path)
print(f"Version 0 Record Count: {v0_data.count()}")

# 3. Bandingkan dengan versi sekarang
print("=== Data Version Latest (Current) ===")
latest_data = spark.read.format("delta").load(silver_path)
print(f"Latest Record Count: {latest_data.count()}")

# 4. Simulasi perubahan data (update example)
print("=== Performing UPDATE on Silver Table ===")
delta_table.update(
    condition="sumber IS NULL",
    set={"sumber": '"Unknown"'}
)

# 5. Bandingkan lagi
print("=== After Update ===")
updated_data = spark.read.format("delta").load(silver_path)
print(f"Updated Record Count: {updated_data.count()}")
print(f"Null sumber di v0: {v0_data.filter('sumber IS NULL').count()}")
print(f"Null sumber sekarang: {updated_data.filter('sumber IS NULL').count()}")
```

**Expected Output:**
```
=== Delta Table History ===
+-------+---------------------------+--------+
|version|      timestamp            |operation|
+-------+---------------------------+--------+
|   0   |2024-05-19 10:30:00.000Z   | ADD    |
|   1   |2024-05-19 10:35:00.000Z   | UPDATE |
+-------+---------------------------+--------+

Version 0 Record Count: 950
Latest Record Count: 950
Null sumber di v0: 100
Null sumber sekarang: 0  ← Update berhasil!
```

---

## 🐛 Troubleshooting

### Error: HDFS Connection Refused

```
Error: Connection to namenode:8020 refused
```

**Solusi:**
1. Pastikan Hadoop services berjalan
2. Ubah hostname ke `localhost` jika di local:
   ```bash
   python 01_bronze.py --hdfs-base "hdfs://localhost:8020/data/news"
   ```
3. Alternatif: gunakan file JSON lokal
   ```bash
   cp -r /mnt/hdfs/data/news ./data_local/
   python 01_bronze.py --hdfs-base "./data_local" --use-local
   ```

### Error: Delta Lake not initialized

```
Error: The package io.delta:delta-spark is not installed
```

**Solusi:**
```bash
pip install --upgrade delta-spark
# Atau jika masih error
pip uninstall delta-spark -y && pip install delta-spark==3.1.0
```

### Memory Issues

Jika mendapat `OutOfMemory` error:

```bash
# Increase Spark memory
python 01_bronze.py --driver-memory 4g --executor-memory 4g
```

Atau di dalam script:
```python
spark = SparkSession.builder \
    .appName("Bronze") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

---

## 📁 Folder Structure Setelah Selesai

```
lakehouse/
├── 00_setup.md
├── README_lakehouse.md
├── 01_bronze.py
├── 02_silver.py
├── 03_gold.py
├── lakehouse_data/
│   ├── bronze/
│   │   ├── news/
│   │   │   ├── _delta_log/
│   │   │   ├── part-00001-...parquet
│   │   │   ├── part-00002-...parquet
│   │   │   └── ...
│   ├── silver/
│   │   ├── news/
│   │   │   ├── _delta_log/
│   │   │   ├── part-00001-...parquet
│   │   │   └── ...
│   └── gold/
│       ├── word_frequency/
│       ├── news_per_source/
│       ├── word_velocity/
│       └── cross_source_topics/
└── logs/
    ├── bronze_*.log
    ├── silver_*.log
    └── gold_*.log
```

---

## ✅ Verification Checklist

- [ ] PySpark dan Delta Lake terinstall
- [ ] Bisa connect ke HDFS (atau copy data lokal)
- [ ] Bronze layer berhasil dibuat (cek `./lakehouse_data/bronze/news/`)
- [ ] Silver layer berhasil dibuat (cek `./lakehouse_data/silver/news/`)
- [ ] Gold tables berhasil dibuat (4 tabel ada di `./lakehouse_data/gold/`)
- [ ] Time Travel demo berhasil menunjukkan history
- [ ] Bisa membaca setiap Gold table tanpa error

---

## 📞 Common Commands

```bash
# Hapus semua data dan restart (careful!)
rm -rf ./lakehouse_data

# Cek ukuran data
du -sh ./lakehouse_data/*

# Run specific layer
python 02_silver.py  # hanya silver
python 03_gold.py    # hanya gold

# Run dengan logging verbose
python 01_bronze.py --log-level DEBUG

# Cleanup Spark
pkill -f "PySpark"
```

---

## 🎯 Next Steps

Setelah setup berhasil:
1. Baca [README_lakehouse.md](README_lakehouse.md) untuk detail transformasi
2. Review output setiap layer di console
3. Jalankan demo Time Travel
4. Siapkan presentasi untuk Week 13
