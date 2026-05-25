# 🚀 Quick Start - Data Lakehouse NewsPulse

## Setup Selesai! Berikut cara menjalankan:

### 1. Bronze Layer (Ingest dari HDFS)
```bash
cd lakehouse
python 01_bronze.py --hdfs-base "hdfs://localhost:8020/data/news" --output "./lakehouse_data/bronze/news"
```

### 2. Silver Layer (Cleaning)
```bash
python 02_silver.py --bronze-path "./lakehouse_data/bronze/news" --output "./lakehouse_data/silver/news"
```

### 3. Gold Layer (Aggregation)
```bash
python 03_gold.py --silver-path "./lakehouse_data/silver/news" --output "./lakehouse_data/gold"
```

### BONUS: Dashboard Integration
```bash
python BONUS_dashboard_integration.py
```

### BONUS: Schema Evolution
```bash
python BONUS_schema_evolution.py
```

## Troubleshooting

### Error: HDFS Connection Refused
Gunakan fallback lokal:
```bash
# Copy file JSON ke lokal dulu
mkdir -p ./data_local
python 01_bronze.py --hdfs-base "./data_local" --use-local
```

### Error: OutOfMemory
Tingkatkan Spark memory:
```bash
python 01_bronze.py --driver-memory 6g --executor-memory 6g
```

## Dokumentasi Lengkap
- README_lakehouse.md - Penjelasan arsitektur & transformasi
- 00_setup.md - Setup guide detail
- SCORING_CHECKLIST.md - Rubrik penilaian

## Informasi Berguna
- Folder Output: ./lakehouse_data/
- Log Files: ./logs/
- Sample Config: .env.example

Selamat! Setup lakehouse Anda sudah siap! 🎉
