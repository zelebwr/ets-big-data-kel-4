# NewsPulse -- Analisis Tren Berita Nasional

> **ETS Big Data — Topik 5**
> PR agency yang perlu memantau isu apa yang sedang paling banyak dibicarakan media nasional dan digital.

---

## Panduan Cepat Menjalankan Project

Jalankan perintah dari root project: `ets-big-data-kel-4`.

### 1. Siapkan Kafka dan Hadoop

```bash
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-hadoop.yml up -d
```

Buat topic Kafka dan folder HDFS:

```bash
docker exec -it kafka-broker kafka-topics --create --topic news-api --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka-broker kafka-topics --create --topic news-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/api
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/rss
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/hasil
```

### 2. Install dependency Python

```bash
pip install kafka-python requests feedparser hdfs flask
```

### 3. Jalankan pipeline data

Buka terminal terpisah untuk setiap proses:

```bash
python kafka/producer_api.py
python kafka/producer_rss.py
python kafka/consumer_to_hdfs.py
```

### 4. Jalankan analisis Spark

Buka `spark/analysis.ipynb`, jalankan semua cell, lalu pastikan hasil JSON tersimpan ke folder `dashboard/data`.

File yang dibaca dashboard:

```text
dashboard/data/spark_results.json
dashboard/data/live_api.json
dashboard/data/live_rss.json
```

### 5. Jalankan dashboard

```bash
cd dashboard
python app.py
```

Buka `http://localhost:5000`.

### 6. Cek hasil

- Kafka topic aktif: `docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092`
- HDFS berisi data: `docker exec -it hadoop-namenode hdfs dfs -ls -R /data/news/`
- Hadoop UI: `http://localhost:9870`
- Dashboard Flask: `http://localhost:5000`

---

## Kelompok 4

| No | Nama | NRP | Job desk |
|:--:|------|-----|----------|
| 1 | Jonathan Zelig Sutopo | 5027241047 | DevOps / Infrastructure |
| 2 | Muhammad Ardiansyah Tri Wibowo | 5027241091 | Kafka Producer API |
| 3 | Muhammad Fatihul Qolbi Ash Shiddiqi | 5027241023 | Kafka Producer RSS + Consumer HDFS |
| 4 | Erlangga Valdhio Putra Sulistio | 5027241030 | Apache Spark Analysis |
| 5 | Tiara Putri Prasetya | 5027241013 | Dashboard Flask |

---

## Arsitektur Sistem

```
┌─────────────────┐     ┌─────────────────┐
│  GNews API      │     │  RSS Feeds      │
│  (top headlines)│     │  Kompas + Tempo  │
└────────┬────────┘     └────────┬────────┘
         │                       │
    producer_api.py         producer_rss.py
         │                       │
    ┌────▼────┐            ┌─────▼────┐
    │news-api │            │ news-rss │
    │ (Kafka) │            │ (Kafka)  │
    └────┬────┘            └────┬─────┘
         │                      │
         └──────┬───────────────┘
                │
       consumer_to_hdfs.py
                │
         ┌──────▼──────┐
         │    HDFS     │
         │ /data/news/ │
         └──────┬──────┘
                │
         analysis.ipynb (Spark)
                │
         ┌──────▼──────────┐
         │ spark_results   │
         └──────┬──────────┘
                │
         ┌──────▼──────────┐
         │  Flask Dashboard │
         │  localhost:5000  │
         └─────────────────┘
```

---

## A. Project Initialization (Anggota 1 — DevOps)

### 1. Setup Kafka via Docker Compose

```bash
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-kafka.yml ps
```

### 2. Create 2 Kafka Topics

```bash
docker exec -it kafka-broker kafka-topics --create --topic news-api --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka-broker kafka-topics --create --topic news-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Verify Topics

```bash
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092
# Expected: news-api, news-rss
```

### 4. Setup Hadoop via Docker Compose

```bash
docker compose -f docker-compose-hadoop.yml up -d
docker compose -f docker-compose-hadoop.yml ps
```

### 5. Create HDFS Directory Structure

```bash
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/api
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/rss
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/hasil
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/news/
```

### 6. Verify HDFS Web UI

Buka browser → `http://localhost:9870`

### 7. Verify End-to-End Infrastructure

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/news/
docker exec -it hadoop-namenode hdfs dfsadmin -report
```

---

## B. Kafka Producer API (Anggota 2)

### 1. Install Dependencies

```bash
pip install kafka-python requests
```

### 2. Setup API Key

Daftar gratis di https://gnews.io → dapatkan API key → edit `GNEWS_API_KEY` di `kafka/producer_api.py`

### 3. Run Producer

```bash
python kafka/producer_api.py
```

### 4. Verify Data Flow

```bash
docker exec -it kafka-broker kafka-console-consumer --topic news-api --from-beginning --property print.key=true --bootstrap-server localhost:9092
```

---

## C. Kafka Producer RSS + Consumer HDFS (Anggota 3)

### 1. Install Dependencies

```bash
pip install kafka-python feedparser hdfs
```

### 2. Run Producer RSS

```bash
python kafka/producer_rss.py
```

### 3. Run Consumer to HDFS

```bash
python kafka/consumer_to_hdfs.py
```

### 4. Verify HDFS Data

```bash
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/news/
```

---

## D. Apache Spark Analysis (Anggota 4)

### 1. Analisis yang dilakukan

| # | Analisis | Metode |
|---|----------|--------|
| 1 | Kata paling sering di judul (Top 15) | split() + explode() + filter stopwords |
| 2 | Distribusi berita per sumber | groupBy("sumber").count() |
| 3 | Volume publikasi per jam | HOUR(TO_TIMESTAMP(waktu_terbit)) |
| **Bonus** | K-Means Clustering (MLlib) | TF-IDF + K-Means (k=5) |

### 2. Run Notebook

```bash
# Di Jupyter Notebook lokal atau Google Colab
# Buka spark/analysis.ipynb dan jalankan semua cell
```

**Catatan Colab:** Jika menggunakan Google Colab, export file JSON dari HDFS ke Google Drive terlebih dahulu.

---

## E. Dashboard Flask (Anggota 5)

### 1. Install Dependencies

```bash
pip install flask
```

### 2. Run Dashboard

```bash
cd dashboard
python app.py
# Buka http://localhost:5000
```

### 3. Fitur Dashboard

| Panel | Deskripsi | Data Source |
|-------|-----------|------------|
| Kata Trending Top 15 | Tabel kata + frekuensi | spark_results.json |
| Distribusi per Sumber | Bar chart Kompas vs Tempo vs GNews | spark_results.json |
| Volume per Jam | Bar chart 24 jam (**Bonus Chart.js**) | spark_results.json |
| Kata Trending Chart | Horizontal bar chart (**Bonus Chart.js**) | spark_results.json |
| Peta Indonesia | Distribusi berita per wilayah | live_api.json + live_rss.json |
| Feed Berita Terbaru | Live feed + auto-refresh 30 detik | live_api.json + live_rss.json |

---

## Tantangan & Solusi

| Tantangan | Solusi |
|-----------|-------|
| RSS feed kadang lambat/timeout | Retry mechanism + timeout handler di producer |
| Duplikat berita antar RSS | Hash URL 8 karakter sebagai deduplication key |
| HDFS upload dari Windows | Docker cp + hdfs dfs -put via subprocess, fallback ke hdfs Python library |
| Spark baca dari HDFS | Fallback ke file lokal jika HDFS tidak tersedia |
