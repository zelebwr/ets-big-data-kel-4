# ETS Big Data &mdash; Weather Pulse: Monitor Cuaca 6 Kota Besar Indonesia

---

## Kelompok 4

| No | Nama                              | NRP        | Job desk |
| :-: | --------------------------------- | ---------- | ------- |
| 1 | Jonathan Zelig Sutopo              | 5027241047 | Project Initialization |
| 2 | Muhammad Ardiansyah Tri Wibowo     | 5027241091 | |
| 3 | Muhammad Fatihul Qolbi Ash Shiddiqi| 5027241023 | |
| 4 | Erlangga Valdhio Putra Sulistio    | 5027241030 | |
| 5 | Tiara Putri Prasetya               | 5027241013 | |

---

## A. Project Initialization

### 1. Initial Project Folder Structure

Command:

```bash
mkdir -p kafka spark dashboard dashboard/{templates,static,data}
touch kafka/producer_api.py kafka/producer_rss.py kafka/consumer_to_hdfs.py spark/analysis.ipynb dashboard/app.py dashboard/templates/index.html dashboard/statis/style.css README.md
```

### 2. Setup Kafka via Docker Compose

Command: 

```bash
docker compose -f docker-compose-kafka.yml up -d
```

### 3. Verify Kafka Broker Activeness + Make 2 Topic

Command:

```bash
docker commpose -f docker-compose-kafka.yml ps
```

### 4. Setup Hadoop via Dokcer Compose

```bash
docker rm -f hadoop-resourcemanager -f hadoop-nodemanager -f hadoop-datanode -f haddop-namenode
docker compose -f docker-compose-hadoop.yml up -d
```

### 5. Verify 4 Hadoop Container Activeness

Command:

```bash
docker compose -f docker-compose-hadoop.yml up -d
```

### 6. Make Directory Structure on HDFS

Command:

```bash
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/weather/{api,rss,hasil}
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/weather/
```

### 7. Verify HDFS Web UI

### 8. Verify End-to-End Infrastructure

Command:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/weather
docker exec -it hadoop-namenode hdfs dfsadmin -report
```

---

## B. Kafka Producer API

### 1. Test Weather API

Command: 

```bash
docker exec -it kafka-broker kafka-console-consumer --topic weather-api --from-beginning --bootstrap-server localhos
t:9092
```

### 2. Check Consumer Group 

Command:

```bash
docker exec -it kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### 3. Check Offset Detail + Lag Topic Weahter API

Command: 

```bash
docker exec -it kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --descibe --group console-consumer-15478
```

### 4. Check `live_api.json`

Command:

```bash
cat dashboard/data/live_api.json
```

## C. Weather Data Producer (API) = Anggota 2

### 1. Install Dependency
```bash
cat python -m pip install kafka-python requests
```

### 2. Implementation (Running the Script)
Script kafka/producer_api.py akan menarik data suhu, kelembapan, dan kecepatan angin untuk kota JKT, SBY, SMG, MDN, MKS, DPS setiap 10 menit.
```bash
cat python kafka/producer_api.py
```

### 3. Verification (Checking Data Flow)
Untuk memastikan data benar-benar sampai ke Kafka Broker, buka terminal baru dan jalankan perintah konsumer internal Kafka:
```bash
cat docker exec -it kafka-broker kafka-console-consumer --topic weather-api --from-beginning --property print.key=true --bootstrap-server localhost:9092
```
Hasil yang diharapkan:
Terminal akan menampilkan Key (Kode Kota) diikuti oleh JSON data cuaca seperti ini:
JKT {"kode_kota": "JKT", "temperature": 24.5, ...}

## 4. Data Ingestion (RSS) & HDFS Storage = Anngota 3

### 1. Install Dependency
Pastikan pustaka untuk parsing RSS dan koneksi Kafka sudah terpasang.
```bash
cat python -m pip install kafka-python feedparser
```
### 2. HDFS Infrastructure Setup (Manual)
Sebelum menjalankan consumer, folder tujuan di HDFS harus dibuat secara manual agar tidak terjadi error "Directory not found".
```bash
cat # Membuat folder induk dan sub-folder data
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/weather/api
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/weather/rss
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/weather/hasil
```

### 3. Implementation (Running the Scripts)
- **Running Producer RSS**
Script ini mengambil berita cuaca terbaru dari portal berita setiap 5 menit.
```bash
cat # Jalankan di Terminal 1
python kafka/producer_rss.py
```
Catatan: Jika muncul "0 artikel baru", berarti belum ada berita cuaca terbaru yang dirilis oleh portal berita pada saat script dijalankan.

- **Running Consumer to HDFS**
Script ini bertugas menyedot data dari topik Kafka (weather-api dan weather-rss) lalu menyimpannya ke Hadoop.
```bash
cat # Jalankan di Terminal 2
python kafka/consumer_to_hdfs.py
```

### Verification (Checking HDFS Data)
Untuk memastikan data telah tersimpan secara permanen di Hadoop, jalankan perintah berikut:
```bash
cat # Melihat daftar file yang masuk ke HDFS secara rekursif
docker exec hadoop-namenode hdfs dfs -ls -R /data/weather/
```
Hasil yang diharapkan:
Muncul daftar file .json di dalam folder /data/weather/api/ (dan /rss/ jika berita sudah tersedia).