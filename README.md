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

