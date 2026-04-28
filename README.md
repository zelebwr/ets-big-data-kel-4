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
docker compose -f docker-compose-kafka.yml ps
```

### 3. Make 2 Topic

Command:

```bash
docker exec -it kafka-broker kafka-topics --create --topic news-api --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka-broker kafka-topics --create --topic news-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka-broker kafka-topics --list --boostrap-server localhost:9092
```

### 4. Setup Hadoop via Dokcer Compose

```bash
docker rm -f hadoop-resourcemanager -f hadoop-nodemanager -f hadoop-datanode -f haddop-namenode
docker compose -f docker-compose-hadoop.yml up -d
docker compose -f docker-compose-hadoop.yml ps
```

### 5. Make Directory Structure on HDFS

Command:

```bash
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/news/{api,rss,hasil}
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/news/
```

### 7. Verify HDFS Web UI

### 8. Verify End-to-End Infrastructure

Command:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/news
docker exec -it hadoop-namenode hdfs dfsadmin -report
```

---

## B. Kafka Producer API

### 1. Test Weather API

Command: 

```bash
docker exec -it kafka-broker kafka-console-consumer --topic news-api --from-beginning --bootstrap-server localhos
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


