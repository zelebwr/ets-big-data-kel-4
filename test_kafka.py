#!/usr/bin/env python3
import json
from kafka import KafkaProducer, KafkaConsumer

try:
    p = KafkaProducer(
        bootstrap_servers='localhost:9092',
        request_timeout_ms=5000
    )
    print("✓ Kafka Producer connected")
    p.close()
except Exception as e:
    print(f"✗ Kafka Producer error: {e}")

try:
    c = KafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        consumer_timeout_ms=2000,
        request_timeout_ms=5000
    )
    print("✓ Kafka Consumer connected")
    c.close()
except Exception as e:
    print(f"✗ Kafka Consumer error: {e}")
