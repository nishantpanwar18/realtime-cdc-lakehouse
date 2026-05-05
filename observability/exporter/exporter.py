"""
Pipeline Exporter — Prometheus metrics for the shipments CDC pipeline.

Scrapes every SCRAPE_INTERVAL seconds:
- Kafka: topic offsets, partition count, Spark consumer lag
- Debezium: connector + task state
- MySQL: reachability
- Hudi: timeline events, inflight ops, file counts, table size

Exposes /metrics on port 9200.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread, Lock

import requests
from confluent_kafka import Consumer, TopicPartition
import pymysql
import boto3
from botocore.client import Config

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shipments.shipments_db.shipments")
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083")
DEBEZIUM_CONNECTOR = os.getenv("DEBEZIUM_CONNECTOR", "shipments-connector")
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "rootpass")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.getenv("S3_KEY", "admin")
S3_SECRET = os.getenv("S3_SECRET", "password")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
HUDI_PREFIX = os.getenv("HUDI_PREFIX", "data/shipments_mor")
HUDI_CHECKPOINT_PREFIX = os.getenv("HUDI_CHECKPOINT_PREFIX", "checkpoints/shipments_mor")
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "15"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9200"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("exporter")

# ── Reusable clients ──────────────────────────────────────────
_s3_client = None
_kafka_consumer = None


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
    return _s3_client


def get_kafka_consumer():
    global _kafka_consumer
    if _kafka_consumer is None:
        _kafka_consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "pipeline-exporter",
            "enable.auto.commit": False,
        })
    return _kafka_consumer


# ── Metrics store (thread-safe) ───────────────────────────────
metrics_lock = Lock()
scalar_metrics = {
    "pipeline_kafka_topic_latest_offset": 0,
    "pipeline_kafka_partition_count": 0,
    "pipeline_debezium_connector_running": 0,
    "pipeline_debezium_task_running": 0,
    "pipeline_mysql_reachable": 0,
    "pipeline_hudi_last_commit_age_seconds": -1,
    "pipeline_hudi_deltacommit_count": 0,
    "pipeline_hudi_commit_count": 0,
    "pipeline_hudi_clean_count": 0,
    "pipeline_hudi_inflight_operations": 0,
    "pipeline_hudi_data_files_total": 0,
    "pipeline_hudi_log_files_total": 0,
    "pipeline_hudi_base_files_total": 0,
    "pipeline_hudi_file_groups_total": 0,
    "pipeline_hudi_table_size_bytes": 0,
    "pipeline_spark_last_batch_written": 0,
    "pipeline_spark_consumer_lag_total": 0,
    "pipeline_exporter_scrape_errors_total": 0,
    "pipeline_exporter_last_scrape_timestamp": 0,
    "pipeline_exporter_scrape_duration_seconds": 0,
}
labeled_metrics = {
    "pipeline_kafka_partition_offset": {},
    "pipeline_spark_consumer_lag_per_partition": {},
}

COMPLETED_SUFFIXES = frozenset({
    "deltacommit", "commit", "clean", "rollback",
    "savepoint", "restore", "replacecommit", "indexing",
})


# ── Kafka ─────────────────────────────────────────────────────
def scrape_kafka():
    try:
        consumer = get_kafka_consumer()
        md = consumer.list_topics(topic=KAFKA_TOPIC, timeout=5)
        if KAFKA_TOPIC not in md.topics or md.topics[KAFKA_TOPIC].error:
            log.warning(f"Topic {KAFKA_TOPIC} not found")
            return

        partitions = md.topics[KAFKA_TOPIC].partitions
        total_offset = 0
        partition_offsets = {}

        for pid in partitions.keys():
            tp = TopicPartition(KAFKA_TOPIC, pid)
            _, high = consumer.get_watermark_offsets(tp, timeout=5)
            partition_offsets[str(pid)] = high
            total_offset += high

        with metrics_lock:
            scalar_metrics["pipeline_kafka_topic_latest_offset"] = total_offset
            scalar_metrics["pipeline_kafka_partition_count"] = len(partitions)
            labeled_metrics["pipeline_kafka_partition_offset"] = {
                (("partition", p),): v for p, v in partition_offsets.items()
            }

        _scrape_spark_consumer_lag(partition_offsets)

    except Exception as e:
        log.warning(f"Kafka scrape failed: {e}")
        # Reset consumer on failure so it reconnects next time
        global _kafka_consumer
        _kafka_consumer = None
        with metrics_lock:
            scalar_metrics["pipeline_exporter_scrape_errors_total"] += 1


def _scrape_spark_consumer_lag(kafka_offsets):
    """Read Spark checkpoint offsets from S3 and compute lag per partition."""
    try:
        s3 = get_s3_client()
        prefix = f"{HUDI_CHECKPOINT_PREFIX}/offsets/"
        paginator = s3.get_paginator("list_objects_v2")

        latest_batch = -1
        latest_key = None
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                name = obj["Key"].rsplit("/", 1)[-1]
                try:
                    n = int(name)
                    if n > latest_batch:
                        latest_batch = n
                        latest_key = obj["Key"]
                except ValueError:
                    pass

        if latest_key is None:
            return

        body = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)["Body"].read().decode()
        lines = body.strip().split("\n")
        if len(lines) < 3:
            return

        offsets_json = json.loads(lines[-1])
        topic_offsets = offsets_json.get(KAFKA_TOPIC, {})

        total_lag = 0
        per_partition_lag = {}
        for pid_str, spark_offset in topic_offsets.items():
            kafka_offset = kafka_offsets.get(pid_str, 0)
            lag = max(0, kafka_offset - spark_offset)
            per_partition_lag[pid_str] = lag
            total_lag += lag

        with metrics_lock:
            scalar_metrics["pipeline_spark_consumer_lag_total"] = total_lag
            scalar_metrics["pipeline_spark_last_batch_written"] = latest_batch
            labeled_metrics["pipeline_spark_consumer_lag_per_partition"] = {
                (("partition", p),): v for p, v in per_partition_lag.items()
            }
    except Exception as e:
        log.warning(f"Spark consumer lag scrape failed: {e}")


# ── Debezium ──────────────────────────────────────────────────
def scrape_debezium():
    try:
        r = requests.get(
            f"{DEBEZIUM_URL}/connectors/{DEBEZIUM_CONNECTOR}/status",
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            conn_state = data.get("connector", {}).get("state", "UNKNOWN")
            task_state = data["tasks"][0].get("state", "UNKNOWN") if data.get("tasks") else "UNKNOWN"
            with metrics_lock:
                scalar_metrics["pipeline_debezium_connector_running"] = 1 if conn_state == "RUNNING" else 0
                scalar_metrics["pipeline_debezium_task_running"] = 1 if task_state == "RUNNING" else 0
        else:
            with metrics_lock:
                scalar_metrics["pipeline_debezium_connector_running"] = 0
                scalar_metrics["pipeline_debezium_task_running"] = 0
    except Exception as e:
        log.warning(f"Debezium scrape failed: {e}")
        with metrics_lock:
            scalar_metrics["pipeline_debezium_connector_running"] = 0
            scalar_metrics["pipeline_debezium_task_running"] = 0
            scalar_metrics["pipeline_exporter_scrape_errors_total"] += 1


# ── MySQL ─────────────────────────────────────────────────────
def scrape_mysql():
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            connect_timeout=5,
        )
        conn.ping(reconnect=False)
        conn.close()
        with metrics_lock:
            scalar_metrics["pipeline_mysql_reachable"] = 1
    except Exception as e:
        log.warning(f"MySQL scrape failed: {e}")
        with metrics_lock:
            scalar_metrics["pipeline_mysql_reachable"] = 0
            scalar_metrics["pipeline_exporter_scrape_errors_total"] += 1


# ── Hudi ──────────────────────────────────────────────────────
def _parse_hudi_commit_ts(name):
    try:
        base = name.split(".")[0]
        return datetime.strptime(base[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def scrape_hudi():
    try:
        s3 = get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")

        # Timeline: parse instants and count completed operations
        timeline_prefix = f"{HUDI_PREFIX}/.hoodie/"
        delta_count = commit_count = clean_count = 0
        latest_commit_ts = None
        instants = {}

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=timeline_prefix, Delimiter="/"):
            for obj in page.get("Contents", []):
                name = obj["Key"].split("/")[-1]
                parts = name.split(".")
                if len(parts) < 2:
                    continue
                instant = parts[0]
                if not instant.isdigit():
                    continue

                suffix = ".".join(parts[1:])
                instants.setdefault(instant, set()).add(suffix)

                if suffix == "deltacommit":
                    delta_count += 1
                    ts = _parse_hudi_commit_ts(name)
                    if ts and (latest_commit_ts is None or ts > latest_commit_ts):
                        latest_commit_ts = ts
                elif suffix == "commit":
                    commit_count += 1
                    ts = _parse_hudi_commit_ts(name)
                    if ts and (latest_commit_ts is None or ts > latest_commit_ts):
                        latest_commit_ts = ts
                elif suffix == "clean":
                    clean_count += 1

        # Truly inflight = has .inflight/.requested but no completed counterpart
        truly_inflight = sum(
            1 for suffixes in instants.values()
            if any(s.endswith("inflight") or s.endswith("requested") for s in suffixes)
            and not any(s in COMPLETED_SUFFIXES for s in suffixes)
        )

        # Data files
        data_prefix = f"{HUDI_PREFIX}/"
        base_files = log_files = total_files = 0
        file_groups = set()
        total_size = 0

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=data_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "/.hoodie/" in key or key.endswith(".hoodie_partition_metadata"):
                    continue
                total_files += 1
                total_size += obj.get("Size", 0)
                name = key.split("/")[-1]

                if name.startswith("."):
                    m = re.match(r"\.([^_]+)_", name)
                    if m:
                        file_groups.add(m.group(1))
                    log_files += 1
                elif name.endswith(".parquet"):
                    m = re.match(r"([^_]+)_", name)
                    if m:
                        file_groups.add(m.group(1))
                    base_files += 1

        with metrics_lock:
            scalar_metrics["pipeline_hudi_deltacommit_count"] = delta_count
            scalar_metrics["pipeline_hudi_commit_count"] = commit_count
            scalar_metrics["pipeline_hudi_clean_count"] = clean_count
            scalar_metrics["pipeline_hudi_inflight_operations"] = truly_inflight
            scalar_metrics["pipeline_hudi_data_files_total"] = total_files
            scalar_metrics["pipeline_hudi_base_files_total"] = base_files
            scalar_metrics["pipeline_hudi_log_files_total"] = log_files
            scalar_metrics["pipeline_hudi_file_groups_total"] = len(file_groups)
            scalar_metrics["pipeline_hudi_table_size_bytes"] = total_size
            if latest_commit_ts:
                scalar_metrics["pipeline_hudi_last_commit_age_seconds"] = int(
                    (datetime.now(timezone.utc) - latest_commit_ts).total_seconds()
                )
            else:
                scalar_metrics["pipeline_hudi_last_commit_age_seconds"] = -1

    except Exception as e:
        log.warning(f"Hudi scrape failed: {e}")
        with metrics_lock:
            scalar_metrics["pipeline_exporter_scrape_errors_total"] += 1


# ── Scrape loop ───────────────────────────────────────────────
def scrape_all():
    while True:
        start = time.time()
        scrape_kafka()
        scrape_debezium()
        scrape_mysql()
        scrape_hudi()
        duration = time.time() - start

        with metrics_lock:
            scalar_metrics["pipeline_exporter_last_scrape_timestamp"] = int(time.time())
            scalar_metrics["pipeline_exporter_scrape_duration_seconds"] = round(duration, 2)

        log.info(
            f"scrape {duration:.1f}s | "
            f"kafka={scalar_metrics['pipeline_kafka_topic_latest_offset']:,} | "
            f"lag={scalar_metrics['pipeline_spark_consumer_lag_total']:,} | "
            f"age={scalar_metrics['pipeline_hudi_last_commit_age_seconds']}s | "
            f"base={scalar_metrics['pipeline_hudi_base_files_total']} "
            f"log={scalar_metrics['pipeline_hudi_log_files_total']}"
        )
        time.sleep(max(0, SCRAPE_INTERVAL - duration))


# ── HTTP server ───────────────────────────────────────────────
class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()

        lines = []
        with metrics_lock:
            for name, value in scalar_metrics.items():
                lines.append(f"# TYPE {name} gauge")
                lines.append(f"{name} {value}")
            for name, labeled in labeled_metrics.items():
                if not labeled:
                    continue
                lines.append(f"# TYPE {name} gauge")
                for label_tuple, value in labeled.items():
                    label_str = ",".join(f'{k}="{v}"' for k, v in label_tuple)
                    lines.append(f"{name}{{{label_str}}} {value}")

        self.wfile.write(("\n".join(lines) + "\n").encode("utf-8"))

    def log_message(self, *args, **kwargs):
        pass


if __name__ == "__main__":
    Thread(target=scrape_all, daemon=True).start()
    log.info(f"Exporter listening on :{METRICS_PORT}")
    HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler).serve_forever()
