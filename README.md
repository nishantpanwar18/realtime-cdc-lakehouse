# 🚀 Real-Time CDC Lakehouse Pipeline

A production-ready Change Data Capture pipeline that streams MySQL changes through Kafka into an Apache Hudi lakehouse — queryable via Trino SQL.

Built with Docker Compose. One command to start. Zero cloud dependencies.

```
MySQL → Debezium → Kafka → Spark Streaming → Hudi (on S3/MinIO) → Trino
                                    ↓
                    Prometheus + Grafana (monitoring & alerting)
```

## ✨ Features

- **Real-time CDC** — every MySQL INSERT/UPDATE/DELETE streams to the lakehouse in ~30 seconds
- **Apache Hudi MOR** — Merge-On-Read with BUCKET index for O(1) upserts at scale
- **Dead-letter queue** — bad messages go to a DLQ topic, never crash the pipeline
- **Schema evolution** — add columns to MySQL without breaking anything
- **Exactly-once semantics** — idempotent upserts via precombine field (no duplicates)
- **Full observability** — 20+ Prometheus metrics, 14 alert rules, Grafana dashboard
- **Graceful shutdown** — SIGTERM completes the current batch before stopping
- **Production-tuned** — tested at 50k+ msgs/sec sustained throughput

## 📋 Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (4+ GB RAM allocated)
- No other software needed — everything runs in containers

## 🏁 Quick Start

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/realtime-cdc-lakehouse.git
cd realtime-cdc-lakehouse

# Set up configuration
cp .env.example .env

# Start all services (first run takes 3-5 min to build images)
docker compose up -d

# Wait ~90 seconds for services to be healthy, then start the Spark streaming job
docker exec lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/kafka_to_hudi.py
```

That's it. The pipeline is now:
1. Capturing changes from MySQL via Debezium
2. Streaming them through Kafka
3. Writing to Hudi on MinIO (S3-compatible storage)
4. Queryable via Trino SQL

## 🔍 Try It

```bash
# Update a shipment in MySQL
docker exec lakehouse-mysql mysql -u root -prootpass shipments_db -e \
  "UPDATE shipments SET status='DELIVERED', current_location='Customer Home' WHERE tracking_id='SHP-10001';"

# Wait ~60 seconds, then query via Trino
docker exec lakehouse-trino trino --execute \
  "SELECT tracking_id, status, current_location FROM lakehouse.default.shipments_mor_rt ORDER BY tracking_id"
```

## 🖥️ Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | admin / password |
| Spark Master | http://localhost:8181 | — |
| Spark Job | http://localhost:4040 | — |
| Trino | http://localhost:8082 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

## 🏗️ Architecture

```
┌──────────────┐     ┌───────────────┐     ┌─────────────┐     ┌──────────────────┐
│    MySQL     │────▶│   Debezium    │────▶│    Kafka     │────▶│  Spark Streaming │
│  (Source DB) │     │    (CDC)      │     │  (Redpanda)  │     │  (Processing)    │
└──────────────┘     └───────────────┘     └─────────────┘     └────────┬─────────┘
                                                                        │
                                                               writes to S3
                                                                        │
                                                                        ▼
┌──────────────┐     ┌───────────────┐     ┌─────────────┐     ┌──────────────────┐
│    Trino     │◀───▶│Hive Metastore │◀────│  Apache Hudi │◀────│     MinIO        │
│  (SQL Query) │     │  (Catalog)    │     │  (MOR Table) │     │  (S3 Storage)    │
└──────────────┘     └───────────────┘     └─────────────┘     └──────────────────┘
                                                                        │
                                                                        ▼
┌──────────────┐     ┌───────────────┐     ┌──────────────────────────────────────┐
│   Grafana    │◀────│  Prometheus   │◀────│  Pipeline Exporter (custom metrics)  │
│ (Dashboards) │     │  (Metrics DB) │     │  Kafka lag, Hudi commits, MySQL, etc │
└──────────────┘     └───────────────┘     └──────────────────────────────────────┘
```

## 📁 Project Structure

```
├── docker-compose.yml              # All 15 services orchestrated
├── .env                            # Configuration (credentials, tuning knobs)
├── .gitignore
├── README.md
│
├── mysql/
│   └── init.sql                    # Shipments table schema + seed data
│
├── debezium/
│   └── register-connector.json     # CDC connector configuration
│
├── spark/
│   ├── Dockerfile                  # Spark 3.4 + Hudi 0.14 + Kafka connector
│   ├── entrypoint.sh              # Master/worker launcher
│   ├── metrics.properties         # Prometheus metrics sink
│   └── requirements.txt
│
├── jobs/
│   └── kafka_to_hudi.py           # ⭐ The streaming job (Kafka → Hudi)
│
├── hive-metastore/
│   ├── Dockerfile                  # Hive 3.1 with S3A support
│   └── core-site.xml             # MinIO connection config
│
├── trino/
│   └── catalog/lakehouse.properties  # Hudi connector for Trino
│
├── observability/
│   ├── exporter/
│   │   ├── exporter.py            # Custom Prometheus exporter (20+ metrics)
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── prometheus/
│   │   ├── prometheus.yml         # Scrape config
│   │   └── rules/pipeline-alerts.yml  # 14 alert rules
│   └── grafana/
│       ├── provisioning/          # Auto-configured datasource
│       └── dashboards/            # Pre-built pipeline dashboard
│
└── scripts/
    └── wait-and-register.sh       # Debezium connector auto-registration
```

## ⚙️ Configuration

All settings are in `.env`. Key tuning knobs:

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_TRIGGER_INTERVAL` | 30 seconds | How often Spark processes batches |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | 2000000 | Max Kafka messages per batch |
| `SPARK_CORES_MAX` | 8 | Total Spark executor cores |
| `SPARK_EXECUTOR_MEMORY` | 4g | Memory per executor |

## 📊 Monitoring

### Grafana Dashboard

Pre-configured at http://localhost:3000 with 6 sections:

1. **🚦 Red Lights** — component up/down status
2. **📊 Key Numbers** — producer rate, consumer rate, lag
3. **⚡ Throughput** — producer vs consumer graph
4. **🫀 Pipeline Health** — batch duration, per-partition lag
5. **🗄️ Hudi Lifecycle** — commits, compactions, file counts
6. **💾 Spark Resources** — memory usage

### Alert Rules (14)

| Alert | Severity | Trigger |
|-------|----------|---------|
| PipelineStalled | 🔴 Critical | No Hudi commit for 5+ min |
| DebeziumConnectorDown | 🔴 Critical | CDC stopped |
| MySQLUnreachable | 🔴 Critical | Source DB down |
| ConsumerLagExploding | 🔴 Critical | Lag > 10M messages |
| ConsumerLagGrowing | 🟡 Warning | Lag > 1M for 3 min |
| SparkBatchOverrun | 🟡 Warning | Batch > 60s |
| CompactionLagging | 🟡 Warning | Too many log files |
| KafkaIngestionStalled | 🟡 Warning | No new messages for 5 min |

## 🛠️ Operations

### Stop the pipeline
```bash
# Graceful stop (finishes current batch)
docker exec lakehouse-spark-master pkill -TERM -f kafka_to_hudi

# Stop all containers
docker compose down
```

### Check health
```bash
curl -s http://localhost:9200/metrics | grep -E "consumer_lag|commit_age|connector_running"
```

### View dead-letter queue
```bash
docker exec lakehouse-kafka rpk topic consume shipments.dead-letter-queue --num 10
```

### Reset everything
```bash
docker compose down -v
docker compose up -d
# Re-submit Spark job
```

## 🔬 How It Works

### Hudi Merge-On-Read (MOR)

Updates write to **delta log files** (fast, append-only). Every 2 batches, **compaction** merges logs into base parquet files (readable by Trino).

```
Batch 1: base.parquet created (INSERT)
Batch 2: .log.1 appended (UPDATE)
Batch 3: COMPACTION → new base.parquet (merged)
Batch 4: .log.1 appended (UPDATE)
Batch 5: COMPACTION → new base.parquet (merged)
...
```

### BUCKET Index

Each `tracking_id` hashes to one of 16 deterministic buckets. Upserts are O(1) — no scanning needed to find existing records.

### Exactly-Once Semantics

- Kafka → Spark: checkpoint-based (offsets committed atomically with batch)
- Spark → Hudi: `precombine` field (`ts`) ensures newer events always win
- Result: even if a batch replays, the final state is identical

## 🚀 Production Deployment (AWS)

| Local | AWS Equivalent |
|-------|---------------|
| MySQL | RDS / Aurora |
| Kafka (Redpanda) | Amazon MSK |
| MinIO | Amazon S3 |
| Spark | EMR / EMR Serverless |
| Hive Metastore | AWS Glue Data Catalog |
| Trino | Amazon Athena |
| Prometheus + Grafana | CloudWatch + Managed Grafana |

## 📄 License

MIT
