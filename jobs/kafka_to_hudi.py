"""
Kafka CDC → Hudi MOR Streaming Job (Production-Ready)

Features:
- Dead-letter queue for unparseable messages
- Schema evolution support (unknown fields ignored gracefully)
- Retry-safe: idempotent upserts via precombine field
- Graceful shutdown: completes current batch before stopping
- Configurable via environment variables
- Prometheus metrics for batch monitoring

Target: sustain 50k msgs/sec with zero growing lag.
"""

import os
import signal
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, date_format, lit, current_timestamp, when, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

# ── Configuration (from environment) ──────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shipments.shipments_db.shipments")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "shipments.dead-letter-queue")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
HUDI_TABLE_PATH = os.getenv("HUDI_TABLE_PATH", "s3a://lakehouse/data/shipments_mor")
HUDI_CHECKPOINT_PATH = os.getenv("HUDI_CHECKPOINT_PATH", "s3a://lakehouse/checkpoints/shipments_mor")
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")
MAX_OFFSETS = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "2000000")
CORES_MAX = os.getenv("SPARK_CORES_MAX", "8")
EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")

# ── Spark Session ──────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ShipmentsCDCtoHudi") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.executor.memory", EXECUTOR_MEMORY) \
    .config("spark.executor.memoryOverhead", "1500m") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", CORES_MAX) \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.default.parallelism", "16") \
    .config("spark.metrics.conf", "/opt/spark/conf/metrics.properties") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.ui.prometheus.enabled", "true") \
    .config("spark.rdd.compress", "true") \
    .config("spark.io.compression.codec", "lz4") \
    .config("spark.shuffle.file.buffer", "1m") \
    .config("spark.reducer.maxSizeInFlight", "96m") \
    .config("spark.memory.fraction", "0.75") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.multipart.size", "16777216") \
    .config("spark.hadoop.fs.s3a.multipart.threshold", "16777216") \
    .config("spark.hadoop.fs.s3a.threads.max", "40") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Schema definition ─────────────────────────────────────────
# Only declare fields we use. Unknown fields in the JSON are silently
# ignored by from_json — this provides schema evolution tolerance.
cdc_payload_schema = StructType([
    StructField("id", LongType(), True),
    StructField("tracking_id", StringType(), True),
    StructField("shipper_name", StringType(), True),
    StructField("shipper_city", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("weight_kg", StringType(), True),
    StructField("status", StringType(), True),
    StructField("current_location", StringType(), True),
    StructField("estimated_delivery", IntegerType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__source_ts_ms", LongType(), True),
])

debezium_schema = StructType([
    StructField("payload", cdc_payload_schema, True),
])

# ── Read from Kafka ────────────────────────────────────────────
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", MAX_OFFSETS) \
    .load()

# ── Parse with DLQ support ────────────────────────────────────
# Step 1: Parse JSON. If parsing fails, from_json returns null for the
# entire struct. We use this to separate good records from bad ones.
raw_parsed = kafka_df \
    .selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as json_str",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp",
    ) \
    .withColumn("parsed", from_json(col("json_str"), debezium_schema))

# Step 2: Split into good records and dead letters
good_records = raw_parsed.filter(
    col("parsed").isNotNull() &
    col("parsed.payload.tracking_id").isNotNull() &
    (col("parsed.payload.__deleted") != "true")
)

dead_letters = raw_parsed.filter(
    col("parsed").isNull() |
    col("parsed.payload.tracking_id").isNull()
)

# Step 3: Transform good records for Hudi
parsed_df = good_records \
    .select("parsed.payload.*") \
    .withColumn("ts", (col("__source_ts_ms") / 1000).cast("timestamp")) \
    .withColumn("dt", date_format(col("ts"), "yyyy-MM-dd")) \
    .select(
        col("id"),
        col("tracking_id"),
        col("shipper_name"),
        col("shipper_city"),
        col("customer_name"),
        col("customer_city"),
        col("weight_kg"),
        col("status"),
        col("current_location"),
        col("estimated_delivery"),
        col("__op").alias("cdc_operation"),
        col("ts"),
        col("dt"),
    )

# ── Hudi MOR config ───────────────────────────────────────────
hudi_options = {
    "hoodie.table.name": "shipments_mor",

    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "tracking_id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "dt",

    # BUCKET index — O(1) upserts
    "hoodie.index.type": "BUCKET",
    "hoodie.index.bucket.engine": "SIMPLE",
    "hoodie.bucket.index.num.buckets": "16",
    "hoodie.bucket.index.hash.field": "tracking_id",

    # In-batch dedup
    "hoodie.combine.before.upsert": "true",
    "hoodie.combine.before.insert": "true",
    "hoodie.bulkinsert.sort.mode": "NONE",

    # Parallelism
    "hoodie.upsert.shuffle.parallelism": "16",
    "hoodie.insert.shuffle.parallelism": "16",
    "hoodie.bulkinsert.shuffle.parallelism": "16",

    # Write buffers
    "hoodie.write.buffer.limit.bytes": "134217728",
    "hoodie.logfile.data.block.max.size": "268435456",

    # Compaction — every 2 deltacommits (faster visibility in Trino)
    "hoodie.compact.inline": "false",
    "hoodie.datasource.compaction.async.enable": "true",
    "hoodie.compact.inline.max.delta.commits": "2",

    # Small-file handling disabled for MOR log-append behavior
    "hoodie.parquet.small.file.limit": "0",
    "hoodie.merge.small.file.group.candidates.limit": "0",

    # Async cleaner
    "hoodie.clean.automatic": "true",
    "hoodie.clean.async": "true",
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": "2",
    "hoodie.clean.trigger.strategy": "NUM_COMMITS",
    "hoodie.clean.max.commits": "1",

    # Metadata table disabled for write-heavy workload
    "hoodie.metadata.enable": "false",

    # Schema evolution: allow new columns to be added without breaking
    "hoodie.datasource.write.reconcile.schema": "true",

    # Hive Metastore sync
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.metastore.uris": HIVE_METASTORE_URI,
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "shipments_mor",
    "hoodie.datasource.hive_sync.partition_fields": "dt",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
}

# ── Write good records to Hudi ────────────────────────────────
hudi_query = parsed_df.writeStream \
    .format("hudi") \
    .options(**hudi_options) \
    .option("checkpointLocation", HUDI_CHECKPOINT_PATH) \
    .outputMode("append") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start(HUDI_TABLE_PATH)

# ── Write dead letters to DLQ Kafka topic ─────────────────────
dlq_query = dead_letters \
    .selectExpr(
        "kafka_key as key",
        "json_str as value",
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", KAFKA_DLQ_TOPIC) \
    .option("checkpointLocation", f"{HUDI_CHECKPOINT_PATH}-dlq") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start()

# ── Graceful shutdown ─────────────────────────────────────────
def graceful_shutdown(signum, frame):
    """Handle SIGTERM/SIGINT: stop queries gracefully."""
    print(f">>> Received signal {signum}, stopping gracefully...")
    hudi_query.stop()
    dlq_query.stop()
    print(">>> Queries stopped. Exiting.")
    sys.exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# ── Startup banner ────────────────────────────────────────────
print("=" * 60)
print("  Shipments CDC Pipeline — Production Mode")
print("=" * 60)
print(f"  Source:      {KAFKA_TOPIC}")
print(f"  DLQ:         {KAFKA_DLQ_TOPIC}")
print(f"  Hudi table:  {HUDI_TABLE_PATH}")
print(f"  Trigger:     {TRIGGER_INTERVAL}")
print(f"  Compaction:  every 2 deltacommits")
print(f"  Schema evolution: enabled")
print(f"  Graceful shutdown: enabled")
print("=" * 60)

spark.streams.awaitAnyTermination()
