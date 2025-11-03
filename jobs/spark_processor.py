from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType, DoubleType,
    LongType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

# Configuration - Use internal Docker network hostnames
KAFKA_BROKER = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "log_finance_transactions_v1"
AGGREGATES_TOPIC = "finance_transaction_aggregates_v1"
ANOMALIES_TOPIC = "finance_transaction_anomalies_v1"
CHECKPOINT_DIR = "/mnt/spark-checkpoints"
STATES_DIR = "/mnt/spark-state"

# Anomaly detection thresholds
AMOUNT_THRESHOLD_MULTIPLIER = 3.0  # Flag transactions 3x above average
TRANSACTION_COUNT_THRESHOLD = 10   # Flag if user has >10 transactions in window

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("FinanceTransactionProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR)
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("transaction_time", LongType(), False),
    StructField("merchant", StringType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_method", StringType(), True),
])

print(f"Connecting to Kafka brokers: {KAFKA_BROKER}")
print(f"Reading from topic: {SOURCE_TOPIC}")

# Read from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "10000")
    .option("kafka.session.timeout.ms", "30000")
    .option("kafka.request.timeout.ms", "40000")
    .option("kafka.max.poll.interval.ms", "300000")
    .load()
)

print("Kafka stream created successfully")

# Parse JSON and add watermark
transaction_df = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_timestamp")
    .select(
        F.from_json(F.col("value"), transaction_schema).alias("data"),
        F.col("kafka_timestamp")
    )
    .select("data.*", "kafka_timestamp")
    .withColumn(
        "transaction_timestamp",
        F.when(
            F.col("transaction_time") > 1000000000000,  # Milliseconds
            F.from_unixtime(F.col("transaction_time") / 1000)
        ).otherwise(
            F.from_unixtime(F.col("transaction_time"))  # Seconds
        ).cast(TimestampType())
    )
    .withWatermark("transaction_timestamp", "10 minutes")
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("amount") > 0)
)

# =============================================================================
# Stream 1: Aggregates by Merchant (Windowed)
# =============================================================================
merchant_agg_df = (
    transaction_df
    .groupBy(
        F.col("merchant"),
        F.window(F.col("transaction_timestamp"), "5 minutes", "1 minute")
    )
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("average_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
        F.countDistinct("user_id").alias("unique_users"),
        F.collect_set("currency").alias("currencies_used")
    )
    .select(
        F.col("merchant"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("total_transactions"),
        F.col("total_amount"),
        F.col("average_amount"),
        F.col("min_amount"),
        F.col("max_amount"),
        F.col("unique_users"),
        F.col("currencies_used")
    )
)

# Write aggregates to Kafka
print(f"Starting aggregates stream to topic: {AGGREGATES_TOPIC}")
agg_query = (
    merchant_agg_df
    .withColumn("key", F.col("merchant").cast("string"))
    .withColumn(
        "value",
        F.to_json(
            F.struct(
                "merchant", "window_start", "window_end",
                "total_transactions", "total_amount", "average_amount",
                "min_amount", "max_amount", "unique_users", "currencies_used"
            )
        ).cast("string")
    )
    .select("key", "value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", AGGREGATES_TOPIC)
    .outputMode("update")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
    .queryName("MerchantAggregates")
    .start()
)

# =============================================================================
# Stream 2: Anomaly Detection
# =============================================================================
# Calculate user statistics in sliding windows
user_stats_df = (
    transaction_df
    .groupBy(
        F.col("user_id"),
        F.window(F.col("transaction_timestamp"), "30 minutes")
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_spent"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.stddev("amount").alias("stddev_amount"),
        F.max("amount").alias("max_amount_in_window")
    )
)

# Join transactions with their user statistics to detect anomalies
anomalies_df = (
    transaction_df.alias("t")
    .join(
        user_stats_df.alias("s"),
        (F.col("t.user_id") == F.col("s.user_id")) &
        (F.col("t.transaction_timestamp") >= F.col("s.window.start")) &
        (F.col("t.transaction_timestamp") < F.col("s.window.end")),
        "left"
    )
    .select(
        F.col("t.*"),
        F.col("s.transaction_count"),
        F.col("s.avg_transaction_amount")
    )
    .filter(
        # Anomaly conditions - use coalesce to handle null avg_transaction_amount
        (F.col("amount") > F.coalesce(F.col("avg_transaction_amount") * AMOUNT_THRESHOLD_MULTIPLIER, F.lit(10000))) |
        (F.col("transaction_count") > TRANSACTION_COUNT_THRESHOLD) |
        ((F.col("is_international") == True) & (F.col("amount") > 1000))
    )
    .withColumn(
        "anomaly_reasons",
        F.array_compact(F.array(
            F.when(
                F.col("amount") > F.coalesce(F.col("avg_transaction_amount") * AMOUNT_THRESHOLD_MULTIPLIER, F.lit(10000)),
                F.lit("HIGH_AMOUNT")
            ),
            F.when(
                F.col("transaction_count") > TRANSACTION_COUNT_THRESHOLD,
                F.lit("HIGH_FREQUENCY")
            ),
            F.when(
                (F.col("is_international") == True) & (F.col("amount") > 1000),
                F.lit("HIGH_INTERNATIONAL")
            )
        ))
    )
    .withColumn("detected_at", F.current_timestamp())
)

# Write anomalies to Kafka
print(f"Starting anomaly detection stream to topic: {ANOMALIES_TOPIC}")
anomaly_query = (
    anomalies_df
    .withColumn("key", F.col("transaction_id").cast("string"))
    .withColumn(
        "value",
        F.to_json(
            F.struct(
                "transaction_id", "user_id", "amount", "merchant",
                "transaction_timestamp", "is_international",
                "transaction_count", "avg_transaction_amount",
                "anomaly_reasons", "detected_at"
            )
        ).cast("string")
    )
    .select("key", "value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", ANOMALIES_TOPIC)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies")
    .queryName("AnomalyDetection")
    .start()
)

# =============================================================================
# Monitor and Wait
# =============================================================================
print("\n" + "="*70)
print("Finance Transaction Processing Streams Started Successfully!")
print("="*70)
print(f"Kafka Brokers: {KAFKA_BROKER}")
print(f"Source Topic: {SOURCE_TOPIC}")
print(f"Aggregates Output Topic: {AGGREGATES_TOPIC}")
print(f"Anomalies Output Topic: {ANOMALIES_TOPIC}")
print(f"\nAggregates Query ID: {agg_query.id}")
print(f"Anomaly Query ID: {anomaly_query.id}")
print("="*70)
print("\nPress Ctrl+C to stop the streams...\n")

try:
    # Keep both streams running
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n" + "="*70)
    print("Stopping streams gracefully...")
    print("="*70)
    agg_query.stop()
    anomaly_query.stop()
    spark.stop()
    print("Streams stopped successfully.")
    print("="*70)