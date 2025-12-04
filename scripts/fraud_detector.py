#!/usr/bin/env python3
"""
=============================================================================
FinTech Fraud Detection Pipeline - Speed Layer (Spark Structured Streaming)
=============================================================================
This script implements real-time fraud detection using PySpark Structured Streaming.

Fraud Detection Logic:
1. High Value Detection: Flag transactions with amount > $5000
2. Impossible Travel Detection: Flag when same user transacts from different 
   countries within a 10-minute window (using stateful processing)

Output Sinks:
- PostgreSQL (JDBC): Flagged fraud alerts
- Parquet Files: All raw transactions for batch layer consumption

EVENT TIME HANDLING:
===================
This script uses EVENT TIME semantics for processing, which is crucial for 
accurate fraud detection. Here's how event time is handled:

1. TIMESTAMP COLUMN: The 'timestamp' field from the transaction JSON is parsed
   and used as the event time. This represents when the transaction actually 
   occurred, not when it was processed by Spark.

2. WATERMARKING: We define a watermark of "10 minutes" on the event time column.
   This tells Spark:
   - How late data can arrive and still be processed
   - When to expire old state (for impossible travel detection)
   - Example: If current max event time is 12:00, Spark will still accept
     events with timestamp >= 11:50 but will drop events older than that.

3. WINDOWING FOR IMPOSSIBLE TRAVEL: We use a 10-minute tumbling window based
   on event time to detect impossible travel. Self-join compares transactions
   within the same event-time window, not processing time.

4. WHY EVENT TIME MATTERS:
   - Network delays may cause out-of-order arrival
   - Fraud patterns depend on actual transaction time, not processing time
   - Batch/stream consistency: same results regardless of processing delays
=============================================================================
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, count, 
    collect_set, size, expr, current_timestamp,
    lit, when, struct, array_distinct, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# =============================================================================
# Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'transactions')

# PostgreSQL Configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'fraud_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'fintech')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'fintech123')

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Datalake paths
DATALAKE_RAW_PATH = os.environ.get('DATALAKE_RAW_PATH', '/opt/datalake/raw')
CHECKPOINT_PATH = os.environ.get('CHECKPOINT_PATH', '/opt/datalake/checkpoints')

# Fraud detection thresholds
HIGH_VALUE_THRESHOLD = 5000.0
IMPOSSIBLE_TRAVEL_WINDOW = "10 minutes"
WATERMARK_DELAY = "10 minutes"

# =============================================================================
# Transaction Schema
# =============================================================================
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True)
])


def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession for streaming with Kafka and PostgreSQL support.
    
    Returns:
        Configured SparkSession
    """
    spark = SparkSession.builder \
        .appName("FinTech-Fraud-Detection-Speed-Layer") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created successfully")
    return spark


def read_kafka_stream(spark: SparkSession):
    """
    Read streaming data from Kafka topic.
    
    Args:
        spark: SparkSession instance
    
    Returns:
        Streaming DataFrame with parsed transactions
    """
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    
    # Read raw stream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and extract fields
    # =========================================================================
    # EVENT TIME HANDLING - STEP 1: Parse the timestamp from the source data
    # The timestamp field represents when the transaction actually occurred
    # (event time), not when Spark receives it (processing time).
    # =========================================================================
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), TRANSACTION_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))
    
    logger.info("Kafka stream configured with JSON parsing")
    return parsed_stream


def detect_high_value_fraud(stream):
    """
    Detect high-value transactions (amount > $5000).
    
    This is a simple stateless filter operation that doesn't require
    windowing or state management.
    
    Args:
        stream: Input streaming DataFrame with transactions
    
    Returns:
        Streaming DataFrame with high-value fraud alerts
    """
    # =========================================================================
    # EVENT TIME HANDLING - STEP 2: Apply watermark for late data handling
    # Even for simple filters, we add watermark to maintain consistency with
    # stateful operations and handle late-arriving data properly.
    # =========================================================================
    high_value_fraud = stream \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .filter(col("amount") > HIGH_VALUE_THRESHOLD) \
        .select(
            col("transaction_id"),
            col("user_id"),
            col("event_time").alias("detected_at"),
            col("merchant_category"),
            col("amount"),
            col("location"),
            col("country"),
            lit("HIGH_VALUE").alias("fraud_type"),
            lit(f"Transaction amount ${HIGH_VALUE_THRESHOLD} exceeded").alias("fraud_reason")
        )
    
    logger.info(f"High-value fraud detection configured (threshold: ${HIGH_VALUE_THRESHOLD})")
    return high_value_fraud


def detect_impossible_travel_fraud(stream):
    """
    Detect impossible travel fraud using stateful processing with self-join.
    
    This detects when the same user makes transactions from different countries
    within a 10-minute window - a physical impossibility indicating fraud.
    
    Uses windowed aggregation with state management to track user locations
    within each time window.
    
    Args:
        stream: Input streaming DataFrame with transactions
    
    Returns:
        Streaming DataFrame with impossible travel fraud alerts
    """
    # =========================================================================
    # EVENT TIME HANDLING - STEP 3: Watermarking for Stateful Processing
    # 
    # The watermark defines:
    # 1. How late data can arrive: Events up to 10 minutes late will still be
    #    considered for the aggregation window.
    # 2. State expiration: Spark will clear state for windows that are more
    #    than 10 minutes older than the current watermark.
    # 
    # This is CRITICAL for the impossible travel detection because:
    # - We need to compare events within the same EVENT TIME window
    # - Late events should still trigger fraud detection if within tolerance
    # - Old state must be cleaned up to prevent memory issues
    # =========================================================================
    watermarked_stream = stream \
        .withWatermark("event_time", WATERMARK_DELAY)
    
    # =========================================================================
    # EVENT TIME HANDLING - STEP 4: Window-based Aggregation
    # 
    # We use a 10-minute TUMBLING window based on EVENT TIME to group
    # transactions. The window function creates buckets like:
    # - [12:00, 12:10), [12:10, 12:20), etc.
    # 
    # All transactions with event_time in the same bucket are grouped together
    # regardless of when they were actually processed by Spark.
    # 
    # Why tumbling window?
    # - Fixed, non-overlapping windows simplify state management
    # - Clear boundaries for fraud detection logic
    # - Matches the business requirement of "within 10 minutes"
    # =========================================================================
    impossible_travel = watermarked_stream \
        .groupBy(
            col("user_id"),
            window(col("event_time"), IMPOSSIBLE_TRAVEL_WINDOW)
        ) \
        .agg(
            count("*").alias("transaction_count"),
            collect_set("country").alias("countries"),
            collect_set("transaction_id").alias("transaction_ids"),
            collect_set("location").alias("locations")
        ) \
        .filter(size(col("countries")) > 1) \
        .select(
            explode(col("transaction_ids")).alias("transaction_id"),
            col("user_id"),
            col("window.end").alias("detected_at"),
            lit("unknown").alias("merchant_category"),
            lit(0.0).alias("amount"),
            col("locations").cast("string").alias("location"),
            col("countries").cast("string").alias("country"),
            lit("IMPOSSIBLE_TRAVEL").alias("fraud_type"),
            expr("concat('User in multiple countries within 10 min: ', array_join(countries, ', '))").alias("fraud_reason")
        )
    
    logger.info(f"Impossible travel detection configured (window: {IMPOSSIBLE_TRAVEL_WINDOW})")
    return impossible_travel


def write_to_postgres(fraud_stream, table_name: str, checkpoint_suffix: str):
    """
    Write fraud alerts to PostgreSQL using JDBC sink.
    
    Uses foreachBatch to handle micro-batch writes to JDBC.
    
    Args:
        fraud_stream: Streaming DataFrame with fraud alerts
        table_name: Target PostgreSQL table name
        checkpoint_suffix: Unique suffix for checkpoint directory
    
    Returns:
        StreamingQuery object
    """
    def write_batch(batch_df, batch_id):
        """Write each micro-batch to PostgreSQL."""
        if batch_df.count() > 0:
            logger.info(f"Writing batch {batch_id} with {batch_df.count()} fraud alerts to PostgreSQL")
            batch_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
    
    query = fraud_stream \
        .writeStream \
        .foreachBatch(write_batch) \
        .outputMode("update") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/postgres_{checkpoint_suffix}") \
        .start()
    
    logger.info(f"PostgreSQL sink started for {table_name}")
    return query


def write_to_parquet(stream, path: str, checkpoint_suffix: str):
    """
    Archive all raw transactions to Parquet files for batch layer processing.
    
    Uses append mode to continuously add new data to the datalake.
    
    Args:
        stream: Streaming DataFrame with transactions
        path: Output path for Parquet files
        checkpoint_suffix: Unique suffix for checkpoint directory
    
    Returns:
        StreamingQuery object
    """
    # =========================================================================
    # EVENT TIME HANDLING - STEP 5: Watermark for Parquet Output
    # 
    # We also apply watermarking to the Parquet writer to ensure:
    # 1. Late data handling is consistent with fraud detection
    # 2. Files are written with bounded delay
    # 3. Exactly-once semantics with checkpoint recovery
    # =========================================================================
    parquet_query = stream \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .writeStream \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/parquet_{checkpoint_suffix}") \
        .outputMode("append") \
        .partitionBy("country") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info(f"Parquet archiving started at {path}")
    return parquet_query


def run_fraud_detection():
    """
    Main function to orchestrate the fraud detection streaming pipeline.
    
    Pipeline Flow:
    1. Read transactions from Kafka
    2. Detect high-value fraud (stateless)
    3. Detect impossible travel fraud (stateful)
    4. Write fraud alerts to PostgreSQL
    5. Archive all transactions to Parquet
    """
    logger.info("=" * 70)
    logger.info("Starting FinTech Fraud Detection - Speed Layer")
    logger.info("=" * 70)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} / Topic: {KAFKA_TOPIC}")
    logger.info(f"PostgreSQL: {POSTGRES_URL}")
    logger.info(f"Datalake Raw Path: {DATALAKE_RAW_PATH}")
    logger.info(f"High Value Threshold: ${HIGH_VALUE_THRESHOLD}")
    logger.info(f"Impossible Travel Window: {IMPOSSIBLE_TRAVEL_WINDOW}")
    logger.info(f"Watermark Delay: {WATERMARK_DELAY}")
    logger.info("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read Kafka stream
    transaction_stream = read_kafka_stream(spark)
    
    # Branch 1: Detect high-value fraud
    high_value_fraud_stream = detect_high_value_fraud(transaction_stream)
    
    # Branch 2: Detect impossible travel fraud  
    impossible_travel_fraud_stream = detect_impossible_travel_fraud(transaction_stream)
    
    # Start PostgreSQL writers for both fraud types
    high_value_query = write_to_postgres(
        high_value_fraud_stream, 
        "fraud_alerts", 
        "high_value"
    )
    
    impossible_travel_query = write_to_postgres(
        impossible_travel_fraud_stream, 
        "fraud_alerts", 
        "impossible_travel"
    )
    
    # Archive all raw transactions to Parquet for batch layer
    parquet_query = write_to_parquet(
        transaction_stream, 
        DATALAKE_RAW_PATH, 
        "raw_transactions"
    )
    
    # Console output for debugging (optional - can be removed in production)
    console_query = high_value_fraud_stream \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/console_debug") \
        .start()
    
    logger.info("All streaming queries started. Waiting for termination...")
    
    # Wait for any query to terminate
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        logger.info("Stopping all streaming queries...")
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        logger.info("Fraud detection pipeline stopped")


if __name__ == "__main__":
    run_fraud_detection()
