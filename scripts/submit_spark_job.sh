#!/bin/bash
# =============================================================================
# FinTech Fraud Detection Pipeline - Spark Job Submission Script
# =============================================================================
# This script submits the fraud detection Spark Structured Streaming job
# to the Spark cluster running in Docker.
# =============================================================================

echo "========================================"
echo "Submitting Fraud Detection Spark Job"
echo "========================================"

# Wait for Spark master to be ready
echo "Waiting for Spark Master to be ready..."
sleep 10

# Submit the Spark job
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.streaming.checkpointLocation=/opt/datalake/checkpoints \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    /opt/spark-scripts/fraud_detector.py

echo "========================================"
echo "Spark Job Completed"
echo "========================================"
