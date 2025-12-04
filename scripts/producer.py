#!/usr/bin/env python3
"""
=============================================================================
FinTech Fraud Detection Pipeline - Transaction Producer
=============================================================================
This script simulates credit card transactions and sends them to a Kafka topic.
It generates both valid transactions and occasional fraud patterns:
1. Impossible Travel: Same user in different countries within 10 minutes
2. High Value: Transactions with amount > $5000
=============================================================================
"""

import json
import random
import time
import uuid
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

# =============================================================================
# Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'transactions')

# Transaction generation parameters
TRANSACTION_INTERVAL_MS = 500  # Base interval between transactions
FRAUD_PROBABILITY = 0.1  # 10% chance of generating a fraud pattern
HIGH_VALUE_FRAUD_PROBABILITY = 0.5  # 50% of fraud is high value, 50% is impossible travel

# =============================================================================
# Sample Data for Transaction Generation
# =============================================================================
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant", "gas_station", "online_shopping",
    "travel", "entertainment", "healthcare", "utilities", "clothing"
]

# Countries with their major cities for location simulation
LOCATIONS = {
    "USA": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "UK": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
    "Brazil": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "India": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"],
    "Singapore": ["Singapore City"]
}

# Simulated user IDs (representing digital wallet users)
USER_IDS = [f"USER_{str(i).zfill(4)}" for i in range(1, 101)]

# Track recent transactions for impossible travel fraud
recent_user_transactions: Dict[str, Dict[str, Any]] = {}


def create_kafka_producer(max_retries: int = 10, retry_interval: int = 5) -> KafkaProducer:
    """
    Create a Kafka producer with retry logic for handling connection issues.
    
    Args:
        max_retries: Maximum number of connection attempts
        retry_interval: Seconds to wait between retries
    
    Returns:
        KafkaProducer instance
    """
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not available, retrying in {retry_interval}s...")
            time.sleep(retry_interval)
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            time.sleep(retry_interval)
    
    raise ConnectionError(f"Failed to connect to Kafka after {max_retries} attempts")


def generate_location() -> Dict[str, str]:
    """Generate a random location (country and city)."""
    country = random.choice(list(LOCATIONS.keys()))
    city = random.choice(LOCATIONS[country])
    return {"country": country, "city": city}


def generate_valid_transaction(user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Generate a valid (non-fraudulent) transaction.
    
    Args:
        user_id: Optional user ID. If not provided, random user is selected.
    
    Returns:
        Transaction dictionary
    """
    if user_id is None:
        user_id = random.choice(USER_IDS)
    
    location = generate_location()
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": round(random.uniform(5.0, 500.0), 2),  # Normal range: $5 - $500
        "location": f"{location['city']}, {location['country']}",
        "country": location['country'],
        "city": location['city']
    }
    
    # Track this transaction for potential impossible travel fraud generation
    recent_user_transactions[user_id] = {
        "timestamp": datetime.utcnow(),
        "country": location['country'],
        "city": location['city']
    }
    
    return transaction


def generate_high_value_fraud() -> Dict[str, Any]:
    """
    Generate a high-value fraudulent transaction (amount > $5000).
    
    Returns:
        Fraudulent transaction dictionary
    """
    user_id = random.choice(USER_IDS)
    location = generate_location()
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "merchant_category": random.choice(["electronics", "jewelry", "luxury_goods", "travel"]),
        "amount": round(random.uniform(5001.0, 50000.0), 2),  # High value: $5001 - $50000
        "location": f"{location['city']}, {location['country']}",
        "country": location['country'],
        "city": location['city']
    }
    
    logger.warning(f"🚨 FRAUD PATTERN: High Value Transaction - User: {user_id}, Amount: ${transaction['amount']}")
    return transaction


def generate_impossible_travel_fraud() -> List[Dict[str, Any]]:
    """
    Generate an impossible travel fraud pattern.
    Creates two transactions for the same user from different countries within a short time window.
    
    Returns:
        List of two fraudulent transactions
    """
    user_id = random.choice(USER_IDS)
    
    # Select two different countries
    countries = random.sample(list(LOCATIONS.keys()), 2)
    
    # First transaction
    location1 = {"country": countries[0], "city": random.choice(LOCATIONS[countries[0]])}
    # Second transaction (within 10 minutes - impossible travel)
    location2 = {"country": countries[1], "city": random.choice(LOCATIONS[countries[1]])}
    
    base_time = datetime.utcnow()
    
    transaction1 = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": base_time.isoformat() + "Z",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": round(random.uniform(50.0, 500.0), 2),
        "location": f"{location1['city']}, {location1['country']}",
        "country": location1['country'],
        "city": location1['city']
    }
    
    # Second transaction 2-8 minutes after the first (within 10-minute window)
    time_offset = timedelta(minutes=random.uniform(2, 8))
    transaction2 = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": (base_time + time_offset).isoformat() + "Z",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": round(random.uniform(50.0, 500.0), 2),
        "location": f"{location2['city']}, {location2['country']}",
        "country": location2['country'],
        "city": location2['city']
    }
    
    logger.warning(
        f"🚨 FRAUD PATTERN: Impossible Travel - User: {user_id}, "
        f"Location 1: {location1['country']}, Location 2: {location2['country']}, "
        f"Time gap: {time_offset.total_seconds() / 60:.1f} minutes"
    )
    
    return [transaction1, transaction2]


def send_transaction(producer: KafkaProducer, transaction: Dict[str, Any]) -> None:
    """
    Send a transaction to the Kafka topic.
    
    Args:
        producer: Kafka producer instance
        transaction: Transaction dictionary to send
    """
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=transaction['user_id'],
            value=transaction
        )
        # Block for 'synchronous' sends (optional, for debugging)
        record_metadata = future.get(timeout=10)
        logger.info(
            f"✅ Sent: {transaction['transaction_id'][:8]}... | "
            f"User: {transaction['user_id']} | "
            f"Amount: ${transaction['amount']:.2f} | "
            f"Location: {transaction['location']}"
        )
    except KafkaError as e:
        logger.error(f"Failed to send transaction: {e}")


def run_producer():
    """Main producer loop that generates and sends transactions."""
    logger.info("=" * 60)
    logger.info("Starting FinTech Transaction Producer")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Fraud Probability: {FRAUD_PROBABILITY * 100}%")
    logger.info("=" * 60)
    
    producer = create_kafka_producer()
    
    transaction_count = 0
    fraud_count = 0
    
    try:
        while True:
            # Determine if this should be a fraud transaction
            is_fraud = random.random() < FRAUD_PROBABILITY
            
            if is_fraud:
                fraud_count += 1
                # Decide fraud type
                if random.random() < HIGH_VALUE_FRAUD_PROBABILITY:
                    # High value fraud
                    transaction = generate_high_value_fraud()
                    send_transaction(producer, transaction)
                    transaction_count += 1
                else:
                    # Impossible travel fraud (generates 2 transactions)
                    transactions = generate_impossible_travel_fraud()
                    for txn in transactions:
                        send_transaction(producer, txn)
                        transaction_count += 1
                        time.sleep(0.1)  # Small delay between the pair
            else:
                # Normal transaction
                transaction = generate_valid_transaction()
                send_transaction(producer, transaction)
                transaction_count += 1
            
            # Log statistics periodically
            if transaction_count % 50 == 0:
                logger.info(
                    f"📊 Statistics - Total Transactions: {transaction_count}, "
                    f"Fraud Patterns: {fraud_count}"
                )
            
            # Random interval between transactions (200ms - 1000ms)
            sleep_time = random.uniform(0.2, 1.0)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("Shutting down producer...")
        logger.info(f"Final Statistics - Total: {transaction_count}, Fraud Patterns: {fraud_count}")
        logger.info("=" * 60)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_producer()
