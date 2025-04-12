from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import json
import os
import time
import logging
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_kafka():
    """Wait for Kafka to become available"""
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(":")[0]
    kafka_port = int(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(":")[1])
    
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            # Test basic TCP connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((kafka_host, kafka_port))
            sock.close()

            # Test Kafka metadata fetch using KafkaConsumer
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
                api_version=(2, 5, 0),
                request_timeout_ms=5000
            )
            consumer.topics()  # This triggers a metadata fetch
            consumer.close()
            
            logger.info("Kafka is available!")
            return True

        except (socket.error, NoBrokersAvailable, KafkaError) as e:
            if attempt == max_retries - 1:
                raise Exception(f"Kafka not available after {max_retries} attempts: {str(e)}")
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready. Waiting {retry_delay} seconds...")
            time.sleep(retry_delay)

def create_producer():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    # Wait for Kafka to be ready
    wait_for_kafka()
    
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        api_version=(2, 5, 0)  # Explicit API version
    )

# Initialize producer
producer = create_producer()

def send_kafka_event(topic, message):
    try:
        future = producer.send(topic, value=message)
        future.get(timeout=10)
        logger.info(f"Message sent to topic {topic}")
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        raise