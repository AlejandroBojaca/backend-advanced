from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

type_log_messages = ["INFO", "ERROR", "WARNING", "DEBUG"]
services = ["auth-service", "payment-service", "user-service", "order-service"]

def custom_partitioner(key, all_partitions, available_partitions):
    """Assign partitions based on service name"""
    if key == b'auth-service':
        return all_partitions[0]
    if key == b'payment-service':
        return all_partitions[1]
    if key == b'user-service':
        return all_partitions[2]
    if key == b'order-service':
        return all_partitions[3]
    return all_partitions[-1]

def on_send_success(record_metadata):
    logger.info(f"Message delivered to {record_metadata.topic} "
                f"[{record_metadata.partition}] @ {record_metadata.offset}")

def on_send_error(excp):
    logger.error("Failed to send message", exc_info=excp)

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        partitioner=custom_partitioner,
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )

    try:
        for _ in range(100):
            service = random.choice(services)
            log = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "type": random.choice(type_log_messages),
                "service": service,
                "message": "Simulated log message"
            }

            producer.send(
                'log_topic',
                key=service.encode('utf-8'),
                value=log
            ).add_callback(on_send_success).add_errback(on_send_error)

            time.sleep(1)

    except Exception as e:
        logger.error(f"Producer error: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()