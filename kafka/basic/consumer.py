from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import psycopg2
import json
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_connection():
    return psycopg2.connect(
        database="postgres",
        host="localhost",
        user="postgres",
        password="321825",
        port="5432"
    )

def main():
    try:
        conn = create_db_connection()
        cur = conn.cursor()

        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            group_id='my_favorite_group_v2',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000  # Exit if no messages for 10 seconds
        )
        
        consumer.subscribe(['log_topic'])
        
        for msg in consumer:
            try:
                log = json.loads(msg.value)
                timestamp = log['timestamp']
                log_type = log['type']  # Avoid shadowing built-in 'type'
                service = log['service']
                message = log['message']

                # Insert into main logs table
                cur.execute(
                    "INSERT INTO logs (timestamp, type, service, message) VALUES (%s, %s, %s, %s)",
                    (timestamp, log_type, service, message)
                )
                logger.info(f"Saved {log_type} {service} message in db")

                # If error, insert into errors table
                if log_type == "ERROR":
                    cur.execute(
                        "INSERT INTO errors (timestamp, type, service, message) VALUES (%s, %s, %s, %s)",
                        (timestamp, log_type, service, message)
                    )
                    logger.info("ERROR message saved in Error table")

                conn.commit()
                consumer.commit()
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
                conn.rollback()
            except psycopg2.Error as e:
                logger.error(f"Database error: {e}")
                conn.rollback()
            except KafkaError as e:
                logger.error(f"Kafka error: {e}")
                conn.rollback()
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}")
                conn.rollback()

    except Exception as e:
        logger.error(f"Consumer setup failed: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    main()