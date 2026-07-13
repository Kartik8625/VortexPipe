import os
import json
import logging
import psycopg2
import redis
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("VortexPipe-Consumer")

# Hardcoded connections to bypass Windows/Docker localhost bugs
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6380
POSTGRES_HOST = '127.0.0.1'
POSTGRES_PORT = 5432
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
TOPIC = "clickstream"

def connect_postgres():
    try:
        logger.info(f"Attempting to connect to Postgres at {POSTGRES_HOST}:{POSTGRES_PORT}")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database="analytics",
            user="user",
            password="password"
        )
        logger.info("Connected to Postgres successfully.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Postgres: {e}")
        raise

def connect_redis():
    try:
        logger.info(f"Attempting to connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        logger.info("Connected to Redis successfully.")
        return r
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

def validate_event(event):
    """Validates the schema of the incoming event."""
    if not event.get('user_id'):
        return False, "Missing user_id"
    if not event.get('url'):
        return False, "Missing url"
    try:
        # Check if timestamp is valid ISO 8601
        datetime.fromisoformat(event.get('ts').replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return False, "Invalid timestamp"
    return True, "Valid"

def assignment_callback(consumer, partitions):
    for p in partitions:
        logger.info(f"Assigned to {p.topic}, partition {p.partition}")

def main():
    logger.info("Initializing VortexPipe Consumer Group worker...")
    
    # Connect to backing services
    pg_conn = connect_postgres()
    pg_cursor = pg_conn.cursor()
    redis_client = connect_redis()

    # Configure Kafka Consumer
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'clickstream-processors',
        'enable.auto.commit': False,  # Manual commits for at-least-once delivery
        'auto.offset.reset': 'earliest' # Start from beginning to catch missed data!
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC], on_assign=assignment_callback)
    
    logger.info(f"Subscribed to topic '{TOPIC}'. Waiting for messages...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka Error: {msg.error()}")
                    continue

            # Parse JSON
            try:
                event = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError:
                redis_client.incr("rejected:total")
                consumer.commit(message=msg)
                continue

            # Validate
            is_valid, reason = validate_event(event)
            
            if not is_valid:
                # Log error to Redis, don't crash, don't save to Postgres
                redis_client.incr("rejected:total")
                logger.warning(f"Malformed event rejected: {reason}. Key: {msg.key()}")
                consumer.commit(message=msg)
                continue

            # Insert into Postgres
            try:
                pg_cursor.execute("""
                    INSERT INTO clicks (user_id, url, ts, referrer, ip)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    event.get('user_id'),
                    event.get('url'),
                    event.get('ts'),
                    event.get('referrer'),
                    event.get('ip')
                ))
                
                # CRITICAL FIX: Commit the database transaction!
                pg_conn.commit()
                
                # Commit Kafka offset ONLY AFTER successful database write
                consumer.commit(message=msg)
                
            except Exception as e:
                logger.error(f"Database error: {e}")
                pg_conn.rollback() # Rollback bad transaction so we can try again
                # Notice we do NOT commit the Kafka offset here, meaning it will retry later
                
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        pg_cursor.close()
        pg_conn.close()

if __name__ == "__main__":
    main()