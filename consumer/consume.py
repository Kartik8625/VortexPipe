import os
import sys
import json
import logging
import signal
import time
from datetime import datetime
import psycopg2
import redis
from confluent_kafka import Consumer, KafkaError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("VortexPipe-Consumer")

# Configuration with hard reset network defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickstream-processors")

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "analytics")

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))  # Redis port 6380

# Global running state
running = True

def handle_shutdown(signum, frame):
    """Graceful shutdown handler."""
    global running
    logger.info(f"Signal {signum} received. Initiating graceful shutdown...")
    running = False

# Register signals for graceful termination
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def is_valid_iso8601(ts_str):
    """Checks if a string is a valid ISO 8601 datetime format."""
    if not isinstance(ts_str, str):
        return False
    try:
        # Standardize UTC 'Z' suffix to '+00:00' to support older Python versions (< 3.11)
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        datetime.fromisoformat(ts_str)
        return True
    except ValueError:
        return False

# Rebalance listener callbacks to log assigned partitions
def on_assign_callback(consumer, partitions):
    assigned_partitions = [p.partition for p in partitions]
    logger.info(f"--- CONSUMER REBALANCE (ASSIGN) --- Assigned partitions: {assigned_partitions}")

def on_revoke_callback(consumer, partitions):
    revoked_partitions = [p.partition for p in partitions]
    logger.info(f"--- CONSUMER REBALANCE (REVOKE) --- Revoked partitions: {revoked_partitions}")

def main():
    global running
    logger.info("Initializing VortexPipe Consumer Group worker...")

    # Connect to Postgres
    logger.info(f"Attempting to connect to Postgres at {DB_HOST}:{DB_PORT}")
    try:
        pg_conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        pg_conn.autocommit = False  # Explicit transactions for manual offset commits
        pg_cursor = pg_conn.cursor()
        logger.info("Connected to Postgres successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Postgres: {e}")
        sys.exit(1)

    # Connect to Redis
    logger.info(f"Attempting to connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
    try:
        r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        pg_conn.close()
        sys.exit(1)

    # Configure Kafka Consumer
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # CRITICAL: Manual commits only
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe(
            [TOPIC],
            on_assign=on_assign_callback,
            on_revoke=on_revoke_callback
        )
        logger.info(f"Subscribed to topic '{TOPIC}' under consumer group '{GROUP_ID}'.")
    except Exception as e:
        logger.error(f"Failed to configure Kafka consumer: {e}")
        pg_conn.close()
        sys.exit(1)

    logger.info("Polling for messages. Press Ctrl+C to stop.")

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event (not a error)
                continue
            else:
                logger.error(f"Kafka error: {msg.error()}")
                break

        # Process received message
        raw_payload = msg.value()
        try:
            payload = json.loads(raw_payload.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to parse JSON message payload: {e}. Rejecting event.")
            r_client.incr("rejected:total")
            consumer.commit(message=msg, asynchronous=False)
            continue

        # Schema Validation
        user_id = payload.get("user_id")
        url = payload.get("url")
        ts = payload.get("ts")
        referrer = payload.get("referrer")
        ip = payload.get("ip")

        is_valid = True
        rejections = []

        if not user_id:
            is_valid = False
            rejections.append("missing 'user_id'")
        if url is None:
            is_valid = False
            rejections.append("missing or null 'url'")
        if not is_valid_iso8601(ts):
            is_valid = False
            rejections.append("missing or invalid ISO 8601 'ts'")

        if not is_valid:
            reason = ", ".join(rejections)
            logger.warning(f"Validation failed for event (Key/User: {msg.key()}). Reason: {reason}. Skipping Postgres.")
            # Record rejection in Redis
            r_client.incr("rejected:total")
            # Commit offset to prevent the consumer from hanging on this malformed event (poison pill)
            consumer.commit(message=msg, asynchronous=False)
            continue

        # Insert into Postgres Database & windowed aggregation in Redis
        try:
            # 1. Postgres Raw SQL Insert
            insert_query = """
                INSERT INTO clicks (user_id, url, ts, referrer, ip)
                VALUES (%s, %s, %s, %s, %s);
            """
            pg_cursor.execute(insert_query, (user_id, url, ts, referrer, ip))
            pg_conn.commit()

            # 2. Redis Real-Time Windowed Aggregation (Tumbling Window)
            # Standardize timestamp and floor it to the nearest minute (YYYY-MM-DDTHH:MM)
            ts_standardized = ts[:-1] + '+00:00' if ts.endswith('Z') else ts
            dt_parsed = datetime.fromisoformat(ts_standardized)
            window_str = dt_parsed.strftime("%Y-%m-%dT%H:%M")
            redis_key = f"window:{window_str}"

            # Group zincrby and expire commands in a single network round-trip using Redis Pipeline
            redis_pipe = r_client.pipeline()
            redis_pipe.zincrby(redis_key, 1, url)
            redis_pipe.expire(redis_key, 300)  # TTL of 5 minutes (300 seconds)
            redis_pipe.execute()

            # CRITICAL: Commit offset manually ONLY AFTER successful Postgres database write.
            # Committing before the database write risks data loss. If we committed the offset
            # first and the database insert failed or the consumer crashed immediately after,
            # that message's offset would still be marked as processed. Upon restarting,
            # the consumer would resume from the next offset, meaning the unwritten message is lost.
            consumer.commit(message=msg, asynchronous=False)

        except (psycopg2.DatabaseError, Exception) as err:
            logger.error(f"Failed to process click event for user {user_id}. Rolling back transaction. Error: {err}")
            try:
                pg_conn.rollback()
            except Exception:
                pass
            # Do NOT commit Kafka offset. We want Kafka to redeliver this message upon retry/restart.
            time.sleep(1)  # Brief pause before retrying to prevent hot loops on persistent errors

    # Clean up and close connections
    logger.info("Closing database and Kafka consumer connections...")
    try:
        consumer.close()
    except Exception:
        pass
    try:
        pg_cursor.close()
        pg_conn.close()
    except Exception:
        pass
    logger.info("Consumer shutdown complete.")

if __name__ == "__main__":
    main()
