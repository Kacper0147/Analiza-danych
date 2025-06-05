import psycopg2
import time
import logging
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.CRITICAL)
logger = logging.getLogger("transactions")


def consume_from_kafka(topic_name):
    while True:
        try:
            logger.info("Connecting to kafka...")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=["kafka:9092"],
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="saving_group",
            )
            logger.info("✅ Connected to kafka as a producer.")
            break
        except NoBrokersAvailable:
            logger.warning("⏳ Kafka unavailable, retrying...")
            time.sleep(5)
    return consumer

consumer = consume_from_kafka("processed_transactions")

while True:
    try:
        logger.info("Connecting to database...")
        conn = psycopg2.connect(
            host="db", database="postgres", user="postgres", password="postgres"
        )
        break
    except psycopg2.OperationalError:
        logger.warning("⏳ Database unavailable, retrying...")
        time.sleep(5)

with conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,
                category VARCHAR(32),
                score INTEGER,
                gt_is_fraud BOOLEAN
            );
        """
        )
        conn.commit()

        transactions_buffer = []
        for msg in consumer:
            transaction = msg.value
            transactions_buffer.append(transaction)

            cur.execute(
                """
                    INSERT INTO transactions (id, category, score, gt_is_fraud)
                    VALUES (%s, %s, %s, %s);
                """,
                (
                    transaction["transaction_id"],
                    transaction["category"],
                    transaction["score"],
                    transaction["gt_is_fraud"]
                ),
            )

            if len(transactions_buffer) == 5:
                conn.commit()
                logger.info(f"Saved transactions.")
                transactions_buffer.clear()
