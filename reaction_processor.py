# reaction_processor.py
# Konsument: odczytuje transakcje z tematu "transactions",
# oblicza pełne score wg 4 reguł i kategoryzuje: normal / suspicious / fraud.

import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from fraud import calculate_fraud_score

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.CRITICAL)
logger = logging.getLogger("reactions")


# 4) Połączenie z KafkaConsumer-em (retry, aż broker będzie gotowy)
while True:
    try:
        consumer = KafkaConsumer(
            "transactions",
            bootstrap_servers=["kafka:9092"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="reaction_group"
        )
        logger.info("✅ Reactions: połączono z Kafka (consumer).")
        break
    except NoBrokersAvailable:
        logger.warning("⏳ Reactions: Kafka niedostępna, czekam 5s i próbuję ponownie...")
        time.sleep(5)

# 5) (Opcjonalnie) jeśli chcemy wysyłać alerty/blokady do oddzielnych tematów
try:
    producer_alerts = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
except NoBrokersAvailable:
    producer_alerts = None


all_transactions = []

# 6) Główna pętla: przetwarzamy każdą wiadomość z „transactions”
for msg in consumer:
    txn = msg.value
    tid     = txn.get("transaction_id")
    country = txn.get("transaction_country")
    amount  = txn.get("amount")
    ts_iso  = txn.get("timestamp")   # ISO8601 string
    txn_time = datetime.fromisoformat(ts_iso)

    # ——— Obliczanie pełnego score według czterech reguł ———
    score = calculate_fraud_score(previous_transactions=all_transactions, transaction=txn)
    all_transactions.append(txn)

    # ——— Kategoryzacja: 0–2 normal / 3–4 suspicious / >=5 fraud ———
    category = None
    if score <= 2:
        category = "NORMAL"
        logger.info(f"✅ NORMAL txn={tid}, score={score}")
    elif 3 <= score <= 4:
        category = "SUSPICIOUS"
        logger.info(f"⚠ SUSPICIOUS txn={tid}, score={score}")
    else:
        category = "FRAUD"
        logger.info(f"⛔ FRAUD txn={tid}, score={score} – BLOCKED")

    producer_alerts.send("processed_transactions", {
        "transaction_id": tid,
        "score": score,
        "category": category,
        "gt_is_fraud": txn["gt_is_fraud"]
    })
