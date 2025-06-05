# transactions.py
# Producent: generuje przykładowe transakcje co sekundę i wysyła do tematu "transactions".
# Wstępnie obliczamy score tylko w oparciu o 3 reguły (foreign_country, large_amount, nighttime).
# Regułę "3 tx w ostatnich 5 minutach" zostawimy wyłącznie w reaction_processor.py.

import json
import random
import time
import logging
from datetime import datetime
from fraud import calculate_fraud_score
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.CRITICAL)
logger = logging.getLogger("transactions")


# 1) Połączenie z KafkaProducer-em (retry w pętli, aż broker wstanie)
while True:
    try:
        logger.info("Łączę z kafką...")
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info("✅ Producer: połączono z Kafką")
        break
    except NoBrokersAvailable:
        logger.warning("⏳ Producer: Kafka niedostępna, czekam 5s i próbuję ponownie...")
        time.sleep(5)

# 2) Funkcja generująca przykładową transakcję
def generate_transaction(transaction_id):
    amount = random.randint(10, 10000)
    tx_country = get_random_country()

    transaction = {
        'transaction_id': str(transaction_id),
        'user_id': f'user_{random.randint(1,20)}',
        'transaction_country': tx_country,
        'amount': amount,
        'timestamp': get_random_datetime().isoformat(),
    }
    
    transaction["gt_is_fraud"] = get_ground_truth_is_fraud_label(all_transactions, transaction)
    
    return transaction


def get_random_country():
    # 90% transakcji z Polski, reszta za granicą
    if random.random() < 0.9:
        return "PL"
    else:
        return random.choice(["DE", "US", "FR", "IT", "ES"])


def get_random_datetime():
    current_datetime = datetime.utcnow()
    return datetime(
        year=current_datetime.year,
        month=current_datetime.month,
        day=current_datetime.day,
        hour=random.randint(0, 23),
        minute=current_datetime.minute,
        second=current_datetime.second,
        tzinfo=current_datetime.tzinfo
    )


# we simulate if the transaction is indeed a fraud
def get_ground_truth_is_fraud_label(previous_transactions, transaction):
    score = calculate_fraud_score(previous_transactions, transaction)
    
    # we mark 10% of transactions with score >= 5 as non frauds
    if score >= 5:
        if random.random() < 0.1:
            return False
        return True
    # we mark 1% of transactions with score < 5 as frauds
    else:
        if random.random() < 0.01:
            return True
        return False


# 3) Główna pętla – co 1 sekundę wysyłamy nową transakcję
if __name__ == "__main__":
    transaction_id = 1
    all_transactions = []
    
    while True:
        transaction = generate_transaction(transaction_id)
        all_transactions.append(transaction)
        
        producer.send("transactions", transaction)
        producer.flush()
        logger.info(f"🔔 sent transaction: id={transaction_id}, ground_truth_is_fraud={transaction['gt_is_fraud']}")
        
        transaction_id += 1
        time.sleep(0.25)
