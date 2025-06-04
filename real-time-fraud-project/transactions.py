# transactions.py
# Producent: generuje przykładowe transakcje co sekundę i wysyła do tematu "transactions".
# Wstępnie obliczamy score tylko w oparciu o 3 reguły (foreign_country, large_amount, nighttime).
# Regułę "3 tx w ostatnich 5 minutach" zostawimy wyłącznie w reaction_processor.py.

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# 1) Połączenie z KafkaProducer-em (retry w pętli, aż broker wstanie)
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Producer: połączono z Kafką")
        break
    except NoBrokersAvailable:
        print("⏳ Producer: Kafka niedostępna, czekam 5s i próbuję ponownie...")
        time.sleep(5)

# 2) Funkcja generująca przykładową transakcję
def generate_transaction(transaction_id):
    countries = ['PL', 'DE', 'US', 'FR', 'CN']
    amount = random.randint(10, 10000)
    tx_country = random.choice(countries)
    timestamp = datetime.utcnow().isoformat()

    # Wstępne liczenie "score" dla 3 reguł (bez reguły freq_3_in_5min)
    score = 0
    if tx_country != 'PL':
        score += 2
    if amount > 5000:
        score += 2
    hour = datetime.utcnow().hour
    if 0 <= hour < 5:
        score += 1

    return {
        'transaction_id': str(transaction_id),
        'user_id': f'user_{random.randint(1,100)}',
        'transaction_country': tx_country,
        'amount': amount,
        'timestamp': timestamp,
        'score': score,
        'true_label': random.choice([0, 1])
    }

# 3) Główna pętla – co 1 sekundę wysyłamy nową transakcję
if __name__ == "__main__":
    tid = 1
    while True:
        txn = generate_transaction(tid)
        producer.send("transactions", txn)
        producer.flush()
        print(f"🔔 sent txn id={tid}, score={txn['score']}")
        tid += 1
        time.sleep(1)
