# reaction_processor.py
# Konsument: odczytuje transakcje z tematu "transactions",
# oblicza pełne score wg 4 reguł i kategoryzuje: normal / suspicious / fraud.

import json
import time
from datetime import datetime, timedelta
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# 1) Wczytanie reguł punktowania z pliku JSON (fraud_score_rules.json)
with open("fraud_score_rules.json", "r") as f:
    rules = json.load(f)
    # rules wygląda np.:
    # [
    #   { "rule": "foreign_country", "points": 2, "condition": "transaction_country != 'PL'" },
    #   { "rule": "large_amount",    "points": 2, "condition": "amount > 5000" },
    #   { "rule": "night_txn",       "points": 1, "condition": "hour(timestamp) BETWEEN 0 AND 5" },
    #   { "rule": "freq_3_in_5min",  "points": 2, "condition": "count_last_5min >= 3" }
    # ]

# 2) Helper: sprawdza, czy transakcja miała miejsce w godzinach 00:00–05:00
def is_night(txn_ts_iso):
    hour = datetime.fromisoformat(txn_ts_iso).hour
    return 0 <= hour < 5

# 3) Kolejka przechowująca timestam­py transakcji z ostatnich 5 minut
recent_tx_times = deque()

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
        print("✅ Reactions: połączono z Kafka (consumer).")
        break
    except NoBrokersAvailable:
        print("⏳ Reactions: Kafka niedostępna, czekam 5s i próbuję ponownie...")
        time.sleep(5)

# 5) (Opcjonalnie) jeśli chcemy wysyłać alerty/blokady do oddzielnych tematów
try:
    producer_alerts = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
except NoBrokersAvailable:
    producer_alerts = None

# 6) Główna pętla: przetwarzamy każdą wiadomość z „transactions”
for msg in consumer:
    txn = msg.value
    tid     = txn.get("transaction_id")
    country = txn.get("transaction_country")
    amount  = txn.get("amount")
    ts_iso  = txn.get("timestamp")   # ISO8601 string
    txn_time = datetime.fromisoformat(ts_iso)

    # ——— Aktualizacja kolejki recent_tx_times (usuń starsze niż 5 minut) ———
    now = txn_time
    cutoff = now - timedelta(minutes=5)
    while recent_tx_times and recent_tx_times[0] < cutoff:
        recent_tx_times.popleft()
    recent_tx_times.append(now)
    count_last_5min = len(recent_tx_times)  # ile tx w ostatnich 5 min

    # ——— Obliczanie pełnego score według czterech reguł ———
    score = 0
    # (1) Foreign country:
    if country != "PL":
        score += 2
    # (2) Amount > 5000:
    if amount > 5000:
        score += 2
    # (3) Nighttime (00:00–05:00):
    if is_night(ts_iso):
        score += 1
    # (4) Przynajmniej 3 tx w ciągu ostatnich 5 min:
    if count_last_5min >= 3:
        score += 2

    # ——— Kategoryzacja: 0–2 normal / 3–4 suspicious / >=5 fraud ———
    if score <= 2:
        # normal → brak akcji (możemy opcjonalnie tylko zalogować)
        # print(f"✅ NORMAL txn={tid}, score={score}")
        pass
    elif 3 <= score <= 4:
        # suspicious → logujemy alert i ewentualnie wysyłamy do topicu „alerts”
        print(f"⚠ SUSPICIOUS txn={tid}, score={score}")
        if producer_alerts:
            producer_alerts.send("alerts", {
                "transaction_id": tid,
                "score": score,
                "category": "SUSPICIOUS",
                "timestamp": ts_iso
            })
    else:
        # fraud → logujemy, że blokujemy, i ewentualnie wysyłamy do topicu „blocks”
        print(f"⛔ FRAUD txn={tid}, score={score} – BLOCKED")
        if producer_alerts:
            producer_alerts.send("blocks", {
                "transaction_id": tid,
                "score": score,
                "category": "FRAUD",
                "timestamp": ts_iso
            })
