import json
import random
import time
from datetime import datetime, timezone

user_transaction_times = {}

def generate_transaction(transaction_id):
    user_id = f"user_{random.randint(1, 1000)}"
    user_home_country = "PL"
    
    # 90% transakcji z Polski, reszta za granicą
    if random.random() < 0.9:
        transaction_country = "PL"
    else:
        transaction_country = random.choice(["DE", "US", "FR", "IT", "ES"])

    amount = round(random.uniform(1.0, 10000.0), 2)
    timestamp = datetime.now()

    # Zaktualizowanie listy transakcji dla użytkownika
    if user_id not in user_transaction_times:
        user_transaction_times[user_id] = []
    # Filtrujemy transakcje, które miały miejsce w ciągu ostatnich 5 minut (300 sekund)
    user_transaction_times[user_id] = [t for t in user_transaction_times[user_id] if (timestamp - t).total_seconds() <= 300]
    user_transaction_times[user_id].append(timestamp)

    # Punkty dla transakcji
    score = 0
    if transaction_country != "PL":
        score += 2
    if amount > 5000:
        score += 2
    if 0 <= timestamp.hour <= 5:
        score += 1
    if len(user_transaction_times[user_id]) >= 3:
        score += 2

    # Zasady przydzielania fraudów
    is_fraud = 1 if score >= 5 else 0

    if is_fraud == 1 and random.random() < 0.05:
        is_fraud = 0 

    if is_fraud == 1 and random.random() < 0.03:
        is_fraud = 0

    # Stworzenie transakcji
    transaction = {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "user_home_country": user_home_country,
        "transaction_country": transaction_country,
        "amount": amount,
        "timestamp": timestamp.isoformat(),
        "score": score,
        "true_label": is_fraud
    }

    return transaction


if __name__ == "__main__":
    transaction_id = 1
    while True:
        transaction = generate_transaction(transaction_id)

        print(json.dumps(transaction))
        transaction_id += 1
        time.sleep(1)