from datetime import datetime


def calculate_fraud_score(previous_transactions, transaction):
    country = transaction.get("transaction_country")
    amount = transaction.get("amount")
    timestamp = transaction.get("timestamp")  # ISO8601 string

    score = 0

    if country != "PL":
        score += 2

    if amount > 5000:
        score += 2

    if __is_at_night(timestamp):
        score += 1

    if (
        __get_number_of_user_transactions(
            transactions=previous_transactions,
            user_id=transaction["user_id"],
            last_n_seconds=300,
        )
        >= 3
    ):
        score += 2

    return score


# sprawdza, czy podany czas jest w godzinach 00:00–05:00
def __is_at_night(timestamp):
    hour = datetime.fromisoformat(timestamp).hour
    return 0 <= hour < 5


def __get_number_of_user_transactions(transactions, user_id, last_n_seconds):
    current_time = datetime.now().timestamp()
    user_transactions = [
        transaction for transaction in transactions if transaction["user_id"] == user_id
    ]
    user_transactions_in_last_time = [
        transaction
        for transaction in user_transactions
        if (current_time - datetime.fromisoformat(transaction["timestamp"]).timestamp())
        <= last_n_seconds
    ]
    return len(user_transactions_in_last_time)
