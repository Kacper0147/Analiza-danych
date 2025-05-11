from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()

# Define schema for transactions
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True)
])

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse Kafka value as JSON
transactions = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Load fraud rules
fraud_rules = spark.read.json("fraud-score-rules.json")
fraud_rules.show()

# Join transactions with fraud rules (example logic)
results = transactions.join(fraud_rules, transactions.amount > fraud_rules.amount_threshold, "left_outer")

# Write results to console
query = results.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()