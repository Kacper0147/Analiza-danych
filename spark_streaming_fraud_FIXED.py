
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from datetime import datetime

# Inicjalizacja sesji Spark
spark = SparkSession.builder     .appName("FraudDetectionStreaming")     .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schemat danych transakcji
schema = StructType()     .add("user_id", IntegerType())     .add("amount", IntegerType())     .add("country", StringType())     .add("timestamp", StringType())

# Czytanie strumienia z Kafka
df_raw = spark.readStream     .format("kafka")     .option("kafka.bootstrap.servers", "kafka:9092")     .option("subscribe", "transactions")     .option("startingOffsets", "earliest")     .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_string")

df = df_json.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Funkcja do obliczania score
def calculate_score(amount, country, timestamp):
    score = 0
    if country != "PL":
        score += 2
    if amount > 5000:
        score += 2
    if timestamp:
        try:
            hour = timestamp.hour
            if 0 <= hour < 5:
                score += 1
        except:
            pass
    return score

@udf(IntegerType())
def score_udf(amount, country, timestamp):
    return calculate_score(amount, country, timestamp)

df_scored = df.withColumn("score", score_udf(col("amount"), col("country"), col("timestamp")))

@udf(StringType())
def risk_category(score):
    if score <= 2:
        return "normal"
    elif score <= 4:
        return "suspicious"
    else:
        return "fraud"

df_final = df_scored.withColumn("risk_level", risk_category(col("score")))

query = df_final.writeStream     .outputMode("append")     .format("console")     .start()

query.awaitTermination()
