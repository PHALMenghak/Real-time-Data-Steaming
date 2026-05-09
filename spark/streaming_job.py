import time

from kafka.admin import NewTopic
from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    stddev,
    sum,
    max,
    min,
    when,
    lit,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

# =========================================================
# Kafka Configuration
# =========================================================

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "stock-data"


# =========================================================
# Spark Session
# =========================================================

def ensure_kafka_topic():

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=1,
        replication_factor=1
    )

    last_error = None

    for attempt in range(1, 6):
        admin_client = None

        try:

            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id="spark-streaming-topic-init"
            )

            admin_client.create_topics([topic])
            return

        except TopicAlreadyExistsError:
            return

        except Exception as exc:

            last_error = exc
            print(f"Kafka topic setup attempt {attempt} failed: {exc}")

            if attempt < 5:
                time.sleep(3)

        finally:
            if admin_client is not None:
                admin_client.close()

    raise RuntimeError(
        f"Unable to ensure Kafka topic '{KAFKA_TOPIC}': {last_error}"
    )


ensure_kafka_topic()

spark = SparkSession.builder \
    .appName("RealTimeStockStreaming") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.postgresql:postgresql:42.7.3"
        ])
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# PostgreSQL Configuration
# =========================================================

POSTGRES_URL = "jdbc:postgresql://postgres:5432/stocks"

POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# =========================================================
# Define JSON Schema
# =========================================================

stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# =========================================================
# Read Stream from Kafka
# =========================================================

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# =========================================================
# Convert Kafka Binary -> String
# =========================================================

json_df = raw_df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)"
)

# =========================================================
# Parse JSON Safely
# =========================================================

parsed_df = json_df.select(
    from_json(
        col("value"),
        stock_schema
    ).alias("data")
)

# =========================================================
# Flatten JSON Structure
# =========================================================

stock_df = parsed_df.select("data.*")

# =========================================================
# Handle Malformed Data
# =========================================================

clean_df = stock_df.filter(
    col("symbol").isNotNull() &
    col("price").isNotNull() &
    col("volume").isNotNull() &
    col("timestamp").isNotNull()
)

# =========================================================
# Convert Timestamp
# =========================================================

clean_df = clean_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

# =========================================================
# Add Processing Timestamp
# =========================================================

clean_df = clean_df.withColumn(
    "processing_time",
    current_timestamp()
)

# =========================================================
# Watermarking
# =========================================================

watermarked_df = clean_df.withWatermark(
    "event_time",
    "1 minute"
)

# =========================================================
# Window Aggregation
# =========================================================

aggregated_df = watermarked_df.groupBy(
    window(col("event_time"), "1 minute"),
    col("symbol")
).agg(
    avg("price").alias("moving_avg_price"),
    stddev("price").alias("price_volatility"),
    sum("volume").alias("total_volume"),
    max("price").alias("max_price"),
    min("price").alias("min_price")
)

# =========================================================
# Percentage Price Change
# =========================================================

aggregated_df = aggregated_df.withColumn(
    "price_change_pct",
    (
        (col("max_price") - col("min_price"))
        / col("min_price")
    ) * 100
)

# =========================================================
# Volume Spike Detection
# =========================================================

aggregated_df = aggregated_df.withColumn(
    "volume_spike",
    when(
        col("total_volume") > 100000,
        lit(True)
    ).otherwise(lit(False))
)

# =========================================================
# Flatten Window Column
# =========================================================

final_df = aggregated_df.select(
    col("symbol"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("moving_avg_price"),
    col("price_volatility"),
    col("total_volume"),
    col("max_price"),
    col("min_price"),
    col("price_change_pct"),
    col("volume_spike")
)

# =========================================================
# Write Batch to PostgreSQL
# =========================================================

def write_to_postgres(batch_df, batch_id):

    print(f"Writing batch {batch_id} to PostgreSQL")

    batch_df.write \
        .jdbc(
            url=POSTGRES_URL,
            table="processed_stock_data",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )

# =========================================================
# Start Streaming Query
# =========================================================

query = final_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option(
        "checkpointLocation",
        "/tmp/spark-checkpoints/stock-streaming"
    ) \
    .trigger(processingTime="10 seconds") \
    .start()

# =========================================================
# Await Termination
# =========================================================

query.awaitTermination()