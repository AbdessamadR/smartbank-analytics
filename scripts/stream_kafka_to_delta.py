
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType


# =====================================================
# SMARTBANK ANALYTICS - KAFKA TO DELTA BRONZE STREAMING
# =====================================================

KAFKA_BOOTSTRAP = "kafka:19092"
KAFKA_TOPIC = "bank.transactions"

BASE_PATH = "/opt/spark/data/lakehouse"

DELTA_PATH = f"{BASE_PATH}/bronze_streaming/transactions"

CHECKPOINT_PATH = (
    f"{BASE_PATH}/checkpoints/bronze_streaming_transactions_v1"
)

# All source fields are preserved as strings in Bronze.
TRANSACTION_SCHEMA = StructType([
    StructField("tran_id", StringType(), True),
    StructField("orig_acct", StringType(), True),
    StructField("bene_acct", StringType(), True),
    StructField("tx_type", StringType(), True),
    StructField("base_amt", StringType(), True),
    StructField("tran_timestamp", StringType(), True),
    StructField("is_sar", StringType(), True),
    StructField("alert_id", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("SmartBank-Kafka-to-Delta-Bronze")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Starting SmartBank Structured Streaming...")

        # 1. Read messages from Kafka
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", "25000")
            .load()
        )

        # 2. Extract Kafka metadata and raw JSON
        raw_stream = kafka_stream.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )

        # 3. Parse JSON payload
        parsed_stream = raw_stream.withColumn(
            "_record",
            F.from_json(F.col("raw_payload"), TRANSACTION_SCHEMA)
        )

        # 4. Build Bronze Streaming records
        bronze_stream = parsed_stream.select(
            F.col("_record.tran_id").alias("tran_id"),
            F.col("_record.orig_acct").alias("orig_acct"),
            F.col("_record.bene_acct").alias("bene_acct"),
            F.col("_record.tx_type").alias("tx_type"),
            F.col("_record.base_amt").alias("base_amt"),
            F.col("_record.tran_timestamp").alias("tran_timestamp"),
            F.col("_record.is_sar").alias("is_sar"),
            F.col("_record.alert_id").alias("alert_id"),

            F.col("raw_payload"),
            F.col("kafka_key"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),

            F.current_timestamp().alias("ingested_at"),

            (
                F.col("_record").isNotNull()
                & F.col("_record.tran_id").isNotNull()
            ).alias("_parse_ok"),
        )

        # 5. Write to Delta Lake with checkpointing
        query = (
            bronze_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("path", DELTA_PATH)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        # 6. Report results
        result = spark.read.format("delta").load(DELTA_PATH)

        total = result.count()
        invalid = result.filter(~F.col("_parse_ok")).count()

        print("\n========== STREAMING SUMMARY ==========")
        print("KAFKA_TOPIC:", KAFKA_TOPIC)
        print("DELTA_PATH:", DELTA_PATH)
        print("TOTAL_RECORDS:", total)
        print("INVALID_JSON_RECORDS:", invalid)
        print("VALID_JSON_RECORDS:", total - invalid)
        print("STREAMING_SUCCESS:", invalid == 0)

        if invalid > 0:
            raise RuntimeError(
                f"{invalid} invalid JSON records found"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
