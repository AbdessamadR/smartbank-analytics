
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable


BASE = "/opt/spark/data/lakehouse"

BRONZE_PATH = f"{BASE}/bronze_streaming/transactions"
ACCOUNTS_PATH = f"{BASE}/silver/accounts"

SILVER_PATH = f"{BASE}/silver_streaming/transactions"
QUARANTINE_PATH = f"{BASE}/quarantine_streaming/transactions"

CHECKPOINT_PATH = (
    f"{BASE}/checkpoints/silver_streaming_transactions_v1"
)

SILVER_COLUMNS = [
    "tran_id",
    "orig_acct",
    "bene_acct",
    "tx_type",
    "base_amt",
    "tran_timestamp",
    "is_sar",
    "alert_id",
    "raw_payload",
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "_source_file",
    "_ingestion_id",
    "_ingested_at",
    "_processed_at",
]


def merge_insert_only(spark, dataframe, path, condition):
    """
    Insert only unseen records.
    Existing records are never duplicated during retries.
    """

    if not DeltaTable.isDeltaTable(spark, path):
        (
            dataframe.limit(0)
            .write.format("delta")
            .mode("overwrite")
            .save(path)
        )

    if dataframe.limit(1).count() == 0:
        return

    target = DeltaTable.forPath(spark, path)

    (
        target.alias("target")
        .merge(dataframe.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(batch_df, batch_id, spark):
    """
    Transform one micro-batch from Bronze Streaming.
    """

    if batch_df.limit(1).count() == 0:
        print(f"BATCH {batch_id}: EMPTY")
        return

    batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        raw_count = batch_df.count()

        # 1. Convert the Bronze string fields to Silver types.
        typed = batch_df.select(
            F.expr("try_cast(tran_id AS INT)").alias("tran_id"),
            F.expr("try_cast(orig_acct AS INT)").alias("orig_acct"),
            F.expr("try_cast(bene_acct AS INT)").alias("bene_acct"),

            F.trim(F.col("tx_type")).alias("tx_type"),

            F.expr("try_cast(base_amt AS DOUBLE)").alias("base_amt"),

            F.to_timestamp(
                F.col("tran_timestamp")
            ).alias("tran_timestamp"),

            F.when(
                F.lower(F.trim(F.col("is_sar"))).isin("true", "1", "yes"),
                F.lit(True)
            ).when(
                F.lower(F.trim(F.col("is_sar"))).isin("false", "0", "no"),
                F.lit(False)
            ).otherwise(
                F.lit(None).cast("boolean")
            ).alias("is_sar"),

            F.expr("try_cast(alert_id AS INT)").alias("alert_id"),

            "raw_payload",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.col("ingested_at").alias("_ingested_at"),
            "_parse_ok",
        )

        # 2. Add lineage metadata.
        typed = (
            typed
            .withColumn(
                "_source_file",
                F.concat(
                    F.lit("kafka://"),
                    F.col("kafka_topic"),
                    F.lit("/partition="),
                    F.col("kafka_partition").cast("string"),
                    F.lit("/offset="),
                    F.col("kafka_offset").cast("string"),
                )
            )
            .withColumn(
                "_ingestion_id",
                F.concat_ws(
                    ":",
                    "kafka_topic",
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string"),
                )
            )
            .withColumn(
                "_processed_at",
                F.current_timestamp()
            )
        )

        # 3. Load reference accounts from existing Silver Batch.
        accounts = (
            spark.read.format("delta")
            .load(ACCOUNTS_PATH)
            .select(
                F.col("acct_id").cast("int").alias("ref_acct_id")
            )
            .filter(F.col("ref_acct_id").isNotNull())
            .distinct()
        )

        # 4. Check origin and beneficiary references.
        joined = (
            typed.alias("t")
            .join(
                accounts.alias("orig"),
                F.col("t.orig_acct") == F.col("orig.ref_acct_id"),
                "left"
            )
            .join(
                accounts.alias("bene"),
                F.col("t.bene_acct") == F.col("bene.ref_acct_id"),
                "left"
            )
            .select(
                *[
                    F.col(f"t.{column}")
                    for column in typed.columns
                ],
                F.col("orig.ref_acct_id").isNotNull().alias("_orig_exists"),
                F.col("bene.ref_acct_id").isNotNull().alias("_bene_exists"),
            )
        )

        # 5. Build rejection reasons.
        reasons = F.concat_ws(
            "|",

            F.when(
                ~F.coalesce(F.col("_parse_ok"), F.lit(False)),
                F.lit("INVALID_JSON")
            ),

            F.when(
                F.col("tran_id").isNull(),
                F.lit("INVALID_TRAN_ID")
            ),

            F.when(
                F.col("orig_acct").isNull(),
                F.lit("INVALID_ORIG_ACCT")
            ),

            F.when(
                F.col("bene_acct").isNull(),
                F.lit("INVALID_BENE_ACCT")
            ),

            F.when(
                F.col("tx_type").isNull() | (F.col("tx_type") == ""),
                F.lit("INVALID_TX_TYPE")
            ),

            F.when(
                F.col("base_amt").isNull(),
                F.lit("INVALID_BASE_AMOUNT")
            ),

            F.when(
                F.col("base_amt") < 0,
                F.lit("NEGATIVE_BASE_AMOUNT")
            ),

            F.when(
                F.col("tran_timestamp").isNull(),
                F.lit("INVALID_TIMESTAMP")
            ),

            F.when(
                F.col("is_sar").isNull(),
                F.lit("INVALID_IS_SAR")
            ),

            F.when(
                F.col("alert_id").isNull(),
                F.lit("INVALID_ALERT_ID")
            ),

            F.when(
                ~F.col("_orig_exists"),
                F.lit("UNKNOWN_ORIG_ACCT")
            ),

            F.when(
                ~F.col("_bene_exists"),
                F.lit("UNKNOWN_BENE_ACCT")
            ),
        )

        checked = joined.withColumn(
            "_rejection_reason",
            reasons
        )

        # 6. Separate valid and invalid records.
        invalid = checked.filter(
            F.length(F.col("_rejection_reason")) > 0
        )

        valid = checked.filter(
            F.col("_rejection_reason") == ""
        )

        invalid_count = invalid.count()
        valid_count = valid.count()

        if raw_count != invalid_count + valid_count:
            raise RuntimeError(
                f"Batch {batch_id}: reconciliation failed"
            )

        # 7. Preserve rejected transactions in quarantine.
        quarantine = (
            invalid
            .select(
                *SILVER_COLUMNS,
                "_rejection_reason"
            )
            .withColumn(
                "_source_table",
                F.lit("transactions")
            )
            .withColumn(
                "_rejected_at",
                F.current_timestamp()
            )
        )

        merge_insert_only(
            spark,
            quarantine,
            QUARANTINE_PATH,
            """
            target.kafka_topic = source.kafka_topic
            AND target.kafka_partition = source.kafka_partition
            AND target.kafka_offset = source.kafka_offset
            """
        )

        # 8. Deduplicate valid records within the micro-batch.
        window = (
            Window.partitionBy("tran_id")
            .orderBy(
                F.col("kafka_timestamp").desc_nulls_last(),
                F.col("kafka_partition").asc(),
                F.col("kafka_offset").asc()
            )
        )

        deduplicated = (
            valid
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop(
                "_rn",
                "_parse_ok",
                "_orig_exists",
                "_bene_exists",
                "_rejection_reason"
            )
            .select(*SILVER_COLUMNS)
        )

        unique_count = deduplicated.count()
        duplicate_count = valid_count - unique_count

        # 9. Incremental Delta MERGE by business key.
        merge_insert_only(
            spark,
            deduplicated,
            SILVER_PATH,
            "target.tran_id = source.tran_id"
        )

        print(f"\n========== SILVER STREAMING BATCH {batch_id} ==========")
        print("SOURCE_RECORDS:", raw_count)
        print("VALID_RECORDS:", valid_count)
        print("DEDUPLICATED_RECORDS:", duplicate_count)
        print("UNIQUE_RECORDS:", unique_count)
        print("QUARANTINED_RECORDS:", invalid_count)
        print(
            "RECONCILIATION_SUCCESS:",
            raw_count == valid_count + invalid_count
        )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("SmartBank-Silver-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Starting Silver Structured Streaming...")

        # Read Bronze Streaming as a Delta streaming source.
        bronze_stream = (
            spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 1)
            .load(BRONZE_PATH)
        )

        query = (
            bronze_stream.writeStream
            .foreachBatch(
                lambda dataframe, batch_id:
                    process_batch(dataframe, batch_id, spark)
            )
            .option(
                "checkpointLocation",
                CHECKPOINT_PATH
            )
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        # 10. Final verification.
        silver = spark.read.format("delta").load(SILVER_PATH)

        silver_count = silver.count()

        quarantine_count = 0

        if DeltaTable.isDeltaTable(spark, QUARANTINE_PATH):
            quarantine_count = (
                spark.read.format("delta")
                .load(QUARANTINE_PATH)
                .count()
            )

        print("\n========== SILVER STREAMING SUMMARY ==========")
        print("BRONZE_SOURCE:", BRONZE_PATH)
        print("SILVER_TARGET:", SILVER_PATH)
        print("QUARANTINE_TARGET:", QUARANTINE_PATH)
        print("TOTAL_SILVER_ROWS:", silver_count)
        print("TOTAL_QUARANTINED_ROWS:", quarantine_count)
        print("SILVER_STREAMING_SUCCESS: True")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
