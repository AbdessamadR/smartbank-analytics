
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class SilverTransformer:

    TABLES = {
        "transactions": ["tran_id"],
        "accounts": ["acct_id"],
        "alert_transactions": ["alert_id", "tran_id"],
        "alert_accounts": ["alert_id", "acct_id"],
        "sar_accounts": ["alert_id", "account_id"],
    }

    def __init__(self, spark, lakehouse_path):
        self.spark = spark
        self.base = lakehouse_path.rstrip("/\\")

    def transform(self, table, keys):
        source = (
            self.spark.read.format("delta")
            .load(f"{self.base}/bronze/{table}")
        )

        # Standardize column names.
        for column in source.columns:
            source = source.withColumnRenamed(
                column, column.lower()
            )

        # Validate business keys.
        for key in keys:
            if key not in source.columns:
                raise ValueError(
                    f"Missing key {key} in {table}"
                )

            source = source.filter(F.col(key).isNotNull())

        # Trim string columns.
        for name, dtype in source.dtypes:
            if dtype == "string":
                source = source.withColumn(
                    name, F.trim(F.col(name))
                )

        # Reject invalid transaction amounts.
        if table in ["transactions", "alert_transactions"]:
            source = source.filter(F.col("base_amt") >= 0)

        # Keep the most recently ingested version.
        window = (
            Window.partitionBy(*keys)
            .orderBy(F.col("_ingested_at").desc())
        )

        cleaned = (
            source
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("_processed_at", F.current_timestamp())
        )

        count = cleaned.count()

        target = f"{self.base}/silver/{table}"

        (
            cleaned.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target)
        )

        print(f"SILVER {table}: {count} rows")

        return count

    def run(self):
        results = {}

        for table, keys in self.TABLES.items():
            results[table] = self.transform(table, keys)

        return results


if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("SmartBankSilver")
        .getOrCreate()
    )

    try:
        transformer = SilverTransformer(
            spark,
            "/opt/spark/data/lakehouse"
        )

        results = transformer.run()
        print("SILVER RESULTS:", results)

    finally:
        spark.stop()
