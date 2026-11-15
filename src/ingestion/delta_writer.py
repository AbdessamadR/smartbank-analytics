from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


class DeltaWriter:
    """Write validated Bronze snapshots into Delta Lake."""

    TABLES = {
        "transactions": "transactions.csv",
        "accounts": "accounts.csv",
        "alert_transactions": "alert_transactions.csv",
        "alert_accounts": "alert_accounts.csv",
        "sar_accounts": "sar_accounts.csv",
    }

    def __init__(self, spark: SparkSession, lakehouse_path: str):
        self.spark = spark
        self.lakehouse_path = lakehouse_path.rstrip("/\\")

    def write_snapshot(self, snapshot_path: str) -> dict:
        snapshot = Path(snapshot_path)

        if not snapshot.is_dir():
            raise FileNotFoundError(f"Snapshot not found: {snapshot}")

        results = {}

        for table_name, filename in self.TABLES.items():
            csv_path = snapshot / filename

            if not csv_path.is_file():
                continue

            dataframe = (
                self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(str(csv_path))
                .withColumn("_source_file", lit(filename))
                .withColumn("_ingestion_id", lit(snapshot.name))
                .withColumn("_ingested_at", current_timestamp())
            )

            target = f"{self.lakehouse_path}/bronze/{table_name}"

            (
                dataframe.write
                .format("delta")
                .mode("append")
                .save(target)
            )

            results[table_name] = dataframe.count()

        return results