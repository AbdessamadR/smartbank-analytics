
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SmartBank-Lakehouse-Schema-Inspection")
    .getOrCreate()
)

BASE = "/opt/spark/data/lakehouse"

tables = {
    "SILVER_TRANSACTIONS": f"{BASE}/silver/transactions",
    "SILVER_ACCOUNTS": f"{BASE}/silver/accounts",
    "BRONZE_STREAMING_TRANSACTIONS": (
        f"{BASE}/bronze_streaming/transactions"
    ),
}

try:
    for name, path in tables.items():
        print(f"\n{'=' * 60}")
        print(name)
        print(f"{'=' * 60}")

        df = spark.read.format("delta").load(path)

        df.printSchema()
        print("TOTAL_ROWS:", df.count())

finally:
    spark.stop()
