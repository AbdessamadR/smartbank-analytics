from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SmartBankBronzeValidation")
    .getOrCreate()
)

tables = [
    "transactions",
    "accounts",
    "alert_transactions",
    "alert_accounts",
    "sar_accounts",
]

base_path = "/opt/spark/data/lakehouse/bronze"

expected_counts = {
    "transactions": 121457,
    "accounts": 1446,
    "alert_transactions": 65,
    "alert_accounts": 73,
    "sar_accounts": 72,
}

try:
    for table in tables:
        path = f"{base_path}/{table}"

        df = spark.read.format("delta").load(path)
        actual_count = df.count()
        expected_count = expected_counts[table]

        print(f"\nTABLE: {table}")
        print(f"Expected: {expected_count}")
        print(f"Actual:   {actual_count}")
        print(f"Status:   {'OK' if actual_count == expected_count else 'ERROR'}")

        df.printSchema()

finally:
    spark.stop()