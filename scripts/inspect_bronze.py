from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("InspectBronze").getOrCreate()
try:
    for table in ["transactions", "accounts", "alert_transactions", "alert_accounts", "sar_accounts"]:
        df = spark.read.format("delta").load("/opt/spark/data/lakehouse/bronze/" + table)
        print("\n=== " + table.upper() + " ===")
        print(df.dtypes)
finally:
    spark.stop()
