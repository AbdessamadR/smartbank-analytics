from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SmartBankDeltaTest") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data = [(1, "test"), (2, "delta")]
df = spark.createDataFrame(data, ["id", "value"])

path = "/tmp/smartbank_delta_test"

df.write.format("delta").mode("overwrite").save(path)

result = spark.read.format("delta").load(path)

print("SPARK VERSION:", spark.version)
print("DELTA TEST ROWS:", result.count())
result.show()

spark.stop()