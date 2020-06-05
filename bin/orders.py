from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
spark = SparkSession \
    .builder \
    .appName("orders") \
    .getOrCreate()
host = ['localhost:9092']
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", host) \
  .option("subscribe", "all") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df.writeStream.format("console").start()