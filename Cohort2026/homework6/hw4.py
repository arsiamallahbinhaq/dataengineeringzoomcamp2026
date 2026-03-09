from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HW6-Q4").getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# hitung durasi trip dalam jam
df_with_duration = df.withColumn(
    "trip_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") -
     F.unix_timestamp("tpep_pickup_datetime")) / 3600
)

# cari trip terpanjang
df_with_duration.select(F.max("trip_hours")).show()