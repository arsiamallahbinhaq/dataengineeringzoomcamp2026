from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# create Spark session
spark = SparkSession.builder \
    .appName("HW6-Q3") \
    .getOrCreate()

# read parquet file
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# filter trips on Nov 15
count = df.filter(
    F.to_date("tpep_pickup_datetime") == "2025-11-15"
).count()

print("Trips on 2025-11-15:", count)