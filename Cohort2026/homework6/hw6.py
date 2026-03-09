from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HW6-Q6").getOrCreate()

# load trip data
trips = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# load zone lookup
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

# join
df = trips.join(
    zones,
    trips.PULocationID == zones.LocationID,
    "inner"
)

# count pickup per zone
result = df.groupBy("Zone") \
    .count() \
    .orderBy("count")

result.show(10, False)