# Module 6 Homework
In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:
```
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

## Question 1: Install Spark and PySpark
- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.
What's the output?
```
uv run python test_spark.py
Spark version: 4.1.1
```

## Question 2: Yellow November 2025
Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB --> answer
- 75MB
- 100MB

Code refer to hw.py
```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HW6") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df_repart = df.repartition(4)

df_repart.write.mode("overwrite").parquet("yellow_2025_11")
```

Using command in linux:
```
ls -lh yellow_2025_11
```

```
-rw-r--r-- 1 codespace codespace   0 Mar  9 13:56 _SUCCESS
-rw-r--r-- 1 codespace codespace 26M Mar  9 13:56 part-00000-836fb95a-3962-4cf7-aee8-8125c4061a48-c000.snappy.parquet
-rw-r--r-- 1 codespace codespace 26M Mar  9 13:56 part-00001-836fb95a-3962-4cf7-aee8-8125c4061a48-c000.snappy.parquet
-rw-r--r-- 1 codespace codespace 26M Mar  9 13:56 part-00002-836fb95a-3962-4cf7-aee8-8125c4061a48-c000.snappy.parquet
-rw-r--r-- 1 codespace codespace 26M Mar  9 13:56 part-00003-836fb95a-3962-4cf7-aee8-8125c4061a48-c000.snappy.parquet
```

## Question 3: Count records
How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- 162,604 --> answer
- 225,768

```
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
```

## Question 4: Longest trip
What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- 90.6 --> answer
- 134.5

```
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

+-----------------+                                                             
|  max(trip_hours)|
+-----------------+
|90.64666666666666|
+-----------------+
```

## Question 5: User Interface
Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040 --> answer
- 8080

## Question 6: Least frequent pickup location zone
Load the zone lookup data into a temp view in Spark:
```
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```
Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay
If multiple answers are correct, select any

```
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

```

```
+---------------------------------------------+-----+                           
|Zone                                         |count|
+---------------------------------------------+-----+
|Governor's Island/Ellis Island/Liberty Island|1    |
|Eltingville/Annadale/Prince's Bay            |1    |
|Arden Heights                                |1    |
|Port Richmond                                |3    |
|Rikers Island                                |4    |
|Rossville/Woodrow                            |4    |
|Great Kills                                  |4    |
|Green-Wood Cemetery                          |4    |
|Jamaica Bay                                  |5    |
|Westerleigh                                  |12   |
+---------------------------------------------+-----+
```