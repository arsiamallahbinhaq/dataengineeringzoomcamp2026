# Quiz Questions

Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.

## 1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
```
gzip -l yellow_tripdata_2020-12.csv.gz

 compressed        uncompressed  ratio uncompressed_name
   26524738           134481400  80.3% yellow_tripdata_2020-12.csv
   Convert from MB to MiB
   1 MB = 0.95367431640625 MiB
```

```
128.3 MiB <--
134.5 MiB  
364.7 MiB
692.6 MiB
```

## 2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

```
Execute flow 4 and pointing to Output files
```

```
{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
green_tripdata_2020-04.csv <--
green_tripdata_04_2020.csv
green_tripdata_2020.csv
```

## 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
```
total=0
for f in yellow_tripdata_2020-*.csv.gz; do
  c=$(zcat "$f" | tail -n +2 | wc -l)
  echo "$f: $c"
  total=$((total + c))
done
echo "TOTAL: $total"
yellow_tripdata_2020-01.csv.gz: 6405008
yellow_tripdata_2020-02.csv.gz: 6299354
yellow_tripdata_2020-03.csv.gz: 3007292
yellow_tripdata_2020-04.csv.gz: 237993
yellow_tripdata_2020-05.csv.gz: 348371
yellow_tripdata_2020-06.csv.gz: 549760
yellow_tripdata_2020-07.csv.gz: 800412
yellow_tripdata_2020-08.csv.gz: 1007284
yellow_tripdata_2020-09.csv.gz: 1341012
yellow_tripdata_2020-10.csv.gz: 1681131
yellow_tripdata_2020-11.csv.gz: 1508985
yellow_tripdata_2020-12.csv.gz: 1461897
TOTAL: 24648499
```
```
13,537.299
24,648,499 <--
18,324,219
29,430,127
```

## 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
```
total=0
for f in green_tripdata_2020-*.csv.gz; do
  c=$(zcat "$f" | tail -n +2 | wc -l)
  echo "$f: $c"
  total=$((total + c))
done
echo "TOTAL: $total

green_tripdata_2020-01.csv.gz: 447770
green_tripdata_2020-02.csv.gz: 398632
green_tripdata_2020-03.csv.gz: 223406
green_tripdata_2020-04.csv.gz: 35612
green_tripdata_2020-05.csv.gz: 57360
green_tripdata_2020-06.csv.gz: 63109
green_tripdata_2020-07.csv.gz: 72257
green_tripdata_2020-08.csv.gz: 81063
green_tripdata_2020-09.csv.gz: 87987
green_tripdata_2020-10.csv.gz: 95120
green_tripdata_2020-11.csv.gz: 88605
green_tripdata_2020-12.csv.gz: 83130
TOTAL: 1734051
```
## 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
```
total=0
for f in yellow_tripdata_2021-03*.csv.gz; do
  c=$(zcat "$f" | tail -n +2 | wc -l)
  echo "$f: $c"
  total=$((total + c))
done
echo "TOTAL: $total
yellow_tripdata_2021-03.csv.gz: 1925152

You can also check from output in Kestra when extract data and load into Postgres
1925152
```

# 6. How would you configure the timezone to New York in a Schedule trigger? 

- Add a timezone property set to EST in the Schedule trigger configuration 
- Add a timezone property set to America/New_York in the Schedule trigger configuration <- 
- Add a timezone property set to UTC-5 in the Schedule trigger configuration 
- Add a location property set to New_York in the Schedule trigger configuration

I got it from kestra documentation: https://kestra.io/docs/workflow-components/triggers/schedule-trigger 
```
triggers:
  - id: daily
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "@daily"
    timezone: America/New_York
```

