## Week 3 - Data Warehouse
- Using Terraform created GCS bucket and trips_data_all dataset in Big Query.
- To add data for Yellow Taxi Trips (Jan 2024 - Jun 2024) in Google Cloud Storage, created dags_week3 code in airflow_hw folder also changed 
config in docker-compose.yaml volumes part (./dags_week3:/opt/airflow/dags) and run the Airflow.
- Once data added in GCS, Created Externall table in Big Query studio
```sql
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_taxi_01_2024`
OPTIONS ( format = 'PARQUET', uris = ["gs://elaborate-haven-440913-q2-terra-bucket/raw/yellow_tripdata_2024-01.parquet"] );

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_taxi_02_2024`
OPTIONS ( format = 'PARQUET', uris = ["gs://elaborate-haven-440913-q2-terra-bucket/raw/yellow_tripdata_2024-02.parquet"] );
  
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_taxi_03_2024`
OPTIONS ( format = 'PARQUET', uris = ["gs://elaborate-haven-440913-q2-terra-bucket/raw/yellow_tripdata_2024-03.parquet"] );

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_taxi_04_2024`
OPTIONS ( format = 'PARQUET', uris = ["gs://elaborate-haven-440913-q2-terra-bucket/raw/yellow_tripdata_2024-04.parquet"] );

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_taxi_05_2024`
OPTIONS ( format = 'PARQUET', uris = ["gs://elaborate-haven-440913-q2-terra-bucket/raw/yellow_tripdata_2024-05.parquet"] );

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_taxi_06_2024`
OPTIONS ( format = 'PARQUET', uris = ["gs://elaborate-haven-440913-q2-terra-bucket/raw/yellow_tripdata_2024-06.parquet"] );
```
- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
```sql
-- Creating regular table

CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_01_2024 AS
SELECT * FROM trips_data_all.external_yellow_taxi_01_2024;

CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_02_2024 AS
SELECT * FROM trips_data_all.external_yellow_taxi_02_2024;

CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_03_2024 AS
SELECT * FROM trips_data_all.external_yellow_taxi_03_2024;  

CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_04_2024 AS 
SELECT * FROM trips_data_all.external_yellow_taxi_04_2024;

CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_05_2024 AS
SELECT * FROM trips_data_all.external_yellow_taxi_05_2024;

CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_06_2024 AS
SELECT * FROM trips_data_all.external_yellow_taxi_06_2024;
```

- To solve Question 1 looked at the Details when regular table created. Added rows for all the tables created.
- For Question 2 only selecting the query and estimate is displayed on top right.
```sql
-- For External table
select count(distinct PULocationID) from trips_data_all.external_yellow_taxi_01_2024;
select count(distinct PULocationID) from trips_data_all.external_yellow_taxi_02_2024;
select count(distinct PULocationID) from trips_data_all.external_yellow_taxi_03_2024;
select count(distinct PULocationID) from trips_data_all.external_yellow_taxi_04_2024;
select count(distinct PULocationID) from trips_data_all.external_yellow_taxi_05_2024;
select count(distinct PULocationID) from trips_data_all.external_yellow_taxi_06_2024;

-- For regular table
select count(distinct PULocationID) from trips_data_all.yellow_taxi_01_2024;
select count(distinct PULocationID) from trips_data_all.yellow_taxi_02_2024;
select count(distinct PULocationID) from trips_data_all.yellow_taxi_03_2024;
select count(distinct PULocationID) from trips_data_all.yellow_taxi_04_2024;
select count(distinct PULocationID) from trips_data_all.yellow_taxi_05_2024;
select count(distinct PULocationID) from trips_data_all.yellow_taxi_06_2024;
```

- for question 3 query 
```sql
select PULocationID from trips_data_all.yellow_taxi_01_2024;
select PULocationID, DOLocationID from trips_data_all.yellow_taxi_01_2024;
```

- For question 4 adding the results from below queries
```sql
select count(fare_amount) from trips_data_all.yellow_taxi_01_2024
where fare_amount = 0;

select count(fare_amount) from trips_data_all.yellow_taxi_02_2024
where fare_amount = 0;

select count(fare_amount) from trips_data_all.yellow_taxi_03_2024
where fare_amount = 0;

select count(fare_amount) from trips_data_all.yellow_taxi_04_2024
where fare_amount = 0;

select count(fare_amount) from trips_data_all.yellow_taxi_05_2024
where fare_amount = 0;

select count(fare_amount) from trips_data_all.yellow_taxi_06_2024
where fare_amount = 0;
```
- For question 5
```sql
CREATE OR REPLACE TABLE trips_data_all.yellow_taxi_partition_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM trips_data_all.yellow_taxi_03_2024;
```
- For question 6 estimate can be made from below query
```sql
-- From normal table
select distinct(VendorID)
from trips_data_all.yellow_taxi_03_2024
where tpep_dropoff_datetime >= "2024-03-01" and tpep_dropoff_datetime <= "2024-03-15";

-- from partitioned table
select distinct(VendorID)
from trips_data_all.yellow_taxi_partition_clustered
where tpep_dropoff_datetime >= "2024-03-01" and tpep_dropoff_datetime <= "2024-03-15";
```
