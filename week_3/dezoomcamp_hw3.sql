CREATE SCHEMA ny_taxi
  OPTIONS (
    default_table_expiration_days = 7,
    description = 'NY taxi rides',
    location = 'europe-west4')
  ;

CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_yellow_tripdata`
OPTIONS (
format = 'PARQUET',
uris = ['gs://dezoomcamp_hw3_2025_adr/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_non_part`
as (select * from `ny_taxi.external_yellow_tripdata`);

select count(*) from `ny_taxi.yellow_tripdata_non_part`;

select count(distinct PULocationID) from `ny_taxi.external_yellow_tripdata`;

select count(distinct PULocationID) from `ny_taxi.yellow_tripdata_non_part`;

select PULocationID from `ny_taxi.yellow_tripdata_non_part`;

select count(*) from `ny_taxi.yellow_tripdata_non_part` where fare_amount=0;

CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_part`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `ny_taxi.external_yellow_tripdata`
);

select distinct VendorID 
from `ny_taxi.yellow_tripdata_non_part` 
where tpep_dropoff_datetime>='2024-03-01' and tpep_dropoff_datetime<='2024-03-15';

select distinct VendorID 
from `ny_taxi.yellow_tripdata_part` 
where tpep_dropoff_datetime>='2024-03-01' and tpep_dropoff_datetime<='2024-03-15';
