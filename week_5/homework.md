# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

The [Dockerfile](../Dockerfile) was updated to include java. We still run everything inside a [devcontainer](../.devcontainer/devcontainer.json)

The homework code can be found in the [notebook](spark.ipynb)

- [X] Install Spark
- [X] Run PySpark
- [X] Create a local spark session
- [X] Execute spark.version.

What's the output?

As specified in the `requirements.txt` file, the version displayed is `3.5.1` which corresponds with what was installed.


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- [ ] 6MB
- [X] 25MB
- [ ] 75MB
- [ ] 100MB


## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

I get 127,993 for trips that both started and ended on 15th of october.

- [ ] 85,567
- [ ] 105,567
- [X] 125,567
- [ ] 145,567


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- [ ] 122
- [ ] 142
- [X] 162
- [ ] 182


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- [ ] 80
- [ ] 443
- [X] 4040
- [ ] 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- [X] Governor's Island/Ellis Island/Liberty Island
- [ ] Arden Heights
- [ ] Rikers Island
- [ ] Jamaica Bay


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website