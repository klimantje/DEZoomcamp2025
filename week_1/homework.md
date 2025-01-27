# Question 1. Understanding docker first run


`docker run -it python:3.12.8 /bin/bash` will open a bash shell in the container.

Now we can find the pip version by running: `pip --version`

Answer:

`pip 24.3.1`

# Question 2. Understanding Docker networking and docker-compose

For this question, we run `docker compose -f 'week_1/compose.yml' up -d --build`

Now we see containers are started for postgres and pgadmin.

We see in pgadmin that we can connect to the database by using `postgres:5432`

![alt text](image.png)

# Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

Up to 1 mile: 
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 miles

## Query: 
```sql
select 
sum(case when trip_distance<=1 then 1 else 0 end) as trips_under_1_mile,
sum(case when trip_distance > 1 and trip_distance <=3 then 1 else 0 end) as trips_between_1_and_3_miles,
sum(case when trip_distance > 3 and trip_distance <=7 then 1 else 0 end) as trips_between_3_and_7_miles,
sum(case when trip_distance > 7 and trip_distance <=10 then 1 else 0 end) as trips_between_7_and_10_miles,
sum(case when trip_distance>10 then 1 else 0 end) as trips_over_10_mile
from green_taxi_Data
where lpep_dropoff_datetime >=timestamp '2019-10-01 00:00:00' 
AND lpep_dropoff_datetime< timestamp '2019-11-01 00:00:00'
```

Result:


104802	198924	109603	27678	35189


# Question 4. Longest trip for each day
```sql
select DATE_TRUNC('day',lpep_pickup_datetime ) as pickup_day, max(trip_distance) as largest_trip
from green_taxi_data
where DATE_TRUNC('day',lpep_pickup_datetime ) in ('2019-10-11', '2019-10-24', '2019-10-26','2019-10-31')
group by pickup_day
order by largest_trip desc
```
## Result: 

| date|distance|
|----|----|
|"2019-10-31 00:00:00"|	515.89|
|"2019-10-11 00:00:00"|	95.78|
|"2019-10-26 00:00:00"|	91.56|
|"2019-10-24 00:00:00"|	90.75|

# Question 5. Three biggest pickup zones

## Query

```sql
Select "Zone", sum(total_amount) as sum_total_amount
from green_taxi_data gtd
left join zones z on z."LocationID" = gtd."PULocationID"
where date_trunc('day', lpep_pickup_datetime)='2019-10-18'
group by "Zone"
having sum(total_amount)>13000
```

## Result:

Zone | total_amount
---|---
"East Harlem North"	| 18686.680000000088
"East Harlem South"	|16797.260000000064
"Morningside Heights"|	13029.790000000043


# Question 6. Largest tip

## Query
```sql
Select doz."Zone", max(tip_amount) as max_tip_amount
from green_taxi_data gtd
left join zones doz on doz."LocationID" = gtd."DOLocationID"
left join zones puz on puz."LocationID" = gtd."PULocationID"
where date_trunc('month', lpep_dropoff_datetime)='2019-10-01'
and puz."Zone"='East Harlem North'
group by doz."Zone"
order by max_tip_amount desc
```

## Result

"Zone"|"max_tip_amount"
--|--
"JFK Airport"|87.3

# Question 7. Terraform Workflow

I had some issues with google cloud cli, however, after watching the videos and consulting the [terraform docs](https://developer.hashicorp.com/terraform/cli/commands/apply) we know the answer is:

`terraform init, terraform apply -auto-approve, terraform destroy`

