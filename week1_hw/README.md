## Solution for Week 1 - HW

q1. Run docker with the python:3.12.8 image in an interactive mode, use the entrypoint bash.
What's the version of pip in the image?
- You need to build and run docker image from dockerfile from week1_hw/docker_sql/dockerfile.
- further after running the docker image you will enter in to bash.
- to check version, command is:
  pip --version

  q2. Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?
  ![image](https://github.com/user-attachments/assets/b6e7506e-4139-4761-a5f4-5ca408cf769f)

  q3. During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:
- Up to 1 mile
- In between 1 (exclusive) and 3 miles (inclusive),
- In between 3 (exclusive) and 7 miles (inclusive),
- In between 7 (exclusive) and 10 miles (inclusive),
- Over 10 miles
, ans -->

```sql
select count(case when trip_distance <= 1 Then 1 End) as up_to_1_mile,
count(case when trip_distance >1 and trip_distance <= 3 Then 1 End) as greater_then_one_mile,
count(case when trip_distance >3 and trip_distance <= 7 Then 1 End) as greater_then_three_mile,
count(case when trip_distance >7 and trip_distance <= 10 Then 1 End) as greater_then_seven_mile,
count(case when trip_distance >10 then 1 End) as over_ten_mile
from green_taxi_trips
where date(lpep_dropoff_datetime) >= '2019-10-01' and date(lpep_dropoff_datetime) < '2019-11-01';
```

q4. Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
Tip: For every day, we only care about one single trip with the longest distance.
2019-10-11
2019-10-24
2019-10-26
2019-10-31
, ans -->

```sql
select date(lpep_pickup_datetime), max(trip_distance)
from green_taxi_trips
group by 1
order by 2 desc;
```
q5. Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
Consider only lpep_pickup_datetime when filtering by date.
East Harlem North, East Harlem South, Morningside Heights
East Harlem North, Morningside Heights
Morningside Heights, Astoria Park, East Harlem South
Bedford, East Harlem North, Astoria Park
, ans -->

```sql
select pickup_zone."Zone", sum(green_taxi_trips."total_amount")
from green_taxi_trips
join taxi_zone_lookup AS pickup_zone
ON green_taxi_trips."PULocationID" = pickup_zone."LocationID"
where date(green_taxi_trips."lpep_pickup_datetime") = '2019-10-18' 
group by 1
having sum(green_taxi_trips."total_amount") > 13000
order by 2 desc;
```

q6. For the passengers picked up in October 2019 in the zone name "East Harlem North" which was the drop off zone that had the largest tip?
Note: it's tip , not trip
We need the name of the zone, not the ID.
Yorkville West
JFK Airport
East Harlem North
East Harlem South
, ans -->

```sql
select dropoff_zone."Zone", max(tip_amount)
from green_taxi_trips
join taxi_zone_lookup as pickup_zone
on green_taxi_trips."PULocationID" = pickup_zone."LocationID"
join taxi_zone_lookup as dropoff_zone
on green_taxi_trips."DOLocationID" = dropoff_zone."LocationID"
where date(lpep_pickup_datetime) >= '2019-10-01' and date(lpep_pickup_datetime) <= '2019-10-31'
and pickup_zone."Zone" = 'East Harlem North'
group by 1
order by 2 desc
limit 1;
```

q7. Which of the following sequences, **respectively**, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`
, ans -->

- terraform init: Downloading the provider plugins and setting up backend,
- terraform apply: auto-approve: Generating proposed changes and auto-executing the plan
- terraform destroy: Remove all resources managed by terraform

