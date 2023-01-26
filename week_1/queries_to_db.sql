/**How many trips were made on January 15th?*/
SELECT
count(*) as cnt
FROM green_taxi_data
WHERE
	lpep_pickup_datetime >= '2019-01-15 00:00:00' AND lpep_dropoff_datetime <= '2019-01-15 23:59:59';

/*Which was the day with the largest trip distance?*/
select * from green_taxi_data gt
order by gt.trip_distance DESC
limit 1

/*In 2019-01-01 how many trips had 2 and 3 passengers?*/
select count(*)filter(where passenger_count=2 ) as two_passanger_cnt ,
	count(*)filter(where passenger_count=3 ) as three_passanger_cnt
from green_taxi_data
where lpep_pickup_datetime >= '2019-01-01 00:00:00' AND lpep_pickup_datetime <= '2019-01-01 23:59:59';

/*For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?*/

SELECT	/**How many trips were made on January 15th?*/
SELECT
count(*) as cnt
FROM green_taxi_data
WHERE
	lpep_pickup_datetime >= '2019-01-15 00:00:00' AND lpep_dropoff_datetime <= '2019-01-15 23:59:59';

/*Which was the day with the largest trip distance?*/
select * from green_taxi_data gt
order by gt.trip_distance DESC
limit 1

/*In 2019-01-01 how many trips had 2 and 3 passengers?*/
select count(*)filter(where passenger_count=2 ) as two_passanger_cnt ,
	count(*)filter(where passenger_count=3 ) as three_passanger_cnt
from green_taxi_data
where lpep_pickup_datetime >= '2019-01-01 00:00:00' AND lpep_pickup_datetime <= '2019-01-01 23:59:59';

/*For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?*/
SELECT	DOr."Zone" AS drop_off_zone_that_had_largest_tip
from public.green_taxi_data gt
	JOIN public.taxi_zone_lookup AS PUr ON gt."PULocationID" = PUr."LocationID"
	JOIN public.taxi_zone_lookup AS DOr ON gt."DOLocationID" = DOr."LocationID"
WHERE	PUr."Zone" = $$Astoria$$
GROUP BY	DOr."Zone"
ORDER BY	MAX(gt.tip_amount) desc LIMIT	1;






