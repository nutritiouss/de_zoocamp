
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.external_fhv_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_hopeful-summer-375416/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

SELECT COUNT(*) FROM `hopeful-summer-375416.dezoomcamp.fhv_2019`;
--- 43,244,696

SELECT COUNT(*) FROM `hopeful-summer-375416.dezoomcamp.external_fhv_2019`;
--- 43,244,696



SELECT COUNT(DISTINCT affiliated_base_number)
FROM dezoomcamp.external_fhv_2019;
SELECT COUNT(DISTINCT affiliated_base_number)
FROM dezoomcamp.fhv_2019;




SELECT COUNT(DISTINCT affiliated_base_number)
FROM dezoomcamp.fhv_2019
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';


SELECT COUNT(DISTINCT affiliated_base_number)
FROM dezoomcamp.fhv_2019_2
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';