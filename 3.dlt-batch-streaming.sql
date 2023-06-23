-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE airline22_bronze
COMMENT "The raw data ingested from Airline dataset"
AS SELECT * FROM cloud_files("/FileStore/tables/airline22/", "csv");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE airline22_silver (
  CONSTRAINT valid_origin EXPECT (OriginName IS NOT NULL),
  CONSTRAINT valid_tail_num EXPECT (TAIL_NUM IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_count EXPECT (DAY_OF_MONTH > 0) ON VIOLATION DROP ROW
)
AS SELECT 
 CAST(DAY_OF_MONTH as INT) AS DAY_OF_MONTH,
 DAY_OF_WEEK AS DAY_OF_WEEK,
 OP_UNIQUE_CARRIER AS OP_UNIQUE_CARRIER,
 TAIL_NUM AS TAIL_NUM,
 ORIGIN AS OriginName,
 ORIGIN_CITY_NAME As OriginCityName,
 DEST AS DestinationName,
 DEST_CITY_NAME AS DestinationCityName,
 DEP_TIME AS DepartureTime,
 ARR_TIME AS ArrivalTime,
 CANCELLED AS CANCELLED,
 CANCELLATION_CODE AS FlightCancelledCode,
 ACTUAL_ELAPSED_TIME AS ActualElapsedTime,
 CAST(DISTANCE AS DECIMAL(6, 2)) AS FlightDistance,
 CARRIER_DELAY AS CarrierDelay,
 CAST(WEATHER_DELAY AS DECIMAL(4, 2)) AS WeatherDelay,
 NAS_DELAY AS NASDelay,
 SECURITY_DELAY AS SecurityDelay,
 LATE_AIRCRAFT_DELAY AS AircraftDelay
FROM STREAM(LIVE.airline22_bronze);


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE airline33_bronze
COMMENT "creating DLT table from a normal Databricks table"
AS SELECT * FROM airlinereporting;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE airline33_silver
COMMENT "Creating DLT Silver Table from Batch and Streaming bronze tables"
AS
SELECT bb.DAY_OF_MONTH,
 bb.DAY_OF_WEEK,
 bb.OP_UNIQUE_CARRIER,
 bb.TAIL_NUM,
 bb.ORIGIN,
 bb.ORIGIN_CITY_NAME,
 bb.DEST,
 bb.DEST_CITY_NAME,
 bb.DEP_TIME,
 bb.ARR_TIME,
 bb.CANCELLED
FROM STREAM(LIVE.airline22_bronze) sb
INNER JOIN LIVE.airline33_bronze bb
ON sb.DAY_OF_MONTH = bb.DAY_OF_MONTH
AND sb.DAY_OF_WEEK = bb.DAY_OF_WEEK
AND sb.TAIL_NUM = bb.TAIL_NUM;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE airline33_cleaned
COMMENT "Creating DLT Silver cleaned Table from Streaming Silver tables";

APPLY CHANGES INTO
  LIVE.airline33_cleaned
FROM
  STREAM(LIVE.airline33_silver)
KEYS
  (DAY_OF_MONTH, DAY_OF_WEEK, TAIL_NUM)
APPLY AS DELETE WHEN
  CANCELLED = 1
SEQUENCE BY
  OP_UNIQUE_CARRIER
COLUMNS * EXCEPT
  (OP_UNIQUE_CARRIER);
