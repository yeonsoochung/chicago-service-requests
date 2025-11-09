-- Create dates table
CREATE OR REPLACE TABLE `chicago_service_requests.dates` AS
WITH date_range AS (
  SELECT 
    (SELECT DATE(MIN(`Created Date`)) FROM `chicago_service_requests.csr_processed_backup`) AS min_date,
    (SELECT DATE(MAX(created_date)) FROM `chicago_service_requests.csr_raw`) AS max_date
),
dates AS (
  SELECT 
    day AS Date
  FROM date_range, 
  UNNEST(GENERATE_DATE_ARRAY(min_date, max_date, INTERVAL 1 DAY)) AS day
),
dates_mo_yr_q AS (
  SELECT
    Date,
    EXTRACT(MONTH FROM Date) AS Month,
    FORMAT_DATE('%b', Date) AS `Month Name`,
    EXTRACT(YEAR FROM Date) AS Year,
    CASE 
      WHEN EXTRACT(MONTH FROM Date) BETWEEN 1 AND 3 THEN 'Q1'
      WHEN EXTRACT(MONTH FROM Date) BETWEEN 4 AND 6 THEN 'Q2'
      WHEN EXTRACT(MONTH FROM Date) BETWEEN 7 AND 9 THEN 'Q3'
      ELSE 'Q4'
    END AS Quarter
  FROM dates
)
SELECT 
  *,
  DATE_SUB(Date, INTERVAL MOD((EXTRACT(DAYOFWEEK FROM Date) + 5), 7) DAY) AS `Week Start`,
  DATE_TRUNC(Date, MONTH) AS `Month Start`,
  DATE_TRUNC(Date, QUARTER) AS `Quarter Start`,
  DATE_TRUNC(Date, YEAR) AS `Year Start`
FROM dates_mo_yr_q;

-- =======================================================
-- Stage csr_raw table
CREATE OR REPLACE TABLE `chicago_service_requests.csr_staged` AS
WITH csr_cleaned AS (
  SELECT 
    sr_number AS `SR Number`,
    sr_type AS `SR Type`,
    sr_short_code AS `SR Code`,
    owner_department AS `Owner Department`,
    status AS `Status`,
    origin AS `Origin`,
    created_date AS `Created Date`,
    last_modified_date AS `Last Modified Date`,
    closed_date AS `Closed Date`,
    TIMESTAMP_DIFF(closed_date, created_date, SECOND) AS `Completion Time (Seconds)`,
    street_address AS `Street Address`,
    street_direction AS `Street Direction`,
    INITCAP(street_name) AS `Street Name`,
    INITCAP(street_type) AS `Street Type`,
    community_area AS `Area Number`,
    created_hour AS `Created Hour`,
    FORMAT_TIMESTAMP('%A', created_date) AS `Created Day of Week`,
    created_month AS `Created Month`,
    latitude AS `Latitude`,
    longitude AS `Longitude`,
    ROUND(TIMESTAMP_DIFF(closed_date, created_date, SECOND) / 86400, 2) AS `Completion Time in Days`
  FROM `chicago_service_requests.csr_raw`
  WHERE sr_type != 'Aircraft Noise Complaint'
    AND sr_type IN (SELECT sr_type FROM `chicago_service_requests.sr_categories`)
    AND (latitude IS NOT NULL OR longitude IS NOT NULL)
    AND status IN ('Open', 'Completed')
)
SELECT 
  c.`SR Number`,
  c.`SR Type`,
  c.`Status`,
  DATE(c.`Created Date`) AS `Created Date`,
  DATE(c.`Closed Date`) AS `Closed Date`,
  c.`Completion Time in Days`,
  s.sr_category AS `SR Category`,
  s.sr_subcategory AS `SR Sub-Category`,
  c.`Street Address`,
  c.`Area Number`,
  c.`Latitude`,
  c.`Longitude`
FROM csr_cleaned AS c
JOIN `chicago_service_requests.sr_categories` AS s
  ON c.`SR Type` = s.sr_type
WHERE c.`Area Number` IS NOT NULL;

-- =======================================================
-- Create community_areas_processed table
CREATE OR REPLACE TABLE `chicago_service_requests.community_areas_processed` AS
WITH population_data AS (
  SELECT * FROM UNNEST([
    STRUCT('Far North Side' AS Side, 76 AS `Area Number`, 55711 AS Population),
    STRUCT('Far North Side', 9, 79265), STRUCT('Far North Side', 10, 57464),
    STRUCT('Far North Side', 11, 42271), STRUCT('Far North Side', 12, 35814),
    STRUCT('Far North Side', 13, 101739), STRUCT('Far North Side', 14, 69099),
    STRUCT('Far North Side', 2, 101208), STRUCT('Far North Side', 4, 11402),
    STRUCT('Far North Side', 3, 39671), STRUCT('Far North Side', 1, 27032),
    STRUCT('Far North Side', 77, 19855),

    STRUCT('Northwest Side', 17, 19398), STRUCT('Northwest Side', 18, 47663),
    STRUCT('Northwest Side', 19, 64213), STRUCT('Northwest Side', 15, 53584),
    STRUCT('Northwest Side', 16, 42574), STRUCT('Northwest Side', 20, 13765),

    STRUCT('North Side', 21, 73991), STRUCT('North Side', 22, 23157),
    STRUCT('North Side', 5, 36374), STRUCT('North Side', 6, 71384),
    STRUCT('North Side', 7, 55416),

    STRUCT('West Side', 25, 86350), STRUCT('West Side', 23, 100120),
    STRUCT('West Side', 24, 16374), STRUCT('West Side', 26, 19901),
    STRUCT('West Side', 27, 65581), STRUCT('West Side', 28, 30409),
    STRUCT('West Side', 29, 69708), STRUCT('West Side', 30, 34237),
    STRUCT('West Side', 31, 41671),

    STRUCT('Central', 8, 28216), STRUCT('Central', 32, 13193),
    STRUCT('Central', 33, 21355),

    STRUCT('South Side', 60, 6977), STRUCT('South Side', 34, 2233),
    STRUCT('South Side', 35, 24813), STRUCT('South Side', 37, 18637),
    STRUCT('South Side', 38, 12366), STRUCT('South Side', 36, 29559),
    STRUCT('South Side', 39, 24223), STRUCT('South Side', 40, 55396),
    STRUCT('South Side', 41, 31341), STRUCT('South Side', 42, 9647),
    STRUCT('South Side', 69, 30315), STRUCT('South Side', 43, 2246),

    STRUCT('Southwest Side', 56, 12087), STRUCT('Southwest Side', 64, 37610),
    STRUCT('Southwest Side', 57, 6856), STRUCT('Southwest Side', 62, 15218),
    STRUCT('Southwest Side', 65, 23942), STRUCT('Southwest Side', 58, 25449),
    STRUCT('Southwest Side', 63, 7817), STRUCT('Southwest Side', 66, 9025),
    STRUCT('Southwest Side', 59, 36401), STRUCT('Southwest Side', 61, 13867),
    STRUCT('Southwest Side', 67, 42243), STRUCT('Southwest Side', 68, 15479),

    STRUCT('Far Southwest Side', 70, 33221), STRUCT('Far Southwest Side', 71, 40957),
    STRUCT('Far Southwest Side', 72, 18366), STRUCT('Far Southwest Side', 73, 34788),
    STRUCT('Far Southwest Side', 74, 24728), STRUCT('Far Southwest Side', 75, 32594),

    STRUCT('Far Southeast Side', 44, 52464), STRUCT('Far Southeast Side', 45, 25772),
    STRUCT('Far Southeast Side', 47, 21378), STRUCT('Far Southeast Side', 48, 28991),
    STRUCT('Far Southeast Side', 46, 42745), STRUCT('Far Southeast Side', 49, 46468),
    STRUCT('Far Southeast Side', 50, 19781), STRUCT('Far Southeast Side', 51, 26456),
    STRUCT('Far Southeast Side', 52, 19121), STRUCT('Far Southeast Side', 53, 20718),
    STRUCT('Far Southeast Side', 54, 14024), STRUCT('Far Southeast Side', 55, 56099)
  ]) 
)
SELECT 
  p.Side,
  p.`Area Number`,
  INITCAP(c.COMMUNITY) AS `Community Area`,
  p.Population
FROM `chicago_service_requests.community_areas` AS c
JOIN population_data AS p
  ON c.AREA_NUMBE = p.`Area Number`
ORDER BY p.Side, p.`Area Number`;



