-- Create csr_processed table in BigQuery
CREATE OR REPLACE TABLE `chicago_service_requests.csr_processed_recent` AS
WITH csr_open AS (
  SELECT
    `SR Number`,
    `SR Type`,
    'Open' AS `Status`,
    `Created Date`,
    `Completion Time in Days`,
    `SR Category`,
    `SR Sub-Category`,
    `Street Address`,
    `Area Number`,
    `Latitude`,
    `Longitude`,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(`Latitude` AS STRING), CAST(`Longitude` AS STRING), `Created Date`, `SR Category`, `SR Sub-Category`, `SR Type`
    ) AS rn
  FROM `chicago_service_requests.csr_staged`
  WHERE `Completion Time in Days` IS NULL
),
csr_not_open AS (
  SELECT
    `SR Number`,
    `SR Type`,
    'Completed' AS `Status`,
    `Created Date`,
    `Completion Time in Days`,
    `SR Category`,
    `SR Sub-Category`,
    `Street Address`,
    `Area Number`,
    `Latitude`,
    `Longitude`,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(`Latitude` AS STRING), CAST(`Longitude` AS STRING), `Created Date`, `SR Category`, `SR Sub-Category`, `SR Type`
      ORDER BY `Completion Time in Days` DESC
    ) AS rn
  FROM `chicago_service_requests.csr_staged`
  WHERE `Completion Time in Days` IS NOT NULL
),
csr_all AS (
  SELECT 
    `SR Number`, `SR Type`, `Status`, `Created Date`, `Completion Time in Days`,
    `SR Category`, `SR Sub-Category`, `Street Address`, `Area Number`, `Latitude`, `Longitude`
  FROM csr_open
  WHERE rn = 1
  UNION ALL
  SELECT 
    `SR Number`, `SR Type`, `Status`, `Created Date`, `Completion Time in Days`,
    `SR Category`, `SR Sub-Category`, `Street Address`, `Area Number`, `Latitude`, `Longitude`
  FROM csr_not_open
  WHERE rn = 1
)
SELECT
  `SR Number`,
  `SR Type`,
  `Status`,
  `Created Date`,
  `Completion Time in Days`,
  DATE_ADD(`Created Date`, INTERVAL CAST(ROUND(`Completion Time in Days`) AS INT64) DAY) AS `Closed Date`,
  CASE 
    WHEN `Status` = 'Open' THEN DATE_DIFF(CURRENT_DATE(), `Created Date`, DAY)
    ELSE NULL
  END AS `Open SR Time in Days`,
  `SR Category`,
  `SR Sub-Category`,
  `Street Address`,
  `Area Number`,
  `Latitude`,
  `Longitude`
FROM csr_all
ORDER BY `Created Date`;

-- Append new processed data to existing
CREATE OR REPLACE TABLE `chicago_service_requests.csr_processed` AS (
SELECT `SR Number`, `SR Type`, `Status`, `Created Date`, `Completion Time in Days`, `Closed Date`, `Open SR Time in Days`,
  `SR Category`, `SR Sub-Category`, `Street Address`, `Area Number`, `Latitude`, `Longitude`
FROM (
    SELECT * FROM `chicago_service_requests.csr_processed`
    WHERE `Created Date` < (SELECT MIN(`Created Date`) FROM `chicago_service_requests.csr_processed_recent`)
    UNION ALL
    SELECT * FROM `chicago_service_requests.csr_processed_recent` )
ORDER BY `Created Date`
)

