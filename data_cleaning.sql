/*

For this project I used BigQuery (free account)
Created 'bellabeat-case-study-07-2023' project, 'fitabeat' dataset and created tables by uploading the csv files that can be found in the 'csv_files_to_import' folder 

*/

-- From minuteIntensitiesNarrow_merged.csv created table minute_intensities 
-- Classified activity type and aggregated the total time grouping by user and date, saved query results as daily_activity_clean table 

SELECT
  id,
  parse_date('%m/%d/%Y', LEFT(activity_minute,9)) AS date,
  SUM(CASE WHEN intensity = 0 THEN 1 ELSE 0 END) AS total_sedentary_time,
  SUM(CASE WHEN intensity = 1 THEN 1 ELSE 0 END) AS total_light_active_time,
  SUM(CASE WHEN intensity = 2 THEN 1 ELSE 0 END) AS total_moderate_active_time,
  SUM(CASE WHEN intensity = 3 THEN 1 ELSE 0 END) AS total_very_active_time,
FROM
  `bellabeat-case-study-07-2023.fitabeat.minute_intensities`
GROUP BY
  id,
  date
ORDER BY
  id,
  date;  

-- From heartrate_seconds_merged.csv created table heartrate_by_sec
-- Grouped heart rate by day (averaging values), formated column with correct date format, saved query results as daily_avg_heart_rate_clean table 

SELECT  
  id,
  parse_date('%m/%d/%Y', LEFT(time,9)) AS date,
  CAST(ROUND(AVG(heart_rate),0) AS INT) AS avg_heart_rate
FROM 
  `bellabeat-case-study-07-2023.fitabeat.heartrate_by_sec` 
GROUP BY
  id,
  date  
ORDER BY 
  id,
  date;  

-- From minuteStepsNarrow_merged.csv created table steps_by_min
-- Grouped steps by day, formated column with correct date format, saved query results as daily_steps_clean table 

SELECT  
  id,
  parse_date('%m/%d/%Y', LEFT(minute,9)) AS date,
  SUM(steps) as steps
FROM 
  `bellabeat-case-study-07-2023.fitabeat.steps_by_min` 
GROUP BY
  id,
  date  
ORDER BY 
  id,
  date; 

-- From sleepDay_merged.csv created table daily_sleep
-- Corrected date format and renamed column, saved query results as daily_sleep_clean

  SELECT  
    id,
    parse_date('%m/%d/%Y', LEFT(date,9)) AS date,
    sleep_records,
    total_min_asleep AS total_time_asleep,
    total_time_in_bed
  FROM
    `bellabeat-case-study-07-2023.fitabeat.daily_sleep`
  ORDER BY
    date,
    id;  

-- From weightLogInfo_merged.csv created table weight_log
-- Grouped records by day, reformated date col, rounding some cols and replacing null values with 0, saved query results as weight_log_clean table 

SELECT  
  id,
  parse_date('%m/%d/%Y', LEFT(date,9)) AS date,
  ROUND(weight_kg, 1) AS weight_kg,
  ROUND(weight_lbs, 1) AS weight_lbs,
  COALESCE(body_fat, 0) as body_fat,
  ROUND(bmi, 1) AS bmi,
  is_manual_report,
  log_id
FROM 
  `bellabeat-case-study-07-2023.fitabeat.weight_log` 
ORDER BY
  id,
  date,
  log_id;

-- Merged all data together for analysis, joining all 5 tables (activity + steps + sleep + heart rate + weight log) and filling null values with 0
-- Saved query results as tables_join table 

SELECT
  id,
  date,
  steps,
  total_sedentary_time,
  total_light_active_time,
  total_moderate_active_time,
  total_very_active_time,
  COALESCE((total_sedentary_time + total_light_active_time +  total_moderate_active_time + total_very_active_time), 0) AS total_time,
  COALESCE(sleep_records,0) AS sleep_records,
  COALESCE(total_time_asleep,0) AS total_time_asleep,
  COALESCE(total_time_in_bed, 0) AS total_time_in_bed,
  COALESCE(avg_heart_rate, 0) AS avg_heart_rate,
  COALESCE(weight_kg, 0) AS weight_kg,
  COALESCE(weight_lbs, 0) AS weight_lbs,
  COALESCE(body_fat, 0) AS body_fat,
  COALESCE(bmi, 0) AS bmi,
  CASE WHEN is_manual_report IS NOT NULL THEN NOT is_manual_report ELSE FALSE
  END AS tracked_with_scale,
  COALESCE(log_id, 0) AS log_id 
FROM
  `bellabeat-case-study-07-2023.fitabeat.daily_steps_clean` 
LEFT JOIN  
  `bellabeat-case-study-07-2023.fitabeat.daily_activity_clean`  
USING
  (id, date) 
LEFT JOIN  
  `bellabeat-case-study-07-2023.fitabeat.daily_sleep_clean` 
USING
  (id, date) 
LEFT JOIN  
  `bellabeat-case-study-07-2023.fitabeat.daily_avg_heart_rate_clean` 
USING
  (id, date)
LEFT JOIN  
  `bellabeat-case-study-07-2023.fitabeat.weight_log_clean` 
USING
  (id, date)    
ORDER BY
date,
id;


-- Checked for duplicate records after joining the tables

SELECT 
  id,
  COUNT(date) AS records,
  COUNT(DISTINCT (date)) as distinct_records,
  (COUNT(date) - COUNT(DISTINCT date)) AS duplicate_records
FROM 
  `bellabeat-case-study-07-2023.fitabeat.tables_join`
GROUP BY
  id
ORDER BY  
  records;

-- Located duplicate records 

SELECT 
  *
FROM  
  `bellabeat-case-study-07-2023.fitabeat.tables_join`
JOIN
  (
    SELECT
      id,
      date,
      COUNT(*)
    FROM
      `bellabeat-case-study-07-2023.fitabeat.tables_join`
    GROUP BY
      id,
      date  
    HAVING COUNT(*) > 1    
  ) AS duplicates
USING
  (id, date)   


-- Created a clean table for all features removing duplicates (can't use delete command with this BigQuery account type) 
-- Saved query results as all_features_clean table
 
WITH CTE AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(
      PARTITION BY
        id,
        date,
        steps,
        total_sedentary_time,
        total_light_active_time,
        total_moderate_active_time,
        total_very_active_time,
        total_time,
        sleep_records,
        total_time_asleep,
        total_time_in_bed,
        avg_heart_rate,
        body_fat,
        tracked_with_scale,
        log_id
      ORDER BY
        date
    ) AS records
  FROM  
    `bellabeat-case-study-07-2023.fitabeat.tables_join`
)  

SELECT  
  id,
  date,
  steps,
  total_sedentary_time,
  total_light_active_time,
  total_moderate_active_time,
  total_very_active_time,
  total_time,
  sleep_records,
  total_time_asleep,
  total_time_in_bed,
  avg_heart_rate,
  weight_kg,
  weight_lbs,
  body_fat,
  bmi,
  tracked_with_scale,
  log_id    
FROM
  CTE
WHERE
  records = 1  
ORDER BY
  id,
  date;

-- Created a table with all the dates cross joining it with the users list, in order to fill the missing dates on all features data
-- Saved query results as all_features_final table.csv, file can be found in the 'tables_created_for_analysis' folder

WITH all_dates AS (
  SELECT 
    date
  FROM 
    UNNEST(
      GENERATE_DATE_ARRAY(
        DATE('2016-04-12'), 
        DATE('2016-05-12'), 
        INTERVAL 1 DAY
      )
    ) AS date
),

users_dates AS (
  SELECT 
    DISTINCT id,
    b.date
  FROM  
    `bellabeat-case-study-07-2023.fitabeat.all_features_clean`
  CROSS JOIN
    all_dates b
)

SELECT
  id,
  date,
  COALESCE(steps, 0) AS steps,
  COALESCE(total_sedentary_time, 0) AS total_sedentary_time,
  COALESCE(total_light_active_time, 0) AS total_light_active_time,
  COALESCE(total_moderate_active_time, 0) AS total_moderate_active_time,
  COALESCE(total_very_active_time, 0) AS total_very_active_time, 
  COALESCE(total_time, 0) AS total_time,
  COALESCE(sleep_records,0) AS sleep_records,
  COALESCE(total_time_asleep,0) AS total_time_asleep,
  COALESCE(total_time_in_bed, 0) AS total_time_in_bed,
  COALESCE(avg_heart_rate, 0) AS avg_heart_rate,
  COALESCE(weight_kg, 0) AS weight_kg,
  COALESCE(weight_lbs, 0) AS weight_lbs,
  COALESCE(body_fat, 0) AS body_fat,
  COALESCE(bmi, 0) AS bmi,
  CASE 
    WHEN tracked_with_scale IS NOT NULL THEN tracked_with_scale 
    ELSE FALSE
  END AS tracked_with_scale,
  COALESCE(log_id, 0) AS log_id
FROM
  users_dates
LEFT JOIN
  `bellabeat-case-study-07-2023.fitabeat.all_features_clean`
USING
  (id,date)
ORDER BY
  id,
  date;
