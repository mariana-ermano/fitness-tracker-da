-- Checked how many users have records for each feature
-- Saved query results as users_tracking_features.csv, file can be found in the 'tables_created_for_analysis' folder

SELECT 
  *
FROM
  (
  SELECT
    COUNT(DISTINCT id) AS users_tracking_steps
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    steps != 0
  )  
CROSS JOIN
  (
  SELECT  
    COUNT(DISTINCT id) AS users_tracking_sleep
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    sleep_records != 0
  )     
CROSS JOIN
  (
  SELECT  
    COUNT(DISTINCT id) AS users_tracking_weight
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    log_id != 0
  ) 
CROSS JOIN
  (
  SELECT  
    COUNT(DISTINCT id) AS users_tracking_heart_rate
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    avg_heart_rate != 0
  )  
CROSS JOIN
  (
  SELECT  
    COUNT(DISTINCT id) AS users_tracking_activity
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    total_time != 0 
  AND 
    total_sedentary_time != 1440
  );

-- Checked how many daily records per user are for each feature
-- Saved query results as daily_records_per_feature.csv, file can be found in the 'tables_created_for_analysis' folder

SELECT
  id,
  days_with_steps,
  COALESCE(days_with_sleep, 0) AS days_with_sleep,
  COALESCE(days_with_weight, 0) AS days_with_weight,
  COALESCE(days_with_heart_rate, 0) AS days_with_heart_rate,
  days_with_activity,
FROM
  (
  SELECT
    id,
    COUNT(DISTINCT date) AS days_with_steps
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    steps != 0
  GROUP BY
    id   
  )  
LEFT JOIN
  (
  SELECT
    id,
    COUNT(DISTINCT date) AS days_with_sleep
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    sleep_records != 0
  GROUP BY
    id 
  )  
USING
  (id)     
LEFT JOIN
  (
  SELECT
    id,
    COUNT(DISTINCT date) AS days_with_weight
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    log_id != 0
  GROUP BY
    id 
  )
USING
  (id)    
LEFT JOIN
  (
  SELECT
    id,
    COUNT(DISTINCT date) AS days_with_heart_rate
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    avg_heart_rate != 0
  GROUP BY
    id 
  ) 
USING
  (id)    
LEFT JOIN
  (
  SELECT
    id,
    COUNT(DISTINCT date) AS days_with_activity
  FROM 
    `bellabeat-case-study-07-2023.fitabeat.all_features_final`
  WHERE 
    total_time != 0 
  AND 
    total_sedentary_time != 1440
  GROUP BY
    id 
  )
USING
  (id)   
ORDER BY
  id;     

-- Analyzed daily average activity per user, classifying the users activity level according to WHO recommendations on physical activity (https://www.who.int/news-room/fact-sheets/detail/physical-activity)
-- Saved query results as activity_level.csv, file can be found in the 'tables_created_for_analysis' folder

SELECT
  id,
  CAST(AVG(total_moderate_active_time) AS INT) AS daily_avg_moderate_activity,
  CAST(AVG(total_very_active_time) AS INT) AS daily_avg_vigorous_activity,
  CASE
    WHEN AVG(total_moderate_active_time) + 2*AVG(total_very_active_time) < 22 THEN 'Sedentary'
    WHEN (AVG(total_moderate_active_time) + 2*AVG(total_very_active_time)) >= 22 AND (AVG(total_moderate_active_time) + 2*AVG(total_very_active_time)) < 42 THEN 'Active'
   ELSE 'Very Active'
  END AS activity_level 
FROM
  `bellabeat-case-study-07-2023.fitabeat.all_features_final`  
WHERE
  total_time != 0 
AND 
  total_sedentary_time != 1440   
GROUP BY
  id;


-- Analyzed the daily time in bed vs time asleep by user, created a function to convert the time from decimals to hh:mm:ss
-- Saved query results as activity_level.csv, file can be found in the 'tables_created_for_analysis' folder

CREATE TEMP FUNCTION time_convert(x FLOAT64)
RETURNS time
AS (
TIME(
    CAST(x/60 AS INT),
    CAST(MOD (CAST(x/60 AS NUMERIC), 1) * 60 AS INT), 
    0)
);

SELECT
  id,
  COUNT(date) AS days_with_data,
  CAST(AVG(total_time_asleep) AS INT) AS avg_time_asleep,
  time_convert(AVG(total_time_asleep)) AS avg_time_asleep_converted,
  CAST(AVG(total_time_in_bed) AS INT) AS avg_time_in_bed,
  time_convert(AVG(total_time_in_bed)) AS avg_time_in_bed_converted,
  CAST(MIN(total_time_asleep) AS INT) AS min_time_asleep,
  time_convert(MIN(total_time_asleep)) AS min_time_asleep_converted,
  CAST(MAX(total_time_asleep) AS INT) AS max_time_asleep,
  time_convert(MAX(total_time_asleep)) AS max_time_asleep_converted,
  CAST(MIN(total_time_in_bed) AS INT) AS min_time_in_bed,
  time_convert(MIN(total_time_in_bed)) AS min_time_in_bed_converted,
  CAST(MAX(total_time_in_bed) AS INT) AS max_time_in_bed,
  time_convert(MAX(total_time_in_bed)) AS max_time_in_bed_converted
FROM
 `bellabeat-case-study-07-2023.fitabeat.all_features_final` 
WHERE
  sleep_records > 0
GROUP BY
  id;    



