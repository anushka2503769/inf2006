-- 02_gdelt_daily.hql : turn the daily GDELT sums into averages and shares.
-- Stored as ORC: columnar and compressed, so later Spark reads are smaller and faster.
USE ${hivevar:DB};

DROP TABLE IF EXISTS gdelt_daily;
CREATE TABLE gdelt_daily STORED AS ORC AS
SELECT
  CAST(day AS DATE)                 AS day,          -- date GDELT published the records (DATEADDED)
  events,
  articles,
  tone_sum / events                 AS avg_tone,     -- below 0 = mostly negative news
  goldstein_sum / events            AS avg_goldstein,
  neg_tone_events / events          AS neg_tone_share,
  conflict_events / events          AS conflict_share,
  bad_rows
FROM gdelt_daily_sums
WHERE events > 0;