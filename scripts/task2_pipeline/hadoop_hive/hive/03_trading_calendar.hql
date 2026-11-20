-- 03_trading_calendar.hql : build the trading-week calendar used for the label
-- "is next Friday's open higher than this Friday's open?" and for the splits.
USE ${hivevar:DB};

-- 0. The price file may write dates as M/D/YYYY (e.g. 2/8/2013) or as
--    YYYY-MM-DD, depending on which copy was staged. This view adds a proper
--    DATE column for either format; the raw table itself is untouched.
--    It also drops the header line itself, so it is safe to read from Spark
--    even if Spark ignores the table's skip-header setting.
CREATE OR REPLACE VIEW raw_prices_dated AS
SELECT r.*,
       CASE
         WHEN `date` RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
           THEN to_date(from_unixtime(unix_timestamp(`date`, 'M/d/yyyy')))
         WHEN `date` RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
           THEN to_date(`date`)
       END AS trade_date
FROM raw_prices r
WHERE `date` IS NULL OR lower(`date`) <> 'date';

-- 1. Market trading days: dates where at least 250 of the 505 series have a row.
--    Uses only well-formed dates inside the project window.
DROP TABLE IF EXISTS trading_days;
CREATE TABLE trading_days STORED AS ORC AS
SELECT trade_date, COUNT(DISTINCT name) AS n_series
FROM raw_prices_dated
WHERE trade_date IS NOT NULL
  AND trade_date BETWEEN DATE '2013-02-08' AND DATE '2017-12-29'
GROUP BY trade_date
HAVING COUNT(DISTINCT name) >= 250;

-- 2. One row per week (weeks end on Friday; 1970-01-02 was a Friday).
--    anchor_date = last trading day on or before Friday, so when a Friday is a
--    market holiday (e.g. Good Friday) the Thursday is used instead.
--    The label compares the open on next_anchor_date with the open on anchor_date.
--    Features must use only data up to the trading day BEFORE anchor_date.
DROP TABLE IF EXISTS trading_weeks;
CREATE TABLE trading_weeks STORED AS ORC AS
WITH tagged AS (
  SELECT trade_date,
         date_add('1970-01-02',
                  CAST(CEIL(datediff(trade_date, '1970-01-02') / 7.0) AS INT) * 7) AS week_friday
  FROM trading_days
),
weeks AS (
  SELECT week_friday, MAX(trade_date) AS anchor_date, COUNT(*) AS trading_days_in_week
  FROM tagged
  GROUP BY week_friday
),
linked AS (
  SELECT week_friday, anchor_date, trading_days_in_week,
         LEAD(week_friday) OVER (ORDER BY week_friday) AS next_week_friday,
         LEAD(anchor_date) OVER (ORDER BY week_friday) AS next_anchor_date
  FROM weeks
)
SELECT
  week_friday,
  anchor_date,
  next_anchor_date,
  trading_days_in_week,
  anchor_date <> week_friday                       AS friday_was_holiday,
  datediff(next_week_friday, week_friday) = 7      AS next_week_consecutive,
  CASE WHEN year(anchor_date) <= 2015 THEN 'train'
       WHEN year(anchor_date) = 2016  THEN 'validation'
       ELSE 'test' END                             AS split,
  -- leakage control: a label is usable only if its outcome is known
  -- inside the same split period (e.g. last week of 2015 -> outcome in 2016 -> not usable for training)
  CASE WHEN next_anchor_date IS NULL THEN false
       WHEN year(anchor_date) <= 2015 THEN year(next_anchor_date) <= 2015
       ELSE year(next_anchor_date) = year(anchor_date) END AS label_known_in_split,

  -- GDELT experiment. GDELT only covers 2015-07-01 to 2017-06-30, so the
  -- experiment that adds news features uses shorter windows than the price
  -- model: train Jul-Dec 2015, validation 2016, test Jan-Jun 2017.
  -- NULL means the week is outside the GDELT experiment.
  CASE WHEN anchor_date BETWEEN DATE '2015-07-01' AND DATE '2015-12-31' THEN 'train'
       WHEN anchor_date BETWEEN DATE '2016-01-01' AND DATE '2016-12-31' THEN 'validation'
       WHEN anchor_date BETWEEN DATE '2017-01-01' AND DATE '2017-06-30' THEN 'test'
       END                                         AS gdelt_split,
  -- same leakage rule as above: the outcome must fall inside the same window
  CASE WHEN next_anchor_date IS NULL THEN false
       WHEN anchor_date BETWEEN DATE '2015-07-01' AND DATE '2015-12-31'
         THEN next_anchor_date <= DATE '2015-12-31'
       WHEN anchor_date BETWEEN DATE '2016-01-01' AND DATE '2016-12-31'
         THEN next_anchor_date <= DATE '2016-12-31'
       WHEN anchor_date BETWEEN DATE '2017-01-01' AND DATE '2017-06-30'
         THEN next_anchor_date <= DATE '2017-06-30'
       ELSE false END                              AS gdelt_label_known_in_split,
  -- true when all 7 days before anchor_date are inside the GDELT coverage, so a
  -- news in the last 7 days feature is complete (false for the first week of Jul 2015)
  (anchor_date >= DATE '2015-07-08' AND anchor_date <= DATE '2017-07-01')
                                                   AS gdelt_prior_7d_covered
FROM linked;