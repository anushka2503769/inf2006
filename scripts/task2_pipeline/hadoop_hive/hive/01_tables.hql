-- 01_tables.hql : register the raw data and the Hadoop Streaming outputs as Hive tables.
-- Run it with  bash run_hive.sh , which passes the database name and the HDFS folders
-- as hivevar settings (DB, PRICE_DIR, AUDIT_DIR, REJECT_DIR, GDELT_SUMS_DIR). The
-- scripts do not run without them. A repeated run uses a new RUN_ID, which gives a
-- new database (bigdata_run2, ...) and the output folders of that run.
-- EXTERNAL tables only point at the HDFS folders; dropping them never deletes the data.
-- Each table is dropped first, so a table left from an earlier run cannot keep
-- pointing at an old folder.

CREATE DATABASE IF NOT EXISTS ${hivevar:DB};
USE ${hivevar:DB};

-- Raw daily prices, exactly as staged by the corpus step. Every column is STRING,
-- so nothing is converted or lost: cleaning and typing happen later in PySpark.
DROP TABLE IF EXISTS raw_prices;
CREATE EXTERNAL TABLE raw_prices (
  `date` STRING, open STRING, high STRING, low STRING,
  close STRING, volume STRING, name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:PRICE_DIR}'
TBLPROPERTIES ('skip.header.line.count'='1');
-- Hive skips the header line because of the table property above. Some Spark
-- versions ignore that property and return the header as a data row, so Spark
-- code should read the view raw_prices_dated (03_trading_calendar.hql), which
-- removes the header itself. run_spark_check.sh records what Spark returns.

-- Per-ticker audit summary written by price_audit_mapper/reducer.
DROP TABLE IF EXISTS price_audit;
CREATE EXTERNAL TABLE price_audit (
  ticker STRING, rows_total INT, ok_rows INT, bad_rows INT, duplicate_dates INT,
  out_of_range_rows INT, missing_rows INT, ohlc_rows INT, other_bad_rows INT,
  first_date STRING, last_date STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:AUDIT_DIR}';

-- Rejected price rows written by the map-only job price_rejects
-- (price_audit_mapper.py with AUDIT_MODE=rejects): one row per bad raw line,
-- with the reason codes and the untouched original line.
DROP TABLE IF EXISTS price_rejects;
CREATE EXTERNAL TABLE price_rejects (
  ticker STRING, trade_date STRING, reasons STRING, raw_line STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:REJECT_DIR}';

-- Daily market-wide GDELT sums written by gdelt_daily_mapper/reducer.
DROP TABLE IF EXISTS gdelt_daily_sums;
CREATE EXTERNAL TABLE gdelt_daily_sums (
  day STRING, events BIGINT, articles BIGINT, tone_sum DOUBLE, goldstein_sum DOUBLE,
  neg_tone_events BIGINT, conflict_events BIGINT, bad_rows BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:GDELT_SUMS_DIR}';