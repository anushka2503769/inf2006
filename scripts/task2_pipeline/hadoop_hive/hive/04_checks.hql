-- 04_checks.hql : reconciliation and sanity checks.
-- Run it with  bash run_hive.sh , which saves the output under evidence/logs/.
-- Each result starts with the name of the check, so the log can be read on its own.
USE ${hivevar:DB};

-- Row reconciliation: rows in the raw file vs rows counted by the audit job
SELECT 'raw_prices rows' AS check_name, COUNT(*) AS value FROM raw_prices;
SELECT 'price_audit totals' AS check_name, SUM(rows_total) AS audited_rows, SUM(ok_rows) AS ok_rows, SUM(bad_rows) AS bad_rows,
       SUM(duplicate_dates) AS duplicate_dates, SUM(out_of_range_rows) AS out_of_range_rows,
       SUM(missing_rows) AS missing_rows, SUM(ohlc_rows) AS ohlc_rows, COUNT(*) AS tickers
FROM price_audit;

-- Reconciliation of the price file. Every row is OK, BAD or out of range (outside
-- 8 Feb 2013 to 29 Dec 2017, which is out of scope and not an error), so
--   rows_total = ok + bad + out_of_range     (buckets_minus_total must be 0)
--   audited rows = rows Hive reads from the raw file   (audited_minus_raw must be 0)
-- ok_rows is compared with the 605,887 accepted bars in the brief (ok_minus_brief must be 0).
-- Expected for the Excel-saved copy filtered to the window:  605,910 = 605,887 + 23 + 0.
-- Expected for the original file:                            619,040 = 605,887 + 23 + 13,130.
SELECT 'price reconciliation' AS check_name,
       r.raw_rows, a.audited_rows, a.audited_rows - r.raw_rows AS audited_minus_raw,
       a.ok_rows, a.bad_rows, a.out_of_range_rows,
       a.ok_rows + a.bad_rows + a.out_of_range_rows - a.audited_rows AS buckets_minus_total,
       a.ok_rows - 605887 AS ok_minus_brief
FROM (SELECT COUNT(*) AS raw_rows FROM raw_prices) r
CROSS JOIN (SELECT SUM(rows_total) AS audited_rows, SUM(ok_rows) AS ok_rows,
                   SUM(bad_rows) AS bad_rows, SUM(out_of_range_rows) AS out_of_range_rows
            FROM price_audit) a;

-- Rejected rows saved by the price_rejects job must equal the bad rows counted by the audit
-- (the two numbers below must be equal)
SELECT 'rejected rows saved' AS check_name, COUNT(*) AS value FROM price_rejects;
SELECT 'bad rows counted by audit' AS check_name, SUM(bad_rows) AS value FROM price_audit;
SELECT 'rejects by reason' AS check_name, reasons, COUNT(*) AS n
FROM price_rejects GROUP BY reasons ORDER BY n DESC;

-- Header line must not be counted as data (expect 0)
SELECT 'header rows in raw_prices (expect 0)' AS check_name, COUNT(*) AS value
FROM raw_prices WHERE lower(`date`) = 'date';
SELECT 'raw_prices_dated rows / rows without a usable date' AS check_name, COUNT(*) AS rows_total,
       SUM(CASE WHEN trade_date IS NULL THEN 1 ELSE 0 END) AS no_date
FROM raw_prices_dated;

-- Tickers with the most problems
SELECT 'worst tickers' AS check_name, ticker, rows_total, bad_rows, duplicate_dates, first_date, last_date
FROM price_audit ORDER BY bad_rows DESC, ticker LIMIT 10;

-- Calendar per split
SELECT 'weeks per split' AS check_name, split, COUNT(*) AS weeks,
       SUM(CASE WHEN label_known_in_split THEN 1 ELSE 0 END) AS weeks_with_usable_label,
       SUM(CASE WHEN friday_was_holiday THEN 1 ELSE 0 END)   AS friday_holidays,
       MIN(anchor_date) AS first_anchor, MAX(anchor_date) AS last_anchor
FROM trading_weeks GROUP BY split ORDER BY first_anchor;

-- Weeks must follow each other with no gap (expect 0)
SELECT 'non-consecutive weeks (expect 0)' AS check_name, COUNT(*) AS value
FROM trading_weeks WHERE next_week_consecutive = false;

-- GDELT experiment windows (train Jul-Dec 2015, validation 2016, test Jan-Jun 2017)
SELECT 'weeks per gdelt_split' AS check_name, gdelt_split, COUNT(*) AS weeks,
       SUM(CASE WHEN gdelt_label_known_in_split THEN 1 ELSE 0 END) AS weeks_with_usable_label,
       SUM(CASE WHEN gdelt_prior_7d_covered THEN 1 ELSE 0 END)     AS weeks_with_full_prior_7d,
       MIN(anchor_date) AS first_anchor, MAX(anchor_date) AS last_anchor
FROM trading_weeks WHERE gdelt_split IS NOT NULL
GROUP BY gdelt_split ORDER BY first_anchor;

-- GDELT coverage
SELECT 'gdelt_daily coverage' AS check_name, COUNT(*) AS days, MIN(day) AS first_day, MAX(day) AS last_day,
       SUM(events) AS events, SUM(bad_rows) AS bad_rows
FROM gdelt_daily;

-- Every day between the first and last must be present (expect 0 missing)
SELECT 'gdelt missing days (expect 0)' AS check_name,
       datediff(MAX(day), MIN(day)) + 1 - COUNT(*) AS value
FROM gdelt_daily;

-- Rows accounted for by the GDELT job. Compare with the RowsRead counter in the
-- job log (run_hive.sh prints it underneath): events + bad_rows + rows without a
-- usable DATEADDED (Bad_FieldCount + Bad_DateAdded counters) = RowsRead.
SELECT 'gdelt rows in output' AS check_name, SUM(events) AS events, SUM(bad_rows) AS bad_rows,
       SUM(events) + SUM(bad_rows) AS events_plus_bad
FROM gdelt_daily_sums;