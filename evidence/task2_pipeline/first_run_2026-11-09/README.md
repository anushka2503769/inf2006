# Evidence from the EMR run on 9 November 2026

All files here come from one session on one cluster. Every job and application
ID starts with `1794207896977`. Times in the files are UTC (Singapore time
minus 8 hours).

This is the first run, kept for comparison. The scripts have been improved
since (see `scripts/task2_pipeline/hadoop_hive/README.md`, "Changes after review"), and the rerun on 17 Nov
2026 is in `../final_run_2026-11-17/` (see `../README.md`). That rerun captured everything
listed below under "Not captured in this run".

## Files

| File | What it shows |
|---|---|
| `logs/price_audit_20261109T073212Z.log` | Hadoop Streaming price audit, `job_1794207896977_0003`, completed successfully, 07:32:19 to 07:32:51. Counters: 605,910 rows read, 605,887 OK, 23 bad (11 missing value, 12 high/low inconsistent). Input: 28,833,918 bytes (the `M/D/YYYY` copy of the price file). |
| `logs/gdelt_daily_20261109T073212Z.log` | Hadoop Streaming GDELT job, `job_1794207896977_0004`, completed successfully, 07:32:58 to 08:06:45 (33 min 47 s). Counters: 144,207,180 rows read and used, 54,706,262,609 bytes read, 731 map tasks, 731 output rows. |
| `logs/run_streaming.log` | The same two jobs as printed by `run_streaming.sh`, one after the other. |
| `hive_checks.txt` | Output of `hive -f hive/04_checks.hql`, run at 08:14 (application `_0008`). |
| `spark_reads_hive.txt` | See below. |
| `yarn_finished_apps.txt` | Output of `yarn application -list -appStates FINISHED`, run at 08:34: nine applications, all SUCCEEDED. |

## `spark_reads_hive.txt`

The file holds one number, `256`. It was produced by this command on the
primary node at 08:32 UTC on 9 November 2026:

```bash
spark-sql -e "SELECT COUNT(*) FROM bigdata.trading_weeks;" | tee evidence/spark_reads_hive.txt
```

It is the number of rows Spark read from the Hive table `bigdata.trading_weeks`
(152 + 52 + 52 weeks), using Spark application `application_1794207896977_0010`.
Spark counts for the other tables were not taken in this run.

## Which application is which

| Application | What it was |
|---|---|
| `_0001` | S3DistCp: price file from S3 to `/project/raw/daily_price` |
| `_0002` | S3DistCp: GDELT from S3 to `/project/raw/gdelt` |
| `_0003` | Hadoop Streaming `price_audit` |
| `_0004` | Hadoop Streaming `gdelt_daily` |
| `_0006` | Hive `02_gdelt_daily.hql` |
| `_0007` | Hive `03_trading_calendar.hql` |
| `_0008` | Hive `04_checks.hql` (confirmed: the same ID is printed in `hive_checks.txt`) |
| `_0009` | An interactive Hive session used to look at the tables |
| `_0010` | Spark: the count in `spark_reads_hive.txt` |

`_0006` and `_0007` are matched by elimination and by the order the scripts
were run; no log was kept for them. `01_tables.hql` only creates tables, so it
ran no job. `_0005` is not in the FINISHED list and no record of it was kept.

## Not captured in this run

- Logs for Hive scripts 01, 02 and 03 (they were run on screen only).
- The rejected price rows themselves (only their count, 23, and their reasons).
- Spark counts for `raw_prices`, `gdelt_daily` and the other tables.
- An input manifest with file sizes and a checksum.

`run_streaming.sh`, `run_hive.sh` and `run_spark_check.sh` now save all four.

## Change made to these files after the run

The cluster's private hostname and IP address were replaced with `<emr-node>`
and `<private-ip>` using `hadoop_hive/redact_evidence.sh`, because the
submission must not contain machine-specific details. 29 lines changed across
four files. Job IDs, application IDs, counters, row counts and times are
exactly as printed by the cluster.