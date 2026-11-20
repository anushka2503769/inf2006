# Evidence for Task 2 (Hadoop Streaming and Hive)

| Folder | Run |
|---|---|
| `final_run_2026-11-17/` | **The evidence to mark.** The run of 17 Nov 2026 on a new EMR cluster with the final scripts. |
| `first_run_2026-11-09/` | The first run, with the old scripts and a different price file. Kept for comparison; it has its own README. |

## The run of 17 Nov 2026

Everything in `final_run_2026-11-17/` comes from one cluster. Every job and application ID
starts with `1794881266970`. Times in the files are UTC (Singapore time minus
8 hours).

**Result.** All three Hadoop Streaming jobs, all four Hive scripts (on the
second attempt) and the Spark check succeeded, and every check matched its
expected value (see `scripts/task2_pipeline/hadoop_hive/README.md`, "Checks and expected results").

**Setup.** AWS EMR emr-7.10.0 (Hadoop 3.4.1, Hive 3.1.3, Spark 3.5.5), one
primary node and two core nodes (m5.xlarge; each core node offers 12,288 MB and
4 vCores to YARN). The inputs were copied from S3 to HDFS with S3DistCp:

- Price file: the filtered copy of the price file, 29,552,047 bytes, 605,910 data rows,
  SHA-256 `7af175996800a2e3c6f61918dbf9813a818078f65c19517f350c2cb17fba75be`.
- GDELT: 731 unzipped daily `.CSV` files in 28 folders, 54,706,262,609 bytes.

**Run stamps.** Streaming `20261117T025210Z`. Hive, first attempt
`20261117T033426Z` (script 03 failed), second attempt `20261117T034432Z`.
Spark check `20261117T035418Z`.

### Files in `final_run_2026-11-17/`

| File | What it shows |
|---|---|
| `input_manifest_20261117T025210Z.txt` | The files in HDFS when the jobs started: the price file (size, SHA-256, 605,910 data rows) and the 731 GDELT files with their sizes |
| `streaming_summary_20261117T025210Z.txt` | Job IDs, seconds per job, key counters, and for each output its rows, bytes and the SHA-256 of its sorted lines; HDFS space after the run |
| `logs/price_audit_…`, `logs/price_rejects_…`, `logs/gdelt_daily_…` | Full output of each Streaming job: job ID, counters, success line |
| `hive_summary_20261117T033426Z.txt` and `logs/hive_01…03_20261117T033426Z.log` | Hive, **first attempt**: script 03 FAILED with a ParseException (see below). Kept on purpose |
| `hive_summary_20261117T034432Z.txt` and `logs/hive_01…04_20261117T034432Z.log` | Hive, **second attempt**: all four scripts SUCCEEDED. `hive_04_checks_….log` holds the 16 checks |
| `spark_reads_hive_20261117T035418Z.txt` and `logs/spark_check_…log` | Spark count and Hive count for six tables: 0 differences |
| `yarn_applications.txt` | `yarn application -list -appStates ALL`: 24 applications |
| `yarn_nodes.txt` | `yarn node -list -showDetails`: two core nodes, 12,288 MB and 4 vCores each |
| `outputs/price_audit.tsv` (505 rows), `outputs/price_rejects.tsv` (23 rows), `outputs/gdelt_daily.tsv` (731 rows) | The contents of the three Streaming outputs, merged with `hdfs dfs -getmerge`. The SHA-256 of the sorted lines of each file equals the value in `streaming_summary_…` |

`price_audit.tsv` has the columns of the Hive table `price_audit`, `price_rejects.tsv`
those of `price_rejects`, and `gdelt_daily.tsv` those of `gdelt_daily_sums` (see
`scripts/task2_pipeline/hadoop_hive/README.md`). The 23 rows in `price_rejects.tsv` are lines of the raw price
file, kept as the record of what was rejected.

### The failed Hive attempt

The first Hive attempt stopped at script 03 with
`FAILED: ParseException line 57:11 cannot recognize input near 'linked' ';' '<EOF>' in joinSource`.
The cause was an apostrophe in an indented comment. The Hive command line only
skips comment lines that start in the first column, and it counts quotes when it
splits statements, so the final `;` was never seen. Before the error the script
had built the view `raw_prices_dated` and the table `trading_days` (application
`_0008`). Two comment lines were reworded and the script was run again from the
start (stamp `20261117T034432Z`); everything passed. Both attempts are kept.

### Which application is which

All IDs start with `application_1794881266970_`.

| Application | What it was |
|---|---|
| `0001` | S3DistCp: price file from S3 to `/project/raw/daily_price` |
| `0002` | S3DistCp: GDELT from S3 to `/project/raw/gdelt` |
| `0003`, `0004`, `0005` | Hadoop Streaming `price_audit`, `price_rejects`, `gdelt_daily` |
| `0007` | Hive `02_gdelt_daily.hql`, first attempt (named in `hive_summary_20261117T033426Z.txt`) |
| `0008` | Hive `03_trading_calendar.hql`, first attempt: built the view and `trading_days`, then stopped at the parse error. The script is marked FAILED; the YARN application itself finished SUCCEEDED |
| `0006`, `0009` | Hive/Tez applications that appear in no log or summary. Both are KILLED; the cause is not known |
| `0010`, `0011`, `0012` | Hive scripts 02, 03 and 04, second attempt |
| `0013`, `0015`, `0017`, `0019`, `0021`, `0023` | Spark counts for `trading_weeks`, `raw_prices`, `raw_prices_dated`, `gdelt_daily`, `price_audit`, `price_rejects` (listed in `spark_reads_hive_20261117T035418Z.txt`) |
| `0014`, `0016`, `0018`, `0020`, `0022`, `0024` | Hive counts taken by the same check, matched by order. `0014` and `0020` are KILLED |

Four Hive/Tez applications show KILLED (`0006`, `0009`, `0014`, `0020`). None of
them is a failed job: for `0014` and `0020` (the counts of the ORC tables
`trading_weeks` and `gdelt_daily`) the Spark check still received the correct
counts. Hive most likely answered those two from table statistics and closed
its Tez session without running a job. That is an inference from the pattern,
not something the logs state.

### Not captured in this run

- A SHA-256 for each GDELT file (the manifest has sizes only).
- The SHA-256 of the original price file. The price filter script overwrote it in
  S3 before this run, so it has to come from the Task 1 corpus record.
- Repetitions. Each step ran once, so there is no median or range.
- A timing for the Spark check.

### Changes made to these files after the run

`redact_evidence.sh` replaced the cluster's private host names and IP addresses
with `<emr-node>` and `<private-ip>` in every file. Job IDs, application IDs,
counters, row counts and times are as printed. A final search of the folder
found no private names, public IP addresses, access keys or personal paths.
`yarn_applications.txt` and `yarn_nodes.txt` were saved by hand and redacted the
same way. The hidden `.crc` checksum files that `getmerge` writes next to each
output were deleted.