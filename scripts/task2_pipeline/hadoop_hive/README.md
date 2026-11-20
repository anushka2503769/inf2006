# Hadoop Streaming and Hive (Task 2)

This part of the pipeline takes the raw data staged in HDFS, checks it, shrinks
the GDELT news data to one row per day, and registers everything as Hive tables
that the PySpark stage reads.

Where the ZIP keeps the files (paths from the ZIP root):

```
scripts/task2_pipeline/hadoop_hive/               this folder: the scripts (run these on the EMR primary node)
evidence/task2_pipeline/final_run_2026-11-17/     the run of 17 Nov 2026: the evidence to mark
evidence/task2_pipeline/first_run_2026-11-09/     the first run, kept for comparison
                                                  (evidence/task2_pipeline/README.md describes both)
```

## Status

| | |
|---|---|
| Last full run on EMR | 17 Nov 2026, on a new cluster with the final scripts (evidence in `evidence/task2_pipeline/final_run_2026-11-17/`). All three Streaming jobs, all four Hive scripts and the Spark check succeeded, and every check matched its expected value. The first run, on 9 Nov with the old scripts, is kept in `evidence/first_run_2026-11-09/`. |
| Price file used | The filtered copy of the price file: 29,552,047 bytes, 605,910 data rows, `YYYY-MM-DD` dates, SHA-256 `7af175996800a2e3c6f61918dbf9813a818078f65c19517f350c2cb17fba75be`. Result: 605,887 rows OK, 23 rejected (11 missing value, 12 high/low inconsistent), none out of range. |
| Changed since 9 Nov | GDELT mapper that adds up inside the mapper, rejected-rows job, extra checks, GDELT experiment columns, logging scripts, the 15 Nov review fixes (see "Changes after review" below), and fixes and additions from the 17 Nov run (see "Changes after the 17 Nov run") |
| Performance | The GDELT job took 2,028 s (33 min 48 s), the same wall-clock time as the 9 Nov mapper (2,027 s), with 3.4 times less CPU time. See "Performance options". Each step was run once. |
| Needed next | Three timed repetitions per configuration, with median and range, and a dev-profile run, for the performance section. See "Still open". |

## What must be in HDFS first

| Path | Must contain |
|---|---|
| `/project/raw/daily_price/` | **Exactly one** price file (`all_stocks_5yr.csv`). Two copies would double every row. It may be any of three copies: the `YYYY-MM-DD` original (29,580,549 bytes, 619,040 rows, runs to Feb 2018); the `M/D/YYYY` Excel-saved copy filtered to the project window (28,833,918 bytes, 605,910 rows, used on 9 Nov); or the filtered copy with `YYYY-MM-DD` dates (29,552,047 bytes, 605,910 rows, used on 17 Nov). All are read correctly; the expected numbers for each are in "Checks and expected results". |
| `/project/raw/gdelt/` | The 731 **unzipped** daily `.CSV` files, in any sub-folders (on 17 Nov: 28 folders, 54,706,262,609 bytes in total). No `.zip` files and no manifests (Hadoop ignores files whose names start with `_` or `.`). |

`run_streaming.sh` checks both before it starts and stops with a message if
either is wrong. It also saves what it found in `evidence/input_manifest_<stamp>.txt`:
the file sizes, the **SHA-256 and row count of the price file**, and which of the
two older copies (the original and the Excel-saved one) the size matches. For
the filtered copy it says the size matches neither; the SHA-256 identifies the
file. To make a run stop when the wrong price file is staged, set the expected
values. This is the command used on 17 Nov:

```bash
EXPECTED_PRICE_SHA256=7af175996800a2e3c6f61918dbf9813a818078f65c19517f350c2cb17fba75be EXPECTED_PRICE_ROWS=605910 bash run_streaming.sh
```

## Run order

Copy the `hadoop_hive` folder (in `scripts/task2_pipeline/` of the ZIP) to the primary node, then:

```bash
cd ~/hadoop_hive
bash run_streaming.sh # 3 Hadoop Streaming jobs
bash run_hive.sh # Hive scripts 01 to 04
bash run_spark_check.sh # Spark reads the Hive tables
```

**Repeated runs.** The brief asks for fresh output locations for every
repetition. Give each repetition its own `RUN_ID` (letters, digits and `_`) and
use the same one for all three scripts:

```bash
RUN_ID=_run1 bash run_streaming.sh
RUN_ID=_run1 bash run_hive.sh
RUN_ID=_run1 bash run_spark_check.sh
```

With `RUN_ID=_run1` the outputs go to `/project/audit/price_audit_run1`,
`/project/rejected/price_audit_run1` and `/project/staging/gdelt_daily_run1`,
and the Hive tables to the database `bigdata_run1`. `run_streaming.sh` stops if
a folder for that `RUN_ID` already exists. With no `RUN_ID` the folders and the
database are the plain names used in the tables below, and a rerun overwrites
them. Run the Hive scripts with `run_hive.sh`; they need the settings it passes
in and do not run with a plain `hive -f`.

Then copy `~/hadoop_hive/evidence` back to your laptop before the cluster is
terminated (HDFS and the Hive tables are deleted with the cluster).

**Saving the outputs and the cluster facts.** Before the cluster ends, also save
the contents of the three Streaming outputs (they are small) and the YARN
listings. Run this on the primary node after the three scripts:

```bash
mkdir -p ~/b_outputs
hdfs dfs -getmerge /project/audit/price_audit    ~/b_outputs/price_audit.tsv
hdfs dfs -getmerge /project/rejected/price_audit ~/b_outputs/price_rejects.tsv
hdfs dfs -getmerge /project/staging/gdelt_daily  ~/b_outputs/gdelt_daily.tsv
rm -f ~/b_outputs/.*.crc     # getmerge writes hidden checksum files; do not keep them
yarn application -list -appStates ALL > evidence/yarn_applications.txt 2>&1
yarn node -list -showDetails          > evidence/yarn_nodes.txt 2>&1
bash redact_evidence.sh
```

The `gdelt_daily` output is the input of Hive script 02, so a saved copy could be
put back into `/project/staging/gdelt_daily` instead of rerunning the 34-minute
job. That restore has not been tried.

| Step | Reads | Writes | Time on 9 Nov | Time on 17 Nov |
|---|---|---|---|---|
| `price_audit` (Streaming) | `/project/raw/daily_price` | `/project/audit/price_audit` | 32 s | 34 s |
| `price_rejects` (Streaming, map-only) | `/project/raw/daily_price` | `/project/rejected/price_audit` | new | 30 s |
| `gdelt_daily` (Streaming) | `/project/raw/gdelt` | `/project/staging/gdelt_daily` | 33 min 47 s with the old mapper | 33 min 48 s (2,028 s) with the default mapper |
| `hive/01_tables.hql` | the folders above | external tables `raw_prices`, `price_audit`, `price_rejects`, `gdelt_daily_sums` | about 1 s | 12 s |
| `hive/02_gdelt_daily.hql` | `gdelt_daily_sums` | table `gdelt_daily` (ORC) | not recorded | 20 s |
| `hive/03_trading_calendar.hql` | `raw_prices` | view `raw_prices_dated`, tables `trading_days`, `trading_weeks` (ORC) | not recorded | 26 s |
| `hive/04_checks.hql` | all tables | check results in the log | about 15 s | 30 s |
| `run_spark_check.sh` | Hive tables | `evidence/spark_reads_hive_<stamp>.txt` | not recorded | not recorded (six tables, 0 differences) |

The raw files are never changed. Raw, audit, rejected and prepared data are in
separate HDFS folders.

## Output tables (Hive database `bigdata`, or `bigdata<RUN_ID>`)

**`raw_prices`** – the price file as staged. All columns are text: `date`,
`open`, `high`, `low`, `close`, `volume`, `name`.

**`raw_prices_dated`** (view) – `raw_prices` plus `trade_date` (DATE), with the
header line removed. **Spark code should read this one**, not `raw_prices`.

**`price_audit`** – one row per ticker.

| Column | Meaning |
|---|---|
| `ticker` | Ticker (`_MALFORMED` groups lines with the wrong number of columns) |
| `rows_total`, `ok_rows`, `bad_rows` | Rows read, rows passing every check inside the project window, rows inside the window failing at least one check. `rows_total = ok_rows + bad_rows + out_of_range_rows` |
| `duplicate_dates` | Rows repeating a date already seen for that ticker |
| `out_of_range_rows` | Rows dated outside 2013-02-08 to 2017-12-29. These are out of scope, not errors: they are counted here only, not in `bad_rows`, and not written to `price_rejects`. The date alone decides, so no other check is applied to them. |
| `missing_rows` | A price or volume is empty |
| `ohlc_rows` | High or low inconsistent with open and close |
| `other_bad_rows` | Any other problem (bad date, not a number, zero or negative price, negative volume) |
| `first_date`, `last_date` | First and last valid date |

**`price_rejects`** – one row per rejected raw line: `ticker`, `trade_date`,
`reasons` (codes separated by commas), `raw_line` (the original line, untouched).
Duplicates are counted in `price_audit` only. Rows outside the project window are not rejects.

**`gdelt_daily_sums`** – one row per day: `day`, `events`, `articles`,
`tone_sum`, `goldstein_sum`, `neg_tone_events`, `conflict_events`, `bad_rows`.

**`gdelt_daily`** – one row per day, market-wide (GDELT has no ticker).

| Column | Meaning |
|---|---|
| `day` | Date GDELT **published** the records (`DATEADDED`), not the date the event happened. This is when the information became available, so it cannot leak into earlier days. |
| `events`, `articles` | Number of events and of articles mentioning them |
| `avg_tone` | Average tone (below 0 = mostly negative news) |
| `avg_goldstein` | Average Goldstein score |
| `neg_tone_share` | Share of events with negative tone |
| `conflict_share` | Share of events in QuadClass 4 (material conflict) |
| `bad_rows` | Rows with a valid date but unreadable numbers |

**`trading_days`** – `trade_date`, `n_series` (days on which at least 250 of the
505 series traded).

**`trading_weeks`** – one row per week. Weeks end on Friday.

| Column | Meaning |
|---|---|
| `week_friday` | The Friday that ends the week |
| `anchor_date` | Last trading day on or before that Friday (the Thursday when Friday is a market holiday) |
| `next_anchor_date` | `anchor_date` of the following week |
| `trading_days_in_week` | Trading days in the week |
| `friday_was_holiday` | True when `anchor_date` is not the Friday |
| `next_week_consecutive` | True when the next row is exactly one week later |
| `split` | Price model: `train` (2013–2015), `validation` (2016), `test` (2017), by `anchor_date` |
| `label_known_in_split` | False when the outcome falls outside the same split (the last week of each split) |
| `gdelt_split` | GDELT experiment: `train` (Jul–Dec 2015), `validation` (2016), `test` (Jan–Jun 2017); empty outside those windows |
| `gdelt_label_known_in_split` | Same rule as `label_known_in_split`, for the GDELT windows |
| `gdelt_prior_7d_covered` | True when all 7 days before `anchor_date` are inside GDELT's coverage (false for the first week of July 2015) |

Label: the open on `next_anchor_date` compared with the open on `anchor_date`.
Features must only use data from before `anchor_date`.

`gdelt_prior_7d_covered` is true only for anchors from 8 Jul 2015 to 30 Jun 2017,
the weeks whose 7 calendar days before `anchor_date` all lie inside GDELT's
coverage (1 Jul 2015 to 30 Jun 2017). It is false for every week outside the
GDELT experiment, and for the first week of the train window (anchor 2 Jul 2015),
which is why that window shows 26 such weeks out of 27. A "news in the last
7 days" feature is complete only where this flag is true, so that first week
must be dropped or given a shorter news window.

## Checks and expected results

`hive/04_checks.hql` prints each check with its name. `run_hive.sh` saves the
output in `evidence/logs/hive_04_checks_<stamp>.log`.

The price numbers depend on which price file is staged. Every copy gives the
same 605,887 accepted rows and 23 bad rows, and they differ only in the rows
after 29 Dec 2017:

| Price file | Rows in `raw_prices` = rows audited | OK | Bad | Out of range |
|---|---|---|---|---|
| Excel-saved copy filtered to the window (28,833,918 bytes) | 605,910 | 605,887 | 23 | 0 |
| Original (29,580,549 bytes) | 619,040 | 605,887 | 23 | 13,130 |
| Filtered copy, `YYYY-MM-DD` dates (29,552,047 bytes; used on 17 Nov) | 605,910 | 605,887 | 23 | 0 |

| Check | Expected | Seen on EMR, 17 Nov |
|---|---|---|
| Rows in `raw_prices` | 605,910 or 619,040, see the table above | 605,910 |
| Rows audited / OK / bad / out of range | 605,910 / 605,887 / 23 / 0, or 619,040 / 605,887 / 23 / 13,130 | 605,910 / 605,887 / 23 / 0 |
| Price reconciliation: audited rows minus raw rows, OK + bad + out of range minus audited rows, OK rows minus 605,887 (brief) | 0, 0, 0 | 0, 0, 0 |
| Bad rows by reason | 11 missing value, 12 high/low inconsistent | 11 and 12 |
| Duplicates | 0 | 0 |
| Tickers | 505 | 505 |
| Rejected rows saved = bad rows counted | 23 = 23 | 23 = 23 |
| Header rows counted as data; rows without a usable date | 0; 0 | 0; 0 |
| Weeks per split (train / validation / test) | 152 / 52 / 52 | 152 / 52 / 52 |
| Weeks with a usable label | 151 / 51 / 51 | 151 / 51 / 51 |
| Friday holidays | 7 / 1 / 1 | 7 / 1 / 1 |
| Non-consecutive weeks | 0 | 0 |
| Weeks per `gdelt_split` | 27 / 52 / 26 | 27 / 52 / 26 |
| GDELT weeks with a usable label | 26 / 51 / 25 | 26 / 51 / 25 |
| GDELT weeks with a full prior 7 days | 26 / 52 / 26 | 26 / 52 / 26 |
| GDELT days, events, bad rows | 731, 144,207,180, 0 | 731, 144,207,180, 0 |
| GDELT missing days | 0 | 0 |
| GDELT `RowsRead` counter = events + bad rows in the output | 144,207,180 | 144,207,180 = 144,207,180 + 0 |
| Spark count equals Hive count for `trading_weeks`, `raw_prices`, `raw_prices_dated`, `gdelt_daily`, `price_audit`, `price_rejects` (`raw_prices` may be one higher if Spark counts the header) | Equal; reference values 256, 731 and 505 for `trading_weeks`, `gdelt_daily` and `price_audit` | All six equal (`raw_prices` 605,911 against 605,910 because of the header); the three reference values matched |

Expected values for the calendar and GDELT checks came from a simulation of the
calendar rules on the 2013-2017 US market calendar, made before the 17 Nov run.
EMR gave the same value for every check.

## Evidence written by the scripts

| File | Shows |
|---|---|
| `evidence/input_manifest_<stamp>.txt` | The files in HDFS when the jobs started: sizes, count, price file SHA-256 and row count, which known copy the size matches |
| `evidence/logs/<job><RUN_ID>_<stamp>.log` | Full output of each Streaming job: job ID, counters, success line |
| `evidence/streaming_summary_<stamp>.txt` | Job IDs, seconds per job, key counters, and for each output its rows, bytes and the SHA-256 of its sorted lines; HDFS space after the run |
| `evidence/logs/hive_<script>_<stamp>.log` | Output of each Hive script, with the YARN application ID it used |
| `evidence/hive_summary_<stamp>.txt` | Seconds and application ID for each Hive script |
| `evidence/spark_reads_hive_<stamp>.txt` | Date, exact command, Spark count and Hive count for each table, with the reference value where one exists |

On the cluster these files are written under `~/hadoop_hive/evidence/`. The
copies from the 17 Nov run are in `evidence/task2_pipeline/final_run_2026-11-17/` of the ZIP
(`evidence/task2_pipeline/README.md` explains each file and which YARN application was which).

The scripts replace the cluster's private hostname and IP address in these
files with `<emr-node>` and `<private-ip>` (`redact_evidence.sh`). It runs
when each script ends, also when a job fails, and prints a warning if a
hostname or IP address is still left. Job IDs, application IDs, counters and
times are left exactly as printed.

## Performance options

```bash
bash run_streaming.sh                               # default: adds up inside the mapper
GDELT_MAPPER=rowwise bash run_streaming.sh          # baseline: the 9 Nov mapper, with a combiner
GDELT_COMBINE_SPLITS_MB=256 bash run_streaming.sh   # optional: several daily files per map task
```

- **Baseline (`rowwise`)**: prints one line and two counter updates for each of
  the 144 million events, and a combiner adds them up. Measured on EMR on
  9 Nov: 33 min 47 s; mapper output 5.5 GB, cut to 86 KB by the combiner.
- **Default (`inmapper`)**: adds up inside the mapper and prints one line per
  day. Measured on EMR on 17 Nov: 2,028 s (33 min 48 s), **the same wall-clock
  time as the baseline (2,027 s)**, with 3.4 times less CPU time (1,378,140 ms
  against 4,673,790 ms) and slightly less total map time (about 8.24 million ms
  against 8.84 million ms). On a laptop test of 420,000 rows it gave identical
  output, ran the mapper about 2.7 times faster, and cut mapper output from
  16 MB to 395 bytes. That did not shorten the job on EMR, so do not report a
  speedup in wall-clock time.
- **Combine splits**: the job starts one map task per daily file (731 tasks).
  This option packs several files into each task. It has **not been run on
  EMR**; check that the output is identical before using its timing.
- **What limits the job.** Each daily file is one map task of about 11 s. On
  average about 4 ran at the same time, although the two core nodes (12,288 MB
  and 4 vCores each, 3,072 MB per map container) have room for up to 8. Peak map
  memory was about 570 MB, so smaller containers are a candidate
  configuration. Neither idea has been tried, and the reason for the average of
  4 is not known.

Both mappers give the same output through the same reducer, so the two can be
compared on identical inputs for the performance section.

## Test on a laptop

```bash
cd hadoop_hive
python run_local.py price          sample/prices_sample.csv
python run_local.py rejects        sample/prices_sample.csv
python run_local.py gdelt          sample/gdelt_sample.CSV
python run_local.py gdelt-rowwise  sample/gdelt_sample.CSV
```

The sample files contain deliberately broken rows and two rows outside the
project window. The price jobs end with a line `RowsRead = OK + Bad + OutOfScope`
that must read `OK`. `gdelt` and `gdelt-rowwise` must print the same rows.

## Changes after review (14 Nov 2026)

| Review point | What changed |
|---|---|
| GDELT job took over half the 60-minute limit | New default mapper adds up per day inside the mapper; the old one is kept as `gdelt_daily_mapper_rowwise.py` for comparison |
| Logs contain the private hostname and IP | `redact_evidence.sh`, run automatically; also applied to the 9 Nov evidence |
| No README | This file |
| No logs for Hive scripts 01 to 03 | `run_hive.sh` logs every script |
| `spark_reads_hive.txt` had no context | `run_spark_check.sh` records date, command and expected count for six tables; context for the 9 Nov file is in `evidence/task2_pipeline/first_run_2026-11-09/README.md` |
| Rejected rows counted but not saved | New map-only job `price_rejects` and table `price_rejects` |
| Extra checks | Non-consecutive weeks, rejects reconciliation, header rows, GDELT missing days, GDELT rows against the job counter |
| GDELT experiment window not flagged | New columns `gdelt_split`, `gdelt_label_known_in_split`, `gdelt_prior_7d_covered` |
| Price folder must hold one file; GDELT must be unzipped CSVs only | Checked by `run_streaming.sh` before any job starts |

## Changes after review (15 Nov 2026)

| Review point | What changed |
|---|---|
| The staged price file now has `YYYY-MM-DD` dates, and its rows after 29 Dec 2017 were counted as bad rows | Rows outside the window are a separate status, `OOS`, counted in `out_of_range_rows` and the counter `RowsOutOfScope`, not in `bad_rows` and not in `price_rejects`. Both date formats are read; a byte-order mark before the header is ignored. On data without out-of-window rows the output is identical to before. |
| Nothing recorded which price file was used | The manifest holds the SHA-256, row count and known-copy label; `EXPECTED_PRICE_SHA256` and `EXPECTED_PRICE_ROWS` stop the run on a mismatch |
| Repetitions would overwrite each other | `RUN_ID` gives each run its own output folders and Hive database; a reused `RUN_ID` stops the run |
| Hive tables from an earlier run could keep pointing at old folders | Every external table is dropped before it is created, and the folders are passed in as settings |
| Expected counts were fixed numbers | `run_spark_check.sh` compares Spark with Hive on each table, and accepts the header-counted case for `raw_prices` |
| No price reconciliation | New check in `04_checks.hql`: audited rows against raw rows, and OK + bad + out of range against the total |
| Hostnames could stay in the evidence after a failed run | The redaction runs when each script ends; it also handles `ip-…internal` and `ec2-…amazonaws.com` names and warns if anything is left |
| A GDELT row with tone `nan` or `inf` would have produced a `nan` sum | The default mapper counts it as `Bad_Number` (the baseline mapper is unchanged) |

## Changes after the 17 Nov run

| What happened | What changed |
|---|---|
| Hive script 03 failed with a ParseException | An apostrophe in an indented comment stopped the Hive command line from seeing the end of a statement. Two comment lines were reworded. The failed log and summary are kept in `evidence/task2_pipeline/final_run_2026-11-17/` (stamp `20261117T033426Z`) as the record of the failure; the rerun passed |
| The streaming summary held counters only | `run_streaming.sh` now also records, for each output, its rows, bytes and the SHA-256 of its sorted lines, and the HDFS space after the run. Tested on EMR |
| The mapper was described as faster | Measured on EMR: same wall-clock time, 3.4 times less CPU (see "Performance options") |
| The contents of the outputs disappeared with the cluster | The three outputs are saved with `hdfs dfs -getmerge` (see "Run order") and kept in `evidence/task2_pipeline/final_run_2026-11-17/outputs/` |
| The staged price file was neither of the two known copies | Added to "What must be in HDFS first" and to "Checks and expected results" |

## Still open

1. **Zips or unzipped CSVs as the timed input** (teaching team's answer). The
   GDELT job cannot read `.zip` files. If the zips must be the timed input, an
   unzip stage has to be added before `gdelt_daily`. The input on 17 Nov was the
   731 unzipped `.CSV` files.
2. **Who writes rejected rows for the cleaned data.** `price_rejects` holds the
   rows the audit rejects (23 on 17 Nov, saved in
   `evidence/task2_pipeline/final_run_2026-11-17/outputs/price_rejects.tsv`). The PySpark cleaning stage
   should either reuse it or write its own and reconcile to the same 23 rows.
   The team should also confirm that these 23 lines of the raw file may stay in
   the repository.
3. **Repetitions for the performance section.** Each step ran once on 17 Nov.
   The brief asks for three repetitions per configuration, with median and
   range, and a dev-profile run. The GDELT job takes about 34 minutes, so three
   repetitions need about 1 hour 42 minutes of cluster time for that job alone.
   This belongs to the performance section (Report Section 6).
4. **Price file record.** The staged file is the filtered copy of the price file
   (SHA-256 and row count above). The original file (29,580,549 bytes, 619,040
   rows) was overwritten in S3 by the price filter script before the run, so its
   SHA-256 has to be recorded in the corpus record and a copy kept. The corpus
   step has to state which file it staged.

## Troubleshooting

- `$'\r': command not found` when running a `.sh` file: the file has Windows
  line endings. Fix with `sed -i 's/\r$//' *.sh hive/*.hql streaming/*.py`.
- `STOP: ... must hold exactly one price file`: remove the extra copy from
  `/project/raw/daily_price/`.
- Hive tables missing on a new cluster: HDFS and Hive are empty on every new
  cluster. Stage the data, then run the three scripts again.
- `FAILED: ParseException ... cannot recognize input near ... ';' '<EOF>'` in a
  Hive script: an apostrophe or a quote inside an indented `--` comment. The Hive
  command line only skips comment lines that start in the first column, and it
  counts quotes when it splits statements, so one stray quote hides the final
  `;`. Keep apostrophes and quotes out of comments, or start the comment in the
  first column.
- `hdfs dfs -getmerge` leaves hidden `.crc` files next to its output. Delete
  them before copying the outputs anywhere.