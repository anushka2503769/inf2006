# Task 4: Analytics and visualisation

Answers three questions with Hive tables and PySpark, keeps the full data distributed, and only brings small
aggregates to the driver for tables and charts. The questions follow the Task 4 brief:

| Script | Question | Main output tables (`analytics_output/tables/`) |
|---|---|---|
| `q1_volume_analysis.py` | Does unusual trading volume, measured against the stock's own history, go with larger or different price moves? | `q1_bucket_summary`, `q1_weekly_spread`, `q1_weekly_spread_by_week`, `q1_sameday_abs_change` |
| `q2_gdelt_news_analysis.py` | Do weeks with more or more negative GDELT news coincide with different market volatility? | `q2_group_summary`, `q2_high_vs_low`, `q2_spearman` and the `_robust` copies without extreme moves |
| `q3_model_reliability.py` | Where is the final model more or less reliable than the baseline, by quarter and volatility group? | `q3_metrics`, `q3_paired_vs_baseline`, `q3_vol_cutoffs` |
| `q4_gdelt_vs_price_only.py` | Did adding GDELT features change test accuracy, overall and by group? | `q4_paired`, `q4_weekly`, `q4_disagreement`, `q4_model_families` |

Supporting files: `analytics_config.py` (paths, table names, shared helpers), `check_env.py` (package check),
`make_figures.py` (four figures from the CSV tables), `verify_analytics.py` (checks against the Task 3 run summary).

All results are associations. They do not show that volume, news or GDELT features cause price moves, and the
Task 4 tables are exploratory, with no tuning on the 2017 test period.

## What the scripts read

The Hive database from the notebook (default `bigdata`): `train`, `val`, `test`, `clean_prices`, `gdelt_daily`,
`pred_final_test` and `pred_gdelt_test`. Q3 and Q4 need the Task 3 prediction tables to exist, so run the notebook
through Part 2 first. If a table is not in Hive the scripts read Parquet from `PROJ_TABLE_ROOT/<table>`.

## Run order

Window C is the cluster primary node. Run each step from the repository root.

1. **Check the Python packages** (once per cluster):

   ```bash
   python3 scripts/task4_analytics/check_env.py
   ```

   If anything is missing, this one command installs it (it runs `pip install --user` with pinned versions) and then checks again:

   ```bash
   python3 scripts/task4_analytics/check_env.py --install
   ```

   The same by hand:

   ```bash
   python3 -m pip install --user "numpy==1.26.4" "pandas==2.2.3" "matplotlib==3.9.4" "python-dateutil>=2.8.2,<=2.9.0"
   ```

   Keep numpy at 1.26.4 because pandas and matplotlib here are built for numpy 1.x. Keep python-dateutil below 3 because
   the AWS command-line tools on the node need it. pip installs the other dependencies by itself. The node needs a route
   to PyPI. If it has none, install the same packages from a notebook session with `sc.install_pypi_package(...)`.

2. **Set one run id** so every output of this run carries the same id:

   ```bash
   export PROJ_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
   ```

   Optional settings, all environment variables with defaults in `analytics_config.py`:

   | Variable | Default | Use |
   |---|---|---|
   | `PROJ_HIVE_DB` | `bigdata` | Hive database (for example the one a rerun used) |
   | `PROJ_OUT_SUFFIX` | empty | Suffix of the prediction tables, same as the notebook's `OUT_SUFFIX` |
   | `PROJ_MODEL_ROOT` | `hdfs:///project/models` | Where Task 3 saved the models, read by `q4_model_families` |
   | `PROJ_LOCAL_OUT` | `analytics_output` | Folder for tables, figures, run log |
   | `PROJ_S3_RESULTS` | empty | When set, every result is also written to this S3 prefix |
   | `PROJ_RESULTS_DIR` | `hdfs:///project/analytics/results` | HDFS folder for the Parquet results |

3. **Run the four analyses in order.** Each one prints its tables and appends to `analytics_output/analytics_run_log.jsonl`.
   Client mode keeps the driver on the primary node, so `analytics_output` is written there. Put `time` in front of each
   command to record its runtime.

   ```bash
   time spark-submit --master yarn --deploy-mode client scripts/task4_analytics/q1_volume_analysis.py
   time spark-submit --master yarn --deploy-mode client scripts/task4_analytics/q2_gdelt_news_analysis.py
   time spark-submit --master yarn --deploy-mode client scripts/task4_analytics/q3_model_reliability.py
   time spark-submit --master yarn --deploy-mode client scripts/task4_analytics/q4_gdelt_vs_price_only.py
   ```

   A stopped notebook session frees the YARN memory these jobs need. If a job stays at ACCEPTED, stop the notebook session first.

   Q2 runs twice in one go, once as is and once without stock-days whose daily move is above 50 per cent (`_robust` tables).

4. **Draw the figures** (pandas and matplotlib only):

   ```bash
   python3 scripts/task4_analytics/make_figures.py
   ```

   This writes `fig1_q1_volume.png` to `fig4_q4_gdelt_gain.png` in `analytics_output/figures/`.

5. **Verify against Task 3:**

   ```bash
   python3 scripts/task4_analytics/verify_analytics.py
   ```

   It compares row counts, volatility cut-offs, baseline and model accuracy, group sizes and the GDELT gain with
   `evidence/task3_model/run_*/model_run_summary.json` (or with the file whose path you give as the first argument,
   for example `python3 scripts/task4_analytics/verify_analytics.py path/to/model_run_summary.json`), writes `analytics_output/verification_results.csv` and
   exits with status 1 if any check fails. The Expected and Actual columns fill Appendix B of the report.

6. **Keep the evidence before the cluster is terminated.** Copy the small outputs into the repository:

   ```bash
   mkdir -p evidence/task4_analytics/run_<date>
   cp -r analytics_output/tables analytics_output/figures evidence/task4_analytics/run_<date>/
   cp analytics_output/analytics_run_log.jsonl analytics_output/verification_results.csv evidence/task4_analytics/run_<date>/
   ```

   Do not commit credentials, key files, host names or machine paths. `verify_analytics.py` scans the tables and the
   run log for laptop paths and host names, so run it before copying.

## Method choices worth knowing

- **Baselines.** The final model is compared with the training-majority rule over all training weeks (`baseline`).
  The GDELT models are compared with the majority rule over the GDELT-window training weeks (`baseline_gdelt`),
  because that window has a different share of up weeks. Each model is paired with its own baseline on identical rows.
- **Volatility groups.** Cut-offs are the training terciles of `volatility_20d` from `approxQuantile`
  (1/3 and 2/3, relative error 0.001), the same call as the notebook, so groups match Task 3.
- **Intervals.** Differences are averaged per week first and use Student's t with weeks minus one degrees of freedom,
  because stocks in one week share market moves. Row-level Wilson columns exist in `q3_metrics` but are not plotted.
- **GDELT comparison.** `gdelt_price_only` is a random forest and `gdelt_price_gdelt` is logistic regression
  (`q4_model_families`), so the accuracy gain mixes a model-family change with the GDELT features.
  The tone and event-volume groups hold only a few test weeks, and very unequal numbers (`n_weeks` in `q4_paired`),
  so they are shown but not interpreted.
- **Spearman intervals** in `q2_spearman` use the Fisher z approximation and treat weeks as independent. Weekly series
  are autocorrelated, so the intervals are too narrow and serve only as a guide. The 72 tests are exploratory.
- **No random numbers** are used in Task 4, so there is no seed to report.

## Running from the notebook instead

If the cluster has the scripts on S3, a notebook cell can import them. Add each script with `sc.addPyFile` (for
example `sc.addPyFile("s3://<your-bucket>/scripts/task4_analytics/analytics_config.py")`), import `run_q1`, `run_q2`,
`run_q3` and `run_q4`, and call each one. The packages in step 1 then have to be installed in the notebook session
with `sc.install_pypi_package("numpy==1.26.4")` and the same for the others.
