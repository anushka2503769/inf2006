# Task 4 analytics: run 2026-11-20

Run id `20261119T185121Z` (UTC), which is 20 Nov 2026 02:51 in Singapore. Amazon EMR 7.10.0, Spark 3.5.5, Python 3.9.
The code is in `scripts/task4_analytics/` and its README gives the run order.

## Files

| Path | What it holds |
|---|---|
| `tables/` | 30 CSV result tables from `q1_` to `q4_` (small aggregates only) |
| `figures/` | `fig1_q1_volume.png`, `fig2_q2_news.png`, `fig3_q3_reliability.png`, `fig4_q4_gdelt_gain.png` |
| `analytics_run_log.jsonl` | One line per saved table, with row counts and the run id |
| `verification_results.csv` | 39 checks against the Task 3 run summary, all PASS |
| `e2_check.py`, `e2_result.txt` | Hand recomputation of one volume z-score (series A, 21 Apr 2014) compared with Spark |

## Inputs

The Task 3 outputs of 17 Nov 2026, read from the team's S3 bucket: the Parquet tables `train`, `val`, `test`,
`clean_prices`, `pred_final_test` and `pred_gdelt_test`, and the saved models. The new cluster had no `bigdata`
Hive database, so `gdelt_daily` was rebuilt in Hive from the Task 2 daily GDELT file of 17 Nov (731 rows,
1 Jul 2015 to 30 Jun 2017) with the SQL of `02_gdelt_daily.hql`. No raw datasets are stored here.

## Runtimes

Measured with `time` on the primary node, client mode: q1 2 min 0 s, q2 1 min 22 s, q3 2 min 6 s, q4 1 min 7 s.

## Checks

- `verification_results.csv`: row counts, volatility cut-offs, baseline and model accuracy, group sizes and the GDELT gain match Task 3 within the stated tolerances.
- Saved results: 30 result folders in HDFS and 30 in S3, equal to the 30 CSV tables.
- Rerun: Q3 was run a second time on the same cluster and `q3_metrics.csv` matched the first run (largest difference 2.2e-16). The rerun output is not kept.
- Hand check: `e2_result.txt` shows the z-score of one stock and day recomputed by hand, equal to Spark's value.

## Notes

- The four figures were redrawn from these tables with `make_figures.py` on 20 Nov after layout fixes. The first drawing on the cluster had cropped labels.
- Console logs (`q1.log` to `q4.log`) are not included because they contain internal host names.
- The run log contains the S3 bucket name and YARN application ids, and no keys, host names or machine paths.
- All results are associations and are exploratory. See Section 5 of the report.
