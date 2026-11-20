"""
scripts/task4_analytics/q3_model_reliability.py  --  Task 4 (Analytics & Visualisation)

Q3 (reliability): Across periods and volatility groups, where is the model most
and least reliable versus the baseline?

Input: the long 'predictions' view built in analytics_config from the team's
pred_final_test and pred_gdelt_test (test rows, 2017) plus two training-majority
baselines. Models: 'full_price' (final model), 'gdelt_price_only',
'gdelt_price_gdelt'. Baselines, as in the modelling step: 'baseline' (learned on
all training weeks) for 'full_price', and 'baseline_gdelt' (learned on the
training weeks inside the GDELT window) for the two GDELT models.
Validation-period predictions are not saved by the modelling step, so this
analysis covers the 2017 test period only.

Fair comparison: every model is compared with its baseline ONLY on the rows both
have scored (e.g. the GDELT models are restricted to the GDELT window), so no model
is credited or penalised for covering different rows.

Groups (all computed distributed, so rows with unavailable targets are dropped
first and counted in q3_coverage):
  overall            : whole split
  quarter            : calendar quarter of the decision date (e.g. 2017Q2)
  vol_group          : volatility_20d of the series (prior 20 trading rows), split into
                       terciles with cut-offs learned on TRAIN rows only (nothing is
                       tuned on test). Same call as the notebook: approxQuantile at
                       1/3 and 2/3 with relative error 0.001
  quarter_x_vol      : both together

Metrics per (comparison, model, split, dimension, group): n, confusion counts
(tp, fp, tn, fn), accuracy with Wilson 95% interval, precision, recall, F1,
balanced accuracy, base rate (share of 'up'), majority-class accuracy, AUC,
log-loss and Brier score. The Wilson columns treat rows as independent and are too
narrow, so do not plot them.
Paired table: accuracy(model) - accuracy(baseline) on identical rows, with a
standard error clustered by week (series in one week share market shocks, so the
naive row-level interval would be far too narrow). Its 95% interval uses Student's t
with (weeks - 1) degrees of freedom, because a quarter has only about 13 weeks.
For the final model the difference is 0 by construction when its predictions equal
the baseline's, so its interval [0, 0] shows no certainty, only no difference.

Outputs (HDFS Parquet under RESULTS_DIR + CSV under TABLE_DIR):
  q3_coverage, q3_vol_cutoffs, q3_metrics, q3_paired_vs_baseline, q3_symbol_test

Notebook use:   from q3_model_reliability import run_q3;  res = run_q3(spark)

Interpretation rule: these are held-out classification diagnostics on
source-price ratios. They are not trading returns and do not show that a
strategy would have been investable. Subgroup sizes are reported; do not read
differences between small groups. q3_symbol_test ranks about 500 series on at most
51 weeks, so its best and worst series are mostly luck: exploratory, name no tickers.
Do not retune the model on these results.
"""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import Window as W
from pyspark.sql import functions as F

from analytics_config import (BASELINE_GDELT_NAME, BASELINE_NAME, BASELINES, COLS,
                              GDELT_MODEL_PREFIX, get_spark, load_table, log_run,
                              logloss, save_result, t_crit)

# Baselines are built by analytics_config (training-majority rule), see its header.
VOL_LABELS = ["1_low_vol", "2_mid_vol", "3_high_vol"]
MIN_SYMBOL_ROWS = 20                  # for the per-series table
KEYS = ["comparison", "model", "split", "dimension", "grp"]

SYM, DT = COLS["symbol"], COLS["date"]


# Inputs
def load_predictions(spark):
    pr = load_table(spark, "predictions")
    prob = (F.col(COLS["prob"]).cast("double")
            if COLS["prob"] in pr.columns else F.lit(None).cast("double"))
    p = (pr.select(F.col(SYM).alias("symbol"),
                   F.to_date(F.col(DT)).alias("d"),
                   F.col(COLS["split"]).alias("split"),
                   F.col(COLS["label"]).cast("double").alias("label"),
                   F.col(COLS["pred"]).cast("double").alias("pred"),
                   prob.alias("prob"),
                   F.col(COLS["model"]).alias("model")))
    n_raw = p.count()
    p = p.where(F.col("label").isNotNull() & F.col("pred").isNotNull())
    return p, n_raw


def vol_frame(feats):
    """Trailing 20-day volatility from the team's prepared tables (column
    volatility_20d: prior trading rows only, never the prediction day)."""
    return feats.select(F.col(SYM).alias("symbol"),
                        F.to_date(F.col(DT)).alias("d"),
                        F.col(COLS["split"]).alias("split_f"),
                        F.col(COLS["vol"]).cast("double").alias("vol20"))


def train_cutoffs(vol):
    """Tercile cut-offs of vol20 on TRAIN observations only. Same call as the
    notebook (approxQuantile at 1/3 and 2/3, relative error 0.001), so the cut-offs
    equal the notebook's (0.010648 and 0.014899 in the 17 Nov run)."""
    tr = vol.where((F.col("split_f") == "train") & F.col("vol20").isNotNull())
    c = tr.approxQuantile("vol20", [1 / 3, 2 / 3], 0.001)
    if len(c) < 2:
        raise ValueError("No training rows with volatility_20d.")
    return float(c[0]), float(c[1])


def vol_group_expr(c1, c2):
    return (F.when(F.col("vol20") <= c1, VOL_LABELS[0])
             .when(F.col("vol20") <= c2, VOL_LABELS[1])
             .otherwise(VOL_LABELS[2]))


# Paired frame: model and baseline on identical rows
def build_paired(p, baselines, vol_g):
    """Pair every model row with the row of ITS baseline: 'baseline_gdelt' for models
    whose name starts with 'gdelt_', 'baseline' for the others."""
    names = [r["model"] for r in p.select("model").distinct().collect()]
    missing = [b for b in baselines if b not in names]
    if missing:
        raise ValueError(f"Baseline(s) {missing} not in prediction table; "
                         f"found models {sorted(names)}.")
    base = (p.where(F.col("model").isin(*baselines))
             .select("symbol", "d", "split", F.col("model").alias("baseline_model"),
                     F.col("pred").alias("pred_b"), F.col("prob").alias("prob_b")))
    oth = (p.where(~F.col("model").isin(*baselines))
            .withColumn("baseline_model",
                        F.when(F.col("model").startswith(GDELT_MODEL_PREFIX),
                               F.lit(BASELINE_GDELT_NAME))
                         .otherwise(F.lit(BASELINE_NAME))))
    j = (oth.join(base, ["symbol", "d", "split", "baseline_model"], "inner")
            .withColumn("comparison", F.col("model"))
            .withColumn("correct_m", (F.col("pred") == F.col("label")).cast("double"))
            .withColumn("correct_b", (F.col("pred_b") == F.col("label")).cast("double")))
    n_paired = j.count()
    j = (j.join(vol_g, ["symbol", "d"], "inner")
          .withColumn("quarter", F.concat(F.year("d").cast("string"), F.lit("Q"),
                                          F.quarter("d").cast("string"))))
    return j, sorted(names), n_paired


def to_long(j):
    cols = ["symbol", "d", "split", "label", "quarter", "vol_group", "comparison"]
    a = j.select(*cols, F.col("model"), "pred", "prob")
    b = j.select(*cols, F.col("baseline_model").alias("model"),
                 F.col("pred_b").alias("pred"), F.col("prob_b").alias("prob"))
    return a.unionByName(b)


def explode_dims(df):
    dims = F.explode(F.array(
        F.struct(F.lit("overall").alias("dimension"), F.lit("all").alias("grp")),
        F.struct(F.lit("quarter").alias("dimension"), F.col("quarter").alias("grp")),
        F.struct(F.lit("vol_group").alias("dimension"), F.col("vol_group").alias("grp")),
        F.struct(F.lit("quarter_x_vol").alias("dimension"),
                 F.concat_ws("|", F.col("quarter"), F.col("vol_group")).alias("grp")),
    )).alias("dg")
    return (df.select("*", dims)
              .select(*[c for c in df.columns], F.col("dg.dimension").alias("dimension"),
                      F.col("dg.grp").alias("grp")))


# Metrics
def metrics_table(x):
    pos, prd = F.col("label") == 1, F.col("pred") == 1
    m = (x.groupBy(*KEYS)
         .agg(F.count("*").alias("n"),
              F.sum((pos & prd).cast("int")).alias("tp"),
              F.sum((~pos & prd).cast("int")).alias("fp"),
              F.sum((~pos & ~prd).cast("int")).alias("tn"),
              F.sum((pos & ~prd).cast("int")).alias("fn"),
              F.avg(logloss(F.col("prob"), F.col("label"))).alias("logloss"),
              F.avg(F.pow(F.col("prob") - F.col("label"), 2)).alias("brier")))
    n = F.col("n").cast("double")
    p_ = (F.col("tp") + F.col("tn")) / n
    z = 1.96
    m = (m.withColumn("accuracy", p_)
          .withColumn("acc_wilson_lo",
                      ((p_ + z * z / (2 * n)) - z * F.sqrt(p_ * (1 - p_) / n + z * z / (4 * n * n)))
                      / (1 + z * z / n))
          .withColumn("acc_wilson_hi",
                      ((p_ + z * z / (2 * n)) + z * F.sqrt(p_ * (1 - p_) / n + z * z / (4 * n * n)))
                      / (1 + z * z / n))
          .withColumn("precision", F.when(F.col("tp") + F.col("fp") > 0,
                                          F.col("tp") / (F.col("tp") + F.col("fp"))))
          .withColumn("recall", F.when(F.col("tp") + F.col("fn") > 0,
                                       F.col("tp") / (F.col("tp") + F.col("fn"))))
          .withColumn("specificity", F.when(F.col("tn") + F.col("fp") > 0,
                                            F.col("tn") / (F.col("tn") + F.col("fp"))))
          .withColumn("f1", F.when(F.col("precision").isNotNull() & F.col("recall").isNotNull()
                                   & (F.col("precision") + F.col("recall") > 0),
                                   2 * F.col("precision") * F.col("recall")
                                   / (F.col("precision") + F.col("recall"))))
          .withColumn("balanced_accuracy", (F.col("recall") + F.col("specificity")) / 2)
          .withColumn("base_rate_up", (F.col("tp") + F.col("fn")) / n)
          .withColumn("majority_accuracy",
                      F.greatest(F.col("base_rate_up"), 1 - F.col("base_rate_up"))))
    return m.join(auc_table(x), KEYS, "left")


def auc_table(x):
    """Rank-based AUC with average ranks for ties (Mann-Whitney)."""
    d = x.where(F.col("prob").isNotNull())
    wr = W.partitionBy(*KEYS).orderBy("prob")
    wt = W.partitionBy(*KEYS, "prob")
    d = (d.withColumn("r_min", F.rank().over(wr))
          .withColumn("ties", F.count("*").over(wt))
          .withColumn("r_avg", F.col("r_min") + (F.col("ties") - 1) / 2))
    a = (d.groupBy(*KEYS)
         .agg(F.sum((F.col("label") == 1).cast("double")).alias("n_pos"),
              F.sum((F.col("label") == 0).cast("double")).alias("n_neg"),
              F.sum(F.when(F.col("label") == 1, F.col("r_avg"))).alias("sr")))
    return a.select(*KEYS, F.when((F.col("n_pos") > 0) & (F.col("n_neg") > 0),
                                  (F.col("sr") - F.col("n_pos") * (F.col("n_pos") + 1) / 2)
                                  / (F.col("n_pos") * F.col("n_neg"))).alias("auc"))


def paired_table(j):
    pdx = explode_dims(j.select("comparison", "split", "d", "quarter", "vol_group",
                                (F.col("correct_m") - F.col("correct_b")).alias("diff")))
    byd = (pdx.groupBy("comparison", "split", "dimension", "grp", "d")
           .agg(F.count("*").alias("n_d"), F.sum("diff").alias("sum_d"),
                F.avg("diff").alias("mean_d")))
    out = (byd.groupBy("comparison", "split", "dimension", "grp")
           .agg(F.sum("n_d").alias("n_rows"),
                F.count("*").alias("n_weeks"),
                (F.sum("sum_d") / F.sum("n_d")).alias("acc_diff_vs_baseline"),
                (F.stddev("mean_d") / F.sqrt(F.count("*"))).alias("se_week_clustered")))
    tc = t_crit(F.col("n_weeks"))   # Student's t, (weeks - 1) degrees of freedom
    return (out.withColumn("ci95_lo", F.col("acc_diff_vs_baseline") - tc * F.col("se_week_clustered"))
               .withColumn("ci95_hi", F.col("acc_diff_vs_baseline") + tc * F.col("se_week_clustered"))
               .orderBy("comparison", "split", "dimension", "grp"))


def symbol_table(j):
    return (j.where(F.col("split") == "test")
            .groupBy("comparison", "symbol")
            .agg(F.count("*").alias("n"),
                 F.avg("correct_m").alias("acc_model"),
                 F.avg("correct_b").alias("acc_baseline"))
            .where(F.col("n") >= MIN_SYMBOL_ROWS)
            .withColumn("acc_diff", F.col("acc_model") - F.col("acc_baseline"))
            .orderBy("comparison", F.desc("acc_diff")))


def run_q3(spark=None, baselines=BASELINES):
    spark = spark or get_spark("q3_model_reliability")
    p, n_raw = load_predictions(spark)
    p = p.cache()
    n_valid = p.count()
    dups = (p.groupBy("symbol", "d", "split", "model").count()
            .where(F.col("count") > 1).count())

    feats = load_table(spark, "features")
    vol = vol_frame(feats)
    c1, c2 = train_cutoffs(vol)
    vol_g = (vol.where(F.col("vol20").isNotNull())
             .withColumn("vol_group", vol_group_expr(c1, c2))
             .select("symbol", "d", "vol_group"))

    j, models, n_paired = build_paired(p, baselines, vol_g)
    j = j.cache()
    n_final = j.count()
    long_x = explode_dims(to_long(j)).cache()

    cov = spark.createDataFrame(
        [("prediction rows (all models)", n_raw),
         ("rows dropped: label or prediction unavailable", n_raw - n_valid),
         ("duplicate (series, date, split, model) keys", dups),
         ("non-baseline rows paired with a baseline row", n_paired),
         ("paired rows with a volatility group", n_final),
         ("paired rows dropped: no trailing volatility", n_paired - n_final)],
        "item string, n long")
    cuts = spark.createDataFrame(
        [("vol20 tercile cut 1 (train only)", c1), ("vol20 tercile cut 2 (train only)", c2)],
        "item string, value double")

    res = {
        "coverage": save_result(cov, "q3_coverage"),
        "vol_cutoffs": save_result(cuts, "q3_vol_cutoffs"),
        "metrics": save_result(metrics_table(long_x), "q3_metrics"),
        "paired_vs_baseline": save_result(paired_table(j), "q3_paired_vs_baseline"),
        "symbol_test": save_result(symbol_table(j), "q3_symbol_test"),
    }
    log_run(spark, "q3_model_reliability", inputs=["pred_final_test", "pred_gdelt_test", "train", "features"],
            outputs=list(res), rows=n_final,
            note=f"models={models}, baselines={list(baselines)}, vol_cuts=({c1:.6f},{c2:.6f})")
    for df in (p, j, long_x):
        df.unpersist()
    return res


if __name__ == "__main__":
    out = run_q3()
    for name, pdf in out.items():
        print(f"\n=== {name} ===")
        print(pdf.head(30).to_string(index=False))