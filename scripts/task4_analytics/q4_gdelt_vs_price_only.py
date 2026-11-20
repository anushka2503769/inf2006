"""
scripts/task4_analytics/q4_gdelt_vs_price_only.py  --  Task 4 (Analytics & Visualisation)

Modelling question: Do GDELT features improve on price-only features on the
SAME rows?

Input: the team's pred_gdelt_test (test rows inside the GDELT window,
Jan-Jun 2017): price-only and price+GDELT predictions side by side for
identical (Name, date) rows, joined to the prepared `test` table for
volatility_20d and the market-wide GDELT features g_events_7d / g_tone_7d.
Both models were trained on Jul-Dec 2015 and tuned on 2016 by the modelling
step; nothing is refitted or retuned here.

Because GDELT features are market wide, every series in a week shares the same
GDELT values. The effective sample is the ~26 test weeks, not the row count, so
every difference below carries a week-clustered standard error, and a count of
weeks in which GDELT was better / worse / tied is reported.

Gains are defined so that POSITIVE = GDELT model better:
  acc_gain     = correct(price+GDELT) - correct(price-only)
  logloss_gain = logloss(price-only)  - logloss(price+GDELT)
  brier_gain   = brier(price-only)    - brier(price+GDELT)

Groups: overall, quarter, volatility group (volatility_20d terciles from TRAIN
rows), GDELT tone group and GDELT event-volume group (terciles of g_tone_7d and
g_events_7d from TRAIN rows inside the GDELT window; low tone = most negative).

Outputs (HDFS Parquet under RESULTS_DIR + CSV under TABLE_DIR):
  q4_coverage, q4_cutoffs, q4_metrics, q4_paired, q4_weekly, q4_disagreement,
  q4_model_families

Notebook use:   from q4_gdelt_vs_price_only import run_q4;  res = run_q4(spark)

Model families: the two models were chosen separately on the validation rows, so
they can be different algorithms (in the 17 Nov run the frozen price-only model
was a random forest and the price+GDELT model a logistic regression). The gain
then mixes a GDELT effect with a model-family effect. q4_model_families records
which algorithm each saved model ends with, so the report can say so.

Interpretation rule: a gain, if any, is an association on one half-year of
held-out data (about 26 weeks), measured as classification accuracy / loss on
source-price ratios. It is not a causal effect of news and not evidence of an
investable strategy. No gain is a valid finding. Do not interpret the tone and
event-volume groups: they hold only a few test weeks each, in very unequal numbers (see n_weeks in q4_paired).
"""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F

from analytics_config import (COLS, MODEL_ROOT, OUT_SUFFIX, PRED_GDELT, get_spark,
                              load_table, log_run, logloss, save_result, t_crit)
from q3_model_reliability import (metrics_table, train_cutoffs, vol_frame,
                                  vol_group_expr)

T_LABELS = ["1_low", "2_mid", "3_high"]
MODELS = ("price_only", "price_gdelt")
COMPARISON = "gdelt_vs_price_only"
GAIN_METRICS = ["acc_gain", "logloss_gain", "brier_gain"]

SYM, DT = COLS["symbol"], COLS["date"]


def tercile_expr(col, c1, c2):
    return (F.when(F.col(col).isNull(), F.lit(None).cast("string"))
             .when(F.col(col) <= c1, T_LABELS[0])
             .when(F.col(col) <= c2, T_LABELS[1])
             .otherwise(T_LABELS[2]))


def model_families(spark):
    """(model, final stage class) of the saved PipelineModels, read from HDFS.
    Returns 'unknown' for a model that cannot be loaded here."""
    rows = []
    for name in ("gdelt_price_only", "gdelt_price_gdelt"):
        try:
            from pyspark.ml import PipelineModel
            fam = type(PipelineModel.load(f"{MODEL_ROOT}/{name}{OUT_SUFFIX}").stages[-1]).__name__
        except Exception as e:   # keep the analysis going, but say it is unknown
            fam = "unknown"
            print(f"[model_families] could not load {name}: {e}")
        rows.append((name, fam))
    return spark.createDataFrame(rows, "model string, final_stage string")


# -----
def build_frame(spark):
    feats = load_table(spark, "features")
    gd = load_table(spark, "pred_gdelt")

    f = feats.select(F.col(SYM).alias("symbol"), F.to_date(F.col(DT)).alias("d"),
                     F.col(COLS["split"]).alias("split_f"),
                     F.col(COLS["vol"]).cast("double").alias("vol20"),
                     F.col("g_events_7d").cast("double").alias("g_events"),
                     F.col("g_tone_7d").cast("double").alias("g_tone"))
    vol = vol_frame(feats)
    v1, v2 = train_cutoffs(vol)
    gtr = f.where((F.col("split_f") == "train") & F.col("g_tone").isNotNull())
    e1, e2 = [float(x) for x in gtr.approxQuantile("g_events", [1 / 3, 2 / 3], 0.001)]
    t1, t2 = [float(x) for x in gtr.approxQuantile("g_tone", [1 / 3, 2 / 3], 0.001)]

    y = F.col(COLS["label"]).cast("double")
    pp, qp = F.col(PRED_GDELT["pred_p"]).cast("double"), F.col(PRED_GDELT["prob_p"]).cast("double")
    pg, qg = F.col(PRED_GDELT["pred_g"]).cast("double"), F.col(PRED_GDELT["prob_g"]).cast("double")
    g = (gd.select(F.col(SYM).alias("symbol"), F.to_date(F.col(DT)).alias("d"),
                   y.alias("label"), pp.alias("pred_p"), qp.alias("prob_p"),
                   pg.alias("pred_g"), qg.alias("prob_g"))
         .where(F.col("label").isNotNull() & F.col("pred_p").isNotNull()
                & F.col("pred_g").isNotNull()))
    n_pred = gd.count()
    n_valid = g.count()

    j = (g.join(f.where(F.col("split_f") == "test")
                .select("symbol", "d", "vol20", "g_events", "g_tone"),
                ["symbol", "d"], "left"))
    j = (j.withColumn("vol_group", F.when(F.col("vol20").isNotNull(), vol_group_expr(v1, v2)))
          .withColumn("tone_group", tercile_expr("g_tone", t1, t2))
          .withColumn("events_group", tercile_expr("g_events", e1, e2))
          .withColumn("quarter", F.concat(F.year("d").cast("string"), F.lit("Q"),
                                          F.quarter("d").cast("string")))
          .withColumn("correct_p", (F.col("pred_p") == F.col("label")).cast("double"))
          .withColumn("correct_g", (F.col("pred_g") == F.col("label")).cast("double"))
          .withColumn("ll_p", logloss(F.col("prob_p"), F.col("label")))
          .withColumn("ll_g", logloss(F.col("prob_g"), F.col("label")))
          .withColumn("br_p", F.pow(F.col("prob_p") - F.col("label"), 2))
          .withColumn("br_g", F.pow(F.col("prob_g") - F.col("label"), 2))
          .withColumn("acc_gain", F.col("correct_g") - F.col("correct_p"))
          .withColumn("logloss_gain", F.col("ll_p") - F.col("ll_g"))
          .withColumn("brier_gain", F.col("br_p") - F.col("br_g")))
    cuts = [("vol20 cut 1 (train)", v1), ("vol20 cut 2 (train)", v2),
            ("g_tone_7d cut 1 (train, GDELT window)", t1), ("g_tone_7d cut 2", t2),
            ("g_events_7d cut 1 (train, GDELT window)", e1), ("g_events_7d cut 2", e2)]
    return j, n_pred, n_valid, cuts


def explode_dims(df):
    def d(name, col):
        return F.struct(F.lit(name).alias("dimension"), F.col(col).alias("grp"))
    arr = F.array(F.struct(F.lit("overall").alias("dimension"), F.lit("all").alias("grp")),
                  d("quarter", "quarter"), d("vol_group", "vol_group"),
                  d("tone_group", "tone_group"), d("events_group", "events_group"))
    out = df.select("*", F.explode(arr).alias("dg"))
    return (out.where(F.col("dg.grp").isNotNull())
               .select(*df.columns, F.col("dg.dimension").alias("dimension"),
                       F.col("dg.grp").alias("grp")))


def metrics_long(j):
    base = ["symbol", "d", "label", "dimension", "grp"]
    ex = explode_dims(j)
    def side(model, pred, prob):
        return ex.select(*base, F.lit(COMPARISON).alias("comparison"),
                         F.lit(model).alias("model"), F.lit("test").alias("split"),
                         F.col(pred).alias("pred"), F.col(prob).alias("prob"))
    x = side("price_only", "pred_p", "prob_p").unionByName(
        side("price_gdelt", "pred_g", "prob_g"))
    return metrics_table(x).orderBy("dimension", "grp", "model")   # includes logloss, brier


def paired_table(j):
    ex = explode_dims(j.select("d", "quarter", "vol_group", "tone_group", "events_group",
                               *GAIN_METRICS))
    aggs = [F.count("*").alias("n_d")]
    for g in GAIN_METRICS:
        aggs += [F.sum(g).alias(f"sum_{g}"), F.avg(g).alias(f"wk_{g}")]
    byd = ex.groupBy("dimension", "grp", "d").agg(*aggs)
    out_aggs = [F.sum("n_d").alias("n_rows"), F.count("*").alias("n_weeks")]
    for g in GAIN_METRICS:
        out_aggs += [(F.sum(f"sum_{g}") / F.sum("n_d")).alias(g),
                     (F.stddev(f"wk_{g}") / F.sqrt(F.count("*"))).alias(f"se_{g}")]
    out_aggs += [F.sum((F.col("wk_acc_gain") > 0).cast("int")).alias("weeks_gdelt_better"),
                 F.sum((F.col("wk_acc_gain") < 0).cast("int")).alias("weeks_gdelt_worse"),
                 F.sum((F.col("wk_acc_gain") == 0).cast("int")).alias("weeks_tied")]
    out = byd.groupBy("dimension", "grp").agg(*out_aggs)
    tc = t_crit(F.col("n_weeks"))   # Student's t, (weeks - 1) degrees of freedom
    for g in GAIN_METRICS:
        out = (out.withColumn(f"{g}_ci95_lo", F.col(g) - tc * F.col(f"se_{g}"))
                  .withColumn(f"{g}_ci95_hi", F.col(g) + tc * F.col(f"se_{g}")))
    return out.orderBy("dimension", "grp")


def weekly_table(j):
    return (j.groupBy("d")
            .agg(F.count("*").alias("n"),
                 F.avg("correct_p").alias("acc_price_only"),
                 F.avg("correct_g").alias("acc_price_gdelt"),
                 F.avg("acc_gain").alias("acc_gain"),
                 F.avg("prob_p").alias("mean_prob_price_only"),
                 F.avg("prob_g").alias("mean_prob_price_gdelt"),
                 F.avg("label").alias("share_up"),
                 F.first("g_tone", ignorenulls=True).alias("g_tone_7d"),
                 F.first("g_events", ignorenulls=True).alias("g_events_7d"))
            .orderBy("d"))


def disagreement_table(j):
    both = j.unionByName(j.withColumn("quarter", F.lit("all")))
    diff = F.col("pred_p") != F.col("pred_g")
    return (both.groupBy("quarter")
            .agg(F.count("*").alias("n_rows"),
                 F.sum(diff.cast("int")).alias("n_predictions_differ"),
                 F.sum((diff & (F.col("correct_g") == 1)).cast("int")).alias("n_gdelt_right"),
                 F.sum((diff & (F.col("correct_p") == 1)).cast("int")).alias("n_price_only_right"))
            .withColumn("share_differ", F.col("n_predictions_differ") / F.col("n_rows"))
            .withColumn("gdelt_right_share_when_differ",
                        F.when(F.col("n_predictions_differ") > 0,
                               F.col("n_gdelt_right") / F.col("n_predictions_differ")))
            .orderBy("quarter"))


# -----
def run_q4(spark=None):
    spark = spark or get_spark("q4_gdelt_vs_price_only")
    j, n_pred, n_valid, cuts = build_frame(spark)
    j = j.cache()
    n_joined = j.where(F.col("vol20").isNotNull()).count()
    n_weeks = j.select("d").distinct().count()

    cov = spark.createDataFrame(
        [("rows in pred_gdelt_test", n_pred),
         ("rows with label and both predictions", n_valid),
         ("rows matched to test features (volatility_20d)", n_joined),
         ("distinct test weeks (effective sample)", n_weeks)],
        "item string, n long")
    cut_df = spark.createDataFrame(cuts, "item string, value double")

    res = {
        "coverage": save_result(cov, "q4_coverage"),
        "cutoffs": save_result(cut_df, "q4_cutoffs"),
        "metrics": save_result(metrics_long(j), "q4_metrics"),
        "paired": save_result(paired_table(j), "q4_paired"),
        "weekly": save_result(weekly_table(j), "q4_weekly"),
        "disagreement": save_result(disagreement_table(j), "q4_disagreement"),
        "model_families": save_result(model_families(spark), "q4_model_families"),
    }
    log_run(spark, "q4_gdelt_vs_price_only", inputs=["pred_gdelt_test", "test", "train"],
            outputs=list(res), rows=n_valid, note=f"weeks={n_weeks}")
    j.unpersist()
    return res


if __name__ == "__main__":
    out = run_q4()
    for name, pdf in out.items():
        print(f"\n=== {name} ===")
        print(pdf.head(30).to_string(index=False))