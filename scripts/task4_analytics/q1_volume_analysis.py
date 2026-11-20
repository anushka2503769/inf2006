"""
scripts/task4_analytics/q1_volume_analysis.py  --  Task 4 (Analytics & Visualisation)

Q1 (volume): When is volume unusually high versus a series' own history, and do
next-week price changes differ then?

Method (all distributed in PySpark; only small aggregates reach pandas)
  1. Per-series volume z-score on log1p(volume): each day is compared with the
     same series' previous Z_WINDOW trading days (current day excluded, so the
     history is strictly past-only). Raw volume is never compared across series.
  2. vol_z_5d = mean of the z-scores of the 5 trading days BEFORE the decision
     date (Friday open), i.e. information available at prediction time.
  3. Join to the prepared Friday observations (features table) that have a known
     next-week target, bucket vol_z_5d, and summarise next-week change by bucket
     and split (train / val / test).
  4. Robustness: because all series in a week share market shocks, per-row
     t-statistics overstate evidence. We therefore also compute a weekly
     high-minus-normal spread (one number per Friday) and test it across weeks.
  5. Timing: monthly share of series-days with unusually high volume and the
     most market-wide high-volume days (descriptive, same-day z).

Outputs (HDFS Parquet under RESULTS_DIR + CSV under TABLE_DIR):
  q1_coverage, q1_zscore_quantiles, q1_bucket_summary, q1_weekly_spread,
  q1_weekly_spread_by_week, q1_sameday_abs_change, q1_high_share_by_month,
  q1_top_market_days

Notebook use:   from q1_volume_analysis import run_q1;  res = run_q1(spark)
Command line:   spark-submit scripts/task4_analytics/q1_volume_analysis.py

Column names: next-week change in the opening price is called "pchg" (open_next /
open - 1). It is a ratio of source prices, not an investment return.

Interpretation rule: differences are ASSOCIATIONS in historical source-price
ratios, not causal effects, and not evidence of a tradable strategy. The analysis
is exploratory: four periods x three buckets are compared, so a few differences
will look large by chance. The row-level standard errors of an earlier version were
removed, because all series in one week share market moves and they were far too
narrow. Use the weekly spread (one value per Friday) for any claim.
"""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import Window as W
from pyspark.sql import functions as F

from analytics_config import (COLS, get_spark, load_table, log_run,
                              save_result, t_crit)

# Parameters (fixed a priori, before looking at outcomes)
Z_WINDOW = 60 # trailing trading days used as each series' own history
Z_MIN_OBS = 40 # minimum history required to score a day
RECENT_DAYS = 5 # days before the decision date averaged into vol_z_5d
RECENT_MIN = 3 # minimum scored days inside that 5-day window
EDGES = (-0.5, 0.5, 1.0) # bucket edges on vol_z_5d
MIN_SERIES_PER_WEEK = 10 # per group, for the weekly spread
EXTREME_Z = 2.0 # same-day z threshold for the timing analysis
MIN_SERIES_FOR_MARKET_DAY = 100
SAME_DAY_EDGES = (-1.0, 1.0, EXTREME_Z) # same-day z buckets for the same-day price-move table
MAX_RETURN_GAP_DAYS = 5 # daily change only if the previous bar is this close

SYM, DT = COLS["symbol"], COLS["date"]
BUCKET_ORDER = ["1_low", "2_normal", "3_high", "4_very_high"]


# Step 1-2: volume z-scores
def build_volume_z(prices):
    """Return daily rows (symbol, date, z_same_day, vol_z_5d).

    z_same_day  : day's log-volume vs the previous Z_WINDOW days (descriptive).
    vol_z_5d    : mean z of the 5 days strictly before this row (predictive-safe).
    """
    p = (prices
         .select(F.col(SYM).alias("symbol"),
                 F.to_date(F.col(DT)).alias("d"),
                 F.col(COLS["volume"]).cast("double").alias("vol"))
         .where(F.col("vol").isNotNull() & (F.col("vol") >= 0)
                & F.col("d").isNotNull())
         .withColumn("lv", F.log1p("vol")))

    w_hist = (W.partitionBy("symbol").orderBy("d")
              .rowsBetween(-Z_WINDOW, -1))
    p = (p.withColumn("h_mean", F.avg("lv").over(w_hist))
          .withColumn("h_sd", F.stddev("lv").over(w_hist))
          .withColumn("h_n", F.count("lv").over(w_hist)))
    p = p.withColumn(
        "z_same_day",
        F.when((F.col("h_n") >= Z_MIN_OBS) & (F.col("h_sd") > 0),
               (F.col("lv") - F.col("h_mean")) / F.col("h_sd")))

    w_recent = (W.partitionBy("symbol").orderBy("d")
                .rowsBetween(-RECENT_DAYS, -1))
    p = (p.withColumn("z_recent_n", F.count("z_same_day").over(w_recent))
          .withColumn("z_recent_avg", F.avg("z_same_day").over(w_recent))
          .withColumn("vol_z_5d",
                      F.when(F.col("z_recent_n") >= RECENT_MIN,
                             F.col("z_recent_avg"))))
    return p.select("symbol", "d", "z_same_day", "vol_z_5d")


def bucket_expr(col):
    lo, hi, vhi = EDGES
    return (F.when(F.col(col).isNull(), F.lit(None).cast("string"))
             .when(F.col(col) < lo, "1_low")
             .when(F.col(col) < hi, "2_normal")
             .when(F.col(col) < vhi, "3_high")
             .otherwise("4_very_high"))


# Step 3: bucket summary joined to prepared observations
def build_analysis_frame(spark, feats, zdf):
    f = (feats.select(F.col(SYM).alias("symbol"),
                      F.to_date(F.col(DT)).alias("d"),
                      F.col(COLS["split"]).alias("split"),
                      F.col(COLS["label"]).cast("double").alias("label"),
                      F.col(COLS["next_ret"]).cast("double").alias("next_ret")))
    n_all = f.count()
    f = f.where(F.col("next_ret").isNotNull() & F.col("label").isNotNull())
    n_target = f.count()

    j = f.join(zdf.select("symbol", "d", "vol_z_5d"), ["symbol", "d"], "left")
    n_scored = j.where(F.col("vol_z_5d").isNotNull()).count()
    j = (j.where(F.col("vol_z_5d").isNotNull())
          .withColumn("vol_bucket", bucket_expr("vol_z_5d"))
          .withColumn("abs_ret", F.abs("next_ret")))

    cov = spark.createDataFrame(
        [("feature rows (all)", n_all),
         ("rows with known next-week target", n_target),
         ("rows also with a vol_z_5d score", n_scored),
         ("rows dropped: target unavailable", n_all - n_target),
         ("rows dropped: insufficient volume history", n_target - n_scored)],
        "item string, n long")
    return j, cov


def bucket_summary(j):
    both = j.unionByName(j.withColumn("split", F.lit("all")))
    agg = (both.groupBy("split", "vol_bucket")
           .agg(F.count("*").alias("n"),
                F.countDistinct("symbol").alias("n_series"),
                F.countDistinct("d").alias("n_weeks"),
                F.avg("next_ret").alias("mean_pchg"),
                F.stddev("next_ret").alias("sd_pchg"),
                F.expr("percentile_approx(next_ret, 0.5)").alias("median_pchg"),
                F.avg("abs_ret").alias("mean_abs_pchg"),
                F.avg("label").alias("share_up")))
    normal = agg.where(F.col("vol_bucket") == "2_normal").select(
        "split", F.col("mean_pchg").alias("nm_mean"),
        F.col("mean_abs_pchg").alias("nm_abs"))
    agg = (agg.join(normal, "split", "left")
              .withColumn("diff_mean_vs_normal", F.col("mean_pchg") - F.col("nm_mean"))
              .withColumn("diff_abs_vs_normal", F.col("mean_abs_pchg") - F.col("nm_abs")))
    return agg.drop("nm_mean", "nm_abs").orderBy("split", "vol_bucket")


# Step 4: week-level robustness (one observation per Friday)
def weekly_spread_by_week(j):
    """One row per Friday: high-volume series minus normal-volume series."""
    hi = F.col("vol_bucket").isin("3_high", "4_very_high")
    nm = F.col("vol_bucket") == "2_normal"
    return (j.groupBy("split", "d")
            .agg(F.count(F.when(hi, 1)).alias("n_hi"),
                 F.count(F.when(nm, 1)).alias("n_nm"),
                 F.avg(F.when(hi, F.col("next_ret"))).alias("hi_pchg"),
                 F.avg(F.when(nm, F.col("next_ret"))).alias("nm_pchg"),
                 F.avg(F.when(hi, F.col("abs_ret"))).alias("hi_abs"),
                 F.avg(F.when(nm, F.col("abs_ret"))).alias("nm_abs"))
            .where((F.col("n_hi") >= MIN_SERIES_PER_WEEK)
                   & (F.col("n_nm") >= MIN_SERIES_PER_WEEK))
            .withColumn("spread_pchg", F.col("hi_pchg") - F.col("nm_pchg"))
            .withColumn("spread_abs", F.col("hi_abs") - F.col("nm_abs"))
            .orderBy("split", "d"))


def weekly_spread(wk):
    """Summary of the per-week spreads, one row per split (and all)."""
    wk_all = wk.unionByName(wk.withColumn("split", F.lit("all")))
    out = (wk_all.groupBy("split")
           .agg(F.count("*").alias("n_weeks"),
                F.avg("spread_pchg").alias("mean_spread_pchg"),
                F.stddev("spread_pchg").alias("sd_spread_pchg"),
                F.avg("spread_abs").alias("mean_spread_abs"),
                F.stddev("spread_abs").alias("sd_spread_abs"),
                F.avg("n_hi").alias("avg_series_high"),
                F.avg("n_nm").alias("avg_series_normal")))
    se_pchg = F.col("sd_spread_pchg") / F.sqrt("n_weeks")
    se_abs = F.col("sd_spread_abs") / F.sqrt("n_weeks")
    out = (out.withColumn("t_spread_pchg", F.col("mean_spread_pchg") / se_pchg)
              .withColumn("t_spread_abs", F.col("mean_spread_abs") / se_abs)
              .withColumn("ci95_lo_spread_pchg", F.col("mean_spread_pchg") - t_crit(F.col("n_weeks")) * se_pchg)
              .withColumn("ci95_hi_spread_pchg", F.col("mean_spread_pchg") + t_crit(F.col("n_weeks")) * se_pchg)
              .withColumn("ci95_lo_spread_abs", F.col("mean_spread_abs") - t_crit(F.col("n_weeks")) * se_abs)
              .withColumn("ci95_hi_spread_abs", F.col("mean_spread_abs") + t_crit(F.col("n_weeks")) * se_abs))
    return out.orderBy("split")


# Same-day view: does unusual volume go with bigger price moves on the same day?
def sameday_abs_change(prices, zdf):
    """Mean absolute close-to-close change by same-day volume z bucket and year.
    Descriptive and contemporaneous: it shows association on the same day, it
    does not predict anything."""
    p = (prices.select(F.col(SYM).alias("symbol"),
                       F.to_date(F.col(DT)).alias("d"),
                       F.col(COLS["close"]).cast("double").alias("close"))
         .where(F.col("close") > 0))
    w = W.partitionBy("symbol").orderBy("d")
    p = (p.withColumn("prev_close", F.lag("close").over(w))
          .withColumn("prev_d", F.lag("d").over(w))
          .where(F.col("prev_close").isNotNull()
                 & (F.datediff("d", "prev_d") <= MAX_RETURN_GAP_DAYS))
          .withColumn("abs_chg", F.abs(F.col("close") / F.col("prev_close") - 1)))
    lo, hi, vhi = SAME_DAY_EDGES
    z = F.col("z_same_day")
    j = (p.join(zdf.select("symbol", "d", "z_same_day"), ["symbol", "d"], "inner")
          .where(z.isNotNull())
          .withColumn("z_bucket",
                      F.when(z < lo, "1_low")
                       .when(z < hi, "2_normal")
                       .when(z < vhi, "3_high")
                       .otherwise("4_very_high"))
          .withColumn("year", F.year("d")))
    both = j.unionByName(j.withColumn("year", F.lit(0)))   # year 0 = all years
    return (both.groupBy("year", "z_bucket")
            .agg(F.count("*").alias("n"),
                 F.countDistinct("d").alias("n_days"),
                 F.avg("abs_chg").alias("mean_abs_chg"),
                 F.expr("percentile_approx(abs_chg, 0.5)").alias("median_abs_chg"))
            .orderBy("year", "z_bucket"))


# Step 5: timing of unusual volume (descriptive, same-day)
def timing_tables(zdf):
    s = zdf.where(F.col("z_same_day").isNotNull())
    monthly = (s.groupBy(F.date_format("d", "yyyy-MM").alias("month"))
               .agg(F.count("*").alias("n_series_days"),
                    F.avg((F.col("z_same_day") >= EXTREME_Z).cast("double"))
                    .alias("share_high"),
                    F.avg((F.col("z_same_day") <= -EXTREME_Z).cast("double"))
                    .alias("share_low"),
                    F.avg("z_same_day").alias("mean_z"))
               .orderBy("month"))
    top = (s.groupBy("d")
           .agg(F.count("*").alias("n_series"),
                F.avg((F.col("z_same_day") >= EXTREME_Z).cast("double"))
                .alias("share_series_high"),
                F.avg("z_same_day").alias("mean_z"))
           .where(F.col("n_series") >= MIN_SERIES_FOR_MARKET_DAY)
           .orderBy(F.desc("share_series_high")).limit(15))
    return monthly, top


def zscore_quantiles(j):
    qs = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    row = j.agg(F.expr(f"percentile_approx(vol_z_5d, array({','.join(map(str, qs))}))")
                .alias("q")).first()["q"]
    return j.sparkSession.createDataFrame(
        [(q, float(v)) for q, v in zip(qs, row)], "quantile double, vol_z_5d double")


def run_q1(spark=None):
    spark = spark or get_spark("q1_volume_analysis")
    feats = load_table(spark, "features")
    prices = load_table(spark, "prices_clean")

    zdf = build_volume_z(prices).cache()
    j, cov = build_analysis_frame(spark, feats, zdf)
    j = j.cache()

    wk = weekly_spread_by_week(j).cache()
    res = {
        "coverage": save_result(cov, "q1_coverage"),
        "zscore_quantiles": save_result(zscore_quantiles(j), "q1_zscore_quantiles"),
        "bucket_summary": save_result(bucket_summary(j), "q1_bucket_summary"),
        "weekly_spread": save_result(weekly_spread(wk), "q1_weekly_spread"),
        "weekly_spread_by_week": save_result(wk, "q1_weekly_spread_by_week"),
        "sameday_abs_change": save_result(sameday_abs_change(prices, zdf),
                                          "q1_sameday_abs_change"),
    }
    monthly, top = timing_tables(zdf)
    res["high_share_by_month"] = save_result(monthly, "q1_high_share_by_month")
    res["top_market_days"] = save_result(top, "q1_top_market_days")

    log_run(spark, "q1_volume_analysis",
            inputs=["features", "prices_clean"],
            outputs=[k for k in res], rows=int(res["coverage"].iloc[2]["n"]),
            note=f"Z_WINDOW={Z_WINDOW}, EDGES={EDGES}, RECENT_DAYS={RECENT_DAYS}")
    zdf.unpersist()
    j.unpersist()
    wk.unpersist()
    return res


if __name__ == "__main__":
    out = run_q1()
    for name, pdf in out.items():
        print(f"\n=== {name} ===")
        print(pdf.to_string(index=False))