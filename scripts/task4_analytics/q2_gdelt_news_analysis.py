"""
scripts/task4_analytics/q2_gdelt_news_analysis.py  --  Task 4 (Analytics & Visualisation)

Q2 (news): From July 2015 to June 2017, are weeks with high GDELT event volume
or negative tone associated with different MARKET-WIDE volatility or volume?

GDELT has no ticker field, so everything is market wide: one row per trading
week. Market measures come from the cleaned daily prices of all series.

Weekly market measures (computed from prices_clean, distributed in PySpark)
  market_vol    : std-dev of the equal-weighted daily market return in the week
  mean_abs_ret  : mean over series-days of |daily return| (breadth of movement)
  volume_z      : mean over series-days of the per-series volume z-score
                  (log1p volume vs the series' own previous 60 days; Q1 helper)

GDELT signals (aggregated here from the Hive table gdelt_daily, DATEADDED day)
  events_level  : mean events per day in the week
  events_rel    : events / mean of the previous 8 weeks (removes slow drift in
                  archive size; needs >= 4 prior weeks)
  tone          : event-weighted average tone (low tercile = most negative)

Two timings are reported and kept separate:
  lag 0 : GDELT week t vs market week t   -> contemporaneous ASSOCIATION only
          (GDELT for week t is complete only after the week ends)
  lag 1 : GDELT week t vs market week t+1 -> the timing is available at
          prediction time (the tercile grouping itself is descriptive, see below)

Week alignment: both sources are mapped to the Monday-start week of their date,
so every GDELT day and every price date fall in the same calendar week key. If the team's calendar defines weeks differently,
change `to_week_key()` only.

Outputs (HDFS Parquet under RESULTS_DIR + CSV under TABLE_DIR):
  q2_coverage, q2_weekly_panel, q2_group_summary, q2_high_vs_low, q2_spearman
  and the same five tables with the suffix _robust, from a second run that leaves
  out every series-day with a close-to-close move above 50% (six such moves were kept
  in the cleaned prices and could shift an equal-weighted daily average).

Notebook use:   from q2_gdelt_news_analysis import run_q2;  res = run_q2(spark)

Interpretation rule: ~100 weekly observations, strongly autocorrelated, with a
few market-wide shocks (name a date only if it appears in q2_weekly_panel or
q1_top_market_days). Results are associations in a short window, not causal
effects. q2_spearman holds 72 tests (3 signals x 3 outcomes x 2 lags x 4 periods),
so some will look significant by chance: report them as exploratory. Confidence
intervals and t-statistics are indicative and too narrow, and the tercile cut-offs
use the whole window, so they describe the data and are not a prediction rule.
"""
import os
import sys

if "__file__" in globals():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import Window as W
from pyspark.sql import functions as F

from analytics_config import (COLS, GDELT_SPLITS, get_spark, load_table,
                              log_run, save_result)
from q1_volume_analysis import build_volume_z

# Parameters (fixed a priori)
GDELT_START = GDELT_SPLITS["train"][0] # 2015-07-01
GDELT_END = GDELT_SPLITS["test"][1] # 2017-06-30
MAX_RETURN_GAP_DAYS = 5 # daily return only if previous bar is this close
MIN_DAYS_PER_WEEK = 3
MIN_GDELT_DAYS = 4 # GDELT days needed for a usable week
EXTREME_MOVE = 0.5 # robustness run drops series-days with |daily move| above this
REL_WINDOW, REL_MIN = 8, 4 # trailing weeks for events_rel
SIGNALS = ["events_level", "events_rel", "tone"]
OUTCOMES = ["market_vol", "mean_abs_ret", "volume_z"]
GROUPS = ["1_low", "2_mid", "3_high"]

SYM, DT = COLS["symbol"], COLS["date"]


def to_week_key(col):
    """Monday-start calendar week of a date column (single alignment rule)."""
    return F.to_date(F.date_trunc("week", F.to_date(col)))


# Market-wide weekly measures from prices
def build_market_weekly(prices, min_series, max_abs_ret=None):
    p = (prices.select(F.col(SYM).alias("symbol"),
                       F.to_date(F.col(DT)).alias("d"),
                       F.col(COLS["close"]).cast("double").alias("close"))
         .where(F.col("close") > 0))
    w = W.partitionBy("symbol").orderBy("d")
    p = (p.withColumn("prev_close", F.lag("close").over(w))
          .withColumn("prev_d", F.lag("d").over(w))
          .where(F.col("prev_close").isNotNull()
                 & (F.datediff("d", "prev_d") <= MAX_RETURN_GAP_DAYS))
          .withColumn("ret", F.col("close") / F.col("prev_close") - 1))
    if max_abs_ret is not None:  # robustness run: leave out extreme series-days
        p = p.where(F.abs("ret") <= max_abs_ret)

    daily = (p.groupBy("d")
             .agg(F.count("*").alias("n_series"),
                  F.avg("ret").alias("mkt_ret"),
                  F.avg(F.abs("ret")).alias("mean_abs_ret"))
             .where(F.col("n_series") >= min_series))

    zday = (build_volume_z(prices).where(F.col("z_same_day").isNotNull())
            .groupBy("d").agg(F.avg("z_same_day").alias("mkt_vol_z")))
    daily = daily.join(zday, "d", "left")

    return (daily.withColumn("week_key", to_week_key(F.col("d")))
            .groupBy("week_key")
            .agg(F.count("*").alias("n_days"),
                 F.stddev("mkt_ret").alias("market_vol"),
                 F.avg("mean_abs_ret").alias("mean_abs_ret"),
                 F.avg("mkt_vol_z").alias("volume_z"))
            .where(F.col("n_days") >= MIN_DAYS_PER_WEEK))


# GDELT weekly signals
def build_gdelt_weekly(gdelt):
    """Aggregate the team's Hive table gdelt_daily to Monday-start weeks.

    events_level is the MEAN events per day in the week (not the sum), so the
    partial first/last weeks of the archive are not biased; weeks with fewer
    than MIN_GDELT_DAYS days are dropped. tone is the event-weighted mean of
    avg_tone."""
    day = F.coalesce(F.to_date(F.col(COLS["gd_day"]).cast("string"), "yyyy-MM-dd"),
                     F.to_date(F.col(COLS["gd_day"]).cast("string"), "yyyyMMdd"))
    g = (gdelt.select(day.alias("day"),
                      F.col(COLS["gd_events"]).cast("double").alias("ev"),
                      F.col(COLS["gd_tone"]).cast("double").alias("tn"))
         .where(F.col("day").isNotNull() & (F.col("ev") > 0))
         .withColumn("week_key", to_week_key(F.col("day")))
         .groupBy("week_key")
         .agg(F.count("*").alias("n_gdelt_days"),
              F.avg("ev").alias("events_level"),
              (F.sum(F.col("tn") * F.col("ev")) / F.sum("ev")).alias("tone"))
         .where(F.col("n_gdelt_days") >= MIN_GDELT_DAYS))
    w = W.orderBy("week_key").rowsBetween(-REL_WINDOW, -1)
    g = (g.withColumn("prior_avg", F.avg("events_level").over(w))
          .withColumn("prior_n", F.count("events_level").over(w))
          .withColumn("events_rel",
                      F.when(F.col("prior_n") >= REL_MIN,
                             F.col("events_level") / F.col("prior_avg")))
          .drop("prior_avg", "prior_n"))
    return g


def period_expr():
    (a0, a1), (b0, b1), (c0, c1) = (GDELT_SPLITS["train"], GDELT_SPLITS["val"],
                                    GDELT_SPLITS["test"])
    d = F.col("week_key")
    return (F.when(d.between(a0, a1), "train_2015H2")
             .when(d.between(b0, b1), "val_2016")
             .when(d.between(c0, c1), "test_2017H1")
             .otherwise("outside"))


def build_panel(gdelt_w, market_w):
    """One row per week in the study window with signals and outcomes (lag 0/1)."""
    win = F.col("week_key").between(GDELT_START, GDELT_END)
    panel = (gdelt_w.where(win)
             .join(market_w, "week_key", "inner")
             .withColumn("period", period_expr()))
    w = W.orderBy("week_key")
    nxt_ok = F.datediff(F.lead("week_key", 1).over(w), F.col("week_key")) == 7
    for o in OUTCOMES:
        panel = panel.withColumn(f"{o}_next",
                                 F.when(nxt_ok, F.lead(o, 1).over(w)))
    return panel


def tercile_groups(panel):
    """Add g_<signal> = low/mid/high using study-window tercile cut-offs.
    Cut-offs use the whole window: descriptive, NOT a prediction-time rule."""
    aggs = [F.expr(f"percentile_approx({s}, array(0.3333, 0.6667))").alias(s)
            for s in SIGNALS]
    cuts = panel.agg(*aggs).first()
    for s in SIGNALS:
        c1, c2 = cuts[s]
        panel = panel.withColumn(
            f"g_{s}",
            F.when(F.col(s).isNull(), F.lit(None).cast("string"))
             .when(F.col(s) <= c1, GROUPS[0])
             .when(F.col(s) <= c2, GROUPS[1])
             .otherwise(GROUPS[2]))
    return panel


# Long-format helpers (stack keeps everything in one small Spark job)
def signal_long(panel, with_group=True):
    items = ", ".join(f"'{s}', {s}" for s in SIGNALS)
    x = F.expr(f"stack({len(SIGNALS)}, {items}) as (signal, x)")
    out = panel.select("week_key", "period", x).where(F.col("x").isNotNull())
    if with_group:
        gitems = ", ".join(f"'{s}', g_{s}" for s in SIGNALS)
        g = panel.select("week_key",
                         F.expr(f"stack({len(SIGNALS)}, {gitems}) as (signal, grp)"))
        out = out.join(g, ["week_key", "signal"]).where(F.col("grp").isNotNull())
    return out


def outcome_long(panel):
    parts = []
    for o in OUTCOMES:
        parts += [f"'{o}', 0, {o}", f"'{o}', 1, {o}_next"]
    y = F.expr(f"stack({len(parts)}, {', '.join(parts)}) as (outcome, lag, y)")
    return panel.select("week_key", y).where(F.col("y").isNotNull())


def group_summary(panel):
    j = signal_long(panel).join(outcome_long(panel), "week_key")
    return (j.groupBy("signal", "outcome", "lag", "grp")
            .agg(F.count("*").alias("n_weeks"),
                 F.avg("y").alias("mean_y"),
                 F.stddev("y").alias("sd_y"),
                 F.expr("percentile_approx(y, 0.5)").alias("median_y"),
                 F.avg("x").alias("mean_signal"))
            .orderBy("signal", "outcome", "lag", "grp"))


def high_vs_low(summary):
    def pick(col, g):
        return F.max(F.when(F.col("grp") == g, F.col(col)))
    out = (summary.groupBy("signal", "outcome", "lag")
           .agg(pick("n_weeks", "3_high").alias("n_high"),
                pick("n_weeks", "1_low").alias("n_low"),
                pick("mean_y", "3_high").alias("mean_high"),
                pick("mean_y", "1_low").alias("mean_low"),
                pick("sd_y", "3_high").alias("sd_high"),
                pick("sd_y", "1_low").alias("sd_low")))
    out = (out.withColumn("diff_high_minus_low", F.col("mean_high") - F.col("mean_low"))
              .withColumn("welch_t_indicative",
                          F.col("diff_high_minus_low") /
                          F.sqrt(F.pow("sd_high", 2) / F.col("n_high")
                                 + F.pow("sd_low", 2) / F.col("n_low"))))
    return out.orderBy("signal", "outcome", "lag")


def spearman_table(panel):
    j = signal_long(panel, with_group=False).join(outcome_long(panel), "week_key")
    j = j.unionByName(j.withColumn("period", F.lit("all")))
    keys = ["period", "signal", "outcome", "lag"]
    j = (j.withColumn("rx", F.rank().over(W.partitionBy(*keys).orderBy("x")))
          .withColumn("ry", F.rank().over(W.partitionBy(*keys).orderBy("y"))))
    out = (j.groupBy(*keys)
           .agg(F.count("*").alias("n_weeks"),
                F.corr("rx", "ry").alias("spearman_rho"))
           .withColumn("n_tests_in_table",
                       F.lit(len(SIGNALS) * len(OUTCOMES) * 2 * 4)))   # lags x periods
    ok = (F.col("n_weeks") > 3) & (F.abs("spearman_rho") < 1)
    out = (out.withColumn("ci95_lo", F.when(ok, F.expr(
                "tanh(atanh(spearman_rho) - 1.96 / sqrt(n_weeks - 3))")))
              .withColumn("ci95_hi", F.when(ok, F.expr(
                "tanh(atanh(spearman_rho) + 1.96 / sqrt(n_weeks - 3))"))))
    return out.orderBy("period", "signal", "outcome", "lag")


def run_q2(spark=None, min_series=100, max_abs_ret=None, tag=""):
    """min_series: minimum series with a valid return for a day to count
    (default 100 of the 505 series; lower it only for small test samples).
    max_abs_ret / tag: robustness run, e.g. max_abs_ret=0.5, tag="_robust"
    leaves out series-days with a move above 50% and saves q2_*_robust tables."""
    spark = spark or get_spark("q2_gdelt_news_analysis")
    prices = load_table(spark, "prices_clean")
    gdelt = load_table(spark, "gdelt_daily")

    market_w = build_market_weekly(prices, min_series, max_abs_ret).cache()
    gdelt_w = build_gdelt_weekly(gdelt).cache()
    panel = tercile_groups(build_panel(gdelt_w, market_w)).cache()

    win = F.col("week_key").between(GDELT_START, GDELT_END)
    cov = spark.createDataFrame(
        [("GDELT weeks in study window", gdelt_w.where(win).count()),
         ("market weeks in study window", market_w.where(win).count()),
         ("weeks matched (panel rows)", panel.count()),
         ("matched weeks with events_rel", panel.where(F.col("events_rel").isNotNull()).count()),
         ("matched weeks with next-week outcome",
          panel.where(F.col("market_vol_next").isNotNull()).count())],
        "item string, n long")

    panel_out = panel.select(
        "week_key", "period", "n_days", *SIGNALS, *OUTCOMES,
        *[f"{o}_next" for o in OUTCOMES], *[f"g_{s}" for s in SIGNALS]
    ).orderBy("week_key")

    summary = group_summary(panel)
    res = {
        "coverage": save_result(cov, f"q2_coverage{tag}"),
        "weekly_panel": save_result(panel_out, f"q2_weekly_panel{tag}"),
        "group_summary": save_result(summary, f"q2_group_summary{tag}"),
        "high_vs_low": save_result(high_vs_low(summary), f"q2_high_vs_low{tag}"),
        "spearman": save_result(spearman_table(panel), f"q2_spearman{tag}"),
    }
    log_run(spark, f"q2_gdelt_news_analysis{tag}",
            inputs=["prices_clean", "gdelt_daily"], outputs=list(res),
            rows=int(res["coverage"].iloc[2]["n"]),
            note=(f"window={GDELT_START}..{GDELT_END}, min_series={min_series}, "
                  f"max_abs_ret={max_abs_ret}"))
    for df in (market_w, gdelt_w, panel):
        df.unpersist()
    return res


if __name__ == "__main__":
    out = run_q2()
    run_q2(max_abs_ret=EXTREME_MOVE, tag="_robust")   # same analysis without the extreme moves
    for name, pdf in out.items():
        print(f"\n=== {name} ===")
        print(pdf.head(30).to_string(index=False))