"""
scripts/task4_analytics/analytics_config.py  --  Task 4 (Analytics & Visualisation)
INF2006 Big Data Project, Team 18

Single place where every Task 4 script gets paths, table names, column names and helpers. 
Aligned with INF2006_team18_Notebook.ipynb (Part 1 data preparation, Part 2 modelling):

    Hive database     HIVE_DB = 'bigdata'   (or e.g. 'bigdata_run1'; set PROJ_HIVE_DB)
    Prepared tables   train, val, test          one row per Name per week (known target)
                      clean_prices              cleaned daily bars (Name, date, OHLCV)
                      gdelt_daily               day, events, avg_tone, neg_tone_share, ...
                      trading_weeks             calendar built in Hive
    Model outputs     pred_final_test           test rows, final model (model_id)
                      pred_gdelt_test           test rows in the GDELT window, price-only
                                                and price+GDELT side by side
    Split values      'train' | 'validation' | 'test'

Tables are read from Hive (HIVE_DB) first; if a table is not in Hive, from
Parquet under PROJ_TABLE_ROOT/<table> (the notebook's OUT_S3 layout), so the
scripts also run on the S3 copies. Prediction tables honour PROJ_OUT_SUFFIX
(the notebook's OUT_SUFFIX).

Two virtual tables are built here from the real ones:
    features     = train UNION val UNION test
    predictions  = long format, one row per (Name, date, model):
                   models 'full_price' (final model id), 'gdelt_price_only',
                   'gdelt_price_gdelt', plus two training-majority baselines
                   (probability = training share of up weeks), all on test rows:
                     'baseline'        learned on ALL training weeks, for 'full_price'
                     'baseline_gdelt'  learned on the training weeks inside the GDELT
                                       window, for the two GDELT models (this is the
                                       baseline the modelling step used for them)

Nothing here collects full data to the driver: results larger than MAX_PLOT_ROWS are refused. 
No random numbers are used anywhere in Task 4, so there is no seed to report for this stage.
"""
import json
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F

# 1. Locations (override with environment variables; never hard-code secrets)
HIVE_DB = os.environ.get("PROJ_HIVE_DB", "bigdata")
HDFS_BASE = os.environ.get("PROJ_HDFS_BASE", "hdfs:///project")
TABLE_ROOT = os.environ.get("PROJ_TABLE_ROOT", f"{HDFS_BASE}/tables")
MODEL_ROOT = os.environ.get("PROJ_MODEL_ROOT", f"{HDFS_BASE}/models")  # saved PipelineModels
OUT_SUFFIX = os.environ.get("PROJ_OUT_SUFFIX", "")

RESULTS_DIR = os.environ.get("PROJ_RESULTS_DIR", f"{HDFS_BASE}/analytics/results")
# Optional durable copy of every result table, e.g. s3://<your-bucket>/analytics/results.
# HDFS and the primary node's disk are erased when the cluster is terminated.
S3_RESULTS = os.environ.get("PROJ_S3_RESULTS", "").rstrip("/")
LOCAL_OUT = os.environ.get("PROJ_LOCAL_OUT", "analytics_output")
FIG_DIR = os.path.join(LOCAL_OUT, "figures")
TABLE_DIR = os.path.join(LOCAL_OUT, "tables")        # small CSV copies of results
RUN_LOG = os.path.join(LOCAL_OUT, "analytics_run_log.jsonl")

MAX_PLOT_ROWS = int(os.environ.get("PROJ_MAX_PLOT_ROWS", "50000"))
# One id per process, written on every run-log line (set PROJ_RUN_ID to pin it).
RUN_ID = os.environ.get("PROJ_RUN_ID",
                        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

# 2. Physical tables (name in HIVE_DB; Parquet fallback TABLE_ROOT/<name>)
PHYSICAL = {
    "train": "train", "val": "val", "test": "test",
    "prices_clean": "clean_prices",
    "gdelt_daily": "gdelt_daily",
    "calendar": "trading_weeks",
    "weekly_all": "weekly_all",
    "pred_final": f"pred_final_test{OUT_SUFFIX}",
    "pred_gdelt": f"pred_gdelt_test{OUT_SUFFIX}",
}

# 3. Column names (as in the notebook)
COLS = {
    "symbol": "Name",
    "date": "date",  # anchor date (last trading day on/before Friday)
    "split": "split",
    "label": "label",  # 1 = next-anchor open > this anchor open
    "next_ret": "pchg_next",  # open_next / open - 1 (never a feature)
    "volume": "volume",
    "close": "close",
    "vol": "volatility_20d",  # feature in the prepared tables
    # virtual long predictions table (built below)
    "pred": "prediction",
    "prob": "probability",  # P(label = 1)
    "model": "model_name",
    # gdelt_daily
    "gd_day": "day",
    "gd_events": "events",
    "gd_tone": "avg_tone",
    "gd_neg_share": "neg_tone_share",
}
# pred_gdelt_test columns
PRED_GDELT = {
    "pred_p": "pred_price_only", "prob_p": "prob_up_price_only",
    "pred_g": "pred_price_gdelt", "prob_g": "prob_up_price_gdelt",
}
GDELT_FEATURES = ["g_events_7d", "g_events_chg", "g_tone_7d",
                  "g_neg_share_7d", "g_conflict_share_7d"]

REQUIRED = {
    "features": ["symbol", "date", "split", "label", "next_ret", "vol"],
    "prices_clean": ["symbol", "date", "volume", "close"],
    "gdelt_daily": ["gd_day", "gd_events", "gd_tone"],
    "predictions": ["symbol", "date", "split", "label", "pred", "prob", "model"],
}

BASELINE_NAME = "baseline"  # all training weeks, for the full-price model
BASELINE_GDELT_NAME = "baseline_gdelt"  # training weeks in the GDELT window, for the GDELT models
BASELINES = (BASELINE_NAME, BASELINE_GDELT_NAME)
GDELT_MODEL_PREFIX = "gdelt_"

# 97.5th percentile of Student's t for 1..30 degrees of freedom. 
# A group of about 13 to 26 weeks is too few for the normal value 1.96, so week-clustered intervals use these.
T975 = {1: 12.7062, 2: 4.3027, 3: 3.1824, 4: 2.7764, 5: 2.5706, 6: 2.4469, 7: 2.3646,
        8: 2.306, 9: 2.2622, 10: 2.2281, 11: 2.201, 12: 2.1788, 13: 2.1604, 14: 2.1448,
        15: 2.1314, 16: 2.1199, 17: 2.1098, 18: 2.1009, 19: 2.093, 20: 2.086,
        21: 2.0796, 22: 2.0739, 23: 2.0687, 24: 2.0639, 25: 2.0595, 26: 2.0555,
        27: 2.0518, 28: 2.0484, 29: 2.0452, 30: 2.0423}


def baseline_for(model_name):
    """Name of the baseline a model is compared with (the one Task 3 used for it)."""
    return BASELINE_GDELT_NAME if str(model_name).startswith(GDELT_MODEL_PREFIX) else BASELINE_NAME


def t_crit(n_weeks_col):
    """Spark column: t critical value (95%, two-sided) for n_weeks - 1 degrees of
    freedom. Falls back to 1.96 above 30 degrees of freedom."""
    df = n_weeks_col - 1
    expr = F.lit(1.96)
    for k in sorted(T975, reverse=True):
        expr = F.when(df == k, F.lit(T975[k])).otherwise(expr)
    return expr
SPLITS = {"train": ("2013-01-01", "2015-12-31"),
          "validation": ("2016-01-01", "2016-12-31"),
          "test": ("2017-01-01", "2017-12-31")}
# GDELT experiment windows (report Section 1.4)
GDELT_SPLITS = {"train": ("2015-07-01", "2015-12-31"),
                "val": ("2016-01-01", "2016-12-31"),
                "test": ("2017-01-01", "2017-06-30")}


# 4. Spark / data access helpers
def get_spark(app_name="inf2006_analytics"):
    """Reuse the EMR notebook's session if present; else build one with Hive."""
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName(app_name)
            .enableHiveSupport().getOrCreate())


def table_exists(spark, full_name):
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False


def _read_physical(spark, key):
    name = PHYSICAL[key]
    full = f"{HIVE_DB}.{name}"
    if table_exists(spark, full):
        print(f"[load_table] {key} <- hive:{full}")
        return spark.table(full)
    path = f"{TABLE_ROOT}/{name}"
    print(f"[load_table] {key} <- parquet:{path}")
    return spark.read.parquet(path)


def _build_features(spark):
    return (_read_physical(spark, "train")
            .unionByName(_read_physical(spark, "val"))
            .unionByName(_read_physical(spark, "test")))


def _gdelt_window_train(spark):
    """Training rows inside the GDELT window: the rows the modelling step used to
    learn the baseline for the two GDELT models (notebook, run_experiment)."""
    tr = _read_physical(spark, "train")
    if "in_gdelt_window" in tr.columns:
        return tr.where(F.col("in_gdelt_window").cast("boolean"))
    lo, hi = GDELT_SPLITS["train"]
    print("[load_table] no in_gdelt_window column: using the GDELT training dates "
          f"{lo} to {hi} for the GDELT baseline")
    return tr.where(F.to_date(F.col(COLS["date"])).between(lo, hi))


def _build_predictions(spark):
    """Long predictions table incl. the two training-majority baselines."""
    S, D, L = COLS["symbol"], COLS["date"], COLS["label"]
    out = [S, D, COLS["split"], L, COLS["pred"], COLS["prob"], COLS["model"]]
    final = _read_physical(spark, "pred_final")
    gd = _read_physical(spark, "pred_gdelt")
    share = float(_read_physical(spark, "train").agg(F.avg(L)).first()[0])
    majority = 1.0 if share >= 0.5 else 0.0
    share_g = float(_gdelt_window_train(spark).agg(F.avg(L)).first()[0])
    majority_g = 1.0 if share_g >= 0.5 else 0.0

    a = final.select(
        F.col(S), F.col(D), F.col(COLS["split"]), F.col(L).cast("double").alias(L),
        F.col("prediction").cast("double").alias(COLS["pred"]),
        F.col("prob_up").cast("double").alias(COLS["prob"]),
        F.col("model_id").alias(COLS["model"]))
    parts = [a]
    for tag, pc, qc in (("gdelt_price_only", PRED_GDELT["pred_p"], PRED_GDELT["prob_p"]),
                        ("gdelt_price_gdelt", PRED_GDELT["pred_g"], PRED_GDELT["prob_g"])):
        parts.append(gd.select(
            F.col(S), F.col(D), F.lit("test").alias(COLS["split"]),
            F.col(L).cast("double").alias(L),
            F.col(pc).cast("double").alias(COLS["pred"]),
            F.col(qc).cast("double").alias(COLS["prob"]),
            F.lit(tag).alias(COLS["model"])))
    base = a.select(
        S, D, COLS["split"], L,
        F.lit(majority).alias(COLS["pred"]), F.lit(share).alias(COLS["prob"]),
        F.lit(BASELINE_NAME).alias(COLS["model"])).distinct()
    base_g = gd.select(
        F.col(S), F.col(D), F.lit("test").alias(COLS["split"]),
        F.col(L).cast("double").alias(L),
        F.lit(majority_g).alias(COLS["pred"]), F.lit(share_g).alias(COLS["prob"]),
        F.lit(BASELINE_GDELT_NAME).alias(COLS["model"])).distinct()
    parts += [base, base_g]
    res = parts[0]
    for p in parts[1:]:
        res = res.unionByName(p.select(*out))
    print(f"[load_table] predictions built: baseline majority={majority}, "
          f"train share up={share:.4f} | baseline_gdelt majority={majority_g}, "
          f"GDELT-window train share up={share_g:.4f}")
    return res.select(*out)


BUILDERS = {"features": _build_features, "predictions": _build_predictions}


def load_table(spark, key):
    """Load a project table by logical key (physical, or virtual builder)."""
    if key in BUILDERS:
        df = BUILDERS[key](spark)
    elif key in PHYSICAL:
        df = _read_physical(spark, key)
    else:
        raise KeyError(f"Unknown table key '{key}'. "
                       f"Known: {sorted(set(PHYSICAL) | set(BUILDERS))}")
    if key in REQUIRED:
        check_schema(df, key)
    return df


def check_schema(df, key):
    need = [COLS[c] for c in REQUIRED[key]]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Table '{key}' is missing columns {missing}. "
                         f"Found {df.columns}. Edit COLS in analytics_config.py.")


def register_views(spark):
    """Expose tables as temp views q_<key> for Spark SQL / Hive-style queries."""
    for key in list(PHYSICAL) + list(BUILDERS):
        try:
            load_table(spark, key).createOrReplaceTempView(f"q_{key}")
        except Exception as e:
            print(f"[register_views] skipped {key}: {e}")


# 5. Output helpers (results stay distributed until aggregated and small)
def save_result(df, name, to_pandas=True):
    """Write an aggregated result to HDFS (Parquet); return a pandas copy for
    plotting and save a CSV under TABLE_DIR. Refuses oversized collects."""
    path = f"{RESULTS_DIR}/{name}"
    df.write.mode("overwrite").parquet(path)
    n = df.sparkSession.read.parquet(path).count()
    outputs = [path]
    if S3_RESULTS:
        s3_path = f"{S3_RESULTS}/{name}"
        try:
            df.sparkSession.read.parquet(path).write.mode("overwrite").parquet(s3_path)
            outputs.append(s3_path)
        except Exception as e:   # a missing S3 copy must not stop the analysis
            print(f"[save_result] WARNING: S3 copy of {name} failed: {e}")
    log_run(df.sparkSession, name, outputs=outputs, rows=n)
    if not to_pandas:
        return None
    require_pandas()
    if n > MAX_PLOT_ROWS:
        raise ValueError(f"Result '{name}' has {n} rows > MAX_PLOT_ROWS "
                         f"({MAX_PLOT_ROWS}); aggregate further before plotting.")
    pdf = df.sparkSession.read.parquet(path).toPandas()
    os.makedirs(TABLE_DIR, exist_ok=True)
    pdf.to_csv(os.path.join(TABLE_DIR, f"{name}.csv"), index=False)
    return pdf


EPS = 1e-15


def logloss(p, y):
    """Per-row log-loss as a Spark column (p = P(up), y = 0/1), clipped at EPS."""
    q = F.least(F.greatest(p, F.lit(EPS)), F.lit(1 - EPS))
    return -(y * F.log(q) + (1 - y) * F.log(1 - q))


def require_pandas():
    """Fail early with the fix if pandas cannot be imported on this node. The EMR
    image has pandas but not all of its dependencies (see README, step 2)."""
    try:
        import pandas  # noqa: F401
    except ImportError as e:
        raise ImportError(
            f"pandas cannot be imported on this node ({e}). Run check_env.py and "
            "follow the printed pip line. Keep numpy at 1.26.4.") from e


def save_figure(fig, name, dpi=150):
    os.makedirs(FIG_DIR, exist_ok=True)
    path = os.path.join(FIG_DIR, f"{name}.png")
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    print(f"[save_figure] {path}")
    return path


def log_run(spark, step, inputs=None, outputs=None, rows=None, note=""):
    """Append one JSON line of execution evidence (for report Appendix B)."""
    os.makedirs(LOCAL_OUT, exist_ok=True)
    rec = {
        "run_id": RUN_ID,
        "utc_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "step": step,
        "spark_app_id": spark.sparkContext.applicationId,
        "spark_version": spark.version,
        "hive_db": HIVE_DB,
        "inputs": inputs or [],
        "outputs": outputs or [],
        "rows": rows,
        "note": note,
    }
    with open(RUN_LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return rec


__all__ = [n for n in dir() if not n.startswith("_")] + ["_read_physical"]