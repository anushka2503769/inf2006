#!/usr/bin/env python3
"""
reload_check.py - fresh-session check that a saved PipelineModel reproduces its predictions WITHOUT retraining.

Run in a NEW Spark application (not inside the training notebook), for example on the EMR primary node:

    spark-submit scripts/reload_check.py --emr

It loads the saved PipelineModel, scores the test table and compares the result with the prediction
table the training notebook saved. It never calls fit().

Local dry run (after running the notebook with ON_EMR = False):

    spark-submit reload_check.py

Options
    --emr            read Hive tables from the cluster (default: local files)
    --model NAME     which saved model to check (default: the final model named in the saved prediction table)
    --suffix TEXT    the OUT_SUFFIX used by the training run (default: empty)
    --db NAME        the Hive database of the prepared and prediction tables (default: bigdata; HIVE_DB in the notebook)
    --tol NUMBER     numerical tolerance for probabilities (default: 1e-9)
    --out PATH       where to write the result (default: evidence/reload_check.json); use another name for
                     the negative test, so the passing record is not overwritten

Result: PASS only if the model loads, every test row is matched, predictions are identical and the
largest probability difference is within the tolerance. A missing or incompatible model file is reported
as FAIL with the reason. The result is printed and written to the --out file (exit code 0 = PASS, 1 = FAIL).
"""
import argparse
import json
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

ap = argparse.ArgumentParser()
ap.add_argument("--emr", action="store_true")
ap.add_argument("--model", default=None)
ap.add_argument("--suffix", default="")
ap.add_argument("--db", default="bigdata")
ap.add_argument("--tol", type=float, default=1e-9)
ap.add_argument("--out", default="evidence/reload_check.json")
args = ap.parse_args()

MODEL_ROOT = "hdfs:///project/models" if args.emr else "models"
LOCAL_DIR, LOCAL_OUT = "local_tables", "local_out"
SAVED_PRED = f"pred_final_test{args.suffix}"  # written by the training notebook for the final model

# Same columns and types as the prepared tables (used for the local CSV export)
SCHEMA = T.StructType([
    T.StructField("Name", T.StringType()), T.StructField("date", T.DateType()),
    T.StructField("week_friday", T.DateType()), T.StructField("open", T.DoubleType()),
    T.StructField("open_next_fri", T.DoubleType()), T.StructField("label", T.IntegerType()),
    T.StructField("pchg_next", T.DoubleType()), T.StructField("split", T.StringType()),
    T.StructField("next_anchor_date", T.DateType()), T.StructField("in_gdelt_window", T.BooleanType()),
    T.StructField("pchg_1d", T.DoubleType()), T.StructField("pchg_5d", T.DoubleType()),
    T.StructField("pchg_20d", T.DoubleType()), T.StructField("vol_ratio_5_20", T.DoubleType()),
    T.StructField("volatility_20d", T.DoubleType()), T.StructField("range_prev", T.DoubleType()),
    T.StructField("g_events_7d", T.LongType()), T.StructField("g_events_chg", T.DoubleType()),
    T.StructField("g_tone_7d", T.DoubleType()), T.StructField("g_neg_share_7d", T.DoubleType()),
    T.StructField("g_conflict_share_7d", T.DoubleType())])

builder = SparkSession.builder.appName("reload_check")
if args.emr:  # without Hive support, spark-submit cannot see the Hive tables the notebook saved
    builder = builder.enableHiveSupport()
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

result = {"check": "saved PipelineModel reproduces its predictions without retraining",
          "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          "spark_version": spark.version, "application_id": spark.sparkContext.applicationId,
          "saved_predictions": SAVED_PRED, "tolerance": args.tol,
          "retrained": False}


def finish(status, reason=None):
    result["status"], result["reason"] = status, reason
    text = json.dumps(result, indent=2, default=str)
    print(text)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        f.write(text)
    spark.stop()
    sys.exit(0 if status == "PASS" else 1)


# 1. Read the test rows and the predictions saved by the training run
try:
    if args.emr:
        test = spark.table(f"{args.db}.test")
        saved = spark.table(f"{args.db}.{SAVED_PRED}")
    else:  # local CSV export of the test table and the parquet written by the notebook
        test = spark.read.option("header", True).option("mode", "FAILFAST").schema(SCHEMA).csv(f"{LOCAL_DIR}/test")
        saved = spark.read.parquet(f"{LOCAL_OUT}/{SAVED_PRED}")
except Exception as e:  # noqa: BLE001
    finish("FAIL", f"input tables could not be read: {type(e).__name__}: {str(e).splitlines()[0][:300]}")

try:
    model_id = args.model or saved.select("model_id").distinct().first()["model_id"]
except Exception as e:  # noqa: BLE001
    finish("FAIL", f"saved prediction table has no model_id column: {type(e).__name__}: {str(e).splitlines()[0][:300]}")
MODEL_PATH = f"{MODEL_ROOT}/{model_id}{args.suffix}"
result.update({"model_id": model_id, "model_path": MODEL_PATH})

# 2. Load the saved model (a missing or incompatible model is reported, never silently ignored)
try:
    model = PipelineModel.load(MODEL_PATH)
except Exception as e:  # noqa: BLE001 - report whatever Spark says
    finish("FAIL", f"model could not be loaded: {type(e).__name__}: {str(e).splitlines()[0][:300]}")
result["stages"] = [type(s).__name__ for s in model.stages]

# the GDELT experiments were scored only on weeks with complete GDELT features
test = test.withColumn("label", F.col("label").cast("double"))
if model_id.startswith("gdelt"):
    test = test.filter("in_gdelt_window")

# 3. Score with the loaded model and compare with the saved predictions (Spark evaluates lazily, so errors appear here)
try:
    new = (model.transform(test)
           .select("Name", "date", F.col("prediction").alias("prediction_new"),
                   vector_to_array("probability")[1].alias("prob_up_new")))
    old = saved.select("Name", "date", F.col("prediction").alias("prediction_old"),
                       F.col("prob_up").alias("prob_up_old"))
    stats = new.join(old, ["Name", "date"]).agg(
        F.count("*").alias("matched_rows"),
        F.sum((F.col("prediction_new") != F.col("prediction_old")).cast("int")).alias("prediction_mismatches"),
        F.max(F.abs(F.col("prob_up_new") - F.col("prob_up_old"))).alias("max_abs_prob_diff")).first().asDict()
    result.update({"test_rows": test.count(), "saved_rows": saved.count(), **stats})
except Exception as e:  # noqa: BLE001
    finish("FAIL", f"model is incompatible with the test table: {type(e).__name__}: {str(e).splitlines()[0][:300]}")

problems = []
if result["matched_rows"] != result["test_rows"] or result["matched_rows"] != result["saved_rows"]:
    problems.append("row counts differ: test %d, saved %d, matched %d" %
                    (result["test_rows"], result["saved_rows"], result["matched_rows"]))
if result["prediction_mismatches"]:
    problems.append("%d predictions differ" % result["prediction_mismatches"])
if result["max_abs_prob_diff"] is None or result["max_abs_prob_diff"] > args.tol:
    problems.append("largest probability difference %s exceeds the tolerance %s" %
                    (result["max_abs_prob_diff"], args.tol))
finish("FAIL" if problems else "PASS", "; ".join(problems) or None)