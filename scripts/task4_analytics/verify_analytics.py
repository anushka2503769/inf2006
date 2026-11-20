"""
scripts/task4_analytics/verify_analytics.py  --  Task 4 (Analytics & Visualisation)

Checks the Task 4 result tables against the Task 3 run summary and against the
row counts the earlier stages produced. Its output is the "Actual result" column of
the Appendix B verification records. No Spark is needed: it reads the CSV copies
in analytics_output/tables/.

    python3 scripts/task4_analytics/verify_analytics.py [path/to/model_run_summary.json]

Writes analytics_output/verification_results.csv (check, expected, actual, tolerance,
status, source) and exits with status 1 if any check fails or a table is missing.
Without a path it looks for evidence/task3_model/run_*/model_run_summary.json.

Expected values come from the Task 3 run summary where it has them, so nothing is typed
in by hand except the volatility cut-offs printed by the notebook (cell 2.5) and the
plausible range for matched weeks.

Tolerances: AUC differs a little between Spark's evaluator (which bins scores) and the
exact rank-based AUC used here, so it gets 0.002. The volatility cut-offs and the 12
quarter x volatility group sizes can move slightly because approxQuantile is approximate
(relative error 0.001), so the cut-offs get 0.00001 and each group size 30 rows.
Everything else must match exactly or to the 6 decimals the summary stores.
"""
import csv
import glob
import json
import os
import re
import sys

import pandas as pd

LOCAL_OUT = os.environ.get("PROJ_LOCAL_OUT", "analytics_output")
TABLE_DIR = os.path.join(LOCAL_OUT, "tables")
OUT_CSV = os.path.join(LOCAL_OUT, "verification_results.csv")
RUN_LOG = os.path.join(LOCAL_OUT, "analytics_run_log.jsonl")

# Printed by the notebook, cell 2.5 (17 Nov 2026 run): training terciles of volatility_20d.
VOL_CUTS = (0.010648, 0.014899)
CUT_TOL = 1e-5
GROUP_TOL = 30
AUC_TOL = 2e-3
STUDY_WEEKS = (90, 104)  # plausible matched weeks, July 2015 to June 2017

CHECKS = []


def add(check, expected, actual, tol, source, kind="equal"):
    """kind: equal (numbers within tol), range ((lo, hi) inclusive) or true (actual is bool)."""
    if actual is None:
        status = "FAIL"
    elif kind == "equal":
        status = "PASS" if abs(float(actual) - float(expected)) <= tol else "FAIL"
    elif kind == "range":
        status = "PASS" if expected[0] <= float(actual) <= expected[1] else "FAIL"
    else:
        status = "PASS" if bool(actual) else "FAIL"
    CHECKS.append({"check": check, "expected": expected, "actual": actual,
                   "tolerance": tol, "status": status, "source": source})


def table(name):
    path = os.path.join(TABLE_DIR, f"{name}.csv")
    if not os.path.exists(path):
        CHECKS.append({"check": f"table {name}.csv exists", "expected": "present", "actual": "missing",
                       "tolerance": "", "status": "FAIL", "source": path})
        return None
    return pd.read_csv(path)


def coverage_item(df, startswith):
    if df is None:
        return None
    r = df[df["item"].str.startswith(startswith)]
    return None if r.empty else int(r.iloc[0]["n"])


def find_summary():
    if len(sys.argv) > 1:
        return sys.argv[1]
    hits = sorted(glob.glob(os.path.join("evidence", "task3_model", "run_*", "model_run_summary.json")))
    return hits[-1] if hits else None


def overall(met, comp, model):
    r = met[(met["comparison"] == comp) & (met["model"] == model) & (met["split"] == "test")
            & (met["dimension"] == "overall")]
    return None if r.empty else r.iloc[0]


def main():
    sp = find_summary()
    if not sp or not os.path.exists(sp):
        print("Task 3 run summary not found. Pass its path as the first argument.")
        sys.exit(2)
    with open(sp) as f:
        s = json.load(f)
    res = s["results"]
    src = os.path.basename(sp)

    # Q1 coverage
    q1 = table("q1_coverage")
    rows_all = sum(res["full_price"]["rows"].values())
    add("Q1 feature rows equal train + val + test rows", rows_all, coverage_item(q1, "feature rows"), 0, src)
    add("Q1 rows with known target equal feature rows", rows_all, coverage_item(q1, "rows with known"), 0, src)
    scored = coverage_item(q1, "rows also with")
    add("Q1 scored rows are at most the rows with a target", True,
        None if scored is None else scored <= rows_all, "", "q1_coverage", kind="true")

    # Q2 coverage and test count
    q2 = table("q2_coverage")
    add("Q2 matched weeks inside the study window", STUDY_WEEKS, coverage_item(q2, "weeks matched"), "",
        "q2_coverage", kind="range")
    sp2 = table("q2_spearman")
    n_tests = None if sp2 is None or sp2.empty else int(sp2["n_tests_in_table"].iloc[0])
    add("Q2 Spearman table has one row per planned test", n_tests, None if sp2 is None else len(sp2), 0,
        "q2_spearman (n_tests_in_table = signals x outcomes x lags x periods)")
    for name in ("q2_high_vs_low_robust", "q2_spearman_robust"):
        table(name)       # the robustness run must exist

    # Q3 coverage, cut-offs, metrics
    q3 = table("q3_coverage")
    r = res["full_price"]["rows"]["test"]
    g = res["gdelt_price_only"]["rows"]["test"]
    add("Q3 prediction rows (3 models + 2 baselines)", r * 2 + g * 3, coverage_item(q3, "prediction rows"), 0,
        src + " (the 17 Nov run had one shared baseline: 77,174 rows)")
    add("Q3 duplicate prediction keys", 0, coverage_item(q3, "duplicate"), 0, "q3_coverage")
    add("Q3 rows paired with their baseline", r + 2 * g, coverage_item(q3, "non-baseline rows paired"), 0, src)
    add("Q3 rows dropped for missing volatility", 0, coverage_item(q3, "paired rows dropped"), 0, "q3_coverage")
    cuts = table("q3_vol_cutoffs")
    if cuts is not None:
        add("Q3 volatility cut-off 1 equals the notebook's", VOL_CUTS[0], float(cuts.iloc[0]["value"]), CUT_TOL,
            "notebook cell 2.5")
        add("Q3 volatility cut-off 2 equals the notebook's", VOL_CUTS[1], float(cuts.iloc[1]["value"]), CUT_TOL,
            "notebook cell 2.5")
    met = table("q3_metrics")
    if met is not None:
        for comp, base in (("full_price", "baseline"), ("gdelt_price_only", "baseline_gdelt"),
                           ("gdelt_price_gdelt", "baseline_gdelt")):
            exp = res[comp]
            m, b = overall(met, comp, comp), overall(met, comp, base)
            add(f"Q3 {comp}: model test accuracy", exp["model"]["test"]["accuracy"],
                None if m is None else m["accuracy"], 2e-5, src)
            add(f"Q3 {comp}: model test AUC", exp["model"]["test"]["auc"],
                None if m is None else m["auc"], AUC_TOL, src)
            add(f"Q3 {comp}: baseline test accuracy", exp["baseline"]["test"]["accuracy"],
                None if b is None else b["accuracy"], 2e-5, src)
        # the 12 quarter x volatility cells in the Task 3 reliability table
        cells = s.get("reliability_by_quarter_and_volatility", [])
        for c in cells:
            grp = f"{c['period']}|{c['vol_group']}_vol"
            rr = met[(met["comparison"] == "full_price") & (met["model"] == "full_price")
                     & (met["split"] == "test") & (met["dimension"] == "quarter_x_vol") & (met["grp"] == grp)]
            add(f"Q3 group size {grp}", c["n"], None if rr.empty else int(rr.iloc[0]["n"]), GROUP_TOL, src)

    # Q4 coverage and gain
    q4 = table("q4_coverage")
    gn = res["gdelt_price_only"]["rows"]["test"]
    add("Q4 rows in pred_gdelt_test", gn, coverage_item(q4, "rows in pred_gdelt_test"), 0, src)
    add("Q4 rows with label and both predictions", gn, coverage_item(q4, "rows with label"), 0, src)
    add("Q4 rows matched to test features", gn, coverage_item(q4, "rows matched"), 0, src)
    cmp_ = s["comparisons"]["accuracy_gain_price_plus_gdelt_vs_price_only"]
    add("Q4 distinct test weeks", cmp_["n_weeks"], coverage_item(q4, "distinct test weeks"), 0, src)
    pr = table("q4_paired")
    if pr is not None:
        o = pr[(pr["dimension"] == "overall")]
        add("Q4 overall accuracy gain equals Task 3's", cmp_["diff"],
            None if o.empty else float(o.iloc[0]["acc_gain"]), 2e-5, src)
        add("Q4 overall weeks in the paired table", cmp_["n_weeks"],
            None if o.empty else int(o.iloc[0]["n_weeks"]), 0, src)
    table("q4_model_families")

    # Safe to publish: no laptop paths or cluster host names in the outputs
    bad = []
    pat = re.compile(r"[A-Za-z]:\\Users|/home/[a-z]+|ip-\d+-\d+-\d+-\d+|ec2-\d+|\.compute\.amazonaws")
    files = glob.glob(os.path.join(TABLE_DIR, "*.csv")) + ([RUN_LOG] if os.path.exists(RUN_LOG) else [])
    for fp in files:
        with open(fp, errors="ignore") as fh:
            if pat.search(fh.read()):
                bad.append(os.path.basename(fp))
    CHECKS.append({"check": "No laptop paths or host names in the tables and run log",
                   "expected": "none found", "actual": ", ".join(bad) if bad else "none found",
                   "tolerance": "", "status": "FAIL" if bad else "PASS", "source": "analytics_output"})

    os.makedirs(LOCAL_OUT, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["check", "expected", "actual", "tolerance", "status", "source"])
        w.writeheader()
        w.writerows(CHECKS)
    n_fail = sum(c["status"] == "FAIL" for c in CHECKS)
    for c in CHECKS:
        print(f"[{c['status']}] {c['check']}: expected {c['expected']}, actual {c['actual']}")
    print(f"\n{len(CHECKS) - n_fail} passed, {n_fail} failed. Saved to {OUT_CSV}")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
