#!/usr/bin/env python3
"""
make_synthetic_tables.py - SYNTHETIC train / val / test tables for a local dry run of the Task 3 notebook.

The numbers are random with a very weak signal. They only test that the notebook runs end to end
(plumbing, schema, saving and reloading the model). 

Usage:   python make_synthetic_tables.py [output_dir]      (default: local_tables)
Needs:   numpy and pandas only.

The layout matches the tables written by the PySpark data-preparation notebook:
  train: weekly rows 2013-03 to 2015-12, validation: 2016, test: 2017
  GDELT features exist only from 2015-07-17 to 2017-06-30 (in_gdelt_window), and are the same for every
  stock in a week (market wide).
"""
import os
import sys

import numpy as np
import pandas as pd

out = sys.argv[1] if len(sys.argv) > 1 else "local_tables"
rng = np.random.default_rng(7)
N_STOCKS = 60

fridays = pd.date_range("2013-03-15", "2017-12-22", freq="W-FRI")
names = [f"S{i:03d}" for i in range(N_STOCKS)]

# One set of market-wide GDELT values per week
g_weeks = pd.DataFrame({"date": fridays})
g_weeks["g_events_7d"] = rng.integers(1_500_000, 2_500_000, len(fridays))
g_weeks["g_events_chg"] = rng.normal(0, 0.08, len(fridays))
g_weeks["g_tone_7d"] = rng.normal(-1.8, 0.25, len(fridays))
g_weeks["g_neg_share_7d"] = rng.uniform(0.45, 0.6, len(fridays))
g_weeks["g_conflict_share_7d"] = rng.uniform(0.18, 0.27, len(fridays))
in_window = (g_weeks["date"] >= "2015-07-17") & (g_weeks["date"] <= "2017-06-30")
for c in ["g_events_7d", "g_events_chg", "g_tone_7d", "g_neg_share_7d", "g_conflict_share_7d"]:
    g_weeks.loc[~in_window, c] = np.nan
g_weeks["in_gdelt_window"] = in_window

rows = []
for d in fridays:
    for n in names:
        rows.append((n, d))
df = pd.DataFrame(rows, columns=["Name", "date"])
m = len(df)
df["week_friday"] = df["date"]
df["open"] = rng.uniform(20, 200, m).round(2)
df["pchg_1d"] = rng.normal(0, 0.012, m)
df["pchg_5d"] = rng.normal(0, 0.025, m)
df["pchg_20d"] = rng.normal(0.01, 0.05, m)
df["vol_ratio_5_20"] = rng.lognormal(0, 0.25, m)
df["volatility_20d"] = rng.gamma(4, 0.004, m)
df["range_prev"] = rng.gamma(3, 0.006, m)

# A very weak link between the features and the next-week direction
score = 0.6 * df["pchg_20d"] / 0.05 - 0.4 * df["range_prev"] / 0.02 + rng.normal(0, 2.2, m)
df["label"] = (score + 0.45 > 0).astype(int)  # a little over half of the weeks are up
df["pchg_next"] = np.where(df["label"] == 1, rng.gamma(2, 0.01, m), -rng.gamma(2, 0.01, m))
df["open_next_fri"] = (df["open"] * (1 + df["pchg_next"])).round(2)
df["next_anchor_date"] = df["date"] + pd.Timedelta(days=7)

df = df.merge(g_weeks, on="date", how="left")
df["in_gdelt_window"] = df["in_gdelt_window"].fillna(False).astype(bool)
df["split"] = np.where(df["date"] <= "2015-12-31", "train", np.where(df["date"] <= "2016-12-31", "validation", "test"))

# Same column order as the real tables
cols = ["Name", "date", "week_friday", "open", "open_next_fri", "label", "pchg_next", "split", "next_anchor_date",
        "in_gdelt_window", "pchg_1d", "pchg_5d", "pchg_20d", "vol_ratio_5_20", "volatility_20d", "range_prev",
        "g_events_7d", "g_events_chg", "g_tone_7d", "g_neg_share_7d", "g_conflict_share_7d"]
df = df[cols]

# The last training week has no outcome inside the training period in the real tables
last = {"train": "2015-12-24", "validation": "2016-12-23", "test": "2017-12-22"}
df = df[~((df["split"] == "train") & (df["date"] > last["train"]))]

os.makedirs(out, exist_ok=True)
for name, split in [("train", "train"), ("val", "validation"), ("test", "test")]:
    part = df[df["split"] == split].copy()
    n_gdelt = int(part["in_gdelt_window"].sum())
    part["date"] = part["date"].dt.strftime("%Y-%m-%d")
    part["week_friday"] = part["week_friday"].dt.strftime("%Y-%m-%d")
    part["next_anchor_date"] = part["next_anchor_date"].dt.strftime("%Y-%m-%d")
    part["in_gdelt_window"] = part["in_gdelt_window"].map({True: "true", False: "false"})  # Spark's boolean text
    part["g_events_7d"] = part["g_events_7d"].astype("Int64")  # whole numbers, blank outside the window
    path = os.path.join(out, name)
    os.makedirs(path, exist_ok=True)
    part.to_csv(os.path.join(path, f"{name}.csv"), index=False)  # a folder with one CSV, like Spark writes
    print(f"{name:5s} {len(part):6d} rows  share_up {part['label'].mean():.3f}  "
          f"gdelt rows {n_gdelt}  ({part['date'].min()} to {part['date'].max()})")
print("SYNTHETIC data written to", out, "- for plumbing tests only")