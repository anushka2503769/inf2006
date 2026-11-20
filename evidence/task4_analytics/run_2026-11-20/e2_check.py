"""E2 check (not part of the submission): recompute one volume z-score by hand and compare with Spark."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from analytics_config import COLS, get_spark, load_table
from q1_volume_analysis import (RECENT_DAYS, RECENT_MIN, Z_MIN_OBS, Z_WINDOW,
                                build_volume_z)

ROW = 300      # use the 300th trading day of the longest series


def manual_z(lv, j):
    h = lv[max(0, j - Z_WINDOW):j]
    if len(h) < Z_MIN_OBS:
        return np.nan
    sd = np.std(h, ddof=1)
    if not sd > 0:
        return np.nan
    return (lv[j] - np.mean(h)) / sd


def manual_v5(lv, j):
    zs = [manual_z(lv, k) for k in range(max(0, j - RECENT_DAYS), j)]
    zs = [z for z in zs if not np.isnan(z)]
    return float(np.mean(zs)) if len(zs) >= RECENT_MIN else np.nan


def main():
    spark = get_spark("e2_check")
    spark.sparkContext.setLogLevel("ERROR")
    prices = load_table(spark, "prices_clean")
    base = (prices.select(F.col(COLS["symbol"]).alias("symbol"), F.to_date(F.col(COLS["date"])).alias("d"),
                          F.col(COLS["volume"]).cast("double").alias("vol"))
            .where(F.col("vol").isNotNull() & (F.col("vol") >= 0) & F.col("d").isNotNull()))
    sym = base.groupBy("symbol").count().orderBy(F.desc("count"), "symbol").first()["symbol"]
    one = base.where(F.col("symbol") == sym).orderBy("d").toPandas().reset_index(drop=True)
    lv = np.log1p(one["vol"].values)
    day = pd.Timestamp(one.loc[ROW, "d"])

    z_hand, v5_hand = manual_z(lv, ROW), manual_v5(lv, ROW)
    incl = lv[ROW - Z_WINDOW + 1:ROW + 1]
    z_wrong = (lv[ROW] - incl.mean()) / incl.std(ddof=1)

    sp = build_volume_z(prices).where(F.col("symbol") == sym).toPandas()
    sp["d"] = pd.to_datetime(sp["d"])
    r = sp[sp["d"] == day].iloc[0]

    d1, d2 = abs(r["z_same_day"] - z_hand), abs(r["vol_z_5d"] - v5_hand)
    print(f"E2 series {sym}, row {ROW}, date {day.date()}")
    print(f"E2 same-day z    by hand {z_hand:.10f}   Spark {r['z_same_day']:.10f}   difference {d1:.2e}")
    print(f"E2 5-day average by hand {v5_hand:.10f}   Spark {r['vol_z_5d']:.10f}   difference {d2:.2e}")
    print(f"E2 for contrast, z with the current day inside the window would be {z_wrong:.10f}")
    print("E2 RESULT", "PASS" if max(d1, d2) < 1e-9 and abs(z_wrong - z_hand) > 1e-6 else "FAIL")


if __name__ == "__main__":
    main()
