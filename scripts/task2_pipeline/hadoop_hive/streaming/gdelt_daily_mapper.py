#!/usr/bin/env python3
"""
gdelt_daily_mapper.py - Hadoop Streaming mapper (in-mapper combining).

Turns GDELT 1.0 daily event files (tab-separated, despite the .CSV name;
58 columns) into per-day sums. GDELT has no stock ticker, so the features
are market-wide: one row per day for the whole world.

How it differs from the baseline (gdelt_daily_mapper_rowwise.py):
  The baseline printed one line and two counter updates for every event
  (144 million of each) and left the adding-up to the combiner. This version
  adds up inside the mapper and prints one line per day at the end, so
  Hadoop has almost nothing to sort, combine or shuffle. The output is the
  same.

Key choice - which date to use:
  SQLDATE   (column 2)  = when the event happened (can be weeks earlier)
  DATEADDED (column 57) = when GDELT published the record
We key on DATEADDED, because that is when the information became available.
Using SQLDATE could leak news into days before anyone could have seen it.

Output: <dateadded YYYY-MM-DD> TAB events TAB articles TAB tone_sum
        TAB goldstein_sum TAB neg_tone_events TAB conflict_events TAB bad_rows

Sums are kept as whole numbers (tone in millionths, Goldstein in
ten-thousandths), so the result does not depend on the order rows arrive in
or on how the files are split between mappers.
"""

import math
import sys

N_FIELDS = 58
I_GOLDSTEIN, I_NUMARTICLES, I_AVGTONE = 30, 33, 34
I_QUADCLASS, I_DATEADDED = 29, 56
REPORT_EVERY = 100000  # rows between counter updates (also tells Hadoop the task is alive)


def fixed(units, places):
    """Whole number of 10^-places units -> decimal text, e.g. (-5136162763, 6) -> '-5136.162763'."""
    sign = "-" if units < 0 else ""
    whole, frac = divmod(abs(units), 10 ** places)
    return f"{sign}{whole}.{frac:0{places}d}"


def main():
    sums = {}  # YYYYMMDD -> [events, articles, tone_millionths, goldstein_1e4, neg, conflict, bad]
    counts = {"RowsRead": 0, "RowsUsed": 0, "Bad_FieldCount": 0, "Bad_DateAdded": 0, "Bad_Number": 0}
    reported = dict.fromkeys(counts, 0)

    def report():
        for name, total in counts.items():
            delta = total - reported[name]
            if delta:
                sys.stderr.write(f"reporter:counter:GDELT,{name},{delta}\n")
                reported[name] = total
        sys.stderr.flush()

    for raw in sys.stdin:
        line = raw.rstrip("\r\n")
        if not line:
            continue
        counts["RowsRead"] += 1
        if counts["RowsRead"] % REPORT_EVERY == 0:
            report()

        f = line.split("\t")
        if len(f) != N_FIELDS:
            counts["Bad_FieldCount"] += 1
            continue

        d = f[I_DATEADDED].strip()
        if len(d) != 8 or not d.isdigit():
            counts["Bad_DateAdded"] += 1
            continue

        acc = sums.get(d)
        if acc is None:
            acc = sums[d] = [0, 0, 0, 0, 0, 0, 0]

        try:
            articles = int(f[I_NUMARTICLES] or 0)
            tone = float(f[I_AVGTONE])
            goldstein = float(f[I_GOLDSTEIN]) if f[I_GOLDSTEIN] else 0.0
            if not (math.isfinite(tone) and math.isfinite(goldstein)):
                raise ValueError("nan or inf")  # float() accepts these words; they are not usable numbers
        except ValueError:
            counts["Bad_Number"] += 1
            acc[6] += 1
            continue

        counts["RowsUsed"] += 1
        acc[0] += 1
        acc[1] += articles
        # same rounding as the baseline (6 and 4 decimal places), kept as whole numbers
        acc[2] += int(f"{tone:.6f}".replace(".", ""))
        acc[3] += int(f"{goldstein:.4f}".replace(".", ""))
        if tone < 0:
            acc[4] += 1
        if f[I_QUADCLASS].strip() == "4":  # QuadClass 4 = material conflict
            acc[5] += 1

    for d in sorted(sums):
        a = sums[d]
        print(f"{d[0:4]}-{d[4:6]}-{d[6:8]}\t{a[0]}\t{a[1]}\t{fixed(a[2], 6)}\t{fixed(a[3], 4)}"
              f"\t{a[4]}\t{a[5]}\t{a[6]}")
    report()


if __name__ == "__main__":
    main()