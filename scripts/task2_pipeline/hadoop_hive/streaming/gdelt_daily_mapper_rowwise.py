#!/usr/bin/env python3
"""
gdelt_daily_mapper_rowwise.py - Hadoop Streaming mapper, BASELINE version.

This is the mapper that ran on EMR on 9 Nov 2026 (job_1794207896977_0004,
33 min 47 s). It prints one line and two counter updates for every event and
leaves the adding-up to the combiner. It is kept only as the reference for the
performance comparison; the default mapper is gdelt_daily_mapper.py, which
adds up inside the mapper and gives the same output.
Run it with:  GDELT_MAPPER=rowwise bash run_streaming.sh

Turns GDELT 1.0 daily event files (tab-separated, despite the .CSV name;
58 columns) into per-day partial sums. GDELT has no stock ticker, so the
features are market-wide: one row per day for the whole world.

Key choice - which date to use:
  SQLDATE   (column 2)  = when the event happened (can be weeks earlier)
  DATEADDED (column 57) = when GDELT published the record
We key on DATEADDED, because that is when the information became available.
Using SQLDATE could leak news into days before anyone could have seen it.

Output: <dateadded YYYY-MM-DD> TAB events TAB articles TAB tone_sum
        TAB goldstein_sum TAB neg_tone_events TAB conflict_events TAB bad_rows
All values are sums, so the reducer can also run as a combiner.
"""

import sys

N_FIELDS = 58
I_GOLDSTEIN, I_NUMARTICLES, I_AVGTONE = 30, 33, 34
I_QUADCLASS, I_DATEADDED = 29, 56


def counter(name, n=1):
    sys.stderr.write(f"reporter:counter:GDELT,{name},{n}\n")


bad_by_day = {}

for raw in sys.stdin:
    line = raw.rstrip("\r\n")
    if not line:
        continue
    f = line.split("\t")
    counter("RowsRead")

    if len(f) != N_FIELDS:
        counter("Bad_FieldCount")
        continue

    d = f[I_DATEADDED].strip()
    if len(d) != 8 or not d.isdigit():
        counter("Bad_DateAdded")
        continue
    day = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

    try:
        articles = int(f[I_NUMARTICLES] or 0)
        tone = float(f[I_AVGTONE])
        goldstein = float(f[I_GOLDSTEIN]) if f[I_GOLDSTEIN] else 0.0
        quad = f[I_QUADCLASS].strip()
    except ValueError:
        counter("Bad_Number")
        bad_by_day[day] = bad_by_day.get(day, 0) + 1
        continue

    counter("RowsUsed")
    neg = 1 if tone < 0 else 0
    conflict = 1 if quad == "4" else 0  # QuadClass 4 = material conflict
    print(f"{day}\t1\t{articles}\t{tone:.6f}\t{goldstein:.4f}\t{neg}\t{conflict}\t0")

# Rows that had a valid date but unusable numbers are still counted per day
for day, n in bad_by_day.items():
    print(f"{day}\t0\t0\t0\t0\t0\t0\t{n}")
