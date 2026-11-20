#!/usr/bin/env python3
"""
gdelt_daily_reducer.py - Hadoop Streaming reducer.

Adds up the per-day sums from the mappers and writes one row per day.
Input and output have the same shape, so with the baseline row-by-row mapper
this file is also used as the combiner.

Output: day TAB events TAB articles TAB tone_sum TAB goldstein_sum
        TAB neg_tone_events TAB conflict_events TAB bad_rows
Averages (e.g. average tone) are computed later in Hive, from these sums.

Tone and Goldstein sums are added as whole numbers (millionths and
ten-thousandths), not as floating-point numbers, so the total is exact and
does not depend on the order the lines arrive in.
"""

import sys


def to_units(text, places):
    """Decimal text -> whole number of 10^-places units, e.g. ('-2.5', 6) -> -2500000."""
    text = text.strip()
    negative = text.startswith("-")
    if text[:1] in "+-":
        text = text[1:]
    whole, _, frac = text.partition(".")
    if not (whole or frac) or not (whole + frac).isdigit():
        raise ValueError(text)
    units = int(whole or "0") * 10 ** places + int((frac + "0" * places)[:places])
    return -units if negative else units


def fixed(units, places):
    sign = "-" if units < 0 else ""
    whole, frac = divmod(abs(units), 10 ** places)
    return f"{sign}{whole}.{frac:0{places}d}"


def emit(day, a):
    print(f"{day}\t{a[0]}\t{a[1]}\t{fixed(a[2], 6)}\t{fixed(a[3], 4)}\t{a[4]}\t{a[5]}\t{a[6]}")


current, acc = None, None

for raw in sys.stdin:
    parts = raw.rstrip("\r\n").split("\t")
    if len(parts) != 8:
        continue
    day = parts[0]
    try:
        vals = [int(parts[1]), int(parts[2]), to_units(parts[3], 6), to_units(parts[4], 4),
                int(parts[5]), int(parts[6]), int(parts[7])]
    except ValueError:
        continue

    if day != current:
        if current is not None:
            emit(current, acc)
        current, acc = day, [0, 0, 0, 0, 0, 0, 0]
    for i, v in enumerate(vals):
        acc[i] += v

if current is not None:
    emit(current, acc)
