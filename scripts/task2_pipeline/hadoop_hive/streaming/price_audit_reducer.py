#!/usr/bin/env python3
"""
price_audit_reducer.py - Hadoop Streaming reducer.

Receives the mapper output sorted by ticker and writes one audit summary
row per ticker.

Output (tab-separated):
  ticker, rows, ok_rows, bad_rows, duplicate_dates, out_of_range_rows,
  missing_rows, ohlc_rows, other_bad_rows, first_date, last_date

rows = ok_rows + bad_rows + out_of_range_rows. Rows outside the project window
(status OOS) are counted only in out_of_range_rows, not in bad_rows.

first_date / last_date are taken from rows with a valid date inside the
project window, so they describe the usable coverage of each series.
"""

import sys


def emit(t, s):
    print("\t".join(str(x) for x in (
        t, s["rows"], s["ok"], s["bad"], s["dups"], s["out_range"],
        s["missing"], s["ohlc"], s["other"],
        s["first"] or "", s["last"] or "",
    )))


def fresh():
    return {"rows": 0, "ok": 0, "bad": 0, "dups": 0, "out_range": 0,
            "missing": 0, "ohlc": 0, "other": 0,
            "first": None, "last": None, "seen": set()}


current, stats = None, None

for raw in sys.stdin:
    parts = raw.rstrip("\r\n").split("\t")
    if len(parts) != 4:
        continue
    ticker, date, status, reasons = parts

    if ticker != current:
        if current is not None:
            emit(current, stats)
        current, stats = ticker, fresh()

    stats["rows"] += 1
    if date in stats["seen"]:
        stats["dups"] += 1  # same ticker + same date seen before
    else:
        stats["seen"].add(date)

    if status == "OK":
        stats["ok"] += 1
    elif status == "OOS":
        stats["out_range"] += 1  # outside the project window: out of scope, not bad
    else:
        stats["bad"] += 1
        codes = set(reasons.split(","))
        if "MISSING" in codes:
            stats["missing"] += 1
        if "OHLC" in codes:
            stats["ohlc"] += 1
        if codes - {"MISSING", "OHLC"}:
            stats["other"] += 1

    # Coverage: valid, in-window dates only
    bad_date = any(code in reasons for code in ("DATE", "OUT_RANGE", "FIELDS"))
    if not bad_date:
        if stats["first"] is None or date < stats["first"]:
            stats["first"] = date
        if stats["last"] is None or date > stats["last"]:
            stats["last"] = date

if current is not None:
    emit(current, stats)