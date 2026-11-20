#!/usr/bin/env python3
"""
price_audit_mapper.py - Hadoop Streaming mapper (audit only, no cleaning).

Reads the raw daily price CSV (all_stocks_5yr.csv) line by line and checks
each row. It never changes or drops the data in HDFS; it only reports what it
finds. Cleaning is done later in PySpark.

Input  : date,open,high,low,close,volume,Name   (header line is skipped)
         Dates are expected as YYYY-MM-DD (the original file). M/D/YYYY
         (e.g. 2/8/2013, the Excel-saved copy) is accepted as a fallback.
         Dates are converted to YYYY-MM-DD in the output.
Output : <ticker> TAB <date> TAB <status> TAB <reasons>
         status  = OK    row passes every check and is inside the project window
                   BAD   row is inside the window but fails at least one check
                   OOS   row is outside the project window (out of scope; this is
                         not a data error, so it is not counted as BAD and is not
                         written to the rejects file)
         reasons = comma-separated problem codes, or "-" when there are none
         Every row is OK, BAD or OOS, so rows = OK + BAD + OOS.

Rejects mode (set AUDIT_MODE=rejects, used by a map-only job):
         writes only the BAD rows, with the reasons and the untouched original
         line, so rejected rows are kept separately from raw and cleaned data:
         <ticker> TAB <date> TAB <reasons> TAB <original line>
         Out-of-window rows (OOS) are not rejects. Duplicate rows are found by
         the reducer, so they are counted in the audit table (price_audit) and
         are not part of this output.

Problem codes
  FIELDS      wrong number of columns
  NO_TICKER   ticker name is empty
  DATE        date is missing or is not a real date (YYYY-MM-DD, or M/D/YYYY)
  OUT_RANGE   date outside the project window (2013-02-08 to 2017-12-29); the row
              gets status OOS and no other check is reported for it
  MISSING     one of open/high/low/close/volume is empty
  NOT_NUM     a price or volume is not a number
  NONPOS      a price is zero or negative
  NEG_VOL     volume is negative
  OHLC        high/low are inconsistent with open/close
"""

import os
import sys
from datetime import datetime

START, END = "2013-02-08", "2017-12-29"
EXPECTED_FIELDS = 7
REJECTS_MODE = os.environ.get("AUDIT_MODE", "audit").lower() == "rejects"


def parse_date(text):
    """Return YYYY-MM-DD, or None if the text is not a real date."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


counts = {}


def counter(name):
    # Hadoop Streaming counters: they appear in the job output and the job history, which gives dated execution evidence for the report.
    # Counted in memory and written once at the end (one write per row is slow).
    counts[name] = counts.get(name, 0) + 1


def check(line):
    """Return (ticker, date, reasons) for one data line."""
    f = line.split(",")
    if len(f) != EXPECTED_FIELDS:
        # cannot trust which column is the ticker, so group these together
        return "_MALFORMED", (f[0].strip() if f else ""), ["FIELDS"]

    reasons = []
    date, o, h, l, c, v, ticker = (x.strip() for x in f)
    if not ticker:
        reasons.append("NO_TICKER")
        ticker = "UNKNOWN"

    iso = parse_date(date)
    if iso is None:
        reasons.append("DATE")
    else:
        date = iso
        if not (START <= date <= END):
            # out of scope: the date alone decides, whatever else is in the row
            return ticker, date, ["OUT_RANGE"]

    values = [o, h, l, c, v]
    if any(x == "" for x in values):
        reasons.append("MISSING")
    else:
        try:
            o, h, l, c = float(o), float(h), float(l), float(c)
            v = float(v)
            if min(o, h, l, c) <= 0:
                reasons.append("NONPOS")
            if v < 0:
                reasons.append("NEG_VOL")
            if h < max(o, c, l) or l > min(o, c, h):
                reasons.append("OHLC")
        except ValueError:
            reasons.append("NOT_NUM")
    return ticker, date, reasons


for raw in sys.stdin:
    line = raw.rstrip("\r\n").lstrip("\ufeff")  # a byte-order mark can precede the header
    if not line:
        continue
    if line.lower().startswith("date,"):  # header row
        counter("HeaderRows")
        continue

    counter("RowsRead")
    ticker, date, reasons = check(line)
    if reasons == ["OUT_RANGE"]:
        status = "OOS"
    else:
        status = "BAD" if reasons else "OK"
    counter({"OK": "RowsOK", "BAD": "RowsBad", "OOS": "RowsOutOfScope"}[status])
    for r in reasons:
        counter(f"Reason_{r}")

    if REJECTS_MODE:
        if status == "BAD":
            counter("RejectsWritten")
            print(f"{ticker}\t{date}\t{','.join(reasons)}\t{line}")
    else:
        print(f"{ticker}\t{date}\t{status}\t{','.join(reasons) or '-'}")

for name in sorted(counts):
    sys.stderr.write(f"reporter:counter:PriceAudit,{name},{counts[name]}\n")