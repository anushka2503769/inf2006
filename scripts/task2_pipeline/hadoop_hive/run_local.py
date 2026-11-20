#!/usr/bin/env python3
"""
run_local.py - test a mapper/reducer pair on your laptop before using EMR.

Does what Hadoop Streaming does, on one machine:
    input file -> mapper -> sort by key -> reducer -> printed output
Works on Windows, Mac and Linux (no need for the `sort` command).

Usage (from this folder):
    python run_local.py price          sample/prices_sample.csv
    python run_local.py rejects        sample/prices_sample.csv
    python run_local.py gdelt          sample/gdelt_sample.CSV
    python run_local.py gdelt-rowwise  sample/gdelt_sample.CSV

`gdelt` and `gdelt-rowwise` must print the same rows: the first adds up inside
the mapper, the second is the baseline that prints one line per event.
"""

import os
import subprocess
import sys

# job name -> (mapper, reducer or None for a map-only job, extra environment)
JOBS = {
    "price": ("streaming/price_audit_mapper.py", "streaming/price_audit_reducer.py", {}),
    "rejects": ("streaming/price_audit_mapper.py", None, {"AUDIT_MODE": "rejects"}),
    "gdelt": ("streaming/gdelt_daily_mapper.py", "streaming/gdelt_daily_reducer.py", {}),
    "gdelt-rowwise": ("streaming/gdelt_daily_mapper_rowwise.py", "streaming/gdelt_daily_reducer.py", {}),
}

if len(sys.argv) != 3 or sys.argv[1] not in JOBS:
    sys.exit(__doc__)

mapper, reducer, extra_env = JOBS[sys.argv[1]]
with open(sys.argv[2], encoding="utf-8", errors="replace") as f:
    data = f.read()

env = dict(os.environ, **extra_env)
mapped = subprocess.run([sys.executable, mapper], input=data, capture_output=True, text=True, env=env)
if mapped.returncode:
    sys.exit(mapped.stderr)

# Hadoop sorts mapper output by key (the text before the first tab)
lines = [l for l in mapped.stdout.splitlines() if l]
lines.sort(key=lambda l: l.split("\t", 1)[0])

if reducer is None:
    print("\n".join(lines))
else:
    reduced = subprocess.run([sys.executable, reducer], input="\n".join(lines) + "\n",
                             capture_output=True, text=True)
    if reduced.returncode:
        sys.exit(reduced.stderr)
    print(reduced.stdout, end="")

# Show the counters the job would report on EMR
counts = {}
for l in mapped.stderr.splitlines():
    if l.startswith("reporter:counter:"):
        _, _, rest = l.split(":", 2)
        group, name, n = rest.split(",")
        counts[f"{group}.{name}"] = counts.get(f"{group}.{name}", 0) + int(n)
if counts:
    print("\n-- counters --", file=sys.stderr)
    for k in sorted(counts):
        print(f"{k:32s} {counts[k]}", file=sys.stderr)
    if "PriceAudit.RowsRead" in counts:  # every price row is OK, BAD or out of scope (OOS)
        parts = [counts.get(f"PriceAudit.{n}", 0) for n in ("RowsOK", "RowsBad", "RowsOutOfScope")]
        ok = sum(parts) == counts["PriceAudit.RowsRead"]
        print(f"RowsRead = OK + Bad + OutOfScope : {counts['PriceAudit.RowsRead']} = {' + '.join(map(str, parts))}"
              f"  {'OK' if ok else 'MISMATCH'}", file=sys.stderr)