#!/bin/bash
# run_hive.sh : run the four Hive scripts in order and keep a log of each one.
#
#   bash run_hive.sh                    (run after run_streaming.sh)
#   RUN_ID=_run1 bash run_hive.sh       (use the same RUN_ID as run_streaming.sh)
#
#   hive/01_tables.hql            register the raw data and the job outputs as tables
#   hive/02_gdelt_daily.hql       daily GDELT sums -> averages and shares (ORC)
#   hive/03_trading_calendar.hql  trading days, weekly calendar, splits (ORC)
#   hive/04_checks.hql            reconciliation and sanity checks
#
# Tables go into the database bigdata<RUN_ID> (bigdata when RUN_ID is empty) and read the output folders written by run_streaming.sh with the same RUN_ID. 
# A new RUN_ID therefore gives new tables and new output locations for each repetition.
#
# Evidence: evidence/logs/hive_<script>_<stamp>.log for every script. 
# Each log shows the YARN application ID Hive used, so every Tez job can be traced to its script.
set -euo pipefail
cd "$(dirname "$0")"
trap 'bash redact_evidence.sh evidence > /dev/null || true' EXIT

RUN_ID=${RUN_ID:-}
case "$RUN_ID" in *[!A-Za-z0-9_]*) echo "STOP: RUN_ID may contain only letters, digits and _"; exit 1;; esac
DB=bigdata$RUN_ID
PRICE_DIR=${PRICE_DIR:-/project/raw/daily_price}
AUDIT_DIR=${AUDIT_DIR:-/project/audit/price_audit$RUN_ID}
REJECT_DIR=${REJECT_DIR:-/project/rejected/price_audit$RUN_ID}
GDELT_SUMS_DIR=${GDELT_SUMS_DIR:-/project/staging/gdelt_daily$RUN_ID}
HIVEVARS=(--hivevar "DB=$DB" --hivevar "PRICE_DIR=$PRICE_DIR" --hivevar "AUDIT_DIR=$AUDIT_DIR"
          --hivevar "REJECT_DIR=$REJECT_DIR" --hivevar "GDELT_SUMS_DIR=$GDELT_SUMS_DIR")

# Every folder the tables point at must exist and hold something
for d in "$PRICE_DIR" "$AUDIT_DIR" "$REJECT_DIR" "$GDELT_SUMS_DIR"; do
  if ! hdfs dfs -test -d "$d"; then
    echo "STOP: $d does not exist in HDFS. Run run_streaming.sh first (with the same RUN_ID)."
    exit 1
  fi
done

LOGDIR=evidence/logs
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
SUMMARY=evidence/hive_summary_$STAMP.txt
mkdir -p "$LOGDIR"
{
  echo "# Hive run summary"
  echo "# run stamp (UTC): $STAMP    RUN_ID: ${RUN_ID:-(none)}    database: $DB"
  echo "# tables read: $PRICE_DIR  $AUDIT_DIR  $REJECT_DIR  $GDELT_SUMS_DIR"
  echo "# script  seconds  result  yarn_application"
} > "$SUMMARY"

for name in 01_tables 02_gdelt_daily 03_trading_calendar 04_checks; do
  log="$LOGDIR/hive_${name}_$STAMP.log"
  echo "== hive/$name.hql =="
  start=$SECONDS
  result=SUCCEEDED
  hive "${HIVEVARS[@]}" -f "hive/$name.hql" 2>&1 | tee "$log" || result=FAILED
  app=$(grep -o "application_[0-9]*_[0-9]*" "$log" | sort -u | tr '\n' ' ' || true)
  echo "$name  $((SECONDS - start))  $result  ${app:-none (no job needed)}" >> "$SUMMARY"
  [ "$result" = SUCCEEDED ] || { echo "STOP: hive/$name.hql failed. See $log"; exit 1; }
done

# Compare the GDELT job's own row counter with what Hive sees in its output
checks="$LOGDIR/hive_04_checks_$STAMP.log"
latest=$(ls -t "$LOGDIR"/gdelt_daily"${RUN_ID}"_[0-9]*Z.log 2>/dev/null | head -1 || true)
{
  echo
  echo "== GDELT job counters, for comparison with 'gdelt rows in output' above =="
  if [ -n "$latest" ]; then
    echo "from $latest"
    grep -E "^\s+(RowsRead|RowsUsed|Bad_[A-Za-z]+)=" "$latest" | sed "s/^\s*/  /" || true
    echo "RowsRead should equal events + bad_rows + Bad_FieldCount + Bad_DateAdded."
  else
    echo "no gdelt_daily${RUN_ID} log found in $LOGDIR (run run_streaming.sh first)"
  fi
} | tee -a "$checks"

# (redact_evidence.sh runs automatically when the script ends, see the trap at the top)
echo
echo "== Summary (also saved in $SUMMARY) =="
cat "$SUMMARY"
echo
echo "Check results: $checks"
echo "Next: ${RUN_ID:+RUN_ID=$RUN_ID }bash run_spark_check.sh"