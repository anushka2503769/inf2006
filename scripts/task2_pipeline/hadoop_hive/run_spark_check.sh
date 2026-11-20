#!/bin/bash
# run_spark_check.sh : show that Spark can read the Hive tables, and record how.
#
#   bash run_spark_check.sh                  (run after run_hive.sh)
#   RUN_ID=_run1 bash run_spark_check.sh     (same RUN_ID as the earlier steps)
#
# For each table the script counts the rows with Spark and with Hive and compares them, so the check works for either price file.
# Writes evidence/spark_reads_hive_<stamp>.txt with the date, the exact command for each table, the Spark count, the Hive count and the result.
#
# Reference values from the earlier runs are shown for three tables only:
#   trading_weeks 256 (152 + 52 + 52 weeks), gdelt_daily 731 days, price_audit 505 series.
# They are reported separately and do not change the OK / DIFFERENT result.
#
# Why raw_prices is counted twice: Hive skips the CSV header line because of a table setting. 
# Some Spark versions ignore that setting and count the header as a row (one more than Hive). 
# That case is reported as OK (header counted). 
# The view raw_prices_dated removes the header itself, so it is the table Spark code should read.
set -uo pipefail
cd "$(dirname "$0")"
trap 'bash redact_evidence.sh evidence > /dev/null || true' EXIT

RUN_ID=${RUN_ID:-}
case "$RUN_ID" in *[!A-Za-z0-9_]*) echo "STOP: RUN_ID may contain only letters, digits and _"; exit 1;; esac
DB=bigdata$RUN_ID

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=evidence/spark_reads_hive_$STAMP.txt
LOG=evidence/logs/spark_check_$STAMP.log
mkdir -p evidence/logs

TABLES=(trading_weeks raw_prices raw_prices_dated gdelt_daily price_audit price_rejects)
REFERENCE=(256 - - 731 505 -)  # "-" = no documented reference value

{
  echo "# Spark reading the Hive tables in database $DB"
  echo "# run at (UTC): $(date -u '+%Y-%m-%d %H:%M:%S')    RUN_ID: ${RUN_ID:-(none)}"
  echo "# table  spark_count  hive_count  result  reference  vs_reference  spark_command"
} > "$OUT"

count() {  # count <engine command...> : the last output line that is only a number
  local n
  n=$("$@" 2>> "$LOG" | tr -d '\r' | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | tail -1 | tr -d '[:space:]' || true)
  echo "${n:-ERROR}"
}

problems=0
for i in "${!TABLES[@]}"; do
  t=${TABLES[$i]}; ref=${REFERENCE[$i]}
  sql="SELECT COUNT(*) FROM $DB.$t;"
  cmd="spark-sql -e \"$sql\""
  echo "== $cmd =="
  spark=$(count spark-sql -e "$sql")
  hive_n=$(count hive -S -e "$sql")
  if [ "$spark" = ERROR ] || [ "$hive_n" = ERROR ]; then
    result=ERROR
  elif [ "$spark" = "$hive_n" ]; then
    result=OK
  elif [ "$t" = raw_prices ] && [ "$spark" -eq $((hive_n + 1)) ]; then
    result="OK (header counted)"
  else
    result=DIFFERENT
  fi
  case "$result" in OK*) ;; *) problems=$((problems + 1));; esac
  if [ "$ref" = "-" ]; then vs=-; elif [ "$hive_n" = ERROR ]; then vs=ERROR
  elif [ "$hive_n" = "$ref" ]; then vs=MATCH; else vs=DIFFERENT; fi
  echo "$t  $spark  $hive_n  $result  $ref  $vs  $cmd" | tee -a "$OUT"
done

{
  echo "# Spark applications used:"
  grep -o "application_[0-9]*_[0-9]*" "$LOG" | sort -u | sed 's/^/#   /'
  echo "# Note: 'OK (header counted)' on raw_prices means Spark counted the header line."
  echo "#       Spark code must then read $DB.raw_prices_dated instead."
  echo "# Problems (ERROR or DIFFERENT): $problems"
} >> "$OUT"

# (redact_evidence.sh runs automatically when the script ends, see the trap at the top)
echo
cat "$OUT"
[ "$problems" -eq 0 ]