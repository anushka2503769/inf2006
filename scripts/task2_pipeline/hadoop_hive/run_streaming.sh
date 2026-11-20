#!/bin/bash
# run_streaming.sh : run the Hadoop Streaming jobs on the EMR primary node.
#
#   bash run_streaming.sh                       # default: fast GDELT mapper
#   GDELT_MAPPER=rowwise bash run_streaming.sh  # baseline GDELT mapper (the 9 Nov version)
#   RUN_ID=_run1 bash run_streaming.sh          # fresh output folders for a repeated run (see below)
#
# Jobs, in order:
#   1. price_audit    raw prices -> one quality row per ticker   /project/audit/price_audit
#   2. price_rejects  raw prices -> the rejected rows + reasons  /project/rejected/price_audit
#   3. gdelt_daily    raw GDELT  -> one row of sums per day      /project/staging/gdelt_daily
#
# RUN_ID: text added to the end of every output folder, for example _run1 gives /project/audit/price_audit_run1. 
# Uses a new RUN_ID for each repetition so that no run overwrites another run's outputs (letters, digits and _ only). 
# Leaves it empty for a single run. Uses the same RUN_ID for run_hive.sh and run_spark_check.sh.
#
# Optional checks on the staged price file (the script stops if they do not match):
#   EXPECTED_PRICE_SHA256=<64 hex characters>   EXPECTED_PRICE_ROWS=<data rows>
#
# Evidence written under evidence/ (hostnames and private IPs are replaced at the
# end, also when a job fails):
#   input_manifest_<stamp>.txt     what was in HDFS when the jobs started
#   logs/<job><RUN_ID>_<stamp>.log full job output: job ID, counters
#   streaming_summary_<stamp>.txt  job IDs, seconds per job, key counters, and per job the output
#                                  rows, bytes and a checksum of the sorted output lines (to compare runs)
set -euo pipefail
cd "$(dirname "$0")"
trap 'bash redact_evidence.sh evidence > /dev/null || true' EXIT

PRICE_DIR=${PRICE_DIR:-/project/raw/daily_price}
GDELT_DIR=${GDELT_DIR:-/project/raw/gdelt}
GDELT_MAPPER=${GDELT_MAPPER:-inmapper}  # inmapper | rowwise
GDELT_COMBINE_SPLITS_MB=${GDELT_COMBINE_SPLITS_MB:-0}  # optional experiment, see README
EXPECTED_GDELT_FILES=${EXPECTED_GDELT_FILES:-731}
RUN_ID=${RUN_ID:-}
EXPECTED_PRICE_SHA256=${EXPECTED_PRICE_SHA256:-}
EXPECTED_PRICE_ROWS=${EXPECTED_PRICE_ROWS:-}
case "$RUN_ID" in *[!A-Za-z0-9_]*) echo "STOP: RUN_ID may contain only letters, digits and _"; exit 1;; esac
AUDIT_OUT=/project/audit/price_audit$RUN_ID
REJECT_OUT=/project/rejected/price_audit$RUN_ID
GDELT_OUT=/project/staging/gdelt_daily$RUN_ID
if [ -n "$RUN_ID" ]; then  # a named run must not reuse a folder from an earlier run
  for d in "$AUDIT_OUT" "$REJECT_OUT" "$GDELT_OUT"; do
    if hdfs dfs -test -e "$d"; then
      echo "STOP: $d already exists, so RUN_ID=$RUN_ID was used before."
      echo "      Use a new RUN_ID, or remove the old folder first:  hdfs dfs -rm -r $d"
      exit 1
    fi
  done
fi

STREAMING_JAR=/usr/lib/hadoop/hadoop-streaming.jar
LOGDIR=evidence/logs
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
MANIFEST=evidence/input_manifest_$STAMP.txt
SUMMARY=evidence/streaming_summary_$STAMP.txt
mkdir -p "$LOGDIR"

# Input checks
echo "== Checking the inputs in HDFS =="
# files only (lines starting with "-"); names starting with _ or . are ignored by Hadoop
list_files() { hdfs dfs -ls -R "$1" | awk '$1 ~ /^-/' | grep -v -E '/[_.][^/]*$' || true; }

PRICE_FILES=$(list_files "$PRICE_DIR")
N_PRICE=$(printf '%s\n' "$PRICE_FILES" | grep -c . || true)
if [ "$N_PRICE" -ne 1 ]; then
  echo "STOP: $PRICE_DIR must hold exactly one price file, found $N_PRICE:"
  printf '%s\n' "$PRICE_FILES"
  echo "Two copies would double every row. Remove the extra file and run again."
  exit 1
fi

GDELT_FILES=$(list_files "$GDELT_DIR")
N_GDELT=$(printf '%s\n' "$GDELT_FILES" | grep -c . || true)
GDELT_OTHER=$(printf '%s\n' "$GDELT_FILES" | grep -v -i -E '\.csv$' || true)
if [ -n "$GDELT_OTHER" ]; then
  echo "STOP: $GDELT_DIR contains files that are not unzipped .CSV files:"
  printf '%s\n' "$GDELT_OTHER" | head -5
  echo "The job reads every file in that folder and cannot read .zip files."
  echo "Move manifests and zips out of $GDELT_DIR (or rename them to start with _) and run again."
  exit 1
fi
if [ "$N_GDELT" -ne "$EXPECTED_GDELT_FILES" ]; then
  echo "WARNING: found $N_GDELT GDELT files, expected $EXPECTED_GDELT_FILES. Continuing."
fi

# Identify the staged price file: size, content hash and line count (about 30 MB, a few seconds)
PRICE_FILE=$(printf '%s\n' "$PRICE_FILES" | awk '{print $NF}')
PRICE_BYTES=$(printf '%s\n' "$PRICE_FILES" | awk '{print $5}')
PRICE_SHA=$(hdfs dfs -cat "$PRICE_FILE" | sha256sum | awk '{print $1}')
PRICE_LINES=$(hdfs dfs -cat "$PRICE_FILE" | awk 'END {print NR}')  # also counts a last line with no newline
PRICE_ROWS=$((PRICE_LINES - 1))  # minus the header line
case "$PRICE_BYTES" in
  29580549) PRICE_KIND="the original file (YYYY-MM-DD dates, 619,040 data rows, includes rows after 29 Dec 2017)";;
  28833918) PRICE_KIND="the Excel-saved copy filtered to the project window (M/D/YYYY dates, 605,910 data rows)";;
  *)        PRICE_KIND="not one of the two copies seen so far; record its SHA-256 in the corpus inventory";;
esac
if [ -n "$EXPECTED_PRICE_SHA256" ] && [ "$PRICE_SHA" != "$EXPECTED_PRICE_SHA256" ]; then
  echo "STOP: the staged price file has SHA-256 $PRICE_SHA"
  echo "      but EXPECTED_PRICE_SHA256 is $EXPECTED_PRICE_SHA256. Stage the right file and run again."
  exit 1
fi
if [ -n "$EXPECTED_PRICE_ROWS" ] && [ "$PRICE_ROWS" -ne "$EXPECTED_PRICE_ROWS" ]; then
  echo "STOP: the staged price file has $PRICE_ROWS data rows but EXPECTED_PRICE_ROWS is $EXPECTED_PRICE_ROWS."
  exit 1
fi

{
  echo "# Inputs in HDFS when run_streaming.sh started"
  echo "# run stamp (UTC): $STAMP    RUN_ID: ${RUN_ID:-(none)}"
  echo "# GDELT mapper: $GDELT_MAPPER    combine splits (MB): $GDELT_COMBINE_SPLITS_MB"
  echo
  echo "## Price file ($PRICE_DIR): $N_PRICE file"
  printf '%s\n' "$PRICE_FILES"
  echo "sha256: $PRICE_SHA"
  echo "data rows (lines minus the header): $PRICE_ROWS"
  echo "size matches: $PRICE_KIND"
  hdfs dfs -checksum "$PRICE_FILE" 2>/dev/null || true
  echo
  echo "## GDELT ($GDELT_DIR): $N_GDELT files"
  echo "# folders, files, bytes:"
  hdfs dfs -count "$GDELT_DIR"
  printf '%s\n' "$GDELT_FILES"
} > "$MANIFEST"
echo "Price file: $PRICE_BYTES bytes, $PRICE_ROWS data rows, sha256 $PRICE_SHA"
echo "            size matches $PRICE_KIND"
echo "GDELT: $N_GDELT files. Manifest saved to $MANIFEST"

# Helpers
: > "$SUMMARY"
{
  echo "# Hadoop Streaming run summary"
  echo "# run stamp (UTC): $STAMP    RUN_ID: ${RUN_ID:-(none)}    GDELT mapper: $GDELT_MAPPER    combine splits (MB): $GDELT_COMBINE_SPLITS_MB"
  echo "# job  job_id  seconds  result"
} >> "$SUMMARY"

run_job() {  # run_job <name> <hdfs output dir> <hadoop streaming arguments...>
  local name=$1 out=$2; shift 2
  local log="$LOGDIR/${name}${RUN_ID}_$STAMP.log" start=$SECONDS
  echo "== Job: $name =="
  hdfs dfs -rm -r -f "$out" > /dev/null
  hadoop jar "$STREAMING_JAR" -D mapreduce.job.name="${name}_$STAMP" "$@" -output "$out" 2>&1 | tee "$log" || true
  local secs=$((SECONDS - start))
  local job; job=$(grep -o "Running job: job_[0-9_]*" "$log" | head -1 | awk '{print $3}' || true)
  local result=FAILED; grep -q "completed successfully" "$log" && result=SUCCEEDED
  echo "$name  ${job:-unknown}  $secs  $result" >> "$SUMMARY"
  grep -E "^\s+(RowsRead|RowsOK|RowsBad|RowsOutOfScope|RowsUsed|RejectsWritten|Reason_[A-Z_]+|Bad_[A-Za-z]+|Map input records|Map output records|Reduce shuffle bytes|Reduce output records|Bytes Read)=" "$log" \
    | sed "s/^\s*/    /" >> "$SUMMARY" || true
  [ "$result" = SUCCEEDED ] || { echo "STOP: job $name failed. See $log"; exit 1; }
  # Output check: rows, bytes and a checksum of the sorted lines. The checksum does not depend on
  # how many part files there are, so two runs (or two mapper versions) can be compared with it.
  local o_rows o_bytes o_sha
  o_rows=$(hdfs dfs -cat "$out/part-*" | awk 'END {print NR}') || o_rows=unavailable
  o_bytes=$(hdfs dfs -du -s "$out" | awk '{print $1}') || o_bytes=unavailable
  o_sha=$(hdfs dfs -cat "$out/part-*" | LC_ALL=C sort | sha256sum | awk '{print $1}') || o_sha=unavailable
  echo "    output $out: rows=$o_rows bytes=$o_bytes sha256_of_sorted_lines=$o_sha" >> "$SUMMARY"
}

# Jobs
run_job price_audit "$AUDIT_OUT" \
  -files streaming/price_audit_mapper.py,streaming/price_audit_reducer.py \
  -mapper "python3 price_audit_mapper.py" \
  -reducer "python3 price_audit_reducer.py" \
  -input "$PRICE_DIR"

# map-only job: keeps the rejected rows (with reasons) separate from raw and cleaned data
run_job price_rejects "$REJECT_OUT" \
  -D mapreduce.job.reduces=0 \
  -files streaming/price_audit_mapper.py \
  -cmdenv AUDIT_MODE=rejects \
  -mapper "python3 price_audit_mapper.py" \
  -input "$PRICE_DIR"

GDELT_ARGS=(-D mapreduce.input.fileinputformat.input.dir.recursive=true)
if [ "$GDELT_COMBINE_SPLITS_MB" -gt 0 ]; then
  # Optional experiment (not yet run on EMR): pack several daily files into one
  # map task so fewer tasks have to start. Output must be identical.
  GDELT_ARGS+=(-D mapreduce.input.fileinputformat.split.maxsize=$((GDELT_COMBINE_SPLITS_MB * 1024 * 1024))
               -D stream.map.input.ignoreKey=true)
fi
if [ "$GDELT_MAPPER" = rowwise ]; then
  GDELT_ARGS+=(-files streaming/gdelt_daily_mapper_rowwise.py,streaming/gdelt_daily_reducer.py
               -mapper "python3 gdelt_daily_mapper_rowwise.py"
               -combiner "python3 gdelt_daily_reducer.py")
else
  GDELT_ARGS+=(-files streaming/gdelt_daily_mapper.py,streaming/gdelt_daily_reducer.py
               -mapper "python3 gdelt_daily_mapper.py")
fi
GDELT_ARGS+=(-reducer "python3 gdelt_daily_reducer.py")
if [ "$GDELT_COMBINE_SPLITS_MB" -gt 0 ]; then
  GDELT_ARGS+=(-inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat)
fi
run_job gdelt_daily "$GDELT_OUT" "${GDELT_ARGS[@]}" -input "$GDELT_DIR"

# Wrap up
# (redact_evidence.sh runs automatically when the script ends, see the trap at the top)
{
  echo "# HDFS space after the run (hdfs dfs -df -h /)"
  hdfs dfs -df -h / 2>/dev/null | sed 's/^/    /' || true
} >> "$SUMMARY"
echo
echo "== Summary (also saved in $SUMMARY) =="
cat "$SUMMARY"
echo
echo "Next: ${RUN_ID:+RUN_ID=$RUN_ID }bash run_hive.sh"