#!/bin/bash
# redact_evidence.sh : replace machine-specific names in the evidence files.
#
#   bash redact_evidence.sh [folder]      (default folder: evidence)
#
# Why? - Must not contain machine-specific details, and Hadoop, Hive and Spark all print the cluster's private hostname and IP address in their output.
# This replaces them with <emr-node>, <emr-node-public> and <private-ip>. 
#
# After replacing, it searches the folder again. 
# If a private name or address is still there it prints a WARNING to stderr and exits with status 1, so a leftover is never missed silently.
set -euo pipefail
DIR=${1:-evidence}
[ -d "$DIR" ] || exit 0

PRIV='(10\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])|192\.168)\.[0-9]{1,3}\.[0-9]{1,3}'

find "$DIR" -type f \( -name '*.log' -o -name '*.txt' \) -print0 | while IFS= read -r -d '' f; do
  # The third rule uses a loop (:a ... ta): two addresses separated by a single character share that character, so one pass alone would miss the second one
  sed -i -E \
    -e 's/ip-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}(\.[a-z0-9-]+)*\.internal/<emr-node>/g' \
    -e 's/ip-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}/<emr-node>/g' \
    -e 's/ec2-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}\.[a-z0-9.-]*amazonaws\.com(\.cn)?/<emr-node-public>/g' \
    -e ':a' \
    -e "s/(^|[^0-9.])$PRIV([^0-9.]|\$)/\1<private-ip>\4/;ta" \
    "$f"
  echo "redacted: $f"
done

# Verify: nothing machine-specific may remain
left=$(grep -r -n -E -e 'ip-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}' \
                     -e 'ec2-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}' \
                     -e "(^|[^0-9.])$PRIV([^0-9.]|\$)" \
                     --include='*.log' --include='*.txt' "$DIR" || true)
if [ -n "$left" ]; then
  echo "WARNING: machine-specific names or addresses are still in $DIR:" >&2
  printf '%s\n' "$left" | head -10 >&2
  exit 1
fi