#!/usr/bin/env bash
# Build one immutable Gold run from the published Silver runs.
#   bash src/pipeline/Gold/scripts/run_gold_2026.sh [GOLD_RUN_ID]
# The operating dates are every date the NeTEx Bronze manifest marks as downloaded
# (the 53 sampled 2026 dates). Gold refuses to overwrite an existing run ID.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../../../.." && pwd)}"
DATA_ROOT="${DAT535_DATA_ROOT:-/srv/dat535/data}"
BRONZE_MANIFEST="${BRONZE_MANIFEST:-$DATA_ROOT/bronze/netex/kol/manifest.json}"
RUN_ID="${1:-gold-kol-$(date -u +%Y%m%dT%H%M%SZ)}"

export JAVA_HOME="${JAVA_HOME:-/home/ubuntu/dat535-runtime/java17/jdk-17.0.21+9-jre}"
export PATH="$JAVA_HOME/bin:/home/ubuntu/spark-env/bin:$PATH"
export PYTHONPATH="$REPO_ROOT/src"
unset SPARK_HOME PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS

dates=$(python3 - "$BRONZE_MANIFEST" <<'PY'
import json, sys
records = json.load(open(sys.argv[1], encoding="utf-8"))["records"]
print(" ".join(sorted({r["target_date"] for r in records if r.get("status") == "downloaded"})))
PY
)
echo "Gold run $RUN_ID over $(wc -w <<< "$dates") dates"
cd "$REPO_ROOT"
# shellcheck disable=SC2086  # one argument per date
python3 -m pipeline.Gold.build_gold --gold-run-id "$RUN_ID" \
  --silver-runs-dir "$DATA_ROOT/silver/kol/runs" --gold-runs-dir "$DATA_ROOT/gold/kol/runs" --dates $dates
cat "$DATA_ROOT/gold/kol/runs/$RUN_ID/quality/quality_report.json"
