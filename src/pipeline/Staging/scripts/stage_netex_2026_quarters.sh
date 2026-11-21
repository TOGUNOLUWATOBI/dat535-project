#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-/home/ubuntu/dat535-project}"
DATA_ROOT="${DAT535_DATA_ROOT:-/srv/dat535/data}"
BRONZE_MANIFEST="${BRONZE_MANIFEST:-$DATA_ROOT/bronze/netex/kol/manifest.json}"
STAGING_RUNS_DIR="${STAGING_RUNS_DIR:-$DATA_ROOT/staging/netex/kol/runs}"

export DAT535_DATA_ROOT="$DATA_ROOT"
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://127.0.0.1:7077}"
export PYSPARK_DRIVER_PYTHON="python3"
unset PYSPARK_DRIVER_PYTHON_OPTS
export JAVA_HOME="${JAVA_HOME:-/home/ubuntu/dat535-runtime/java17/jdk-17.0.21+9-jre}"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/srv/dat535/spark/conf}"
export PATH="$JAVA_HOME/bin:/home/ubuntu/spark-env/bin:$PATH"
unset SPARK_HOME

# Verified constrained Spark baseline for large NeTEx XML member serialization
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-1}"
TOTAL_EXECUTOR_CORES="${TOTAL_EXECUTOR_CORES:-1}"
CACHE_BATCH_SIZE="${CACHE_BATCH_SIZE:-1}"

# Quarters to run: label|start_inclusive|end_exclusive
QUARTERS=(
  "q1|2026-01-01|2026-04-01"
  "q2|2026-04-01|2026-07-01"
  "q3|2026-07-01|2026-10-01"
  "q4|2026-10-01|2027-01-01"
)

# Optional filter: pass e.g. "q1 q2" or "q3,q4" as args; defaults to all 4 quarters
SELECTED_ARGS="${*:-}"

quarter_selected() {
  local q_label="$1"
  if [[ -z "$SELECTED_ARGS" ]]; then
    return 0
  fi
  local normalized="${SELECTED_ARGS//,/ }"
  for item in $normalized; do
    if [[ "${item,,}" == "${q_label,,}" ]]; then
      return 0
    fi
  done
  return 1
}

mkdir -p "$STAGING_RUNS_DIR"
cd "$REPO_ROOT"

for spec in "${QUARTERS[@]}"; do
  IFS='|' read -r q_label start_date end_date <<< "$spec"
  if ! quarter_selected "$q_label"; then
    continue
  fi

  run_name="batch-2026-${q_label}"
  out_dir="$STAGING_RUNS_DIR/$run_name"

  dates_csv=$(python3 - "$BRONZE_MANIFEST" "$start_date" "$end_date" <<'PY'
import json
import sys

manifest_path, start_d, end_d = sys.argv[1], sys.argv[2], sys.argv[3]
with open(manifest_path, "r", encoding="utf-8") as fh:
    manifest = json.load(fh)

records = manifest.get("records", [])
dates = sorted({
    str(r["target_date"])
    for r in records
    if r.get("status") == "downloaded"
    and r.get("target_date") is not None
    and start_d <= str(r["target_date"]) < end_d
})
if not dates:
    sys.exit(f"No downloaded dates found in [{start_d}, {end_d})")
print(",".join(dates))
PY
)

  zip_count=$(awk -F',' '{print NF}' <<< "$dates_csv")
  echo "============================================================"
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Starting $run_name ($zip_count ZIPs): $dates_csv"
  echo "Output target: $out_dir"
  echo "============================================================"

  rm -rf "$out_dir" "$STAGING_RUNS_DIR/.${run_name}."*
  before_kb=$(df -k /dev/vda1 | awk 'NR==2 {print $3}')

  spark-submit \
    --master "$SPARK_MASTER_URL" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --total-executor-cores "$TOTAL_EXECUTOR_CORES" \
    --conf "spark.sql.inMemoryColumnarStorage.batchSize=$CACHE_BATCH_SIZE" \
    --py-files src/pipeline/Staging/schemas.py,src/pipeline/Staging/zip_xml_reader.py \
    src/pipeline/Staging/stage_netex.py \
    --dates "$dates_csv" \
    --output-dir "$out_dir" \
    --zip-slices "$zip_count"

  after_kb=$(df -k /dev/vda1 | awk 'NR==2 {print $3}')
  delta_mib=$(( (after_kb - before_kb) / 1024 ))
  dir_size=$(du -sh "$out_dir" | awk '{print $1}')

  echo "[$run_name] Net /dev/vda1 delta: ${delta_mib} MiB | Retained size: $dir_size"
  df -h /dev/vda1
  cat "$out_dir/staging_manifest.json"
  echo ""
done

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] All requested 2026 NeTEx staging quarters completed."