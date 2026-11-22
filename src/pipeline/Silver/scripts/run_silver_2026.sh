#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
DATA_ROOT="${DAT535_DATA_ROOT:-/srv/dat535/data}"
SILVER_RUNS_DIR="${SILVER_RUNS_DIR:-$DATA_ROOT/silver/kol/runs}"

export DAT535_DATA_ROOT="$DATA_ROOT"
export PYTHONPATH="$REPO_ROOT/src:${PYTHONPATH:-}"
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://127.0.0.1:7077}"
export PYSPARK_DRIVER_PYTHON="python3"
unset PYSPARK_DRIVER_PYTHON_OPTS
export JAVA_HOME="${JAVA_HOME:-/home/ubuntu/dat535-runtime/java17/jdk-17.0.21+9-jre}"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/srv/dat535/spark/conf}"
export PATH="$JAVA_HOME/bin:/home/ubuntu/spark-env/bin:$PATH"
unset SPARK_HOME

EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-1}"
TOTAL_EXECUTOR_CORES="${TOTAL_EXECUTOR_CORES:-1}"
CACHE_BATCH_SIZE="${CACHE_BATCH_SIZE:-1}"

RUN_ID="${1:-batch-2026-full}"
DATES_CSV="${2:-}"
OUT_DIR="$SILVER_RUNS_DIR/$RUN_ID"
PKG_ZIP="/tmp/silver_pkg_${RUN_ID}.zip"

cd "$REPO_ROOT"
rm -rf "$OUT_DIR" "$SILVER_RUNS_DIR/.${RUN_ID}."* "$PKG_ZIP"
python3 -m zipfile -c "$PKG_ZIP" "$REPO_ROOT/src/pipeline/Silver"

CMD=(
  spark-submit
  --master "$SPARK_MASTER_URL"
  --executor-memory "$EXECUTOR_MEMORY"
  --executor-cores "$EXECUTOR_CORES"
  --total-executor-cores "$TOTAL_EXECUTOR_CORES"
  --conf "spark.sql.inMemoryColumnarStorage.batchSize=$CACHE_BATCH_SIZE"
  --py-files "$PKG_ZIP"
  src/pipeline/Silver/build_silver.py
  --output-dir "$OUT_DIR"
)

if [[ -n "$DATES_CSV" ]]; then
  CMD+=(--dates "$DATES_CSV")
fi

"${CMD[@]}"
cat "$OUT_DIR/quality/quality_report.json"