#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
DATA_ROOT="${DAT535_DATA_ROOT:-/srv/dat535/data}"
SILVER_RUNS_DIR="${SILVER_RUNS_DIR:-$DATA_ROOT/silver/kol/runs}"

export DAT535_DATA_ROOT="$DATA_ROOT"
export PYTHONPATH="$REPO_ROOT/src:${PYTHONPATH:-}"
export PYTHONSAFEPATH=1
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://127.0.0.1:7077}"
export PYSPARK_DRIVER_PYTHON="python3"
unset PYSPARK_DRIVER_PYTHON_OPTS
export JAVA_HOME="${JAVA_HOME:-/home/ubuntu/dat535-runtime/java17/jdk-17.0.21+9-jre}"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/srv/dat535/spark/conf}"
export PATH="$JAVA_HOME/bin:/home/ubuntu/spark-env/bin:$PATH"
unset SPARK_HOME

EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-1g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-1}"
TOTAL_EXECUTOR_CORES="${TOTAL_EXECUTOR_CORES:-1}"
CACHE_BATCH_SIZE="${CACHE_BATCH_SIZE:-1}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-4}"

RUN_ID="${1:-batch-2026-full}"
DATES_CSV="${2:-}"
OUT_DIR="$SILVER_RUNS_DIR/$RUN_ID"
PKG_ZIP="/tmp/silver_pkg_${RUN_ID}.zip"
LAUNCHER="/tmp/run_silver_mod_${RUN_ID}.py"

cd "$REPO_ROOT"
rm -rf "$OUT_DIR" "$SILVER_RUNS_DIR/.${RUN_ID}."* "$PKG_ZIP" "$LAUNCHER"

python3 - "$REPO_ROOT/src" "$PKG_ZIP" <<'PYEOF'
import sys
import zipfile
from pathlib import Path

src_root = Path(sys.argv[1])
zip_path = Path(sys.argv[2])
silver_dir = src_root / "pipeline" / "Silver"

with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for file_path in sorted(silver_dir.rglob("*")):
        if file_path.is_file() and "__pycache__" not in file_path.parts:
            zf.write(file_path, file_path.relative_to(src_root).as_posix())
PYEOF

python3 - "$PKG_ZIP" <<'PYEOF'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as zf:
    names = zf.namelist()
    assert any(n.startswith("pipeline/Silver/") for n in names), f"Missing pipeline/Silver in {names[:10]}"
    print(f"Packaged {len(names)} files into {sys.argv[1]}")
PYEOF

cat <<PYEOF > "$LAUNCHER"
import runpy, sys
for entry in ("", "/home/ubuntu/dat535-project"):
    while entry in sys.path:
        sys.path.remove(entry)
if "$REPO_ROOT/src" not in sys.path:
    sys.path.insert(0, "$REPO_ROOT/src")
sys.modules.pop("pipeline", None)
sys.exit(runpy.run_module("pipeline.Silver.build_silver", run_name="__main__"))
PYEOF

CMD=(
  spark-submit
  --master "$SPARK_MASTER_URL"
  --executor-memory "$EXECUTOR_MEMORY"
  --executor-cores "$EXECUTOR_CORES"
  --total-executor-cores "$TOTAL_EXECUTOR_CORES"
  --conf "spark.sql.inMemoryColumnarStorage.batchSize=$CACHE_BATCH_SIZE"
  --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS"
  --conf "spark.sql.autoBroadcastJoinThreshold=-1"
  --conf "spark.executorEnv.PYTHONPATH=$PKG_ZIP:$REPO_ROOT/src"
  --py-files "$PKG_ZIP"
  "$LAUNCHER"
  --output-dir "$OUT_DIR"
)

if [[ -n "$DATES_CSV" ]]; then
  CMD+=(--dates "$DATES_CSV")
fi

"${CMD[@]}"
cat "$OUT_DIR/quality/quality_report.json"
