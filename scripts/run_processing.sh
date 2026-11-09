#!/usr/bin/env bash
# Run from the cloned project with the school's Spark environment and Java 17.
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
spark_python="${DAT535_SPARK_PYTHON:-$HOME/spark-env/bin/python}"
runtime_metadata="${DAT535_JAVA_METADATA:-$HOME/dat535-runtime/java17.json}"
if [[ -f "$runtime_metadata" ]]; then
  export JAVA_HOME="$("$spark_python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["java_home"])' "$runtime_metadata")"
fi
if [[ -n "${JAVA_HOME:-}" ]]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
export PYSPARK_PYTHON="$spark_python"
exec "$spark_python" -B -u -m pipeline.run_snapshot "$@"
