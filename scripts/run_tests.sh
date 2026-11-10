#!/usr/bin/env bash
# Run the whole test suite, including the Spark transformation tests, with the
# school Spark environment and project Java 17 (same setup as run_processing.sh).
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
spark_python="${DAT535_SPARK_PYTHON:-$HOME/spark-env/bin/python}"
runtime_metadata="${DAT535_JAVA_METADATA:-$HOME/dat535-runtime/java17.json}"
if [[ -f "$runtime_metadata" ]]; then
  export JAVA_HOME="$("$spark_python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["java_home"])' "$runtime_metadata")"
fi
if [[ -n "${JAVA_HOME:-}" ]]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
unset SPARK_HOME  # use the Spark 4.2 bundled with spark-env, not the lab's /opt/spark
export PYSPARK_PYTHON="$spark_python"
# Fail instead of silently skipping the Spark tests if PySpark is missing.
"$spark_python" -c 'import pyspark' || { echo "PySpark not available in $spark_python" >&2; exit 1; }
exec "$spark_python" -B -m unittest discover -s tests -t . "$@"
