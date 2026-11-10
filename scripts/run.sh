#!/usr/bin/env bash
# Run a pipeline command with the school Spark environment and the project's Java 17.
#
#   bash scripts/run.sh <command> [options]      (add --help after a command for its options)
#
#   processing      one ingestion manifest through checkpointed staging, Silver and Gold
#   rebuild         whole-month plans for every registered snapshot, one Spark process per job
#   history         route changes across all completed snapshots (after processing)
#   inspect         print route-history results: summary | candidates | month | line | date
#   quality-report  before/after Silver quality table and publication-gate results
#   quality-trace   trace real journeys from the source XML through Silver and Gold
#   tests           the whole test suite, including the Spark tests
#
# Override the environment with DAT535_SPARK_PYTHON (default ~/spark-env/bin/python)
# and DAT535_JAVA_METADATA (default ~/dat535-runtime/java17.json).
set -euo pipefail

case "${1:-}" in
  processing)     module=pipeline.run_snapshot ;;
  rebuild)        module=pipeline.rebuild_monthly ;;
  history)        module=pipeline.analysis.route_history ;;
  inspect)        module=pipeline.analysis.inspect_history ;;
  quality-report) module=pipeline.quality.report ;;
  quality-trace)  module=pipeline.quality.trace ;;
  tests)          module="" ;;
  *)
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    [[ -z "${1:-}" || "$1" == "-h" || "$1" == "--help" ]] && exit 0
    echo "Unknown command: $1" >&2
    exit 2 ;;
esac
shift

cd "$(dirname -- "${BASH_SOURCE[0]}")/.."
spark_python="${DAT535_SPARK_PYTHON:-$HOME/spark-env/bin/python}"
runtime_metadata="${DAT535_JAVA_METADATA:-$HOME/dat535-runtime/java17.json}"
if [[ -f "$runtime_metadata" ]]; then
  JAVA_HOME="$("$spark_python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["java_home"])' "$runtime_metadata")"
  export JAVA_HOME
fi
if [[ -n "${JAVA_HOME:-}" ]]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
unset SPARK_HOME  # use the Spark 4.2 bundled with spark-env, not the lab's /opt/spark
export PYSPARK_PYTHON="$spark_python"

if [[ -z "$module" ]]; then
  # Fail instead of silently skipping the Spark tests if PySpark is missing.
  "$spark_python" -c 'import pyspark' || { echo "PySpark not available in $spark_python" >&2; exit 1; }
  exec "$spark_python" -B -m unittest discover -s tests -t . "$@"
fi
exec "$spark_python" -B -u -m "$module" "$@"
