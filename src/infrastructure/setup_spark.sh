#!/usr/bin/env bash
# Configure and manage single-server Spark using the existing ~/spark-env.
set -euo pipefail

# Print CLI usage, available commands, defaults, and environment overrides.
usage() {
  cat <<'HELP'
Usage: bash src/infrastructure/setup_spark.sh [setup|start|status|check|stop]

  setup   Check Spark/Java and prepare directories and Spark configuration (default)
  start   Start one master and one worker, then wait for worker registration
  status  Show daemon and worker registration status
  check   Run a small executor job with a Parquet write/read check
  stop    Stop this installation's worker and master (ends running applications)

Defaults: 2 worker cores, one 2g executor, 1g driver, 256m daemon heaps.
Uses existing PySpark 4.2.0 and Java 17+; does not install packages or use sudo.

Overrides:
  DAT535_SPARK_PYTHON          Python in Spark environment (~/spark-env/bin/python)
  DAT535_JAVA_METADATA         JSON with java_home (~/dat535-runtime/java17.json)
  DAT535_SPARK_ROOT            Configuration, logs and scratch (/srv/dat535/spark)
  DAT535_DATA_ROOT             Persistent data (/srv/dat535/data)
  DAT535_SPARK_WORKER_CORES    Worker CPU budget (2)
  DAT535_SPARK_WORKER_MEMORY   Executor heap budget (2g)
  DAT535_SPARK_DRIVER_MEMORY   Driver heap (1g)
  DAT535_SPARK_MASTER_PORT     Master RPC port (7077)
  DAT535_SPARK_MASTER_UI_PORT  Master UI port (8080)
  DAT535_SPARK_WORKER_PORT     Worker RPC port (7078)
  DAT535_SPARK_WORKER_UI_PORT  Worker UI port (8081)
HELP
}

# Print an error message to stderr and exit with status 1.
die() { echo "Error: $*" >&2; exit 1; }

# Parse and validate the single CLI subcommand (defaults to "setup").
action="${1:-setup}"
[[ $# -le 1 ]] || die "Expected one command; use --help."
case "$action" in
  -h|--help|help) usage; exit 0 ;;
  setup|start|status|check|stop) ;;
  *) die "Unknown command: $action. Use --help." ;;
esac

# Define core paths and Spark standalone daemon class names.
spark_python="${DAT535_SPARK_PYTHON:-$HOME/spark-env/bin/python}"
runtime="${DAT535_SPARK_ROOT:-/srv/dat535/spark}"
data_root="${DAT535_DATA_ROOT:-/srv/dat535/data}"
master_class=org.apache.spark.deploy.master.Master
worker_class=org.apache.spark.deploy.worker.Worker

# Validate that the Python binary exists and runtime/data paths are safe for Spark scripts.
[[ -x "$spark_python" ]] || die "Spark Python not found: $spark_python. Set DAT535_SPARK_PYTHON to the existing environment."
# Spark's bundled daemon script does not quote every runtime path.
[[ "$runtime" == /* && "$runtime" != *[[:space:]]* ]] || die "DAT535_SPARK_ROOT must be an absolute path without whitespace."
[[ "$data_root" == /* ]] || die "DAT535_DATA_ROOT must be absolute."

# Isolate Spark daemon directories and identifier so this instance does not clash with others.
export SPARK_CONF_DIR="$runtime/conf"
export SPARK_LOG_DIR="$runtime/logs"
export SPARK_PID_DIR="$runtime/pids"
export SPARK_IDENT_STRING=dat535

# Clear inherited Spark variables and resolve the absolute Python path and PySpark 4.2.0 installation directory.
# Ignore any lab-wide Spark installation or already-loaded Spark environment.
unset SPARK_HOME SPARK_ENV_LOADED SPARK_NO_DAEMONIZE SPARK_MASTER
runtime_paths="$("$spark_python" - "$spark_python" <<'PY'
import os, pathlib, sys, pyspark

if pyspark.__version__ != "4.2.0":
    sys.exit(f"Expected PySpark 4.2.0, found {pyspark.__version__}")
print(os.path.abspath(sys.argv[1]), pathlib.Path(pyspark.__file__).resolve().parent, sep="\t")
PY
)" || die "Could not validate the Spark Python environment."
# Split on a tab rather than spaces; retain the virtual environment's Python path.
IFS=$'\t' read -r spark_python SPARK_HOME <<< "$runtime_paths"
export SPARK_HOME

# Verify that the Spark daemon launcher scripts exist inside SPARK_HOME.
daemon="$SPARK_HOME/sbin/spark-daemon.sh"
[[ -f "$daemon" && -x "$SPARK_HOME/bin/spark-class" ]] || die "Spark daemon launcher missing from $SPARK_HOME."

# Load JAVA_HOME from optional runtime metadata JSON and prepend it to PATH if configured.
runtime_metadata="${DAT535_JAVA_METADATA:-$HOME/dat535-runtime/java17.json}"
if [[ -f "$runtime_metadata" ]]; then
  JAVA_HOME="$("$spark_python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["java_home"])' "$runtime_metadata")"
  export JAVA_HOME
elif [[ -n "${DAT535_JAVA_METADATA:-}" ]]; then
  die "Java metadata not found: $runtime_metadata."
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
  [[ -x "$JAVA_HOME/bin/java" ]] || die "Java executable missing under JAVA_HOME=$JAVA_HOME."
  export PATH="$JAVA_HOME/bin:$PATH"
fi

# Verify that Java is available and meets the Java 17+ version requirement.
command -v java >/dev/null || die "Java 17+ is required; configure JAVA_HOME or DAT535_JAVA_METADATA."
java_major="$(java -version 2>&1 | sed -n '1s/.*version "\([0-9]*\).*/\1/p')"
[[ "$java_major" =~ ^[0-9]+$ && "$java_major" -ge 17 ]] || die "Java 17+ is required."

# Check whether a Spark daemon (Master or Worker) is running by matching its PID file and Java class name.
running() {
  local pid_file="$SPARK_PID_DIR/spark-$SPARK_IDENT_STRING-$1-1.pid" pid
  [[ -f "$pid_file" ]] || return 1
  read -r pid < "$pid_file"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  # Check the class too: a stale PID must never target an unrelated Java process.
  [[ "$(ps -p "$pid" -o args= 2>/dev/null)" == *"$1"* ]]
}

# Query the Master Web UI JSON endpoint to confirm the Master is reachable and has an ALIVE local Worker.
registered() {
  "$spark_python" - "$SPARK_MASTER_WEBUI_PORT" "$SPARK_MASTER_PORT" <<'PY'
import json, sys, urllib.request

try:
    with urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/json/", timeout=2) as response:
        state = json.load(response)
    expected = f"spark://127.0.0.1:{sys.argv[2]}"
    ready = state.get("url") == expected and any(
        worker.get("state") == "ALIVE" and worker.get("host") == "127.0.0.1"
        for worker in state.get("workers", [])
    )
except (OSError, ValueError):
    ready = False
sys.exit(0 if ready else 1)
PY
}

# Convert a Spark memory string (e.g., "2g" or "512m") into megabytes for validation.
heap_to_mb() {
  local value="$1" number unit
  [[ "$value" =~ ^([1-9][0-9]{0,8})([mg])$ ]] || return 1
  number="${BASH_REMATCH[1]}"
  unit="${BASH_REMATCH[2]}"
  if [[ "$unit" == g ]]; then echo "$((number * 1024))"; else echo "$number"; fi
}

# Handle the "setup" subcommand: validate resource/port overrides and generate Spark config files.
if [[ "$action" == setup ]]; then
  # Refuse to overwrite configs while daemons are actively running.
  for class in "$master_class" "$worker_class"; do
    running "$class" && die "Stop running Spark daemons before changing configuration."
  done

  # Read resource and port overrides with defaults.
  cores="${DAT535_SPARK_WORKER_CORES:-2}"
  memory="${DAT535_SPARK_WORKER_MEMORY:-2g}"
  driver_memory="${DAT535_SPARK_DRIVER_MEMORY:-1g}"
  master_port="${DAT535_SPARK_MASTER_PORT:-7077}"
  master_ui="${DAT535_SPARK_MASTER_UI_PORT:-8080}"
  worker_port="${DAT535_SPARK_WORKER_PORT:-7078}"
  worker_ui="${DAT535_SPARK_WORKER_UI_PORT:-8081}"

  # Validate CPU core budget against available host cores.
  cpu_count="$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)"
  [[ "$cores" =~ ^[1-9][0-9]*$ && ${#cores} -le ${#cpu_count} ]] && (( cores <= cpu_count )) \
    || die "Worker cores must be between 1 and $cpu_count."

  # Validate executor and driver heap sizes (minimum 512m each).
  memory_mb="$(heap_to_mb "$memory")" || die "Invalid executor memory: $memory (use 2g or 1024m)."
  driver_mb="$(heap_to_mb "$driver_memory")" || die "Invalid driver memory: $driver_memory (use 1g or 512m)."
  (( memory_mb >= 512 && driver_mb >= 512 )) || die "Driver and executor heaps must each be at least 512m."

  # Validate that all four RPC/UI ports are unprivileged (1024-65535) and mutually distinct.
  ports=("$master_port" "$master_ui" "$worker_port" "$worker_ui")
  for ((i=0; i<${#ports[@]}; i++)); do
    port="${ports[i]}"
    [[ "$port" =~ ^[1-9][0-9]{3,4}$ ]] && (( port >= 1024 && port <= 65535 )) || die "Invalid port: $port."
    for ((j=0; j<i; j++)); do
      [[ "$port" != "${ports[j]}" ]] || die "Master/worker RPC and UI ports must be distinct."
    done
  done

  # Create runtime, log, PID, scratch, worker, and persistent data directories.
  mkdir -p "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR" "$runtime/scratch" "$runtime/worker" "$data_root"

  # Write spark-env.sh with daemon bindings, resource limits, and Python/Java paths.
  # These files belong to this script. setup regenerates them from the overrides.
  {
    echo '# Generated by src/infrastructure/setup_spark.sh; change DAT535_* overrides and rerun setup.'
    printf 'export %s=%q\n' \
      PYSPARK_PYTHON "$spark_python" SPARK_LOCAL_IP 127.0.0.1 \
      SPARK_DAEMON_MEMORY 256m SPARK_LOCAL_DIRS "$runtime/scratch" \
      SPARK_MASTER_HOST 127.0.0.1 SPARK_MASTER_PORT "$master_port" \
      SPARK_MASTER_WEBUI_PORT "$master_ui" SPARK_WORKER_PORT "$worker_port" \
      SPARK_WORKER_WEBUI_PORT "$worker_ui" SPARK_WORKER_CORES "$cores" \
      SPARK_WORKER_MEMORY "$memory" SPARK_WORKER_DIR "$runtime/worker" \
      SPARK_MASTER_OPTS "-Dspark.master.rest.enabled=false -Dspark.deploy.defaultCores=$cores"
    if [[ -n "${JAVA_HOME:-}" ]]; then printf 'export JAVA_HOME=%q\n' "$JAVA_HOME"; fi
  } > "$SPARK_CONF_DIR/spark-env.sh"

  # Write spark-defaults.conf for client-mode standalone execution on localhost.
  cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<CONF
# Generated by src/infrastructure/setup_spark.sh
spark.master                       spark://127.0.0.1:$master_port
spark.submit.deployMode            client
spark.driver.host                  127.0.0.1
spark.driver.bindAddress           127.0.0.1
spark.driver.memory                $driver_memory
spark.executor.cores               $cores
spark.executor.memory              $memory
spark.cores.max                    $cores
spark.dynamicAllocation.enabled    false
spark.sql.shuffle.partitions       4
spark.sql.session.timeZone         Europe/Oslo
CONF

  printf 'Configured Spark 4.2.0: %s cores, %s executor, %s driver.\n' "$cores" "$memory" "$driver_memory"
  printf 'Config: %s\nData: %s\nNext: bash %s start\n' "$SPARK_CONF_DIR" "$data_root" "$0"
  exit 0
fi

# For start/status/check/stop, ensure setup has been run and load the generated environment.
[[ -f "$SPARK_CONF_DIR/spark-env.sh" ]] || die "Run '$0 setup' first."
# shellcheck source=/dev/null
source "$SPARK_CONF_DIR/spark-env.sh"
master_url="spark://127.0.0.1:$SPARK_MASTER_PORT"

# Execute the requested lifecycle command against the local standalone cluster.
case "$action" in
  start)
    # Start the standalone Master if it is not already running.
    if ! running "$master_class"; then
      bash "$daemon" start "$master_class" 1 --host 127.0.0.1 \
        --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_MASTER_WEBUI_PORT"
    fi
    running "$master_class" || die "Master failed to start; see $SPARK_LOG_DIR."

    # Start the standalone Worker if it is not already running.
    if ! running "$worker_class"; then
      bash "$daemon" start "$worker_class" 1 --host 127.0.0.1 \
        --port "$SPARK_WORKER_PORT" --webui-port "$SPARK_WORKER_WEBUI_PORT" \
        --cores "$SPARK_WORKER_CORES" --memory "$SPARK_WORKER_MEMORY" \
        --work-dir "$SPARK_WORKER_DIR" "$master_url"
    fi

    # Poll for up to 20 seconds until the Worker registers as ALIVE with the Master.
    for ((attempt=0; attempt<20; attempt++)); do
      if running "$worker_class" && registered; then
        printf 'Spark ready: %s\nMaster UI: http://127.0.0.1:%s\n' "$master_url" "$SPARK_MASTER_WEBUI_PORT"
        exit 0
      fi
      sleep 1
    done
    die "Worker did not register. See $SPARK_LOG_DIR; use '$0 stop' before retrying."
    ;;

  status)
    # Report daemon process state and Worker registration; exit non-zero if anything is unhealthy.
    healthy=0
    for class in "$master_class" "$worker_class"; do
      if running "$class"; then echo "$class: running"; else echo "$class: stopped"; healthy=1; fi
    done
    if registered; then echo "Worker registered with $master_url"; else echo "No registered worker at $master_url"; healthy=1; fi
    exit "$healthy"
    ;;

  check)
    # Run an end-to-end standalone PySpark job that aggregates 100k rows and verifies Parquet read/write.
    registered || die "No registered worker. Run '$0 start' first."
    # Use this installation's defaults, not an inherited local-mode gateway.
    unset PYSPARK_GATEWAY_PORT PYSPARK_GATEWAY_SECRET PYSPARK_SUBMIT_ARGS
    "$spark_python" - "$master_url" "$SPARK_LOCAL_DIRS" <<'PY'
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from pyspark.sql import SparkSession, functions as F

with TemporaryDirectory(prefix="spark_check_", dir=sys.argv[2]) as temporary:
    output = Path(temporary) / "counts.parquet"
    spark = None
    try:
        spark = SparkSession.builder.appName("DAT535-standalone-check").getOrCreate()
        if spark.sparkContext.master != sys.argv[1]:
            raise RuntimeError(f"Expected standalone master, got {spark.sparkContext.master}")
        spark.sparkContext.setLogLevel("WARN")
        counts = spark.range(100_000, numPartitions=4).groupBy((F.col("id") % 100).alias("bucket")).count()
        counts.write.mode("errorifexists").parquet(str(output))
        total = spark.read.parquet(str(output)).agg(F.sum("count").alias("total")).first().total
        if total != 100_000:
            raise RuntimeError(f"Parquet read-back mismatch: {total}")
        print(f"PASS: standalone executor processed {total:,} rows and verified Parquet I/O.")
    finally:
        if spark is not None:
            spark.stop()
PY
    ;;

  stop)
    # Gracefully stop the Worker first, then the Master.
    # Stop only daemons whose recorded PIDs still identify the expected classes.
    for class in "$worker_class" "$master_class"; do
      if running "$class"; then bash "$daemon" stop "$class" 1; else echo "$class: already stopped (no matching process)"; fi
    done
    ;;
esac