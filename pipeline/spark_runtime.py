"""Start Spark sessions and bounded workers, and report failures even after the JVM has exited."""

import os
import re
import shlex
import sys


def error_text(error):
    """``Type: message`` for any exception, even one whose ``str()`` needs the dead Java gateway."""
    try:
        detail = str(error)
    except Exception:
        detail = "details unavailable; the Java gateway may have exited"
    return f"{type(error).__name__}: {detail}"


def memory_size(value):
    """Validate a JVM heap size such as ``2g`` or ``1536m`` (also used as an argparse type)."""
    if not re.fullmatch(r"[1-9][0-9]*[mMgG]", value):
        raise ValueError("Driver memory must be a positive size such as 2g or 1536m")
    return value.lower()


def worker_environment(driver_memory="2g"):
    """Environment for a new Spark process with an explicit driver heap.

    The heap must be set before the JVM starts, so it goes into
    ``PYSPARK_SUBMIT_ARGS``. Any existing Python gateway is dropped so the child
    never attaches to its parent's JVM.
    """
    environment = dict(os.environ)
    environment.pop("PYSPARK_GATEWAY_PORT", None)
    environment.pop("PYSPARK_GATEWAY_SECRET", None)
    arguments = shlex.split(environment.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell"))
    # This invocation's explicit budget wins; preserve unrelated submit settings.
    cleaned = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "--driver-memory":
            index += 2
            continue
        if argument.startswith("--driver-memory="):
            index += 1
            continue
        if argument == "--conf" and index + 1 < len(arguments) and arguments[index + 1].startswith("spark.driver.memory="):
            index += 2
            continue
        if argument.startswith("--conf=spark.driver.memory="):
            index += 1
            continue
        cleaned.append(argument)
        index += 1
    environment["PYSPARK_SUBMIT_ARGS"] = shlex.join(["--driver-memory", memory_size(driver_memory), *cleaned])
    environment["PYSPARK_PYTHON"] = sys.executable
    return environment


def start_spark(app_name, master="local[2]", log_level="WARN"):
    """The project's standard Spark session: Oslo time zone, few shuffle partitions, quiet console.

    Local masters run Python workers with this interpreter, so the driver and
    workers use the same environment and package versions.
    """
    from pyspark.sql import SparkSession

    if master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName(app_name).master(master)
             .config("spark.sql.session.timeZone", "Europe/Oslo")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel(log_level)
    return spark


def stop_spark(spark):
    """Stop a session if one was started, without hiding the error that ended the job."""
    if spark is not None:
        try:
            spark.stop()
        except Exception as error:
            # Preserve the original failure and the parent's checkpoint report.
            print(f"Spark shutdown: {error_text(error)}", file=sys.stderr)
