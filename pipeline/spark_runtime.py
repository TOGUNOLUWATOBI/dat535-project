"""Start bounded Spark workers and report failures even after the JVM has exited."""

import os
import re
import shlex
import sys


def error_text(error):
    try:
        detail = str(error)
    except Exception:
        detail = "details unavailable; the Java gateway may have exited"
    return f"{type(error).__name__}: {detail}"


def memory_size(value):
    if not re.fullmatch(r"[1-9][0-9]*[mMgG]", value):
        raise ValueError("Driver memory must be a positive size such as 2g or 1536m")
    return value.lower()


def worker_environment(driver_memory="2g"):
    """Set the heap before JVM launch; never inherit an existing Python gateway."""
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


def stop_spark(spark):
    if spark is not None:
        try:
            spark.stop()
        except Exception as error:
            # Preserve the original failure and the parent's checkpoint report.
            print(f"Spark shutdown: {error_text(error)}", file=sys.stderr)
