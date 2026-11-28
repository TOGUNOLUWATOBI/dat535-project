"""Measure one frozen Gold checkout on Linux; never touch published outputs."""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import time


def memory_sample(group):
    rss = count = 0
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            fields = path.read_text().rsplit(')', 1)[1].split()
            if int(fields[2]) == group:
                rss += int(fields[21]) * os.sysconf('SC_PAGE_SIZE') // 1024
                count += 1
        except (OSError, ValueError, IndexError):
            continue
    available = next(int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()
                     if line.startswith('MemAvailable:'))
    return rss, count, available


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--dates', nargs='+', required=True)
    parser.add_argument('--driver-memory', default='2g')
    parser.add_argument('--silver-runs-dir', default='/srv/dat535/data/silver/kol/runs')
    args = parser.parse_args()
    source, output = args.source_dir.resolve(), args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    events = output / 'spark-events'
    events.mkdir()
    launcher = output / 'launch_gold.py'
    launcher.write_text('import runpy\nrunpy.run_module("pipeline.Gold.build_gold", run_name="__main__")\n')
    digest = hashlib.sha256()
    for path in sorted((source / 'src/pipeline/Gold').glob('*.py')):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    command = ['/usr/bin/time', '-v', '-o', str(output / 'time.txt'),
        '/home/ubuntu/spark-env/bin/spark-submit', '--master', 'local[1]',
        '--driver-memory', args.driver_memory,
        '--conf', 'spark.sql.shuffle.partitions=4',
        '--conf', 'spark.sql.autoBroadcastJoinThreshold=-1',
        '--conf', 'spark.sql.inMemoryColumnarStorage.batchSize=1',
        '--conf', 'spark.sql.files.maxPartitionBytes=16777216',
        '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=false',
        '--conf', 'spark.eventLog.enabled=true', '--conf', 'spark.eventLog.compress=false',
        '--conf', 'spark.eventLog.rolling.enabled=false',
        '--conf', f'spark.eventLog.dir={events.as_uri()}', str(launcher),
        '--gold-run-id', args.run_id, '--dates', *args.dates,
        '--silver-runs-dir', args.silver_runs_dir, '--gold-runs-dir', str(output / 'gold-runs'),
        '--shuffle-partitions', '4', '--driver-memory', args.driver_memory]
    env = dict(os.environ, JAVA_HOME='/home/ubuntu/dat535-runtime/java17/jdk-17.0.21+9-jre',
               PYTHONPATH=str(source / 'src'), PYSPARK_PYTHON='/home/ubuntu/spark-env/bin/python',
               PYTHONDONTWRITEBYTECODE='1')
    for key in ('SPARK_HOME', 'PYSPARK_DRIVER_PYTHON', 'PYSPARK_DRIVER_PYTHON_OPTS'):
        env.pop(key, None)
    report = {'status': 'running', 'revision': args.revision, 'gold_code_sha256': digest.hexdigest(),
              'dates': args.dates, 'command': command, 'platform': platform.platform(),
              'python': platform.python_version(), 'pyspark': importlib.metadata.version('pyspark'),
              'started_at_utc': datetime.now(timezone.utc).isoformat(),
              'rss_definition': 'Sampled sum of RSS for the benchmark process group; shared pages may count more than once.'}
    report_path = output / 'measurement.json'
    report_path.write_text(json.dumps(report, indent=2))
    start = time.monotonic()
    peak = 0
    with (output / 'console.log').open('w') as log, (output / 'memory.csv').open('w') as samples:
        samples.write('elapsed_sec,process_group_rss_kib,process_count,host_available_kib\n')
        process = subprocess.Popen(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        while process.poll() is None:
            rss, count, available = memory_sample(process.pid)
            peak = max(peak, rss)
            samples.write(f'{time.monotonic()-start:.3f},{rss},{count},{available}\n')
            samples.flush()
            time.sleep(1)
        code = process.wait()
    report.update(status='complete' if code == 0 else 'failed', returncode=code,
                  elapsed_sec=round(time.monotonic()-start, 3), sampled_peak_process_group_rss_kib=peak)
    manifest = output / 'gold-runs' / args.run_id / 'gold_manifest.json'
    if manifest.is_file():
        gold = json.loads(manifest.read_text())
        report.update(gate_status=gold['gate_status'], date_authority=gold['date_authority'],
                      table_rows={name: meta['row_count'] for name, meta in gold['output_tables'].items()})
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), flush=True)
    raise SystemExit(code)


if __name__ == '__main__':
    main()
