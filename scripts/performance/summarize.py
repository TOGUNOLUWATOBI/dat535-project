"""Summarise finished measurements and Spark task metrics without starting Spark."""

import argparse
import json
from pathlib import Path
import statistics


def spark_metrics(root):
    totals = dict(jobs=0, tasks=0, executor_run_ms=0, jvm_gc_ms=0, input_bytes=0,
                  shuffle_read_bytes=0, shuffle_write_bytes=0, disk_spill_bytes=0)
    for path in sorted(root.rglob('*')):
        if not path.is_file() or path.name.startswith('.'):
            continue
        with path.open() as handle:
            for line in handle:
                event = json.loads(line)
                if event['Event'] == 'SparkListenerJobEnd':
                    totals['jobs'] += 1
                if event['Event'] != 'SparkListenerTaskEnd':
                    continue
                metric = event.get('Task Metrics', {})
                totals['tasks'] += 1
                totals['executor_run_ms'] += metric.get('Executor Run Time', 0)
                totals['jvm_gc_ms'] += metric.get('JVM GC Time', 0)
                totals['input_bytes'] += metric.get('Input Metrics', {}).get('Bytes Read', 0)
                shuffle = metric.get('Shuffle Read Metrics', {})
                totals['shuffle_read_bytes'] += shuffle.get('Remote Bytes Read', 0) + shuffle.get('Local Bytes Read', 0)
                totals['shuffle_write_bytes'] += metric.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                totals['disk_spill_bytes'] += metric.get('Disk Bytes Spilled', 0)
    return totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    reports = {}
    for kind in ('baseline', 'candidate'):
        reports[kind] = []
        for path in sorted(args.root.glob(f'{kind}-*/measurement.json')):
            run = json.loads(path.read_text())
            if run['status'] == 'complete':
                run['measurement_name'] = path.parent.name
                run['spark_metrics'] = spark_metrics(path.parent / 'spark-events')
                reports[kind].append(run)
    summary = {'runs': reports, 'comparison_files': {p.name: json.loads(p.read_text())
               for p in sorted(args.root.glob('comparison-[0-9][0-9].json'))}}
    pairs = [name.removeprefix('comparison-').removesuffix('.json')
             for name, result in summary['comparison_files'].items() if result['equal']]
    pairs = [number for number in pairs if all(any(r['measurement_name'] == f'{kind}-{number}'
             for r in reports[kind]) for kind in reports)]
    summary['paired_repetitions'] = len(pairs)
    if pairs:
        summary['median_seconds'] = {kind: statistics.median(
            r['elapsed_sec'] for r in runs if r['measurement_name'].rsplit('-', 1)[-1] in pairs)
            for kind, runs in reports.items()}
        before, after = (summary['median_seconds'][kind] for kind in ('baseline', 'candidate'))
        summary['median_reduction_percent'] = 100 * (before-after) / before
    (args.root / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps({key: value for key, value in summary.items() if key not in ('runs', 'comparison_files')}, indent=2))


if __name__ == '__main__':
    main()
