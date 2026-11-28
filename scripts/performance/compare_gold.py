"""Compare every Gold table as a canonical row multiset, excluding the run ID."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq


def canonical(value):
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return sorted((canonical(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True, default=str))
    return value


def rows(path, excluded):
    result = Counter()
    for file in sorted(path.glob('*.parquet')):
        parquet = pq.ParquetFile(file)
        for batch in parquet.iter_batches(batch_size=2048):
            for row in batch.to_pylist():
                data = canonical({key: value for key, value in row.items() if key not in excluded})
                encoded = json.dumps(data, sort_keys=True, default=str, ensure_ascii=False).encode()
                result[hashlib.sha256(encoded).hexdigest()] += 1
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline', type=Path)
    parser.add_argument('candidate', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    baseline = json.loads((args.baseline / 'gold_manifest.json').read_text())
    candidate = json.loads((args.candidate / 'gold_manifest.json').read_text())
    report = {'baseline_gate': baseline['gate_status'], 'candidate_gate': candidate['gate_status'],
              'same_input_authorities': baseline['date_authority'] == candidate['date_authority'],
              'same_percentile_policy': baseline['percentile_method'] == candidate['percentile_method'],
              'normalization': 'Only gold_run_id excluded; array/map ordering canonicalized; SHA-256 row multisets.',
              'tables': {}}
    same = (set(baseline['output_tables']) == set(candidate['output_tables'])
            and report['same_input_authorities'] and report['same_percentile_policy']
            and report['baseline_gate'] == report['candidate_gate'] == 'passed')
    for name in baseline['output_tables']:
        before_path, after_path = args.baseline / name, args.candidate / name
        before, after = rows(before_path, {'gold_run_id'}), rows(after_path, {'gold_run_id'})
        schema_equal = pq.read_schema(next(before_path.glob('*.parquet'))).equals(
            pq.read_schema(next(after_path.glob('*.parquet'))), check_metadata=False)
        result = {'baseline_rows': sum(before.values()), 'candidate_rows': sum(after.values()),
                  'schema_equal': schema_equal, 'baseline_only_rows': sum((before-after).values()),
                  'candidate_only_rows': sum((after-before).values())}
        result['equal'] = schema_equal and before == after
        report['tables'][name] = result
        same &= result['equal']
    report['equal'] = bool(same)
    args.output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if same else 1)


if __name__ == '__main__':
    main()
