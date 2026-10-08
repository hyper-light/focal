#!/usr/bin/env python3
"""Replay a bounded focal-load report and optional write trace, without dependencies."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path

MAX_BYTES = 256 * 1024 * 1024
MAX_ROWS = 2_000_000  # focal-load's 1M logical writes, at most two attempts each


def bounded_text(path):
    with path.open('rb') as source:
        data = source.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError(f'{path} exceeds the {MAX_BYTES}-byte input bound')
    return data.decode('utf-8'), hashlib.sha256(data).hexdigest()


def percentiles(values):
    ordered = sorted(values)
    if not ordered:
        return None
    last = len(ordered) - 1
    return {name: ordered[(last * rank + 500) // 1000] / 1e6
            for name, rank in [('p50', 500), ('p95', 950), ('p99', 990),
                               ('p999', 999), ('max', 1000)]}


def trace_rows(path):
    text, digest = bounded_text(path)
    reader = csv.DictReader(text.splitlines())
    fields = set(reader.fieldnames or [])
    if not {'start_ns', 'latency_ns'} <= fields:
        raise ValueError('CSV needs start_ns and latency_ns')
    rows = []
    for row in reader:
        if len(rows) >= MAX_ROWS:
            raise ValueError(f'CSV exceeds the {MAX_ROWS}-row bound')
        start, latency = int(row['start_ns']), int(row['latency_ns'])
        if start < 0 or latency < 0:
            raise ValueError('negative timing value')
        item = {'start_ns': start, 'latency_ns': latency,
                'finished_ns': start + latency}
        for key in ('sent_ns', 'finished_ns'):
            if key in row:
                item[key] = int(row[key])
        for key in ('worker', 'write', 'attempt', 'epoch', 'request', 'outcome'):
            if key in row:
                item[key] = row[key]
        if 'sent_ns' in item and not start <= item['sent_ns'] <= item['finished_ns']:
            raise ValueError('intended/send/completion timestamps are out of order')
        rows.append(item)
    rows.sort(key=lambda item: item['start_ns'])
    return rows, sorted(fields), digest


def describe(rows):
    return {'samples': len(rows),
            'latency_ms': percentiles([row['latency_ns'] for row in rows])}


def analyze(report_path, csv_path, bin_ms, slow_ms):
    raw, digest = bounded_text(report_path)
    report = json.loads(raw)
    shape = report['shape']
    results = {
        'report': str(report_path), 'report_sha256': digest, 'shape': shape,
        'counts_including_warmup': {key: report.get(key) for key in
                                  ('committed', 'refused', 'unknown', 'expired', 'floors_advanced')},
        'reported_latency_ms': {key: value / 1e6 for key, value in report['latency_ns'].items()},
        'warnings': [],
    }
    if report['refused'] or report['unknown'] or report.get('expired', 0):
        results['warnings'].append('The aggregate latency includes unsuccessful attempts; it is not successful-write latency.')
    if not csv_path:
        results['warnings'].append('No per-write trace: the report alone cannot place or attribute stalls.')
        return results
    rows, fields, csv_digest = trace_rows(csv_path)
    results.update({'csv': str(csv_path), 'csv_sha256': csv_digest, 'columns': fields,
                    'trace': describe(rows)})
    if results['trace']['latency_ms'] != results['reported_latency_ms']:
        results['warnings'].append('CSV percentiles do not exactly reproduce the report.')
    if 'outcome' not in fields:
        results['warnings'].append('The trace has no outcomes: refusal latency cannot be separated after the fact.')
    else:
        by_outcome = {}
        for row in rows:
            by_outcome.setdefault(row['outcome'], []).append(row)
        results['by_outcome'] = {outcome: describe(group) for outcome, group in by_outcome.items()}
    if 'sent_ns' not in fields:
        results['warnings'].append('The trace has no actual-send timestamps: generator backlog cannot be separated from request time.')
    else:
        results['send_delay_ms'] = percentiles([row['sent_ns'] - row['start_ns'] for row in rows])
        results['client_roundtrip_ms'] = percentiles([row['finished_ns'] - row['sent_ns'] for row in rows])
    slow_ns, bin_ns = int(slow_ms * 1e6), int(bin_ms * 1e6)
    by_bin = {}
    for row in rows:
        by_bin.setdefault(row['start_ns'] // bin_ns, []).append(row)
    results['windows'] = [
        {'start_s': slot * bin_ms / 1000, **describe(group),
         'slow_samples': sum(row['latency_ns'] >= slow_ns for row in group)}
        for slot, group in sorted(by_bin.items())
    ]
    interval_ns = (1e9 * shape.get('concurrency', 1) / shape['rate']) if shape.get('rate') else 0
    merge_gap_ns = max(100_000_000, int(interval_ns * 2))
    groups = []
    for row in rows:
        if row['latency_ns'] < slow_ns:
            continue
        if groups and row['start_ns'] - groups[-1][-1]['start_ns'] <= merge_gap_ns:
            groups[-1].append(row)
        else:
            groups.append([row])
    results['slow_threshold_ms'] = slow_ms
    results['slow_samples'] = sum(len(group) for group in groups)
    results['clusters'] = [
        {'start_s': group[0]['start_ns'] / 1e9,
         'end_intended_s': group[-1]['start_ns'] / 1e9,
         'first_completion_s': min(row['finished_ns'] for row in group) / 1e9,
         'last_completion_s': max(row['finished_ns'] for row in group) / 1e9,
         **describe(group)} for group in groups
    ]
    if len(results['clusters']) > 64:
        results['cluster_count'] = len(results['clusters'])
        results['clusters'] = sorted(results['clusters'], key=lambda row: row['latency_ms']['max'], reverse=True)[:64]
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--csv', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--bin-ms', type=float, default=5000)
    parser.add_argument('--slow-ms', type=float, default=50)
    parser.add_argument('--max-p999-ms', type=float, help='Optional diagnostic failure threshold, not a product SLO')
    args = parser.parse_args()
    for name in ('bin_ms', 'slow_ms'):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            parser.error(f'{name} must be positive and finite')
    try:
        result = analyze(args.report, args.csv, args.bin_ms, args.slow_ms)
    except (OSError, ValueError, KeyError, csv.Error) as error:
        parser.exit(2, f'analysis refused: {error}\n')
    text = json.dumps(result, indent=2) + '\n'
    if args.out:
        args.out.write_text(text)
    else:
        print(text, end='')
    if args.max_p999_ms is not None and result['reported_latency_ms']['p999'] > args.max_p999_ms:
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
