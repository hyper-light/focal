#!/usr/bin/env python3
"""Bounded Linux host cgroup sampler. No docker exec per sample and no node mutation.

Example on the benchmark runner, alongside focal-load:
python3 collect_cpu.py --container compose-focal1-1 --container compose-focal2-1 \
  --container compose-focal3-1 --container compose-focal-load-1 --seconds 90 --out cpu.jsonl
"""
import argparse
import json
import math
import subprocess
import time
from pathlib import Path

MAX_CONTAINERS = 8
MAX_FILE_BYTES = 64 * 1024


def read(path):
    try:
        with path.open('rb') as source:
            value = source.read(MAX_FILE_BYTES + 1)
        if len(value) > MAX_FILE_BYTES:
            return {'unavailable': f'exceeds {MAX_FILE_BYTES} bytes'}
        return value.decode('utf-8').strip()
    except (OSError, UnicodeError) as error:
        return {'unavailable': str(error)}


def resolve(name):
    result = subprocess.run(['docker', 'inspect', '--format', '{{.State.Pid}}', name],
                            capture_output=True, text=True, check=True, timeout=10)
    pid = int(result.stdout.strip())
    if pid <= 0:
        raise ValueError(f'{name} has no running process')
    membership = read(Path(f'/proc/{pid}/cgroup'))
    if not isinstance(membership, str):
        raise ValueError(f'{name}: this sampler runs on the Linux Docker host; {membership}')
    for line in membership.splitlines():
        hierarchy, controllers, group = line.split(':', 2)
        if hierarchy == '0' and controllers == '':
            directory = Path('/sys/fs/cgroup') / group.lstrip('/')
            if not directory.is_dir():
                raise ValueError(f'{name}: cgroup directory unavailable: {directory}')
            return directory
    raise ValueError(f'{name}: requires cgroup v2 (record the v1 counters separately)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', action='append', default=[])
    parser.add_argument('--cgroup', action='append', default=[], help='NAME=PATH for an explicit v2 cgroup')
    parser.add_argument('--seconds', type=float, default=90)
    parser.add_argument('--interval-ms', type=float, default=50)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if not math.isfinite(args.seconds) or not 0 < args.seconds <= 900:
        parser.error('seconds must be in (0, 900]')
    if not math.isfinite(args.interval_ms) or not 10 <= args.interval_ms <= 5000:
        parser.error('interval-ms must be in [10, 5000]')
    if not 1 <= len(args.container) + len(args.cgroup) <= MAX_CONTAINERS:
        parser.error(f'choose 1..{MAX_CONTAINERS} containers/cgroups')
    try:
        groups = {name: resolve(name) for name in args.container}
        for explicit in args.cgroup:
            name, directory = explicit.split('=', 1)
            if name in groups or not Path(directory).is_dir():
                raise ValueError(f'invalid or duplicate cgroup {explicit}')
            groups[name] = Path(directory)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(2, f'sampling refused: {error}\n')
    started = time.monotonic_ns()
    deadline = started + int(args.seconds * 1e9)
    interval = int(args.interval_ms * 1e6)
    max_samples = math.ceil(args.seconds * 1000 / args.interval_ms) + 1
    with args.out.open('w') as output:
        metadata = {'event': 'metadata', 'monotonic_ns': started, 'epoch_ns': time.time_ns(),
                    'interval_ms': args.interval_ms, 'duration_s': args.seconds,
                    'groups': {name: {'path': str(directory),
                                     **{key: read(directory / key) for key in
                                        ('cpu.max', 'cpu.weight', 'cpuset.cpus.effective')}}
                               for name, directory in groups.items()}}
        output.write(json.dumps(metadata) + '\n')
        samples = 0
        interrupted = False
        try:
            for slot in range(max_samples):
                wait = started + slot * interval - time.monotonic_ns()
                if wait > 0:
                    time.sleep(wait / 1e9)
                before = time.monotonic_ns()
                if before >= deadline:
                    break
                values = {name: {key: read(directory / key) for key in
                                 ('cpu.stat', 'cpu.pressure', 'io.stat', 'io.pressure',
                                  'memory.current', 'memory.events')}
                          for name, directory in groups.items()}
                row = {'event': 'sample', 'elapsed_ns': before - started,
                       'epoch_ns': time.time_ns(), 'read_duration_ns': time.monotonic_ns() - before,
                       'groups': values}
                output.write(json.dumps(row) + '\n')
                samples += 1
        except KeyboardInterrupt:
            interrupted = True
        output.write(json.dumps({'event': 'complete', 'samples': samples,
                                 'interrupted': interrupted,
                                 'elapsed_ns': time.monotonic_ns() - started}) + '\n')
    print(json.dumps({'out': str(args.out), 'samples': samples, 'interrupted': interrupted}))
    return 130 if interrupted else 0


if __name__ == '__main__':
    raise SystemExit(main())
