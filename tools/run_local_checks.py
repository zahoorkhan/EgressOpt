#!/usr/bin/env python3
"""EgressOpt public-release LOCAL-ONLY test runner.

All commands in CHECKS are fixed, reviewed offline operations. No cloud CLI
commands, real cloud endpoints, live credentials, or VM provisioning.
The optional TLS fixture binds to 127.0.0.1 only.
"""
from __future__ import annotations
import argparse
import ast
import csv
import importlib.util
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'analysis' / 'phase2' / 'evidence'
REPORT = ROOT / 'analysis' / 'phase2' / 'report_sf1_sf10.py'


def command(label, arguments, timeout=120):
    print(f'\n[CHECK] {label}', flush=True)
    try:
        p = subprocess.run([sys.executable, *arguments], cwd=ROOT,
                           text=True, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f'{label}: timed out after {timeout}s') from None
    print(p.stdout.rstrip()[-1600:], flush=True)
    if p.returncode:
        raise RuntimeError(f'{label}: exit code {p.returncode}')
    return p.stdout


def main():
    parser = argparse.ArgumentParser(description='Safe offline release checks; never uses cloud credentials or cloud VMs')
    parser.add_argument('--with-loopback', action='store_true',
                        help='Also run local-only TLS test (requires openssl and free port 18443) and mocked cloud lifecycle')
    args = parser.parse_args()
    started = time.monotonic()
    count = 0

    # AST parse: a syntactic check that does not import or execute application code.
    files = sorted(ROOT.rglob('*.py'))
    files = [p for p in files if not any(part in {'.git','.venv','venv','__pycache__','results','data'}
                                        for part in p.relative_to(ROOT).parts)]
    for path in files:
        ast.parse(path.read_text(encoding='utf8'), filename=str(path))
    print(f'[PASS] Python syntax: {len(files)} source files', flush=True)
    count += 1

    command('Static credential-pattern scan', ['tools/scan_public_release.py', str(ROOT)])
    count += 1
    out = command('TPC-H SF1/SF10 setup CLI tests (dry-run only, no data/network)',
                  ['tests/test_tpch_setup_cli.py'])
    if 'OK' not in out:
        raise RuntimeError('TPC-H setup CLI tests did not complete successfully')
    count += 1


    out = command('Optimizer selfcheck (tariff direction, SLO, Pareto)', ['egressopt.py', 'selfcheck'])
    if 'EGRESSOPT_SELFCHECK_PASS' not in out:
        raise RuntimeError('optimizer selfcheck did not emit its expected PASS marker')
    count += 1

    for query in ('q03', 'q05'):
        out = command(f'{query.upper()} source/SQL guards (inspect: no execution)',
                      [f'experiments/{query}-phase2.py', '--mode', 'inspect'])
        report = json.loads(out)
        if report['source_guard'] != 'PASS' or report['query'].lower() != query:
            raise RuntimeError(f'{query.upper()}: inspect/source-guard mismatch')
        count += 1

    with tempfile.TemporaryDirectory(prefix='egressopt_offline_report_') as directory:
        out_dir = pathlib.Path(directory)
        arguments = [str(REPORT), '--output', str(out_dir)]
        for query in ('q03', 'q05'):
            for sf in (1, 10):
                path = EVIDENCE / f'{query}_sf{sf}_run.json'
                if not path.is_file():
                    raise RuntimeError(f'Missing packaged evidence: {path.relative_to(ROOT)}')
                arguments.extend([f'--{query}-sf{sf}', str(path)])
        out = command('Reproduce + audit paired SF1/SF10 evidence (no cloud)', arguments, timeout=180)
        if 'EGRESSOPT_SF1_SF10_REPEATED_AUDIT_PASS' not in out or 'measured pairs: 20' not in out:
            raise RuntimeError('Evidence regeneration failed its PASS marker or expected pair count')
        with (out_dir / 'sf1_sf10_summary.csv').open(newline='') as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 4, f'Expected four configurations, got {len(rows)}'
        expected = {('Q03', '1'):87.8516, ('Q03', '10'):87.8327,
                    ('Q05', '1'):83.0709, ('Q05', '10'):83.0846}
        for r in rows:
            label = (r['query'], r['sf'])
            if label not in expected:
                raise RuntimeError(f'Unexpected query/SF: {label}')
            if abs(float(r['payload_paired_reduction_pct'])-expected[label]) > 0.02:
                raise RuntimeError(f'Payload reduction mismatch: {label}')
        for name in ('fig01_cross_cloud_payload.pdf', 'fig02_paired_reductions.pdf',
                     'fig03_worker_vs_controller.pdf', 'sf1_sf10_results.tex'):
            if not (out_dir / name).is_file():
                raise RuntimeError(f'Expected regenerated artifact missing: {name}')
        print('[PASS] Four evidence files; 20 measured pairs; percentages and figures verified', flush=True)
    count += 1

    if args.with_loopback:
        import shutil
        if not shutil.which('openssl'):
            raise RuntimeError('openssl not found: required for the optional local TLS fixture')
        out = command('Loopback-only authenticated TLS, synthetic checksum and mocked VM lifecycle',
                      ['tests/test_m6l_direct_offline.py'], timeout=90)
        for marker in ('M6L_OFFLINE_TEST_PASS', 'M6L_MOCK_VM_LIFECYCLE_PASS'):
            if marker not in out:
                raise RuntimeError(f'Missing mock/TLS success marker: {marker}')
        count += 1

    # Explicitly document a known publication-only source-guard mismatch.
    source = (ROOT/'src/provision_tpch_sf10.py').read_text()
    m = re.search(r"^PHASE1_SHA256\s*=\s*'([0-9a-f]{64})'", source, re.M)
    from hashlib import sha256
    public_hash = sha256((ROOT/'experiments/q05.py').read_bytes()).hexdigest()
    if m and m.group(1) != public_hash:
        print('[WARN] Historical SF10 provisioning source hash differs from sanitized public q05.py. '
              'Cloud deployment is NOT approved by this test run; see docs/PUBLIC_RELEASE_REVIEW.md.',flush=True)

    print(f'\nEGRESSOPT_LOCAL_CHECKS_PASS ({count} offline check groups, {time.monotonic()-started:.1f}s; no cloud calls)',flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (AssertionError, OSError, ValueError, RuntimeError, SyntaxError) as e:
        print(f'EGRESSOPT_LOCAL_CHECKS_FAIL: {e}', file=sys.stderr)
        sys.exit(1)
