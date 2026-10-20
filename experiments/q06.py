#!/usr/bin/env python3
"""EgressOpt Q06 — standalone single-relation negative-control experiment.

Locked SF1/P1: `lineitem` resides on Azure (eastus).  Three physical plans:
  azure_local — filter and SUM on Azure; 0 inter-cloud intermediate bytes.
  aws_remote  — filter/project on Azure, ship tuples to AWS, SUM on AWS.
  gcp_remote  — filter/project on Azure, ship tuples to GCP, SUM on GCP.

One independent Python file: it never imports any other EgressOpt query file.

    python experiments/q06.py --mode prepare --trials 1
    python experiments/q06.py --mode cloud --trials 10
    python experiments/q06.py --mode reproduce
    python experiments/q06.py --mode audit

IMPORTANT: `prepare` is OFFLINE. `cloud` runs an Azure-local control and
two TLS-protected transfers (Azure→AWS/GCP); live Q06 cloud remains untested.
Only HTTP request-body bytes are reported, not provider billing bytes.

Running reproducible cloud work requires authorized accounts, documented cloud
resources and an independent runtime. This runner pins only validated SF1.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import sys
import tempfile
import time
import uuid

QUERY = 'Q06'
PLACEMENT = 'P1'
DEFAULT_SF = int(os.environ.get('EGRESSOPT_SF', '1'))
VALIDATED_SF = (1,)
PLANS = ('azure_local', 'aws_remote', 'gcp_remote')
DATA_CLOUD = 'azure'
DESTINATIONS = {'azure_local': 'azure', 'aws_remote': 'aws', 'gcp_remote': 'gcp'}
EDGES = {'azure_local': (), 'aws_remote': (('azure', 'aws'),),
         'gcp_remote': (('azure', 'gcp'),)}
VERSION = 'q06-sf1-negative-control-cloud-portfix-v3'
Q06_PORT = 8443  # Matches the embedded Q06 HTTPS worker and temporary firewall rules
MONEY = Decimal('0.01')
FIELDS = (
    'run_id', 'query_id', 'scale_factor', 'placement_id', 'execution_mode',
    'plan_id', 'trial', 'execution_cloud', 'transfer_edges',
    'intermediate_rows', 'intermediate_parquet_bytes', 'stage_seconds',
    'aggregate_seconds', 'total_seconds', 'result_row_count',
    'result_sha256', 'correct_vs_reference', 'network_bytes_measured',
    'network_seconds_measured', 'egress_usd_estimated', 'timestamp_utc',
)


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda: f.read(1024 * 1024), b''):
            h.update(part)
    return h.hexdigest()


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + '\n', encoding='utf-8')


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def quoted(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def scan(path: Path) -> str:
    return f'read_parquet({quoted(path)})'


def normalize_result(rows) -> list[dict]:
    """Q06 is one scalar; compare cents to bound SUM order noise.

    Raw FLOAT/DECIMAL values remain recorded in trial evidence for inspection.
    This normalization is not a substitute for validating query semantics.
    """
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError('Q06 must return exactly one scalar revenue value')
    value = rows[0][0]
    if value is None:
        return [{'revenue': None}]
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError('Q06 revenue must be finite')
    return [{'revenue': format(number.quantize(MONEY, rounding=ROUND_HALF_EVEN), '.2f')}]


def result_hash(rows: list[dict]) -> str:
    return digest(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode())


def load_independent_reference(path: Path) -> list[dict]:
    """The existing q06_results.json archive has columns + rows format."""
    if not path.is_file():
        raise FileNotFoundError(f'Independent Q06 reference missing: {path}')
    obj = read_json(path)
    if not isinstance(obj, dict) or obj.get('columns') != ['revenue']:
        raise ValueError('Q06 reference must contain columns=["revenue"]')
    rows = obj.get('rows')
    if not isinstance(rows, list):
        raise ValueError('Q06 reference rows missing')
    return normalize_result(rows)


def strip_sql_comments(text: str) -> str:
    text = re.sub(r'/\*.*?\*/', ' ', text, flags=re.S)
    return re.sub(r'--[^\n]*', ' ', text)


def parse_q06(sql: str) -> dict:
    """Extract WHERE verbatim; never substitute guessed benchmark parameters.

    Supports DuckDB's actual saved Q06 SQL with DATE, CAST and INTERVAL
    predicates, and keeps the entire filter expression unchanged. Rejects
    unfamiliar logical shapes instead of silently changing their semantics.
    """
    clean = strip_sql_comments(sql).strip().rstrip(';').strip()
    if ';' in clean:
        raise ValueError('Q06 must be a single SQL statement')
    # Do not rewrite arithmetic and NULL semantics from an unverified query.
    match = re.fullmatch(
        r'\s*SELECT\s+SUM\s*\(\s*l_extendedprice\s*\*\s*l_discount\s*\)\s*'
        r'(?:AS\s+)?revenue\s+FROM\s+lineitem\s*'
        r'(?:(?:AS\s+)?(?P<alias>[a-zA-Z][a-zA-Z0-9_]*)\s+)?'
        r'WHERE\s+(?P<predicate>.+?)\s*',
        clean, flags=re.I | re.S,
    )
    if not match:
        raise ValueError('Q06 SQL is outside the supported single-table SUM template')
    clause = match.group('predicate').strip()
    normalized = re.sub(r'\s+', ' ', clause).lower()
    for column in ('l_shipdate', 'l_discount', 'l_quantity'):
        if column not in normalized:
            raise ValueError(f'Q06 missing expected filter column: {column}')
    if not re.search(r'\bl_shipdate\s*>=', normalized):
        raise ValueError('Q06 lacks inclusive shipping-date lower bound')
    if not re.search(r'\bl_shipdate\s*<(?!=)', normalized):
        raise ValueError('Q06 lacks exclusive shipping-date upper bound')
    if not re.search(r'\bl_discount\s+between\b', normalized):
        raise ValueError('Q06 lacks discount BETWEEN predicate')
    if not re.search(r'\bl_quantity\s*<(?!=)', normalized):
        raise ValueError('Q06 lacks quantity upper bound')
    if re.search(r'\b(join|union|group\s+by|order\s+by|having|limit|select|delete|update)\b', normalized):
        raise ValueError('Q06 contains unsupported SQL structure')
    alias = match.group('alias')
    return {'predicate_sql': clause, 'table_alias': alias,
            'source_sql_normalized': re.sub(r'\s+', ' ', clean)}


def source_scan_sql(path: Path, params: dict) -> str:
    alias = f' AS {params["table_alias"]}' if params['table_alias'] else ''
    return f'{scan(path)}{alias}'


def qualifying_sql(path: Path, params: dict) -> str:
    return (f'SELECT l_extendedprice, l_discount FROM {source_scan_sql(path, params)} '
            f'WHERE {params["predicate_sql"]}')


def final_sql_from_stage(path: Path) -> str:
    return f'SELECT SUM(l_extendedprice * l_discount) AS revenue FROM {scan(path)}'


def runner_dir(args) -> Path:
    return args.output_dir/'q06'/f'sf{args.sf}'/'p1'


def choose_archive(args) -> Path:
    if args.run_id:
        folder = runner_dir(args)/args.run_id
        if not (folder/'run.json').exists():
            raise FileNotFoundError(f'Q06 run not found: {folder}')
        return folder
    options = []
    for path in runner_dir(args).glob('*/run.json'):
        try:
            info = read_json(path)
            if (info.get('status') == 'success' and info.get('query_id') == QUERY
                and info.get('scale_factor') == f'SF{args.sf}'
                and info.get('placement_id') == PLACEMENT):
                options.append(path.parent)
        except (ValueError, OSError):
            continue
    if not options:
        raise FileNotFoundError('No successful Q06 run found; first run --mode prepare')
    return sorted(options)[-1]


def write_csv(path: Path, rows: list[dict], fields=FIELDS):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict]:
    with path.open('r', newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def local_plan(con, path: Path, params: dict, plan: str, original_sql: str, temp: Path):
    if plan not in PLANS:
        raise ValueError('Unsupported Q06 plan')
    if plan == 'azure_local':
        # The relation is physically local to Azure in the actual P1 placement.
        # Here this is a local emulation of that plan, with zero exchange edges.
        start = time.perf_counter()
        rows = con.execute(original_sql).fetchall()
        elapsed = time.perf_counter() - start
        explanation = '\n'.join(str(x[-1]) for x in con.execute('EXPLAIN '+original_sql).fetchall())
        return {'plan': plan, 'result': normalize_result(rows), 'raw': repr(rows),
                'edges': [], 'rows_shipped': 0, 'bytes_shipped': 0,
                'stage_seconds': 0.0, 'aggregate_seconds': elapsed,
                'total_seconds': elapsed, 'sql': original_sql, 'physical_plan': explanation}

    stage = temp/(plan+'.parquet')
    fragment = qualifying_sql(path, params)
    started = time.perf_counter()
    con.execute(f'COPY ({fragment}) TO {quoted(stage)} (FORMAT PARQUET, COMPRESSION ZSTD)')
    rows_shipped = con.execute(f'SELECT COUNT(*) FROM {scan(stage)}').fetchone()[0]
    stage_seconds = time.perf_counter() - started
    aggregate_sql = final_sql_from_stage(stage)
    started = time.perf_counter()
    rows = con.execute(aggregate_sql).fetchall()
    agg_seconds = time.perf_counter() - started
    explanation = '\n'.join(str(x[-1]) for x in con.execute('EXPLAIN '+aggregate_sql).fetchall())
    target = DESTINATIONS[plan]
    edges = [{'source_cloud': 'azure', 'destination_cloud': target,
              'rows': int(rows_shipped), 'parquet_bytes': stage.stat().st_size,
              'sha256': hash_file(stage), 'measurement': 'LOCAL_FILE_NOT_NETWORK'}]
    return {'plan': plan, 'result': normalize_result(rows), 'raw': repr(rows),
            'edges': edges, 'rows_shipped': int(rows_shipped),
            'bytes_shipped': stage.stat().st_size,
            'stage_seconds': stage_seconds, 'aggregate_seconds': agg_seconds,
            'total_seconds': stage_seconds+agg_seconds,
            'sql': aggregate_sql, 'physical_plan': explanation}


def prepare(args):
    try:
        import duckdb
    except ImportError as e:
        raise RuntimeError('DuckDB is unavailable; activate your existing egressopt environment') from e
    source = args.data_dir/'lineitem.parquet'
    for path in (source, args.query_file, args.reference_file):
        if not path.is_file():
            raise FileNotFoundError(f'Q06 required file missing: {path}')
    sql = args.query_file.read_text(encoding='utf-8')
    params = parse_q06(sql)
    independent = load_independent_reference(args.reference_file)
    with duckdb.connect(':memory:') as conn:
        conn.execute(f'SET threads={args.threads}')
        conn.execute(f'CREATE VIEW lineitem AS SELECT * FROM {scan(source)}')
        try:
            observed_reference = normalize_result(conn.execute(sql).fetchall())
        finally:
            conn.execute('DROP VIEW lineitem')
    if independent != observed_reference:
        raise RuntimeError('Q06 source-SQL result disagrees with independent archived SF1 reference')

    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8]
    folder = runner_dir(args)/run_id
    folder.mkdir(parents=True, exist_ok=False)
    (folder/'source_q06.sql').write_text(sql, encoding='utf-8')
    save_json(folder/'independent_reference.json', independent)
    info = {
        'schema_version': 1, 'runner_build': VERSION, 'query_id': QUERY,
        'scale_factor': f'SF{args.sf}', 'placement_id': PLACEMENT,
        'run_id': run_id, 'status': 'running', 'created_at_utc': utcnow(),
        'execution_mode': 'local_emulation_NO_NETWORK',
        'trial_count_per_plan': args.trials,
        'source_sql_sha256': hash_file(args.query_file),
        'source_sql_copy_sha256': hash_file(folder/'source_q06.sql'),
        'source_sql_path': str(args.query_file.resolve()),
        'source_sql_parameters': params,
        'independent_reference_sha256': result_hash(independent),
        'independent_reference_file_sha256': hash_file(args.reference_file),
        'independent_reference_origin': 'preexisting_results/reference/sf1/q06_results.json',
        'independent_reference_copy_sha256': hash_file(folder/'independent_reference.json'),
        'source_dataset': {'table': 'lineitem', 'cloud': 'azure',
                           'sha256': hash_file(source), 'bytes': source.stat().st_size,
                           'path': str(source.resolve()),
                           'rows': 6001215 if args.sf == 1 else None,
                           'row_count_note': 'SF1 expected from prior dataset manifest, not independently counted here'},
        'runner_sha256': hash_file(Path(__file__)),
        'environment': {'python': platform.python_version(), 'duckdb': duckdb.__version__,
                        'platform': platform.platform(), 'threads': args.threads},
        'timing_definition': 'Local query / Parquet materialization; no inter-cloud transport',
        'network_bytes_measured': False, 'cloud_cost_measured': False,
        'completeness': 'Q06 SF1/P1 local baseline, NOT completed cloud benchmark',
        'round_execution_order': [], 'reference_row_count': 1,
    }
    save_json(folder/'run.json', info)
    rows = []
    try:
        with duckdb.connect(':memory:') as con, tempfile.TemporaryDirectory(prefix='egressopt_q06_') as work:
            con.execute(f'SET threads={args.threads}')
            con.execute(f'CREATE VIEW lineitem AS SELECT * FROM {scan(source)}')
            temporary = Path(work)
            for trial in range(1, args.trials+1):
                order = PLANS[(trial-1)%len(PLANS):] + PLANS[:(trial-1)%len(PLANS)]
                info['round_execution_order'].append({'trial': trial, 'plans': list(order)})
                for plan in order:
                    obs = local_plan(con, source, params, plan, sql, temporary)
                    correct = obs['result'] == independent
                    record = {
                        'run_id': run_id, 'query_id': QUERY, 'scale_factor':f'SF{args.sf}',
                        'placement_id': PLACEMENT, 'execution_mode': 'local_emulation_NO_NETWORK',
                        'plan_id': plan, 'trial': trial,
                        'execution_cloud': DESTINATIONS[plan],
                        'transfer_edges': len(obs['edges']),
                        'intermediate_rows': obs['rows_shipped'],
                        'intermediate_parquet_bytes': obs['bytes_shipped'],
                        'stage_seconds': obs['stage_seconds'],
                        'aggregate_seconds': obs['aggregate_seconds'],
                        'total_seconds': obs['total_seconds'],
                        'result_row_count': len(obs['result']),
                        'result_sha256': result_hash(obs['result']),
                        'correct_vs_reference': correct,
                        'network_bytes_measured': '',
                        'network_seconds_measured': '', 'egress_usd_estimated': '',
                        'timestamp_utc': utcnow(),
                    }
                    explain = folder/f'trial_{trial:02d}_{plan}_physical_plan.txt'
                    explain.write_text(obs['physical_plan']+'\n', encoding='utf-8')
                    save_json(folder/f'trial_{trial:02d}_{plan}.json', {
                        'plan_id': plan, 'trial': trial, 'metrics': record,
                        'result': obs['result'], 'raw_result': obs['raw'],
                        'transfers': obs['edges'], 'fragment_sql': obs['sql'],
                        'physical_plan_file': explain.name,
                        'physical_plan_sha256': hash_file(explain),
                    })
                    rows.append(record)
                    write_csv(folder/'trial_metrics.csv', rows)
                    print(f'{plan:17s} trial={trial} correctness={"PASS" if correct else "FAIL"} '
                          f'emulated_intermediate={obs["bytes_shipped"]:,} bytes '
                          f'edges={len(obs["edges"])} local_workflow={obs["total_seconds"]:.3f}s', flush=True)
                    if not correct:
                        raise RuntimeError(f'Q06 plan {plan} disagrees with archived SQL reference')
        info.update(status='success', completed_at_utc=utcnow(),
                    trial_metrics_sha256=hash_file(folder/'trial_metrics.csv'),
                    measured_trial_count=len(rows))
        save_json(folder/'run.json', info)
        print('Q06_LOCAL_PLAN_VALIDATION_PASS')
        print('Q06 local evidence: '+str(folder.resolve()))
        print('NOTE: no cross-cloud bytes, network seconds or billed egress measured.')
    except Exception as e:
        info.update(status='failed', failure=type(e).__name__+': '+str(e),
                    completed_at_utc=utcnow())
        save_json(folder/'run.json', info)
        raise


def audit(args, quiet=False):
    folder = choose_archive(args)
    info = read_json(folder/'run.json')
    if info.get('execution_mode') == 'real_multicloud':
        return audit_cloud(folder, quiet=quiet)
    errors = []
    metric_file = folder/'trial_metrics.csv'
    if not metric_file.exists():
        raise FileNotFoundError(metric_file)
    records = read_csv(metric_file)
    if hash_file(metric_file) != info.get('trial_metrics_sha256'):
        errors.append('CSV SHA-256 mismatch')
    if info.get('status') != 'success' or info.get('query_id') != QUERY:
        errors.append('Invalid run completion status or query identity')
    if info.get('execution_mode') != 'local_emulation_NO_NETWORK':
        errors.append('Unsupported local evidence mode')
    if (hash_file(folder/'source_q06.sql') != info.get('source_sql_copy_sha256')
        or hash_file(folder/'source_q06.sql') != info.get('source_sql_sha256')):
        errors.append('Source SQL snapshot modified')
    if parse_q06((folder/'source_q06.sql').read_text(encoding='utf-8')) != info['source_sql_parameters']:
        errors.append('Source SQL parsing inconsistent')
    independent = read_json(folder/'independent_reference.json')
    if (hash_file(folder/'independent_reference.json') != info.get('independent_reference_copy_sha256')
        or result_hash(independent) != info.get('independent_reference_sha256')):
        errors.append('Independent result copy modified')
    trial_keys = set()
    for row in records:
        try:
            plan = row['plan_id']; trial = int(row['trial'])
            key = (plan, trial)
            if plan not in PLANS or key in trial_keys:
                raise ValueError('Unknown plan or duplicate trial')
            trial_keys.add(key)
            saved = read_json(folder/f'trial_{trial:02d}_{plan}.json')
            if saved['plan_id'] != plan or saved['trial'] != trial:
                raise ValueError('Saved plan identity mismatch')
            if any(str(saved['metrics'][k]) != row[k] for k in FIELDS):
                raise ValueError('CSV differs from archived trial JSON')
            if (saved['result'] != independent or not row['correct_vs_reference'] == 'True'
                or row['result_sha256'] != result_hash(independent)):
                raise ValueError('Q06 reference correctness mismatch')
            edges = saved['transfers']
            if len(edges) != len(EDGES[plan]) or int(row['transfer_edges']) != len(edges):
                raise ValueError('Incorrect number of transfer edges')
            if [(e['source_cloud'], e['destination_cloud']) for e in edges] != list(EDGES[plan]):
                raise ValueError('Incorrect cloud edges')
            if (sum(int(e['parquet_bytes']) for e in edges) != int(row['intermediate_parquet_bytes'])
                or sum(int(e['rows']) for e in edges) != int(row['intermediate_rows'])):
                raise ValueError('Intermediate byte/row accounting mismatch')
            if row['execution_cloud'] != DESTINATIONS[plan]:
                raise ValueError('Incorrect execution destination')
            if any(not re.fullmatch('[0-9a-f]{64}', e['sha256']) or e['measurement'] != 'LOCAL_FILE_NOT_NETWORK'
                   for e in edges):
                raise ValueError('Stage payload integrity or label invalid')
            if any(row[k] for k in ('network_bytes_measured', 'network_seconds_measured', 'egress_usd_estimated')):
                raise ValueError('Local evidence falsely claims a real network/billed metric')
            if (float(row['stage_seconds']) < 0 or float(row['aggregate_seconds']) < 0
                or abs(float(row['total_seconds'])-float(row['stage_seconds'])-float(row['aggregate_seconds'])) > 1e-8):
                raise ValueError('Invalid timing arithmetic')
            physical = folder/saved['physical_plan_file']
            if hash_file(physical) != saved['physical_plan_sha256']:
                raise ValueError('DuckDB EXPLAIN artifact modified')
            if row['run_id'] != info['run_id'] or row['scale_factor'] != info['scale_factor']:
                raise ValueError('Record run identity invalid')
        except Exception as e:
            errors.append(f'Trial {row.get("plan_id")}/{row.get("trial")}: {type(e).__name__}: {e}')
    expected = {(p, n) for p in PLANS for n in range(1, int(info['trial_count_per_plan'])+1)}
    if trial_keys != expected or len(records) != len(expected):
        errors.append('Missing or duplicate trials')
    report = {'checked_at_utc': utcnow(), 'status': 'PASS' if not errors else 'FAIL',
              'run_id': info['run_id'], 'query_id': QUERY, 'validated_trials': len(records),
              'errors': errors,
              'limitations': ['Archived consistency check, not independent cloud replication',
                              'Intermediate Parquet files not retained; stage hashes are historical assertions',
                              'Offline trial durations are not network timing or monetary charges']}
    save_json(folder/'q06_analysis'/'audit.json', report)
    if errors:
        raise RuntimeError('Q06_EVIDENCE_AUDIT_FAIL: '+ '; '.join(errors[:5]))
    if not quiet:
        print('Q06_EVIDENCE_AUDIT_PASS')
        print('Evidence: '+str(folder.resolve()))
    return folder, records, info


def reproduce(args):
    selected = choose_archive(args)
    if read_json(selected/'run.json').get('execution_mode') == 'real_multicloud':
        return reproduce_cloud(selected,args)
    folder, rows, info = audit(args, quiet=True)
    out = folder/'q06_analysis'
    out.mkdir(exist_ok=True)
    summary = []
    for plan in PLANS:
        subset = [r for r in rows if r['plan_id'] == plan]
        durations = [float(r['total_seconds'])*1000 for r in subset]
        bytes_values = [int(r['intermediate_parquet_bytes']) for r in subset]
        summary.append({'plan_id': plan, 'execution_cloud': DESTINATIONS[plan],
                        'n': len(subset),
                        'mean_duration_ms': statistics.mean(durations),
                        'sample_sd_duration_ms': statistics.stdev(durations) if len(durations)>1 else None,
                        'median_duration_ms': statistics.median(durations),
                        'mean_emulated_parquet_bytes': statistics.mean(bytes_values),
                        'data_transfer_edges': len(EDGES[plan]),
                        'correct_trials': sum(r['correct_vs_reference']=='True' for r in subset),
                        'measurement_type': 'LOCAL_EMULATION_NOT_CLOUD'})
    save_json(out/'paper_summary.json', {'source_run_id': info['run_id'],
        'generated_at_utc': utcnow(), 'query_id': QUERY, 'statistics': summary,
        'scope': 'Local-only candidate validation, not real-cloud benchmark'})
    write_csv(out/'paper_summary.csv', summary, fields=tuple(summary[0]))
    lines = ['% Q06 SF1/P1 — LOCAL EMULATION ONLY, NOT CROSS-CLOUD BENCHMARK',
             r'\begin{tabular}{lrrr}', r'\hline',
             r'Plan & Mean duration (ms) & Parquet bytes & Trials \\', r'\hline']
    for item in summary:
        escaped_name = item['plan_id'].replace('_', r'\_')
        lines.append(f'{escaped_name} & {item["mean_duration_ms"]:.2f} & '
                     f'{item["mean_emulated_parquet_bytes"]:.0f} & {item["n"]} '+r'\\')
    lines.extend([r'\hline',r'\end{tabular}',
                  '% Do not cite as measured cloud transfer or total billed egress.'])
    (out/'q06_local_table.tex').write_text('\n'.join(lines)+'\n', encoding='utf-8')
    if args.plots:
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
        except ImportError as e:
            raise RuntimeError('Matplotlib required for --plots; use --no-plots if absent') from e
        names = [item['plan_id'].replace('_','\n') for item in summary]
        for metric, ylabel, stem in (
            ('mean_duration_ms', 'Mean local workflow (ms)', 'q06_local_duration'),
            ('mean_emulated_parquet_bytes', 'Local intermediate Parquet bytes', 'q06_local_intermediate'),
        ):
            fig, ax = plt.subplots(figsize=(7.3, 4))
            values = [item[metric] for item in summary]
            ax.bar(names, values)
            ax.set_ylabel(ylabel)
            ax.set_title('Q06 / SF1 / P1 — local emulation (no network)')
            ax.grid(axis='y', alpha=.2)
            fig.tight_layout()
            fig.savefig(out/(stem+'.png'), dpi=220)
            fig.savefig(out/(stem+'.pdf'))
            plt.close(fig)
    print('Q06_REPRODUCE_PASS')
    print('Reproduced local Q06 tables/figures: '+str(out.resolve()))



# Q06 CLOUD EXECUTION: stand-alone, per-query source and authentication.
import base64
import gzip
import ipaddress
import secrets
import shlex
import subprocess

AWS_PROFILE=os.environ.get('EGRESSOPT_AWS_PROFILE','')
AWS_REGION=os.environ.get('EGRESSOPT_AWS_REGION','')
AWS_INSTANCE=os.environ.get('EGRESSOPT_AWS_INSTANCE','')
AZ_RG=os.environ.get('EGRESSOPT_AZURE_RESOURCE_GROUP','')
AZ_VM=os.environ.get('EGRESSOPT_AZURE_VM','')
GCP_PROJECT=os.environ.get('EGRESSOPT_GCP_PROJECT','')
GCP_ZONE=os.environ.get('EGRESSOPT_GCP_ZONE','')
GCP_VM=os.environ.get('EGRESSOPT_GCP_VM','')
Q06_CLOUD_VENV={'aws':'/opt/egressopt/venv311/bin/python','azure':'/opt/egressopt/venv/bin/python','gcp':'/opt/egressopt/venv/bin/python'}
Q06_CLOCK_MAX_SKEW_SECONDS=120
Q06_CLOCK_CHECK_INTERVAL_SECONDS=45
_q06_last_clock_check_monotonic=-1.0
_q06_clock_baseline=None


def q06_verify_aws_clock(force=False):
    """Confirm Mac UTC against AWS time; fail closed for a significant skew.

    The probe only fetches a public HTTPS Date response. When the proxy or
    network does not expose Date, fall back to a fresh, read-only AWS STS
    GetCallerIdentity request. This function does NOT modify the system clock.
    """
    global _q06_last_clock_check_monotonic, _q06_clock_baseline
    mono = time.monotonic()
    now = time.time()
    # Also invalidate the cache when wall time and monotonic time diverge,
    # which can happen on a laptop that sleeps or whose clock changes.
    if (not force and _q06_clock_baseline is not None and
        mono - _q06_last_clock_check_monotonic < Q06_CLOCK_CHECK_INTERVAL_SECONDS and
        abs((now - _q06_clock_baseline[0]) -
            (mono - _q06_clock_baseline[1])) < 5):
        return

    from email.utils import parsedate_to_datetime
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen
    errors=[]
    for endpoint in (f'https://sts.{AWS_REGION}.amazonaws.com/',
                     f'https://ssm.{AWS_REGION}.amazonaws.com/'):
        start=time.time()
        req=Request(endpoint+'?q06_clock_probe='+secrets.token_hex(8),
                    method='HEAD', headers={'Cache-Control':'no-cache'})
        try:
            try:
                response=urlopen(req, timeout=8)
            except HTTPError as exc:
                # An unauthenticated 403 is expected; the Date header is
                # still supplied by the remote HTTPS endpoint.
                response=exc
            date_value=response.headers.get('Date')
            if not date_value:
                errors.append(endpoint+': missing Date header')
                continue
            remote=parsedate_to_datetime(date_value).timestamp()
            finish=time.time()
            skew_seconds=remote-(start+finish)/2
            if abs(skew_seconds) > Q06_CLOCK_MAX_SKEW_SECONDS:
                raise RuntimeError(
                    'Q06_CONTROLLER_CLOCK_SKEW: Mac UTC differs from AWS HTTPS '
                    f'time by approximately {skew_seconds:+.0f} seconds; '
                    'open macOS System Settings > General > Date & Time, '
                    'enable Set time and date automatically, confirm network '
                    'time synchronization, and rerun Q06 cleanup BEFORE a '
                    'new paid benchmark. Do not bypass this verification.')
            _q06_last_clock_check_monotonic=time.monotonic()
            _q06_clock_baseline=(time.time(), _q06_last_clock_check_monotonic)
            return
        except RuntimeError:
            raise
        except Exception as exc:
            errors.append(type(exc).__name__+': '+str(exc)[:120])

    # Some networks block unauthenticated HEAD on AWS service endpoints.
    # Accept a fresh signed, read-only call as a weaker fallback; if that call
    # fails, never start cloud resources without a valid authentication check.
    command=['aws','sts','get-caller-identity','--profile',AWS_PROFILE,
             '--region',AWS_REGION,'--output','json']
    try:
        proc=subprocess.run(command,capture_output=True,text=True,timeout=40)
    except Exception as exc:
        raise RuntimeError('Q06 cannot verify AWS controller time/authentication: '
                           +'; '.join(errors)+'; '+str(exc)) from exc
    if proc.returncode != 0:
        raise RuntimeError('Q06 AWS clock/authentication preflight failed; '
            'verify macOS automatic date/time and AWS SSO login. '
            'AWS STS: '+proc.stderr[-650:]+'; HTTPS probes: '+'; '.join(errors))
    print('  AWS signed STS preflight passed (HTTPS Date unavailable)',flush=True)
    _q06_last_clock_check_monotonic=time.monotonic()
    _q06_clock_baseline=(time.time(), _q06_last_clock_check_monotonic)

def q06_aws_signature_error(message):
    if ('Signature expired:' in message or
        'Signature not yet current' in message or
        'RequestTimeTooSkewed' in message):
        # Do not retry SendCommand: an expired signature must be diagnosed,
        # and blindly retrying remote commands risks duplicate execution.
        return RuntimeError('Q06_AWS_SIGNATURE_TIME_FAILURE: '+message[-400:]+
            ' | Verify macOS automatic date/time, then run --mode cleanup '
            'for this FAILED run. Do not merge its partial results.')
    return None

def q06_cli(cmd,timeout=480):
 if cmd and cmd[0]=='aws':
  q06_verify_aws_clock()
 try:p=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout)
 except FileNotFoundError as e:raise RuntimeError('Missing cloud CLI '+cmd[0]) from e
 if p.returncode:
  error_text=(p.stderr or p.stdout)[-1300:]
  signature_error=q06_aws_signature_error(error_text)
  if signature_error:raise signature_error
  raise RuntimeError(f'{cmd[0]} failed: '+error_text)
 return p.stdout.strip()

def q06_aws(*arg):
 return json.loads(q06_cli(['aws',*arg,'--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json']))

def q06_gcloud(*args,timeout=450):
 return q06_cli(['gcloud',*args,'--project='+GCP_PROJECT],timeout=timeout)

def q06_aws_instance():
 result=q06_aws('ec2','describe-instances','--instance-ids',AWS_INSTANCE)
 arr=[inst for res in result['Reservations'] for inst in res['Instances']]
 if len(arr)!=1:raise RuntimeError('Q06 AWS instance not unique')
 return arr[0]

def q06_azure_state():
 response=json.loads(q06_cli(['az','vm','get-instance-view','--resource-group',AZ_RG,
 '--name',AZ_VM,'--query','instanceView.statuses[].code','--output','json']))
 power=[s for s in response if s.startswith('PowerState/')]
 if len(power)!=1:raise RuntimeError('Cannot obtain Azure power state')
 return power[0]

def q06_gcp_instance():
 return json.loads(q06_gcloud('compute','instances','describe',GCP_VM,
                           '--zone='+GCP_ZONE,'--format=json'))

def q06_start_vm(cloud):
 if cloud=='aws':
  old=q06_aws_instance()['State']['Name']
  if old=='running':return False
  if old!='stopped':raise RuntimeError('Unsupported AWS state: '+old)
  q06_aws('ec2','start-instances','--instance-ids',AWS_INSTANCE)
  q06_cli(['aws','ec2','wait','instance-running','--instance-ids',AWS_INSTANCE,
           '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=550)
  return True
 if cloud=='azure':
  old=q06_azure_state()
  if old=='PowerState/running':return False
  if old not in ('PowerState/stopped','PowerState/deallocated'):
   raise RuntimeError('Unsupported Azure state: '+old)
  q06_cli(['az','vm','start','--resource-group',AZ_RG,'--name',AZ_VM,'--output','none'],timeout=650)
  return True
 if cloud=='gcp':
  old=q06_gcp_instance()['status']
  if old=='RUNNING':return False
  if old not in ('TERMINATED','STOPPED'):raise RuntimeError('Unsupported GCP state: '+old)
  q06_gcloud('compute','instances','start',GCP_VM,'--zone='+GCP_ZONE,'--quiet','--format=json',timeout=650)
  return True
 raise ValueError(cloud)

def q06_remote(cloud,script,label):
 if cloud=='azure':
  cmd=['az','vm','run-command','invoke','--resource-group',AZ_RG,'--name',AZ_VM,
       '--command-id','RunShellScript','--scripts','set -eu\n'+script,
       '--query','value[0].message','--output','tsv']
  for attempt in range(1,11):
   try:out=q06_cli(cmd,timeout=470)
   except RuntimeError as e:
    if ('OperationNotAllowed' not in str(e) or
      'requires the VM to be running' not in str(e) or attempt==10):raise
    print(f'  Azure Run Command warming up ({attempt}/10): {label}',flush=True)
    time.sleep(min(4+2*attempt,20));continue
   if 'Enable succeeded' not in out:
    raise RuntimeError('Q06 Azure remote '+label+': '+out[-900:])
   return out
 if cloud=='gcp':
  code='sudo -n bash -lc '+shlex.quote('set -eu\n'+script)
  # Retry only the IAP 4003 error that explicitly states port 22 could not
  # connect. The guest command did not start in this failure mode, which makes
  # retrying safe even for non-idempotent upload/deployment steps. Never retry
  # arbitrary SSH exit 255 or an interrupted connection after execution began.
  for attempt in range(1,6):
   try:
    return q06_gcloud('compute','ssh',GCP_VM,'--zone='+GCP_ZONE,
      '--tunnel-through-iap','--quiet','--command='+code,timeout=500)
   except RuntimeError as exc:
    err=str(exc)
    preconnect=(('4003' in err and 'failed to connect to backend' in err.lower())
       and ('port 22' in err or 'ConnectionCreationError' in err))
    if not preconnect:
     raise
    try:
     vm_state=q06_gcp_instance()['status']
    except Exception as state_exc:
     raise RuntimeError('Q06 GCP IAP SSH failed, VM status unavailable; '+
      'do not retry query blindly: '+str(state_exc)) from exc
    if vm_state!='RUNNING':
     raise RuntimeError('Q06 GCP IAP SSH failed and VM state is '+vm_state+
       '; inspect guest startup/stop and prior cleanup') from exc
    if attempt==5:
     raise RuntimeError('Q06 GCP IAP SSH still cannot connect to VM port 22 '
       'after 5 pre-connection attempts while instance is RUNNING. '
       'Check IAP allow tcp:22 from 35.235.240.0/20, guest sshd, and '
       'VM health; do not start a new benchmark before cleanup.') from exc
    delay=min(3*attempt,12)
    print(f'  GCP IAP port 22 not ready ({attempt}/5); VM RUNNING; '
      f'retrying in {delay}s [{label}]',flush=True)
    time.sleep(delay)
 if cloud=='aws':
  for attempt in range(1,9):
   try:
    cid=q06_cli(['aws','ssm','send-command','--instance-ids',AWS_INSTANCE,
      '--document-name','AWS-RunShellScript',
      '--parameters',json.dumps({'commands':['set -eu',script]}),
      '--comment',('EgressOpt Q06 '+label)[:100],
      '--query','Command.CommandId','--output','text','--profile',AWS_PROFILE,
      '--region',AWS_REGION],timeout=100)
    break
   except RuntimeError as e:
    if ('InvalidInstanceId' not in str(e) and 'TargetNotConnected' not in str(e)) or attempt==8:
     raise
    time.sleep(min(5*attempt,20))
  deadline=time.monotonic()+470
  while time.monotonic()<deadline:
   q06_verify_aws_clock()
   p=subprocess.run(['aws','ssm','get-command-invocation','--command-id',cid,
    '--instance-id',AWS_INSTANCE,'--profile',AWS_PROFILE,'--region',AWS_REGION,
    '--output','json'],capture_output=True,text=True,timeout=50)
   if p.returncode:
    if 'InvocationDoesNotExist' in p.stderr:time.sleep(2);continue
    signature_error=q06_aws_signature_error(p.stderr)
    if signature_error:raise signature_error
    raise RuntimeError('Q06 AWS SSM '+label+': '+p.stderr[-900:])
   obj=json.loads(p.stdout)
   if obj['Status']=='Success':return obj.get('StandardOutputContent','')
   if obj['Status'] in {'Failed','TimedOut','Cancelled','Cancelling'}:
    raise RuntimeError('Q06 AWS SSM '+label+': '+obj['Status']+' '+
       obj.get('StandardErrorContent','')[-850:]+obj.get('StandardOutputContent','')[-850:])
   time.sleep(2)
  raise RuntimeError('Q06 AWS SSM timeout: '+label)
 raise ValueError(cloud)

def q06_marker(out,prefix='Q06_RPC='):
 found=[line.split(prefix,1)[1].strip() for line in out.splitlines() if line.startswith(prefix)]
 if len(found)!=1:raise RuntimeError('Expected one '+prefix+' remote response: '+out[-350:])
 return found[0]

def q06_rpc(cloud,script,label):
 return json.loads(q06_marker(q06_remote(cloud,script,label)))

def q06_ip(raw):
 ip=ipaddress.ip_address(str(raw).strip())
 if ip.version!=4 or not ip.is_global:raise RuntimeError('Invalid public IPv4: '+str(raw))
 return str(ip)

def q06_outbound(cloud):
 out=q06_remote(cloud,'curl --silent --show-error --fail --connect-timeout 10 --max-time 30 https://checkip.amazonaws.com','public-egress-ip')
 if cloud=='azure' and '[stdout]' in out:out=out.split('[stdout]',1)[1].split('[stderr]',1)[0]
 return q06_ip(out.strip())

def q06_gcp_public(inst):
 for nic in inst['networkInterfaces']:
  for cfg in nic.get('accessConfigs',[]):
   if cfg.get('natIP'):return q06_ip(cfg['natIP'])
 raise RuntimeError('GCP VM has no public IP')

def q06_aws_rule(source_ip,source,key,group):
 permission=[{'IpProtocol':'tcp','FromPort':Q06_PORT,'ToPort':Q06_PORT,
   'IpRanges':[{'CidrIp':source_ip+'/32','Description':'EgressOpt Q06 '+key+' '+source}]}]
 created=q06_aws('ec2','authorize-security-group-ingress','--group-id',group,
                 '--ip-permissions',json.dumps(permission))
 rules=created.get('SecurityGroupRules',[])
 if len(rules)!=1 or not rules[0].get('SecurityGroupRuleId'):
  raise RuntimeError('AWS Q06 ingress created with missing ID; inspect SG')
 return rules[0]['SecurityGroupRuleId']

def q06_gcp_rule(source_ip,key,network,source):
 tag='egressopt-q06-'+key
 rule=tag+'-'+source
 if tag not in q06_gcp_instance().get('tags',{}).get('items',[]):
  q06_gcloud('compute','instances','add-tags',GCP_VM,'--zone='+GCP_ZONE,
            '--tags='+tag,'--quiet','--format=json')
 # Both AWS and Azure may send Q06 intermediates into the GCP join plan.
 q06_gcloud('compute','firewall-rules','create',rule,'--network='+network,
    '--direction=INGRESS','--action=ALLOW','--rules=tcp:8443',
    '--source-ranges='+source_ip+'/32','--target-tags='+tag,
    '--description=EgressOpt Q06 temporary TLS from '+source,
    '--quiet','--format=json')
 return rule,tag

Q06_WORKER_SOURCE = '#!/usr/bin/env python3\n"""Ephemeral Q06 HTTPS worker embedded in the independently runnable q06.py."""\nimport argparse\nimport base64\nfrom decimal import Decimal, ROUND_HALF_EVEN\nimport hashlib, hmac, json, os, re, ssl, time\nfrom http.server import ThreadingHTTPServer, BaseHTTPRequestHandler\nfrom pathlib import Path\nimport duckdb\n\nCLOUD = os.environ[\'Q06_CLOUD\']\nROOT = Path(os.environ[\'Q06_RUNTIME\'])\nDATA = Path(os.environ[\'Q06_DATA_DIR\'])\nTOKEN = os.environ[\'Q06_WORKER_TOKEN\']\nSOURCE_SQL = base64.b64decode(os.environ[\'Q06_SQL_B64\']).decode()\nSTAGE_SQL = base64.b64decode(os.environ[\'Q06_STAGE_SQL_B64\']).decode()\nPORT = 8443\nPLANS = (\'azure_local\',\'aws_remote\',\'gcp_remote\')\nDEST = {\'azure_local\':\'azure\',\'aws_remote\':\'aws\',\'gcp_remote\':\'gcp\'}\nSLUG = re.compile(r\'[wm][0-9]{2,3}\\Z\')\nSHA = re.compile(r\'[a-f0-9]{64}\\Z\')\nif CLOUD not in {\'azure\',\'aws\',\'gcp\'}: raise ValueError(\'Invalid cloud\')\n\ndef lit(path):\n    return "\'" + str(path).replace("\'","\'\'") + "\'"\n\ndef scan(path):\n    return \'read_parquet(\'+lit(path)+\')\'\n\ndef data_file():\n    if CLOUD!=\'azure\': raise RuntimeError(\'Q06 lineitem only lives in Azure P1\')\n    path=DATA/\'lineitem.parquet\'\n    if not path.is_file(): raise FileNotFoundError(path)\n    return path\n\ndef normalized(rows):\n    if len(rows)!=1 or len(rows[0])!=1: raise RuntimeError(\'Q06 expected one scalar\')\n    v=rows[0][0]\n    if v is None:return [{\'revenue\':None}]\n    d=Decimal(str(v))\n    if not d.is_finite():raise ValueError(\'Invalid Q06 revenue\')\n    return [{\'revenue\':format(d.quantize(Decimal(\'0.01\'), rounding=ROUND_HALF_EVEN), \'.2f\')}]\n\ndef hash_file(path):\n    h=hashlib.sha256()\n    with open(path,\'rb\') as f:\n        for block in iter(lambda:f.read(1048576),b\'\'):h.update(block)\n    return h.hexdigest()\n\ndef stage_path(trial,plan):\n    return ROOT/\'stage\'/trial/plan/\'selected.parquet\'\n\ndef recv_path(trial,plan):\n    return ROOT/\'received\'/trial/plan/\'selected.parquet\'\n\nclass Handler(BaseHTTPRequestHandler):\n    server_version=\'EgressOptQ06/1.0\'\n    def log_message(self,*args):pass\n    def respond(self,code,obj):\n        body=json.dumps(obj,sort_keys=True,separators=(\',\',\':\')).encode()\n        self.send_response(code)\n        self.send_header(\'Content-Type\',\'application/json\')\n        self.send_header(\'Content-Length\',str(len(body)))\n        self.end_headers();self.wfile.write(body)\n    def allowed(self):\n        if not hmac.compare_digest(self.headers.get(\'Authorization\',\'\'), \'Bearer \'+TOKEN):\n            self.respond(401,{\'error\':\'unauthorized\'});return False\n        return True\n    def request_info(self):\n        n=int(self.headers.get(\'Content-Length\',\'0\'))\n        if not 1<=n<=2048:raise ValueError(\'Invalid JSON request length\')\n        obj=json.loads(self.rfile.read(n))\n        trial=obj.get(\'trial_id\',\'\'); plan=obj.get(\'plan_id\',\'\')\n        if not isinstance(trial,str) or not SLUG.fullmatch(trial) or plan not in PLANS:\n            raise ValueError(\'Invalid plan/trial\')\n        return obj,trial,plan\n    def do_GET(self):\n        if not self.allowed():return\n        if self.path==\'/health\':\n            self.respond(200,{\'cloud\':CLOUD,\'query\':\'Q06\',\'duckdb\':duckdb.__version__,\'status\':\'ok\'})\n        else:self.respond(404,{\'error\':\'not found\'})\n    def do_POST(self):\n        if not self.allowed():return\n        try:\n            if self.path==\'/q06/stage\':\n                obj,trial,plan=self.request_info()\n                if CLOUD!=\'azure\' or plan==\'azure_local\' or obj.get(\'part\')!=\'selected\':\n                    raise ValueError(\'Q06 stage only allowed on Azure for remote plans\')\n                path=stage_path(trial,plan);path.parent.mkdir(parents=True,exist_ok=True)\n                t0=time.perf_counter()\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    con.execute(\'COPY (\'+STAGE_SQL+\') TO \'+lit(path)+\' (FORMAT PARQUET,COMPRESSION ZSTD)\')\n                    n=con.execute(\'SELECT COUNT(*) FROM \'+scan(path)).fetchone()[0]\n                self.respond(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'part\':\'selected\',\n                    \'rows\':int(n),\'parquet_bytes\':path.stat().st_size,\n                    \'sha256\':hash_file(path),\'stage_seconds\':time.perf_counter()-t0,\n                    \'file_path\':str(path)})\n            elif self.path.startswith(\'/q06/upload/\'):\n                segments=self.path.split(\'/\')\n                if len(segments)!=6 or segments[1:3]!=[\'q06\',\'upload\']:\n                    raise ValueError(\'Wrong Q06 upload path\')\n                trial,plan,part=segments[3:]\n                if not SLUG.fullmatch(trial) or plan not in (\'aws_remote\',\'gcp_remote\') or part!=\'selected\' or CLOUD!=DEST[plan]:\n                    raise ValueError(\'Wrong Q06 upload destination\')\n                expected=self.headers.get(\'X-Q06-SHA256\',\'\')\n                if not SHA.fullmatch(expected):raise ValueError(\'Invalid upload SHA\')\n                remaining=int(self.headers.get(\'Content-Length\',\'0\'))\n                if not 1<=remaining<=300_000_000:raise ValueError(\'Invalid payload length\')\n                path=recv_path(trial,plan);path.parent.mkdir(parents=True,exist_ok=True)\n                tmp=path.with_suffix(\'.part\');received=0;h=hashlib.sha256()\n                t0=time.perf_counter()\n                with tmp.open(\'wb\') as f:\n                    while remaining:\n                        block=self.rfile.read(min(1048576,remaining))\n                        if not block:raise ValueError(\'Truncated transfer\')\n                        f.write(block);h.update(block);received+=len(block);remaining-=len(block)\n                if not hmac.compare_digest(h.hexdigest(),expected):\n                    tmp.unlink(missing_ok=True);raise ValueError(\'Upload SHA mismatch\')\n                tmp.replace(path)\n                self.respond(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'part\':part,\n                    \'sha256\':h.hexdigest(),\'received_payload_bytes\':received,\n                    \'receiver_file_seconds\':time.perf_counter()-t0})\n            elif self.path==\'/q06/finish\':\n                obj,trial,plan=self.request_info()\n                if CLOUD!=DEST[plan]:raise ValueError(\'Wrong finish provider\')\n                if plan==\'azure_local\':\n                    file=data_file()\n                    sql=SOURCE_SQL\n                else:\n                    file=recv_path(trial,plan)\n                    if not file.is_file():raise RuntimeError(\'Q06 intermediate not received\')\n                    sql=\'SELECT SUM(l_extendedprice * l_discount) AS revenue FROM \'+scan(file)\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    if plan==\'azure_local\':\n                        con.execute(\'CREATE VIEW lineitem AS SELECT * FROM \'+scan(file))\n                    explain=\'\\n\'.join(str(x[-1]) for x in con.execute(\'EXPLAIN \'+sql).fetchall())\n                    t0=time.perf_counter();raw=con.execute(sql).fetchall();duration=time.perf_counter()-t0\n                result=normalized(raw)\n                self.respond(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\n                  \'result\':result,\'row_count\':len(result),\n                  \'result_sha256\':hashlib.sha256(json.dumps(result,sort_keys=True,separators=(\',\',\':\')).encode()).hexdigest(),\n                  \'aggregate_seconds\':duration,\'raw_result_strings\':[[str(v) for v in row] for row in raw],\n                  \'physical_plan_sha256\':hashlib.sha256(explain.encode()).hexdigest()})\n            else:self.respond(404,{\'error\':\'not found\'})\n        except Exception as e:\n            self.respond(422,{\'error\':type(e).__name__+\': \'+str(e)[:500]})\n\ndef main():\n    p=argparse.ArgumentParser()\n    p.add_argument(\'--cert\',required=True);p.add_argument(\'--key\',required=True)\n    args=p.parse_args()\n    server=ThreadingHTTPServer((\'0.0.0.0\' if CLOUD!=\'azure\' else \'127.0.0.1\',PORT), Handler)\n    ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n    ctx.minimum_version=ssl.TLSVersion.TLSv1_2\n    ctx.load_cert_chain(args.cert,args.key)\n    server.socket=ctx.wrap_socket(server.socket,server_side=True)\n    server.serve_forever()\nif __name__==\'__main__\':main()\n'


def q06_install_worker(cloud, key):
    source=Q06_WORKER_SOURCE.encode()
    release='/opt/egressopt/releases/q06_'+digest(source)[:12]
    temp=f'/opt/egressopt/runtime/q06_upload_{key}_{cloud}'
    encoded=base64.b64encode(gzip.compress(source)).decode()
    q06_remote(cloud,'umask 077; mkdir -p '+shlex.quote(temp)+' '+shlex.quote(release)+
               '; : > '+shlex.quote(temp+'/worker.gz.b64'),'prepare-Q06-worker')
    for offset in range(0,len(encoded),2400):
        q06_remote(cloud,'printf %s '+shlex.quote(encoded[offset:offset+2400])+
                   ' >> '+shlex.quote(temp+'/worker.gz.b64'),'upload-Q06-worker')
    py=Q06_CLOUD_VENV[cloud];dest=release+'/worker.py'
    script=('base64 -d '+shlex.quote(temp+'/worker.gz.b64')+' | gzip -d > '+shlex.quote(dest)+'\n'+
            'test "$(sha256sum '+shlex.quote(dest)+" | cut -d' ' -f1)\" = "+shlex.quote(digest(source))+'\n'+
            py+' -m py_compile '+shlex.quote(dest)+'\n'+
            'rm -rf '+shlex.quote(temp)+'\n'+'echo Q06_WORKER_DEPLOYED')
    out=q06_remote(cloud,script,'verify-Q06-worker')
    if 'Q06_WORKER_DEPLOYED' not in out:raise RuntimeError('Q06 worker release failed validation on '+cloud)
    return release


def q06_start_service(cloud,key,release,public_ip,token,sql,stage_sql):
    work=f'/opt/egressopt/runtime/q06_{key}_{cloud}'
    unit=f'egressopt-q06-{key}-{cloud}'
    py=Q06_CLOUD_VENV[cloud]
    launch='\n'.join(['#!/bin/sh','set -eu',
        'export Q06_CLOUD='+shlex.quote(cloud),
        'export Q06_RUNTIME='+shlex.quote(work),
        'export Q06_DATA_DIR=/opt/egressopt/data/tpch_sf1',
        'export Q06_WORKER_TOKEN='+shlex.quote(token),
        'export Q06_SQL_B64='+shlex.quote(base64.b64encode(sql.encode()).decode()),
        'export Q06_STAGE_SQL_B64='+shlex.quote(base64.b64encode(stage_sql.encode()).decode()),
        'exec '+py+' '+shlex.quote(release+'/worker.py')+
        ' --cert '+shlex.quote(work+'/server.pem')+' --key '+shlex.quote(work+'/server.key')])+'\n'
    launch64=base64.b64encode(launch.encode()).decode()
    required='test -f /opt/egressopt/data/tpch_sf1/lineitem.parquet\n' if cloud=='azure' else ''
    script=f'''umask 077
mkdir -p {shlex.quote(work)}
test -f {shlex.quote(release+'/worker.py')}
{required}{py} -c 'import duckdb; assert duckdb.__version__ == "1.5.6", duckdb.__version__'
command -v openssl >/dev/null
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 \
 -keyout {work}/ca.key -out {work}/ca.pem -subj '/CN=EgressOpt Q06 temporary CA' \
 -addext 'basicConstraints=critical,CA:TRUE' >/dev/null 2>&1
openssl req -nodes -newkey rsa:2048 -sha256 \
 -keyout {work}/server.key -out {work}/server.csr \
 -subj '/CN=EgressOpt Q06 worker' >/dev/null 2>&1
printf 'basicConstraints=CA:FALSE\nsubjectAltName=IP:{public_ip}\nextendedKeyUsage=serverAuth\n' > {work}/server.ext
openssl x509 -req -days 1 -sha256 -in {work}/server.csr \
 -CA {work}/ca.pem -CAkey {work}/ca.key -CAcreateserial \
 -extfile {work}/server.ext -out {work}/server.pem >/dev/null 2>&1
printf %s {shlex.quote(launch64)} | base64 -d > {work}/launch.sh
chmod 700 {work}/launch.sh
systemd-run --unit={unit} --collect --property=Restart=no /bin/sh {work}/launch.sh >/dev/null
STATUS=000
for i in $(seq 1 40); do
 STATUS=$(curl --insecure -s -o /dev/null -w '%{{http_code}}' --max-time 3 https://127.0.0.1:8443/health || true)
 [ "$STATUS" = 401 ] && break
 sleep 1
done
test "$STATUS" = 401
curl --silent --show-error --fail --insecure --max-time 10 \
 -H {shlex.quote('Authorization: Bearer '+token)} https://127.0.0.1:8443/health | \
 {py} -c 'import sys,json; x=json.load(sys.stdin); assert x["cloud"]=="{cloud}" and x["query"]=="Q06"'
echo Q06_SERVICE_READY
printf 'Q06_CA_B64='; base64 -w0 {work}/ca.pem; echo
'''
    out=q06_remote(cloud,script,'start-Q06-worker')
    if 'Q06_SERVICE_READY' not in out:raise RuntimeError('Q06 authenticated TLS worker failed on '+cloud)
    ca=q06_marker(out,'Q06_CA_B64=')
    if b'BEGIN CERTIFICATE' not in base64.b64decode(ca,validate=True):
        raise RuntimeError('Q06 CA invalid on '+cloud)
    return ca


def q06_stage(plan,trial,token,key):
    cloud='azure'
    work=f'/opt/egressopt/runtime/q06_{key}_{cloud}'
    payload=json.dumps({'part':'selected','plan_id':plan,'trial_id':trial},separators=(',',':'))
    reply=work+'/stage_response.json';py=Q06_CLOUD_VENV[cloud]
    script=('curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 300 '+
        '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
        '-H '+shlex.quote('Content-Type: application/json')+' '+
        '--data-binary '+shlex.quote(payload)+' '+
        'https://127.0.0.1:8443/q06/stage > '+shlex.quote(reply)+'\n'+
        py+' -c '+shlex.quote('import json; x=json.load(open('+repr(reply)+')); '+
        'print("Q06_RPC="+json.dumps(x,separators=(",",":")))'))
    out=q06_rpc(cloud,script,'stage-'+plan)
    expected=f'{work}/stage/{trial}/{plan}/selected.parquet'
    if (out.get('cloud'),out.get('plan_id'),out.get('trial_id'),out.get('part'))!=('azure',plan,trial,'selected'):
        raise RuntimeError('Q06 stage identity mismatch')
    if out.get('file_path')!=expected or not re.fullmatch('[a-f0-9]{64}',out.get('sha256','')):
        raise RuntimeError('Q06 stage artifact mismatch')
    if int(out.get('parquet_bytes',0))<=0 or int(out.get('rows',-1))<0:
        raise RuntimeError('Q06 invalid intermediate')
    return out


def q06_upload(destination,staged,plan,trial,ips,cas,token,key):
    source='azure'
    work=f'/opt/egressopt/runtime/q06_{key}_{source}'
    ca=work+'/peer-ca.pem';reply=work+'/upload_response.json'
    url=f'https://{ips[destination]}:8443/q06/upload/{trial}/{plan}/selected'
    py=Q06_CLOUD_VENV[source]
    script=('printf %s '+shlex.quote(cas[destination])+' | base64 -d > '+shlex.quote(ca)+'\n'+
        'INFO=$(curl --fail --silent --show-error --connect-timeout 25 --max-time 400 '+
        '--cacert '+shlex.quote(ca)+' '+
        '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
        '-H '+shlex.quote('X-Q06-SHA256: '+staged['sha256'])+' '+
        "-H 'Content-Type: application/octet-stream' "+
        '--data-binary @'+shlex.quote(staged['file_path'])+' '+
        '-o '+shlex.quote(reply)+' '+
        "-w '%{time_total} %{size_upload} %{http_code}' "+shlex.quote(url)+')\n'+
        'export INFO\n'+py+' -c '+shlex.quote(
        'import json,os; a,b,c=os.environ["INFO"].split(); r=json.load(open('+repr(reply)+')); '+
        'print("Q06_RPC="+json.dumps({"http_seconds":float(a),"payload_bytes":int(b),'+
        '"http_status":int(c),"response":r},separators=(",",":")))'))
    out=q06_rpc(source,script,'transfer-'+plan)
    ack=out['response']
    if (out['http_status']!=200 or ack.get('cloud')!=destination or
        ack.get('plan_id')!=plan or ack.get('trial_id')!=trial or ack.get('part')!='selected' or
        ack.get('sha256')!=staged['sha256'] or
        int(out['payload_bytes'])!=int(staged['parquet_bytes']) or
        int(ack['received_payload_bytes'])!=int(staged['parquet_bytes'])):
        raise RuntimeError('Q06 HTTPS transfer SHA/bytes identity mismatch')
    return {'source_cloud':source,'destination_cloud':destination,'part':'selected',
        'sha256':staged['sha256'],'rows':int(staged['rows']),
        'parquet_bytes':int(staged['parquet_bytes']),
        'curl_size_upload':int(out['payload_bytes']),
        'curl_http_total_seconds':float(out['http_seconds']),
        'stage_seconds':float(staged['stage_seconds']),
        'receiver_file_seconds':float(ack['receiver_file_seconds'])}


def q06_finish(cloud,plan,trial,token,key):
    work=f'/opt/egressopt/runtime/q06_{key}_{cloud}'
    reply=work+'/finish_response.json';py=Q06_CLOUD_VENV[cloud]
    payload=json.dumps({'plan_id':plan,'trial_id':trial},separators=(',',':'))
    script=('curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 300 '+
        '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
        '-H '+shlex.quote('Content-Type: application/json')+' '+
        '--data-binary '+shlex.quote(payload)+' '+
        'https://127.0.0.1:8443/q06/finish > '+shlex.quote(reply)+'\n'+
        py+' -c '+shlex.quote('import json; x=json.load(open('+repr(reply)+')); '+
        'print("Q06_RPC="+json.dumps(x,separators=(",",":")))'))
    out=q06_rpc(cloud,script,'finish-'+plan)
    if (out.get('cloud'),out.get('plan_id'),out.get('trial_id'))!=(cloud,plan,trial):
        raise RuntimeError('Q06 finish identity mismatch')
    if not re.fullmatch('[a-f0-9]{64}',out.get('physical_plan_sha256','')):
        raise RuntimeError('Q06 physical plan fingerprint missing')
    return out


def q06_cloud(args):
    # Offline fail-fast checks before contacting cloud APIs or starting VMs.
    selfcheck()
    if args.sf!=1:raise RuntimeError('Q06 cloud mode currently supports SF1 only')
    q06_verify_aws_clock(force=True)
    try:import duckdb
    except ImportError as e:raise RuntimeError('Install duckdb into the active Python environment') from e
    source=args.data_dir/'lineitem.parquet'
    for path in (source,args.query_file,args.reference_file):
        if not path.is_file():raise FileNotFoundError(path)
    source_sql=args.query_file.read_text(encoding='utf-8')
    params=parse_q06(source_sql)
    independent=load_independent_reference(args.reference_file)
    with duckdb.connect(':memory:') as con:
        con.execute('SET threads=4')
        con.execute(f'CREATE VIEW lineitem AS SELECT * FROM {scan(source)}')
        reference=normalize_result(con.execute(source_sql).fetchall())
    if reference!=independent:
        raise RuntimeError('Q06 independent SF1 SQL reference mismatch')
    remote_path=Path('/opt/egressopt/data/tpch_sf1/lineitem.parquet')
    stage_sql=qualifying_sql(remote_path,params)
    runkey=uuid.uuid4().hex[:8]
    runid=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+runkey
    directory=runner_dir(args)/runid;directory.mkdir(parents=True,exist_ok=False)
    (directory/'source_q06.sql').write_text(source_sql,encoding='utf-8')
    save_json(directory/'independent_reference.json',independent)
    manifest={'schema_version':2,'runner_build':VERSION,'query_id':QUERY,'run_id':runid,
      'status':'running','scale_factor':'SF1','placement_id':PLACEMENT,
      'execution_mode':'real_multicloud','created_at_utc':utcnow(),
      'trial_count_per_plan':args.trials,'warmup_trials_per_plan':args.warmup_trials,
      'source_sql_sha256':hash_file(args.query_file),'source_sql_copy_sha256':hash_file(directory/'source_q06.sql'),
      'source_sql_parameters':params,'independent_reference_sha256':result_hash(independent),
      'independent_reference_file_sha256':hash_file(args.reference_file),
      'independent_reference_copy_sha256':hash_file(directory/'independent_reference.json'),
      'source_dataset':{'table':'lineitem','cloud':'azure','sha256':hash_file(source),
            'bytes':source.stat().st_size,'path':str(source.resolve())},
      'runner_sha256':hash_file(Path(__file__)),'worker_sha256':digest(Q06_WORKER_SOURCE.encode()),
      'environment':{'python':platform.python_version(),'duckdb':duckdb.__version__},
      'timing_definition':'Azure source Parquet stage + source curl TLS POST (including receiver file write/response) + destination DuckDB SUM; Azure-local uses DuckDB SUM only. Excludes VM startup and controller CLI time.',
      'network_bytes_definition':'HTTP request-body bytes, not cloud-billed bytes. Azure-local has zero inter-cloud intermediate payload bytes; final scalar retrieval and control-plane traffic excluded.',
      'network_seconds_measured':'NOT_ISOLATED','egress_cost_status':'NOT_PRICED',
      'cleanup':{},'cleanup_incomplete':None,'round_execution_order':[],
      'firewall_rules':{},'assigned_public_ips':{},'verified_outbound_public_ips':{},
      'remote_dataset_sha256':{},'worker_releases':{},'cloud_hardware':{},
      'limitations':['No automatic EgressOpt optimizer has been evaluated',
        'HTTP body bytes are not cloud-billed egress bytes',
        'Clock/network variability and VM start costs excluded from workflow duration',
        'Bearer tokens may be visible to authorized cloud control-plane command logs']}
    save_json(directory/'run.json',manifest)
    state={'started':{'aws':False,'azure':False,'gcp':False},
           'worker':{'aws':False,'azure':False,'gcp':False},
           'rules':{'aws':[],'gcp':[]},'aws_sg':None,
           'gcp_external_added':False,'gcp_tag_added':False,'gcp_tag':None}
    measured=[];warmups=[]
    try:
        for cloud in ('aws','azure','gcp'):
            print('Checking '+cloud.upper()+' VM...',flush=True)
            state['started'][cloud]=q06_start_vm(cloud)
            print('  '+cloud.upper()+' running',flush=True)
        aws_vm=q06_aws_instance();gcp_vm=q06_gcp_instance()
        if aws_vm['State']['Name']!='running' or q06_azure_state()!='PowerState/running' or gcp_vm['status']!='RUNNING':
            raise RuntimeError('Q06 VMs did not all reach RUNNING state')
        if not any(config.get('natIP') for nic in gcp_vm['networkInterfaces'] for config in nic.get('accessConfigs',[])):
            state['gcp_external_added']=True
            manifest['gcp_external_ip_added_by_this_run']=True
            save_json(directory/'run.json',manifest)
            q06_gcloud('compute','instances','add-access-config',GCP_VM,'--zone='+GCP_ZONE,
                '--network-interface=nic0','--access-config-name=external-nat','--quiet','--format=json',timeout=280)
            gcp_vm=q06_gcp_instance()
        ips={'aws':q06_ip(aws_vm['PublicIpAddress']),
             'gcp':q06_gcp_public(gcp_vm)}
        manifest['assigned_public_ips']=ips
        outbound={'azure':q06_outbound('azure')}
        manifest['verified_outbound_public_ips']=outbound
        groups=[x['GroupId'] for x in aws_vm['SecurityGroups']]
        if len(groups)!=1:raise RuntimeError('Q06 expected one EC2 security group')
        state['aws_sg']=groups[0]
        manifest['cloud_hardware']={'aws':{'region':AWS_REGION,'instance_type':aws_vm['InstanceType'], 'instance_id':AWS_INSTANCE},
          'azure':{'resource_group':AZ_RG,'vm_name':AZ_VM},
          'gcp':{'zone':GCP_ZONE,'machine_type':gcp_vm['machineType'].rsplit('/',1)[-1]}}
        out=q06_remote('azure','sha256sum /opt/egressopt/data/tpch_sf1/lineitem.parquet','verify-source-dataset')
        if not any(line.split()[:1]==[manifest['source_dataset']['sha256']] and line.strip().endswith('/lineitem.parquet') for line in out.splitlines()):
            raise RuntimeError('Q06 Azure lineitem hash differs from local SF1 data')
        manifest['remote_dataset_sha256']={'azure':manifest['source_dataset']['sha256']}
        for cloud in ('aws','azure','gcp'):
            release=q06_install_worker(cloud,runkey)
            manifest['worker_releases'][cloud]=release
            save_json(directory/'run.json',manifest)
        token=secrets.token_urlsafe(48);cas={}
        for cloud in ('aws','azure','gcp'):
            state['worker'][cloud]=True
            # Azure binds loopback only, so no public Azure ingress rule needed.
            ip=ips.get(cloud,'127.0.0.1')
            cas[cloud]=q06_start_service(cloud,runkey,manifest['worker_releases'][cloud],ip,token,source_sql,stage_sql)
            print('  '+cloud.upper()+' authenticated Q06 TLS worker ready',flush=True)
        rule=q06_aws_rule(outbound['azure'],'azure',runkey,state['aws_sg'])
        state['rules']['aws'].append(rule)
        manifest['firewall_rules']['aws_from_azure']=rule
        save_json(directory/'run.json',manifest)
        network=gcp_vm['networkInterfaces'][0]['network'].rsplit('/',1)[-1]
        state['gcp_tag']='egressopt-q06-'+runkey
        state['gcp_tag_added']=True
        grule,tag=q06_gcp_rule(outbound['azure'],runkey,network,'azure')
        state['rules']['gcp'].append(grule)
        state['gcp_tag']=tag
        manifest['firewall_rules']['gcp_from_azure']=grule
        save_json(directory/'run.json',manifest)
        print('  Q06 source-restricted Azure→AWS/GCP TLS ingress ready',flush=True)
        for index in range(args.warmup_trials+args.trials):
            warm=index<args.warmup_trials
            count=index+1 if warm else index-args.warmup_trials+1
            tid=('w' if warm else 'm')+f'{count:02d}'
            order=PLANS[index%3:]+PLANS[:index%3]
            manifest['round_execution_order'].append({'round':index+1,'trial':count,'warmup':warm,'plans':list(order)})
            save_json(directory/'run.json',manifest)
            for plan in order:
                dest=DESTINATIONS[plan]
                if plan=='azure_local':
                    edge=[]
                    stage_s=0.0;http_s=0.0;payload=0;staged_rows=0
                else:
                    staged=q06_stage(plan,tid,token,runkey)
                    edge=[q06_upload(dest,staged,plan,tid,ips,cas,token,runkey)]
                    stage_s=float(edge[0]['stage_seconds'])
                    http_s=float(edge[0]['curl_http_total_seconds'])
                    payload=int(edge[0]['curl_size_upload'])
                    staged_rows=int(edge[0]['rows'])
                finished=q06_finish(dest,plan,tid,token,runkey)
                if finished['result']!=independent or finished['result_sha256']!=result_hash(independent):
                    raise RuntimeError('Q06 real cloud result mismatch: '+plan+'/'+tid)
                aggregate_s=float(finished['aggregate_seconds'])
                record={'run_id':runid,'query_id':QUERY,'scale_factor':'SF1',
                    'placement_id':PLACEMENT,'execution_mode':'real_multicloud',
                    'plan_id':plan,'trial':count,'execution_cloud':dest,'transfer_edges':len(edge),
                    'intermediate_rows':staged_rows,'intermediate_parquet_bytes':payload,
                    'stage_seconds':stage_s,'aggregate_seconds':aggregate_s,
                    'total_seconds':stage_s+http_s+aggregate_s,'result_row_count':len(independent),
                    'result_sha256':finished['result_sha256'],'correct_vs_reference':True,
                    'network_bytes_measured':payload,'network_seconds_measured':'',
                    'egress_usd_estimated':'','timestamp_utc':utcnow()}
                item={'schema_version':2,'plan_id':plan,'trial':count,'metrics':record,
                    'result':finished['result'],'raw_result_strings':finished.get('raw_result_strings'),
                    'transfers':edge,'curl_http_total_seconds':http_s,
                    'physical_plan_sha256':finished['physical_plan_sha256'],
                    'timing_definition':manifest['timing_definition']}
                if warm:
                    save_json(directory/f'warmup_{count:02d}_{plan}.json',item)
                    warmups.append({'plan_id':plan,'warmup':count,'total_seconds':record['total_seconds'],
                        'intermediate_parquet_bytes':payload,'correct_vs_reference':True})
                    write_csv(directory/'warmup_metrics.csv',warmups,fields=list(warmups[0]))
                else:
                    save_json(directory/f'trial_{count:02d}_{plan}.json',item)
                    measured.append(record)
                    write_csv(directory/'trial_metrics.csv',measured)
                label=f'WARMUP={count}' if warm else f'trial={count}'
                print(f'{plan:17s} {label} correctness=PASS '
                      f'real_HTTP_bytes={payload:,} stage+HTTPS+aggregate={record["total_seconds"]:.3f}s',flush=True)
        manifest.update(status='success',completed_at_utc=utcnow(),
            trial_metrics_sha256=hash_file(directory/'trial_metrics.csv'),
            measured_trial_count=len(measured),warmup_count=len(warmups))
        print('Q06_REAL_MULTICLOUD_PASS')
        print('Q06 cloud run: '+str(directory))
    except Exception as exc:
        manifest.update(status='failed',completed_at_utc=utcnow(),
            failure=type(exc).__name__+': '+str(exc))
        raise
    finally:
        for cloud in ('aws','azure','gcp'):
            if not state['worker'][cloud]:continue
            try:
                q06_remote(cloud,'systemctl stop egressopt-q06-'+runkey+'-'+cloud+' >/dev/null 2>&1 || true; '+
                    'rm -rf /opt/egressopt/runtime/q06_'+runkey+'_'+cloud+'; '+
                    'test ! -e /opt/egressopt/runtime/q06_'+runkey+'_'+cloud+'; echo Q06_RUNTIME_REMOVED',
                    'remove-Q06-runtime')
                manifest['cleanup'][cloud+'_runtime']='removed'
            except Exception as exc:
                manifest['cleanup'][cloud+'_runtime']='FAILED: '+str(exc)
        for rule in state['rules']['aws']:
            try:
                q06_aws('ec2','revoke-security-group-ingress','--group-id',state['aws_sg'],
                    '--security-group-rule-ids',rule)
                manifest['cleanup']['aws_rule_'+rule]='removed'
            except Exception as exc:manifest['cleanup']['aws_rule_'+rule]='FAILED: '+str(exc)
        for rule in state['rules']['gcp']:
            try:
                q06_gcloud('compute','firewall-rules','delete',rule,'--quiet')
                manifest['cleanup']['gcp_rule_'+rule]='removed'
            except Exception as exc:manifest['cleanup']['gcp_rule_'+rule]='FAILED: '+str(exc)
        if state['gcp_tag_added']:
            try:
                q06_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
                    '--tags='+state['gcp_tag'],'--quiet','--format=json')
                manifest['cleanup']['gcp_tag']='removed'
            except Exception as exc:manifest['cleanup']['gcp_tag']='FAILED: '+str(exc)
        if state['gcp_external_added']:
            try:
                q06_gcloud('compute','instances','delete-access-config',GCP_VM,'--zone='+GCP_ZONE,
                    '--network-interface=nic0','--access-config-name=external-nat','--quiet','--format=json')
                manifest['cleanup']['gcp_external_ip']='removed'
            except Exception as exc:manifest['cleanup']['gcp_external_ip']='FAILED: '+str(exc)
        for cloud in ('aws','azure','gcp'):
            if not (args.stop_all_after_run or state['started'][cloud]):continue
            try:
                if cloud=='aws':
                    if q06_aws_instance()['State']['Name'] not in ('stopped','stopping'):
                        q06_aws('ec2','stop-instances','--instance-ids',AWS_INSTANCE)
                    q06_cli(['aws','ec2','wait','instance-stopped','--instance-ids',AWS_INSTANCE,
                        '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=670)
                    if q06_aws_instance()['State']['Name']!='stopped':raise RuntimeError('AWS stop not verified')
                    result='stopped_verified'
                elif cloud=='azure':
                    if q06_azure_state()!='PowerState/deallocated':
                        q06_cli(['az','vm','deallocate','--resource-group',AZ_RG,'--name',AZ_VM,
                            '--output','none'],timeout=690)
                    if q06_azure_state()!='PowerState/deallocated':raise RuntimeError('Azure deallocation not verified')
                    result='deallocated_verified'
                else:
                    if q06_gcp_instance()['status']!='TERMINATED':
                        q06_gcloud('compute','instances','stop',GCP_VM,'--zone='+GCP_ZONE,
                            '--quiet','--format=json',timeout=690)
                    if q06_gcp_instance()['status']!='TERMINATED':raise RuntimeError('GCP stop not verified')
                    result='stopped_verified'
                manifest['cleanup'][cloud+'_vm']=result
            except Exception as exc:
                manifest['cleanup'][cloud+'_vm']='FAILED: '+str(exc)
        manifest['cleanup_incomplete']=any(str(v).startswith('FAILED') for v in manifest['cleanup'].values())
        save_json(directory/'run.json',manifest)
        if manifest['cleanup_incomplete']:
            print('WARNING: Q06 cloud cleanup incomplete; inspect run.json',file=sys.stderr)
        print('Q06 experiment report: '+str(directory),flush=True)


def audit_cloud(folder,quiet=False):
    info=read_json(folder/'run.json')
    if info.get('execution_mode')!='real_multicloud' or info.get('status')!='success':
        raise RuntimeError('Q06_CLOUD_AUDIT_FAIL: cloud run incomplete')
    csvfile=folder/'trial_metrics.csv'
    if hash_file(csvfile)!=info.get('trial_metrics_sha256'):
        raise RuntimeError('Q06_CLOUD_AUDIT_FAIL: measured CSV hash mismatch')
    if hash_file(folder/'source_q06.sql')!=info.get('source_sql_copy_sha256'):
        raise RuntimeError('Q06_CLOUD_AUDIT_FAIL: archived SQL modified')
    if hash_file(folder/'independent_reference.json')!=info.get('independent_reference_copy_sha256'):
        raise RuntimeError('Q06_CLOUD_AUDIT_FAIL: reference snapshot modified')
    if info.get('cleanup_incomplete') is not False:
        raise RuntimeError('Q06_CLOUD_AUDIT_FAIL: cleanup incomplete/unverified')
    if any(not str(v).endswith('verified') for k,v in info.get('cleanup',{}).items() if k.endswith('_vm')):
        raise RuntimeError('Q06_CLOUD_AUDIT_FAIL: VM power-off verification failed')
    rows=read_csv(csvfile); seen=set();reference=read_json(folder/'independent_reference.json')
    for row in rows:
        plan=row['plan_id'];index=int(row['trial']); key=(plan,index)
        if plan not in PLANS or key in seen or index<1:raise RuntimeError('Q06 trial identity invalid')
        seen.add(key)
        snap=read_json(folder/f'trial_{index:02d}_{plan}.json')
        if any(str(snap['metrics'][k])!=row[k] for k in FIELDS):
            raise RuntimeError('Q06 saved cloud CSV differs from trial JSON')
        if snap['result']!=reference or row['result_sha256']!=result_hash(reference) or row['correct_vs_reference']!='True':
            raise RuntimeError('Q06 result hash/SQL correctness invalid')
        edges=snap['transfers']
        if len(edges)!=len(EDGES[plan]) or int(row['transfer_edges'])!=len(edges):
            raise RuntimeError('Q06 cloud transfer edge count invalid')
        for edge,(src,dst) in zip(edges,EDGES[plan]):
            if (edge['source_cloud'],edge['destination_cloud'])!=(src,dst):
                raise RuntimeError('Q06 directed cloud edge invalid')
            if not re.fullmatch('[a-f0-9]{64}',edge['sha256']):
                raise RuntimeError('Q06 SHA mismatch')
            if int(edge['curl_size_upload'])!=int(edge['parquet_bytes']) or int(edge['parquet_bytes'])<=0:
                raise RuntimeError('Q06 transfer byte count mismatch')
        n=sum(int(x['curl_size_upload']) for x in edges)
        if n!=int(row['intermediate_parquet_bytes']) or n!=int(row['network_bytes_measured']):
            raise RuntimeError('Q06 HTTP payload count mismatch')
        if plan=='azure_local' and (n!=0 or float(row['stage_seconds'])!=0):
            raise RuntimeError('Q06 zero-intercloud negative control invalid')
        if abs(float(row['total_seconds'])-float(row['stage_seconds'])-
               float(snap['curl_http_total_seconds'])-float(row['aggregate_seconds']))>1e-6:
            raise RuntimeError('Q06 workflow time arithmetic invalid')
        if row['execution_mode']!='real_multicloud' or row['scale_factor']!='SF1' or row['placement_id']!='P1':
            raise RuntimeError('Q06 cloud record metadata mismatch')
    expected_trials={(p,t) for p in PLANS for t in range(1,int(info['trial_count_per_plan'])+1)}
    if seen!=expected_trials or len(rows)!=len(expected_trials):
        raise RuntimeError('Q06 cloud evidence missing expected trials')
    if info.get('measured_trial_count')!=len(rows) or info.get('warmup_count')!=3*int(info.get('warmup_trials_per_plan',0)):
        raise RuntimeError('Q06 cloud expected trial counts inconsistent')
    report={'status':'PASS','audited_at_utc':utcnow(),'run_id':info['run_id'],
        'mode':'REAL_MULTICLOUD','checked_trials':len(rows),
        'limitations':['No independent cloud replay; checks saved evidence only',
            'Cloud transfer hashes historically verified at receipt; temporary payloads not archived',
            'HTTP body bytes are not billed provider egress bytes']}
    save_json(folder/'q06_analysis'/'audit.json',report)
    if not quiet:
        print('Q06_EVIDENCE_AUDIT_PASS')
        print('Evidence: '+str(folder.resolve()))
    return folder,rows,info


def reproduce_cloud(folder,args):
    folder,rows,info=audit_cloud(folder,quiet=True)
    out=folder/'q06_analysis';out.mkdir(exist_ok=True)
    summary=[]
    for plan in PLANS:
        group=[row for row in rows if row['plan_id']==plan]
        durations=[float(row['total_seconds'])*1000 for row in group]
        values=[int(row['intermediate_parquet_bytes']) for row in group]
        summary.append({'plan_id':plan,'execution_cloud':DESTINATIONS[plan],
            'n':len(group),'mean_duration_ms':statistics.mean(durations),
            'median_duration_ms':statistics.median(durations),
            'sample_sd_duration_ms':statistics.stdev(durations) if len(durations)>1 else None,
            'mean_http_payload_bytes':statistics.mean(values),
            'transfer_edges':len(EDGES[plan]),'correct_trials':len(group),
            'measurement_type':'REAL_MULTICLOUD_APPLICATION_HTTP_PAYLOAD'})
    save_json(out/'paper_summary.json',{'source_run_id':info['run_id'],
        'generated_at_utc':utcnow(),'query_id':QUERY,'statistics':summary,
        'scope':'Q06 controlled cloud benchmark, app-level bytes and stage+HTTPS+aggregate duration'})
    write_csv(out/'paper_summary.csv',summary,fields=tuple(summary[0]))
    lines=['% Q06 SF1/P1 — REAL MULTICLOUD: HTTP payload bytes, not billed cost',
        r'\begin{tabular}{lrrr}',r'\hline',
        r'Plan & Mean duration (ms) & HTTP payload bytes & Trials \\',r'\hline']
    for row in summary:
        label=row['plan_id'].replace('_',r'\_')
        lines.append(f'{label} & {row["mean_duration_ms"]:.2f} & {row["mean_http_payload_bytes"]:.0f} & {row["n"]} '+r'\\')
    lines.extend([r'\hline',r'\end{tabular}'])
    (out/'q06_cloud_table.tex').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    if args.plots:
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
        except ImportError as e:raise RuntimeError('Install matplotlib or specify --no-plots') from e
        labels=[x['plan_id'].replace('_','\n') for x in summary]
        for metric,ylabel,stem in (
            ('mean_duration_ms','Mean cloud workflow duration (ms)','q06_cloud_duration'),
            ('mean_http_payload_bytes','Application HTTP payload bytes','q06_cloud_payload')):
            fig,ax=plt.subplots(figsize=(7.4,4.2))
            ax.bar(labels,[x[metric] for x in summary])
            ax.set_ylabel(ylabel)
            ax.set_title('Q06 / SF1 / P1 — real AWS, Azure, GCP comparison')
            ax.grid(axis='y',alpha=.2)
            fig.tight_layout()
            fig.savefig(out/(stem+'.png'),dpi=220)
            fig.savefig(out/(stem+'.pdf'))
            plt.close(fig)
    print('Q06_REPRODUCE_PASS')
    print('Reproduced real-cloud Q06 tables/figures: '+str(out.resolve()))



def selfcheck():
    print('Q06_RUNNER_BUILD: '+VERSION)
    compile(Q06_WORKER_SOURCE, 'Q06 embedded HTTPS worker', 'exec')
    sample = """SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATE '1995-01-01'
          AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
          AND l_quantity < 24;"""
    parsed = parse_q06(sample)
    assert 'l_quantity < 24' in parsed['predicate_sql']
    assert 'l_shipdate' in qualifying_sql(Path('lineitem.parquet'), parsed)
    assert normalize_result([(Decimal('123.456'),)]) == [{'revenue': '123.46'}]
    try:
        parse_q06(sample.replace('SUM(l_extendedprice * l_discount)', 'AVG(l_extendedprice * l_discount)'))
    except ValueError:
        pass
    else:
        raise AssertionError('Q06 rejected-alternative-aggregate test failed')
    assert EDGES['azure_local'] == ()
    assert Q06_PORT == 8443
    # Exercise the actual AWS rule builder to catch undefined constants before cloud setup.
    from unittest.mock import patch
    calls = []
    def fake_aws(*command):
        calls.append(command)
        return {'SecurityGroupRules': [{'SecurityGroupRuleId': 'sgr-selfcheck'}]}
    with patch.dict(globals(), {'q06_aws': fake_aws}):
        assert q06_aws_rule('198.51.100.10', 'azure', 'selfcheck', 'sg-selfcheck') == 'sgr-selfcheck'
    assert len(calls) == 1
    permission = json.loads(calls[0][-1])
    assert permission[0]['FromPort'] == permission[0]['ToPort'] == Q06_PORT
    assert permission[0]['IpRanges'][0]['CidrIp'] == '198.51.100.10/32'
    print('Q06_SELFCHECK_PASS (SQL, zero-egress control, AWS firewall builder)')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', required=True, choices=('prepare','cloud','reproduce','audit','selfcheck'))
    p.add_argument('--sf', type=int, default=DEFAULT_SF)
    p.add_argument('--trials', type=int, default=10)
    p.add_argument('--warmup-trials', type=int, default=2)
    p.add_argument('--threads', type=int, default=4)
    p.add_argument('--run-id')
    p.add_argument('--data-dir', type=Path)
    p.add_argument('--query-file', type=Path, default=Path('queries/tpch/q06.sql'))
    p.add_argument('--reference-file', type=Path)
    p.add_argument('--output-dir', type=Path, default=Path('results/experiments'))
    p.add_argument('--plots', action=argparse.BooleanOptionalAction, default=True)
    p.add_argument('--stop-all-after-run', action=argparse.BooleanOptionalAction, default=True)
    args = p.parse_args()
    if args.sf not in VALIDATED_SF:
        p.error('Only validated SF1 is currently enabled; SF10 and SF100 require verified data')
    if not (1 <= args.trials <= 100) or not (1 <= args.threads <= 64) or not (0 <= args.warmup_trials <= 20):
        p.error('Trial, thread or warm-up settings out of range')
    if args.data_dir is None:
        args.data_dir = Path(f'data/tpch/parquet_sf{args.sf}')
    if args.reference_file is None:
        args.reference_file = Path(f'results/reference/sf{args.sf}/q06_results.json')
    if args.mode == 'selfcheck':
        return selfcheck()
    if args.mode == 'cloud':
        return q06_cloud(args)
    if args.mode == 'prepare':
        return prepare(args)
    if args.mode == 'audit':
        audit(args)
        return 0
    return reproduce(args)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, InvalidOperation, NotImplementedError) as err:
        print(f'Q06 ERROR: {type(err).__name__}: {err}', file=sys.stderr)
        sys.exit(2)
