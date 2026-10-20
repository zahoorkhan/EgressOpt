#!/usr/bin/env python3
"""EgressOpt Q09 — standalone Product Type Profit Measure query runner.

Locked SF1 / P1 workload. No imports from other EgressOpt query scripts.

    python experiments/q09.py --mode prepare --trials 1
    python experiments/q09.py --mode cloud --trials 10
    python experiments/q09.py --mode reproduce
    python experiments/q09.py --mode audit

IMPLEMENTATION:
- prepare: four correct LOCAL Parquet-shipping emulations and source-SQL reference.
- cloud: authenticated, TLS-protected directed transfers across AWS/Azure/GCP.
- audit/reproduce: offline validation and figures for completed local/cloud runs.
The r2 cloud version passed a live four-plan smoke test; r3 adds controller
clock verification and has not itself undergone a live cloud run. Cloud runs incur charges and modify temporary rules.

P1: orders=AWS; lineitem,nation=Azure; part,partsupp,supplier=GCP.

Default SF=1. A scale setting alone does not validate larger-scale data.
"""
from __future__ import annotations

import argparse
import io
import math
import zipfile
import csv
from datetime import datetime, timezone
import base64
import gzip
import ipaddress
import secrets
import shlex
import subprocess
from decimal import Decimal, ROUND_HALF_EVEN
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

QUERY_ID = 'Q09'
Q09_RUNNER_BUILD = 'q09-chunked-clockguard-autopurge-prospective-20261016-r6'
PLACEMENT_ID = 'P1'
DEFAULT_SF = int(os.environ.get('EGRESSOPT_SF', '1'))
VALIDATED_SF = (1,)
PLANS = ('azure_join', 'gcp_join', 'aws_join', 'partial_aggregate_aws')
TABLE_LOCATION = {
    'orders': 'aws', 'lineitem': 'azure', 'nation': 'azure',
    'part': 'gcp', 'partsupp': 'gcp', 'supplier': 'gcp',
}
PLAN_EDGES = {
    'azure_join': [('gcp', 'azure', 'green_supply'), ('aws', 'azure', 'orders_projection')],
    'gcp_join': [('aws', 'gcp', 'orders_projection'), ('azure', 'gcp', 'line_projection'),
                 ('azure', 'gcp', 'nation_projection')],
    'aws_join': [('gcp', 'azure', 'green_supply'), ('azure', 'aws', 'joined_line_profit')],
    'partial_aggregate_aws': [('gcp', 'azure', 'green_supply'),
                              ('azure', 'aws', 'order_nation_profit')],
}
METRIC_FIELDS = (
    'run_id', 'query_id', 'scale_factor', 'placement_id', 'execution_mode',
    'plan_id', 'trial', 'intermediate_parquet_bytes', 'intermediate_rows',
    'edge_count', 'stage_seconds', 'join_aggregate_seconds', 'total_seconds',
    'result_sha256', 'correct_vs_reference', 'network_bytes_measured',
    'network_seconds_measured', 'egress_usd_estimated', 'timestamp_utc',
)
CENT = Decimal('0.01')


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def dump_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def load_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def quoted(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def scan(path: Path) -> str:
    return 'read_parquet(' + quoted(str(path.resolve())) + ')'


def relation(query: str, alias: str) -> str:
    """DuckDB table function in FROM needs no surrounding parentheses."""
    if not re.fullmatch(r'[a-z][a-z0-9_]*', alias):
        raise ValueError('Invalid relation alias')
    q = query.strip()
    if re.match(r'^(?:SELECT|WITH)\b', q, flags=re.I):
        return f'({q}) {alias}'
    if re.match(r'^read_parquet\s*\(', q, flags=re.I):
        return f'{q} {alias}'
    raise ValueError('Q09 unexpected staged relation type')


def canonical(raw_rows) -> list[dict]:
    """One row per (nation, order year), currency rounded to cents.

    This is a SQL-result comparator, NOT proof of decimal exactness or network
    reproducibility. Raw unrounded query values are saved in trial JSON.
    """
    out, seen = [], set()
    for row in raw_rows:
        if len(row) != 3 or any(v is None for v in row):
            raise ValueError('Q09 expected non-null (nation, year, profit)')
        name, year, amount = str(row[0]), int(row[1]), Decimal(str(row[2]))
        if not amount.is_finite():
            raise ValueError('Non-finite Q09 profit')
        key = (name, year)
        if key in seen:
            raise ValueError(f'Duplicate Q09 result group {key}')
        seen.add(key)
        out.append({'nation': name, 'o_year': year,
                    'sum_profit': format(amount.quantize(CENT, rounding=ROUND_HALF_EVEN), '.2f')})
    return sorted(out, key=lambda x: (x['nation'], -x['o_year']))


def result_hash(canon: list[dict]) -> str:
    return sha_bytes(json.dumps(canon, sort_keys=True, separators=(',', ':')).encode())


def source_parameters(sql: str) -> dict:
    """Fail closed unless source resembles the standard six-table Q09 template.

    Detects the archived color LIKE literal; never replaces the user's SQL as
    the reference. Rewrites of Q09 should be reviewed, not silently assumed
    semantically equivalent to these four physical plans.
    """
    stripped = re.sub(r'/\*.*?\*/', ' ', re.sub(r'--[^\n]*', ' ', sql), flags=re.S)
    # Accept qualifiers in SELECT / WHERE of the original six-table template.
    norm = re.sub(r'\b[a-z_][a-z0-9_]*\s*\.\s*(?=[a-z_])', '', stripped.lower())
    norm = re.sub(r'\s+', ' ', norm)
    for token in ('part', 'partsupp', 'supplier', 'lineitem', 'orders', 'nation',
                  'l_extendedprice', 'l_discount', 'ps_supplycost', 'l_quantity',
                  'group by', 'order by', 'sum_profit', 'o_year'):
        if not re.search(r'\b' + re.escape(token) + r'\b', norm):
            raise ValueError(f'Q09 source SQL missing expected Q09 token: {token}')
    expected_joins = (
        ('s_suppkey', 'l_suppkey'), ('ps_suppkey', 'l_suppkey'),
        ('ps_partkey', 'l_partkey'), ('p_partkey', 'l_partkey'),
        ('o_orderkey', 'l_orderkey'), ('s_nationkey', 'n_nationkey'),
    )
    for left, right in expected_joins:
        if not re.search(r'\b(?:' + left + r'\s*=\s*' + right + r'|' +
                         right + r'\s*=\s*' + left + r')\b', norm):
            raise ValueError(f'Q09 source SQL lacks join equality {left}={right}')
    # Match the *entire* source amount expression. Substring checking would
    # wrongly accept modified formulas such as "... * l_quantity * 0.5".
    amount_match = re.search(r'\bas\s+o_year\s*,\s*(.*?)\s+as\s+amount\b', norm, flags=re.S)
    expected_profit = 'l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity'
    if (not amount_match or
            re.sub(r'\s+', '', amount_match.group(1)) != expected_profit):
        raise ValueError('Q09 source SQL uses an unsupported profit expression; inspect before running')
    if not (re.search(r'\bextract\s*\(\s*year\s+from\s+o_orderdate\s*\)', norm) or
            re.search(r'\byear\s*\(\s*o_orderdate\s*\)', norm)):
        raise ValueError('Q09 source SQL requires year derived from o_orderdate')
    pattern = re.findall(r"\bp_name\s+like\s+'((?:''|[^'])*)'", norm, flags=re.I)
    if len(pattern) != 1:
        raise ValueError('Q09 source SQL requires exactly one literal p_name LIKE predicate')
    literal = pattern[0].replace("''", "'")
    if not (literal.startswith('%') and literal.endswith('%') and
            re.fullmatch(r'%[a-z][a-z -]*%', literal)):
        raise ValueError('Q09 currently supports a standard substring color LIKE pattern')
    if re.search(r'\b(or|union|having|limit)\b', norm):
        raise ValueError('Q09 source SQL has additional unsupported logical operators')
    if len(re.findall(r'\blike\b', norm)) != 1:
        raise ValueError('Q09 source SQL has extra LIKE predicates')
    return {'color_substring': literal[1:-1], 'part_name_pattern': literal,
            'query_form': 'TPC-H-derived Q09 standard six-relation profit'}


def project_root():
    return Path(__file__).resolve().parent.parent


def inputs(args):
    root = project_root()
    path = Path(args.data_dir).expanduser() if args.data_dir else root / 'data' / 'tpch' / f'parquet_sf{args.sf}'
    sql_path = Path(args.sql).expanduser() if args.sql else root / 'queries' / 'tpch' / 'q09.sql'
    if not sql_path.is_file():
        raise FileNotFoundError(f'Missing archived Q09 SQL: {sql_path}')
    files = {t: path / f'{t}.parquet' for t in TABLE_LOCATION}
    missing = [str(p) for p in files.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError('Missing SF1 Parquet data: ' + ', '.join(missing))
    return sql_path, files


def output_root(args):
    root = (Path(args.results_root).expanduser() if args.results_root else
            project_root() / 'results' / 'experiments' / 'q09')
    dest=root / f'sf{args.sf}' / PLACEMENT_ID.lower()
    return dest / 'optimizer_prospective' if getattr(args,'prospective_run',False) else dest


def connection(duckdb, threads):
    c = duckdb.connect(':memory:')
    c.execute(f'SET threads={int(threads)}')
    return c


def authoritative_answer(duckdb, files, source_sql: str, threads):
    with connection(duckdb, threads) as con:
        for name, path in files.items():
            con.execute(f'CREATE VIEW {name} AS SELECT * FROM {scan(path)}')
        raw = con.execute(source_sql).fetchall()
    answer = canonical(raw)
    if not answer:
        raise ValueError('Q09 archived source SQL returned no nation/year rows')
    return answer, [[str(v) for v in row] for row in raw]


def base_parts(files, params):
    src = {t: scan(path) for t, path in files.items()}
    return {
        **src,
        'green_supply': (
            'SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_supplycost, s.s_nationkey '
            f'FROM {src["part"]} p '
            f'JOIN {src["partsupp"]} ps ON p.p_partkey=ps.ps_partkey '
            f'JOIN {src["supplier"]} s ON ps.ps_suppkey=s.s_suppkey '
            f'WHERE p.p_name LIKE {quoted(params["part_name_pattern"])}'
        ),
        'orders_projection': f'SELECT o_orderkey,o_orderdate FROM {src["orders"]}',
        'line_projection': (
            'SELECT l_orderkey,l_partkey,l_suppkey,l_extendedprice,l_discount,l_quantity '
            f'FROM {src["lineitem"]}'
        ),
        'nation_projection': f'SELECT n_nationkey,n_name FROM {src["nation"]}',
    }


def full_profit_query(orders, line, supply, nation):
    return (
        'SELECT n.n_name AS nation, EXTRACT(YEAR FROM o.o_orderdate) AS o_year, '
        'SUM(l.l_extendedprice*(1-l.l_discount)-g.ps_supplycost*l.l_quantity) AS sum_profit '
        f'FROM {relation(line,"l")} '
        f'JOIN {relation(supply,"g")} ON l.l_partkey=g.ps_partkey AND l.l_suppkey=g.ps_suppkey '
        f'JOIN {relation(orders,"o")} ON o.o_orderkey=l.l_orderkey '
        f'JOIN {relation(nation,"n")} ON n.n_nationkey=g.s_nationkey '
        'GROUP BY n.n_name,EXTRACT(YEAR FROM o.o_orderdate) '
        'ORDER BY nation,o_year DESC'
    )


def joined_line_profit_query(line, supply, nation, aggregate=False):
    """Stage intermediate after GCP selective join has reached Azure."""
    amount = 'l.l_extendedprice*(1-l.l_discount)-g.ps_supplycost*l.l_quantity'
    if aggregate:
        projection = f'l.l_orderkey, n.n_name AS nation, SUM({amount}) AS amount'
        group = ' GROUP BY l.l_orderkey,n.n_name'
    else:
        projection = f'l.l_orderkey, n.n_name AS nation, {amount} AS amount'
        group = ''
    return (
        f'SELECT {projection} FROM {relation(line,"l")} '
        f'JOIN {relation(supply,"g")} ON l.l_partkey=g.ps_partkey AND l.l_suppkey=g.ps_suppkey '
        f'JOIN {relation(nation,"n")} ON n.n_nationkey=g.s_nationkey' + group
    )


def finish_on_aws(orders, partial):
    return (
        'SELECT v.nation,EXTRACT(YEAR FROM o.o_orderdate) AS o_year,'
        'SUM(v.amount) AS sum_profit '
        f'FROM {relation(partial,"v")} '
        f'JOIN {relation(orders,"o")} ON v.l_orderkey=o.o_orderkey '
        'GROUP BY v.nation,EXTRACT(YEAR FROM o.o_orderdate) '
        'ORDER BY v.nation,o_year DESC'
    )


def execute_plan(duckdb, files, params, plan, threads, trial):
    """Emulate exactly specified directed edges using Parquet temp files locally."""
    fragments = base_parts(files, params)
    con = connection(duckdb, threads)
    edges = []
    sum_stage = 0.0
    begin = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix=f'egressopt_q09_{plan}_') as td:
            work = Path(td)

            def ship(src, dst, label, query):
                nonlocal sum_stage
                wanted = PLAN_EDGES[plan][len(edges)]
                if wanted != (src, dst, label):
                    raise ValueError(f'Unexpected Q09 exchange edge: {src, dst, label}, expected {wanted}')
                file = work / f'{len(edges):02d}_{label}.parquet'
                started = time.perf_counter()
                con.execute(f'COPY ({query}) TO {quoted(str(file))} (FORMAT PARQUET,COMPRESSION ZSTD)')
                duration = time.perf_counter() - started
                row_count = con.execute('SELECT COUNT(*) FROM ' + scan(file)).fetchone()[0]
                edges.append({
                    'label': label, 'source_cloud': src, 'destination_cloud': dst,
                    'rows': int(row_count), 'parquet_bytes': file.stat().st_size,
                    'sha256': sha_file(file), 'stage_seconds': duration,
                    'stage_sql_sha256': sha_bytes(query.encode('utf-8')),
                    'execution_mode': 'local_emulation',
                })
                sum_stage += duration
                return scan(file)

            if plan == 'azure_join':
                supply = ship('gcp', 'azure', 'green_supply', fragments['green_supply'])
                orders = ship('aws', 'azure', 'orders_projection', fragments['orders_projection'])
                final = full_profit_query(orders, fragments['lineitem'], supply, fragments['nation'])
            elif plan == 'gcp_join':
                orders = ship('aws', 'gcp', 'orders_projection', fragments['orders_projection'])
                line = ship('azure', 'gcp', 'line_projection', fragments['line_projection'])
                nations = ship('azure', 'gcp', 'nation_projection', fragments['nation_projection'])
                final = full_profit_query(orders, line, fragments['green_supply'], nations)
            elif plan == 'aws_join':
                supply = ship('gcp', 'azure', 'green_supply', fragments['green_supply'])
                partial = ship('azure', 'aws', 'joined_line_profit',
                               joined_line_profit_query(fragments['lineitem'], supply, fragments['nation']))
                final = finish_on_aws(fragments['orders'], partial)
            elif plan == 'partial_aggregate_aws':
                supply = ship('gcp', 'azure', 'green_supply', fragments['green_supply'])
                partial = ship('azure', 'aws', 'order_nation_profit',
                               joined_line_profit_query(fragments['lineitem'], supply, fragments['nation'], True))
                final = finish_on_aws(fragments['orders'], partial)
            else:
                raise ValueError(f'Unknown Q09 plan: {plan}')

            explain = '\n'.join(str(row[-1]) for row in con.execute('EXPLAIN ' + final).fetchall())
            exec_start = time.perf_counter()
            raw = con.execute(final).fetchall()
            join_seconds = time.perf_counter() - exec_start
            total_seconds = time.perf_counter() - begin
            answer = canonical(raw)
            return {
                'query_id': QUERY_ID, 'plan_id': plan, 'trial': trial,
                'scale_factor': 1, 'placement_id': PLACEMENT_ID,
                'execution_mode': 'local_emulation',
                'transfer_edges': edges,
                'intermediate_parquet_bytes': sum(e['parquet_bytes'] for e in edges),
                'intermediate_rows': sum(e['rows'] for e in edges),
                'stage_seconds': sum_stage, 'join_aggregate_seconds': join_seconds,
                'total_seconds': total_seconds,
                'result': answer, 'result_sha256': result_hash(answer),
                'raw_result_strings': [[str(v) for v in row] for row in raw],
                'physical_plan': explain, 'physical_plan_sha256': sha_bytes(explain.encode()),
                'final_sql_sha256': sha_bytes(final.encode()),
                'network_bytes_measured': False, 'network_seconds_measured': False,
                'egress_usd_estimated': None, 'timestamp_utc': now_utc(),
            }
    finally:
        con.close()


def summary_row(run_id, record):
    return {
        'run_id': run_id, 'query_id': QUERY_ID,
        'scale_factor': 1, 'placement_id': PLACEMENT_ID,
        'execution_mode': record['execution_mode'],
        'plan_id': record['plan_id'], 'trial': record['trial'],
        'intermediate_parquet_bytes': record['intermediate_parquet_bytes'],
        'intermediate_rows': record['intermediate_rows'],
        'edge_count': len(record['transfer_edges']),
        'stage_seconds': record['stage_seconds'],
        'join_aggregate_seconds': record['join_aggregate_seconds'],
        'total_seconds': record['total_seconds'],
        'result_sha256': record['result_sha256'],
        'correct_vs_reference': record['correct_vs_reference'],
        'network_bytes_measured': False, 'network_seconds_measured': False,
        'egress_usd_estimated': '', 'timestamp_utc': record['timestamp_utc'],
    }


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def prepare(args):
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError('Please activate the egressopt environment with DuckDB installed') from exc
    sql_file, files = inputs(args)
    sql_text = sql_file.read_text(encoding='utf-8')
    parameters = source_parameters(sql_text)
    reference, reference_raw = authoritative_answer(duckdb, files, sql_text, args.threads)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8]
    directory = output_root(args) / run_id
    directory.mkdir(parents=True, exist_ok=False)
    manifest = {
        'query_id': QUERY_ID, 'run_id': run_id, 'sf': args.sf,
        'placement': PLACEMENT_ID, 'execution_mode': 'local_emulation',
        'status': 'RUNNING', 'created_utc': now_utc(),
        'source_sql': str(sql_file.resolve()), 'source_sql_text': sql_text,
        'source_sql_sha256': sha_file(sql_file), 'parameters': parameters,
        'reference_result': reference, 'reference_raw': reference_raw,
        'reference_sha256': result_hash(reference),
        'data_sources': {table: {'cloud': TABLE_LOCATION[table],
                                 'file': str(path.resolve()), 'bytes': path.stat().st_size,
                                 'sha256': sha_file(path)} for table, path in files.items()},
        'code_sha256': sha_file(Path(__file__).resolve()),
        'python_version': platform.python_version(),
        'duckdb_version': duckdb.__version__, 'os_platform': platform.platform(),
        'threads': args.threads, 'trials_per_plan': args.trials,
        'plan_order': list(PLANS), 'expected_edges': PLAN_EDGES,
        'precision': 'Decimal rounded to 0.01 for reference comparison; raw values preserved',
        'limitation': 'LOCAL PARQUET EXCHANGE EMULATION, NOT MEASURED CLOUD EGRESS',
    }
    dump_json(directory / 'run.json', manifest)
    rows = []
    try:
        for trial in range(1, args.trials + 1):
            rotation = (trial - 1) % len(PLANS)
            for plan in PLANS[rotation:] + PLANS[:rotation]:
                record = execute_plan(duckdb, files, parameters, plan, args.threads, trial)
                record['run_id'] = run_id
                record['correct_vs_reference'] = record['result_sha256'] == manifest['reference_sha256']
                target = directory / 'trials' / plan / f'trial_{trial:03d}.json'
                dump_json(target, record)
                rows.append(summary_row(run_id, record))
                write_csv(directory / 'trial_metrics.csv', rows, METRIC_FIELDS)
                print(f'{plan:<23} trial={trial} '
                      f'correctness={"PASS" if record["correct_vs_reference"] else "FAIL"} '
                      f'emulated_intermediate={record["intermediate_parquet_bytes"]:,} bytes '
                      f'edges={len(record["transfer_edges"])} '
                      f'local_workflow={record["total_seconds"]:.3f}s', flush=True)
                if not record['correct_vs_reference']:
                    raise ValueError(f'Q09 plan {plan} does not reproduce archived source SQL')
        manifest['status'] = 'COMPLETED'
        manifest['completed_utc'] = now_utc()
        manifest['validated_trial_count'] = len(rows)
        dump_json(directory / 'run.json', manifest)
        print('Q09_LOCAL_PLAN_VALIDATION_PASS')
        print(f'Q09 local evidence: {directory}')
        print('NOTE: NO inter-cloud network bytes, timing, or billed egress were measured.')
        return 0
    except Exception as exc:
        manifest['status'] = 'FAILED'
        manifest['failure'] = f'{type(exc).__name__}: {exc}'
        dump_json(directory / 'run.json', manifest)
        print(f'Q09_LOCAL_PLAN_VALIDATION_FAILED: {exc}', file=sys.stderr)
        raise


def select_run(args):
    base = output_root(args)
    if args.run_id:
        directory = base / args.run_id
        if not directory.is_dir():
            raise FileNotFoundError(f'Q09 run not found: {directory}')
        return directory
    if not base.is_dir():
        raise FileNotFoundError('No Q09 results yet: run --mode prepare')
    candidates = []
    for directory in base.iterdir():
        if not directory.is_dir() or not (directory / 'run.json').exists():
            continue
        try:
            manifest = load_json(directory / 'run.json')
        except (ValueError, OSError):
            continue
        if (manifest.get('query_id') == QUERY_ID and manifest.get('status') == 'COMPLETED'
                and (directory / 'trial_metrics.csv').exists()):
            candidates.append(directory)
    if not candidates:
        raise FileNotFoundError('No completed Q09 evidence found')
    return sorted(candidates)[-1]


def audit_records(directory):
    manifest = load_json(directory / 'run.json')
    if manifest.get('query_id') != QUERY_ID or manifest.get('status') != 'COMPLETED':
        raise ValueError('Q09 manifest is not a completed run')
    if manifest.get('sf') != 1 or manifest.get('placement') != PLACEMENT_ID:
        raise ValueError('Wrong Q09 scale factor or placement')
    if manifest.get('execution_mode') != 'local_emulation':
        raise ValueError('Q09 cloud evidence audit not implemented in this release')
    if sha_bytes(manifest['source_sql_text'].encode('utf-8')) != manifest['source_sql_sha256']:
        raise ValueError('Q09 archived source SQL hash mismatch')
    if source_parameters(manifest['source_sql_text']) != manifest['parameters']:
        raise ValueError('Q09 stored parameters do not match source SQL')
    reference_hash = result_hash(manifest['reference_result'])
    if reference_hash != manifest['reference_sha256']:
        raise ValueError('Q09 reference result fingerprint mismatch')
    with (directory / 'trial_metrics.csv').open(newline='', encoding='utf-8') as stream:
        metrics = list(csv.DictReader(stream))
    want = len(PLANS) * int(manifest['trials_per_plan'])
    if len(metrics) != want or want != int(manifest['validated_trial_count']):
        raise ValueError('Q09 trial count mismatch')
    seen = set()
    for m in metrics:
        plan, trial = m['plan_id'], int(m['trial'])
        if plan not in PLANS or trial < 1 or trial > int(manifest['trials_per_plan']):
            raise ValueError('Invalid Q09 plan/trial in CSV')
        key = (plan, trial)
        if key in seen:
            raise ValueError('Duplicated Q09 plan/trial record')
        seen.add(key)
        rec = load_json(directory / 'trials' / plan / f'trial_{trial:03d}.json')
        if (rec['run_id'] != manifest['run_id'] or rec['plan_id'] != plan or rec['trial'] != trial
                or rec['execution_mode'] != 'local_emulation' or m['execution_mode'] != 'local_emulation'):
            raise ValueError('Q09 JSON/CSV identity mismatch')
        if result_hash(rec['result']) != reference_hash or rec['result_sha256'] != reference_hash:
            raise ValueError('Q09 trial result diverges from reference')
        if rec['physical_plan_sha256'] != sha_bytes(rec['physical_plan'].encode()):
            raise ValueError('Q09 physical plan fingerprint mismatch')
        edges = rec['transfer_edges']
        if [(e['source_cloud'], e['destination_cloud'], e['label']) for e in edges] != PLAN_EDGES[plan]:
            raise ValueError('Q09 directed transfer graph mismatch')
        for edge in edges:
            if not re.fullmatch(r'[0-9a-f]{64}', edge['sha256']):
                raise ValueError('Q09 intermediate SHA-256 invalid')
            if not re.fullmatch(r'[0-9a-f]{64}', edge['stage_sql_sha256']):
                raise ValueError('Q09 staged SQL SHA-256 invalid')
            if int(edge['rows']) < 0 or int(edge['parquet_bytes']) <= 0:
                raise ValueError('Invalid Q09 staged row/byte count')
        byte_total = sum(int(e['parquet_bytes']) for e in edges)
        row_total = sum(int(e['rows']) for e in edges)
        if (byte_total != int(rec['intermediate_parquet_bytes']) or
                row_total != int(rec['intermediate_rows']) or
                byte_total != int(m['intermediate_parquet_bytes']) or
                row_total != int(m['intermediate_rows']) or
                len(edges) != int(m['edge_count'])):
            raise ValueError('Q09 transfer totals disagree with trial CSV')
        for item in ('stage_seconds', 'join_aggregate_seconds', 'total_seconds'):
            v, mv = float(rec[item]), float(m[item])
            if not (0 <= v < 24 * 3600 and abs(v - mv) <= max(1e-9, v * 1e-9)):
                raise ValueError('Q09 trial timing mismatch')
        if (rec['network_bytes_measured'] or rec['network_seconds_measured'] or
                m['network_bytes_measured'] != 'False' or m['network_seconds_measured'] != 'False'):
            raise ValueError('Q09 local emulation mislabeled as cloud network evidence')
        if not rec['correct_vs_reference'] or m['correct_vs_reference'] != 'True':
            raise ValueError('Q09 incorrect correctness flag')
        if m['result_sha256'] != reference_hash:
            raise ValueError('Q09 result hash differs between files')
    return manifest, metrics


def audit(args):
    directory = select_run(args)
    if load_json(directory / 'run.json').get('execution_mode') == 'real_multicloud':
        return q09_audit_cloud_action(directory)
    manifest, metrics = audit_records(directory)
    dump_json(directory / 'q09_audit.json', {
        'run_id': manifest['run_id'], 'audited_at_utc': now_utc(),
        'status': 'PASS', 'record_count': len(metrics),
        'source_sql_sha256': manifest['source_sql_sha256'],
        'reference_sha256': manifest['reference_sha256'],
        'limitations': [
            'Offline manifest/record consistency does not rerun SQL or Parquet execution.',
            'Staged intermediate Parquet files are temporary; their bytes cannot be rehashed now.',
            'This local run contains no measured network traffic or egress charges.',
        ],
    })
    print('Q09_EVIDENCE_AUDIT_PASS')
    print(f'Evidence: {directory}')
    return 0


def reproduce(args):
    directory = select_run(args)
    if load_json(directory / 'run.json').get('execution_mode') == 'real_multicloud':
        return q09_reproduce_cloud(directory, args)
    manifest, metrics = audit_records(directory)
    analysis = directory / 'q09_analysis'
    analysis.mkdir(parents=True, exist_ok=True)
    summary = []
    for plan in PLANS:
        recs = [m for m in metrics if m['plan_id'] == plan]
        seconds = [float(m['total_seconds']) for m in recs]
        bytes_ = [int(m['intermediate_parquet_bytes']) for m in recs]
        summary.append({
            'plan_id': plan, 'execution_mode': 'local_emulation',
            'trials': len(recs), 'mean_local_seconds': statistics.mean(seconds),
            'median_local_seconds': statistics.median(seconds),
            'sd_local_seconds': statistics.stdev(seconds) if len(seconds) > 1 else None,
            'mean_staged_parquet_bytes': statistics.mean(bytes_),
            'min_staged_parquet_bytes': min(bytes_),
            'max_staged_parquet_bytes': max(bytes_),
            'edge_count': len(PLAN_EDGES[plan]), 'correct_trials': len(recs),
        })
    write_csv(analysis / 'plan_summary.csv', summary, list(summary[0]))
    dump_json(analysis / 'paper_summary.json', {
        'query_id': QUERY_ID, 'sf': 1, 'placement': PLACEMENT_ID,
        'run_id': manifest['run_id'], 'mode': 'local_emulation', 'plans': summary,
        'limitation': 'LOCAL staging only; not measured multi-cloud transfers or timings',
    })
    latex = [
        '% Q09 SF1/P1 — LOCAL EMULATION ONLY; not a cloud measurement',
        r'\begin{tabular}{lrrr}', r'\hline',
        r'Plan & Trials & Mean local (ms) & Mean staged bytes \\', r'\hline',
    ]
    for value in summary:
        plan = value['plan_id'].replace('_', r'\_')
        latex.append(f'{plan} & {value["trials"]} & {value["mean_local_seconds"] * 1000:.2f} & '
                     f'{value["mean_staged_parquet_bytes"]:.0f} ' + r'\\')
    latex += [r'\hline', r'\end{tabular}']
    (analysis / 'q09_table_local.tex').write_text('\n'.join(latex) + '\n', encoding='utf-8')
    if args.plots:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(7.7, 3.8))
        xx = range(len(PLANS))
        ax.bar(xx, [row['mean_staged_parquet_bytes'] / 1e6 for row in summary])
        ax.set_xticks(list(xx), PLANS, rotation=17, ha='right')
        ax.set_ylabel('Staged Parquet payload (MB)')
        ax.set_title('Q09 SF1/P1: LOCAL shipping emulation (NOT cloud traffic)')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(analysis / f'q09_local_staged_bytes.{ext}', dpi=220)
        plt.close(fig)
        fig, ax = plt.subplots(figsize=(7.7, 3.8))
        for i, plan in enumerate(PLANS):
            vals = [float(m['total_seconds']) * 1000 for m in metrics if m['plan_id'] == plan]
            ax.scatter([i] * len(vals), vals)
        ax.set_xticks(list(xx), PLANS, rotation=17, ha='right')
        ax.set_ylabel('Local stage + query duration (ms)')
        ax.set_title('Q09 SF1/P1: LOCAL workflow (NOT cloud latency)')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(analysis / f'q09_local_workflow_ms.{ext}', dpi=220)
        plt.close(fig)
    print('Q09_REPRODUCE_PASS')
    print(f'Reproduced Q09 local tables and figures: {analysis}')
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--mode', required=True, choices=('prepare', 'cloud', 'reproduce', 'audit', 'cleanup', 'selfcheck', 'optimizer-preview', 'optimizer-cloud'))
    parser.add_argument('--trials', type=int, default=10)
    parser.add_argument('--warmup-trials', type=int, default=2)
    parser.add_argument('--stop-all-after-run', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--sf', type=int, default=DEFAULT_SF)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--data-dir', type=str, default=None)
    parser.add_argument('--sql', type=str, default=None)
    parser.add_argument('--results-root', type=str, default=None)
    parser.add_argument('--run-id', type=str, default=None)
    parser.add_argument('--prospective-run',action='store_true',default=False,
                        help='Audit/reproduce/cleanup a dedicated optimizer_prospective run')
    parser.add_argument('--optimizer-archive',default='EgressOpt_Paper_Input.zip')
    parser.add_argument('--optimizer-tariffs',default='tariffs_public_list_marginal.json')
    parser.add_argument('--optimizer-delta',type=float,default=0.5)
    parser.add_argument('--plots', action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args(argv)
    if args.sf not in VALIDATED_SF:
        parser.error(f'Q09 SF{args.sf} not yet validated; supported: {VALIDATED_SF}')
    if args.trials < 1 or args.trials > 1000:
        parser.error('--trials must be from 1 to 1000')
    if args.threads < 1 or args.threads > 128:
        parser.error('--threads must be from 1 to 128')
    if args.warmup_trials < 0 or args.warmup_trials > 100:
        parser.error('--warmup-trials must be from 0 to 100')
    if args.mode == 'selfcheck':
        return q09_selfcheck()
    if args.mode == 'prepare':
        return prepare(args)
    if args.mode == 'audit':
        return audit(args)
    if args.mode == 'reproduce':
        return reproduce(args)
    if args.mode == 'cloud':
        return q09_cloud(args)
    if args.mode in ('optimizer-preview','optimizer-cloud'):
        decision=q09_optimizer_decision(args)
        print('Q09_EGRESSOPT_PREDECISION', json.dumps({
          'selected_plan':decision['selected_plan'],
          'predeclared_baseline':decision['baseline_plan'],
          'estimated_slo_ms':decision['predicted_slo_ms'],
          'pricing_scenario':decision['tariff_scenario_id'],
          'tariff_vs_bytes_same':decision['selected_plan']==decision['bytes_only_plan']},sort_keys=True),flush=True)
        if args.mode=='optimizer-preview':
            print('Q09_EGRESSOPT_PREVIEW_PASS (NO cloud resources started)')
            return 0
        args.prospective_run=True
        args._q09_optimizer_decision=decision
        args._q09_run_plans=tuple(dict.fromkeys((decision['selected_plan'],decision['baseline_plan'])))
        if len(args._q09_run_plans)!=2:
            raise ValueError('Q09 prospective comparison must contain two distinct plans')
        return q09_cloud(args)
    if args.mode == 'cleanup':
        return q09_cleanup_failed_run(args)
    raise AssertionError('unreachable')



# Q09 cloud engine is self-contained: no import of q05.py, q12.py, or q14.py.
utcnow = now_utc

Q09_WORKER_SOURCE = '#!/usr/bin/env python3\n"""Ephemeral Q09 HTTPS worker; embedded in experiments/q09.py at release time."""\nimport argparse\nimport base64\nimport gzip\nimport hashlib\nimport hmac\nimport json\nimport os\nimport re\nimport ssl\nimport time\nfrom decimal import Decimal, ROUND_HALF_EVEN\nfrom http.server import ThreadingHTTPServer, BaseHTTPRequestHandler\nfrom pathlib import Path\nimport duckdb\n\nCLOUD = os.environ[\'Q09_CLOUD\']\nROOT = Path(os.environ[\'Q09_RUNTIME\'])\nDATA = Path(os.environ[\'Q09_DATA_DIR\'])\nTOKEN = os.environ[\'Q09_WORKER_TOKEN\']\nPATTERN = os.environ[\'Q09_PART_PATTERN\']\nPORT = 8443\nPLANS = (\'azure_join\', \'gcp_join\', \'aws_join\', \'partial_aggregate_aws\')\nSLUG = re.compile(r\'[wm][0-9]{2,3}\')\nSHA = re.compile(r\'[0-9a-f]{64}\')\nLOCATIONS = {\n    \'orders\': \'aws\', \'lineitem\': \'azure\', \'nation\': \'azure\',\n    \'part\': \'gcp\', \'partsupp\': \'gcp\', \'supplier\': \'gcp\',\n}\nPIPE = {\n    \'azure_join\': [(\'gcp\', \'azure\', \'green_supply\'), (\'aws\', \'azure\', \'orders_projection\')],\n    \'gcp_join\': [(\'aws\', \'gcp\', \'orders_projection\'), (\'azure\', \'gcp\', \'line_projection\'),\n                 (\'azure\', \'gcp\', \'nation_projection\')],\n    \'aws_join\': [(\'gcp\', \'azure\', \'green_supply\'), (\'azure\', \'aws\', \'joined_line_profit\')],\n    \'partial_aggregate_aws\': [(\'gcp\', \'azure\', \'green_supply\'), (\'azure\', \'aws\', \'order_nation_profit\')],\n}\n\n\ndef lit(value):\n    return "\'" + str(value).replace("\'", "\'\'") + "\'"\n\n\ndef scan(path):\n    return \'read_parquet(\' + lit(str(path)) + \')\'\n\n\ndef ref(sql, alias):\n    sql = sql.strip()\n    if re.match(r\'^(SELECT|WITH)\\b\', sql, flags=re.I):\n        return \'(\' + sql + \') \' + alias\n    if re.match(r\'^read_parquet\\s*\\(\', sql, flags=re.I):\n        return sql + \' \' + alias\n    raise ValueError(\'Unsupported Q09 relation\')\n\n\ndef local(table):\n    if LOCATIONS[table] != CLOUD:\n        raise RuntimeError(\'Q09 \' + CLOUD + \' cannot read local table \' + table)\n    path = DATA / (table + \'.parquet\')\n    if not path.is_file():\n        raise FileNotFoundError(path)\n    return scan(path)\n\n\ndef received(trial, plan, part):\n    path = ROOT / \'received\' / trial / plan / (part + \'.parquet\')\n    if not path.is_file():\n        raise RuntimeError(\'Q09 received intermediate missing: \' + part)\n    return scan(path)\n\n\ndef fragment(trial, plan, part):\n    if part == \'green_supply\':\n        return (\'SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_supplycost, s.s_nationkey \'\n                \'FROM \' + local(\'part\') + \' p JOIN \' + local(\'partsupp\') + \' ps \'\n                \'ON p.p_partkey=ps.ps_partkey JOIN \' + local(\'supplier\') + \' s \'\n                \'ON ps.ps_suppkey=s.s_suppkey WHERE p.p_name LIKE \' + lit(PATTERN))\n    if part == \'orders_projection\':\n        return \'SELECT o_orderkey,o_orderdate FROM \' + local(\'orders\')\n    if part == \'line_projection\':\n        return (\'SELECT l_orderkey,l_partkey,l_suppkey,l_extendedprice,l_discount,l_quantity \'\n                \'FROM \' + local(\'lineitem\'))\n    if part == \'nation_projection\':\n        return \'SELECT n_nationkey,n_name FROM \' + local(\'nation\')\n    if part in (\'joined_line_profit\', \'order_nation_profit\'):\n        l = local(\'lineitem\')\n        g = received(trial, plan, \'green_supply\')\n        n = local(\'nation\')\n        amount = \'l.l_extendedprice*(1-l.l_discount)-g.ps_supplycost*l.l_quantity\'\n        if part == \'order_nation_profit\':\n            cols = \'l.l_orderkey, n.n_name AS nation, SUM(\' + amount + \') AS amount\'\n            grouping = \' GROUP BY l.l_orderkey,n.n_name\'\n        else:\n            cols = \'l.l_orderkey, n.n_name AS nation, \' + amount + \' AS amount\'\n            grouping = \'\'\n        return (\'SELECT \' + cols + \' FROM \' + ref(l, \'l\') + \' JOIN \' + ref(g, \'g\') +\n                \' ON l.l_partkey=g.ps_partkey AND l.l_suppkey=g.ps_suppkey JOIN \' +\n                ref(n, \'n\') + \' ON n.n_nationkey=g.s_nationkey\' + grouping)\n    raise ValueError(\'Unknown Q09 fragment \' + part)\n\n\ndef full_profit(orders, line, supply, nation):\n    return (\'SELECT n.n_name AS nation,EXTRACT(YEAR FROM o.o_orderdate) AS o_year,\'\n            \'SUM(l.l_extendedprice*(1-l.l_discount)-g.ps_supplycost*l.l_quantity) AS sum_profit \'\n            \'FROM \' + ref(line,\'l\') + \' JOIN \' + ref(supply,\'g\') +\n            \' ON l.l_partkey=g.ps_partkey AND l.l_suppkey=g.ps_suppkey JOIN \' +\n            ref(orders,\'o\') + \' ON o.o_orderkey=l.l_orderkey JOIN \' + ref(nation,\'n\') +\n            \' ON n.n_nationkey=g.s_nationkey GROUP BY n.n_name,EXTRACT(YEAR FROM o.o_orderdate) \'\n            \'ORDER BY nation,o_year DESC\')\n\n\ndef finish_on_aws(partial):\n    return (\'SELECT v.nation,EXTRACT(YEAR FROM o.o_orderdate) AS o_year,SUM(v.amount) AS sum_profit \'\n            \'FROM \' + ref(partial,\'v\') + \' JOIN \' + ref(local(\'orders\'),\'o\') +\n            \' ON v.l_orderkey=o.o_orderkey GROUP BY v.nation,EXTRACT(YEAR FROM o.o_orderdate) \'\n            \'ORDER BY v.nation,o_year DESC\')\n\n\ndef final_sql(trial, plan):\n    if plan == \'azure_join\' and CLOUD == \'azure\':\n        return full_profit(received(trial,plan,\'orders_projection\'),local(\'lineitem\'),\n                           received(trial,plan,\'green_supply\'),local(\'nation\'))\n    if plan == \'gcp_join\' and CLOUD == \'gcp\':\n        return full_profit(received(trial,plan,\'orders_projection\'),\n                           received(trial,plan,\'line_projection\'),\n                           fragment(trial,plan,\'green_supply\'),\n                           received(trial,plan,\'nation_projection\'))\n    if plan == \'aws_join\' and CLOUD == \'aws\':\n        return finish_on_aws(received(trial,plan,\'joined_line_profit\'))\n    if plan == \'partial_aggregate_aws\' and CLOUD == \'aws\':\n        return finish_on_aws(received(trial,plan,\'order_nation_profit\'))\n    raise RuntimeError(\'Q09 finish destination mismatches plan\')\n\n\ndef canonical(raw):\n    out = []\n    seen = set()\n    for row in raw:\n        if len(row) != 3 or any(x is None for x in row):\n            raise ValueError(\'Invalid Q09 result row\')\n        nation, year, profit = str(row[0]), int(row[1]), Decimal(str(row[2]))\n        if not profit.is_finite() or (nation,year) in seen:\n            raise ValueError(\'Nonfinite or duplicate Q09 result\')\n        seen.add((nation,year))\n        out.append({\'nation\':nation,\'o_year\':year,\n                    \'sum_profit\':format(profit.quantize(Decimal(\'0.01\'),rounding=ROUND_HALF_EVEN),\'.2f\')})\n    return sorted(out,key=lambda x:(x[\'nation\'],-x[\'o_year\']))\n\n\ndef sha(path):\n    h=hashlib.sha256()\n    with path.open(\'rb\') as f:\n        for block in iter(lambda:f.read(1048576),b\'\'):\n            h.update(block)\n    return h.hexdigest()\n\n\nclass Handler(BaseHTTPRequestHandler):\n    server_version=\'EgressOptQ09/1.0\'\n    def log_message(self,*args):\n        pass\n\n    def response(self,status,obj):\n        payload=json.dumps(obj,sort_keys=True,separators=(\',\',\':\')).encode()\n        self.send_response(status)\n        self.send_header(\'Content-Type\',\'application/json\')\n        self.send_header(\'Content-Length\',str(len(payload)))\n        self.end_headers()\n        self.wfile.write(payload)\n\n    def authorized(self):\n        if not hmac.compare_digest(self.headers.get(\'Authorization\',\'\'),\'Bearer \'+TOKEN):\n            self.response(401,{\'error\':\'Unauthorized\'})\n            return False\n        return True\n\n    def metadata(self):\n        n=int(self.headers.get(\'Content-Length\',\'0\'))\n        if not 1<=n<=2048: raise ValueError(\'Unexpected JSON request size\')\n        obj=json.loads(self.rfile.read(n))\n        trial,plan=obj.get(\'trial_id\'),obj.get(\'plan_id\')\n        if not isinstance(trial,str) or not SLUG.fullmatch(trial) or plan not in PLANS:\n            raise ValueError(\'Malformed trial or plan\')\n        return obj,trial,plan\n\n    def do_GET(self):\n        if not self.authorized(): return\n        if self.path == \'/health\':\n            self.response(200,{\'status\':\'ok\',\'cloud\':CLOUD,\'query\':\'Q09\',\'duckdb\':duckdb.__version__})\n        else:\n            self.response(404,{\'error\':\'not_found\'})\n\n    def do_POST(self):\n        if not self.authorized(): return\n        try:\n            if self.path == \'/q09/stage\':\n                obj,trial,plan=self.metadata()\n                part=obj.get(\'part\')\n                if (CLOUD,part) not in [(src,label) for src,dst,label in PIPE[plan]]:\n                    raise ValueError(\'Q09 invalid stage/cloud pairing\')\n                sql=fragment(trial,plan,part)\n                out=ROOT/\'stage\'/trial/plan/(part+\'.parquet\')\n                out.parent.mkdir(parents=True,exist_ok=True)\n                started=time.perf_counter()\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    con.execute(\'COPY (\'+sql+\') TO \'+lit(out)+\' (FORMAT PARQUET,COMPRESSION ZSTD)\')\n                    rows=int(con.execute(\'SELECT COUNT(*) FROM \'+scan(out)).fetchone()[0])\n                self.response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'part\':part,\n                                   \'rows\':rows,\'parquet_bytes\':out.stat().st_size,\n                                   \'sha256\':sha(out),\'stage_seconds\':time.perf_counter()-started,\n                                   \'file_path\':str(out)})\n            elif self.path.startswith(\'/q09/upload/\'):\n                parts=self.path.split(\'/\')\n                if len(parts)!=6 or parts[1:3]!=[\'q09\',\'upload\']:\n                    raise ValueError(\'Invalid upload endpoint\')\n                trial,plan,part=parts[3:]\n                if not SLUG.fullmatch(trial) or plan not in PIPE or (\n                        (CLOUD,part) not in [(dst,label) for src,dst,label in PIPE[plan]]):\n                    raise ValueError(\'Unexpected Q09 upload target\')\n                expected=self.headers.get(\'X-Q09-SHA256\',\'\')\n                if not SHA.fullmatch(expected): raise ValueError(\'Invalid SHA-256 header\')\n                n=int(self.headers.get(\'Content-Length\',\'0\'))\n                if not 1<=n<=350_000_000: raise ValueError(\'Excessive Q09 upload\')\n                out=ROOT/\'received\'/trial/plan/(part+\'.parquet\')\n                out.parent.mkdir(parents=True,exist_ok=True)\n                temp=out.with_suffix(\'.part\')\n                digest=hashlib.sha256()\n                remaining=n\n                started=time.perf_counter()\n                with temp.open(\'wb\') as stream:\n                    while remaining:\n                        block=self.rfile.read(min(1048576,remaining))\n                        if not block: raise ValueError(\'Truncated upload\')\n                        stream.write(block)\n                        digest.update(block)\n                        remaining-=len(block)\n                if not hmac.compare_digest(digest.hexdigest(),expected):\n                    temp.unlink(missing_ok=True)\n                    raise ValueError(\'Q09 upload SHA mismatch\')\n                temp.replace(out)\n                self.response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'part\':part,\n                                   \'received_payload_bytes\':n,\'sha256\':digest.hexdigest(),\n                                   \'receive_file_seconds\':time.perf_counter()-started})\n            elif self.path == \'/q09/finish\':\n                obj,trial,plan=self.metadata()\n                sql=final_sql(trial,plan)\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    physical=\'\\n\'.join(str(row[-1]) for row in con.execute(\'EXPLAIN \'+sql).fetchall())\n                    started=time.perf_counter()\n                    raw=con.execute(sql).fetchall()\n                    duration=time.perf_counter()-started\n                result=canonical(raw)\n                explanation=gzip.compress(physical.encode())\n                digest=hashlib.sha256(json.dumps(result,sort_keys=True,separators=(\',\',\':\')).encode()).hexdigest()\n                self.response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\n                                   \'result\':result,\'result_sha256\':digest,\'row_count\':len(result),\n                                   \'join_seconds\':duration,\n                                   \'raw_result_strings\':[[str(v) for v in row] for row in raw],\n                                   \'explain_gzip_b64\':base64.b64encode(explanation).decode(),\n                                   \'physical_plan_sha256\':hashlib.sha256(physical.encode()).hexdigest()})\n            else:\n                self.response(404,{\'error\':\'not_found\'})\n        except Exception as exc:\n            self.response(422,{\'error\':type(exc).__name__+\': \'+str(exc)[:500]})\n\n\ndef main():\n    parser=argparse.ArgumentParser()\n    parser.add_argument(\'--cert\',required=True)\n    parser.add_argument(\'--key\',required=True)\n    args=parser.parse_args()\n    server=ThreadingHTTPServer((\'0.0.0.0\',PORT),Handler)\n    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n    context.minimum_version=ssl.TLSVersion.TLSv1_2\n    context.load_cert_chain(args.cert,args.key)\n    server.socket=context.wrap_socket(server.socket,server_side=True)\n    server.serve_forever()\n\n\nif __name__==\'__main__\':\n    main()\n'

AWS_PROFILE=os.environ.get('EGRESSOPT_AWS_PROFILE','')
AWS_REGION=os.environ.get('EGRESSOPT_AWS_REGION','')
AWS_INSTANCE=os.environ.get('EGRESSOPT_AWS_INSTANCE','')
AZ_RG=os.environ.get('EGRESSOPT_AZURE_RESOURCE_GROUP','')
AZ_VM=os.environ.get('EGRESSOPT_AZURE_VM','')
AZ_NSG=os.environ.get('EGRESSOPT_AZURE_NSG','')
AZ_PUBLIC=os.environ.get('EGRESSOPT_AZURE_PUBLIC_IP_RESOURCE','')
GCP_PROJECT=os.environ.get('EGRESSOPT_GCP_PROJECT','')
GCP_ZONE=os.environ.get('EGRESSOPT_GCP_ZONE','')
GCP_VM=os.environ.get('EGRESSOPT_GCP_VM','')
Q09_PORT=8443
Q09_PIPE={
 'azure_join':[('gcp','azure','green_supply'),('aws','azure','orders_projection')],
 'gcp_join':[('aws','gcp','orders_projection'),('azure','gcp','line_projection'),('azure','gcp','nation_projection')],
 'aws_join':[('gcp','azure','green_supply'),('azure','aws','joined_line_profit')],
 'partial_aggregate_aws':[('gcp','azure','green_supply'),('azure','aws','order_nation_profit')],
}
Q09_DEST={'azure_join':'azure','gcp_join':'gcp','aws_join':'aws','partial_aggregate_aws':'aws'}
Q09_CLOUD_TABLES={'aws':('orders',),'azure':('lineitem','nation'),
                  'gcp':('part','partsupp','supplier')}
Q09_CLOUD_VENV={'aws':'/opt/egressopt/venv311/bin/python',
 'azure':'/opt/egressopt/venv/bin/python','gcp':'/opt/egressopt/venv/bin/python'}

# The laptop is the AWS SigV4 signing client, not the EC2 instance.
# Validate its UTC against an HTTPS response before sending AWS commands.
# This detects the clock skew that can appear after laptop suspend/resume.
Q09_CLOCK_MAX_SKEW_SECONDS = 120
Q09_CLOCK_CHECK_INTERVAL_SECONDS = 45
_q09_last_clock_check_monotonic = -1.0
_q09_clock_baseline = None


def q09_verify_aws_clock(force=False):
    """Confirm Mac UTC against AWS time; fail closed for a significant skew.

    The probe only fetches a public HTTPS Date response. When the proxy or
    network does not expose Date, fall back to a fresh, read-only AWS STS
    GetCallerIdentity request. This function does NOT modify the system clock.
    """
    global _q09_last_clock_check_monotonic, _q09_clock_baseline
    mono = time.monotonic()
    now = time.time()
    # Also invalidate the cache when wall time and monotonic time diverge,
    # which can happen on a laptop that sleeps or whose clock changes.
    if (not force and _q09_clock_baseline is not None and
        mono - _q09_last_clock_check_monotonic < Q09_CLOCK_CHECK_INTERVAL_SECONDS and
        abs((now - _q09_clock_baseline[0]) -
            (mono - _q09_clock_baseline[1])) < 5):
        return

    from email.utils import parsedate_to_datetime
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen
    errors=[]
    for endpoint in (f'https://sts.{AWS_REGION}.amazonaws.com/',
                     f'https://ssm.{AWS_REGION}.amazonaws.com/'):
        start=time.time()
        req=Request(endpoint+'?q09_clock_probe='+secrets.token_hex(8),
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
            if abs(skew_seconds) > Q09_CLOCK_MAX_SKEW_SECONDS:
                raise RuntimeError(
                    'Q09_CONTROLLER_CLOCK_SKEW: Mac UTC differs from AWS HTTPS '
                    f'time by approximately {skew_seconds:+.0f} seconds; '
                    'open macOS System Settings > General > Date & Time, '
                    'enable Set time and date automatically, confirm network '
                    'time synchronization, and rerun Q09 cleanup BEFORE a '
                    'new paid benchmark. Do not bypass this verification.')
            _q09_last_clock_check_monotonic=time.monotonic()
            _q09_clock_baseline=(time.time(), _q09_last_clock_check_monotonic)
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
        raise RuntimeError('Q09 cannot verify AWS controller time/authentication: '
                           +'; '.join(errors)+'; '+str(exc)) from exc
    if proc.returncode != 0:
        raise RuntimeError('Q09 AWS clock/authentication preflight failed; '
            'verify macOS automatic date/time and AWS SSO login. '
            'AWS STS: '+proc.stderr[-650:]+'; HTTPS probes: '+'; '.join(errors))
    print('  AWS signed STS preflight passed (HTTPS Date unavailable)',flush=True)
    _q09_last_clock_check_monotonic=time.monotonic()
    _q09_clock_baseline=(time.time(), _q09_last_clock_check_monotonic)


def q09_aws_signature_error(message):
    if ('Signature expired:' in message or
        'Signature not yet current' in message or
        'RequestTimeTooSkewed' in message):
        # Do not retry SendCommand: an expired signature must be diagnosed,
        # and blindly retrying remote commands risks duplicate execution.
        return RuntimeError('Q09_AWS_SIGNATURE_TIME_FAILURE: '+message[-400:]+
            ' | Verify macOS automatic date/time, then run --mode cleanup '
            'for this FAILED run. Do not merge its partial results.')
    return None


def q09_cli(cmd,timeout=480):
 if cmd and cmd[0]=='aws':
  q09_verify_aws_clock()
 try:p=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout)
 except FileNotFoundError as e:raise RuntimeError('Missing cloud CLI '+cmd[0]) from e
 if p.returncode:
  error_text=(p.stderr or p.stdout)[-1300:]
  signature_error=q09_aws_signature_error(error_text)
  if signature_error:raise signature_error
  raise RuntimeError(f'{cmd[0]} failed: '+error_text)
 return p.stdout.strip()

def q09_aws(*arg):
 return json.loads(q09_cli(['aws',*arg,'--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json']))

def q09_gcloud(*args,timeout=450):
 return q09_cli(['gcloud',*args,'--project='+GCP_PROJECT],timeout=timeout)

def q09_aws_instance():
 result=q09_aws('ec2','describe-instances','--instance-ids',AWS_INSTANCE)
 arr=[inst for res in result['Reservations'] for inst in res['Instances']]
 if len(arr)!=1:raise RuntimeError('Q09 AWS instance not unique')
 return arr[0]

def q09_azure_state():
 response=json.loads(q09_cli(['az','vm','get-instance-view','--resource-group',AZ_RG,
 '--name',AZ_VM,'--query','instanceView.statuses[].code','--output','json']))
 power=[s for s in response if s.startswith('PowerState/')]
 if len(power)!=1:raise RuntimeError('Cannot obtain Azure power state')
 return power[0]

def q09_gcp_instance():
 return json.loads(q09_gcloud('compute','instances','describe',GCP_VM,
                           '--zone='+GCP_ZONE,'--format=json'))

def q09_start_vm(cloud):
 if cloud=='aws':
  old=q09_aws_instance()['State']['Name']
  if old=='running':return False
  if old!='stopped':raise RuntimeError('Unsupported AWS state: '+old)
  q09_aws('ec2','start-instances','--instance-ids',AWS_INSTANCE)
  q09_cli(['aws','ec2','wait','instance-running','--instance-ids',AWS_INSTANCE,
           '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=550)
  return True
 if cloud=='azure':
  old=q09_azure_state()
  if old=='PowerState/running':return False
  if old not in ('PowerState/stopped','PowerState/deallocated'):
   raise RuntimeError('Unsupported Azure state: '+old)
  q09_cli(['az','vm','start','--resource-group',AZ_RG,'--name',AZ_VM,'--output','none'],timeout=650)
  return True
 if cloud=='gcp':
  old=q09_gcp_instance()['status']
  if old=='RUNNING':return False
  if old not in ('TERMINATED','STOPPED'):raise RuntimeError('Unsupported GCP state: '+old)
  q09_gcloud('compute','instances','start',GCP_VM,'--zone='+GCP_ZONE,'--quiet','--format=json',timeout=650)
  return True
 raise ValueError(cloud)

def q09_remote(cloud,script,label):
 if cloud=='azure':
  cmd=['az','vm','run-command','invoke','--resource-group',AZ_RG,'--name',AZ_VM,
       '--command-id','RunShellScript','--scripts','set -eu\n'+script,
       '--query','value[0].message','--output','tsv']
  for attempt in range(1,11):
   try:out=q09_cli(cmd,timeout=470)
   except RuntimeError as e:
    if ('OperationNotAllowed' not in str(e) or
      'requires the VM to be running' not in str(e) or attempt==10):raise
    print(f'  Azure Run Command warming up ({attempt}/10): {label}',flush=True)
    time.sleep(min(4+2*attempt,20));continue
   if 'Enable succeeded' not in out:
    raise RuntimeError('Q09 Azure remote '+label+': '+out[-900:])
   return out
 if cloud=='gcp':
  code='sudo -n bash -lc '+shlex.quote('set -eu\n'+script)
  # Retry only the IAP 4003 error that explicitly states port 22 could not
  # connect. The guest command did not start in this failure mode, which makes
  # retrying safe even for non-idempotent upload/deployment steps. Never retry
  # arbitrary SSH exit 255 or an interrupted connection after execution began.
  for attempt in range(1,6):
   try:
    return q09_gcloud('compute','ssh',GCP_VM,'--zone='+GCP_ZONE,
      '--tunnel-through-iap','--quiet','--command='+code,timeout=500)
   except RuntimeError as exc:
    err=str(exc)
    preconnect=(('4003' in err and 'failed to connect to backend' in err.lower())
       and ('port 22' in err or 'ConnectionCreationError' in err))
    if not preconnect:
     raise
    try:
     vm_state=q09_gcp_instance()['status']
    except Exception as state_exc:
     raise RuntimeError('Q09 GCP IAP SSH failed, VM status unavailable; '+
      'do not retry query blindly: '+str(state_exc)) from exc
    if vm_state!='RUNNING':
     raise RuntimeError('Q09 GCP IAP SSH failed and VM state is '+vm_state+
       '; inspect guest startup/stop and prior cleanup') from exc
    if attempt==5:
     raise RuntimeError('Q09 GCP IAP SSH still cannot connect to VM port 22 '
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
    cid=q09_cli(['aws','ssm','send-command','--instance-ids',AWS_INSTANCE,
      '--document-name','AWS-RunShellScript',
      '--parameters',json.dumps({'commands':['set -eu',script]}),
      '--comment',('EgressOpt Q09 '+label)[:100],
      '--query','Command.CommandId','--output','text','--profile',AWS_PROFILE,
      '--region',AWS_REGION],timeout=100)
    break
   except RuntimeError as e:
    if ('InvalidInstanceId' not in str(e) and 'TargetNotConnected' not in str(e)) or attempt==8:
     raise
    time.sleep(min(5*attempt,20))
  deadline=time.monotonic()+470
  while time.monotonic()<deadline:
   q09_verify_aws_clock()
   p=subprocess.run(['aws','ssm','get-command-invocation','--command-id',cid,
    '--instance-id',AWS_INSTANCE,'--profile',AWS_PROFILE,'--region',AWS_REGION,
    '--output','json'],capture_output=True,text=True,timeout=50)
   if p.returncode:
    if 'InvocationDoesNotExist' in p.stderr:time.sleep(2);continue
    signature_error=q09_aws_signature_error(p.stderr)
    if signature_error:raise signature_error
    raise RuntimeError('Q09 AWS SSM '+label+': '+p.stderr[-900:])
   obj=json.loads(p.stdout)
   if obj['Status']=='Success':return obj.get('StandardOutputContent','')
   if obj['Status'] in {'Failed','TimedOut','Cancelled','Cancelling'}:
    raise RuntimeError('Q09 AWS SSM '+label+': '+obj['Status']+' '+
       obj.get('StandardErrorContent','')[-850:]+obj.get('StandardOutputContent','')[-850:])
   time.sleep(2)
  raise RuntimeError('Q09 AWS SSM timeout: '+label)
 raise ValueError(cloud)

def q09_marker(out,prefix='Q09_RPC='):
 found=[line.split(prefix,1)[1].strip() for line in out.splitlines() if line.startswith(prefix)]
 if len(found)!=1:raise RuntimeError('Expected one '+prefix+' remote response: '+out[-350:])
 return found[0]

def q09_rpc(cloud,script,label):
 return json.loads(q09_marker(q09_remote(cloud,script,label)))

def q09_ip(raw):
 ip=ipaddress.ip_address(str(raw).strip())
 if ip.version!=4 or not ip.is_global:raise RuntimeError('Invalid public IPv4: '+str(raw))
 return str(ip)

def q09_outbound(cloud):
 out=q09_remote(cloud,'curl --silent --show-error --fail --connect-timeout 10 --max-time 30 https://checkip.amazonaws.com','public-egress-ip')
 if cloud=='azure' and '[stdout]' in out:out=out.split('[stdout]',1)[1].split('[stderr]',1)[0]
 return q09_ip(out.strip())

def q09_gcp_public(inst):
 for nic in inst['networkInterfaces']:
  for cfg in nic.get('accessConfigs',[]):
   if cfg.get('natIP'):return q09_ip(cfg['natIP'])
 raise RuntimeError('GCP VM has no public IP')

def q09_install_worker(cloud,runkey):
 source=Q09_WORKER_SOURCE.encode()
 release='/opt/egressopt/releases/q09_'+sha_bytes(source)[:12]
 temp=f'/opt/egressopt/runtime/q09_upload_{runkey}_{cloud}'
 encoded=base64.b64encode(gzip.compress(source)).decode()
 q09_remote(cloud,'umask 077; mkdir -p '+shlex.quote(temp)+' '+shlex.quote(release)+
             '; : > '+shlex.quote(temp+'/worker.gz.b64'), 'prepare-worker')
 for idx in range(0,len(encoded),2400):
  q09_remote(cloud,'printf %s '+shlex.quote(encoded[idx:idx+2400])+
   ' >> '+shlex.quote(temp+'/worker.gz.b64'),'upload-worker')
 dest=release+'/worker.py';py=Q09_CLOUD_VENV[cloud]
 script=('base64 -d '+shlex.quote(temp+'/worker.gz.b64')+' | gzip -d > '+shlex.quote(dest)+'\n'+
  'test "$(sha256sum '+shlex.quote(dest)+" | cut -d' ' -f1)\" = "+shlex.quote(sha_bytes(source))+'\n'+
  py+' -m py_compile '+shlex.quote(dest)+'\n'+
  'rm -rf '+shlex.quote(temp)+'\n'+'echo Q09_WORKER_DEPLOYED')
 out=q09_remote(cloud,script,'verify-worker')
 if 'Q09_WORKER_DEPLOYED' not in out:raise RuntimeError('Q09 worker deploy validation failed on '+cloud)
 return release

def q09_start_service(cloud,key,release,public_ip,token,params):
 work=f'/opt/egressopt/runtime/q09_{key}_{cloud}'
 py=Q09_CLOUD_VENV[cloud]
 unit=f'egressopt-q09-{key}-{cloud}'
 launch='\n'.join(['#!/bin/sh','set -eu','export Q09_CLOUD='+shlex.quote(cloud),
  'export Q09_RUNTIME='+shlex.quote(work),
  'export Q09_DATA_DIR=/opt/egressopt/data/tpch_sf1',
  'export Q09_WORKER_TOKEN='+shlex.quote(token),
  'export Q09_PART_PATTERN='+shlex.quote(params['part_name_pattern']),
  'exec '+py+' '+shlex.quote(release+'/worker.py')+
  ' --cert '+shlex.quote(work+'/server.pem')+' --key '+shlex.quote(work+'/server.key')])+'\n'
 b64=base64.b64encode(launch.encode()).decode()
 required=' '.join('/opt/egressopt/data/tpch_sf1/'+t+'.parquet' for t in Q09_CLOUD_TABLES[cloud])
 script=f'''umask 077
mkdir -p {shlex.quote(work)}
test -f {shlex.quote(release+'/worker.py')}
for f in {required}; do test -f "$f"; done
{py} -c 'import duckdb; assert duckdb.__version__ == "1.5.6",duckdb.__version__'
command -v openssl >/dev/null
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 \
 -keyout {work}/ca.key -out {work}/ca.pem -subj '/CN=EgressOpt Q09 temporary CA' \
 -addext 'basicConstraints=critical,CA:TRUE' >/dev/null 2>&1
openssl req -nodes -newkey rsa:2048 -sha256 \
 -keyout {work}/server.key -out {work}/server.csr \
 -subj '/CN=EgressOpt Q09 worker' >/dev/null 2>&1
printf 'basicConstraints=CA:FALSE\\nsubjectAltName=IP:{public_ip}\\nextendedKeyUsage=serverAuth\\n' > {work}/server.ext
openssl x509 -req -days 1 -sha256 -in {work}/server.csr \
 -CA {work}/ca.pem -CAkey {work}/ca.key -CAcreateserial \
 -extfile {work}/server.ext -out {work}/server.pem >/dev/null 2>&1
printf %s {shlex.quote(b64)} | base64 -d > {work}/launch.sh
chmod 700 {work}/launch.sh
systemd-run --unit={unit} --collect --property=Restart=no /bin/sh {work}/launch.sh >/dev/null
STATUS=000
for i in $(seq 1 40); do
 STATUS=$(curl --insecure -s -o /dev/null -w '%{{http_code}}' --max-time 3 https://127.0.0.1:{Q09_PORT}/health || true)
 [ "$STATUS" = 401 ] && break
 sleep 1
done
test "$STATUS" = 401
curl --silent --show-error --fail --insecure --max-time 10 \
 -H {shlex.quote('Authorization: Bearer '+token)} https://127.0.0.1:{Q09_PORT}/health | \
 {py} -c 'import sys,json; x=json.load(sys.stdin); assert x["cloud"]=="{cloud}" and x["query"]=="Q09"'
echo Q09_SERVICE_READY
printf 'Q09_CA_B64='; base64 -w0 {work}/ca.pem; echo
'''
 out=q09_remote(cloud,script,'start-worker')
 if 'Q09_SERVICE_READY' not in out:raise RuntimeError('Q09 TLS startup error on '+cloud)
 ca=q09_marker(out,'Q09_CA_B64=')
 if b'BEGIN CERTIFICATE' not in base64.b64decode(ca,validate=True):raise RuntimeError('Invalid CA on '+cloud)
 return ca

def q09_azure_rule(src,source_ip,key):
 name=f'egressopt-q09-{key}-{src}'
 existing=json.loads(q09_cli(['az','network','nsg','rule','list','--resource-group',AZ_RG,
   '--nsg-name',AZ_NSG,'--output','json']))
 used={int(r['priority']) for r in existing if r.get('direction')=='Inbound'}
 prio=next((i for i in range(1100,2500) if i not in used),None)
 if prio is None:raise RuntimeError('No free Azure ingress priority')
 q09_cli(['az','network','nsg','rule','create','--resource-group',AZ_RG,
   '--nsg-name',AZ_NSG,'--name',name,'--priority',str(prio),'--direction','Inbound',
   '--access','Allow','--protocol','Tcp','--source-address-prefixes',source_ip+'/32',
   '--destination-port-ranges',str(Q09_PORT),'--description','EgressOpt Q09 temporary TLS',
   '--output','none'],timeout=300)
 return name

def q09_aws_rule(source_ip,source,key,group):
 permission=[{'IpProtocol':'tcp','FromPort':Q09_PORT,'ToPort':Q09_PORT,
   'IpRanges':[{'CidrIp':source_ip+'/32','Description':'EgressOpt Q09 '+key+' '+source}]}]
 created=q09_aws('ec2','authorize-security-group-ingress','--group-id',group,
                 '--ip-permissions',json.dumps(permission))
 rules=created.get('SecurityGroupRules',[])
 if len(rules)!=1 or not rules[0].get('SecurityGroupRuleId'):
  raise RuntimeError('AWS Q09 ingress created with missing ID; inspect SG')
 return rules[0]['SecurityGroupRuleId']

def q09_gcp_rule(source_ip,key,network,source):
 tag='egressopt-q09-'+key
 rule=tag+'-'+source
 if tag not in q09_gcp_instance().get('tags',{}).get('items',[]):
  q09_gcloud('compute','instances','add-tags',GCP_VM,'--zone='+GCP_ZONE,
            '--tags='+tag,'--quiet','--format=json')
 # Both AWS and Azure may send Q09 intermediates into the GCP join plan.
 q09_gcloud('compute','firewall-rules','create',rule,'--network='+network,
    '--direction=INGRESS','--action=ALLOW','--rules=tcp:8443',
    '--source-ranges='+source_ip+'/32','--target-tags='+tag,
    '--description=EgressOpt Q09 temporary TLS from '+source,
    '--quiet','--format=json')
 return rule,tag

def q09_stage(cloud,part,plan,trial,token,key):
 work=f'/opt/egressopt/runtime/q09_{key}_{cloud}'
 payload=json.dumps({'part':part,'plan_id':plan,'trial_id':trial},separators=(',',':'))
 dest=work+'/stage_response.json';py=Q09_CLOUD_VENV[cloud]
 script=(f'curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 380 '+
  '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
  '-H '+shlex.quote('Content-Type: application/json')+' '+
  '--data-binary '+shlex.quote(payload)+' '+
  'https://127.0.0.1:8443/q09/stage > '+shlex.quote(dest)+'\n'+
  py+' -c '+shlex.quote('import json; x=json.load(open('+repr(dest)+')); print("Q09_RPC="+json.dumps(x,separators=(",",":")))'))
 result=q09_rpc(cloud,script,'stage-'+plan+'-'+part)
 if (result.get('cloud'),result.get('plan_id'),result.get('trial_id'),result.get('part'))!=(cloud,plan,trial,part):
  raise RuntimeError('Q09 wrong staging identity')
 if (int(result['parquet_bytes'])<1 or int(result['rows'])<0 or not re.fullmatch('[0-9a-f]{64}',result['sha256'])):
  raise RuntimeError('Bad Q09 staged Parquet metadata')
 if result['file_path']!=f'{work}/stage/{trial}/{plan}/{part}.parquet':
  raise RuntimeError('Bad Q09 staged file path')
 return result

def q09_upload(source,dest,stage,plan,trial,ips,cas,token,key):
 part=stage['part'];work=f'/opt/egressopt/runtime/q09_{key}_{source}'
 ca=work+'/peer-ca.pem';reply=work+'/upload_response.json'
 url=f'https://{ips[dest]}:8443/q09/upload/{trial}/{plan}/{part}'
 py=Q09_CLOUD_VENV[source]
 script=('printf %s '+shlex.quote(cas[dest])+' | base64 -d > '+shlex.quote(ca)+'\n'+
  'INFO=$(curl --fail --silent --show-error --connect-timeout 25 --max-time 400 '+
  '--cacert '+shlex.quote(ca)+' '+
  '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
  '-H '+shlex.quote('X-Q09-SHA256: '+stage['sha256'])+' '+
  "-H 'Content-Type: application/octet-stream' "+
  '--data-binary @'+shlex.quote(stage['file_path'])+' '+
  '-o '+shlex.quote(reply)+' '+
  "-w '%{time_total} %{size_upload} %{http_code}' "+shlex.quote(url)+')\n'+
  'export INFO\n'+py+' -c '+shlex.quote(
   'import json,os; a,b,c=os.environ["INFO"].split(); r=json.load(open('+repr(reply)+')); '+
   'print("Q09_RPC="+json.dumps({"http_seconds":float(a),"payload_bytes":int(b),'+
   '"http_status":int(c),"response":r},separators=(",",":")))'))
 rpc=q09_rpc(source,script,'upload-'+plan+'-'+part)
 ack=rpc['response']
 if (rpc['http_status']!=200 or ack.get('cloud')!=dest or ack.get('plan_id')!=plan or
  ack.get('trial_id')!=trial or ack.get('part')!=part or
  ack.get('sha256')!=stage['sha256'] or
  int(rpc['payload_bytes'])!=int(stage['parquet_bytes']) or
  int(ack['received_payload_bytes'])!=int(stage['parquet_bytes'])):
  raise RuntimeError('Q09 real TLS transfer validation failed '+plan+'/'+part)
 return {'label':part,'source_cloud':source,'destination_cloud':dest,
  'rows':int(stage['rows']),'parquet_bytes':int(stage['parquet_bytes']),
  'sha256':stage['sha256'],'stage_seconds':float(stage['stage_seconds']),
  'curl_http_total_seconds':float(rpc['http_seconds']),
  'curl_size_upload':int(rpc['payload_bytes']),
  'receiver_file_seconds':float(ack['receive_file_seconds']),
  'execution_mode':'real_multicloud'}

def q09_collect_finish_response(cloud, first_output, response_b64_path):
 """Recover the complete compressed Q09 finish response despite cloud CLI caps.

 Azure Action Run Command returns only the last 4 KiB of stdout; AWS SSM
 truncates StandardOutputContent at 24,000 chars. Never send a large result
 through either management API in a single line. All chunks are bounded,
 position-checked, and verified against a SHA-256 from the first response.
 Remote chunks are fetched from the immutable response staged on the VM;
 the actual SQL finish is executed only once.
 """
 meta=json.loads(q09_marker(first_output,'Q09_RPC_GZIP_META='))
 size=int(meta['length'])
 digest=meta['sha256']
 size_per=int(meta['chunk_size'])
 if (not 0<size<=2_000_000 or not 0<size_per<=16_000 or
     not re.fullmatch(r'[a-f0-9]{64}',digest) or
     size_per!=({'azure':2200,'aws':16000,'gcp':16000}[cloud])):
  raise RuntimeError('Invalid Q09 remote finish envelope')
 initial=q09_marker(first_output,'Q09_RPC_GZIP_CHUNK=')
 if len(initial)!=min(size,size_per):
  raise RuntimeError('Q09 initial result chunk truncated or wrong length')
 chunks=[initial]
 # Fetch only missing parts. Each remote call emits well below the smallest
 # cloud-control stdout limit; no data is silently lost on output truncation.
 py=Q09_CLOUD_VENV[cloud]
 for offset in range(len(initial),size,size_per):
  length=min(size_per,size-offset)
  body=('from pathlib import Path; s=Path('+repr(response_b64_path)+').read_text(); '
        'print("Q09_RPC_GZIP_CHUNK="+s['+str(offset)+':'+str(offset+length)+'])')
  out=q09_remote(cloud,py+' -c '+shlex.quote(body),'finish-chunk-'+str(offset))
  part=q09_marker(out,'Q09_RPC_GZIP_CHUNK=')
  if len(part)!=length:
   raise RuntimeError('Q09 remote finish chunk truncated at '+str(offset))
  chunks.append(part)
 encoded=''.join(chunks)
 if len(encoded)!=size or sha_bytes(encoded.encode('ascii'))!=digest:
  raise RuntimeError('Q09 compressed result SHA-256 mismatch')
 try:
  return json.loads(gzip.decompress(base64.b64decode(encoded,validate=True)))
 except (ValueError,OSError,UnicodeError) as exc:
  raise RuntimeError('Invalid Q09 compressed result payload') from exc


def q09_selfcheck():
 """Offline Q09 controller transport regression test; never accesses clouds.

 Simulates Azure's 4096-byte retained stdout and two other provider paths,
 validates SHA-256 over full reconstructed payload, and rejects corrupt pages.
 """
 import random
 rng=random.Random(20261016)
 noise=bytes(rng.randrange(256) for _ in range(13000))
 exemplar={'query_id':'Q09','test_only':True,
           'proof_data':base64.b64encode(noise).decode('ascii')}
 original_remote=q09_remote
 try:
  for provider,size_per in (('azure',2200),('aws',16000),('gcp',16000)):
   encoded=base64.b64encode(gzip.compress(json.dumps(exemplar,separators=(',',':')).encode())).decode()
   assert len(encoded)>4096
   metadata={'length':len(encoded),'sha256':sha_bytes(encoded.encode('ascii')),
             'chunk_size':size_per}
   first=('Enable succeeded:\n[stdout]\nQ09_RPC_GZIP_META='+
          json.dumps(metadata,separators=(',',':'))+'\nQ09_RPC_GZIP_CHUNK='+
          encoded[:size_per]+'\n[stderr]\n')
   if provider=='azure':first=first[-4096:]
   if provider=='aws':first=first[:24000]
   def read_mock(cloud,script,label):
    indices=re.search(r's\[(\d+):(\d+)\]',script)
    if not indices:raise AssertionError('Q09 chunk read lacked explicit offsets')
    lo,hi=map(int,indices.groups())
    line='Q09_RPC_GZIP_CHUNK='+encoded[lo:hi]+'\n'
    if cloud=='azure':return ('Enable succeeded:\n[stdout]\n'+line+'[stderr]\n')[-4096:]
    return line
   globals()['q09_remote']=read_mock
   actual=q09_collect_finish_response(provider,first,'/unused/selfcheck.b64')
   if actual!=exemplar:raise AssertionError('Q09 chunked response mismatch: '+provider)
  # An altered chunk must invalidate the whole response, rather than silently
  # producing malformed or misleading query evidence.
  small=base64.b64encode(gzip.compress(json.dumps(exemplar).encode())).decode()
  metadata={'length':len(small),'sha256':sha_bytes(small.encode('ascii')),'chunk_size':2200}
  first='Q09_RPC_GZIP_META='+json.dumps(metadata)+'\nQ09_RPC_GZIP_CHUNK='+small[:2200]
  def corrupt(cloud,script,label):
   indices=re.search(r's\[(\d+):(\d+)\]',script)
   lo,hi=map(int,indices.groups())
   part=small[lo:hi]
   return 'Q09_RPC_GZIP_CHUNK='+('A' if part[0]!='A' else 'B')+part[1:]
  globals()['q09_remote']=corrupt
  try:
   q09_collect_finish_response('azure',first,'/unused/selfcheck.b64')
  except RuntimeError as exc:
   if 'SHA-256 mismatch' not in str(exc):raise
  else:
   raise AssertionError('Q09 accepted tampered compressed response')
 finally:
  globals()['q09_remote']=original_remote
 print('Q09_RUNNER_BUILD: '+Q09_RUNNER_BUILD)
 print('Q09_CHUNKED_TRANSPORT_SELFCHECK_PASS (Azure, AWS, GCP; tamper rejected)')
 return 0


def q09_finish(cloud,plan,trial,token,key):
 work=f'/opt/egressopt/runtime/q09_{key}_{cloud}'
 resp=work+'/finish_response.json'
 reply=work+'/finish_response.b64'
 py=Q09_CLOUD_VENV[cloud]
 payload=json.dumps({'plan_id':plan,'trial_id':trial},separators=(',',':'))
 size_per={'azure':2200,'aws':16000,'gcp':16000}[cloud]
 # We preserve the full actual SQL rows and DuckDB EXPLAIN evidence. Returning
 # only a digest would make independent auditing weaker, so instead spool the
 # response on the guest and print an authenticated-control-channel fragment.
 body=('import json,gzip,base64,hashlib,pathlib; '
       'x=json.load(open('+repr(resp)+')); '
       'b=base64.b64encode(gzip.compress(json.dumps(x,separators=(",",":")).encode())).decode(); '
       'pathlib.Path('+repr(reply)+').write_text(b); '
       'print("Q09_RPC_GZIP_META="+json.dumps({"length":len(b),'
       '"sha256":hashlib.sha256(b.encode()).hexdigest(),'
       '"chunk_size":'+str(size_per)+'},separators=(",",":"))); '
       'print("Q09_RPC_GZIP_CHUNK="+b[:'+str(size_per)+'])')
 script=('curl --fail --silent --show-error --insecure --max-time 340 '+
  '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
  '-H '+shlex.quote('Content-Type: application/json')+' '+
  '--data-binary '+shlex.quote(payload)+' '+
  'https://127.0.0.1:8443/q09/finish > '+shlex.quote(resp)+'\n'+
  py+' -c '+shlex.quote(body))
 output=q09_remote(cloud,script,'finish-'+plan)
 data=q09_collect_finish_response(cloud,output,reply)
 if (data.get('cloud'),data.get('plan_id'),data.get('trial_id'))!=(cloud,plan,trial):
  raise RuntimeError('Q09 wrong finish identity')
 explain=gzip.decompress(base64.b64decode(data.pop('explain_gzip_b64'),validate=True)).decode()
 if sha_bytes(explain.encode())!=data.pop('physical_plan_sha256'):
  raise RuntimeError('Q09 EXPLAIN integrity failure')
 data['physical_plan']=explain
 return data


def q09_trial(plan,trial,token,key,ips,cas):
 edges=[]
 for source,dest,part in Q09_PIPE[plan]:
  s=q09_stage(source,part,plan,trial,token,key)
  edges.append(q09_upload(source,dest,s,plan,trial,ips,cas,token,key))
 dest=Q09_DEST[plan]
 result=q09_finish(dest,plan,trial,token,key)
 return edges,result,dest




def q09_optimizer_decision(args):
    """Offline-only plan selection; a restricted physical-plan optimizer.

    Fits stage and residual latency medians from Q09's five numbered calibration
    trials per plan. Its objective is directional modeled HTTP body egress.
    Evaluation trials 6–10 are deliberately excluded. It does NOT infer new
    physical plans or measure cloud billable bytes.
    """
    if not math.isfinite(args.optimizer_delta) or not 0 <= args.optimizer_delta <= 100:
        raise ValueError('Invalid --optimizer-delta; finite value 0..100 required')
    archive=Path(args.optimizer_archive).expanduser()
    tariffs_path=Path(args.optimizer_tariffs).expanduser()
    tariffs=load_json(tariffs_path)
    if tariffs.get('classification') not in (
        'PUBLISHED_LIST_RATE_MARGINAL_SCENARIO_NOT_BILLED',
        'ILLUSTRATIVE_SCENARIO_NOT_VERIFIED_RATES') or \
       tariffs.get('tier_assumption')!='fixed_marginal_rates_no_free_tier':
        raise ValueError('Unsupported/unclassified tariff scenario')
    rates={}
    for item in tariffs['directed_rates']:
        edge=(item['source'].lower(),item['destination'].lower())
        rate=Decimal(str(item['usd_per_unit']))
        unit=int(item.get('unit_bytes',tariffs['unit_bytes']))
        if edge in rates or edge[0]==edge[1] or not rate.is_finite() or rate<0 or unit<=0:
            raise ValueError('Invalid/duplicate directed tariff')
        rates[edge]=(rate,unit)
    expected={(a,b) for a in ('aws','azure','gcp') for b in ('aws','azure','gcp') if a!=b}
    if set(rates)!=expected:raise ValueError('Exactly six directed tariffs are required')
    with zipfile.ZipFile(archive) as z:
        if z.testzip() is not None:raise ValueError('Source ZIP CRC failure')
        run='20261016T075336Z_737221d6'
        suffix='/'+run+'/run.json'
        prefixes=[name[:-len('run.json')] for name in z.namelist() if name.endswith(suffix)]
        if len(prefixes)!=1:raise ValueError('Locked Q09 SF1/P1 run not unique in archive')
        prefix=prefixes[0]
        run_bytes=z.read(prefix+'run.json')
        manifest=json.loads(run_bytes)
        if (manifest.get('query_id'),manifest.get('run_id'),manifest.get('status'),
                manifest.get('execution_mode'),manifest.get('sf'),manifest.get('placement')) != \
                ('Q09',run,'COMPLETED','real_multicloud',1,'P1'):
            raise ValueError('Q09 calibration run identity mismatch')
        csv_bytes=z.read(prefix+'trial_metrics.csv')
        if sha_bytes(csv_bytes)!=manifest.get('trial_metrics_sha256'):
            raise ValueError('Q09 calibration CSV SHA-256 mismatch')
        records=list(csv.DictReader(io.StringIO(csv_bytes.decode('utf-8-sig'))))
        if len(records)!=40:raise ValueError('Q09 expected exactly 40 archived measurements')
        by_plan={p:[] for p in PLANS};seen=set()
        for record in records:
            plan=record['plan_id'];trial=int(record['trial'])
            if (plan not in by_plan or not 1<=trial<=10 or
                (plan,trial) in seen or
                record.get('correct_vs_reference','').lower()!='true'):
                raise ValueError('Q09 invalid/duplicated calibration record')
            seen.add((plan,trial))
            if trial>5:continue
            item=json.loads(z.read(prefix+f'trials/{plan}/trial_{trial:03}.json'))
            edges=item['transfer_edges']
            if [(e['source_cloud'],e['destination_cloud'],e['label']) for e in edges]!=Q09_PIPE[plan]:
                raise ValueError('Q09 source edge graph changed')
            widths=[int(e['curl_size_upload']) for e in edges]
            if sum(widths)!=int(record['http_payload_bytes']):
                raise ValueError('Q09 source edge bytes inconsistent with CSV')
            duration=float(record['total_seconds']);stage=float(record['stage_seconds'])
            if not (math.isfinite(duration) and math.isfinite(stage) and 0<=stage<=duration and duration>0):
                raise ValueError('Q09 invalid source timing')
            by_plan[plan].append({'trial':trial,'duration':duration,
                                  'stage':stage,'edge_bytes':widths})
        if len(seen)!=40:raise ValueError('Missing Q09 source trial(s)')
    candidates=[]
    for plan in PLANS:
        rows=sorted(by_plan[plan],key=lambda x:x['trial'])
        if [r['trial'] for r in rows]!=[1,2,3,4,5]:
            raise ValueError('Incomplete Q09 calibration indices')
        stage=statistics.median(r['stage'] for r in rows)
        residual=statistics.median(r['duration']-r['stage'] for r in rows)
        cost=Decimal(0);edges=[]
        for j,(src,dest,label) in enumerate(Q09_PIPE[plan]):
            size=int(statistics.median(r['edge_bytes'][j] for r in rows))
            rate,unit=rates[(src,dest)]
            cost+=Decimal(size)*rate/Decimal(unit)
            edges.append({'source_cloud':src,'destination_cloud':dest,'label':label,
                          'predicted_http_body_bytes':size})
        candidates.append({'plan':plan,'predicted_latency_ms':1000*(stage+residual),
                           'predicted_http_bytes':sum(e['predicted_http_body_bytes'] for e in edges),
                           'predicted_cost_usd_proxy':str(cost),'predicted_edges':edges})
    fastest=min(candidates,key=lambda p:(p['predicted_latency_ms'],p['plan']))
    deadline_ms=fastest['predicted_latency_ms']*(1+args.optimizer_delta)
    feasible=[p for p in candidates if p['predicted_latency_ms']<=deadline_ms+1e-9]
    if not feasible:raise ValueError('No feasible Q09 plan')
    selected=min(feasible,key=lambda p:(Decimal(p['predicted_cost_usd_proxy']),
                                         p['predicted_latency_ms'],p['plan']))
    bytes_only=min(feasible,key=lambda p:(p['predicted_http_bytes'],
                                          p['predicted_latency_ms'],p['plan']))
    # This independent comparator is a fixed Azure coordinator, not an oracle.
    # If the optimizer chooses the fixed coordinator, use the predeclared AWS
    # coordinator. Neither choice consults evaluation/live performance outcomes.
    baseline='azure_join' if selected['plan']!='azure_join' else 'aws_join'
    return {
      'schema_version':1,'query':'Q09','optimizer_kind':'restricted_prevalidated_plan_selector',
      'selection_without_evaluation_trials':True,'calibration_trial_numbers':[1,2,3,4,5],
      'excluded_evaluation_trial_numbers':[6,7,8,9,10],
      'source_run_id':run,'source_run_json_sha256':sha_bytes(run_bytes),
      'source_trial_csv_sha256':sha_bytes(csv_bytes),
      'source_zip_sha256':sha_file(archive),'source_zip_filename':archive.name,
      'tariff_file_sha256':sha_file(tariffs_path),'tariff_scenario_id':tariffs['scenario_id'],
      'tariff_classification':tariffs['classification'],
      'selected_plan':selected['plan'],'bytes_only_plan':bytes_only['plan'],
      'fastest_plan':fastest['plan'],'baseline_plan':baseline,
      'baseline_definition':'fixed Azure coordinator; AWS coordinator fallback if selected is Azure',
      'predicted_slo_ms':deadline_ms,'optimizer_delta':args.optimizer_delta,
      'candidate_predictions':candidates,
      'model_description':'calibration-only median staging plus median nonstaging workflow residual',
      'cost_description':'six-directed-rate HTTP request-body monetary proxy, not invoiced charges',
      'prospective_claim':'selection recorded before cloud VM startup; the same SF1/P1 placement used for historical calibration'}


def q09_audit_cloud(directory):
    manifest = load_json(directory / 'run.json')
    if (manifest.get('query_id'), manifest.get('status'), manifest.get('execution_mode'),
            manifest.get('sf'), manifest.get('placement')) != (
            QUERY_ID, 'COMPLETED', 'real_multicloud', 1, PLACEMENT_ID):
        raise ValueError('Q09 is not a completed SF1/P1 real multi-cloud run')
    if sha_bytes(manifest['source_sql_text'].encode()) != manifest['source_sql_sha256']:
        raise ValueError('Q09 cloud archived SQL fingerprint invalid')
    if source_parameters(manifest['source_sql_text']) != manifest['parameters']:
        raise ValueError('Q09 cloud source SQL parameters changed')
    expected_sha = result_hash(manifest['reference_result'])
    if expected_sha != manifest['reference_sha256']:
        raise ValueError('Q09 cloud reference SHA-256 invalid')
    expected_edges = {p: [list(e) for e in PLAN_EDGES[p]] for p in PLANS}
    if manifest.get('full_edge_plan') != expected_edges:
        raise ValueError('Q09 cloud transfer graph differs from validated local plan')
    path = directory / 'trial_metrics.csv'
    if manifest.get('trial_metrics_sha256') and sha_file(path) != manifest['trial_metrics_sha256']:
        raise ValueError('Q09 cloud trial CSV hash mismatch')
    with path.open(newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    active=tuple(manifest.get('executed_plans',PLANS))
    if not active or len(set(active))!=len(active) or any(p not in PLANS for p in active):
        raise ValueError('Q09 invalid active plan list')
    if manifest.get('optimizer_decision') is not None:
        decision=manifest['optimizer_decision']
        if (manifest.get('experiment_subtype')!='prospective_optimizer_vs_fixed_coordinator' or
            decision.get('query')!='Q09' or
            tuple(active)!=tuple(dict.fromkeys((decision.get('selected_plan'),decision.get('baseline_plan')))) or
            len(active)!=2):
            raise ValueError('Q09 optimizer plan commitment inconsistent')
        sealed=directory/'optimizer_selection.json'
        if (not sealed.is_file() or sha_file(sealed)!=manifest.get('optimizer_selection_sha256') or
            load_json(sealed)!=decision):
            raise ValueError('Q09 optimizer decision seal mismatch')
    elif len(active)!=len(PLANS):
        raise ValueError('Q09 baseline benchmark must retain all plans')
    expected_total = len(active) * int(manifest['trials_per_plan'])
    if len(rows) != expected_total or expected_total != manifest['validated_trial_count']:
        raise ValueError('Q09 cloud measured trial count incomplete')
    seen = set()
    for item in rows:
        plan = item['plan_id']
        trial = int(item['trial'])
        if plan not in active or not 1 <= trial <= manifest['trials_per_plan']:
            raise ValueError('Invalid Q09 cloud plan/trial')
        if (plan, trial) in seen:
            raise ValueError('Duplicated Q09 cloud trial')
        seen.add((plan, trial))
        record = load_json(directory / 'trials' / plan / f'trial_{trial:03d}.json')
        if (record.get('run_id'), record.get('plan_id'), record.get('trial'),
                record.get('execution_mode'), record.get('joined_at')) != (
                manifest['run_id'], plan, trial, 'real_multicloud', Q09_DEST[plan]):
            raise ValueError('Q09 cloud trial identity incorrect')
        if item['execution_mode'] != 'real_multicloud':
            raise ValueError('Q09 local/cloud metrics labeling mismatch')
        edges = record['transfer_edges']
        if [(e['source_cloud'], e['destination_cloud'], e['label']) for e in edges] != PLAN_EDGES[plan]:
            raise ValueError('Q09 cloud transfer order or direction changed')
        for edge in edges:
            if not re.fullmatch(r'[a-f0-9]{64}', edge['sha256']):
                raise ValueError('Q09 cloud intermediate SHA-256 invalid')
            if edge.get('execution_mode') != 'real_multicloud':
                raise ValueError('Q09 mislabeled intermediate')
            if int(edge['parquet_bytes']) <= 0 or int(edge['rows']) < 0:
                raise ValueError('Q09 invalid intermediate byte/row count')
            if int(edge['curl_size_upload']) != int(edge['parquet_bytes']):
                raise ValueError('Q09 receiver data movement different from stage size')
            if not (0 <= float(edge['stage_seconds']) < 3600 and
                    0 <= float(edge['curl_http_total_seconds']) < 3600):
                raise ValueError('Q09 cloud invalid timing')
        total = sum(int(e['parquet_bytes']) for e in edges)
        payload = sum(int(e['curl_size_upload']) for e in edges)
        rows_total = sum(int(e['rows']) for e in edges)
        if (total != int(record['intermediate_parquet_bytes']) or
                total != int(item['intermediate_parquet_bytes']) or
                payload != int(record['http_payload_bytes']) or
                payload != int(item['http_payload_bytes']) or
                rows_total != int(record['intermediate_rows']) or
                rows_total != int(item['intermediate_rows']) or
                len(edges) != int(item['edge_count'])):
            raise ValueError('Q09 cloud transfer bytes or rows inconsistent')
        expected_time = (sum(float(e['stage_seconds']) + float(e['curl_http_total_seconds']) for e in edges)
                         + float(record['join_aggregate_seconds']))
        if (abs(expected_time - float(record['total_seconds'])) > 1e-9 or
                abs(expected_time - float(item['total_seconds'])) > 1e-9):
            raise ValueError('Q09 cloud duration components inconsistent')
        if (result_hash(record['result']) != expected_sha or
                record['result_sha256'] != expected_sha or item['result_sha256'] != expected_sha):
            raise ValueError('Q09 cloud SQL result inconsistent with archived reference')
        if not record['correct_vs_reference'] or item['correct_vs_reference'] not in ('True','true','1'):
            raise ValueError('Q09 cloud correctness flag invalid')
        if not record['network_bytes_measured'] or item['network_bytes_measured'] not in ('True','true','1'):
            raise ValueError('Q09 cloud HTTP bytes were not measured')
        if record['network_seconds_measured'] or item['network_seconds_measured'] not in ('False','false','0'):
            raise ValueError('Q09 falsely claims isolated network timing')
        plan_text=record.get('physical_plan')
        if not isinstance(plan_text,str) or not plan_text.strip():
            raise ValueError('Missing Q09 DuckDB EXPLAIN plan')
    return manifest, rows


def q09_audit_cloud_action(directory):
    manifest, rows = q09_audit_cloud(directory)
    dump_json(directory / 'q09_audit.json', {
        'run_id': manifest['run_id'], 'audited_utc': now_utc(),
        'status': 'PASS', 'execution_mode': 'real_multicloud',
        'measured_trials': len(rows), 'reference_sha256': manifest['reference_sha256'],
        'source_sql_sha256': manifest['source_sql_sha256'],
        'cleanup_incomplete': manifest.get('cleanup_incomplete'),
        'limitations': ['Audit is offline, not a rerun of cloud SQL.',
                        'HTTP request bodies are not provider billing amounts.',
                        'Curl duration includes receiver processing, not isolated network time.'],
    })
    print('Q09_EVIDENCE_AUDIT_PASS')
    print('Evidence: ' + str(directory))
    if manifest.get('cleanup_incomplete'):
        print('WARNING: Q09 cloud cleanup incomplete; check run.json and --mode cleanup')
    return 0


def q09_reproduce_cloud(directory, args):
    manifest, rows = q09_audit_cloud(directory)
    analysis = directory / 'q09_analysis'
    analysis.mkdir(parents=True, exist_ok=True)
    summary = []
    for plan in manifest.get('executed_plans',PLANS):
        subset = [r for r in rows if r['plan_id'] == plan]
        duration_ms = [float(r['total_seconds'])*1000 for r in subset]
        bytes_http = [int(r['http_payload_bytes']) for r in subset]
        summary.append({
            'plan_id': plan, 'trials': len(subset),
            'mean_workflow_ms': statistics.mean(duration_ms),
            'median_workflow_ms': statistics.median(duration_ms),
            'sample_sd_workflow_ms': statistics.stdev(duration_ms) if len(duration_ms)>1 else '',
            'mean_http_payload_bytes': statistics.mean(bytes_http),
            'min_http_payload_bytes': min(bytes_http),
            'max_http_payload_bytes': max(bytes_http),
            'transfer_edges': len(PLAN_EDGES[plan]), 'correct_trials': len(subset),
        })
    write_csv(analysis / 'plan_summary.csv', summary, tuple(summary[0]))
    dump_json(analysis / 'paper_summary.json', {
        'query_id':QUERY_ID, 'run_id':manifest['run_id'],
        'execution_mode':'real_multicloud', 'summary':summary,
        'limitations':['These are application-level HTTP payloads, not provider billed costs.',
                       'Duration includes staging, HTTPS and join; excludes VM startup.'],
    })
    lines = ['% Q09 SF1/P1 real multi-cloud candidate plans, warm-ups excluded',
             r'\begin{tabular}{lrrr}', r'\hline',
             r'Plan & Trials & Mean (ms) & Mean HTTP bytes \\', r'\hline']
    for item in summary:
        name = item['plan_id'].replace('_', r'\_')
        lines.append(f'{name} & {item["trials"]} & {item["mean_workflow_ms"]:.2f} & '
                     f'{item["mean_http_payload_bytes"]:.0f} ' + r'\\')
    lines += [r'\hline', r'\end{tabular}']
    (analysis / 'q09_cloud_table.tex').write_text('\n'.join(lines)+'\n', encoding='utf-8')
    if args.plots:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        labels = list(manifest.get('executed_plans', PLANS))
        xs = list(range(len(labels)))
        for plot, metric, ylabel in (
            ('duration','mean_workflow_ms','Mean stage + HTTPS + join (ms)'),
            ('payload','mean_http_payload_bytes','Mean request body bytes'),
        ):
            fig, ax = plt.subplots(figsize=(7.6,3.9))
            ax.bar(xs, [p[metric] for p in summary])
            ax.set_xticks(xs, labels, rotation=16, ha='right')
            ax.set_ylabel(ylabel)
            ax.set_title('Q09 SF1/P1: Real AWS–Azure–GCP plans')
            fig.tight_layout()
            for ext in ('png','pdf'):
                fig.savefig(analysis / f'q09_cloud_{plot}.{ext}', dpi=220)
            plt.close(fig)
    print('Q09_REPRODUCE_PASS')
    print('Reproduced REAL MULTICLOUD Q09 figures: ' + str(analysis))
    return 0


def q09_purge_deferred_runtimes(root):
 """Purge only explicitly recorded Q09 directories after all VMs have started.

 Previous interrupted runs remain FAILED. No sibling directory, data file,
 or shared deployed worker release is touched. Verify deletion remotely.
 Refuse to start a new Q09 experiment if a recorded purge fails.
 """
 summary=[]
 for manifest_path in sorted(Path(root).glob('*/run.json')):
  old=load_json(manifest_path)
  run_id=old.get('run_id')
  if (old.get('query_id')!='Q09' or old.get('placement')!='P1' or
      not isinstance(run_id,str) or not re.fullmatch(r'\d{8}T\d{6}Z_[0-9a-f]{8}',run_id) or
      manifest_path.parent.name!=run_id):
   continue
  pending=old.get('cleanup_deferred_runtime_purge',[])
  if not pending:continue
  if not isinstance(pending,list):
   raise RuntimeError('Malformed deferred-purge manifest: '+str(manifest_path))
  key=run_id.rsplit('_',1)[-1]
  completed=[]
  for entry in pending:
   if not isinstance(entry,dict):
    raise RuntimeError('Malformed deferred-purge entry: '+run_id)
   cloud=entry.get('cloud')
   if cloud not in ('aws','azure','gcp'):
    raise RuntimeError('Invalid deferred-purge provider in '+run_id)
   expected='/opt/egressopt/runtime/q09_'+key+'_'+cloud
   expected_unit='egressopt-q09-'+key+'-'+cloud
   if entry.get('runtime_directory')!=expected or entry.get('unit')!=expected_unit:
    raise RuntimeError('Deferred-purge path/unit mismatch: '+run_id+'/'+cloud)
   # No wildcard paths; no recursive cleanup of the parent directory.
   command=('set -eu; systemctl stop '+shlex.quote(expected_unit)+
            ' >/dev/null 2>&1 || true; rm -rf -- '+shlex.quote(expected)+
            '; test ! -e '+shlex.quote(expected)+'; echo Q09_DEFERRED_PURGE_VERIFIED')
   out=q09_remote(cloud,command,'deferred-purge-'+run_id)
   if 'Q09_DEFERRED_PURGE_VERIFIED' not in out:
    raise RuntimeError('Remote Q09 deferred purge not verified: '+run_id+'/'+cloud)
   completed.append(entry)
   # Persist a partial-success checkpoint to avoid repeating an already
   # verified deletion if the next provider has a transient failure.
   old['cleanup_deferred_runtime_purge']=[x for x in pending if x not in completed]
   old.setdefault('cleanup',{})[cloud+'_runtime']='deferred_files_removed_verified'
   old.setdefault('cleanup_purge_history',[]).append({
      'cloud':cloud,'runtime_directory':expected,'purged_utc':utcnow()})
   dump_json(manifest_path,old)
   summary.append({'run_id':run_id,'cloud':cloud,'runtime_directory':expected})
  if not old.get('cleanup_deferred_runtime_purge') and not old.get('cleanup_repair_unresolved'):
   old['cleanup_repair_status']='PASS_AFTER_DEFERRED_PURGE'
   old['cleanup_incomplete']=False
   dump_json(manifest_path,old)
 if summary:
  print('Q09 verified deferred runtime purges: '+', '.join(x['cloud'] for x in summary),flush=True)
 return summary


def q09_cloud(args):
 print('Q09_RUNNER_BUILD: '+Q09_RUNNER_BUILD,flush=True)
 if args.sf!=1:raise RuntimeError('Validated Q09 only SF1')
 # Fail before starting any paid VM if the Mac will sign stale AWS requests.
 q09_verify_aws_clock(force=True)
 import duckdb
 sql_path,paths=inputs(args)
 source_sql=sql_path.read_text(encoding='utf-8')
 params=source_parameters(source_sql)
 ref,raw=authoritative_answer(duckdb,paths,source_sql,args.threads)
 constraints={'note':'All Q09 local plans previously validated; cloud runner independently checks reference SQL and all VM dataset SHA-256 fingerprints'}
 if not ref:raise RuntimeError('Refusing empty Q09 reference')
 runkey=uuid.uuid4().hex[:8]
 run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+runkey
 directory=output_root(args)/run_id;directory.mkdir(parents=True,exist_ok=False)
 manifest={
  'schema_version':2,'runner_build':Q09_RUNNER_BUILD,'query_id':QUERY_ID,'run_id':run_id,'sf':args.sf,'placement':PLACEMENT_ID,
  'execution_mode':'real_multicloud','status':'RUNNING','created_utc':utcnow(),
  'source_sql':str(sql_path.resolve()),'source_sql_text':source_sql,'source_sql_sha256':sha_file(sql_path),
  'reference_result':ref,'reference_raw':raw,'reference_sha256':result_hash(ref),
  'parameters':params,'key_validation':constraints,
  'data_sources':{t:{'cloud':TABLE_LOCATION[t],'sha256':sha_file(p),'bytes':p.stat().st_size}
                  for t,p in paths.items()},
  'code_sha256':sha_file(Path(__file__)),'worker_sha256':sha_bytes(Q09_WORKER_SOURCE.encode()),
  'python_version':platform.python_version(),'duckdb_version':duckdb.__version__,
  'trials_per_plan':args.trials,'warmup_trials_per_plan':args.warmup_trials,
  'expected_edges':{k:[[a,b] for a,b,c in v] for k,v in Q09_PIPE.items()},
  'full_edge_plan':{k:[[a,b,c] for a,b,c in v] for k,v in Q09_PIPE.items()},
  'execution_order':[], 'cloud_datasets':{},'worker_releases':{},'assigned_public_ips':{},
  'gcp_external_ip_added_by_this_run':False,
  'verified_outbound_public_ips':{},'firewall_rules':{},'cleanup':{},
  'network_bytes_definition':'Exact HTTP request-body size, not provider-billed egress',
  'timing_definition':'sum(stage Parquet materialization on source + TLS curl POST including receiver processing + destination DuckDB join), excludes cloud management and VM startup',
  'network_time_isolated':False,'egress_usd_estimated':None,
  'executed_plans':list(getattr(args,'_q09_run_plans',PLANS)),
  'experiment_subtype':'prospective_optimizer_vs_fixed_coordinator' if getattr(args,'_q09_optimizer_decision',None) else 'four_plan_candidate_baseline',
  'limitations':['Actual provider billing was not measured','Restricted historical-profile plan selector, not generalized physical plan generation',
                 'Tokens may remain in authorized cloud-management history'],
 }
 if getattr(args,'_q09_optimizer_decision',None) is not None:
  # Write/hash the plan commitment before AWS/Azure/GCP VMs are inspected or started.
  manifest['optimizer_decision']=args._q09_optimizer_decision
  manifest['optimizer_committed_utc']=utcnow()
  dump_json(directory/'optimizer_selection.json',args._q09_optimizer_decision)
  manifest['optimizer_selection_sha256']=sha_file(directory/'optimizer_selection.json')
 dump_json(directory/'run.json',manifest)
 state={'started':{c:False for c in ('aws','azure','gcp')},
  'service':{c:False for c in ('aws','azure','gcp')},
  'rules':{'aws':[], 'azure':[], 'gcp':[]},'gcp_tag':None,'gcp_tag_added':False,
  'gcp_external_added':False}
 measured=[];warmups=[]
 try:
  for cloud in ('aws','azure','gcp'):
   print('Checking '+cloud.upper()+' VM...',flush=True)
   state['started'][cloud]=q09_start_vm(cloud)
   print('  '+cloud.upper()+' running',flush=True)
  # Remove only runtime paths explicitly recorded by prior QUIESCENT
  # recovery manifests before deploying a new authenticated Q09 worker.
  # Never purge unrecorded or cross-query directories.
  manifest['deferred_purges_before_run']=q09_purge_deferred_runtimes(output_root(args))
  dump_json(directory/'run.json',manifest)
  aws_inst=q09_aws_instance();gcp_inst=q09_gcp_instance()
  if aws_inst['State']['Name']!='running' or q09_azure_state()!='PowerState/running' or gcp_inst['status']!='RUNNING':
   raise RuntimeError('Q09 expected all three cloud VMs running')
  if not any(c.get('natIP') for nic in gcp_inst['networkInterfaces'] for c in nic.get('accessConfigs',[])):
   state['gcp_external_added']=True
   manifest['gcp_external_ip_added_by_this_run']=True
   dump_json(directory/'run.json',manifest)
   q09_gcloud('compute','instances','add-access-config',GCP_VM,'--zone='+GCP_ZONE,
              '--network-interface=nic0','--access-config-name=external-nat',
              '--quiet','--format=json',timeout=280)
   gcp_inst=q09_gcp_instance()
  ips={'aws':q09_ip(aws_inst['PublicIpAddress']),
   'azure':q09_ip(json.loads(q09_cli(['az','network','public-ip','show',
              '--resource-group',AZ_RG,'--name',AZ_PUBLIC,'--output','json']))['ipAddress']),
   'gcp':q09_gcp_public(gcp_inst)}
  manifest['assigned_public_ips']=ips
  outbound={c:q09_outbound(c) for c in ('aws','azure','gcp')}
  manifest['verified_outbound_public_ips']=outbound
  aws_sg=[x['GroupId'] for x in aws_inst['SecurityGroups']]
  if len(aws_sg)!=1:raise RuntimeError('Q09 expected one AWS security group')
  manifest['cloud_hardware']={
   'aws':{'region':AWS_REGION,'instance_type':aws_inst['InstanceType'],'instance_id':AWS_INSTANCE},
   'azure':{'resource_group':AZ_RG,'vm_name':AZ_VM},
   'gcp':{'zone':GCP_ZONE,'machine_type':gcp_inst['machineType'].rsplit('/',1)[-1]}}
  for cloud in ('aws','azure','gcp'):
   names=Q09_CLOUD_TABLES[cloud]
   output=q09_remote(cloud,'\n'.join('sha256sum /opt/egressopt/data/tpch_sf1/'+n+'.parquet' for n in names),
     'dataset-hashes')
   found={}
   for line in output.splitlines():
    match=re.search(r'([a-f0-9]{64})\s+(/opt/egressopt/data/tpch_sf1/([a-z]+)\.parquet)',line)
    if match:found[match.group(3)]=match.group(1)
   if set(found)!=set(names):raise RuntimeError('Q09 remote dataset hash list invalid on '+cloud)
   for name in names:
    if found[name]!=manifest['data_sources'][name]['sha256']:
     raise RuntimeError('Q09 remote dataset mismatch on '+cloud+'/'+name)
   manifest['cloud_datasets'][cloud]=found
   manifest['worker_releases'][cloud]=q09_install_worker(cloud,runkey)
   dump_json(directory/'run.json',manifest)
  token=secrets.token_urlsafe(48);cas={}
  for cloud in ('aws','azure','gcp'):
   state['service'][cloud]=True
   cas[cloud]=q09_start_service(cloud,runkey,manifest['worker_releases'][cloud],ips[cloud],token,params)
   print('  '+cloud.upper()+' authenticated Q09 TLS worker ready',flush=True)
  # Allow only real, directed source→destination edges; no internet-wide rules.
  for src in ('azure',):
   rule=q09_aws_rule(outbound[src],src,runkey,aws_sg[0]);state['rules']['aws'].append(rule)
   manifest['firewall_rules']['aws_from_'+src]=rule
   dump_json(directory/'run.json',manifest)
  for src in ('aws','gcp'):
   rule=q09_azure_rule(src,outbound[src],runkey);state['rules']['azure'].append(rule)
   manifest['firewall_rules']['azure_from_'+src]=rule
   dump_json(directory/'run.json',manifest)
  network=gcp_inst['networkInterfaces'][0]['network'].rsplit('/',1)[-1]
  # Record the shared GCP tag before attempting either rule so a partial
  # add-tags/create failure is recoverable by the finally cleanup path.
  state['gcp_tag']=f'egressopt-q09-{runkey}'
  state['gcp_tag_added']=True
  for src in ('aws','azure'):
   rule,tag=q09_gcp_rule(outbound[src],runkey,network,src)
   state['rules']['gcp'].append(rule);state['gcp_tag']=tag;state['gcp_tag_added']=True
   manifest['firewall_rules']['gcp_from_'+src]=rule
   dump_json(directory/'run.json',manifest)
  dump_json(directory/'run.json',manifest)
  print('  Three-cloud source-restricted TLS ingress active',flush=True)
  for idx in range(args.warmup_trials+args.trials):
   warm=idx<args.warmup_trials
   count=idx+1 if warm else idx-args.warmup_trials+1
   trial_id=('w' if warm else 'm')+f'{count:02d}'
   active=tuple(manifest['executed_plans'])
   rotate=idx%len(active);order=active[rotate:]+active[:rotate]
   manifest['execution_order'].append({'round':idx+1,'trial_id':trial_id,'warmup':warm,'plans':order})
   for plan in order:
    edges,finished,dest=q09_trial(plan,trial_id,token,runkey,ips,cas)
    if finished['result']!=ref or finished['result_sha256']!=manifest['reference_sha256']:
     raise RuntimeError('Q09 cloud SQL mismatch: '+plan+'/'+trial_id)
    stage_sec=sum(float(e['stage_seconds']) for e in edges)
    curl_sec=sum(float(e['curl_http_total_seconds']) for e in edges)
    join_sec=float(finished['join_seconds'])
    payload=sum(int(e['curl_size_upload']) for e in edges)
    rec={'query_id':QUERY_ID,'run_id':run_id,'plan_id':plan,'trial':count,
      'execution_mode':'real_multicloud','scale_factor':1,'placement_id':PLACEMENT_ID,
      'joined_at':dest,'transfer_edges':edges,
      'optimizer_role':('selected_egressopt' if plan==manifest.get('optimizer_decision',{}).get('selected_plan')
                        else 'predeclared_fixed_coordinator' if manifest.get('optimizer_decision') else 'candidate_baseline'),
      'intermediate_parquet_bytes':sum(e['parquet_bytes'] for e in edges),
      'http_payload_bytes':payload,'intermediate_rows':sum(e['rows'] for e in edges),
      'stage_seconds':stage_sec,'join_aggregate_seconds':join_sec,
      'curl_http_total_seconds':curl_sec,'total_seconds':stage_sec+curl_sec+join_sec,
      'result':finished['result'],'result_sha256':finished['result_sha256'],
      'raw_result':finished.get('raw_result_strings'),
      'physical_plan':finished['physical_plan'],'correct_vs_reference':True,
      'network_bytes_measured':True,'network_seconds_measured':False,
      'egress_usd_estimated':None,'timestamp_utc':utcnow()}
    plan_dir=directory/'trials'/plan;plan_dir.mkdir(parents=True,exist_ok=True)
    if warm:
     warmups.append({'plan_id':plan,'warmup':count,'total_seconds':rec['total_seconds'],
       'http_payload_bytes':payload,'correct_vs_reference':True})
     dump_json(plan_dir/f'warmup_{count:03d}.json',rec)
     write_csv(directory/'warmup_metrics.csv',warmups,tuple(warmups[0]))
    else:
     dump_json(plan_dir/f'trial_{count:03d}.json',rec)
     measured.append({**summary_row(run_id,rec),
                     'network_bytes_measured':True, 'http_payload_bytes':payload})
     write_csv(directory/'trial_metrics.csv',measured,METRIC_FIELDS+('http_payload_bytes',))
    typ=f'WARMUP={count}' if warm else f'trial={count}'
    print(f'{plan:<20} {typ} correctness=PASS real_HTTP_bytes={payload:,} '
       f'edges={len(edges)} stage+HTTPS+join={rec["total_seconds"]:.3f}s',flush=True)
  manifest['status']='COMPLETED';manifest['completed_utc']=utcnow()
  manifest['validated_trial_count']=len(measured)
  manifest['warmup_count']=len(warmups)
  manifest['trial_metrics_sha256']=sha_file(directory/'trial_metrics.csv')
  dump_json(directory/'run.json',manifest)
  if manifest.get('optimizer_decision'):
   print('Q09_EGRESSOPT_PROSPECTIVE_CLOUD_PASS')
  else:
   print('Q09_REAL_THREE_CLOUD_PASS')
  print('Q09 cloud run: '+str(directory))
  return 0
 except Exception as exc:
  manifest['status']='FAILED';manifest['failure']=type(exc).__name__+': '+str(exc)
  manifest['completed_utc']=utcnow()
  raise
 finally:
  for cloud in ('aws','azure','gcp'):
   if state['service'][cloud]:
    try:
     q09_remote(cloud,'systemctl stop egressopt-q09-'+runkey+'-'+cloud+' >/dev/null 2>&1 || true; '
       'rm -rf /opt/egressopt/runtime/q09_'+runkey+'_'+cloud+'; echo Q09_RUNTIME_REMOVED',
       'stop-runtime')
     manifest['cleanup'][cloud+'_runtime']='removed'
    except Exception as e:manifest['cleanup'][cloud+'_runtime']='FAILED: '+str(e)
  for rid in state['rules']['aws']:
   try:q09_aws('ec2','revoke-security-group-ingress','--group-id',aws_sg[0],
               '--security-group-rule-ids',rid)
   except Exception as e:manifest['cleanup']['aws_rule_'+rid]='FAILED: '+str(e)
   else:manifest['cleanup']['aws_rule_'+rid]='removed'
  for name in state['rules']['azure']:
   try:q09_cli(['az','network','nsg','rule','delete','--resource-group',AZ_RG,
                 '--nsg-name',AZ_NSG,'--name',name,'--output','none'])
   except Exception as e:manifest['cleanup']['azure_rule_'+name]='FAILED: '+str(e)
   else:manifest['cleanup']['azure_rule_'+name]='removed'
  for name in state['rules']['gcp']:
   try:q09_gcloud('compute','firewall-rules','delete',name,'--quiet')
   except Exception as e:manifest['cleanup']['gcp_rule_'+name]='FAILED: '+str(e)
   else:manifest['cleanup']['gcp_rule_'+name]='removed'
  if state['gcp_tag_added']:
   try:q09_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
              '--tags='+state['gcp_tag'],'--quiet','--format=json')
   except Exception as e:manifest['cleanup']['gcp_tag']='FAILED: '+str(e)
   else:manifest['cleanup']['gcp_tag']='removed'
  if state['gcp_external_added']:
   try:q09_gcloud('compute','instances','delete-access-config',GCP_VM,'--zone='+GCP_ZONE,
          '--network-interface=nic0','--access-config-name=external-nat',
          '--quiet','--format=json')
   except Exception as e:manifest['cleanup']['gcp_external_ip']='FAILED: '+str(e)
   else:manifest['cleanup']['gcp_external_ip']='removed'
  for cloud in ('aws','azure','gcp'):
   if not (args.stop_all_after_run or state['started'][cloud]):continue
   try:
    if cloud=='aws':
     current=q09_aws_instance()['State']['Name']
     if current not in {'stopped','stopping'}:
      q09_aws('ec2','stop-instances','--instance-ids',AWS_INSTANCE)
     q09_cli(['aws','ec2','wait','instance-stopped','--instance-ids',AWS_INSTANCE,
       '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=670)
     if q09_aws_instance()['State']['Name']!='stopped':raise RuntimeError('AWS stop not verified')
     result='stopped_verified'
    elif cloud=='azure':
     if q09_azure_state()!='PowerState/deallocated':
      q09_cli(['az','vm','deallocate','--resource-group',AZ_RG,'--name',AZ_VM,
          '--output','none'],timeout=690)
     if q09_azure_state()!='PowerState/deallocated':raise RuntimeError('Azure deallocation not verified')
     result='deallocated_verified'
    else:
     if q09_gcp_instance()['status']!='TERMINATED':
      q09_gcloud('compute','instances','stop',GCP_VM,'--zone='+GCP_ZONE,
             '--quiet','--format=json',timeout=690)
     if q09_gcp_instance()['status']!='TERMINATED':raise RuntimeError('GCP stop not verified')
     result='stopped_verified'
    manifest['cleanup'][cloud+'_vm']=result
   except Exception as e:manifest['cleanup'][cloud+'_vm']='FAILED: '+str(e)
  manifest['cleanup_incomplete']=any(str(x).startswith('FAILED') for x in manifest['cleanup'].values())
  dump_json(directory/'run.json',manifest)
  if manifest['cleanup_incomplete']:
   print('WARNING: Q09 cleanup incomplete; inspect run.json',file=sys.stderr)
  print('Q09 experiment report: '+str(directory),flush=True)




def q09_cleanup_failed_run(args):
 """Retry cleanup from a failed run manifest, using only its recorded IDs.

 This does NOT change status=FAILED or turn partial trials into an experiment.
 It never starts VMs, creates network rules, or guesses an unrecorded IP.
 """
 if not args.run_id or not re.fullmatch(r'\d{8}T\d{6}Z_[0-9a-f]{8}',args.run_id):
  raise ValueError('Supply the exact failed Q09 --run-id YYYYMMDDTHHMMSSZ_hex8')
 directory=output_root(args)/args.run_id
 manifest=load_json(directory/'run.json')
 if (manifest.get('query_id'),manifest.get('run_id'),manifest.get('placement'))!=('Q09',args.run_id,'P1'):
  raise ValueError('Q09 run identity mismatch; refusing cleanup')
 if manifest.get('execution_mode')!='real_multicloud' or manifest.get('status') not in ('FAILED','COMPLETED'):
  raise ValueError('Cleanup requires an existing completed/failed real-cloud run')
 key=args.run_id.rsplit('_',1)[-1]
 cleanup=manifest.setdefault('cleanup',{})
 cleanout={}
 def call(label,func):
  try:
   detail=func()
   cleanout[label]='removed' if detail is None else str(detail)
  except Exception as exc:
   cleanout[label]='FAILED: '+type(exc).__name__+': '+str(exc)
   print('  Cleanup warning '+label+': '+str(exc)[-250:],flush=True)

 # Remove per-run TLS workers when access is available; leave unrelated
 # services untouched. If an instance was stopped during the failed run,
 # do not restart it simply to erase ephemeral files.
 for cloud in ('aws','azure','gcp'):
  def remove_runtime(cloud=cloud):
   status=({
    'aws':lambda:q09_aws_instance()['State']['Name'],
    'azure':q09_azure_state,
    'gcp':lambda:q09_gcp_instance()['status'],
   }[cloud])()
   if status not in ('running','PowerState/running','RUNNING'):
    return 'vm_not_running; runtime deletion not independently confirmed'
   script=('systemctl stop egressopt-q09-'+key+'-'+cloud+
     ' >/dev/null 2>&1 || true; rm -rf /opt/egressopt/runtime/q09_'+key+'_'+cloud+
     '; echo Q09_RUNTIME_REMOVED')
   output=q09_remote(cloud,script,'recovery-stop-runtime')
   if 'Q09_RUNTIME_REMOVED' not in output:
    raise RuntimeError('Q09 cleanup confirmation missing')
   return 'removed'
  call(cloud+'_runtime',remove_runtime)

 # Delete only resource identifiers present in the archived manifest.
 fw=manifest.get('firewall_rules',{})
 if not isinstance(fw,dict):raise ValueError('Malformed Q09 firewall manifest')
 aws_rules=[v for k,v in fw.items() if k=='aws_from_azure']
 for rid in aws_rules:
  if not re.fullmatch(r'sgr-[0-9a-f]+',str(rid)):
   cleanout['aws_rule_'+str(rid)]='FAILED: invalid recorded AWS rule ID';continue
  def remove_aws(rid=rid):
   instance=q09_aws_instance()
   sgs=[r['GroupId'] for r in instance['SecurityGroups']]
   if len(sgs)!=1:raise RuntimeError('Expected one AWS security group')
   try:q09_aws('ec2','revoke-security-group-ingress','--group-id',sgs[0],
        '--security-group-rule-ids',rid)
   except RuntimeError as exc:
    if 'InvalidSecurityGroupRuleId.NotFound' not in str(exc):raise
   return 'removed_or_absent'
  call('aws_rule_'+rid,remove_aws)
 for origin in ('aws','gcp'):
  name=fw.get('azure_from_'+origin)
  if not name:continue
  if name!=f'egressopt-q09-{key}-{origin}':
   cleanout['azure_rule_'+str(name)]='FAILED: foreign rule ID';continue
  def remove_az(name=name):
   existing=json.loads(q09_cli(['az','network','nsg','rule','list',
      '--resource-group',AZ_RG,'--nsg-name',AZ_NSG,'--output','json']))
   if any(r.get('name')==name for r in existing):
    q09_cli(['az','network','nsg','rule','delete','--resource-group',AZ_RG,
        '--nsg-name',AZ_NSG,'--name',name,'--output','none'])
   return 'removed_or_absent'
  call('azure_rule_'+name,remove_az)
 for origin in ('aws','azure'):
  if 'gcp_from_'+origin not in fw:continue
  name=fw['gcp_from_'+origin]
  expected='egressopt-q09-'+key+'-'+origin
  if name==expected:
   def remove_gcp():
    try:q09_gcloud('compute','firewall-rules','describe',name,'--format=json')
    except RuntimeError as exc:
     if 'not found' not in str(exc).lower() and 'notfound' not in str(exc).lower():raise
     return 'removed_or_absent'
    q09_gcloud('compute','firewall-rules','delete',name,'--quiet')
    return 'removed_or_absent'
   call('gcp_rule_'+name,remove_gcp)
   def remove_gcp_tag():
    inst=q09_gcp_instance()
    if 'egressopt-q09-'+key in inst.get('tags',{}).get('items',[]):
     q09_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
       '--tags='+'egressopt-q09-'+key,'--quiet','--format=json')
    return 'removed_or_absent'
   call('gcp_tag',remove_gcp_tag)
  else:cleanout['gcp_rule_'+str(name)]='FAILED: foreign rule ID'
 if manifest.get('gcp_external_ip_added_by_this_run') is True:
  def remove_dynamic_gcp_nat():
   inst=q09_gcp_instance()
   configs=[c for nic in inst['networkInterfaces'] for c in nic.get('accessConfigs',[])]
   if any(c.get('name')=='external-nat' for c in configs):
    q09_gcloud('compute','instances','delete-access-config',GCP_VM,
      '--zone='+GCP_ZONE,'--network-interface=nic0',
      '--access-config-name=external-nat','--quiet','--format=json')
   return 'removed_or_absent'
  call('gcp_external_ip',remove_dynamic_gcp_nat)
 elif 'gcp_external_ip_added_by_this_run' not in manifest:
  # The older runner did not record whether it assigned the external IP.
  # Verify absence without deleting a potentially pre-existing shared address.
  def unknown_external_ip():
   inst=q09_gcp_instance()
   configs=[c for nic in inst['networkInterfaces'] for c in nic.get('accessConfigs',[])]
   if not any(c.get('natIP') for c in configs):return 'absent_verified'
   return 'origin_not_recorded_in_old_runner; inspect manually'
  call('gcp_external_ip',unknown_external_ip)

 if args.stop_all_after_run:
  for cloud in ('aws','azure','gcp'):
   def stop(cloud=cloud):
    if cloud=='aws':
     if q09_aws_instance()['State']['Name']!='stopped':
      q09_aws('ec2','stop-instances','--instance-ids',AWS_INSTANCE)
     q09_cli(['aws','ec2','wait','instance-stopped','--instance-ids',AWS_INSTANCE,
         '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=670)
     if q09_aws_instance()['State']['Name']!='stopped':raise RuntimeError('AWS not stopped')
     return 'stopped_verified'
    if cloud=='azure':
     if q09_azure_state()!='PowerState/deallocated':
      q09_cli(['az','vm','deallocate','--resource-group',AZ_RG,'--name',AZ_VM,
        '--output','none'],timeout=690)
     if q09_azure_state()!='PowerState/deallocated':raise RuntimeError('Azure not deallocated')
     return 'deallocated_verified'
    if q09_gcp_instance()['status']!='TERMINATED':
     q09_gcloud('compute','instances','stop',GCP_VM,'--zone='+GCP_ZONE,
       '--quiet','--format=json',timeout=690)
    if q09_gcp_instance()['status']!='TERMINATED':raise RuntimeError('GCP not stopped')
    return 'stopped_verified'
   call(cloud+'_vm',stop)
 # A stopped/deallocated VM cannot host a running Q09 TLS listener. The
 # previous repair code counted the inaccessible filesystem as a live cleanup
 # failure. Distinguish network/service quiescence from secure file deletion:
 # do NOT start a stopped VM merely to remove one run's runtime directory.
 # Require a fresh provider power-state verification for this exception.
 pending_disk_purge=[]
 for cloud in ('aws','azure','gcp'):
  runtime_label=cloud+'_runtime'
  report=str(cleanout.get(runtime_label,''))
  if not (report.startswith('FAILED') or 'not independently confirmed' in report):
   continue
  try:
   actual=({'aws':lambda:q09_aws_instance()['State']['Name'],
     'azure':q09_azure_state,
     'gcp':lambda:q09_gcp_instance()['status']}[cloud])()
   off=({'aws':{'stopped'},
         'azure':{'PowerState/deallocated'},
         'gcp':{'TERMINATED'}}[cloud])
   if actual in off:
    cleanout[runtime_label]='vm_power_off_verified; runtime_files_not_deleted'
    pending_disk_purge.append({
      'cloud':cloud,'power_state':actual,
      'runtime_directory':'/opt/egressopt/runtime/q09_'+key+'_'+cloud,
      'unit':'egressopt-q09-'+key+'-'+cloud,
      'note':'Files were NOT deleted. Purge only this run directory when the VM is next running, before any new Q09 worker starts.'})
   else:
    # If the VM is running (or transitional) and the remote deletion failed,
    # there may still be a live worker: preserve FAILED and block approval.
    print('  Q09 cleanup '+cloud+' still not verified; VM state='+str(actual),flush=True)
  except Exception as exc:
   print('  Q09 cannot verify '+cloud+' runtime safety: '+str(exc)[-230:],flush=True)

 cleanup.update(cleanout)
 manifest['cleanup_repair_utc']=utcnow()
 unresolved=[k for k,v in cleanout.items() if v.startswith('FAILED') or
    'not independently confirmed' in v or v.startswith('origin_not_recorded')]
 manifest['cleanup_incomplete']=bool(unresolved)
 manifest['cleanup_repair_unresolved']=unresolved
 manifest['cleanup_deferred_runtime_purge']=pending_disk_purge
 manifest['cleanup_repair_status']=('INCOMPLETE' if unresolved else
   'QUIESCENT_WITH_RESIDUAL_FILES' if pending_disk_purge else 'PASS')
 dump_json(directory/'run.json',manifest)
 print('Q09_CLEANUP_REPAIR_'+('INCOMPLETE' if unresolved else
   'QUIESCENT' if pending_disk_purge else 'PASS'))
 print('Q09 cleanup manifest: '+str(directory/'run.json'))
 if unresolved:print('Remaining issues: '+', '.join(unresolved))
 if pending_disk_purge:
  print('Verified power-off; Q09 TLS workers cannot run on the stopped VMs.')
  print('DEFERRED FILE PURGE (not falsely reported deleted): '+
        ', '.join(x['cloud'] for x in pending_disk_purge))
  print('Only recorded per-run runtime directories remain to be deleted when VMs are next running.')
 print('Existing partial Q09 trials remain FAILED and excluded from summaries.')
 return 1 if unresolved else 0




if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'Q09 ERROR: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
