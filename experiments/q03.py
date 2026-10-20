#!/usr/bin/env python3
"""EgressOpt Q03 — standalone Shipping Priority experiment (SF1/P1).

Interface (the same names used by other independently maintained query files):
    python experiments/q03.py --mode cloud --trials 10
    python experiments/q03.py --mode reproduce
    python experiments/q03.py --mode audit

First-time Q03 prerequisite (local correctness/plan-shape gate, no cloud charges):
    python experiments/q03.py --mode prepare

CURRENT RELEASE: prepare, cloud, reproduce and audit are implemented. The
Q03 cloud worker is embedded and securely deployed from THIS FILE. New cloud
execution is not yet verified on the user's live VMs. Local times are never
reported as cloud timings.

No imports from q12.py or other project Python files. Requires duckdb in local
prepare mode, and matplotlib for optional figures. All data remain in the
user's own EgressOpt project. The actual queries/tpch/q03.sql is authoritative.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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

QUERY = 'Q03'
PLACEMENT = 'P1'
DEFAULT_SF = int(os.environ.get('EGRESSOPT_SF', '1'))
VALIDATED_SCALE_FACTORS = (1,)
PLANS = ('aws_join', 'azure_join', 'semijoin_azure')
TABLE_PLACEMENT = {'customer': 'azure', 'orders': 'aws', 'lineitem': 'azure'}
FIELDS = ('run_id', 'query_id', 'scale_factor', 'placement_id', 'execution_mode',
          'plan_id', 'trial', 'join_cloud', 'source_clouds', 'transfer_edges',
          'intermediate_rows', 'intermediate_parquet_bytes', 'stage_seconds',
          'join_aggregate_seconds', 'total_seconds', 'result_row_count',
          'result_sha256', 'correct_vs_reference', 'network_bytes_measured',
          'network_seconds_measured', 'egress_usd_estimated', 'timestamp_utc')
REVENUE_PLACES = Decimal('0.0001')


def now():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def literal(value: Path) -> str:
    return "'" + str(value.resolve()).replace("'", "''") + "'"


def string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def canonical(rows) -> list[dict]:
    """Normalize DuckDB DECIMAL, DATE and numeric outputs exactly for Q03."""
    answer = []
    for r in rows:
        if len(r) != 4:
            raise ValueError('Q03 result must have four columns')
        revenue = Decimal(str(r[1]))
        if not revenue.is_finite():
            raise ValueError('Non-finite Q03 revenue')
        answer.append({'l_orderkey': int(r[0]),
                       'revenue': format(revenue.quantize(REVENUE_PLACES), '.4f'),
                       'o_orderdate': str(r[2])[:10],
                       'o_shippriority': int(r[3])})
    # Sort for equivalence checking, not to change SQL TOP 10 membership.
    return sorted(answer, key=lambda r: (r['l_orderkey'], r['o_orderdate'], r['o_shippriority']))


def canonical_hash(rows) -> str:
    return digest_bytes(json.dumps(rows, separators=(',', ':'), sort_keys=True).encode())


def load_params(sql: str) -> dict:
    """Fail closed if the parameterized SQL isn't the supported Q03 shape.

    Accept DATE 'YYYY-MM-DD', CAST('YYYY-MM-DD' AS DATE), and quoted ISO dates
    for the two predicates. Do not silently substitute textbook parameters.
    """
    source = re.sub(r'--[^\n]*', '', sql)
    source = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    segment = re.search(r"\bc_mktsegment\s*=\s*'([A-Za-z]+)'", source, re.I)
    date_expression = (r"(?:DATE\s*'(?P<date1>\d{4}-\d{2}-\d{2})'|"
                       r"CAST\s*\(\s*'(?P<date2>\d{4}-\d{2}-\d{2})'\s+AS\s+DATE\s*\)|"
                       r"'(?P<date3>\d{4}-\d{2}-\d{2})'(?:\s*::\s*DATE)?)")
    order_date = re.search(r'\bo_orderdate\s*<\s*' + date_expression, source, re.I)
    ship_date = re.search(r'\bl_shipdate\s*>\s*' + date_expression, source, re.I)
    if not (segment and order_date and ship_date):
        raise ValueError('Q03 SQL predicates could not be verified: expected customer segment, order date < and ship date >. Show queries/tpch/q03.sql before running cloud.')
    od = next(v for v in order_date.groups() if v)
    sd = next(v for v in ship_date.groups() if v)
    if od != sd:
        raise ValueError('This Q03 prototype requires identical order and ship cutoff dates')
    # Strict semantic gate: forbid stealth changes to joins/aggregation/ranking.
    required_tokens = ['c_custkey', 'o_custkey', 'l_orderkey', 'o_orderkey',
                       'l_extendedprice', 'l_discount', 'o_shippriority',
                       'group by', 'order by', 'limit 10']
    compressed = re.sub(r'\s+', ' ', source).lower()
    if any(term not in compressed for term in required_tokens):
        raise ValueError('Unsupported Q03 SQL shape; missing required join, aggregate or top-10 element')
    if not (re.search(r'\bc_custkey\s*=\s*o_custkey\b|\bo_custkey\s*=\s*c_custkey\b', source, re.I)
            and re.search(r'\bl_orderkey\s*=\s*o_orderkey\b|\bo_orderkey\s*=\s*l_orderkey\b', source, re.I)):
        raise ValueError('Q03 SQL must include customer↔orders and orders↔lineitem equijoins')
    return {'segment': segment.group(1), 'cutoff': od}


def reference_rows(con, paths, sql: str) -> list[dict]:
    for table in ('customer', 'orders', 'lineitem'):
        con.execute(f'CREATE VIEW {table} AS SELECT * FROM read_parquet({literal(paths[table])})')
    try:
        return canonical(con.execute(sql).fetchall())
    finally:
        for table in ('customer', 'orders', 'lineitem'):
            con.execute(f'DROP VIEW {table}')


def filtered_specs(paths, params):
    c = literal(paths['customer']); o = literal(paths['orders']); l = literal(paths['lineitem'])
    seg = string_literal(params['segment']); date = string_literal(params['cutoff'])
    return {
        'customer_keys': f'SELECT c_custkey FROM read_parquet({c}) WHERE c_mktsegment = {seg}',
        'customer_selected': f'SELECT c_custkey FROM read_parquet({c}) WHERE c_mktsegment = {seg}',
        'lineitem_selected': f'SELECT l_orderkey,l_extendedprice,l_discount FROM read_parquet({l}) WHERE l_shipdate > CAST({date} AS DATE)',
        'orders_selected': f'SELECT o_orderkey,o_custkey,o_orderdate,o_shippriority FROM read_parquet({o}) WHERE o_orderdate < CAST({date} AS DATE)',
    }


def finish_sql(customer: str, orders: str, lineitem: str, *, skip_customer: bool = False) -> str:
    """One semantic aggregate; the choice of precomputed inputs changes plan shape."""
    if skip_customer:
        joins = f'FROM ({orders}) AS o JOIN ({lineitem}) AS l ON o.o_orderkey=l.l_orderkey'
    else:
        joins = (f'FROM ({customer}) AS c JOIN ({orders}) AS o ON c.c_custkey=o.o_custkey '
                 f'JOIN ({lineitem}) AS l ON l.l_orderkey=o.o_orderkey')
    return (f'SELECT l.l_orderkey, SUM(l.l_extendedprice * (1-l.l_discount)) AS revenue, '
            f'o.o_orderdate, o.o_shippriority {joins} '
            'GROUP BY l.l_orderkey,o.o_orderdate,o.o_shippriority '
            'ORDER BY revenue DESC,o.o_orderdate LIMIT 10')


def local_plan(duckdb, paths, params, plan: str, threads: int) -> dict:
    """Materialize cloud-shaped exchange edges using local Parquet only."""
    con = duckdb.connect(database=':memory:')
    con.execute(f'SET threads={threads}')
    specs = filtered_specs(paths, params)
    c = specs['customer_selected']; o = specs['orders_selected']; l = specs['lineitem_selected']
    start_all = time.perf_counter()
    edges = []
    stage_elapsed = 0.0
    try:
        with tempfile.TemporaryDirectory(prefix='egressopt_q03_') as temp:
            tempdir = Path(temp)

            def stage(fragment: str, name: str, source: str, dest: str):
                nonlocal stage_elapsed
                out = tempdir / f'{name}.parquet'
                tick = time.perf_counter()
                con.execute(f'COPY ({fragment}) TO {literal(out)} (FORMAT PARQUET, COMPRESSION ZSTD)')
                elapsed = time.perf_counter() - tick
                stage_elapsed += elapsed
                count = con.execute(f'SELECT COUNT(*) FROM read_parquet({literal(out)})').fetchone()[0]
                edges.append({'name': name, 'source_cloud': source, 'destination_cloud': dest,
                              'rows': int(count), 'parquet_bytes': out.stat().st_size,
                              'stage_seconds': elapsed, 'sha256': sha256(out)})
                return f'read_parquet({literal(out)})'

            if plan == 'aws_join':
                # Two projections originate on Azure; joined with filtered AWS orders.
                c = f'SELECT * FROM {stage(c, "customer", "azure", "aws")}'
                l = f'SELECT * FROM {stage(l, "lineitem", "azure", "aws")}'
                destination = 'aws'
                query = finish_sql(c, o, l)
            elif plan == 'azure_join':
                o = f'SELECT * FROM {stage(o, "orders", "aws", "azure")}'
                destination = 'azure'
                query = finish_sql(c, o, l)
            elif plan == 'semijoin_azure':
                # Azure→AWS: selected customer keys, then AWS→Azure: qualifying orders.
                key_scan = stage(specs['customer_keys'], 'customer_keys', 'azure', 'aws')
                reduced = (f'SELECT o.* FROM ({o}) AS o SEMI JOIN '
                           f'{key_scan} AS keys ON o.o_custkey=keys.c_custkey')
                o = f'SELECT * FROM {stage(reduced, "orders_semijoin", "aws", "azure")}'
                destination = 'azure'
                query = finish_sql('', o, l, skip_customer=True)
            else:
                raise ValueError(f'Unknown Q03 plan: {plan}')
            # Capture DuckDB's optimized plan: SQL join order alone cannot prove
            # physical join order, which may change under optimizer rewrites.
            physical_plan = "\n".join(str(row[-1]) for row in con.execute('EXPLAIN ' + query).fetchall())
            started_join = time.perf_counter()
            result = canonical(con.execute(query).fetchall())
            join_seconds = time.perf_counter() - started_join
        return {'plan_id': plan, 'join_cloud': destination, 'edges': edges,
                'intermediate_rows': sum(e['rows'] for e in edges),
                'intermediate_parquet_bytes': sum(e['parquet_bytes'] for e in edges),
                'stage_seconds': stage_elapsed, 'join_aggregate_seconds': join_seconds,
                'total_seconds': time.perf_counter() - start_all,
                'result': result, 'physical_plan': physical_plan, 'sql_fragment': query}
    finally:
        con.close()


def root(args) -> Path:
    return args.output_dir / 'q03' / f'sf{args.sf}' / 'p1'


def choose_run(args, *, allow_local=True) -> Path:
    if args.run_id:
        candidate = root(args) / args.run_id
        if not candidate.is_dir():
            raise FileNotFoundError(f'Q03 archived run missing: {candidate}')
        return candidate
    candidates = []
    for path in root(args).glob('*/run.json'):
        try:
            obj = read_json(path)
            if obj.get('status') == 'success' and obj.get('query_id') == QUERY and obj.get('scale_factor') == f'SF{args.sf}':
                if allow_local or obj.get('execution_mode') == 'real_multicloud':
                    candidates.append(path.parent)
        except (ValueError, OSError):
            pass
    if not candidates:
        raise RuntimeError(f'No completed Q03 runs under {root(args)}. First execute --mode prepare (offline).')
    return sorted(candidates)[-1]


def require_reference(expected: list[dict], path: Path, source_sql_hash: str) -> dict:
    if path.exists():
        data = read_json(path)
        if data.get('query_id') != QUERY or data.get('scale_factor') != 'SF1':
            raise ValueError('Archived Q03 reference has wrong query ID or SF')
        if data.get('source_sql_sha256') != source_sql_hash:
            raise ValueError('Q03 SQL differs from archived reference; do not reuse old results')
        if data.get('canonical_result_sha256') != canonical_hash(expected):
            raise ValueError('Q03 fresh SQL result differs from archived reference')
        if data.get('canonical_rows') != expected:
            raise ValueError('Q03 reference rows differ from independent fresh SQL')
    else:
        # An automatically created local baseline is NOT an independent gold standard.
        save_json(path, {'query_id': QUERY, 'scale_factor': 'SF1',
                         'source_sql_sha256': source_sql_hash,
                         'canonical_result_sha256': canonical_hash(expected),
                         'reference_origin': 'first_local_execution_of_archived_SQL_not_independent',
                         'canonical_rows': expected})
    return read_json(path)


def prepare(args) -> int:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError('Install duckdb in the active environment before --mode prepare') from exc
    inputs = {key: args.data_dir / f'{key}.parquet' for key in TABLE_PLACEMENT}
    for name, path in {**inputs, 'q03.sql': args.query_file}.items():
        if not path.is_file():
            raise FileNotFoundError(f'Missing {name}: {path}')
    sql = args.query_file.read_text(encoding='utf-8')
    params = load_params(sql)
    con = duckdb.connect(':memory:')
    try:
        con.execute(f'SET threads={args.threads}')
        t = time.perf_counter()
        expected = reference_rows(con, inputs, sql)
        reference_secs = time.perf_counter() - t
    finally:
        con.close()
    source_hash = sha256(args.query_file)
    reference = require_reference(expected, args.reference_file, source_hash)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8]
    directory = root(args) / run_id
    directory.mkdir(parents=True, exist_ok=False)
    result_hash = canonical_hash(expected)
    rows = []
    report = {'schema_version': 1, 'query_id': QUERY, 'run_id': run_id,
              'status': 'running', 'scale_factor': 'SF1', 'placement_id': PLACEMENT,
              'execution_mode': 'local_emulation_NO_NETWORK', 'created_at_utc': now(),
              'query_parameters': params, 'reference_sha256': result_hash,
              'reference_origin': reference['reference_origin'],
              'reference_duration_seconds': reference_secs,
              'reference_row_count': len(expected),
              'source_sql': {'path': str(args.query_file), 'sha256': source_hash},
              'reference_file': {'path': str(args.reference_file), 'sha256': sha256(args.reference_file)},
              'datasets': {table: {'path': str(path), 'sha256': sha256(path),
                                   'bytes': path.stat().st_size, 'cloud': TABLE_PLACEMENT[table]}
                           for table, path in inputs.items()},
              'runner_sha256': sha256(Path(__file__)),
              'versions': {'python': platform.python_version(), 'duckdb': duckdb.__version__},
              'trial_count_per_plan': args.trials, 'plan_order': list(PLANS),
              'timing_definition': 'local COPY plus local SQL; includes Python overhead; NOT network or cloud latency',
              'egress_cost_status': 'NOT_MEASURED', 'network_status': 'NO_NETWORK',
              'hypotheses': ['AWS join versus Azure join', 'local selection/projection',
                             'two-edge semijoin reduction'],
              'reference_rows_file': 'q03_reference_rows.json'}
    save_json(directory / 'q03_reference_rows.json', expected)
    save_json(directory / 'run.json', report)
    try:
        for trial in range(1, args.trials+1):
            # Alternate order by trial to avoid drifting local warm-cache advantage.
            order = PLANS if trial % 2 else tuple(reversed(PLANS))
            for plan in order:
                result = local_plan(duckdb, inputs, params, plan, args.threads)
                valid = result['result'] == expected
                record = {'run_id': run_id, 'query_id': QUERY, 'scale_factor': 'SF1',
                          'placement_id': PLACEMENT, 'execution_mode': 'local_emulation_NO_NETWORK',
                          'plan_id': plan, 'trial': trial, 'join_cloud': result['join_cloud'],
                          'source_clouds': 'azure' if plan == 'aws_join' else ('aws' if plan == 'azure_join' else 'azure+aws'),
                          'transfer_edges': len(result['edges']),
                          'intermediate_rows': result['intermediate_rows'],
                          'intermediate_parquet_bytes': result['intermediate_parquet_bytes'],
                          'stage_seconds': result['stage_seconds'],
                          'join_aggregate_seconds': result['join_aggregate_seconds'],
                          'total_seconds': result['total_seconds'],
                          'result_row_count': len(result['result']),
                          'result_sha256': canonical_hash(result['result']),
                          'correct_vs_reference': valid,
                          'network_bytes_measured': '', 'network_seconds_measured': '',
                          'egress_usd_estimated': '', 'timestamp_utc': now()}
                rows.append(record)
                explain_path = directory / f'trial_{trial:02d}_{plan}_physical_plan.txt'
                explain_path.write_text(result['physical_plan'] + "\n", encoding='utf-8')
                save_json(directory / f'trial_{trial:02d}_{plan}.json',
                          {'schema_version': 1, 'run_id': run_id, 'trial': trial,
                           'plan_id': plan, 'transfer_edges': result['edges'],
                           'result': result['result'], 'metrics': record,
                           'plan_explain_file': explain_path.name,
                           'plan_explain_sha256': sha256(explain_path),
                           'sql_fragment': result['sql_fragment']})
                print(f'{plan:16s} trial={trial} correctness={"PASS" if valid else "FAIL"} '
                      f'emulated_intermediate={result["intermediate_parquet_bytes"]:,} bytes '
                      f'local_workflow={result["total_seconds"]:.3f}s', flush=True)
                if not valid:
                    raise RuntimeError(f'Local Q03 plan {plan} is NOT equivalent to archived SQL')
                # Crash-resilient partial evidence; not a successful complete run yet.
                write_metrics(directory / 'trial_metrics.csv', rows)
        report.update({'status': 'success', 'completed_at_utc': now(),
                       'trial_metrics_csv': 'trial_metrics.csv',
                       'trial_metrics_sha256': sha256(directory / 'trial_metrics.csv')})
        save_json(directory / 'run.json', report)
        print('Q03_LOCAL_PLAN_VALIDATION_PASS')
        print('Q03 local evidence: ' + str(directory))
        print('NOTE: NO inter-cloud bytes, real network duration or egress USD measured.')
        return 0
    except Exception as exc:
        report.update({'status': 'failed', 'error': f'{type(exc).__name__}: {exc}', 'completed_at_utc': now()})
        save_json(directory / 'run.json', report)
        raise


def write_metrics(path: Path, rows: list[dict]):
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


def archived_data(args):
    directory = choose_run(args)
    report = read_json(directory / 'run.json')
    path = directory / 'trial_metrics.csv'
    if not path.is_file():
        raise FileNotFoundError(f'No trial CSV in {directory}')
    with path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not set(FIELDS).issubset(reader.fieldnames or []):
            raise ValueError('Missing required columns in Q03 trial metrics')
        records = list(reader)
    return directory, report, records


def audit(args, *, quiet=False):
    directory, run, records = archived_data(args)
    errors = []
    if run.get('status') != 'success':
        errors.append('run is not successful')
    if run.get('query_id') != QUERY or run.get('scale_factor') != f'SF{args.sf}' or run.get('placement_id') != PLACEMENT:
        errors.append('query/scale/placement does not match selected evidence')
    if sha256(directory/'trial_metrics.csv') != run.get('trial_metrics_sha256'):
        errors.append('trial_metrics.csv content hash does not match run manifest')
    try:
        ref = read_json(directory/'q03_reference_rows.json')
        expected_hash = canonical_hash(ref)
        if expected_hash != run['reference_sha256'] or len(ref) != int(run['reference_row_count']):
            errors.append('archived reference result/row count mismatch')
    except Exception as exc:
        errors.append('invalid archived reference: ' + str(exc))
        expected_hash = None
    expected_trials = int(run.get('trial_count_per_plan', 0))
    observed = {}
    for row in records:
        try:
            k = (row['plan_id'], int(row['trial']))
            if row['plan_id'] not in PLANS or int(row['trial']) < 1 or int(row['trial']) > expected_trials:
                errors.append(f'unexpected plan/trial {k}')
            observed[k] = observed.get(k, 0) + 1
            if row['run_id'] != run['run_id'] or row['query_id'] != QUERY or row['scale_factor'] != f'SF{args.sf}':
                errors.append(f'wrong identity {k}')
            if row['correct_vs_reference'] != 'True' or row['result_sha256'] != expected_hash:
                errors.append(f'incorrect result {k}')
            if int(row['transfer_edges']) != (2 if row['plan_id'] != 'azure_join' else 1):
                errors.append(f'wrong edge count {k}')
            if int(row['intermediate_parquet_bytes']) <= 0 or int(row['intermediate_rows']) <= 0:
                errors.append(f'implausible intermediate size {k}')
            if any(float(row[x]) < 0 for x in ('total_seconds', 'stage_seconds', 'join_aggregate_seconds')):
                errors.append(f'negative timing {k}')
            if row['execution_mode'] == 'local_emulation_NO_NETWORK':
                if any(row[key] for key in ('network_bytes_measured', 'network_seconds_measured', 'egress_usd_estimated')):
                    errors.append(f'fabricated network/egress metric {k}')
            elif row['execution_mode'] == 'real_multicloud':
                if row['egress_usd_estimated'] or row['network_seconds_measured']:
                    errors.append(f'unverified billed egress/isolated network time {k}')
                if int(row['network_bytes_measured']) != int(row['intermediate_parquet_bytes']):
                    errors.append(f'HTTP body/Parquet byte mismatch {k}')
            else:
                errors.append(f'unknown execution mode {k}')
            trial_file = directory / f'trial_{int(row["trial"]):02d}_{row["plan_id"]}.json'
            trial_obj = read_json(trial_file)
            if canonical_hash(trial_obj['result']) != row['result_sha256']:
                errors.append(f'trial result content mismatch {k}')
            plan_path = directory / trial_obj['plan_explain_file']
            if sha256(plan_path) != trial_obj['plan_explain_sha256']:
                errors.append(f'optimized physical-plan artifact mismatch {k}')
            metrics = trial_obj['metrics']
            if any(str(metrics[key]) != row[key] for key in FIELDS):
                errors.append(f'trial JSON and CSV differ {k}')
            edge_list = trial_obj['transfer_edges']
            if len(edge_list) != int(row['transfer_edges']):
                errors.append(f'edge list mismatch {k}')
            if sum(int(edge['parquet_bytes']) for edge in edge_list) != int(row['intermediate_parquet_bytes']):
                errors.append(f'edge bytes mismatch {k}')
            if sum(int(edge['rows']) for edge in edge_list) != int(row['intermediate_rows']):
                errors.append(f'edge rows mismatch {k}')
            if row['execution_mode'] == 'real_multicloud':
                if sum(int(edge['curl_size_upload']) for edge in edge_list) != int(row['network_bytes_measured']):
                    errors.append(f'edge measured HTTP bytes mismatch {k}')
                if any(not re.fullmatch(r'[a-f0-9]{64}',edge['sha256']) for edge in edge_list):
                    errors.append(f'missing transfer SHA evidence {k}')
                expected_direction = ([('azure','aws'),('azure','aws')] if row['plan_id']=='aws_join' else
                    ([('aws','azure')] if row['plan_id']=='azure_join' else [('azure','aws'),('aws','azure')]))
                if [(e['source_cloud'],e['destination_cloud']) for e in edge_list] != expected_direction:
                    errors.append(f'wrong cloud edge sequence {k}')
                modeled = sum(float(e['stage_seconds'])+float(e['curl_http_total_seconds']) for e in edge_list) + float(row['join_aggregate_seconds'])
                if abs(modeled-float(row['total_seconds'])) > 0.00000001:
                    errors.append(f'cloud timing components do not add up {k}')
        except Exception as exc:
            errors.append(f'bad trial record: {type(exc).__name__} {exc}')
    wanted = {(p, i) for p in PLANS for i in range(1, expected_trials+1)}
    if set(observed) != wanted or any(count != 1 for count in observed.values()):
        errors.append(f'missing/duplicated plan trials; expected {len(wanted)} got {len(records)}')
    if len(records) != len(wanted):
        errors.append('trial count mismatch')
    status = 'PASS' if not errors else 'FAIL'
    result = {'query_id': QUERY, 'run_id': run['run_id'], 'audited_at_utc': now(),
              'status': status, 'trial_count': len(records), 'errors': errors,
              'limitations': ['consistency audit, not proof of immutable cloud provenance',
                              'local simulation is not a real cloud measurement']}
    analysis_dir = directory / 'q03_analysis'
    save_json(analysis_dir/'audit.json', result)
    if not quiet:
        print('Q03_EVIDENCE_AUDIT_' + status)
        print('Evidence: ' + str(directory))
        if errors:
            print('First discrepancies: ' + '; '.join(errors[:8]), file=sys.stderr)
    if errors:
        raise RuntimeError(f'Q03 audit FAILED with {len(errors)} discrepancies')
    return directory, records


def reproduce(args) -> int:
    """Recalculate Q03 figures/tables strictly from the audited archived trial CSV."""
    directory, records = audit(args, quiet=True)
    run = read_json(directory / 'run.json')
    is_cloud = run['execution_mode'] == 'real_multicloud'
    grouped = []
    for plan in PLANS:
        subset = sorted((r for r in records if r['plan_id'] == plan),
                        key=lambda r:int(r['trial']))
        durations = [float(r['total_seconds']) for r in subset]
        payloads = [int(r['intermediate_parquet_bytes']) for r in subset]
        stage_times = [float(r['stage_seconds']) for r in subset]
        join_times = [float(r['join_aggregate_seconds']) for r in subset]
        grouped.append({
            'plan_id':plan, 'trials':len(subset),
            'mean_workflow_seconds':statistics.mean(durations),
            'median_workflow_seconds':statistics.median(durations),
            'sample_stdev_workflow_seconds':statistics.stdev(durations) if len(durations)>1 else None,
            'min_workflow_seconds':min(durations),'max_workflow_seconds':max(durations),
            'mean_stage_seconds':statistics.mean(stage_times),
            'mean_join_seconds':statistics.mean(join_times),
            'mean_intermediate_parquet_bytes':statistics.mean(payloads),
            'mean_http_body_bytes':(statistics.mean(int(r['network_bytes_measured']) for r in subset)
                                    if is_cloud else None),
            'transfer_edges':int(subset[0]['transfer_edges']),
            'all_correct':all(r['correct_vs_reference']=='True' for r in subset)})
    analysis = directory/'q03_analysis'
    analysis.mkdir(parents=True,exist_ok=True)
    explanation=('Cloud measurements: sum of stage wall times, HTTPS request wall times, '
                 'and destination join time; excludes VM startup and management command overhead. '
                 'HTTP body bytes are not provider-billed egress. No egress dollars were measured.'
                 if is_cloud else
                 'Local Parquet emulation; times and intermediate bytes were NOT measured between clouds.')
    result={'schema_version':2,'run_id':run['run_id'],'query':QUERY,
            'sf':f'SF{args.sf}','placement':PLACEMENT,
            'execution_mode':run['execution_mode'],
            'input_trial_metrics_sha256':sha256(directory/'trial_metrics.csv'),
            'source':'trial_metrics.csv','warmups_excluded':True,
            'not_evaluating_optimizer':True,'notes':explanation,'plans':grouped}
    save_json(analysis/'summary.json',result)
    with (analysis/'summary.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(grouped[0]));w.writeheader();w.writerows(grouped)
    latex=['% Q03 SF1 P1; '+explanation,
           r'\begin{tabular}{lrrrr}',r'\hline',
           r'Plan & $n$ & Mean (ms) & SD (ms) & Parquet (bytes) \\',r'\hline']
    for item in grouped:
        name=item['plan_id'].replace('_',r'\_')
        sd=(item['sample_stdev_workflow_seconds'] or 0)*1000
        latex.append(f'{name} & {item["trials"]} & {item["mean_workflow_seconds"]*1000:.2f} & '
                     f'{sd:.2f} & {item["mean_intermediate_parquet_bytes"]:.0f} '+r'\\')
    latex += [r'\hline',r'\end{tabular}']
    (analysis/'table_q03.tex').write_text('\n'.join(latex)+'\n',encoding='utf-8')
    if not is_cloud:
        # Keep the previous local artifact name for backward compatibility.
        (analysis/'table_local.tex').write_text('\n'.join(latex)+'\n',encoding='utf-8')
    if (directory/'warmup_metrics.csv').is_file():
        with (directory/'warmup_metrics.csv').open(newline='',encoding='utf-8') as f:
            warm=list(csv.DictReader(f))
        save_json(analysis/'warmup_summary.json',{
            'not_included_in_measured_statistics':True,
            'warmup_count':len(warm),
            'warmups':warm,
        })
    if args.plots:
        try:
            import matplotlib.pyplot as plt
        except ImportError as exc:
            raise RuntimeError('Install matplotlib to regenerate Q03 figures') from exc
        suffix='real AWS–Azure transfer' if is_cloud else 'LOCAL EMULATION: no cloud transfer'
        def finish_figure(fig,name):
            fig.tight_layout()
            fig.savefig(analysis/(name+'.png'),dpi=220)
            fig.savefig(analysis/(name+'.pdf'))
            plt.close(fig)
        names=[x['plan_id'] for x in grouped]
        fig,ax=plt.subplots(figsize=(7,4.0))
        ax.bar(names,[x['mean_intermediate_parquet_bytes'] for x in grouped])
        ax.set_ylabel('Total intermediate Parquet bytes per query')
        ax.set_title('Q03 SF1/P1 — '+suffix)
        finish_figure(fig,'q03_intermediate_bytes')
        fig,ax=plt.subplots(figsize=(7,4.0))
        ax.bar(names,[1000*x['mean_workflow_seconds'] for x in grouped])
        ax.set_ylabel('Mean measured workflow duration (ms)' if is_cloud else 'Mean LOCAL workflow duration (ms)')
        ax.set_title('Q03 SF1/P1 — '+suffix)
        finish_figure(fig,'q03_mean_duration')
        fig,ax=plt.subplots(figsize=(7,4.0))
        for plan in PLANS:
            subset=sorted((r for r in records if r['plan_id']==plan),key=lambda x:int(x['trial']))
            ax.plot([int(r['trial']) for r in subset],
                    [1000*float(r['total_seconds']) for r in subset],marker='o',label=plan)
        ax.set_xlabel('Measured trial (warm-ups excluded)')
        ax.set_ylabel('Stage + HTTPS + join (ms)' if is_cloud else 'Local emulated workflow (ms)')
        ax.set_title('Q03 SF1/P1 — '+suffix)
        ax.legend()
        finish_figure(fig,'q03_trial_durations')
    print('Q03_REPRODUCE_PASS')
    print('Reproduced Q03 tables/figures: '+str(analysis))
    return 0


Q03_WORKER_SOURCE = '#!/usr/bin/env python3\n"""Temporary Q03 HTTPS data worker; embedded and deployed by experiments/q03.py."""\nimport argparse\nfrom decimal import Decimal\nimport gzip\nimport hashlib\nfrom http.server import BaseHTTPRequestHandler, ThreadingHTTPServer\nimport json\nimport os\nfrom pathlib import Path\nimport re\nimport ssl\nimport threading\nimport time\nimport traceback\nimport duckdb\n\nCLOUD = os.environ[\'Q03_CLOUD\']\nROOT = Path(os.environ[\'Q03_RUNTIME\'])\nDATA = Path(os.environ[\'Q03_DATA_DIR\'])\nTOKEN = os.environ[\'Q03_WORKER_TOKEN\']\nSEGMENT = os.environ[\'Q03_SEGMENT\']\nCUTOFF = os.environ[\'Q03_CUTOFF\']\nPORT = int(os.environ.get(\'Q03_PORT\', \'8443\'))\nPORTIONS = {\'customer\', \'lineitem\', \'orders\', \'customer_keys\', \'orders_reduced\'}\nPLANS = {\'aws_join\', \'azure_join\', \'semijoin_azure\'}\nSHA = re.compile(r\'^[a-f0-9]{64}$\')\nSLUG = re.compile(r\'^[a-zA-Z0-9_-]{1,40}$\')\nif CLOUD not in {\'aws\', \'azure\'} or not re.fullmatch(r\'\\d{4}-\\d{2}-\\d{2}\', CUTOFF) or not re.fullmatch(r\'[A-Za-z]+\', SEGMENT):\n    raise RuntimeError(\'Invalid Q03 worker environment\')\n\n\ndef sql_path(table):\n    if table not in {\'customer\', \'orders\', \'lineitem\'}:\n        raise ValueError(\'Unknown table\')\n    f = DATA / (table + \'.parquet\')\n    if not f.is_file():\n        raise RuntimeError(\'Required local table missing: \' + str(f))\n    return "\'" + str(f).replace("\'", "\'\'") + "\'"\n\n\ndef scan_file(path):\n    return \'read_parquet(\' + "\'" + str(path).replace("\'", "\'\'") + "\'" + \')\'\n\n\ndef filtered(part, trial, plan):\n    date = "DATE \'" + CUTOFF + "\'"\n    if part in {\'customer\', \'customer_keys\'} and CLOUD == \'azure\':\n        return f\'SELECT c_custkey FROM read_parquet({sql_path("customer")}) WHERE c_mktsegment = \\\'{SEGMENT}\\\'\'\n    if part == \'lineitem\' and CLOUD == \'azure\':\n        return f\'SELECT l_orderkey,l_extendedprice,l_discount FROM read_parquet({sql_path("lineitem")}) WHERE l_shipdate > {date}\'\n    if part == \'orders\' and CLOUD == \'aws\':\n        return f\'SELECT o_orderkey,o_custkey,o_orderdate,o_shippriority FROM read_parquet({sql_path("orders")}) WHERE o_orderdate < {date}\'\n    if part == \'orders_reduced\' and CLOUD == \'aws\' and plan == \'semijoin_azure\':\n        keys = ROOT/\'received\'/trial/plan/\'customer_keys.parquet\'\n        if not keys.is_file():\n            raise RuntimeError(\'Q03 semijoin keys not uploaded\')\n        o = filtered(\'orders\', trial, plan)\n        return f\'SELECT o.* FROM ({o}) AS o SEMI JOIN {scan_file(keys)} AS keys ON o.o_custkey=keys.c_custkey\'\n    raise RuntimeError(f\'Invalid stage {CLOUD} {part} {plan}\')\n\n\ndef result_sql(trial, plan):\n    r = ROOT/\'received\'/trial/plan\n    customer = (scan_file(r/\'customer.parquet\') if plan == \'aws_join\' else \'(\' + filtered(\'customer\',trial,plan) + \')\')\n    lines = (scan_file(r/\'lineitem.parquet\') if plan == \'aws_join\' else \'(\' + filtered(\'lineitem\',trial,plan) + \')\')\n    if plan == \'aws_join\' and CLOUD == \'aws\':\n        orders = \'(\' + filtered(\'orders\', trial, plan) + \')\'\n    elif plan in {\'azure_join\', \'semijoin_azure\'} and CLOUD == \'azure\':\n        orders = scan_file(r/(\'orders_reduced.parquet\' if plan == \'semijoin_azure\' else \'orders.parquet\'))\n    else:\n        raise RuntimeError(\'Join on wrong provider\')\n    req = ((r/\'customer.parquet\',r/\'lineitem.parquet\') if plan == \'aws_join\'\n           else ((r/\'orders_reduced.parquet\',) if plan == \'semijoin_azure\' else (r/\'orders.parquet\',)))\n    for p in req:\n        if not p.is_file():\n            raise RuntimeError(\'Join missing transferred file: \' + str(p))\n    if plan == \'semijoin_azure\':\n        frm = f\'FROM {orders} AS o JOIN {lines} AS l ON o.o_orderkey=l.l_orderkey\'\n    else:\n        frm = f\'FROM {customer} AS c JOIN {orders} AS o ON c.c_custkey=o.o_custkey JOIN {lines} AS l ON l.l_orderkey=o.o_orderkey\'\n    return (\'SELECT l.l_orderkey, SUM(l.l_extendedprice * (1-l.l_discount)) AS revenue, \'\n            \'o.o_orderdate, o.o_shippriority \' + frm +\n            \' GROUP BY l.l_orderkey,o.o_orderdate,o.o_shippriority ORDER BY revenue DESC,o.o_orderdate LIMIT 10\')\n\n\ndef canonical(rows):\n    result=[]\n    for r in rows:\n        x=Decimal(str(r[1])).quantize(Decimal(\'0.0001\'))\n        result.append({\'l_orderkey\':int(r[0]), \'revenue\':format(x,\'.4f\'),\n                       \'o_orderdate\':str(r[2])[:10], \'o_shippriority\':int(r[3])})\n    return sorted(result,key=lambda x:(x[\'l_orderkey\'],x[\'o_orderdate\'],x[\'o_shippriority\']))\n\n\ndef sha(path):\n    h=hashlib.sha256()\n    with open(path,\'rb\') as f:\n        for chunk in iter(lambda:f.read(1024*1024), b\'\'):\n            h.update(chunk)\n    return h.hexdigest()\n\n\nclass Handler(BaseHTTPRequestHandler):\n    server_version=\'Q03ExperimentalWorker/1.0\'\n    def log_message(self, fmt, *args):\n        pass\n    def json_response(self, code, value):\n        body=json.dumps(value,sort_keys=True,separators=(\',\',\':\')).encode()\n        self.send_response(code)\n        self.send_header(\'Content-Type\',\'application/json\')\n        self.send_header(\'Content-Length\',str(len(body)))\n        self.end_headers()\n        self.wfile.write(body)\n    def auth(self):\n        # Uses constant-time comparison; never echo tokens in HTTP responses.\n        import hmac\n        if not hmac.compare_digest(self.headers.get(\'Authorization\',\'\'), \'Bearer \'+TOKEN):\n            self.json_response(401,{\'error\':\'unauthorized\'})\n            return False\n        return True\n    def body_json(self):\n        size=int(self.headers.get(\'Content-Length\',\'0\'))\n        if size<1 or size>2000:\n            raise ValueError(\'Invalid JSON request size\')\n        obj=json.loads(self.rfile.read(size))\n        trial=obj.get(\'trial_id\',\'\')\n        plan=obj.get(\'plan_id\',\'\')\n        if not SLUG.fullmatch(trial) or plan not in PLANS:\n            raise ValueError(\'Invalid trial or plan\')\n        return obj,trial,plan\n    def do_GET(self):\n        if not self.auth(): return\n        if self.path == \'/health\':\n            self.json_response(200,{\'status\':\'ok\',\'cloud\':CLOUD,\'query\':\'Q03\',\'duckdb\':duckdb.__version__})\n        else:\n            self.json_response(404,{\'error\':\'not found\'})\n    def do_POST(self):\n        if not self.auth(): return\n        try:\n            if self.path == \'/q03/stage\':\n                obj,trial,plan = self.body_json()\n                part=obj.get(\'part\',\'\')\n                if part not in PORTIONS: raise ValueError(\'Unknown Q03 stage part\')\n                out=ROOT/\'stage\'/trial/(part+\'.parquet\')\n                out.parent.mkdir(parents=True,exist_ok=True)\n                sql=filtered(part,trial,plan)\n                t0=time.perf_counter()\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    con.execute(f"COPY ({sql}) TO \'{str(out).replace(chr(39),chr(39)*2)}\' (FORMAT PARQUET, COMPRESSION ZSTD)")\n                    row_count=con.execute(f"SELECT count(*) FROM {scan_file(out)}").fetchone()[0]\n                elapsed=time.perf_counter()-t0\n                self.json_response(200,{\'cloud\':CLOUD,\'part\':part,\'trial_id\':trial,\'plan_id\':plan,\n                    \'stage_seconds\':elapsed,\'rows\':row_count,\'parquet_bytes\':out.stat().st_size,\n                    \'sha256\':sha(out),\'file_path\':str(out)})\n            elif self.path.startswith(\'/q03/upload/\'):\n                segments=self.path.split(\'/\')\n                if len(segments)!=6: raise ValueError(\'Invalid upload URL\')\n                _,q03,upload,trial,plan,part = segments\n                if not SLUG.fullmatch(trial) or plan not in PLANS or part not in PORTIONS:\n                    raise ValueError(\'Invalid upload path\')\n                expected=self.headers.get(\'X-Q03-SHA256\',\'\')\n                if not SHA.fullmatch(expected): raise ValueError(\'Missing content hash\')\n                remaining=int(self.headers.get(\'Content-Length\',\'0\'))\n                if remaining<1 or remaining>300_000_000: raise ValueError(\'Invalid size\')\n                path=ROOT/\'received\'/trial/plan/(part+\'.parquet\')\n                path.parent.mkdir(parents=True,exist_ok=True)\n                h=hashlib.sha256(); got=0\n                started=time.perf_counter()\n                tmp=path.with_suffix(\'.part\')\n                with tmp.open(\'wb\') as f:\n                    while remaining:\n                        chunk=self.rfile.read(min(1024*1024,remaining))\n                        if not chunk: raise ValueError(\'Truncated body\')\n                        f.write(chunk);h.update(chunk)\n                        got+=len(chunk);remaining-=len(chunk)\n                if h.hexdigest()!=expected:\n                    tmp.unlink(missing_ok=True)\n                    raise ValueError(\'Upload SHA256 mismatch\')\n                tmp.replace(path)\n                self.json_response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'part\':part,\'trial_id\':trial,\n                    \'received_payload_bytes\':got,\'sha256\':h.hexdigest(),\n                    \'receive_file_seconds\':time.perf_counter()-started})\n            elif self.path == \'/q03/finish\':\n                obj,trial,plan=self.body_json()\n                sql=result_sql(trial,plan)\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    plan_txt=\'\\n\'.join(str(r[-1]) for r in con.execute(\'EXPLAIN \'+sql).fetchall())\n                    t0=time.perf_counter()\n                    rows=canonical(con.execute(sql).fetchall())\n                    sec=time.perf_counter()-t0\n                row_hash=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(\',\',\':\')).encode()).hexdigest()\n                plan_bytes=gzip.compress(plan_txt.encode())\n                import base64\n                self.json_response(200,{\'cloud\':CLOUD,\'trial_id\':trial,\'plan_id\':plan,\'result\':rows,\n                    \'result_sha256\':row_hash,\'join_seconds\':sec,\'row_count\':len(rows),\n                    \'explain_gzip_b64\':base64.b64encode(plan_bytes).decode(),\n                    \'physical_plan_sha256\':hashlib.sha256(plan_txt.encode()).hexdigest()})\n            else:\n                self.json_response(404,{\'error\':\'not found\'})\n        except Exception as exc:\n            self.json_response(422,{\'error\':type(exc).__name__+\': \'+str(exc)[:200]})\n\n\ndef main():\n    parser=argparse.ArgumentParser()\n    parser.add_argument(\'--cert\',required=True)\n    parser.add_argument(\'--key\',required=True)\n    args=parser.parse_args()\n    server=ThreadingHTTPServer((\'0.0.0.0\',PORT), Handler)\n    ssl_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n    ssl_ctx.minimum_version=ssl.TLSVersion.TLSv1_2\n    ssl_ctx.load_cert_chain(args.cert,args.key)\n    server.socket=ssl_ctx.wrap_socket(server.socket,server_side=True)\n    server.serve_forever()\n\nif __name__==\'__main__\': main()\n'

# ------------------- Independent Q03 real-cloud orchestration ------------------
# This is embedded into q03.py; there are NO Q12 imports or Q12 worker dependencies.
import base64
import gzip
import ipaddress
import secrets
import shlex
import subprocess
import textwrap

AWS_INSTANCE = os.environ.get('EGRESSOPT_AWS_INSTANCE_ID','')
AWS_PROFILE = os.environ.get('EGRESSOPT_AWS_PROFILE','')
AWS_REGION = os.environ.get('EGRESSOPT_AWS_REGION','')
AZURE_RESOURCE_GROUP = os.environ.get('EGRESSOPT_AZURE_RESOURCE_GROUP','')
AZURE_VM = os.environ.get('EGRESSOPT_AZURE_VM','')
AZURE_NSG = os.environ.get('EGRESSOPT_AZURE_NSG','')
AZURE_PUBLIC_IP_RESOURCE = os.environ.get('EGRESSOPT_AZURE_PUBLIC_IP_RESOURCE','')
Q03_PORT=8443


def q03_cli(argv, timeout=420):
    try:
        p=subprocess.run(argv,capture_output=True,text=True,timeout=timeout)
    except FileNotFoundError as exc:
        raise RuntimeError(f'Missing required command: {argv[0]}') from exc
    if p.returncode:
        raise RuntimeError(f'{argv[0]} failed: {(p.stderr or p.stdout)[-1400:]}')
    return p.stdout.strip()


def q03_aws(*args):
    return json.loads(q03_cli(['aws',*args,'--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json']))


def q03_azure_script(script,label):
    """Run an Azure guest command, retrying only VM-not-running transient errors.

    Azure may report PowerState/running before its guest Run Command agent is
    available. Other failures (permissions, script errors, missing data, etc.)
    must surface immediately, not be silently retried.
    """
    argv=['az','vm','run-command','invoke','--resource-group',AZURE_RESOURCE_GROUP,
          '--name',AZURE_VM,'--command-id','RunShellScript',
          '--scripts','set -eu\n'+script,'--query','value[0].message',
          '--output','tsv']
    attempts=10
    for attempt in range(1, attempts+1):
        try:
            output=q03_cli(argv,timeout=450)
        except RuntimeError as exc:
            msg=str(exc)
            is_startup_race=('OperationNotAllowed' in msg and
                'operation requires the vm to be running' in msg.lower())
            if not is_startup_race:
                raise RuntimeError(f'Azure Run Command {label}: {msg}') from exc
            if attempt==attempts:
                raise RuntimeError(
                    f'Azure Run Command {label} unavailable after {attempts} '
                    'attempts; VM may be stopping or guest agent may not be ready. '
                    'Inspect Azure power state and VM agent before retrying.'
                ) from exc
            wait=min(4+2*attempt,20)
            print(f'  Azure guest Run Command not yet ready '
                  f'({label}; attempt {attempt}/{attempts}); retrying...',
                  flush=True)
            time.sleep(wait)
            continue
        if 'Enable succeeded' not in output:
            raise RuntimeError(f'Azure run-command {label} failed: {output[-900:]}')
        return output
    raise AssertionError('Unreachable Azure Run Command retry loop')


def q03_aws_ssm(script,label):
    cmd=q03_cli(['aws','ssm','send-command','--instance-ids',AWS_INSTANCE,
           '--document-name','AWS-RunShellScript',
           '--parameters',json.dumps({'commands':['set -eu',script]}),
           '--comment',('EgressOpt Q03 '+label)[:100],
           '--query','Command.CommandId','--output','text','--profile',AWS_PROFILE,
           '--region',AWS_REGION],timeout=120)
    deadline=time.monotonic()+450
    while time.monotonic()<deadline:
        p=subprocess.run(['aws','ssm','get-command-invocation','--command-id',cmd,
             '--instance-id',AWS_INSTANCE,'--profile',AWS_PROFILE,'--region',AWS_REGION,
             '--output','json'],capture_output=True,text=True,timeout=50)
        if p.returncode:
            if 'InvocationDoesNotExist' in p.stderr:
                time.sleep(2);continue
            raise RuntimeError('AWS SSM failed: '+p.stderr[-900:])
        obj=json.loads(p.stdout)
        if obj['Status']=='Success':return obj.get('StandardOutputContent','')
        if obj['Status'] in {'Failed','TimedOut','Cancelled','Cancelling'}:
            raise RuntimeError(f'AWS SSM {label} {obj["Status"]}: '+
                               obj.get('StandardErrorContent','')[-900:]+
                               obj.get('StandardOutputContent','')[-900:])
        time.sleep(2)
    raise RuntimeError('AWS SSM command timeout')


def q03_remote(cloud,script,label):
    if cloud=='aws':return q03_aws_ssm(script,label)
    if cloud=='azure':return q03_azure_script(script,label)
    raise ValueError('Unknown cloud')


def q03_marker(output):
    found=[line.split('Q03_RPC=',1)[1].strip() for line in output.splitlines()
           if line.startswith('Q03_RPC=')]
    if len(found)!=1:
        raise RuntimeError('Q03 remote command did not return exactly one JSON response; '+output[-400:])
    return json.loads(found[0])


def q03_pystate():
    aws=q03_aws('ec2','describe-instances','--instance-ids',AWS_INSTANCE)['Reservations']
    if len(aws)!=1 or len(aws[0]['Instances'])!=1:
        raise RuntimeError('Expected one AWS research EC2 instance')
    inst=aws[0]['Instances'][0]
    codes=json.loads(q03_cli(['az','vm','get-instance-view','--resource-group',AZURE_RESOURCE_GROUP,
                               '--name',AZURE_VM,'--query','instanceView.statuses[].code',
                               '--output','json']))
    azure=[c for c in codes if isinstance(c,str) and c.startswith('PowerState/')]
    if len(azure)!=1: raise RuntimeError('Azure VM power state unknown')
    return inst,azure[0]


def q03_start_vm(cloud):
    if cloud=='aws':
        inst,_=q03_pystate()
        state=inst['State']['Name']
        if state=='running':return False
        if state not in {'stopped','pending'}:raise RuntimeError(f'Unsafe AWS state: {state}')
        if state=='stopped':
            q03_cli(['aws','ec2','start-instances','--instance-ids',AWS_INSTANCE,
                     '--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json'])
        q03_cli(['aws','ec2','wait','instance-running','--instance-ids',AWS_INSTANCE,
                 '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=650)
        deadline=time.monotonic()+250
        while time.monotonic()<deadline:
            data=q03_aws('ssm','describe-instance-information','--filters',
                         json.dumps([{'Key':'InstanceIds','Values':[AWS_INSTANCE]}]))
            if any(x.get('InstanceId')==AWS_INSTANCE and x.get('PingStatus')=='Online'
                   for x in data.get('InstanceInformationList',[])):
                return state=='stopped'
            time.sleep(5)
        raise RuntimeError('AWS SSM agent is offline')
    if cloud=='azure':
        _,state=q03_pystate()
        was_running=(state=='PowerState/running')
        if not was_running:
            if state not in {'PowerState/deallocated','PowerState/stopped'}:
                raise RuntimeError('Unsafe Azure state: '+state)
            q03_cli(['az','vm','start','--resource-group',AZURE_RESOURCE_GROUP,
                     '--name',AZURE_VM],timeout=650)
        # PowerState/running alone is insufficient for Azure guest commands.
        # Probe the actual VM agent before proceeding to IP discovery/deployment.
        readiness=q03_azure_script('echo Q03_AZURE_GUEST_READY','guest-readiness')
        if 'Q03_AZURE_GUEST_READY' not in readiness:
            raise RuntimeError('Azure guest agent did not confirm Q03 readiness')
        return not was_running
    raise ValueError(cloud)


def q03_global_ip(value):
    ip=ipaddress.ip_address(value.strip())
    if ip.version!=4 or not ip.is_global:raise ValueError('Not global IPv4: '+str(value))
    return str(ip)


def q03_source_ip(cloud):
    response=q03_remote(cloud,'curl -fsS --connect-timeout 10 --max-time 30 https://checkip.amazonaws.com',
                        'verified-outbound-IP')
    if cloud=='azure' and '[stdout]' in response:
        response=response.split('[stdout]',1)[1].split('[stderr]',1)[0]
    return q03_global_ip(response.strip())


def q03_install_worker(cloud,run_key,source):
    # Upload the gzip-compressed embedded worker directly over the provider's
    # authenticated management channel; no public SSH/S3 or separate worker file.
    release=f'/opt/egressopt/releases/q03_{digest_bytes(source.encode())[:12]}'
    script_path=release+'/worker.py'
    compressed=base64.b64encode(gzip.compress(source.encode())).decode()
    tmp=f'/opt/egressopt/runtime/q03_upload_{run_key}_{cloud}'
    q03_remote(cloud,f'umask 077; mkdir -p {shlex.quote(tmp)} {shlex.quote(release)}; : > {shlex.quote(tmp+"/worker.gz.b64")}',
               'prepare-embedded-worker')
    for idx in range(0,len(compressed),1500):
        chunk=compressed[idx:idx+1500]
        q03_remote(cloud,f"printf '%s' {shlex.quote(chunk)} >> {shlex.quote(tmp+'/worker.gz.b64')}",
                   f'upload-worker-{idx//1500+1}')
    expected=digest_bytes(source.encode())
    result=q03_remote(cloud,
       f"base64 -d {shlex.quote(tmp+'/worker.gz.b64')} | gzip -d > {shlex.quote(script_path)}; "
       f"test \"$(sha256sum {shlex.quote(script_path)} | cut -d' ' -f1)\" = {shlex.quote(expected)}; "
       f"{'/opt/egressopt/venv311/bin/python' if cloud=='aws' else '/opt/egressopt/venv/bin/python'} -m py_compile {shlex.quote(script_path)}; "
       f"rm -rf {shlex.quote(tmp)}; echo Q03_WORKER_DEPLOYED",
       'verify-embedded-worker')
    if 'Q03_WORKER_DEPLOYED' not in result:
        raise RuntimeError('Q03 worker verification failed: '+cloud)
    return release


def q03_start_service(cloud,run_key,release,assigned_ip,token,params):
    work=f'/opt/egressopt/runtime/q03_{run_key}_{cloud}'
    python='/opt/egressopt/venv311/bin/python' if cloud=='aws' else '/opt/egressopt/venv/bin/python'
    unit=f'egressopt-q03-{run_key}-{cloud}'
    launch='\n'.join(['#!/bin/sh','set -eu',
       f'export Q03_CLOUD={shlex.quote(cloud)}',
       f'export Q03_RUNTIME={shlex.quote(work)}',
       f'export Q03_DATA_DIR={shlex.quote("/opt/egressopt/data/tpch_sf1")}',
       f'export Q03_WORKER_TOKEN={shlex.quote(token)}',
       f'export Q03_SEGMENT={shlex.quote(params["segment"])}',
       f'export Q03_CUTOFF={shlex.quote(params["cutoff"])}',
       f'exec {python} {shlex.quote(release+"/worker.py")} --cert {work}/server.pem --key {work}/server.key'])+'\n'
    b64=base64.b64encode(launch.encode()).decode()
    table='orders' if cloud=='aws' else 'lineitem'
    script=f'''
umask 077
mkdir -p {work}
test -f {release}/worker.py
test -f /opt/egressopt/data/tpch_sf1/{table}.parquet
{python} -c 'import duckdb; assert duckdb.__version__ == "1.5.6",duckdb.__version__'
command -v openssl >/dev/null
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 \
  -keyout {work}/ca.key -out {work}/ca.pem -subj '/CN=EgressOpt Q03 temporary CA' \
  -addext 'basicConstraints=critical,CA:TRUE' >/dev/null 2>&1
openssl req -nodes -newkey rsa:2048 -sha256 \
  -keyout {work}/server.key -out {work}/server.csr \
  -subj '/CN=EgressOpt Q03 temporary worker' >/dev/null 2>&1
printf 'basicConstraints=CA:FALSE\\nsubjectAltName=IP:{assigned_ip}\\nextendedKeyUsage=serverAuth\\n' > {work}/server.ext
openssl x509 -req -days 1 -sha256 -in {work}/server.csr \
  -CA {work}/ca.pem -CAkey {work}/ca.key -CAcreateserial \
  -extfile {work}/server.ext -out {work}/server.pem >/dev/null 2>&1
printf '%s' '{b64}' | base64 -d > {work}/launch.sh
chmod 700 {work}/launch.sh
systemd-run --unit={unit} --collect --property=Restart=no /bin/sh {work}/launch.sh >/dev/null
STATUS=000
for i in $(seq 1 40); do
  STATUS=$(curl --insecure -s -o /dev/null -w '%{{http_code}}' --max-time 3 https://127.0.0.1:8443/health || true)
  [ "$STATUS" = 401 ] && break
  sleep 1
done
test "$STATUS" = 401 || (systemctl status {unit} --no-pager >&2 || true; exit 1)
echo Q03_SERVICE_READY
printf 'Q03_CA_B64='; base64 -w0 {work}/ca.pem; echo
'''
    out=q03_remote(cloud,script,'start-TLS-worker')
    if 'Q03_SERVICE_READY' not in out:raise RuntimeError(cloud+' worker did not start')
    entries=[x.split('Q03_CA_B64=',1)[1] for x in out.splitlines() if x.startswith('Q03_CA_B64=')]
    if len(entries)!=1 or b'BEGIN CERTIFICATE' not in base64.b64decode(entries[0],validate=True):
        raise RuntimeError('Could not obtain temporary CA from '+cloud)
    return entries[0]


def q03_open_ingress(cloud,source_ip,run_key,aws_sg):
    if cloud=='aws':
        ip_permission=[{'IpProtocol':'tcp','FromPort':8443,'ToPort':8443,
            'IpRanges':[{'CidrIp':source_ip+'/32','Description':f'EgressOpt Q03 {run_key} temporary'}]}]
        data=q03_aws('ec2','authorize-security-group-ingress','--group-id',aws_sg,
                     '--ip-permissions',json.dumps(ip_permission))
        rules=data.get('SecurityGroupRules',[])
        if len(rules)!=1 or not rules[0].get('SecurityGroupRuleId'):
            raise RuntimeError('AWS ingress rule created without returned rule ID; inspect rule manually')
        return rules[0]['SecurityGroupRuleId']
    name=f'egressopt-q03-{run_key}'
    current=json.loads(q03_cli(['az','network','nsg','rule','list','--resource-group',AZURE_RESOURCE_GROUP,
                      '--nsg-name',AZURE_NSG,'--output','json']))
    used={int(r['priority']) for r in current if r.get('direction')=='Inbound'}
    priority=next((k for k in range(1100,2500) if k not in used),None)
    if priority is None:raise RuntimeError('No Azure NSG priority available')
    q03_cli(['az','network','nsg','rule','create','--resource-group',AZURE_RESOURCE_GROUP,
        '--nsg-name',AZURE_NSG,'--name',name,'--priority',str(priority),'--direction','Inbound',
        '--access','Allow','--protocol','Tcp','--source-address-prefixes',source_ip+'/32',
        '--destination-port-ranges','8443','--description',f'EgressOpt Q03 {run_key} temporary TLS',
        '--output','none'],timeout=240)
    return name


def q03_rpc(cloud,script,label):
    return q03_marker(q03_remote(cloud,'set -eu\n'+script,label))


def q03_stage(cloud,part,plan,trial_id,token,run_key):
    data=json.dumps({'part':part,'plan_id':plan,'trial_id':trial_id},separators=(',',':'))
    work=f'/opt/egressopt/runtime/q03_{run_key}_{cloud}'
    out=work+'/stage_response.json'
    script=(f'curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 300 '
            f'-H {shlex.quote("Authorization: Bearer "+token)} -H "Content-Type: application/json" '
            f'--data-binary {shlex.quote(data)} https://127.0.0.1:8443/q03/stage > {out}\n'
            f"{('/opt/egressopt/venv311/bin/python' if cloud=='aws' else '/opt/egressopt/venv/bin/python')} -c "
            +shlex.quote(f'import json; o=json.load(open({out!r})); print("Q03_RPC="+json.dumps(o,separators=(",",":")))'))
    stage=q03_rpc(cloud,script,'stage-'+plan+'-'+part)
    if stage['cloud']!=cloud or stage['part']!=part or stage['trial_id']!=trial_id or stage['plan_id']!=plan:
        raise RuntimeError('Remote Q03 stage identity mismatch')
    if int(stage['parquet_bytes'])<1 or int(stage['rows'])<1:
        raise RuntimeError('Unexpected empty Q03 stage')
    if not re.fullmatch(r'[a-f0-9]{64}',stage['sha256']):raise RuntimeError('Bad remote stage SHA')
    return stage


def q03_upload(source,dest,source_stage,plan,trial_id,peer_ip,peer_ca_b64,token,run_key):
    part=source_stage['part']
    work=f'/opt/egressopt/runtime/q03_{run_key}_{source}'
    ca=work+'/peer-ca.pem'
    out=work+'/upload_response.json'
    source_path=source_stage['file_path']
    if source_path!=work+'/stage/'+trial_id+'/'+part+'.parquet':
        raise RuntimeError('Untrusted remote artifact path')
    url=f'https://{peer_ip}:8443/q03/upload/{trial_id}/{plan}/{part}'
    # The receiver verifies the exact file checksum before accepting the edge.
    # curl elapsed includes TLS, upload, destination write and response.
    script=(f"printf '%s' {shlex.quote(peer_ca_b64)} | base64 -d > {ca}\n"
            f"INFO=$(curl --fail --silent --show-error --connect-timeout 20 --max-time 320 "
            f"--cacert {ca} -H {shlex.quote('Authorization: Bearer '+token)} "
            f"-H {shlex.quote('X-Q03-SHA256: '+source_stage['sha256'])} "
            f"-H 'Content-Type: application/octet-stream' --data-binary @{shlex.quote(source_path)} "
            f"-o {out} -w '%{{time_total}} %{{size_upload}} %{{http_code}}' {url})\n"
            f'export INFO\n'
            f"{('/opt/egressopt/venv311/bin/python' if source=='aws' else '/opt/egressopt/venv/bin/python')} -c "
            +shlex.quote(f'import os,json; t,n,s=os.environ["INFO"].split(); '
                         f'r=json.load(open({out!r})); '
                         'print("Q03_RPC="+json.dumps({"curl_total_seconds":float(t),'
                         '"curl_size_upload":int(n),"http_status":int(s),"received":r},separators=(",",":")))'))
    transport=q03_rpc(source,script,'upload-'+plan+'-'+part)
    received=transport['received']
    if (transport['http_status']!=200 or received['cloud']!=dest or
        received['plan_id']!=plan or received['trial_id']!=trial_id or
        received['part']!=part or received['sha256']!=source_stage['sha256'] or
        int(received['received_payload_bytes'])!=int(source_stage['parquet_bytes']) or
        int(transport['curl_size_upload'])!=int(source_stage['parquet_bytes'])):
        raise RuntimeError('Q03 authenticated HTTPS transfer validation FAILED')
    return {'name':part,'source_cloud':source,'destination_cloud':dest,
            'rows':int(source_stage['rows']),
            'parquet_bytes':int(source_stage['parquet_bytes']),
            'stage_seconds':float(source_stage['stage_seconds']),
            'sha256':source_stage['sha256'],'curl_http_total_seconds':transport['curl_total_seconds'],
            'curl_size_upload':transport['curl_size_upload'],
            'receiver_file_seconds':received['receive_file_seconds']}


def q03_finish(dest,plan,trial_id,token,run_key):
    data=json.dumps({'plan_id':plan,'trial_id':trial_id},separators=(',',':'))
    work=f'/opt/egressopt/runtime/q03_{run_key}_{dest}'
    out=work+'/finish_response.json'
    py='/opt/egressopt/venv311/bin/python' if dest=='aws' else '/opt/egressopt/venv/bin/python'
    script=(f'curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 300 '
            f'-H {shlex.quote("Authorization: Bearer "+token)} -H "Content-Type: application/json" '
            f'--data-binary {shlex.quote(data)} https://127.0.0.1:8443/q03/finish > {out}\n'
            f'{py} -c '
            +shlex.quote(f'import json,gzip,base64; o=json.load(open({out!r})); '
                         'b=gzip.compress(json.dumps(o,separators=(",",":")).encode()); '
                         'print("Q03_RPC_GZIP="+base64.b64encode(b).decode())'))
    stdout=q03_remote(dest,'set -eu\n'+script,'finish-'+plan)
    matched=[line.split('Q03_RPC_GZIP=',1)[1] for line in stdout.splitlines()
             if line.startswith('Q03_RPC_GZIP=')]
    if len(matched)!=1:raise RuntimeError('Compressed Q03 finish response missing: '+stdout[-300:])
    result=json.loads(gzip.decompress(base64.b64decode(matched[0],validate=True)))
    if result['cloud']!=dest or result['plan_id']!=plan or result['trial_id']!=trial_id:
        raise RuntimeError('Q03 finish identity mismatch')
    plain=gzip.decompress(base64.b64decode(result.pop('explain_gzip_b64'),validate=True)).decode()
    if digest_bytes(plain.encode())!=result.pop('physical_plan_sha256'):
        raise RuntimeError('Q03 EXPLAIN artifact hash mismatch')
    result['explain']=plain
    return result


def q03_one_cloud_trial(plan,trial_id,token,run_key,ips,cas):
    edges=[]
    if plan=='aws_join':
        for part in ('customer','lineitem'):
            stage=q03_stage('azure',part,plan,trial_id,token,run_key)
            edges.append(q03_upload('azure','aws',stage,plan,trial_id,ips['aws'],cas['aws'],token,run_key))
        dest='aws'
    elif plan=='azure_join':
        stage=q03_stage('aws','orders',plan,trial_id,token,run_key)
        edges.append(q03_upload('aws','azure',stage,plan,trial_id,ips['azure'],cas['azure'],token,run_key))
        dest='azure'
    elif plan=='semijoin_azure':
        keys=q03_stage('azure','customer_keys',plan,trial_id,token,run_key)
        edges.append(q03_upload('azure','aws',keys,plan,trial_id,ips['aws'],cas['aws'],token,run_key))
        reduced=q03_stage('aws','orders_reduced',plan,trial_id,token,run_key)
        edges.append(q03_upload('aws','azure',reduced,plan,trial_id,ips['azure'],cas['azure'],token,run_key))
        dest='azure'
    else:raise ValueError(plan)
    finish=q03_finish(dest,plan,trial_id,token,run_key)
    return edges,finish,dest


def q03_cloud(args):
    if args.sf!=1:raise RuntimeError('Only verified SF1 is permitted for Q03')
    if args.trials>100 or args.warmup_trials>20:raise ValueError('Trial limit exceeded')
    # Re-evaluate the original SQL locally before sending authenticated remote
    # workloads; do not silently trust an old reference from another parameter.
    import duckdb
    paths={t:args.data_dir/(t+'.parquet') for t in TABLE_PLACEMENT}
    for path in [args.query_file,*paths.values()]:
        if not path.is_file():raise FileNotFoundError(str(path))
    sql=args.query_file.read_text(encoding='utf-8')
    params=load_params(sql)
    with duckdb.connect(':memory:') as con:
        reference=reference_rows(con,paths,sql)
    reference_info=require_reference(reference,args.reference_file,sha256(args.query_file))
    expected_hash=canonical_hash(reference)
    run_key=uuid.uuid4().hex[:8]
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+run_key
    directory=root(args)/run_id
    directory.mkdir(parents=True,exist_ok=False)
    report={'schema_version':2,'query_id':QUERY,'run_id':run_id,'status':'running',
            'execution_mode':'real_multicloud','scale_factor':'SF1','placement_id':PLACEMENT,
            'created_at_utc':now(),'trial_count_per_plan':args.trials,
            'warmup_trials_per_plan':args.warmup_trials,'plan_order':'round-robin rotation of three plans',
            'round_execution_order':[],
            'reference_sha256':expected_hash,'reference_row_count':len(reference),
            'reference_origin':reference_info.get('reference_origin'),
            'query_parameters':params,
            'source_sql':{'path':str(args.query_file),'sha256':sha256(args.query_file)},
            'runner_sha256':sha256(Path(__file__)),
            'worker_sha256':digest_bytes(Q03_WORKER_SOURCE.encode()),
            'datasets_local':{t:{'cloud':TABLE_PLACEMENT[t],'sha256':sha256(path),
                         'bytes':path.stat().st_size} for t,path in paths.items()},
            'timing_definition':'sum of remote stage COPY wall times + authenticated HTTPS curl wall times (TLS, network, receiver write, response) + destination join/aggregate wall time; cloud-management orchestration and VM startup excluded',
            'cost_status':'NOT_PRICED','egress_usd_estimated':None,
            'network_seconds_measured':'NOT isolated from HTTP, recorded per-edge HTTP time',
            'network_bytes_measured':'HTTP request body bytes, NOT provider billed bytes',
            'cleanup':{},'temporary_network_rule_ids':{},'reference_rows_file':'q03_reference_rows.json',
            'security_note':'Ephemeral bearer tokens are transmitted in authorized VM Run Command/SSM history; never stored in experiment output.'}
    save_json(directory/'q03_reference_rows.json',reference)
    save_json(directory/'run.json',report)
    changed={'aws_started':False,'azure_started':False,'aws_service':False,'azure_service':False,
             'aws_rule':None,'azure_rule':None,'aws_sg':None}
    measured=[];warmups=[]
    try:
        for cloud in ('aws','azure'):
            print('Checking '+cloud.upper()+' VM...',flush=True)
            changed[cloud+'_started']=q03_start_vm(cloud)
            print('  '+cloud.upper()+' running',flush=True)
        inst,state=q03_pystate()
        if inst['State']['Name']!='running' or state!='PowerState/running':
            raise RuntimeError('Two cloud VMs did not reach running state')
        ips={'aws':q03_global_ip(inst['PublicIpAddress']),
             'azure':q03_global_ip(json.loads(q03_cli(['az','network','public-ip','show',
                      '--resource-group',AZURE_RESOURCE_GROUP,'--name',AZURE_PUBLIC_IP_RESOURCE,
                      '--output','json']))['ipAddress'])}
        report['assigned_public_ips']=ips
        outbound={cloud:q03_source_ip(cloud) for cloud in ('aws','azure')}
        report['verified_outbound_public_ips']=outbound
        sg_ids=[x['GroupId'] for x in inst['SecurityGroups']]
        if len(sg_ids)!=1:raise RuntimeError('Expected exactly one research AWS security group')
        changed['aws_sg']=sg_ids[0]
        report['cloud_instances']={
            'aws':{'instance':AWS_INSTANCE,'region':AWS_REGION,'type':inst['InstanceType']},
            'azure':json.loads(q03_cli(['az','vm','show','--resource-group',AZURE_RESOURCE_GROUP,
                '--name',AZURE_VM,'--query','{location:location,size:hardwareProfile.vmSize}',
                '--output','json']))}
        report['cloud_datasets']={}
        for cloud in ('aws','azure'):
            release=q03_install_worker(cloud,run_key,Q03_WORKER_SOURCE)
            python='/opt/egressopt/venv311/bin/python' if cloud=='aws' else '/opt/egressopt/venv/bin/python'
            tables=('orders',) if cloud=='aws' else ('customer','lineitem')
            cmd='\n'.join(f"sha256sum /opt/egressopt/data/tpch_sf1/{table}.parquet" for table in tables)
            remote=q03_remote(cloud,cmd,'cloud-dataset-hashes')
            hashes={}
            for line in remote.splitlines():
                parts=line.split()
                if len(parts)==2 and re.fullmatch('[a-f0-9]{64}',parts[0]):
                    hashes[Path(parts[1]).stem]=parts[0]
            if set(hashes)!=set(tables):raise RuntimeError('Unable to verify '+cloud+' remote table hashes')
            for table in tables:
                if hashes[table]!=report['datasets_local'][table]['sha256']:
                    raise RuntimeError(f'{cloud} dataset {table} differs from local SF1 reference')
            report['cloud_datasets'][cloud]=hashes
            report.setdefault('worker_releases',{})[cloud]=release
            save_json(directory/'run.json',report)
        token=secrets.token_urlsafe(48)
        cas={}
        for cloud in ('aws','azure'):
            changed[cloud+'_service']=True
            cas[cloud]=q03_start_service(cloud,run_key,report['worker_releases'][cloud],
                                         ips[cloud],token,params)
            print('  '+cloud.upper()+' authenticated Q03 TLS worker ready',flush=True)
        changed['aws_rule']=q03_open_ingress('aws',outbound['azure'],run_key,changed['aws_sg'])
        report['temporary_network_rule_ids']['aws']=changed['aws_rule']
        changed['azure_rule']=q03_open_ingress('azure',outbound['aws'],run_key,changed['aws_sg'])
        report['temporary_network_rule_ids']['azure']=changed['azure_rule']
        save_json(directory/'run.json',report)
        print('  Temporary source-restricted ingress rules active in both directions',flush=True)
        rounds=args.warmup_trials+args.trials
        for i in range(rounds):
            order=PLANS[i%len(PLANS):]+PLANS[:i%len(PLANS)]
            warm=i<args.warmup_trials
            trial=(i+1 if warm else i-args.warmup_trials+1)
            trial_id=('w' if warm else 'm')+f'{trial:02d}'
            report['round_execution_order'].append({'round':i+1,'trial_id':trial_id,
                                                    'warmup':warm,'plans':list(order)})
            for plan in order:
                edges,finish,dest=q03_one_cloud_trial(plan,trial_id,token,run_key,ips,cas)
                result=finish['result']
                if result!=reference or finish['result_sha256']!=expected_hash or len(result)!=len(reference):
                    raise RuntimeError(f'Cloud Q03 result mismatch, {plan} {trial_id}')
                if len(edges)!=(1 if plan=='azure_join' else 2):
                    raise RuntimeError('Q03 transfer edge count unexpectedly changed')
                transferred=sum(int(x['curl_size_upload']) for x in edges)
                duration=sum(float(e['stage_seconds'])+float(e['curl_http_total_seconds']) for e in edges)+float(finish['join_seconds'])
                record={'run_id':run_id,'query_id':QUERY,'scale_factor':'SF1',
                    'placement_id':PLACEMENT,'execution_mode':'real_multicloud',
                    'plan_id':plan,'trial':trial,'join_cloud':dest,
                    'source_clouds':'azure' if plan=='aws_join' else ('aws' if plan=='azure_join' else 'azure+aws'),
                    'transfer_edges':len(edges),'intermediate_rows':sum(e['rows'] for e in edges),
                    'intermediate_parquet_bytes':sum(e['parquet_bytes'] for e in edges),
                    'stage_seconds':sum(e['stage_seconds'] for e in edges),
                    'join_aggregate_seconds':float(finish['join_seconds']),
                    'total_seconds':duration,'result_row_count':len(result),
                    'result_sha256':finish['result_sha256'],'correct_vs_reference':True,
                    'network_bytes_measured':transferred,'network_seconds_measured':'',
                    'egress_usd_estimated':'','timestamp_utc':now()}
                explain_file=(f'warmup_{trial:02d}_{plan}_physical_plan.txt' if warm else
                              f'trial_{trial:02d}_{plan}_physical_plan.txt')
                (directory/explain_file).write_text(finish['explain']+'\n',encoding='utf-8')
                trial_obj={'schema_version':2,'trial':trial,'plan_id':plan,'metrics':record,
                    'transfer_edges':edges,'result':result,'plan_explain_file':explain_file,
                    'plan_explain_sha256':sha256(directory/explain_file),
                    'source_to_destination':'two-hop' if plan=='semijoin_azure' else 'direct',
                    'timing_note':report['timing_definition']}
                if warm:
                    warmups.append({'warmup':trial,'plan_id':plan,'total_seconds':duration,
                                    'intermediate_parquet_bytes':transferred,'correct_vs_reference':True})
                    save_json(directory/f'warmup_{trial:02d}_{plan}.json',trial_obj)
                    with (directory/'warmup_metrics.csv').open('w',newline='') as f:
                        w=csv.DictWriter(f,fieldnames=list(warmups[0]));w.writeheader();w.writerows(warmups)
                else:
                    measured.append(record)
                    save_json(directory/f'trial_{trial:02d}_{plan}.json',trial_obj)
                    write_metrics(directory/'trial_metrics.csv',measured)
                label=f'WARMUP={trial}' if warm else f'trial={trial}'
                print(f'{plan:16s} {label} correctness=PASS real_HTTP_bytes={transferred:,} '
                      f'stage+HTTPS+join={duration:.3f}s',flush=True)
        report.update({'status':'success','completed_at_utc':now(),
                       'trial_metrics_sha256':sha256(directory/'trial_metrics.csv'),
                       'trial_metrics_csv':'trial_metrics.csv',
                       'warmup_count':len(warmups),'measured_count':len(measured)})
        print('Q03_REAL_MULTICLOUD_PASS',flush=True)
        print('Q03 cloud run: '+str(directory),flush=True)
        return 0
    except Exception as exc:
        report.update({'status':'failed','error':type(exc).__name__+': '+str(exc),'completed_at_utc':now()})
        raise
    finally:
        # Stop listeners before revoking ingress, and preserve detailed cleanup evidence.
        for cloud in ('aws','azure'):
            if changed[cloud+'_service']:
                try:
                    q03_remote(cloud,f'systemctl stop egressopt-q03-{run_key}-{cloud} >/dev/null 2>&1 || true; '
                        f'rm -rf /opt/egressopt/runtime/q03_{run_key}_{cloud}; echo Q03_RUNTIME_REMOVED',
                        'stop-Q03-service')
                    report['cleanup'][cloud+'_runtime']='removed'
                except Exception as exc:
                    report['cleanup'][cloud+'_runtime']='FAILED: '+str(exc)
        if changed['aws_rule']:
            try:
                q03_aws('ec2','revoke-security-group-ingress','--group-id',changed['aws_sg'],
                         '--security-group-rule-ids',changed['aws_rule'])
                report['cleanup']['aws_ingress']='removed'
            except Exception as exc:report['cleanup']['aws_ingress']='FAILED: '+str(exc)
        if changed['azure_rule']:
            try:
                q03_cli(['az','network','nsg','rule','delete','--resource-group',AZURE_RESOURCE_GROUP,
                         '--nsg-name',AZURE_NSG,'--name',changed['azure_rule'],'--output','none'],timeout=300)
                report['cleanup']['azure_ingress']='removed'
            except Exception as exc:report['cleanup']['azure_ingress']='FAILED: '+str(exc)
        # Query command defaults to deallocating/stopping both research VMs.
        if args.stop_all_after_run:
            try:
                q03_cli(['aws','ec2','stop-instances','--instance-ids',AWS_INSTANCE,
                         '--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json'])
                q03_cli(['aws','ec2','wait','instance-stopped','--instance-ids',AWS_INSTANCE,
                         '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=650)
                report['cleanup']['aws_vm']='stopped_verified'
            except Exception as exc:report['cleanup']['aws_vm']='FAILED: '+str(exc)
            try:
                q03_cli(['az','vm','deallocate','--resource-group',AZURE_RESOURCE_GROUP,
                         '--name',AZURE_VM,'--output','none'],timeout=650)
                _,az_state=q03_pystate()
                if az_state!='PowerState/deallocated':
                    raise RuntimeError('Azure state after deallocation: '+az_state)
                report['cleanup']['azure_vm']='deallocated_verified'
            except Exception as exc:report['cleanup']['azure_vm']='FAILED: '+str(exc)
        else:
            for cloud in ('aws','azure'):
                if changed[cloud+'_started']:
                    try:
                        if cloud=='aws':
                            q03_cli(['aws','ec2','stop-instances','--instance-ids',AWS_INSTANCE,
                                     '--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json'])
                        else:q03_cli(['az','vm','deallocate','--resource-group',AZURE_RESOURCE_GROUP,
                                      '--name',AZURE_VM,'--output','none'],timeout=650)
                        report['cleanup'][cloud+'_vm']='stopped_or_deallocated'
                    except Exception as exc:report['cleanup'][cloud+'_vm']='FAILED: '+str(exc)
        report['cleanup_incomplete']=any(str(v).startswith('FAILED') for v in report['cleanup'].values())
        save_json(directory/'run.json',report)
        if report['cleanup_incomplete']:
            print('WARNING: Q03 cloud cleanup incomplete; inspect run.json',file=sys.stderr)
        print('Q03 experiment report: '+str(directory),flush=True)



def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', required=True, choices=('cloud', 'reproduce', 'audit', 'prepare'))
    p.add_argument('--sf', type=int, default=DEFAULT_SF)
    p.add_argument('--trials', type=int, default=10)
    p.add_argument('--warmup-trials', type=int, default=2)
    p.add_argument('--stop-all-after-run', action=argparse.BooleanOptionalAction, default=True)
    p.add_argument('--threads', type=int, default=4)
    p.add_argument('--run-id', default=None)
    p.add_argument('--query-file', type=Path, default=Path('queries/tpch/q03.sql'))
    p.add_argument('--data-dir', type=Path, default=None)
    p.add_argument('--reference-file', type=Path, default=None)
    p.add_argument('--output-dir', type=Path, default=Path('results/experiments'))
    p.add_argument('--plots', action=argparse.BooleanOptionalAction, default=True)
    args = p.parse_args()
    if args.sf not in VALIDATED_SCALE_FACTORS:
        p.error('Only validated SF1 is enabled. Do not relabel SF1 as SF10/SF100.')
    if args.trials < 1 or args.threads < 1 or args.warmup_trials < 0:
        p.error('--trials and --threads must be positive')
    if args.data_dir is None:
        args.data_dir = Path(f'data/tpch/parquet_sf{args.sf}')
    if args.reference_file is None:
        args.reference_file = Path(f'results/reference/sf{args.sf}/q03_results.json')
    if args.mode == 'cloud':
        return q03_cloud(args)
    if args.mode == 'prepare':
        return prepare(args)
    if args.mode == 'audit':
        audit(args)
        return 0
    return reproduce(args)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, FileNotFoundError, InvalidOperation) as exc:
        print(f'Q03 ERROR: {exc}', file=sys.stderr)
        sys.exit(2)
