#!/usr/bin/env python3
"""EgressOpt Q14 — standalone Promotion Effect research runner, SF1/P1.

One query / one independent Python file; no imports from any other EgressOpt
query file. P1: lineitem on Azure (eastus), part on GCP (us-east4).

    python experiments/q14.py --mode prepare --trials 1
    python experiments/q14.py --mode cloud --trials 10
    python experiments/q14.py --mode reproduce
    python experiments/q14.py --mode audit

Current release: local prepare, real Azure-GCP cloud, offline reproduce and
offline audit. Cloud runner starts/deallocates the specified VMs, deploys
temporary embedded TLS workers, and restricts TCP 8443 ingress to verified
peer public IPv4 /32 addresses. A fresh-cloud run remains unverified.
The first real-cloud checkpoint should use --trials 1 --warmup-trials 0.

Requires duckdb and (for plots) matplotlib in the active Python environment.
TPC-H-derived workload; not an audited TPC-H result.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
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

QUERY = 'Q14'
PLACEMENT = 'P1'
DEFAULT_SF = int(os.environ.get('EGRESSOPT_SF', '1'))
VALIDATED_SCALE_FACTORS = (1,)
PLANS = ('gcp_join', 'azure_join', 'partial_aggregate_gcp', 'promo_keys_azure')
TABLE_PLACEMENT = {'lineitem': 'azure', 'part': 'gcp'}
EXPECTED_EDGES = {
    'gcp_join': [('azure', 'gcp')],
    'azure_join': [('gcp', 'azure')],
    'partial_aggregate_gcp': [('azure', 'gcp')],
    'promo_keys_azure': [('gcp', 'azure')],
}
FIELDS = ('run_id','query_id','scale_factor','placement_id','execution_mode',
          'plan_id','trial','join_cloud','source_clouds','transfer_edges',
          'intermediate_rows','intermediate_parquet_bytes','stage_seconds',
          'join_aggregate_seconds','total_seconds','result_row_count',
          'result_sha256','correct_vs_reference','network_bytes_measured',
          'network_seconds_measured','egress_usd_estimated','timestamp_utc')
SIX_PLACES = Decimal('0.000001')


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256(path: Path) -> str:
    hash_obj = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def literal(value: Path) -> str:
    return "'" + str(value.resolve()).replace("'", "''") + "'"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def scan(path: Path) -> str:
    return f'read_parquet({literal(path)})'


def canonical(rows) -> list[dict]:
    """Stable canonical Q14 representation; six-decimal output precision.

    Decimal aggregation is checked against the source SQL before rounding.
    The six-place convention belongs to this research artifact and is not
    a substitute for preserving the unrounded DuckDB result in raw evidence.
    """
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError('Q14 expected exactly one row and one column')
    raw = rows[0][0]
    if raw is None:
        return [{'promo_revenue': None}]
    value = Decimal(str(raw))
    if not value.is_finite():
        raise ValueError('Non-finite Q14 percentage')
    return [{'promo_revenue': format(value.quantize(SIX_PLACES, rounding=ROUND_HALF_EVEN), '.6f')}]


def canonical_hash(rows: list[dict]) -> str:
    return digest_bytes(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode())


def _strip_sql_comments(sql: str) -> str:
    return re.sub(r'/\*.*?\*/', ' ', re.sub(r'--[^\n]*', ' ', sql), flags=re.S)


def _date_from_match(expr: str):
    match = re.search(r"(?:DATE\s*)?'(\d{4}-\d{2}-\d{2})'", expr, re.I)
    return date.fromisoformat(match.group(1)) if match else None


def load_params(sql: str) -> dict:
    """Fail closed if Q14's archived source SQL is not the supported shape.

    Supports the standard TPC-H one-month interval and an explicit upper
    DATE in pre-rendered queries; does not silently replace query parameters.
    """
    source = _strip_sql_comments(sql)
    normalized = re.sub(r'\s+', ' ', source).lower()
    required = ('promo_revenue','l_extendedprice','l_discount','l_shipdate',
                'l_partkey','p_partkey','p_type','case','when','sum','like')
    if any(token not in normalized for token in required):
        raise ValueError('Q14 SQL does not match expected promotion-revenue shape')
    if not re.search(r"p_type\s+like\s*'promo%'", normalized):
        raise ValueError('Q14 promo predicate not verified')
    if not re.search(r'\bl_partkey\s*=\s*p_partkey\b|\bp_partkey\s*=\s*l_partkey\b', normalized):
        raise ValueError('Q14 part/lineitem equality join not verified')
    if not re.search(r'\bl_shipdate\s*>=', normalized) or not re.search(r'\bl_shipdate\s*<(?!=)', normalized):
        raise ValueError('Q14 date selection not verified')
    # Q14 lower bound (works with DATE '...', CAST('...' AS DATE), bare '...').
    lower_segment = re.search(r'\bl_shipdate\s*>=\s*(.*?)(?=\band\s+l_shipdate\s*<|;|$)',
                              source, flags=re.I | re.S)
    upper_segment = re.search(r'\bl_shipdate\s*<\s*(.*?)(?=\band\b|;|$)',
                              source, flags=re.I | re.S)
    if not lower_segment or not upper_segment:
        raise ValueError('Could not extract Q14 start/end predicates')
    lower = _date_from_match(lower_segment.group(1))
    upper_base = _date_from_match(upper_segment.group(1))
    if not lower or not upper_base:
        raise ValueError('Q14 dates must be rendered ISO date literals')
    if lower.day != 1:
        raise ValueError('Q14 standard qualification date must start at first day of a month')
    next_month = date(lower.year + (lower.month == 12), lower.month % 12 + 1, 1)
    upper_expr = upper_segment.group(1).lower()
    has_interval = bool(re.search(r"(?:interval\s*'\s*1\s*'\s*month|interval\s*'\s*1\s*month\s*')", upper_expr))
    if has_interval:
        if upper_base != lower:
            raise ValueError('Q14 interval upper bound must use the same start date')
        upper = next_month
    else:
        upper = upper_base
    if upper != next_month:
        raise ValueError(f'Q14 expected a one-month interval, found {lower} → {upper}')
    return {'start_date': str(lower), 'end_date_exclusive': str(upper),
            'promo_prefix': 'PROMO%'}


def reference_rows(con, paths: dict, sql: str) -> list[dict]:
    for table in TABLE_PLACEMENT:
        con.execute(f'CREATE VIEW {table} AS SELECT * FROM {scan(paths[table])}')
    try:
        return canonical(con.execute(sql).fetchall())
    finally:
        for table in TABLE_PLACEMENT:
            con.execute(f'DROP VIEW {table}')


def fragments(paths, params: dict) -> dict[str, str]:
    l = scan(paths['lineitem'])
    p = scan(paths['part'])
    begin = sql_string(params['start_date'])
    end = sql_string(params['end_date_exclusive'])
    selected = (f'SELECT l_partkey, l_extendedprice, l_discount FROM {l} '
                f'WHERE l_shipdate >= CAST({begin} AS DATE) '
                f'AND l_shipdate < CAST({end} AS DATE)')
    return {
        'lineitem_selected': selected,
        'lineitem_partial': (f'SELECT l_partkey, '
                            f'SUM(l_extendedprice * (1 - l_discount)) AS revenue '
                            f'FROM ({selected}) GROUP BY l_partkey'),
        'part_attributes': f'SELECT p_partkey, p_type FROM {p}',
        'promo_keys': f"SELECT p_partkey FROM {p} WHERE p_type LIKE 'PROMO%'",
    }


def q14_join_sql(line_sql: str, part_sql: str, *, partial=False) -> str:
    """Deterministic relational plan; DuckDB may still reorder physical joins."""
    if partial:
        measure = 'l.revenue'
    else:
        measure = 'l.l_extendedprice * (1 - l.l_discount)'
    return (f"SELECT 100.00 * SUM(CASE WHEN p.p_type LIKE 'PROMO%' "
            f"THEN ({measure}) ELSE 0 END) / SUM({measure}) AS promo_revenue "
            f'FROM ({line_sql}) AS l JOIN ({part_sql}) AS p '
            'ON l.l_partkey = p.p_partkey')


def q14_promo_keys_sql(line_sql: str, keys_sql: str) -> str:
    return (f"WITH l AS ({line_sql}), promo AS ("
            f"SELECT l.* FROM l SEMI JOIN ({keys_sql}) AS p "
            f"ON l.l_partkey = p.p_partkey) "
            f"SELECT 100.00 * (SELECT COALESCE(SUM(l_extendedprice * (1-l_discount)), 0) FROM promo) / "
            f"(SELECT SUM(l_extendedprice * (1-l_discount)) FROM l) AS promo_revenue")


def check_foreign_keys(con, paths: dict) -> dict:
    """Semijoin Q14 is valid only with unique part keys and complete FK joins."""
    p = scan(paths['part']); l = scan(paths['lineitem'])
    duplicated = con.execute(f'SELECT COUNT(*)-COUNT(DISTINCT p_partkey) FROM {p}').fetchone()[0]
    orphans = con.execute(f'SELECT COUNT(*) FROM {l} AS l ANTI JOIN {p} AS p ON l.l_partkey=p.p_partkey').fetchone()[0]
    if duplicated or orphans:
        raise ValueError('Q14 key-only optimization requires unique part keys and no orphan lineitem rows')
    return {'part_primary_key_duplicate_count': int(duplicated),
            'lineitem_missing_part_count': int(orphans)}


def local_plan(duckdb, paths, params, plan, threads) -> dict:
    """One simulated exchange per candidate; no network activity."""
    con = duckdb.connect(':memory:')
    con.execute(f'SET threads={threads}')
    fs = fragments(paths, params)
    edges = []
    stage_total = 0.0
    start = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix='egressopt_q14_') as temp:
            tmpdir = Path(temp)
            def ship(name, source, destination):
                nonlocal stage_total
                fragment = fs[name]
                out = tmpdir / f'{name}.parquet'
                tick = time.perf_counter()
                con.execute(f'COPY ({fragment}) TO {literal(out)} (FORMAT PARQUET, COMPRESSION ZSTD)')
                elapsed = time.perf_counter()-tick
                stage_total += elapsed
                n = int(con.execute(f'SELECT COUNT(*) FROM {scan(out)}').fetchone()[0])
                edge = {'name': name, 'source_cloud': source, 'destination_cloud': destination,
                        'rows': n, 'parquet_bytes': out.stat().st_size, 'stage_seconds': elapsed,
                        'sha256': sha256(out)}
                edges.append(edge)
                return f'SELECT * FROM {scan(out)}'

            if plan == 'gcp_join':
                l = ship('lineitem_selected', 'azure', 'gcp')
                sql = q14_join_sql(l, fs['part_attributes'])
                dest = 'gcp'
            elif plan == 'azure_join':
                p = ship('part_attributes', 'gcp', 'azure')
                sql = q14_join_sql(fs['lineitem_selected'], p)
                dest = 'azure'
            elif plan == 'partial_aggregate_gcp':
                l = ship('lineitem_partial', 'azure', 'gcp')
                sql = q14_join_sql(l, fs['part_attributes'], partial=True)
                dest = 'gcp'
            elif plan == 'promo_keys_azure':
                p = ship('promo_keys', 'gcp', 'azure')
                sql = q14_promo_keys_sql(fs['lineitem_selected'], p)
                dest = 'azure'
            else:
                raise ValueError('Unknown Q14 plan: ' + plan)
            physical = '\n'.join(str(r[-1]) for r in con.execute('EXPLAIN '+sql).fetchall())
            tick = time.perf_counter()
            raw = con.execute(sql).fetchall()
            result = canonical(raw)
            join_seconds = time.perf_counter()-tick
        return {'plan_id':plan,'join_cloud':dest,'edges':edges,
                'intermediate_rows':sum(e['rows'] for e in edges),
                'intermediate_parquet_bytes':sum(e['parquet_bytes'] for e in edges),
                'stage_seconds':stage_total,'join_aggregate_seconds':join_seconds,
                'total_seconds':time.perf_counter()-start,'result':result,
                'physical_plan':physical,'sql_fragment':sql,
                'raw_result_strings': [[str(v) for v in row] for row in raw]}
    finally:
        con.close()


def root(args) -> Path:
    return args.output_dir/'q14'/f'sf{args.sf}'/'p1'


def choose_run(args) -> Path:
    if args.run_id:
        directory = root(args)/args.run_id
        if not directory.is_dir():
            raise FileNotFoundError(f'No Q14 archive {directory}')
        return directory
    candidates = []
    for path in root(args).glob('*/run.json'):
        try:
            obj = read_json(path)
            if (obj.get('status') == 'success' and obj.get('query_id') == QUERY
                and obj.get('scale_factor') == f'SF{args.sf}'
                and obj.get('placement_id') == PLACEMENT):
                candidates.append(path.parent)
        except (OSError, ValueError):
            continue
    if not candidates:
        raise FileNotFoundError('No completed Q14 evidence. Run --mode prepare first.')
    return sorted(candidates)[-1]


def require_reference(expected, path: Path, sql_hash: str) -> dict:
    if path.exists():
        obj = read_json(path)
        if (obj.get('query_id') != QUERY or obj.get('scale_factor') != 'SF1'
            or obj.get('source_sql_sha256') != sql_hash
            or obj.get('canonical_rows') != expected
            or obj.get('canonical_result_sha256') != canonical_hash(expected)):
            raise RuntimeError('Q14 source SQL/result disagrees with archived canonical reference')
    else:
        # No claim of independent reference unless independently checked.
        obj = {'query_id':QUERY,'scale_factor':'SF1','source_sql_sha256':sql_hash,
               'canonical_rows':expected,'canonical_result_sha256':canonical_hash(expected),
               'reference_origin':'first_local_execution_of_archived_SQL_not_independent'}
        save_json(path,obj)
    return obj


def write_csv(path: Path, rows: list[dict], *, fields=FIELDS):
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def prepare(args) -> int:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError('DuckDB is required: activate the egressopt environment') from exc
    paths = {t:args.data_dir/(t+'.parquet') for t in TABLE_PLACEMENT}
    for path in [args.query_file, *paths.values()]:
        if not path.is_file():
            raise FileNotFoundError(f'Missing Q14 file: {path}')
    sql = args.query_file.read_text(encoding='utf-8')
    params = load_params(sql)
    with duckdb.connect(':memory:') as con:
        con.execute(f'SET threads={args.threads}')
        start=time.perf_counter()
        result = reference_rows(con, paths, sql)
        reference_seconds = time.perf_counter()-start
        keys = check_foreign_keys(con, paths)
    reference = require_reference(result,args.reference_file,sha256(args.query_file))
    run_key = uuid.uuid4().hex[:8]
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+run_key
    directory=root(args)/run_id
    directory.mkdir(parents=True,exist_ok=False)
    report={'schema_version':1,'query_id':QUERY,'run_id':run_id,'status':'running',
            'scale_factor':f'SF{args.sf}','placement_id':PLACEMENT,
            'execution_mode':'local_emulation_NO_NETWORK',
            'created_at_utc':now(),'trial_count_per_plan':args.trials,
            'round_execution_order':[],'query_parameters':params,
            'reference_sha256':canonical_hash(result),'reference_row_count':len(result),
            'reference_origin':reference['reference_origin'],
            'reference_duration_seconds':reference_seconds,
            'key_constraints':keys,
            'source_sql':{'path':str(args.query_file),'sha256':sha256(args.query_file)},
            'reference_file':{'path':str(args.reference_file),'sha256':sha256(args.reference_file)},
            'dataset':{table:{'cloud':TABLE_PLACEMENT[table],'path':str(path),
                              'sha256':sha256(path),'bytes':path.stat().st_size}
                       for table,path in paths.items()},
            'runner_sha256':sha256(Path(__file__)),
            'versions':{'python':platform.python_version(),'duckdb':duckdb.__version__},
            'timing_definition':'local Parquet materialization and local SQL, NOT cloud execution',
            'network_status':'NOT_MEASURED','egress_cost_status':'NOT_ESTIMATED',
            'reference_rows_file':'q14_reference_rows.json'}
    save_json(directory/'q14_reference_rows.json',result)
    save_json(directory/'run.json',report)
    records=[]
    try:
        for trial in range(1,args.trials+1):
            plan_order=PLANS[(trial-1)%len(PLANS):]+PLANS[:(trial-1)%len(PLANS)]
            report['round_execution_order'].append({'trial':trial,'plans':list(plan_order)})
            for plan in plan_order:
                observed=local_plan(duckdb,paths,params,plan,args.threads)
                valid=(observed['result']==result)
                row={'run_id':run_id,'query_id':QUERY,'scale_factor':f'SF{args.sf}',
                     'placement_id':PLACEMENT,'execution_mode':'local_emulation_NO_NETWORK',
                     'plan_id':plan,'trial':trial,'join_cloud':observed['join_cloud'],
                     'source_clouds':EXPECTED_EDGES[plan][0][0],
                     'transfer_edges':len(observed['edges']),
                     'intermediate_rows':observed['intermediate_rows'],
                     'intermediate_parquet_bytes':observed['intermediate_parquet_bytes'],
                     'stage_seconds':observed['stage_seconds'],
                     'join_aggregate_seconds':observed['join_aggregate_seconds'],
                     'total_seconds':observed['total_seconds'],
                     'result_row_count':len(observed['result']),
                     'result_sha256':canonical_hash(observed['result']),
                     'correct_vs_reference':valid,'network_bytes_measured':'',
                     'network_seconds_measured':'','egress_usd_estimated':'',
                     'timestamp_utc':now()}
                records.append(row)
                explain=directory/f'trial_{trial:02d}_{plan}_physical_plan.txt'
                explain.write_text(observed['physical_plan']+'\n',encoding='utf-8')
                save_json(directory/f'trial_{trial:02d}_{plan}.json',
                          {'schema_version':1,'run_id':run_id,'trial':trial,'plan_id':plan,
                           'metrics':row,'result':observed['result'],
                           'raw_result_strings':observed['raw_result_strings'],
                           'transfer_edges':observed['edges'],
                           'plan_explain_file':explain.name,
                           'plan_explain_sha256':sha256(explain),
                           'sql_fragment':observed['sql_fragment']})
                write_csv(directory/'trial_metrics.csv',records)
                print(f'{plan:23s} trial={trial} correctness={"PASS" if valid else "FAIL"} '
                      f'emulated_intermediate={observed["intermediate_parquet_bytes"]:,} bytes '
                      f'local_workflow={observed["total_seconds"]:.3f}s',flush=True)
                if not valid:
                    raise RuntimeError(f'Q14 plan {plan} disagrees with archived SQL')
        report.update({'status':'success','completed_at_utc':now(),
                       'trial_metrics_csv':'trial_metrics.csv',
                       'trial_metrics_sha256':sha256(directory/'trial_metrics.csv'),
                       'measured_trial_count':len(records)})
        save_json(directory/'run.json',report)
        print('Q14_LOCAL_PLAN_VALIDATION_PASS')
        print('Q14 local evidence: '+str(directory))
        print('NOTE: NO inter-cloud traffic, measured network duration or egress charge.')
        return 0
    except Exception as exc:
        report.update({'status':'failed','error':f'{type(exc).__name__}: {exc}',
                       'completed_at_utc':now()})
        save_json(directory/'run.json',report)
        raise


def audit(args, *, quiet=False):
    directory=choose_run(args)
    report=read_json(directory/'run.json')
    errors=[]
    path=directory/'trial_metrics.csv'
    if not path.is_file():
        raise FileNotFoundError(str(path))
    if sha256(path)!=report.get('trial_metrics_sha256'):
        errors.append('Trial CSV differs from archived SHA256')
    if (report.get('query_id')!=QUERY or report.get('scale_factor')!=f'SF{args.sf}'
        or report.get('placement_id')!=PLACEMENT or report.get('status')!='success'):
        errors.append('Run identity or status invalid')
    with path.open(newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        if not set(FIELDS).issubset(reader.fieldnames or []):
            raise ValueError('Missing required metrics columns')
        records=list(reader)
    ref=read_json(directory/'q14_reference_rows.json')
    hash_ref=canonical_hash(ref)
    if hash_ref!=report.get('reference_sha256') or len(ref)!=report.get('reference_row_count'):
        errors.append('Reference JSON inconsistent with manifest')
    observed={}
    for row in records:
        try:
            key=(row['plan_id'],int(row['trial']))
            observed[key]=observed.get(key,0)+1
            if (row['query_id']!=QUERY or row['run_id']!=report['run_id']
                or row['scale_factor']!=f'SF{args.sf}' or row['placement_id']!=PLACEMENT):
                errors.append(f'Bad row identity: {key}')
            if row['plan_id'] not in PLANS:
                errors.append('Unknown plan: '+row['plan_id'])
                continue
            if row['correct_vs_reference']!='True' or row['result_sha256']!=hash_ref:
                errors.append(f'Incorrect Q14 result: {key}')
            record=read_json(directory/f'trial_{int(row["trial"]):02d}_{row["plan_id"]}.json')
            if record['result']!=ref or canonical_hash(record['result'])!=row['result_sha256']:
                errors.append(f'Trial JSON result mismatch: {key}')
            if any(str(record['metrics'][field])!=row[field] for field in FIELDS):
                errors.append(f'Trial CSV/JSON mismatch: {key}')
            edges=record['transfer_edges']
            if len(edges)!=1 or int(row['transfer_edges'])!=1:
                errors.append(f'Q14 requires one exchange edge: {key}')
            if [(e['source_cloud'],e['destination_cloud']) for e in edges]!=EXPECTED_EDGES[row['plan_id']]:
                errors.append(f'Incorrect transfer direction: {key}')
            if sum(int(e['parquet_bytes']) for e in edges)!=int(row['intermediate_parquet_bytes']):
                errors.append(f'Transfer bytes mismatch: {key}')
            if sum(int(e['rows']) for e in edges)!=int(row['intermediate_rows']):
                errors.append(f'Transfer rows mismatch: {key}')
            if any(int(e['parquet_bytes'])<=0 or not re.fullmatch('[0-9a-f]{64}',e['sha256']) for e in edges):
                errors.append(f'Invalid transfer hash or size: {key}')
            physical=directory/record['plan_explain_file']
            if sha256(physical)!=record['plan_explain_sha256']:
                errors.append(f'Physical plan text modified: {key}')
            if any(float(row[f])<0 for f in ('total_seconds','stage_seconds','join_aggregate_seconds')):
                errors.append(f'Negative duration: {key}')
            if row['execution_mode'] != report.get('execution_mode', 'local_emulation_NO_NETWORK'):
                errors.append(f'Execution mode disagreement: {key}')
            if row['execution_mode']=='local_emulation_NO_NETWORK':
                if any(row[k] for k in ('network_bytes_measured','network_seconds_measured','egress_usd_estimated')):
                    errors.append(f'False cloud metric in local run: {key}')
            elif row['execution_mode']=='real_multicloud':
                edge=edges[0]
                if row['join_cloud']!=edge['destination_cloud'] or row['source_clouds']!=edge['source_cloud']:
                    errors.append(f'Cloud placement mismatch: {key}')
                if int(row['network_bytes_measured'])!=int(edge['curl_size_upload']):
                    errors.append(f'Actual HTTP payload differs from logged body bytes: {key}')
                if int(edge['curl_size_upload'])!=int(edge['parquet_bytes']):
                    errors.append(f'Actual HTTPS upload differs from staged Parquet length: {key}')
                if float(edge['curl_http_total_seconds'])<0 or float(edge['receiver_file_seconds'])<0:
                    errors.append(f'Negative transport timing: {key}')
                expected_duration=(float(edge['stage_seconds'])+float(edge['curl_http_total_seconds'])+
                    float(row['join_aggregate_seconds']))
                if abs(float(row['total_seconds'])-expected_duration)>0.000001:
                    errors.append(f'Transfer+stage+join duration arithmetic disagrees: {key}')
                if row['network_seconds_measured'] or row['egress_usd_estimated']:
                    errors.append(f'Unjustified isolated network time or egress charge: {key}')
            else:
                errors.append(f'Unknown Q14 execution mode: {key}')
        except Exception as exc:
            errors.append(f'Invalid trial: {type(exc).__name__}: {exc}')
    expected={(p,i) for p in PLANS for i in range(1,int(report['trial_count_per_plan'])+1)}
    if (set(observed)!=expected or any(count!=1 for count in observed.values())
        or len(records)!=len(expected)):
        errors.append('Missing or duplicated plan/trials')
    if report.get('execution_mode')=='real_multicloud':
        if report.get('worker_sha256') is None or not report.get('remote_dataset_sha256'):
            errors.append('Cloud provenance or worker digest missing')
        for cloud, data in report.get('remote_dataset_sha256',{}).items():
            for table, h in data.items():
                if table not in report.get('dataset',{}) or h!=report['dataset'][table]['sha256']:
                    errors.append('Remote dataset SHA256 does not match archived local input')
        warm_file=directory/'warmup_metrics.csv'
        expected_warm=int(report.get('warmup_trials_per_plan',0))*len(PLANS)
        if expected_warm:
            if not warm_file.is_file(): errors.append('Warmup CSV missing')
            else:
                with warm_file.open(newline='',encoding='utf-8') as f:
                    w=list(csv.DictReader(f))
                if len(w)!=expected_warm or any(x['correct_vs_reference']!='True' for x in w):
                    errors.append('Warmup evidence count or correctness mismatch')
        if report.get('cleanup_incomplete'):
            errors.append('Cloud cleanup was incomplete; inspect run.json')
    status='PASS' if not errors else 'FAIL'
    analysis=directory/'q14_analysis'
    save_json(analysis/'audit.json',{'audited_at_utc':now(),'status':status,
        'query_id':QUERY,'run_id':report['run_id'],'trial_count':len(records),
        'limitations':['archived evidence consistency only; cannot prove independent fresh-cloud replicability',
                       'HTTPS timing includes receiver write and response; not isolated network latency',
                       'payload body byte counts are not cloud billing records'],
        'errors':errors})
    if not quiet:
        print('Q14_EVIDENCE_AUDIT_'+status)
        print('Evidence: '+str(directory))
        if errors:
            print('First discrepancies: '+'; '.join(errors[:5]),file=sys.stderr)
    if errors:
        raise RuntimeError(f'Q14 archive audit failed: {len(errors)} discrepancies')
    return directory,records


def reproduce(args):
    directory,records=audit(args,quiet=True)
    report=read_json(directory/'run.json')
    cloud=report.get('execution_mode')=='real_multicloud'
    grouped=[]
    for plan in PLANS:
        subset=sorted((r for r in records if r['plan_id']==plan),key=lambda r:int(r['trial']))
        times=[float(r['total_seconds']) for r in subset]
        payload=[int(r['network_bytes_measured'] if cloud else r['intermediate_parquet_bytes']) for r in subset]
        grouped.append({'plan_id':plan,'trial_count':len(times),
            'mean_workflow_seconds':statistics.mean(times),
            'median_workflow_seconds':statistics.median(times),
            'sample_stddev_workflow_seconds':statistics.stdev(times) if len(times)>1 else None,
            'mean_payload_bytes':statistics.mean(payload),
            'execution_mode':'real_multicloud' if cloud else 'local_emulation_NO_NETWORK',
            'all_correct':all(r['correct_vs_reference']=='True' for r in subset)})
    analysis=directory/'q14_analysis'
    save_json(analysis/'summary.json',{'query_id':QUERY,'scale_factor':f'SF{args.sf}',
        'placement_id':PLACEMENT,'run_id':directory.name,
        'source_trial_metrics_sha256':sha256(directory/'trial_metrics.csv'),
        'mode':'REAL_MULTICLOUD' if cloud else 'LOCAL_EMULATION_NOT_REAL_CLOUD',
        'warmups_excluded':True,'optimizer_not_evaluated':True,
        'workflow_time_definition':report.get('timing_definition'),
        'payload_bytes_definition':('authenticated HTTPS body bytes' if cloud else 'locally materialized Parquet bytes'),
        'plans':grouped})
    write_csv(analysis/'summary.csv',grouped,fields=list(grouped[0]))
    label='realcloud' if cloud else 'local'
    fmt='Actual HTTPS payload bytes' if cloud else 'Emulated Parquet bytes'
    latex=[f'% Q14 SF1/P1 {"REAL AZURE-GCP" if cloud else "LOCAL EMULATION"}; exploratory manual plan comparisons',
           r'\begin{tabular}{lrrr}',r'\hline',
           f'Plan & $n$ & Mean workflow (ms) & {fmt} '+'\\\\',r'\hline']
    for item in grouped:
        name=item['plan_id'].replace('_',r'\_')
        latex.append(f'{name} & {item["trial_count"]} & '
                     f'{item["mean_workflow_seconds"]*1000:.2f} & '
                     f'{item["mean_payload_bytes"]:.0f} '+'\\\\')
    latex += [r'\hline',r'\end{tabular}']
    (analysis/f'table_q14_{label}.tex').write_text('\n'.join(latex)+'\n',encoding='utf-8')
    if args.plots:
        try:
            import matplotlib.pyplot as plt
        except ImportError as exc:
            raise RuntimeError('Install matplotlib for Q14 charts') from exc
        labels=[g['plan_id'] for g in grouped]
        def save_figure(fig,name):
            fig.tight_layout()
            fig.savefig(analysis/(name+'.png'),dpi=220)
            fig.savefig(analysis/(name+'.pdf'))
            plt.close(fig)
        fig,ax=plt.subplots(figsize=(9,4.5))
        ax.bar(labels,[g['mean_payload_bytes'] for g in grouped]);ax.tick_params(axis='x',labelrotation=15)
        ax.set_ylabel(fmt);ax.set_title('Q14 SF1/P1 — '+('real Azure↔GCP' if cloud else 'local emulation'))
        save_figure(fig,f'q14_{label}_payload_bytes')
        fig,ax=plt.subplots(figsize=(9,4.5))
        ax.bar(labels,[1000*g['mean_workflow_seconds'] for g in grouped]);ax.tick_params(axis='x',labelrotation=15)
        ax.set_ylabel('Mean stage+HTTPS+join (ms)' if cloud else 'Mean local workflow (ms)')
        ax.set_title('Q14 SF1/P1 — '+('real Azure↔GCP' if cloud else 'local emulation'))
        save_figure(fig,f'q14_{label}_workflow_ms')
        fig,ax=plt.subplots(figsize=(9,4.5))
        for plan in PLANS:
            subset=sorted((r for r in records if r['plan_id']==plan),key=lambda r:int(r['trial']))
            ax.plot([int(r['trial']) for r in subset],
                    [1000*float(r['total_seconds']) for r in subset],marker='o',label=plan)
        ax.set_xlabel('Measured trial index');ax.set_ylabel('Workflow duration (ms)')
        ax.set_title('Q14 SF1/P1 — '+('real Azure↔GCP' if cloud else 'local emulation'))
        ax.legend();save_figure(fig,f'q14_{label}_trial_durations')
    print('Q14_REPRODUCE_PASS')
    print('Reproduced '+('REAL MULTICLOUD' if cloud else 'LOCAL')+' Q14 figures: '+str(analysis))
    return 0


Q14_WORKER_SOURCE = '#!/usr/bin/env python3\n"""Q14 embedded HTTPS worker. Deployed unchanged from q14.py over cloud-management channels."""\nimport argparse\nimport base64\nfrom decimal import Decimal, ROUND_HALF_EVEN\nimport gzip\nimport hashlib\nimport hmac\nfrom http.server import BaseHTTPRequestHandler, ThreadingHTTPServer\nimport json\nimport os\nfrom pathlib import Path\nimport re\nimport ssl\nimport time\nimport duckdb\n\nCLOUD = os.environ[\'Q14_CLOUD\']\nROOT = Path(os.environ[\'Q14_RUNTIME\'])\nDATA = Path(os.environ[\'Q14_DATA_DIR\'])\nTOKEN = os.environ[\'Q14_WORKER_TOKEN\']\nSTART = os.environ[\'Q14_START_DATE\']\nEND = os.environ[\'Q14_END_DATE\']\nPORT = int(os.environ.get(\'Q14_PORT\', \'8443\'))\nPLANS = {\'gcp_join\', \'azure_join\', \'partial_aggregate_gcp\', \'promo_keys_azure\'}\nPARTS = {\'lineitem_selected\', \'lineitem_partial\', \'part_attributes\', \'promo_keys\'}\nALLOWED = {\'azure\': {\'lineitem_selected\',\'lineitem_partial\'},\n           \'gcp\': {\'part_attributes\',\'promo_keys\'}}\nSHA = re.compile(r\'^[a-f0-9]{64}$\')\nSLUG = re.compile(r\'^[a-zA-Z0-9_-]{1,40}$\')\nDATE = re.compile(r\'^\\d{4}-\\d{2}-\\d{2}$\')\nif CLOUD not in ALLOWED or not DATE.fullmatch(START) or not DATE.fullmatch(END):\n    raise RuntimeError(\'Invalid Q14 worker environment\')\n\ndef sql_path(table):\n    if (table == \'lineitem\' and CLOUD != \'azure\') or (table == \'part\' and CLOUD != \'gcp\'):\n        raise RuntimeError(\'Q14 table unavailable on this cloud\')\n    f = DATA / (table + \'.parquet\')\n    if not f.is_file():\n        raise RuntimeError(\'Required Q14 local data missing: \'+str(f))\n    return "\'" + str(f).replace("\'", "\'\'") + "\'"\n\ndef scan(path):\n    return \'read_parquet(\' + "\'" + str(path).replace("\'", "\'\'") + "\'" + \')\'\n\ndef fragment(part):\n    if part not in ALLOWED[CLOUD]:\n        raise RuntimeError(\'Q14 stage source mismatch\')\n    if CLOUD == \'azure\':\n        selected = (f\'SELECT l_partkey,l_extendedprice,l_discount FROM read_parquet({sql_path("lineitem")}) \'\n                    f"WHERE l_shipdate >= DATE \'{START}\' AND l_shipdate < DATE \'{END}\'")\n        if part == \'lineitem_selected\':\n            return selected\n        if part == \'lineitem_partial\':\n            return (\'SELECT l_partkey,SUM(l_extendedprice * (1-l_discount)) AS revenue \'\n                    f\'FROM ({selected}) GROUP BY l_partkey\')\n    if part == \'part_attributes\':\n        return f\'SELECT p_partkey,p_type FROM read_parquet({sql_path("part")})\'\n    if part == \'promo_keys\':\n        return f"SELECT p_partkey FROM read_parquet({sql_path(\'part\')}) WHERE p_type LIKE \'PROMO%\'"\n    raise RuntimeError(\'Unknown Q14 part\')\n\ndef result_sql(trial, plan):\n    rec = ROOT/\'received\'/trial/plan\n    if plan in {\'gcp_join\',\'partial_aggregate_gcp\'} and CLOUD == \'gcp\':\n        portion = \'lineitem_partial\' if plan == \'partial_aggregate_gcp\' else \'lineitem_selected\'\n        transferred = rec/(portion+\'.parquet\')\n        if not transferred.is_file(): raise RuntimeError(\'Q14 transferred lineitem missing\')\n        l = scan(transferred)\n        p = \'(\' + fragment(\'part_attributes\') + \')\'\n        measure = \'l.revenue\' if plan == \'partial_aggregate_gcp\' else \'l.l_extendedprice*(1-l.l_discount)\'\n    elif plan in {\'azure_join\',\'promo_keys_azure\'} and CLOUD == \'azure\':\n        l = \'(\'+fragment(\'lineitem_selected\')+\')\'\n        portion = \'promo_keys\' if plan == \'promo_keys_azure\' else \'part_attributes\'\n        transferred=rec/(portion+\'.parquet\')\n        if not transferred.is_file(): raise RuntimeError(\'Q14 transferred part missing\')\n        if plan==\'promo_keys_azure\':\n            keys=scan(transferred)\n            return (f\'WITH l AS ({fragment("lineitem_selected")}), \'\n                    f\'promo AS (SELECT l.* FROM l SEMI JOIN {keys} AS p ON l.l_partkey=p.p_partkey) \'\n                    \'SELECT 100.00 * \'\n                    \'(SELECT COALESCE(SUM(l_extendedprice*(1-l_discount)),0) FROM promo) / \'\n                    \'(SELECT SUM(l_extendedprice*(1-l_discount)) FROM l) AS promo_revenue\')\n        p=scan(transferred)\n        measure=\'l.l_extendedprice*(1-l.l_discount)\'\n    else:\n        raise RuntimeError(\'Q14 join on wrong provider\')\n    return ("SELECT 100.00 * SUM(CASE WHEN p.p_type LIKE \'PROMO%\' "\n            f\'THEN ({measure}) ELSE 0 END) / SUM({measure}) AS promo_revenue \'\n            f\'FROM {l} AS l JOIN {p} AS p ON l.l_partkey=p.p_partkey\')\n\ndef canonical(rows):\n    if len(rows)!=1 or len(rows[0])!=1: raise RuntimeError(\'Invalid Q14 result cardinality\')\n    raw=rows[0][0]\n    if raw is None: return [{\'promo_revenue\':None}]\n    dec=Decimal(str(raw))\n    if not dec.is_finite(): raise RuntimeError(\'Nonfinite Q14 percentage\')\n    return [{\'promo_revenue\':format(dec.quantize(Decimal(\'0.000001\'),rounding=ROUND_HALF_EVEN),\'.6f\')}]\n\ndef sha(path):\n    h=hashlib.sha256()\n    with path.open(\'rb\') as f:\n        for part in iter(lambda:f.read(1024*1024),b\'\'):h.update(part)\n    return h.hexdigest()\n\nclass Handler(BaseHTTPRequestHandler):\n    server_version=\'Q14ExperimentalWorker/1.0\'\n    def log_message(self,fmt,*args):pass\n    def response(self,code,value):\n        body=json.dumps(value,sort_keys=True,separators=(\',\',\':\')).encode()\n        self.send_response(code)\n        self.send_header(\'Content-Type\',\'application/json\')\n        self.send_header(\'Content-Length\',str(len(body)))\n        self.end_headers();self.wfile.write(body)\n    def auth(self):\n        if not hmac.compare_digest(self.headers.get(\'Authorization\',\'\'),\'Bearer \'+TOKEN):\n            self.response(401,{\'error\':\'unauthorized\'});return False\n        return True\n    def json_body(self):\n        n=int(self.headers.get(\'Content-Length\',\'0\'))\n        if not 1<=n<=2000: raise ValueError(\'Invalid JSON body length\')\n        obj=json.loads(self.rfile.read(n))\n        trial=obj.get(\'trial_id\',\'\');plan=obj.get(\'plan_id\',\'\')\n        if not SLUG.fullmatch(trial) or plan not in PLANS:raise ValueError(\'Invalid identity\')\n        return obj,trial,plan\n    def do_GET(self):\n        if not self.auth():return\n        if self.path==\'/health\':self.response(200,{\'status\':\'ok\',\'cloud\':CLOUD,\'query\':\'Q14\',\'duckdb\':duckdb.__version__})\n        else:self.response(404,{\'error\':\'not found\'})\n    def do_POST(self):\n        if not self.auth():return\n        try:\n            if self.path==\'/q14/stage\':\n                obj,trial,plan=self.json_body()\n                part=obj.get(\'part\',\'\')\n                if part not in PARTS:raise ValueError(\'Unknown part\')\n                expected_parts={\'gcp_join\':\'lineitem_selected\',\n                                \'azure_join\':\'part_attributes\',\n                                \'partial_aggregate_gcp\':\'lineitem_partial\',\n                                \'promo_keys_azure\':\'promo_keys\'}\n                if part!=expected_parts[plan]:raise ValueError(\'Stage does not match plan\')\n                out=ROOT/\'stage\'/trial/(part+\'.parquet\')\n                out.parent.mkdir(parents=True,exist_ok=True)\n                sql=fragment(part)\n                t0=time.perf_counter()\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    con.execute(f\'COPY ({sql}) TO \'+"\'"+str(out).replace("\'","\'\'")+"\'"+\' (FORMAT PARQUET, COMPRESSION ZSTD)\')\n                    rows=con.execute(\'SELECT COUNT(*) FROM \'+scan(out)).fetchone()[0]\n                self.response(200,{\'cloud\':CLOUD,\'part\':part,\'trial_id\':trial,\'plan_id\':plan,\n                    \'stage_seconds\':time.perf_counter()-t0,\'rows\':int(rows),\n                    \'parquet_bytes\':out.stat().st_size,\'sha256\':sha(out),\'file_path\':str(out)})\n            elif self.path.startswith(\'/q14/upload/\'):\n                seg=self.path.split(\'/\')\n                if len(seg)!=6 or seg[1:3]!=[\'q14\',\'upload\']:raise ValueError(\'Invalid path\')\n                trial,plan,part=seg[3:]\n                if not SLUG.fullmatch(trial) or plan not in PLANS or part not in PARTS:raise ValueError(\'Invalid upload path\')\n                expected_parts={\'gcp_join\':\'lineitem_selected\',\'azure_join\':\'part_attributes\',\n                                \'partial_aggregate_gcp\':\'lineitem_partial\',\'promo_keys_azure\':\'promo_keys\'}\n                if part!=expected_parts[plan]:raise ValueError(\'Wrong upload part\')\n                expected=self.headers.get(\'X-Q14-SHA256\',\'\')\n                if not SHA.fullmatch(expected):raise ValueError(\'Missing upload SHA\')\n                remaining=int(self.headers.get(\'Content-Length\',\'0\'))\n                if not 1<=remaining<=300_000_000:raise ValueError(\'Invalid upload bytes\')\n                path=ROOT/\'received\'/trial/plan/(part+\'.parquet\')\n                path.parent.mkdir(parents=True,exist_ok=True)\n                started=time.perf_counter();h=hashlib.sha256();received=0\n                tmp=path.with_suffix(\'.part\')\n                with tmp.open(\'wb\') as f:\n                    while remaining:\n                        data=self.rfile.read(min(1024*1024,remaining))\n                        if not data:raise ValueError(\'Truncated body\')\n                        f.write(data);h.update(data);received+=len(data);remaining-=len(data)\n                if h.hexdigest()!=expected:\n                    tmp.unlink(missing_ok=True);raise ValueError(\'Upload SHA mismatch\')\n                tmp.replace(path)\n                self.response(200,{\'cloud\':CLOUD,\'part\':part,\'trial_id\':trial,\'plan_id\':plan,\n                  \'sha256\':h.hexdigest(),\'received_payload_bytes\':received,\n                  \'receive_file_seconds\':time.perf_counter()-started})\n            elif self.path==\'/q14/finish\':\n                obj,trial,plan=self.json_body()\n                sql=result_sql(trial,plan)\n                with duckdb.connect(\':memory:\') as con:\n                    con.execute(\'SET threads=4\')\n                    physical=\'\\n\'.join(str(r[-1]) for r in con.execute(\'EXPLAIN \'+sql).fetchall())\n                    t0=time.perf_counter()\n                    raw=con.execute(sql).fetchall()\n                    elapsed=time.perf_counter()-t0\n                rows=canonical(raw)\n                encoded=json.dumps(rows,sort_keys=True,separators=(\',\',\':\')).encode()\n                self.response(200,{\'cloud\':CLOUD,\'trial_id\':trial,\'plan_id\':plan,\n                    \'result\':rows,\'result_sha256\':hashlib.sha256(encoded).hexdigest(),\n                    \'join_seconds\':elapsed,\'row_count\':len(rows),\n                    \'raw_result_strings\':[[str(v) for v in row] for row in raw],\n                    \'explain_gzip_b64\':base64.b64encode(gzip.compress(physical.encode())).decode(),\n                    \'physical_plan_sha256\':hashlib.sha256(physical.encode()).hexdigest()})\n            else:self.response(404,{\'error\':\'not found\'})\n        except Exception as exc:\n            self.response(422,{\'error\':type(exc).__name__+\': \'+str(exc)[:300]})\n\ndef main():\n    p=argparse.ArgumentParser()\n    p.add_argument(\'--cert\',required=True);p.add_argument(\'--key\',required=True)\n    a=p.parse_args()\n    server=ThreadingHTTPServer((\'0.0.0.0\',PORT),Handler)\n    ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n    ctx.minimum_version=ssl.TLSVersion.TLSv1_2\n    ctx.load_cert_chain(a.cert,a.key)\n    server.socket=ctx.wrap_socket(server.socket,server_side=True)\n    server.serve_forever()\nif __name__==\'__main__\':main()\n'

# ----------------------- Azure ↔ GCP real-cloud workflow ----------------------
# Implemented inside this independent Q14 file. No Q03 or shared worker imports.
import base64
import gzip
import ipaddress
import secrets
import shlex
import subprocess

AZURE_RESOURCE_GROUP = os.environ.get('EGRESSOPT_AZURE_RESOURCE_GROUP','')
AZURE_VM = os.environ.get('EGRESSOPT_AZURE_VM','')
AZURE_NSG = os.environ.get('EGRESSOPT_AZURE_NSG','')
AZURE_PUBLIC_IP_RESOURCE = os.environ.get('EGRESSOPT_AZURE_PUBLIC_IP_RESOURCE','')
GCP_PROJECT = os.environ.get('EGRESSOPT_GCP_PROJECT','')
GCP_ZONE = os.environ.get('EGRESSOPT_GCP_ZONE','')
GCP_VM = os.environ.get('EGRESSOPT_GCP_VM','')
Q14_PORT = 8443


def q14_cli(argv, timeout=420):
    try:
        proc = subprocess.run(argv,capture_output=True,text=True,timeout=timeout)
    except FileNotFoundError as exc:
        raise RuntimeError('Required CLI not installed: '+argv[0]) from exc
    if proc.returncode:
        raise RuntimeError(f'{argv[0]} failed: '+(proc.stderr or proc.stdout)[-1400:])
    return proc.stdout.strip()


def q14_gcloud(*argv, timeout=420):
    return q14_cli(['gcloud',*argv,'--project='+GCP_PROJECT],timeout=timeout)


def q14_gcp_instance():
    return json.loads(q14_gcloud('compute','instances','describe',GCP_VM,
                     '--zone='+GCP_ZONE,'--format=json'))


def q14_azure_state():
    statuses=json.loads(q14_cli(['az','vm','get-instance-view','--resource-group',AZURE_RESOURCE_GROUP,
        '--name',AZURE_VM,'--query','instanceView.statuses[].code','--output','json']))
    power=[v for v in statuses if isinstance(v,str) and v.startswith('PowerState/')]
    if len(power)!=1:raise RuntimeError('Cannot determine Azure power state')
    return power[0]


def q14_remote(cloud,script,label):
    if cloud=='azure':
        args=['az','vm','run-command','invoke','--resource-group',AZURE_RESOURCE_GROUP,
              '--name',AZURE_VM,'--command-id','RunShellScript','--scripts','set -eu\n'+script,
              '--query','value[0].message','--output','tsv']
        # Startup transitions can produce stale PowerState/running reports.
        for attempt in range(1,7):
            try:
                stdout=q14_cli(args,timeout=460)
                if 'Enable succeeded' not in stdout:
                    raise RuntimeError('Azure run-command '+label+': '+stdout[-700:])
                return stdout
            except RuntimeError as exc:
                transient=('OperationNotAllowed' in str(exc) and
                    'requires the VM to be running' in str(exc))
                if not transient or attempt==6:raise
                time.sleep(min(5*attempt,20))
    if cloud=='gcp':
        remote_command='sudo -n bash -lc '+shlex.quote('set -eu\n'+script)
        return q14_gcloud('compute','ssh',GCP_VM,'--zone='+GCP_ZONE,
            '--tunnel-through-iap','--quiet','--command='+remote_command,timeout=490)
    raise ValueError('Invalid Q14 cloud: '+cloud)


def q14_marker(out, prefix='Q14_RPC='):
    matches=[line.split(prefix,1)[1].strip() for line in out.splitlines()
             if line.startswith(prefix)]
    if len(matches)!=1:
        raise RuntimeError('Missing or duplicate '+prefix+' remote response: '+out[-400:])
    return matches[0]


def q14_rpc(cloud, script,label):
    return json.loads(q14_marker(q14_remote(cloud,script,label)))


def q14_start_vm(cloud):
    if cloud=='azure':
        state=q14_azure_state()
        if state=='PowerState/running':return False
        if state not in {'PowerState/deallocated','PowerState/stopped'}:
            raise RuntimeError('Unsafe Azure VM state: '+state)
        q14_cli(['az','vm','start','--resource-group',AZURE_RESOURCE_GROUP,
                 '--name',AZURE_VM,'--output','none'],timeout=660)
        return True
    if cloud=='gcp':
        state=q14_gcp_instance()['status']
        if state=='RUNNING':return False
        if state not in {'TERMINATED','STOPPED'}:
            raise RuntimeError('Unsafe GCP VM state: '+state)
        q14_gcloud('compute','instances','start',GCP_VM,'--zone='+GCP_ZONE,
                   '--quiet','--format=json',timeout=660)
        return True
    raise ValueError(cloud)


def q14_ip(raw):
    ip=ipaddress.ip_address(str(raw).strip())
    if ip.version!=4 or not ip.is_global:
        raise RuntimeError('Expected public routable IPv4, found '+str(raw))
    return str(ip)


def q14_gcp_public_ip(inst):
    for nic in inst.get('networkInterfaces',[]):
        for cfg in nic.get('accessConfigs',[]):
            if cfg.get('natIP'):
                return q14_ip(cfg['natIP'])
    raise RuntimeError('GCP VM has no external IPv4. Direct Azure→GCP HTTPS needs an '
        'external address or independently configured private connectivity. '
        'Cloud experiment aborted before firewall changes; no data transfer was measured.')


def q14_outbound_ip(cloud):
    out=q14_remote(cloud,'curl -fsS --connect-timeout 10 --max-time 30 https://checkip.amazonaws.com',
                   'verify-outbound-IP')
    if cloud=='azure' and '[stdout]' in out:
        out=out.split('[stdout]',1)[1].split('[stderr]',1)[0]
    return q14_ip(out.strip())


def q14_install_worker(cloud,run_key):
    data=Q14_WORKER_SOURCE.encode()
    release='/opt/egressopt/releases/q14_'+digest_bytes(data)[:12]
    worker=release+'/worker.py'
    payload=base64.b64encode(gzip.compress(data)).decode()
    tmp=f'/opt/egressopt/runtime/q14_upload_{run_key}_{cloud}'
    q14_remote(cloud,f'umask 077; mkdir -p {shlex.quote(tmp)} {shlex.quote(release)}; '
               f': > {shlex.quote(tmp+"/worker.gz.b64")}', 'prepare-Q14-worker')
    for offset in range(0,len(payload),1500):
        q14_remote(cloud,'printf %s '+shlex.quote(payload[offset:offset+1500])+
                    ' >> '+shlex.quote(tmp+'/worker.gz.b64'), 'upload-Q14-worker')
    py='/opt/egressopt/venv/bin/python'
    verify=(
        'base64 -d '+shlex.quote(tmp+'/worker.gz.b64')+' | gzip -d > '+shlex.quote(worker)+'\n'
        +f'test "$(sha256sum {shlex.quote(worker)} | cut -d\' \' -f1)" = {shlex.quote(digest_bytes(data))}\n'
        +py+' -m py_compile '+shlex.quote(worker)+'\n'
        +'rm -rf '+shlex.quote(tmp)+'\n'
        +'echo Q14_WORKER_DEPLOYED')
    output=q14_remote(cloud,verify,'validate-Q14-worker')
    if 'Q14_WORKER_DEPLOYED' not in output:
        raise RuntimeError('Q14 worker verification failed on '+cloud)
    return release


def q14_start_service(cloud,run_key,release,public_ip,token,params):
    work=f'/opt/egressopt/runtime/q14_{run_key}_{cloud}'
    unit=f'egressopt-q14-{run_key}-{cloud}'
    py='/opt/egressopt/venv/bin/python'
    launch='\n'.join(['#!/bin/sh','set -eu',
        'export Q14_CLOUD='+shlex.quote(cloud),
        'export Q14_RUNTIME='+shlex.quote(work),
        'export Q14_DATA_DIR=/opt/egressopt/data/tpch_sf1',
        'export Q14_WORKER_TOKEN='+shlex.quote(token),
        'export Q14_START_DATE='+shlex.quote(params['start_date']),
        'export Q14_END_DATE='+shlex.quote(params['end_date_exclusive']),
        'exec '+py+' '+shlex.quote(release+'/worker.py')+
        ' --cert '+shlex.quote(work+'/server.pem')+' --key '+shlex.quote(work+'/server.key')])+'\n'
    encoded=base64.b64encode(launch.encode()).decode()
    table='lineitem' if cloud=='azure' else 'part'
    script=f'''
umask 077
mkdir -p {shlex.quote(work)}
test -f {shlex.quote(release+'/worker.py')}
test -f /opt/egressopt/data/tpch_sf1/{table}.parquet
{py} -c 'import duckdb; assert duckdb.__version__ == "1.5.6", duckdb.__version__'
command -v openssl >/dev/null
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 \
 -keyout {work}/ca.key -out {work}/ca.pem -subj '/CN=EgressOpt Q14 temporary CA' \
 -addext 'basicConstraints=critical,CA:TRUE' >/dev/null 2>&1
openssl req -nodes -newkey rsa:2048 -sha256 \
 -keyout {work}/server.key -out {work}/server.csr \
 -subj '/CN=EgressOpt Q14 worker' >/dev/null 2>&1
printf 'basicConstraints=CA:FALSE\\nsubjectAltName=IP:{public_ip}\\nextendedKeyUsage=serverAuth\\n' > {work}/server.ext
openssl x509 -req -days 1 -sha256 -in {work}/server.csr \
 -CA {work}/ca.pem -CAkey {work}/ca.key -CAcreateserial \
 -extfile {work}/server.ext -out {work}/server.pem >/dev/null 2>&1
printf %s {shlex.quote(encoded)} | base64 -d > {work}/launch.sh
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
 {py} -c 'import sys,json; o=json.load(sys.stdin); assert o["cloud"] == "{cloud}" and o["query"] == "Q14"'
echo Q14_SERVICE_READY
printf 'Q14_CA_B64='; base64 -w0 {work}/ca.pem; echo
'''
    out=q14_remote(cloud,script,'launch-Q14-TLS-worker')
    if 'Q14_SERVICE_READY' not in out:raise RuntimeError('Q14 TLS worker failed on '+cloud)
    ca=q14_marker(out,'Q14_CA_B64=')
    if b'BEGIN CERTIFICATE' not in base64.b64decode(ca,validate=True):
        raise RuntimeError('Invalid Q14 worker CA on '+cloud)
    return ca


def q14_azure_ingress(gcp_outbound_ip,run_key):
    name='egressopt-q14-'+run_key
    rules=json.loads(q14_cli(['az','network','nsg','rule','list','--resource-group',AZURE_RESOURCE_GROUP,
                               '--nsg-name',AZURE_NSG,'--output','json']))
    used={int(rule['priority']) for rule in rules if rule.get('direction')=='Inbound'}
    priority=next((p for p in range(1100,2500) if p not in used),None)
    if priority is None:raise RuntimeError('No Azure NSG priority is available')
    q14_cli(['az','network','nsg','rule','create','--resource-group',AZURE_RESOURCE_GROUP,
      '--nsg-name',AZURE_NSG,'--name',name,'--priority',str(priority),'--direction','Inbound',
      '--access','Allow','--protocol','Tcp','--source-address-prefixes',gcp_outbound_ip+'/32',
      '--destination-port-ranges','8443','--description','EgressOpt Q14 temporary HTTPS',
      '--output','none'],timeout=280)
    return name


def q14_gcp_ingress(azure_outbound_ip,run_key,network):
    # Apply a unique network tag to only this VM; avoid VPC-wide ingress.
    tag='egressopt-q14-'+run_key
    q14_gcloud('compute','instances','add-tags',GCP_VM,'--zone='+GCP_ZONE,
               '--tags='+tag,'--quiet','--format=json',timeout=250)
    rule='egressopt-q14-'+run_key
    try:
        q14_gcloud('compute','firewall-rules','create',rule,'--network='+network,
          '--direction=INGRESS','--action=ALLOW','--rules=tcp:8443',
          '--source-ranges='+azure_outbound_ip+'/32','--target-tags='+tag,
          '--description=EgressOpt Q14 temporary Azure-to-GCP TLS',
          '--quiet','--format=json',timeout=270)
    except Exception:
        # Remove tag if firewall creation fails.
        q14_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
                   '--tags='+tag,'--quiet','--format=json',timeout=250)
        raise
    return rule,tag


def q14_stage(cloud,part,plan,trial,token,run_key):
    payload=json.dumps({'part':part,'plan_id':plan,'trial_id':trial},separators=(',',':'))
    work=f'/opt/egressopt/runtime/q14_{run_key}_{cloud}'
    out=work+'/stage_response.json'
    script=(f'curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 300 '
            f'-H {shlex.quote("Authorization: Bearer "+token)} '
            f'-H "Content-Type: application/json" '
            f'--data-binary {shlex.quote(payload)} https://127.0.0.1:8443/q14/stage > {out}\n'
            '/opt/egressopt/venv/bin/python -c '+shlex.quote(
                'import json; o=json.load(open('+repr(out)+')); '
                'print("Q14_RPC="+json.dumps(o,separators=(",",":")))'))
    data=q14_rpc(cloud,script,'stage-'+plan)
    if (data['cloud']!=cloud or data['part']!=part or
        data['plan_id']!=plan or data['trial_id']!=trial):
        raise RuntimeError('Q14 stage identity mismatch')
    if int(data['parquet_bytes'])<1 or int(data['rows'])<1 or not re.fullmatch('[a-f0-9]{64}',data['sha256']):
        raise RuntimeError('Q14 invalid staging payload')
    return data


def q14_upload(source,dest,stage,plan,trial,peer_ip,peer_ca_b64,token,run_key):
    part=stage['part']
    work=f'/opt/egressopt/runtime/q14_{run_key}_{source}'
    ca=work+'/peer-ca.pem'; out=work+'/upload_response.json'
    source_file=stage['file_path']
    if source_file!=work+'/stage/'+trial+'/'+part+'.parquet':
        raise RuntimeError('Q14 rejected remote source file path')
    url=f'https://{peer_ip}:8443/q14/upload/{trial}/{plan}/{part}'
    script=(f'printf %s {shlex.quote(peer_ca_b64)} | base64 -d > {ca}\n'
       f"INFO=$(curl --fail --silent --show-error --connect-timeout 20 --max-time 320 "
       f"--cacert {ca} -H {shlex.quote('Authorization: Bearer '+token)} "
       f"-H {shlex.quote('X-Q14-SHA256: '+stage['sha256'])} "
       f"-H 'Content-Type: application/octet-stream' "
       f"--data-binary @{shlex.quote(source_file)} -o {out} "
       f"-w '%{{time_total}} %{{size_upload}} %{{http_code}}' {url})\n"
       'export INFO\n'
       '/opt/egressopt/venv/bin/python -c '+shlex.quote(
            'import os,json; t,n,s=os.environ["INFO"].split(); '
            'r=json.load(open('+repr(out)+')); '
            'print("Q14_RPC="+json.dumps({"curl_total_seconds":float(t),'
            '"curl_size_upload":int(n),"http_status":int(s),"received":r},'
            'separators=(",",":")))'))
    transfer=q14_rpc(source,script,'transfer-'+plan)
    received=transfer['received']
    if (transfer['http_status']!=200 or received['cloud']!=dest or
        received['trial_id']!=trial or received['plan_id']!=plan or received['part']!=part or
        received['sha256']!=stage['sha256'] or
        int(transfer['curl_size_upload'])!=int(stage['parquet_bytes']) or
        int(received['received_payload_bytes'])!=int(stage['parquet_bytes'])):
        raise RuntimeError('Q14 TLS upload failed checksum/size/identity verification')
    return {'name':part,'source_cloud':source,'destination_cloud':dest,
        'rows':int(stage['rows']),'parquet_bytes':int(stage['parquet_bytes']),
        'stage_seconds':float(stage['stage_seconds']), 'sha256':stage['sha256'],
        'curl_http_total_seconds':float(transfer['curl_total_seconds']),
        'curl_size_upload':int(transfer['curl_size_upload']),
        'receiver_file_seconds':float(received['receive_file_seconds'])}


def q14_finish(dest,plan,trial,token,run_key):
    work=f'/opt/egressopt/runtime/q14_{run_key}_{dest}'
    out=work+'/finish_response.json'
    body=json.dumps({'plan_id':plan,'trial_id':trial},separators=(',',':'))
    script=(f'curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 300 '
            f'-H {shlex.quote("Authorization: Bearer "+token)} '
            f'-H "Content-Type: application/json" --data-binary {shlex.quote(body)} '
            f'https://127.0.0.1:8443/q14/finish > {out}\n'
            '/opt/egressopt/venv/bin/python -c '+shlex.quote(
                'import json,gzip,base64; o=json.load(open('+repr(out)+')); '
                'print("Q14_RPC_GZIP="+base64.b64encode(gzip.compress('
                'json.dumps(o,separators=(",",":")).encode())).decode())'))
    resp=q14_remote(dest,script,'Q14-finish-'+plan)
    obj=json.loads(gzip.decompress(base64.b64decode(q14_marker(resp,'Q14_RPC_GZIP='),validate=True)))
    if obj['cloud']!=dest or obj['plan_id']!=plan or obj['trial_id']!=trial:
        raise RuntimeError('Q14 receiver finish identity mismatch')
    explain=gzip.decompress(base64.b64decode(obj.pop('explain_gzip_b64'),validate=True)).decode()
    if digest_bytes(explain.encode())!=obj.pop('physical_plan_sha256'):
        raise RuntimeError('Q14 EXPLAIN hash mismatch')
    obj['explain']=explain
    return obj


def q14_one_trial(plan,trial,token,run_key,ips,cas):
    exchange={
        'gcp_join':('azure','gcp','lineitem_selected'),
        'azure_join':('gcp','azure','part_attributes'),
        'partial_aggregate_gcp':('azure','gcp','lineitem_partial'),
        'promo_keys_azure':('gcp','azure','promo_keys'),
    }
    source,dest,part=exchange[plan]
    staged=q14_stage(source,part,plan,trial,token,run_key)
    edge=q14_upload(source,dest,staged,plan,trial,ips[dest],cas[dest],token,run_key)
    finished=q14_finish(dest,plan,trial,token,run_key)
    return edge,finished,dest


def q14_cloud(args):
    if args.sf!=1:raise RuntimeError('Only validated Q14 SF1 is available')
    import duckdb
    paths={t:args.data_dir/(t+'.parquet') for t in TABLE_PLACEMENT}
    for path in (args.query_file,*paths.values()):
        if not path.is_file():raise FileNotFoundError(str(path))
    sql=args.query_file.read_text(encoding='utf-8')
    params=load_params(sql)
    with duckdb.connect(':memory:') as con:
        ref=reference_rows(con,paths,sql)
        constraints=check_foreign_keys(con,paths)
    source_sql_hash=sha256(args.query_file)
    ref_obj=require_reference(ref,args.reference_file,source_sql_hash)
    ref_hash=canonical_hash(ref)
    run_key=uuid.uuid4().hex[:8]
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+run_key
    directory=root(args)/run_id
    directory.mkdir(parents=True,exist_ok=False)
    report={'schema_version':2,'query_id':QUERY,'run_id':run_id,'status':'running',
        'scale_factor':'SF1','placement_id':PLACEMENT,'execution_mode':'real_multicloud',
        'created_at_utc':now(),'trial_count_per_plan':args.trials,
        'warmup_trials_per_plan':args.warmup_trials,'plan_order':'rotating four-plan round robin',
        'round_execution_order':[],'reference_sha256':ref_hash,'reference_row_count':len(ref),
        'reference_origin':ref_obj.get('reference_origin'),'query_parameters':params,
        'key_constraints':constraints,'reference_file':{'path':str(args.reference_file),
        'sha256':sha256(args.reference_file)},
        'source_sql':{'path':str(args.query_file),'sha256':source_sql_hash},
        'runner_sha256':sha256(Path(__file__)),'worker_sha256':digest_bytes(Q14_WORKER_SOURCE.encode()),
        'dataset':{t:{'cloud':TABLE_PLACEMENT[t],'path':str(path),
                    'sha256':sha256(path),'bytes':path.stat().st_size} for t,path in paths.items()},
        'timing_definition':'remote Parquet stage wall time + TLS curl request wall time (including network, receiver write, response) + destination query wall time; excludes cloud management and VM startup',
        'network_bytes_definition':'HTTP request body bytes, not provider billed bytes',
        'network_seconds_measured':'NOT_ISOLATED', 'egress_cost_status':'NOT_PRICED',
        'cleanup':{},'temporary_network_rule_ids':{},
        'reference_rows_file':'q14_reference_rows.json',
        'limitations':['no optimizer trained or evaluated','actual billed egress cost not measured',
          'tokens may appear in authorized cloud-management command logs']}
    save_json(directory/'q14_reference_rows.json',ref)
    save_json(directory/'run.json',report)
    changed={'azure_service':False,'gcp_service':False,'azure_rule':None,
       'gcp_rule':None,'gcp_tag':None,'gcp_tag_added':False,
       'gcp_access_config_added':False,'azure_started':False,'gcp_started':False}
    warmups=[]; measured=[]
    try:
        for cloud in ('azure','gcp'):
            print('Checking '+cloud.upper()+' VM...',flush=True)
            changed[cloud+'_started']=q14_start_vm(cloud)
            print('  '+cloud.upper()+' running',flush=True)
        gcp_inst=q14_gcp_instance()
        if gcp_inst['status']!='RUNNING' or q14_azure_state()!='PowerState/running':
            raise RuntimeError('Q14 VMs did not reach RUNNING states')
        if not any(cfg.get('natIP') for nic in gcp_inst.get('networkInterfaces',[])
                   for cfg in nic.get('accessConfigs',[])):
            # A GCP VM accessed through IAP may have no external IP. Assign only
            # a temporary ephemeral NAT configuration; remove it in finally.
            changed['gcp_access_config_added']=True
            report['temporary_gcp_access_config']='external-nat'
            save_json(directory/'run.json',report)
            q14_gcloud('compute','instances','add-access-config',GCP_VM,
                       '--zone='+GCP_ZONE,'--network-interface=nic0',
                       '--access-config-name=external-nat','--quiet','--format=json',timeout=290)
            gcp_inst=q14_gcp_instance()
            print('  GCP temporary ephemeral public IPv4 assigned',flush=True)
        ips={'gcp':q14_gcp_public_ip(gcp_inst),
             'azure':q14_ip(json.loads(q14_cli(['az','network','public-ip','show',
                  '--resource-group',AZURE_RESOURCE_GROUP,'--name',AZURE_PUBLIC_IP_RESOURCE,
                  '--output','json']))['ipAddress'])}
        report['assigned_public_ips']=ips
        report['cloud_machines']={
            'gcp':{'instance':GCP_VM,'project':GCP_PROJECT,'zone':GCP_ZONE,
                   'machine_type':gcp_inst.get('machineType','').rsplit('/',1)[-1]},
            'azure':{'vm':AZURE_VM,'resource_group':AZURE_RESOURCE_GROUP}}
        outbound={cloud:q14_outbound_ip(cloud) for cloud in ('azure','gcp')}
        report['verified_outbound_public_ips']=outbound
        report['remote_dataset_sha256']={}
        for cloud in ('azure','gcp'):
            table='lineitem' if cloud=='azure' else 'part'
            res=q14_remote(cloud,'sha256sum /opt/egressopt/data/tpch_sf1/'+table+'.parquet',
                           'verify-cloud-dataset-sha256')
            expected=report['dataset'][table]['sha256']
            if not any(line.split()[:1]==[expected] and line.strip().endswith('/'+table+'.parquet')
                       for line in res.splitlines()):
                raise RuntimeError('Q14 cloud dataset does not match local SF1: '+table)
            report['remote_dataset_sha256'][cloud]={table:expected}
            report.setdefault('worker_releases',{})[cloud]=q14_install_worker(cloud,run_key)
            save_json(directory/'run.json',report)
        token=secrets.token_urlsafe(48)
        cas={}
        for cloud in ('azure','gcp'):
            changed[cloud+'_service']=True
            cas[cloud]=q14_start_service(cloud,run_key,report['worker_releases'][cloud],
                ips[cloud],token,params)
            print('  '+cloud.upper()+' authenticated Q14 TLS worker ready',flush=True)
        # GCP ingress restricted to Azure's observed source IP and this VM's temporary tag.
        network=gcp_inst['networkInterfaces'][0]['network'].rsplit('/',1)[-1]
        # Record identifiers *before* attempting changes, for manual recovery if interrupted.
        gcp_tag='egressopt-q14-'+run_key
        report['temporary_network_rule_ids']['gcp']=gcp_tag
        save_json(directory/'run.json',report)
        changed['gcp_tag_added']=True
        changed['gcp_tag']=gcp_tag
        rule,tag=q14_gcp_ingress(outbound['azure'],run_key,network)
        changed['gcp_rule']=rule
        report['temporary_network_rule_ids']['gcp']=rule
        report['temporary_gcp_network_tag']=tag
        save_json(directory/'run.json',report)
        report['temporary_network_rule_ids']['azure']='egressopt-q14-'+run_key
        save_json(directory/'run.json',report)
        changed['azure_rule']=q14_azure_ingress(outbound['gcp'],run_key)
        save_json(directory/'run.json',report)
        print('  Source-restricted Azure and GCP TCP 8443 ingress rules active',flush=True)
        for i in range(args.warmup_trials+args.trials):
            warm=i<args.warmup_trials
            trial=i+1 if warm else i-args.warmup_trials+1
            tid=('w' if warm else 'm')+f'{trial:02d}'
            order=PLANS[i%len(PLANS):]+PLANS[:i%len(PLANS)]
            report['round_execution_order'].append({'round':i+1,'trial_id':tid,
                                                'warmup':warm,'plans':list(order)})
            for plan in order:
                edge,finish,dest=q14_one_trial(plan,tid,token,run_key,ips,cas)
                result=finish['result']
                if result!=ref or finish['result_sha256']!=ref_hash or finish['row_count']!=len(ref):
                    raise RuntimeError('Q14 cloud result mismatch: '+plan+' '+tid)
                payload=int(edge['curl_size_upload'])
                total=float(edge['stage_seconds'])+float(edge['curl_http_total_seconds'])+float(finish['join_seconds'])
                row={'run_id':run_id,'query_id':QUERY,'scale_factor':'SF1',
                     'placement_id':PLACEMENT,'execution_mode':'real_multicloud',
                     'plan_id':plan,'trial':trial,'join_cloud':dest,
                     'source_clouds':edge['source_cloud'],'transfer_edges':1,
                     'intermediate_rows':edge['rows'],'intermediate_parquet_bytes':edge['parquet_bytes'],
                     'stage_seconds':edge['stage_seconds'],'join_aggregate_seconds':finish['join_seconds'],
                     'total_seconds':total,'result_row_count':len(result),
                     'result_sha256':finish['result_sha256'],'correct_vs_reference':True,
                     'network_bytes_measured':payload,'network_seconds_measured':'',
                     'egress_usd_estimated':'','timestamp_utc':now()}
                explain_name=(f'warmup_{trial:02d}_{plan}_physical_plan.txt' if warm
                             else f'trial_{trial:02d}_{plan}_physical_plan.txt')
                (directory/explain_name).write_text(finish['explain']+'\n',encoding='utf-8')
                trial_obj={'schema_version':2,'trial':trial,'plan_id':plan,'metrics':row,
                           'result':result,'raw_result_strings':finish.get('raw_result_strings'),
                           'transfer_edges':[edge], 'plan_explain_file':explain_name,
                           'plan_explain_sha256':sha256(directory/explain_name),
                           'timing_note':report['timing_definition']}
                if warm:
                    warmups.append({'warmup':trial,'plan_id':plan,'total_seconds':total,
                           'intermediate_parquet_bytes':payload,'correct_vs_reference':True})
                    save_json(directory/f'warmup_{trial:02d}_{plan}.json',trial_obj)
                    write_csv(directory/'warmup_metrics.csv',warmups,fields=list(warmups[0]))
                else:
                    measured.append(row)
                    save_json(directory/f'trial_{trial:02d}_{plan}.json',trial_obj)
                    write_csv(directory/'trial_metrics.csv',measured)
                label=f'WARMUP={trial}' if warm else f'trial={trial}'
                print(f'{plan:23s} {label} correctness=PASS '
                      f'real_HTTP_bytes={payload:,} stage+HTTPS+join={total:.3f}s',flush=True)
        report.update({'status':'success','completed_at_utc':now(),
                       'trial_metrics_csv':'trial_metrics.csv',
                       'trial_metrics_sha256':sha256(directory/'trial_metrics.csv'),
                       'warmup_count':len(warmups),'measured_count':len(measured)})
        print('Q14_REAL_MULTICLOUD_PASS')
        print('Q14 cloud run: '+str(directory))
        return 0
    except Exception as exc:
        report.update({'status':'failed','error':type(exc).__name__+': '+str(exc),
                       'completed_at_utc':now()})
        raise
    finally:
        # Cleanup records are persisted even on failure; never silently assert success.
        for cloud in ('azure','gcp'):
            if changed[cloud+'_service']:
                try:
                    q14_remote(cloud,
                        f'systemctl stop egressopt-q14-{run_key}-{cloud} >/dev/null 2>&1 || true; '
                        f'rm -rf /opt/egressopt/runtime/q14_{run_key}_{cloud}; echo Q14_RUNTIME_REMOVED',
                        'stop-Q14-TLS-worker')
                    report['cleanup'][cloud+'_runtime']='removed'
                except Exception as exc:report['cleanup'][cloud+'_runtime']='FAILED: '+str(exc)
        if changed['azure_rule']:
            try:
                q14_cli(['az','network','nsg','rule','delete','--resource-group',AZURE_RESOURCE_GROUP,
                         '--nsg-name',AZURE_NSG,'--name',changed['azure_rule'],'--output','none'],timeout=280)
                report['cleanup']['azure_ingress']='removed'
            except Exception as exc:report['cleanup']['azure_ingress']='FAILED: '+str(exc)
        if changed['gcp_rule']:
            try:
                q14_gcloud('compute','firewall-rules','delete',changed['gcp_rule'],'--quiet',timeout=250)
                report['cleanup']['gcp_ingress']='removed'
            except Exception as exc:report['cleanup']['gcp_ingress']='FAILED: '+str(exc)
        if changed['gcp_tag_added']:
            try:
                q14_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
                           '--tags='+changed['gcp_tag'],'--quiet','--format=json',timeout=250)
                report['cleanup']['gcp_network_tag']='removed'
            except Exception as exc:report['cleanup']['gcp_network_tag']='FAILED: '+str(exc)
        if changed['gcp_access_config_added']:
            try:
                q14_gcloud('compute','instances','delete-access-config',GCP_VM,
                           '--zone='+GCP_ZONE,'--network-interface=nic0',
                           '--access-config-name=external-nat','--quiet','--format=json',timeout=290)
                report['cleanup']['gcp_temporary_external_ip']='removed'
            except Exception as exc:
                report['cleanup']['gcp_temporary_external_ip']='FAILED: '+str(exc)
        if args.stop_all_after_run:
            try:
                q14_cli(['az','vm','deallocate','--resource-group',AZURE_RESOURCE_GROUP,
                         '--name',AZURE_VM,'--output','none'],timeout=660)
                if q14_azure_state()!='PowerState/deallocated':
                    raise RuntimeError('Azure deallocation not verified')
                report['cleanup']['azure_vm']='deallocated_verified'
            except Exception as exc:report['cleanup']['azure_vm']='FAILED: '+str(exc)
            try:
                q14_gcloud('compute','instances','stop',GCP_VM,'--zone='+GCP_ZONE,
                           '--quiet','--format=json',timeout=660)
                if q14_gcp_instance()['status']!='TERMINATED':
                    raise RuntimeError('GCP shutdown not verified')
                report['cleanup']['gcp_vm']='stopped_verified'
            except Exception as exc:report['cleanup']['gcp_vm']='FAILED: '+str(exc)
        else:
            for cloud in ('azure','gcp'):
                if changed[cloud+'_started']:
                    try:
                        if cloud=='azure':
                            q14_cli(['az','vm','deallocate','--resource-group',AZURE_RESOURCE_GROUP,
                                     '--name',AZURE_VM,'--output','none'],timeout=660)
                        else:q14_gcloud('compute','instances','stop',GCP_VM,
                                         '--zone='+GCP_ZONE,'--quiet','--format=json',timeout=660)
                        report['cleanup'][cloud+'_vm']='stop_requested'
                    except Exception as exc:report['cleanup'][cloud+'_vm']='FAILED: '+str(exc)
        report['cleanup_incomplete']=any(str(v).startswith('FAILED') for v in report['cleanup'].values())
        save_json(directory/'run.json',report)
        if report['cleanup_incomplete']:
            print('WARNING: Q14 cloud cleanup incomplete: inspect run.json',file=sys.stderr)
        print('Q14 experiment report: '+str(directory),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',required=True,choices=('cloud','prepare','reproduce','audit'))
    parser.add_argument('--sf',type=int,default=DEFAULT_SF)
    parser.add_argument('--trials',type=int,default=10)
    parser.add_argument('--warmup-trials',type=int,default=2)
    parser.add_argument('--stop-all-after-run',action=argparse.BooleanOptionalAction,default=True)
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--run-id')
    parser.add_argument('--query-file',type=Path,default=Path('queries/tpch/q14.sql'))
    parser.add_argument('--data-dir',type=Path)
    parser.add_argument('--reference-file',type=Path)
    parser.add_argument('--output-dir',type=Path,default=Path('results/experiments'))
    parser.add_argument('--plots',action=argparse.BooleanOptionalAction,default=True)
    args=parser.parse_args()
    if args.sf not in VALIDATED_SCALE_FACTORS:
        parser.error('Only validated SF1 is enabled; no SF10/SF100 datasets are deployed')
    if args.trials<1 or args.threads<1 or args.trials>100 or args.warmup_trials<0 or args.warmup_trials>20:
        parser.error('Choose 1–100 trials and at least one CPU thread')
    if args.data_dir is None:
        args.data_dir=Path(f'data/tpch/parquet_sf{args.sf}')
    if args.reference_file is None:
        args.reference_file=Path(f'results/reference/sf{args.sf}/q14_canonical_reference.json')
    if args.mode=='cloud':
        return q14_cloud(args)
    if args.mode=='prepare':
        return prepare(args)
    if args.mode=='audit':
        audit(args)
        return 0
    return reproduce(args)


if __name__=='__main__':
    try:
        sys.exit(main())
    except (RuntimeError,ValueError,FileNotFoundError,InvalidOperation) as exc:
        print(f'Q14 ERROR: {exc}',file=sys.stderr)
        sys.exit(2)
