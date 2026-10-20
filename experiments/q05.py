#!/usr/bin/env python3
"""EgressOpt Q05 — standalone Local Supplier Volume research runner.

LOCKED SF1/P1 workload. One independent script for this query only.

    python experiments/q05.py --mode prepare --trials 1
    python experiments/q05.py --mode cloud --trials 10
    python experiments/q05.py --mode reproduce
    python experiments/q05.py --mode audit
    python experiments/q05.py --mode cleanup --run-id FAILED_RUN_ID
    python experiments/q05.py --mode optimizer-preview --optimizer-archive EgressOpt_Paper_Input.zip --optimizer-tariffs tariffs_public_list_marginal.json --optimizer-delta 0.5
    python experiments/q05.py --mode optimizer-cloud --optimizer-archive EgressOpt_Paper_Input.zip --optimizer-tariffs tariffs_public_list_marginal.json --optimizer-delta 0.5 --trials 1 --warmup-trials 0
    python experiments/q05.py --mode reproduce --prospective-run
    python experiments/q05.py --mode audit --prospective-run

P1: orders (AWS); customer, lineitem, nation (Azure); supplier, region (GCP).
Uses actual user-supplied q05.sql and the eight-table TPC-H-derived SF1 parquet
snapshot. Exact source SQL is independently executed for reference validation.

PREPARE stages intermediate Parquet files *locally* to emulate directed cloud
shipments; NO actual network traffic is sent. CLOUD uses temporary TLS workers
and real transfers across AWS, Azure and GCP. New CLOUD mode must pass live
one-trial smoke test before being trusted for the full benchmark.
No imports from Q03, Q12, Q14, or any other EgressOpt Python file.
"""
from __future__ import annotations

import argparse
import csv
import io
import math
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_EVEN
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import sys
import tempfile
import time
import uuid

QUERY_ID = 'Q05'
PLACEMENT_ID = 'P1'
DEFAULT_SF = int(os.getenv('EGRESSOPT_SF', '1'))
VALIDATED_SF = (1,)
PLANS = ('azure_join', 'aws_join', 'gcp_join', 'semijoin_azure')
TABLE_LOCATION = {
    'orders': 'aws',
    'customer': 'azure', 'lineitem': 'azure', 'nation': 'azure',
    'supplier': 'gcp', 'region': 'gcp',
}
EXPECTED_EDGES = {
    'azure_join': [('aws','azure'),('gcp','azure')],
    'aws_join': [('azure','aws'),('azure','aws'),('gcp','aws')],
    'gcp_join': [('aws','azure'),('azure','gcp')],
    'semijoin_azure': [
        ('gcp','azure'),('azure','aws'),('aws','azure'),
        ('azure','gcp'),('gcp','azure'),
    ],
}
RESULT_FIELDS = ('n_name', 'revenue')
METRIC_FIELDS = (
    'run_id','query_id','scale_factor','placement_id','execution_mode',
    'plan_id','trial','intermediate_parquet_bytes','intermediate_rows',
    'edge_count','stage_seconds','join_aggregate_seconds','total_seconds',
    'result_sha256','correct_vs_reference','network_bytes_measured',
    'network_seconds_measured','egress_usd_estimated','timestamp_utc',
)
CENT = Decimal('0.01')


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha_file(path: Path) -> str:
    dig = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024*1024), b''):
            dig.update(chunk)
    return dig.hexdigest()


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + '\n', encoding='utf-8')


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parquet(path: Path) -> str:
    return f'read_parquet({quote(str(path.resolve()))})'


def canonical(rows) -> list[dict]:
    """Canonical Q05 nation/revenue answer, rounded to SQL currency cents.

    The unrounded raw values are also recorded in each trial JSON. The
    comparator intentionally rejects missing/extra/duplicate nation rows.
    """
    result = []
    nations = set()
    for row in rows:
        if len(row) != 2 or row[0] is None or row[1] is None:
            raise ValueError('Q05 requires non-null (nation_name,revenue) rows')
        nation = str(row[0])
        if nation in nations:
            raise ValueError(f'Q05 duplicate nation group: {nation}')
        nations.add(nation)
        revenue = Decimal(str(row[1]))
        if not revenue.is_finite():
            raise ValueError('Non-finite revenue')
        result.append({'n_name': nation,
                       'revenue': format(revenue.quantize(CENT, rounding=ROUND_HALF_EVEN), '.2f')})
    result.sort(key=lambda v: v['n_name'])
    return result


def result_hash(rows: list[dict]) -> str:
    return sha_bytes(json.dumps(rows, sort_keys=True, separators=(',',':')).encode('utf-8'))


def _normalize_q05_date_literals(sql: str) -> str:
    """Normalize equivalent DuckDB DATE literal forms for SQL validation.

    Only the source SQL is canonicalized; the authoritative reference query
    continues to execute exactly as archived, without rewriting its semantics.
    """
    forms = (
        # CAST('1994-01-01' AS DATE)
        r"\bCAST\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s+AS\s+DATE\s*\)",
        # DATE('1994-01-01')
        r"\bDATE\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s*\)",
        # '1994-01-01'::DATE
        r"'(\d{4}-\d{2}-\d{2})'\s*::\s*DATE\b",
    )
    for form in forms:
        sql = re.sub(form, lambda match: "DATE '" + match.group(1) + "'", sql, flags=re.I)
    return sql


def source_parameters(sql: str) -> dict:
    """Extract verified Q05 parameters without substituting fixed dates/region.

    Accept equivalent one-calendar-year date ranges in Q05 SQL:
      >= DATE '1994-01-01' AND < DATE '1995-01-01'
      >= DATE '1994-01-01' AND < DATE '1994-01-01' + INTERVAL '1' YEAR
      >= CAST('1994-01-01' AS DATE) AND < CAST('1995-01-01' AS DATE)
      BETWEEN DATE '1994-01-01' AND DATE '1994-12-31' (DATE column)

    Fail closed for an unrecognized or non-one-year filter. The archived query
    is always executed unchanged by source_reference() for correctness checking.
    """
    stripped = re.sub(r'/\*.*?\*/', ' ', re.sub(r'--[^\n]*', ' ', sql), flags=re.S)
    source = _normalize_q05_date_literals(stripped)
    norm = re.sub(r'\s+', ' ', source.lower())
    required = ('n_name','l_extendedprice','l_discount','customer','orders',
                'lineitem','supplier','nation','region','sum','group by','order by')
    if any(token not in norm for token in required):
        raise ValueError('Q05 source SQL is not the supported six-relation grouped query')
    equalities = (
        ('c_custkey','o_custkey'), ('l_orderkey','o_orderkey'),
        ('l_suppkey','s_suppkey'), ('c_nationkey','s_nationkey'),
        ('s_nationkey','n_nationkey'), ('n_regionkey','r_regionkey'),
    )
    for left, right in equalities:
        pattern = rf'\b(?:{left}\s*=\s*{right}|{right}\s*=\s*{left})\b'
        if not re.search(pattern, norm):
            raise ValueError(f'Q05 source SQL lacks verified join {left}={right}')
    region = re.search(r"\br_name\s*=\s*'((?:''|[^'])*)'", source, re.I)
    if not region:
        raise ValueError('Q05 source SQL must contain a literal r_name selection')

    # Query's DATE predicates may use aliases such as o.o_orderdate.
    col = r'(?:\b[a-z_][a-z_0-9]*\s*\.\s*)?o_orderdate\b'
    literal = r"(?:DATE\s*)?'(\d{4}-\d{2}-\d{2})'"
    between = re.search(
        rf'{col}\s+BETWEEN\s+{literal}\s+AND\s+{literal}',
        source, re.I | re.S,
    )
    if between:
        begin = date.fromisoformat(between.group(1))
        actual_last = date.fromisoformat(between.group(2))
        try:
            exclusive_end = begin.replace(year=begin.year + 1)
        except ValueError as exc:
            raise ValueError('Q05 unsupported February 29 year range') from exc
        if actual_last != exclusive_end - timedelta(days=1):
            raise ValueError('Q05 BETWEEN range is not exactly one calendar year')
    else:
        start = re.search(rf'{col}\s*>=\s*\(*\s*{literal}', source, re.I | re.S)
        upper = re.search(
            rf'{col}\s*(<|<=)\s*\(*\s*{literal}',
            source, re.I | re.S,
        )
        if not start or not upper:
            # Keep a useful diagnostic without printing SQL or other secrets.
            date_ops = re.findall(rf'{col}\s*(?:BETWEEN|>=|>|<=|<)',source,re.I)
            raise ValueError(
                'Q05 source SQL date filter is not recognized as a one-year '
                'range (supported: >= and </<=, or BETWEEN; ISO DATE literals '
                'and CAST(... AS DATE)). Found date operators: '
                + repr([re.sub(r'\s+', ' ', op) for op in date_ops])
            )
        begin = date.fromisoformat(start.group(1))
        upper_literal = date.fromisoformat(upper.group(2))
        try:
            exclusive_end = begin.replace(year=begin.year + 1)
        except ValueError as exc:
            raise ValueError('Q05 unsupported February 29 year range') from exc
        remainder = source[upper.end():]
        # Optional one-year interval on the upper-bound date literal.
        interval = re.match(
            r"\s*\+\s*INTERVAL\s*(?:'\s*1\s*'\s*YEAR\b|'\s*1\s+YEAR\s*'|1\s+YEAR\b)",
            remainder, re.I,
        )
        if interval:
            if upper_literal != begin or upper.group(1) != '<':
                raise ValueError('Q05 one-year interval upper bound is inconsistent')
        else:
            expected_upper = (exclusive_end - timedelta(days=1)) if upper.group(1) == '<=' else exclusive_end
            if upper_literal != expected_upper:
                raise ValueError('Q05 explicit upper date is not the requested one-year boundary')

    # The remaining Q05 plans are parameterized by [begin,exclusive_end).
    return {
        'region': region.group(1).replace("''", "'"),
        'start_date': str(begin), 'end_date_exclusive': str(exclusive_end),
    }


def root_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def inputs(args):
    project = root_dir()
    base = Path(args.data_dir).expanduser() if args.data_dir else project / 'data' / 'tpch' / f'parquet_sf{args.sf}'
    sql = Path(args.sql).expanduser() if args.sql else project / 'queries' / 'tpch' / 'q05.sql'
    if not sql.is_file():
        raise FileNotFoundError(f'Missing archived Q05 SQL: {sql}')
    paths = {name: base / f'{name}.parquet' for name in TABLE_LOCATION}
    missing = [str(p) for p in paths.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError('Missing SF1 Parquet files: ' + ', '.join(missing))
    return sql, paths


def output_root(args):
    return (Path(args.results_root).expanduser() if args.results_root else
            root_dir() / 'results' / 'experiments' / 'q05') / f'sf{args.sf}' / 'p1'


def q05_run_root(args):
    """Isolate prospective optimizer evidence from historical four-plan baselines."""
    base = output_root(args)
    return base/'optimizer_prospective' if getattr(args,'prospective_run',False) else base


def make_connection(duckdb, threads):
    con = duckdb.connect(':memory:')
    con.execute(f'SET threads={int(threads)}')
    return con


def source_reference(con, paths, sql):
    for name, path in paths.items():
        con.execute(f'CREATE VIEW {name} AS SELECT * FROM {parquet(path)}')
    try:
        rows = con.execute(sql).fetchall()
        return canonical(rows), [list(map(str, row)) for row in rows]
    finally:
        for name in paths:
            con.execute(f'DROP VIEW {name}')


def verify_sf1_keys(con, paths, params):
    """Primary key assumptions required by semijoin transformations."""
    keys = {
        'customer':'c_custkey','orders':'o_orderkey',
        'supplier':'s_suppkey','nation':'n_nationkey','region':'r_regionkey',
    }
    evidence = {}
    for name, key in keys.items():
        count, distinct_count = con.execute(
            f'SELECT COUNT(*), COUNT(DISTINCT {key}) FROM {parquet(paths[name])}'
        ).fetchone()
        duplicates = int(count - distinct_count)
        evidence[f'{name}_duplicate_keys'] = duplicates
        if duplicates:
            raise ValueError(f'Cannot safely apply Q05 semijoin plan: duplicate {name}.{key}')
    region_matches = con.execute(
        f'SELECT COUNT(*) FROM {parquet(paths["region"])} WHERE r_name={quote(params["region"])}'
    ).fetchone()[0]
    evidence['selected_region_rows'] = int(region_matches)
    if region_matches != 1:
        raise ValueError('Q05 semijoin plan requires exactly one matching region')
    return evidence


def sql_parts(paths, params):
    s = {name: parquet(path) for name,path in paths.items()}
    begin = quote(params['start_date'])
    end = quote(params['end_date_exclusive'])
    return {
        **s,
        'orders_year': (
            f'SELECT o_orderkey,o_custkey FROM {s["orders"]} '
            f'WHERE o_orderdate >= CAST({begin} AS DATE) AND o_orderdate < CAST({end} AS DATE)'
        ),
        'supplier_region': (
            f'SELECT s_suppkey,s_nationkey,r_regionkey FROM {s["supplier"]} '
            f'CROSS JOIN {s["region"]} WHERE r_name={quote(params["region"])}'
        ),
        'region_keys': (
            f'SELECT r_regionkey FROM {s["region"]} WHERE r_name={quote(params["region"])}'
        ),
    }


def relation_ref(sql: str, alias: str) -> str:
    """Render an accepted Q05 relation in FROM/JOIN position.

    A SELECT needs parentheses as a derived table. DuckDB table functions
    (including read_parquet(...)) must NOT be enclosed as `(read_parquet(...))`:
    that is a parenthesized expression, not a valid relation in a JOIN.
    """
    source = sql.strip()
    if not re.fullmatch(r'[a-z][a-z0-9_]*', alias):
        raise ValueError('Invalid SQL relation alias')
    if re.match(r'^(?:SELECT|WITH)\b', source, flags=re.I):
        return f'({source}) {alias}'
    if re.match(r'^read_parquet\s*\(', source, flags=re.I):
        return f'{source} {alias}'
    raise ValueError('Unexpected Q05 relation; expected a SELECT or read_parquet()')


def joined_final(orders_sql, line_sql, customer_sql, nation_sql,
                 supplier_sql, *, aggregate_lines=False, region_sql=None):
    """Exact Q05 multiway join after staged exchanges.

    supplier_sql has s_suppkey, s_nationkey, r_regionkey if region_sql=None;
    otherwise supplier_sql has two supplier fields and region_sql has r_regionkey.
    customer_sql has c_custkey, c_nationkey; nation_sql carries n_regionkey.
    """
    if aggregate_lines:
        rev = 'l.revenue'
    else:
        rev = 'l.l_extendedprice * (1 - l.l_discount)'
    if region_sql is None:
        region_join = 'AND n.n_regionkey = s.r_regionkey '
    else:
        region_join = f'JOIN {relation_ref(region_sql, "r")} ON n.n_regionkey=r.r_regionkey '
    return (
        f'SELECT n.n_name, SUM({rev}) AS revenue '
        f'FROM {relation_ref(orders_sql, "o")} '
        f'JOIN {relation_ref(customer_sql, "c")} ON o.o_custkey=c.c_custkey '
        f'JOIN {relation_ref(line_sql, "l")} ON l.l_orderkey=o.o_orderkey '
        f'JOIN {relation_ref(supplier_sql, "s")} ON l.l_suppkey=s.s_suppkey '
        f'JOIN {relation_ref(nation_sql, "n")} ON n.n_nationkey=s.s_nationkey '
        f'AND c.c_nationkey=s.s_nationkey {region_join}'
        'GROUP BY n.n_name ORDER BY revenue DESC'
    )


def execute_plan(duckdb, paths, params, plan, threads, run_dir, trial):
    """Materialize actual Parquet intermediates, then reread them for joins.

    Staging files exist only during the local trial. Their SHA256, exact byte
    counts and row counts are retained; `audit` does not claim to rehash
    deleted intermediates. The final result and SQL are retained per trial.
    """
    con = make_connection(duckdb, threads)
    fragments = sql_parts(paths, params)
    edges = []
    stage_total = 0.0
    start_total = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix=f'egressopt_q05_{plan}_') as temp:
        tmp = Path(temp)

        def stage(source, destination, label, query):
            nonlocal stage_total
            if source == destination:
                raise ValueError('Internal staging error: same source and destination')
            if not re.fullmatch(r'[a-z][a-z0-9_]*', label):
                raise ValueError('Invalid stage name')
            out = tmp / f'{len(edges):02d}_{label}.parquet'
            began = time.perf_counter()
            con.execute(f'COPY ({query}) TO {quote(str(out))} (FORMAT PARQUET, COMPRESSION ZSTD)')
            stage_sec = time.perf_counter() - began
            rows = int(con.execute(f'SELECT COUNT(*) FROM {parquet(out)}').fetchone()[0])
            edge = {'label':label,'source_cloud':source,'destination_cloud':destination,
                    'rows':rows,'parquet_bytes':out.stat().st_size,
                    'sha256':sha_file(out),'stage_seconds':stage_sec,
                    'stage_sql_sha256':sha_bytes(query.encode('utf-8')),
                    'execution_mode':'local_emulation'}
            edges.append(edge)
            stage_total += stage_sec
            return parquet(out)

        if plan == 'azure_join':
            orders = stage('aws','azure','orders_year',fragments['orders_year'])
            suppliers = stage('gcp','azure','supplier_region',fragments['supplier_region'])
            final = joined_final(orders,fragments['lineitem'],fragments['customer'],
                                 fragments['nation'],suppliers)
        elif plan == 'aws_join':
            line = stage('azure','aws','lineitem_order_supplier_agg',(
                f'SELECT l_orderkey,l_suppkey, '
                f'SUM(l_extendedprice * (1 - l_discount)) AS revenue '
                f'FROM {fragments["lineitem"]} GROUP BY l_orderkey,l_suppkey'
            ))
            cust_nat = stage('azure','aws','customer_nation',(
                f'SELECT c_custkey,c_nationkey,n_name,n_regionkey,n_nationkey '
                f'FROM {fragments["customer"]} c JOIN {fragments["nation"]} n '
                f'ON c.c_nationkey=n.n_nationkey'
            ))
            suppliers = stage('gcp','aws','supplier_region',fragments['supplier_region'])
            # Customer and nation are already joined on Azure. Rejoin the
            # projected dimension columns on AWS without duplicating rows.
            customer = f'SELECT c_custkey,c_nationkey FROM {cust_nat}'
            nation = f'SELECT DISTINCT n_nationkey,n_name,n_regionkey FROM {cust_nat}'
            final = joined_final(fragments['orders_year'],line,customer,nation,suppliers,
                                 aggregate_lines=True)
        elif plan == 'gcp_join':
            orders = stage('aws','azure','orders_year',fragments['orders_year'])
            partial = (
                f'SELECT l.l_suppkey,c.c_nationkey,n.n_name,n.n_regionkey, '
                f'l.l_extendedprice * (1-l.l_discount) AS revenue '
                f'FROM {fragments["lineitem"]} l '
                f'JOIN {relation_ref(orders, "o")} ON l.l_orderkey=o.o_orderkey '
                f'JOIN {fragments["customer"]} c ON o.o_custkey=c.c_custkey '
                f'JOIN {fragments["nation"]} n ON n.n_nationkey=c.c_nationkey'
            )
            stage_part = stage('azure','gcp','joined_partial',partial)
            final = (
                f'SELECT p.n_name,SUM(p.revenue) AS revenue FROM {stage_part} p '
                f'JOIN {fragments["supplier"]} s ON p.l_suppkey=s.s_suppkey '
                f'AND p.c_nationkey=s.s_nationkey '
                f'JOIN {fragments["region"]} r ON p.n_regionkey=r.r_regionkey '
                f'WHERE r.r_name={quote(params["region"])} '
                'GROUP BY p.n_name ORDER BY revenue DESC'
            )
        elif plan == 'semijoin_azure':
            region = stage('gcp','azure','region_keys',fragments['region_keys'])
            eligible_customers = stage('azure','aws','eligible_customer_keys',(
                f'SELECT c.c_custkey FROM {fragments["customer"]} c '
                f'JOIN {fragments["nation"]} n ON c.c_nationkey=n.n_nationkey '
                f'JOIN {region} r ON n.n_regionkey=r.r_regionkey'
            ))
            orders = stage('aws','azure','semijoin_filtered_orders',(
                f'SELECT o.o_orderkey,o.o_custkey FROM ({fragments["orders_year"]}) o '
                f'SEMI JOIN {eligible_customers} c ON o.o_custkey=c.c_custkey'
            ))
            eligible_nations = stage('azure','gcp','eligible_nations',(
                f'SELECT n.n_nationkey FROM {fragments["nation"]} n '
                f'JOIN {region} r ON n.n_regionkey=r.r_regionkey'
            ))
            suppliers = stage('gcp','azure','region_filtered_suppliers',(
                f'SELECT s.s_suppkey,s.s_nationkey FROM {fragments["supplier"]} s '
                f'SEMI JOIN {eligible_nations} n ON s.s_nationkey=n.n_nationkey'
            ))
            final = joined_final(orders,fragments['lineitem'],fragments['customer'],
                                 fragments['nation'],suppliers,region_sql=region)
        else:
            raise ValueError(f'Unknown Q05 plan {plan}')

        explain = con.execute('EXPLAIN ' + final).fetchall()
        joined_at = time.perf_counter()
        raw = con.execute(final).fetchall()
        join_secs = time.perf_counter() - joined_at
        total_secs = time.perf_counter() - start_total
        result = canonical(raw)
        record = {
            'query_id':QUERY_ID,'plan_id':plan,'trial':trial,'execution_mode':'local_emulation',
            'scale_factor':1,'placement_id':PLACEMENT_ID,
            'transfer_edges':edges,
            'intermediate_parquet_bytes':sum(x['parquet_bytes'] for x in edges),
            'intermediate_rows':sum(x['rows'] for x in edges),
            'stage_seconds':stage_total,'join_aggregate_seconds':join_secs,
            'total_seconds':total_secs,'result':result,'result_sha256':result_hash(result),
            'raw_result':[[str(v) for v in row] for row in raw],
            'physical_plan':str(explain),
            'network_bytes_measured':False,'network_seconds_measured':False,
            'egress_usd_estimated':None,
            'timestamp_utc':utcnow(),
        }
    con.close()
    return record


def summary_row(run_id, rec, reference_hash):
    return {
        'run_id':run_id,'query_id':QUERY_ID,'scale_factor':1,
        'placement_id':PLACEMENT_ID,'execution_mode':rec['execution_mode'],
        'plan_id':rec['plan_id'],'trial':rec['trial'],
        'intermediate_parquet_bytes':rec['intermediate_parquet_bytes'],
        'intermediate_rows':rec['intermediate_rows'],
        'edge_count':len(rec['transfer_edges']),
        'stage_seconds':rec['stage_seconds'],
        'join_aggregate_seconds':rec['join_aggregate_seconds'],
        'total_seconds':rec['total_seconds'],
        'result_sha256':rec['result_sha256'],
        'correct_vs_reference':rec['result_sha256']==reference_hash,
        'network_bytes_measured':False,'network_seconds_measured':False,
        'egress_usd_estimated':'','timestamp_utc':rec['timestamp_utc'],
    }


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def prepare(args):
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError('DuckDB is required: activate your existing egressopt environment') from exc
    sql_path, paths = inputs(args)
    source_sql = sql_path.read_text(encoding='utf-8')
    params = source_parameters(source_sql)
    reference_con = make_connection(duckdb, args.threads)
    reference, reference_raw = source_reference(reference_con, paths, source_sql)
    keys = verify_sf1_keys(reference_con, paths, params)
    reference_con.close()
    if not reference:
        raise ValueError('Q05 source SQL produced zero groups; refusing empty reference benchmark')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8]
    run_path = output_root(args) / run_id
    run_path.mkdir(parents=True, exist_ok=False)
    manifest = {
        'query_id':QUERY_ID,'run_id':run_id,'sf':args.sf,'placement':PLACEMENT_ID,
        'execution_mode':'local_emulation','status':'RUNNING',
        'created_utc':utcnow(), 'source_sql':str(sql_path.resolve()),
        'source_sql_sha256':sha_file(sql_path),'source_sql_text':source_sql,
        'parameters':params,'reference_result':reference,'reference_raw':reference_raw,
        'reference_sha256':result_hash(reference),'key_validation':keys,
        'data_sources': {table: {'cloud':TABLE_LOCATION[table],
                     'file':str(path.resolve()),'bytes':path.stat().st_size,
                     'sha256':sha_file(path)} for table,path in paths.items()},
        'code_sha256':sha_file(Path(__file__)),'python_version':platform.python_version(),
        'duckdb_version':duckdb.__version__,'os_platform':platform.platform(),
        'threads':args.threads,'trials_per_plan':args.trials,
        'plan_order':list(PLANS),'expected_edges':EXPECTED_EDGES,
        'limitation':'LOCAL PARQUET EXCHANGE EMULATION, NOT MEASURED CLOUD EGRESS',
    }
    write_json(run_path/'run.json',manifest)
    records = []
    try:
        for trial in range(1,args.trials+1):
            # Rotate order across trials without affecting user-specified plan set.
            rotation = (trial-1) % len(PLANS)
            for plan in PLANS[rotation:] + PLANS[:rotation]:
                rec = execute_plan(duckdb, paths, params, plan, args.threads, run_path, trial)
                ok = rec['result_sha256']==manifest['reference_sha256']
                rec['correct_vs_reference'] = ok
                rec['run_id'] = run_id
                plan_dir = run_path/'trials'/plan
                plan_dir.mkdir(parents=True,exist_ok=True)
                write_json(plan_dir/f'trial_{trial:03d}.json', rec)
                records.append(summary_row(run_id,rec,manifest['reference_sha256']))
                write_csv(run_path/'trial_metrics.csv',records,METRIC_FIELDS)
                print(f'{plan:<20} trial={trial} correctness={"PASS" if ok else "FAIL"} '
                      f'emulated_intermediate={rec["intermediate_parquet_bytes"]:,} bytes '
                      f'edges={len(rec["transfer_edges"])} '
                      f'local_workflow={rec["total_seconds"]:.3f}s',flush=True)
                if not ok:
                    raise ValueError(f'Q05 plan {plan} diverged from archived SQL reference')
        manifest['status']='COMPLETED'
        manifest['completed_utc']=utcnow()
        manifest['validated_trial_count']=len(records)
        write_json(run_path/'run.json',manifest)
        print('Q05_LOCAL_PLAN_VALIDATION_PASS')
        print(f'Q05 local evidence: {run_path}')
        print('NOTE: NO inter-cloud bytes, network times, or billed egress dollars were measured.')
        return 0
    except Exception as exc:
        manifest['status']='FAILED'
        manifest['failure']=f'{type(exc).__name__}: {exc}'
        write_json(run_path/'run.json',manifest)
        print(f'Q05_LOCAL_PLAN_VALIDATION_FAILED: {exc}',file=sys.stderr)
        raise


def find_run(args):
    base = q05_run_root(args)
    if args.run_id:
        directory = base / args.run_id
        if not directory.is_dir():
            raise FileNotFoundError(f'Q05 run not found: {directory}')
        return directory
    if not base.is_dir():
        raise FileNotFoundError(f'No Q05 evidence at {base}; run --mode prepare first')
    complete = []
    for path in base.iterdir():
        if path.is_dir() and (path/'run.json').exists():
            try:
                data=read_json(path/'run.json')
                if data.get('status')=='COMPLETED' and (path/'trial_metrics.csv').is_file():
                    complete.append(path)
            except Exception:
                pass
    if not complete:
        raise FileNotFoundError('No completed Q05 evidence; run --mode prepare first')
    return sorted(complete)[-1]


def audit_evidence(directory: Path):
    """Validate archived trial/summary consistency, source provenance and hashes.

    This does not claim independently verified network bytes or independently
    re-executed source SQL. Deleted intermediate Parquet cannot be rehashed.
    """
    run=read_json(directory/'run.json')
    if run.get('status')!='COMPLETED':
        raise ValueError('Cannot audit incomplete/failed Q05 experiment')
    if run.get('query_id')!=QUERY_ID or run.get('sf')!=1 or run.get('placement')!=PLACEMENT_ID:
        raise ValueError('Incorrect Q05 run identity')
    if not isinstance(run.get('reference_result'),list):
        raise ValueError('Missing source-SQL Q05 reference')
    reference = result_hash(run['reference_result'])
    if reference != run.get('reference_sha256'):
        raise ValueError('Source reference hash corrupted')
    if sha_bytes(run['source_sql_text'].encode('utf-8'))!=run.get('source_sql_sha256'):
        raise ValueError('Archived SQL text digest mismatch')
    if source_parameters(run['source_sql_text'])!=run['parameters']:
        raise ValueError('Archived SQL parameters mismatch')
    with (directory/'trial_metrics.csv').open(newline='',encoding='utf-8') as f:
        metric_rows = list(csv.DictReader(f))
    expected_count = int(run['trials_per_plan'])*len(PLANS)
    if len(metric_rows)!=expected_count or expected_count!=int(run['validated_trial_count']):
        raise ValueError('Missing/extra Q05 metric rows')
    observed = set()
    for met in metric_rows:
        plan=met['plan_id']; trial=int(met['trial'])
        if plan not in PLANS or not (1<=trial<=run['trials_per_plan']):
            raise ValueError('Unknown plan/trial in CSV')
        key=(plan,trial)
        if key in observed:
            raise ValueError('Duplicated plan/trial')
        observed.add(key)
        rec_path=directory/'trials'/plan/f'trial_{trial:03d}.json'
        rec=read_json(rec_path)
        if rec['run_id']!=run['run_id'] or rec['plan_id']!=plan or rec['trial']!=trial:
            raise ValueError('Trial JSON identity mismatch')
        if rec['execution_mode']!=run['execution_mode'] or met['execution_mode']!=run['execution_mode']:
            raise ValueError('Trial execution mode mismatch')
        if rec['result_sha256']!=reference or result_hash(rec['result'])!=reference:
            raise ValueError('Plan output differs from source SQL reference')
        if len(rec['transfer_edges'])!=len(EXPECTED_EDGES[plan]):
            raise ValueError('Incorrect number of transfer edges')
        if [(x['source_cloud'],x['destination_cloud']) for x in rec['transfer_edges']]!=EXPECTED_EDGES[plan]:
            raise ValueError('Wrong transfer direction')
        if any(not re.fullmatch(r'[0-9a-f]{64}',e['sha256']) for e in rec['transfer_edges']):
            raise ValueError('Missing/corrupted Parquet SHA256')
        total_bytes=sum(int(x['parquet_bytes']) for x in rec['transfer_edges'])
        total_rows=sum(int(x['rows']) for x in rec['transfer_edges'])
        if total_bytes!=int(rec['intermediate_parquet_bytes']) or total_rows!=int(rec['intermediate_rows']):
            raise ValueError('Wrong intermediate bytes/row sum')
        if int(met['intermediate_parquet_bytes'])!=total_bytes or int(met['intermediate_rows'])!=total_rows:
            raise ValueError('Trial CSV and transfer JSON disagree')
        if int(met['edge_count'])!=len(rec['transfer_edges']):
            raise ValueError('Trial CSV and edge count disagree')
        for name in ('stage_seconds','join_aggregate_seconds','total_seconds'):
            value=float(rec[name]); actual=float(met[name])
            if not (0<=value<3600 and abs(value-actual)<=1e-9*max(1.,value)):
                raise ValueError(f'Timing inconsistency in {plan}: {name}')
        if not rec['correct_vs_reference'] or met['correct_vs_reference'] not in ('True','true','1'):
            raise ValueError('Incorrect correctness flag')
        if rec['network_bytes_measured'] or rec['network_seconds_measured'] or met['network_bytes_measured']!='False':
            raise ValueError('Local emulation mislabeled as real network measurement')
        if str(met['result_sha256'])!=reference:
            raise ValueError('Trial CSV result digest mismatch')
    return run, metric_rows


def audit(args):
    run_dir = find_run(args)
    if read_json(run_dir/'run.json').get('execution_mode')=='real_multicloud':
        run, records=q05_audit_cloud(run_dir)
        report={'audit_utc':utcnow(),'status':'PASS','run_id':run['run_id'],
                'audited_records':len(records),'execution_mode':'real_multicloud',
                'limitation':'Internal record consistency is checked; cloud computation is not rerun offline.'}
        write_json(run_dir/'q05_audit.json',report)
        print('Q05_EVIDENCE_AUDIT_PASS')
        print('Evidence: '+str(run_dir))
        return 0
    run, records = audit_evidence(run_dir)
    audit_report = {
        'audit_utc':utcnow(),'run_id':run['run_id'],
        'status':'PASS','audited_records':len(records),
        'reference_sha256':run['reference_sha256'],
        'limitations':[
            'Archived source SQL and trial records checked for consistency, not recomputed against cloud data.',
            'Staged Parquet files were ephemeral; their hashes cannot be independently reverified offline.',
            'This run contains local emulation bytes/timings, NOT measured inter-cloud network traffic.',
        ],
    }
    write_json(run_dir/'q05_audit.json',audit_report)
    print('Q05_EVIDENCE_AUDIT_PASS')
    print(f'Evidence: {run_dir}')
    return 0


def reproduce(args):
    directory=find_run(args)
    if read_json(directory/'run.json').get('execution_mode')=='real_multicloud':
        return q05_reproduce_cloud(directory,args)
    run, raw=audit_evidence(directory)
    analysis=directory/'q05_analysis'; analysis.mkdir(exist_ok=True)
    summary=[]
    for plan in PLANS:
        rows=[x for x in raw if x['plan_id']==plan]
        durations=[float(x['total_seconds']) for x in rows]
        sizes=[int(x['intermediate_parquet_bytes']) for x in rows]
        summary.append({
            'plan_id':plan,'execution_mode':'local_emulation',
            'n':len(rows),'mean_local_seconds':statistics.mean(durations),
            'median_local_seconds':statistics.median(durations),
            'sample_sd_local_seconds':statistics.stdev(durations) if len(durations)>1 else None,
            'mean_emulated_parquet_bytes':statistics.mean(sizes),
            'min_emulated_parquet_bytes':min(sizes),
            'max_emulated_parquet_bytes':max(sizes),
            'expected_edge_count':len(EXPECTED_EDGES[plan]),
            'correct_trials':len(rows),
        })
    write_json(analysis/'paper_summary.json',{
        'query_id':QUERY_ID,'run_id':run['run_id'],'source_execution_mode':'local_emulation',
        'values':summary,'limitation':'NOT real inter-cloud traffic, timing or egress cost',
    })
    write_csv(analysis/'plan_summary.csv',summary,list(summary[0]))
    lines = [
        '% Q05 SF1/P1 — LOCAL EMULATION ONLY, not cloud results',
        r'\begin{tabular}{lrrr}',r'\hline',
        r'Plan & Trials & Mean local time (ms) & Mean staged bytes \\',r'\hline',
    ]
    for s in summary:
        plan = s['plan_id'].replace('_', r'\_')
        ms = 1000*s['mean_local_seconds']
        byte_count=s['mean_emulated_parquet_bytes']
        lines.append(f'{plan} & {s["n"]} & {ms:.2f} & {byte_count:.0f} '+ r'\\')
    lines += [r'\hline',r'\end{tabular}']
    (analysis/'q05_local_table.tex').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    if args.plots:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        x = list(range(len(PLANS)))
        fig,ax=plt.subplots(figsize=(7.3,3.5))
        ax.bar(x,[s['mean_emulated_parquet_bytes']/1e6 for s in summary])
        ax.set_xticks(x,PLANS,rotation=18,ha='right')
        ax.set_ylabel('Total staged Parquet bytes (MB)')
        ax.set_title('Q05 SF1/P1 local emulation (NOT real cloud traffic)')
        fig.tight_layout()
        for extension in ('png','pdf'):
            fig.savefig(analysis/f'q05_local_staged_payload.{extension}',dpi=220)
        plt.close(fig)
        fig,ax=plt.subplots(figsize=(7.3,3.5))
        for idx,plan in enumerate(PLANS):
            points=[float(r['total_seconds'])*1000 for r in raw if r['plan_id']==plan]
            ax.scatter([idx]*len(points),points,label=plan)
        ax.set_xticks(x,PLANS,rotation=18,ha='right')
        ax.set_ylabel('Local workflow time (ms)')
        ax.set_title('Q05 SF1/P1 local emulation (NOT cloud latency)')
        fig.tight_layout()
        for extension in ('png','pdf'):
            fig.savefig(analysis/f'q05_local_latency.{extension}',dpi=220)
        plt.close(fig)
    print('Q05_REPRODUCE_PASS')
    print(f'Reproduced Q05 local tables/figures: {analysis}')
    return 0


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--mode',choices=('prepare','cloud','reproduce','audit','cleanup','optimizer-preview','optimizer-cloud'),required=True)
    parser.add_argument('--sf',type=int,default=DEFAULT_SF)
    parser.add_argument('--trials',type=int,default=10)
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--warmup-trials',type=int,default=2)
    parser.add_argument('--stop-all-after-run',action=argparse.BooleanOptionalAction,default=True)
    parser.add_argument('--plots',action=argparse.BooleanOptionalAction,default=True)
    parser.add_argument('--data-dir',type=str,default=None)
    parser.add_argument('--sql',type=str,default=None)
    parser.add_argument('--results-root',type=str,default=None)
    parser.add_argument('--run-id',type=str,default=None)
    parser.add_argument('--prospective-run',action='store_true',default=False,
                        help='Audit/reproduce/repair an optimizer_prospective run rather than a baseline run')
    parser.add_argument('--optimizer-archive',default='EgressOpt_Paper_Input.zip')
    parser.add_argument('--optimizer-tariffs',default='tariffs_public_list_marginal.json')
    parser.add_argument('--optimizer-delta',type=float,default=0.5,
                        help='Maximum predicted slowdown fraction relative to predicted fastest plan')
    args=parser.parse_args(argv)
    if args.sf not in VALIDATED_SF:
        parser.error(f'SF{args.sf} has not been generated, deployed and reference-validated. Currently SF1 only.')
    if args.trials<1 or args.threads<1 or args.warmup_trials<0 or args.trials>100 or args.warmup_trials>20:
        parser.error('--trials and --threads must be positive')
    try:
        if args.mode=='prepare':return prepare(args)
        if args.mode=='audit':return audit(args)
        if args.mode=='reproduce':return reproduce(args)
        if args.mode=='cloud':return q05_cloud(args)
        if args.mode in ('optimizer-preview','optimizer-cloud'):
            decision=q05_optimizer_decision(args)
            print('Q05_EGRESSOPT_PREDECISION',json.dumps({
                'selected_plan':decision['selected_plan'],
                'predeclared_baseline':decision['fastest_plan'],
                'estimated_slo_ms':decision['predicted_slo_ms'],
                'pricing_scenario':decision['tariff_scenario_id'],
                'tariff_vs_bytes_same':decision['selected_plan']==decision['bytes_only_plan']},
                sort_keys=True),flush=True)
            if args.mode=='optimizer-preview':
                print('Q05_EGRESSOPT_PREVIEW_PASS (NO cloud resources started)')
                return 0
            args.prospective_run=True
            args._q05_optimizer_decision=decision
            args._q05_run_plans=tuple(dict.fromkeys((decision['selected_plan'],decision['fastest_plan'])))
            return q05_cloud(args)
        if args.mode=='cleanup':return q05_cleanup_failed_run(args)
        raise RuntimeError('Unknown Q05 mode')
    except Exception as exc:
        print(f'Q05 ERROR: {type(exc).__name__}: {exc}',file=sys.stderr)
        return 1



Q05_WORKER_SOURCE = '#!/usr/bin/env python3\n"""Embedded, short-lived, authenticated Q05 cloud worker. Bundled inside q05.py."""\nimport argparse, base64, gzip, hashlib, hmac, json, os, re, ssl, time\nfrom datetime import datetime\nfrom decimal import Decimal, ROUND_HALF_EVEN\nfrom http.server import ThreadingHTTPServer, BaseHTTPRequestHandler\nfrom pathlib import Path\nimport duckdb\n\nCLOUD=os.environ[\'Q05_CLOUD\']\nROOT=Path(os.environ[\'Q05_RUNTIME\'])\nDATA=Path(os.environ[\'Q05_DATA_DIR\'])\nTOKEN=os.environ[\'Q05_WORKER_TOKEN\']\nREGION=os.environ[\'Q05_REGION\']\nSTART=os.environ[\'Q05_START_DATE\']\nEND=os.environ[\'Q05_END_DATE\']\nPORT=8443\nPLANS=(\'azure_join\',\'aws_join\',\'gcp_join\',\'semijoin_azure\')\nSLUG=re.compile(r\'[wm][0-9]{2,3}\')\nSHA=re.compile(r\'[0-9a-f]{64}\')\nPATHS={x:DATA/(x+\'.parquet\') for x in (\'orders\',\'customer\',\'lineitem\',\'nation\',\'supplier\',\'region\')}\nPIPE={\n \'azure_join\': [(\'aws\',\'azure\',\'orders_year\'),(\'gcp\',\'azure\',\'supplier_region\')],\n \'aws_join\': [(\'azure\',\'aws\',\'lineitem_order_supplier_agg\'),(\'azure\',\'aws\',\'customer_nation\'),(\'gcp\',\'aws\',\'supplier_region\')],\n \'gcp_join\': [(\'aws\',\'azure\',\'orders_year\'),(\'azure\',\'gcp\',\'joined_partial\')],\n \'semijoin_azure\': [(\'gcp\',\'azure\',\'region_keys\'),(\'azure\',\'aws\',\'eligible_customer_keys\'),(\'aws\',\'azure\',\'semijoin_filtered_orders\'),(\'azure\',\'gcp\',\'eligible_nations\'),(\'gcp\',\'azure\',\'region_filtered_suppliers\')],\n}\n\ndef lit(x):return "\'"+str(x).replace("\'","\'\'")+"\'"\ndef scan(path):return \'read_parquet(\'+lit(str(path))+\')\'\ndef ref(sql,alias):\n if sql.lstrip().upper().startswith(\'SELECT \'):return \'(\'+sql+\') \'+alias\n return sql+\' \'+alias\n\ndef local(table):\n if {\'orders\':\'aws\',\'customer\':\'azure\',\'lineitem\':\'azure\',\'nation\':\'azure\',\'supplier\':\'gcp\',\'region\':\'gcp\'}[table]!=CLOUD:\n  raise RuntimeError(\'Q05 cannot read \'+table+\' on \'+CLOUD)\n if not PATHS[table].is_file():raise RuntimeError(\'Q05 missing \'+table)\n return scan(PATHS[table])\n\ndef recv(trial,plan,part):\n path=ROOT/\'received\'/trial/plan/(part+\'.parquet\')\n if not path.is_file():raise RuntimeError(\'Missing transferred dependency: \'+part)\n return scan(path)\n\ndef fragment(trial,plan,part):\n if part==\'orders_year\':\n  return (\'SELECT o_orderkey,o_custkey FROM \'+local(\'orders\')+\n          \' WHERE o_orderdate >= CAST(\'+lit(START)+\' AS DATE) AND o_orderdate < CAST(\'+lit(END)+\' AS DATE)\')\n if part==\'supplier_region\':\n  return (\'SELECT s_suppkey,s_nationkey,r_regionkey FROM \'+local(\'supplier\')+\n          \' CROSS JOIN \'+local(\'region\')+\' WHERE r_name=\'+lit(REGION))\n if part==\'region_keys\':\n  return \'SELECT r_regionkey FROM \'+local(\'region\')+\' WHERE r_name=\'+lit(REGION)\n if part==\'lineitem_order_supplier_agg\':\n  return (\'SELECT l_orderkey,l_suppkey,SUM(l_extendedprice*(1-l_discount)) AS revenue FROM \'+\n           local(\'lineitem\')+\' GROUP BY l_orderkey,l_suppkey\')\n if part==\'customer_nation\':\n  return (\'SELECT c_custkey,c_nationkey,n_name,n_regionkey,n_nationkey FROM \'+\n     local(\'customer\')+\' c JOIN \'+local(\'nation\')+\' n ON c.c_nationkey=n.n_nationkey\')\n if part==\'joined_partial\':\n  o=recv(trial,plan,\'orders_year\')\n  return (\'SELECT l.l_suppkey,c.c_nationkey,n.n_name,n.n_regionkey,\'+\n          \'l.l_extendedprice*(1-l.l_discount) AS revenue FROM \'+local(\'lineitem\')+\' l \'+\n          \'JOIN \'+ref(o,\'o\')+\' ON l.l_orderkey=o.o_orderkey \'+\n          \'JOIN \'+local(\'customer\')+\' c ON o.o_custkey=c.c_custkey \'+\n          \'JOIN \'+local(\'nation\')+\' n ON n.n_nationkey=c.c_nationkey\')\n if part==\'eligible_customer_keys\':\n  r=recv(trial,plan,\'region_keys\')\n  return (\'SELECT c.c_custkey FROM \'+local(\'customer\')+\' c \'+\n          \'JOIN \'+local(\'nation\')+\' n ON c.c_nationkey=n.n_nationkey \'+\n          \'JOIN \'+ref(r,\'r\')+\' ON n.n_regionkey=r.r_regionkey\')\n if part==\'semijoin_filtered_orders\':\n  o=fragment(trial,plan,\'orders_year\')\n  c=recv(trial,plan,\'eligible_customer_keys\')\n  return (\'SELECT o.o_orderkey,o.o_custkey FROM (\'+o+\') o \'+\n          \'SEMI JOIN \'+ref(c,\'c\')+\' ON o.o_custkey=c.c_custkey\')\n if part==\'eligible_nations\':\n  r=recv(trial,plan,\'region_keys\')\n  return (\'SELECT n.n_nationkey FROM \'+local(\'nation\')+\' n \'+\n          \'JOIN \'+ref(r,\'r\')+\' ON n.n_regionkey=r.r_regionkey\')\n if part==\'region_filtered_suppliers\':\n  n=recv(trial,plan,\'eligible_nations\')\n  return (\'SELECT s.s_suppkey,s.s_nationkey FROM \'+local(\'supplier\')+\' s \'+\n          \'SEMI JOIN \'+ref(n,\'n\')+\' ON s.s_nationkey=n.n_nationkey\')\n raise RuntimeError(\'Unknown Q05 stage part\')\n\ndef joined_final(orders_sql,line_sql,customer_sql,nation_sql,supplier_sql,aggregate_lines=False,region_sql=None):\n rev=\'l.revenue\' if aggregate_lines else \'l.l_extendedprice*(1-l.l_discount)\'\n if region_sql is None:\n  region_join=\'AND n.n_regionkey=s.r_regionkey \'\n else:\n  region_join=\'JOIN \'+ref(region_sql,\'r\')+\' ON n.n_regionkey=r.r_regionkey \'\n return (\'SELECT n.n_name,SUM(\'+rev+\') AS revenue FROM \'+ref(orders_sql,\'o\')+\' \'+\n     \'JOIN \'+ref(customer_sql,\'c\')+\' ON o.o_custkey=c.c_custkey \'+\n     \'JOIN \'+ref(line_sql,\'l\')+\' ON l.l_orderkey=o.o_orderkey \'+\n     \'JOIN \'+ref(supplier_sql,\'s\')+\' ON l.l_suppkey=s.s_suppkey \'+\n     \'JOIN \'+ref(nation_sql,\'n\')+\' ON n.n_nationkey=s.s_nationkey \'+\n     \'AND c.c_nationkey=s.s_nationkey \'+region_join+\n     \'GROUP BY n.n_name ORDER BY revenue DESC\')\n\ndef final_sql(trial,plan):\n if plan==\'azure_join\' and CLOUD==\'azure\':\n  return joined_final(recv(trial,plan,\'orders_year\'),local(\'lineitem\'),local(\'customer\'),\n      local(\'nation\'),recv(trial,plan,\'supplier_region\'))\n if plan==\'aws_join\' and CLOUD==\'aws\':\n  cn=recv(trial,plan,\'customer_nation\')\n  return joined_final(\'SELECT o_orderkey,o_custkey FROM (\'+fragment(trial,plan,\'orders_year\')+\')\',\n    recv(trial,plan,\'lineitem_order_supplier_agg\'),\n    \'SELECT c_custkey,c_nationkey FROM \'+cn,\n    \'SELECT DISTINCT n_nationkey,n_name,n_regionkey FROM \'+cn,\n    recv(trial,plan,\'supplier_region\'),aggregate_lines=True)\n if plan==\'gcp_join\' and CLOUD==\'gcp\':\n  p=recv(trial,plan,\'joined_partial\')\n  return (\'SELECT p.n_name,SUM(p.revenue) AS revenue FROM \'+p+\' p \'+\n      \'JOIN \'+local(\'supplier\')+\' s ON p.l_suppkey=s.s_suppkey \'+\n      \'AND p.c_nationkey=s.s_nationkey \'+\n      \'JOIN \'+local(\'region\')+\' r ON p.n_regionkey=r.r_regionkey \'+\n      \'WHERE r.r_name=\'+lit(REGION)+\' GROUP BY p.n_name ORDER BY revenue DESC\')\n if plan==\'semijoin_azure\' and CLOUD==\'azure\':\n  return joined_final(recv(trial,plan,\'semijoin_filtered_orders\'),local(\'lineitem\'),\n       local(\'customer\'),local(\'nation\'),recv(trial,plan,\'region_filtered_suppliers\'),\n       region_sql=recv(trial,plan,\'region_keys\'))\n raise RuntimeError(\'Wrong Q05 finish destination\')\n\ndef canonical(raw):\n out=[]; names=set()\n for row in raw:\n  if len(row)!=2 or row[0] is None or row[1] is None:raise RuntimeError(\'Bad Q05 result row\')\n  name=str(row[0]); amount=Decimal(str(row[1]))\n  if name in names or not amount.is_finite():raise RuntimeError(\'Duplicate/nonfinite Q05 result\')\n  names.add(name)\n  out.append({\'n_name\':name,\'revenue\':format(amount.quantize(Decimal(\'0.01\'),rounding=ROUND_HALF_EVEN),\'.2f\')})\n return sorted(out,key=lambda x:x[\'n_name\'])\n\ndef sha(path):\n h=hashlib.sha256()\n with path.open(\'rb\') as f:\n  for b in iter(lambda:f.read(1024*1024),b\'\'):h.update(b)\n return h.hexdigest()\n\nclass Handler(BaseHTTPRequestHandler):\n server_version=\'EgressOptQ05/1.0\'\n def log_message(self,*args):pass\n def response(self,code,obj):\n  body=json.dumps(obj,sort_keys=True,separators=(\',\',\':\')).encode()\n  self.send_response(code);self.send_header(\'Content-Type\',\'application/json\')\n  self.send_header(\'Content-Length\',str(len(body)));self.end_headers();self.wfile.write(body)\n def auth(self):\n  if not hmac.compare_digest(self.headers.get(\'Authorization\',\'\'),\'Bearer \'+TOKEN):\n   self.response(401,{\'error\':\'Unauthorized\'});return False\n  return True\n def info(self):\n  n=int(self.headers.get(\'Content-Length\',\'0\'))\n  if not 1<=n<=2048:raise ValueError(\'Invalid JSON length\')\n  obj=json.loads(self.rfile.read(n))\n  trial=obj.get(\'trial_id\');plan=obj.get(\'plan_id\')\n  if not isinstance(trial,str) or not SLUG.fullmatch(trial) or plan not in PLANS:\n   raise ValueError(\'Invalid trial/plan\')\n  return obj,trial,plan\n def do_GET(self):\n  if not self.auth():return\n  if self.path==\'/health\':self.response(200,{\'status\':\'ok\',\'cloud\':CLOUD,\'query\':\'Q05\',\'duckdb\':duckdb.__version__})\n  else:self.response(404,{\'error\':\'not_found\'})\n def do_POST(self):\n  if not self.auth():return\n  try:\n   if self.path==\'/q05/stage\':\n    obj,trial,plan=self.info();part=obj.get(\'part\')\n    if (CLOUD,part) not in [(a,p) for a,b,p in PIPE[plan]]:\n     raise ValueError(\'Stage/plan/cloud mismatch\')\n    query=fragment(trial,plan,part)\n    out=ROOT/\'stage\'/trial/plan/(part+\'.parquet\');out.parent.mkdir(parents=True,exist_ok=True)\n    start=time.perf_counter()\n    with duckdb.connect(\':memory:\') as con:\n     con.execute(\'SET threads=4\')\n     con.execute(\'COPY (\'+query+\') TO \'+lit(str(out))+\' (FORMAT PARQUET,COMPRESSION ZSTD)\')\n     rows=int(con.execute(\'SELECT COUNT(*) FROM \'+scan(out)).fetchone()[0])\n    self.response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'part\':part,\n      \'rows\':rows,\'parquet_bytes\':out.stat().st_size,\'sha256\':sha(out),\n      \'stage_seconds\':time.perf_counter()-start,\'file_path\':str(out)})\n   elif self.path.startswith(\'/q05/upload/\'):\n    path=self.path.split(\'/\')\n    if len(path)!=6 or path[1:3]!=[\'q05\',\'upload\']:\n     raise ValueError(\'Bad Q05 upload path\')\n    trial,plan,part=path[3:]\n    if not SLUG.fullmatch(trial) or plan not in PIPE or (CLOUD,part) not in [(b,p) for a,b,p in PIPE[plan]]:\n     raise ValueError(\'Unexpected Q05 upload\')\n    expected=self.headers.get(\'X-Q05-SHA256\',\'\')\n    if not SHA.fullmatch(expected):raise ValueError(\'Invalid SHA header\')\n    n=int(self.headers.get(\'Content-Length\',\'0\'))\n    if not 1<=n<=250_000_000:raise ValueError(\'Payload out of bounds\')\n    out=ROOT/\'received\'/trial/plan/(part+\'.parquet\')\n    out.parent.mkdir(parents=True,exist_ok=True)\n    temp=out.with_suffix(\'.part\');h=hashlib.sha256();remain=n\n    started=time.perf_counter()\n    with temp.open(\'wb\') as f:\n     while remain:\n      block=self.rfile.read(min(remain,1024*1024))\n      if not block:raise ValueError(\'Truncated Q05 upload\')\n      f.write(block);h.update(block);remain-=len(block)\n    if not hmac.compare_digest(h.hexdigest(),expected):\n     temp.unlink(missing_ok=True);raise ValueError(\'Bad uploaded SHA256\')\n    temp.replace(out)\n    self.response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'part\':part,\n      \'received_payload_bytes\':n,\'sha256\':h.hexdigest(),\n      \'receive_file_seconds\':time.perf_counter()-started})\n   elif self.path==\'/q05/finish\':\n    obj,trial,plan=self.info()\n    sql=final_sql(trial,plan)\n    with duckdb.connect(\':memory:\') as con:\n     con.execute(\'SET threads=4\')\n     physical=\'\\n\'.join(str(r[-1]) for r in con.execute(\'EXPLAIN \'+sql).fetchall())\n     t0=time.perf_counter();raw=con.execute(sql).fetchall();duration=time.perf_counter()-t0\n    rows=canonical(raw)\n    digest=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(\',\',\':\')).encode()).hexdigest()\n    self.response(200,{\'cloud\':CLOUD,\'plan_id\':plan,\'trial_id\':trial,\'result\':rows,\n      \'result_sha256\':digest,\'row_count\':len(rows),\'join_seconds\':duration,\n      \'raw_result_strings\':[[str(v) for v in row] for row in raw],\n      \'explain_gzip_b64\':base64.b64encode(gzip.compress(physical.encode())).decode(),\n      \'physical_plan_sha256\':hashlib.sha256(physical.encode()).hexdigest()})\n   else:self.response(404,{\'error\':\'not_found\'})\n  except Exception as exc:\n   self.response(422,{\'error\':type(exc).__name__+\': \'+str(exc)[:500]})\n\ndef main():\n ap=argparse.ArgumentParser();ap.add_argument(\'--cert\',required=True);ap.add_argument(\'--key\',required=True)\n args=ap.parse_args();server=ThreadingHTTPServer((\'0.0.0.0\',PORT),Handler)\n ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.minimum_version=ssl.TLSVersion.TLSv1_2\n ctx.load_cert_chain(args.cert,args.key);server.socket=ctx.wrap_socket(server.socket,server_side=True)\n server.serve_forever()\nif __name__==\'__main__\':main()\n'

# Q05 standalone three-provider cloud transport; embedded in experiments/q05.py.
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
AZ_NSG=os.environ.get('EGRESSOPT_AZURE_NSG','')
AZ_PUBLIC=os.environ.get('EGRESSOPT_AZURE_PUBLIC_IP_RESOURCE','')
GCP_PROJECT=os.environ.get('EGRESSOPT_GCP_PROJECT','')
GCP_ZONE=os.environ.get('EGRESSOPT_GCP_ZONE','')
GCP_VM=os.environ.get('EGRESSOPT_GCP_VM','')
Q05_PORT=8443
Q05_PIPE={
 'azure_join':[('aws','azure','orders_year'),('gcp','azure','supplier_region')],
 'aws_join':[('azure','aws','lineitem_order_supplier_agg'),('azure','aws','customer_nation'),('gcp','aws','supplier_region')],
 'gcp_join':[('aws','azure','orders_year'),('azure','gcp','joined_partial')],
 'semijoin_azure':[('gcp','azure','region_keys'),('azure','aws','eligible_customer_keys'),('aws','azure','semijoin_filtered_orders'),('azure','gcp','eligible_nations'),('gcp','azure','region_filtered_suppliers')],
}
Q05_DEST={'azure_join':'azure','aws_join':'aws','gcp_join':'gcp','semijoin_azure':'azure'}
Q05_CLOUD_TABLES={
 'aws':('orders',), 'azure':('customer','lineitem','nation'), 'gcp':('supplier','region')}
Q05_CLOUD_VENV={'aws':'/opt/egressopt/venv311/bin/python',
 'azure':'/opt/egressopt/venv/bin/python','gcp':'/opt/egressopt/venv/bin/python'}

def q05_cli(cmd,timeout=480):
 try:p=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout)
 except FileNotFoundError as e:raise RuntimeError('Missing cloud CLI '+cmd[0]) from e
 if p.returncode:raise RuntimeError(f'{cmd[0]} failed: '+(p.stderr or p.stdout)[-1300:])
 return p.stdout.strip()

def q05_aws(*arg):
 return json.loads(q05_cli(['aws',*arg,'--profile',AWS_PROFILE,'--region',AWS_REGION,'--output','json']))

def q05_gcloud(*args,timeout=450):
 return q05_cli(['gcloud',*args,'--project='+GCP_PROJECT],timeout=timeout)

def q05_aws_instance():
 result=q05_aws('ec2','describe-instances','--instance-ids',AWS_INSTANCE)
 arr=[inst for res in result['Reservations'] for inst in res['Instances']]
 if len(arr)!=1:raise RuntimeError('Q05 AWS instance not unique')
 return arr[0]

def q05_azure_state():
 response=json.loads(q05_cli(['az','vm','get-instance-view','--resource-group',AZ_RG,
 '--name',AZ_VM,'--query','instanceView.statuses[].code','--output','json']))
 power=[s for s in response if s.startswith('PowerState/')]
 if len(power)!=1:raise RuntimeError('Cannot obtain Azure power state')
 return power[0]

def q05_gcp_instance():
 return json.loads(q05_gcloud('compute','instances','describe',GCP_VM,
                           '--zone='+GCP_ZONE,'--format=json'))

def q05_start_vm(cloud):
 if cloud=='aws':
  old=q05_aws_instance()['State']['Name']
  if old=='running':return False
  if old!='stopped':raise RuntimeError('Unsupported AWS state: '+old)
  q05_aws('ec2','start-instances','--instance-ids',AWS_INSTANCE)
  q05_cli(['aws','ec2','wait','instance-running','--instance-ids',AWS_INSTANCE,
           '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=550)
  return True
 if cloud=='azure':
  old=q05_azure_state()
  if old=='PowerState/running':return False
  if old not in ('PowerState/stopped','PowerState/deallocated'):
   raise RuntimeError('Unsupported Azure state: '+old)
  q05_cli(['az','vm','start','--resource-group',AZ_RG,'--name',AZ_VM,'--output','none'],timeout=650)
  return True
 if cloud=='gcp':
  old=q05_gcp_instance()['status']
  if old=='RUNNING':return False
  if old not in ('TERMINATED','STOPPED'):raise RuntimeError('Unsupported GCP state: '+old)
  q05_gcloud('compute','instances','start',GCP_VM,'--zone='+GCP_ZONE,'--quiet','--format=json',timeout=650)
  return True
 raise ValueError(cloud)

def q05_remote(cloud,script,label):
 if cloud=='azure':
  cmd=['az','vm','run-command','invoke','--resource-group',AZ_RG,'--name',AZ_VM,
       '--command-id','RunShellScript','--scripts','set -eu\n'+script,
       '--query','value[0].message','--output','tsv']
  for attempt in range(1,11):
   try:out=q05_cli(cmd,timeout=470)
   except RuntimeError as e:
    if ('OperationNotAllowed' not in str(e) or
      'requires the VM to be running' not in str(e) or attempt==10):raise
    print(f'  Azure Run Command warming up ({attempt}/10): {label}',flush=True)
    time.sleep(min(4+2*attempt,20));continue
   if 'Enable succeeded' not in out:
    raise RuntimeError('Q05 Azure remote '+label+': '+out[-900:])
   return out
 if cloud=='gcp':
  code='sudo -n bash -lc '+shlex.quote('set -eu\n'+script)
  # Retry only the IAP 4003 error that explicitly states port 22 could not
  # connect. The guest command did not start in this failure mode, which makes
  # retrying safe even for non-idempotent upload/deployment steps. Never retry
  # arbitrary SSH exit 255 or an interrupted connection after execution began.
  for attempt in range(1,6):
   try:
    return q05_gcloud('compute','ssh',GCP_VM,'--zone='+GCP_ZONE,
      '--tunnel-through-iap','--quiet','--command='+code,timeout=500)
   except RuntimeError as exc:
    err=str(exc)
    preconnect=(('4003' in err and 'failed to connect to backend' in err.lower())
       and ('port 22' in err or 'ConnectionCreationError' in err))
    if not preconnect:
     raise
    try:
     vm_state=q05_gcp_instance()['status']
    except Exception as state_exc:
     raise RuntimeError('Q05 GCP IAP SSH failed, VM status unavailable; '+
      'do not retry query blindly: '+str(state_exc)) from exc
    if vm_state!='RUNNING':
     raise RuntimeError('Q05 GCP IAP SSH failed and VM state is '+vm_state+
       '; inspect guest startup/stop and prior cleanup') from exc
    if attempt==5:
     raise RuntimeError('Q05 GCP IAP SSH still cannot connect to VM port 22 '
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
    cid=q05_cli(['aws','ssm','send-command','--instance-ids',AWS_INSTANCE,
      '--document-name','AWS-RunShellScript',
      '--parameters',json.dumps({'commands':['set -eu',script]}),
      '--comment',('EgressOpt Q05 '+label)[:100],
      '--query','Command.CommandId','--output','text','--profile',AWS_PROFILE,
      '--region',AWS_REGION],timeout=100)
    break
   except RuntimeError as e:
    if ('InvalidInstanceId' not in str(e) and 'TargetNotConnected' not in str(e)) or attempt==8:
     raise
    time.sleep(min(5*attempt,20))
  deadline=time.monotonic()+470
  while time.monotonic()<deadline:
   p=subprocess.run(['aws','ssm','get-command-invocation','--command-id',cid,
    '--instance-id',AWS_INSTANCE,'--profile',AWS_PROFILE,'--region',AWS_REGION,
    '--output','json'],capture_output=True,text=True,timeout=50)
   if p.returncode:
    if 'InvocationDoesNotExist' in p.stderr:time.sleep(2);continue
    raise RuntimeError('Q05 AWS SSM '+label+': '+p.stderr[-900:])
   obj=json.loads(p.stdout)
   if obj['Status']=='Success':return obj.get('StandardOutputContent','')
   if obj['Status'] in {'Failed','TimedOut','Cancelled','Cancelling'}:
    raise RuntimeError('Q05 AWS SSM '+label+': '+obj['Status']+' '+
       obj.get('StandardErrorContent','')[-850:]+obj.get('StandardOutputContent','')[-850:])
   time.sleep(2)
  raise RuntimeError('Q05 AWS SSM timeout: '+label)
 raise ValueError(cloud)

def q05_marker(out,prefix='Q05_RPC='):
 found=[line.split(prefix,1)[1].strip() for line in out.splitlines() if line.startswith(prefix)]
 if len(found)!=1:raise RuntimeError('Expected one '+prefix+' remote response: '+out[-350:])
 return found[0]

def q05_rpc(cloud,script,label):
 return json.loads(q05_marker(q05_remote(cloud,script,label)))

def q05_ip(raw):
 ip=ipaddress.ip_address(str(raw).strip())
 if ip.version!=4 or not ip.is_global:raise RuntimeError('Invalid public IPv4: '+str(raw))
 return str(ip)

def q05_outbound(cloud):
 out=q05_remote(cloud,'curl --silent --show-error --fail --connect-timeout 10 --max-time 30 https://checkip.amazonaws.com','public-egress-ip')
 if cloud=='azure' and '[stdout]' in out:out=out.split('[stdout]',1)[1].split('[stderr]',1)[0]
 return q05_ip(out.strip())

def q05_gcp_public(inst):
 for nic in inst['networkInterfaces']:
  for cfg in nic.get('accessConfigs',[]):
   if cfg.get('natIP'):return q05_ip(cfg['natIP'])
 raise RuntimeError('GCP VM has no public IP')

def q05_install_worker(cloud,runkey):
 source=Q05_WORKER_SOURCE.encode()
 release='/opt/egressopt/releases/q05_'+sha_bytes(source)[:12]
 temp=f'/opt/egressopt/runtime/q05_upload_{runkey}_{cloud}'
 encoded=base64.b64encode(gzip.compress(source)).decode()
 q05_remote(cloud,'umask 077; mkdir -p '+shlex.quote(temp)+' '+shlex.quote(release)+
             '; : > '+shlex.quote(temp+'/worker.gz.b64'), 'prepare-worker')
 for idx in range(0,len(encoded),2400):
  q05_remote(cloud,'printf %s '+shlex.quote(encoded[idx:idx+2400])+
   ' >> '+shlex.quote(temp+'/worker.gz.b64'),'upload-worker')
 dest=release+'/worker.py';py=Q05_CLOUD_VENV[cloud]
 script=('base64 -d '+shlex.quote(temp+'/worker.gz.b64')+' | gzip -d > '+shlex.quote(dest)+'\n'+
  'test "$(sha256sum '+shlex.quote(dest)+" | cut -d' ' -f1)\" = "+shlex.quote(sha_bytes(source))+'\n'+
  py+' -m py_compile '+shlex.quote(dest)+'\n'+
  'rm -rf '+shlex.quote(temp)+'\n'+'echo Q05_WORKER_DEPLOYED')
 out=q05_remote(cloud,script,'verify-worker')
 if 'Q05_WORKER_DEPLOYED' not in out:raise RuntimeError('Q05 worker deploy validation failed on '+cloud)
 return release

def q05_start_service(cloud,key,release,public_ip,token,params):
 work=f'/opt/egressopt/runtime/q05_{key}_{cloud}'
 py=Q05_CLOUD_VENV[cloud]
 unit=f'egressopt-q05-{key}-{cloud}'
 launch='\n'.join(['#!/bin/sh','set -eu','export Q05_CLOUD='+shlex.quote(cloud),
  'export Q05_RUNTIME='+shlex.quote(work),
  'export Q05_DATA_DIR=/opt/egressopt/data/tpch_sf1',
  'export Q05_WORKER_TOKEN='+shlex.quote(token),
  'export Q05_REGION='+shlex.quote(params['region']),
  'export Q05_START_DATE='+shlex.quote(params['start_date']),
  'export Q05_END_DATE='+shlex.quote(params['end_date_exclusive']),
  'exec '+py+' '+shlex.quote(release+'/worker.py')+
  ' --cert '+shlex.quote(work+'/server.pem')+' --key '+shlex.quote(work+'/server.key')])+'\n'
 b64=base64.b64encode(launch.encode()).decode()
 required=' '.join('/opt/egressopt/data/tpch_sf1/'+t+'.parquet' for t in Q05_CLOUD_TABLES[cloud])
 script=f'''umask 077
mkdir -p {shlex.quote(work)}
test -f {shlex.quote(release+'/worker.py')}
for f in {required}; do test -f "$f"; done
{py} -c 'import duckdb; assert duckdb.__version__ == "1.5.6",duckdb.__version__'
command -v openssl >/dev/null
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 \
 -keyout {work}/ca.key -out {work}/ca.pem -subj '/CN=EgressOpt Q05 temporary CA' \
 -addext 'basicConstraints=critical,CA:TRUE' >/dev/null 2>&1
openssl req -nodes -newkey rsa:2048 -sha256 \
 -keyout {work}/server.key -out {work}/server.csr \
 -subj '/CN=EgressOpt Q05 worker' >/dev/null 2>&1
printf 'basicConstraints=CA:FALSE\\nsubjectAltName=IP:{public_ip}\\nextendedKeyUsage=serverAuth\\n' > {work}/server.ext
openssl x509 -req -days 1 -sha256 -in {work}/server.csr \
 -CA {work}/ca.pem -CAkey {work}/ca.key -CAcreateserial \
 -extfile {work}/server.ext -out {work}/server.pem >/dev/null 2>&1
printf %s {shlex.quote(b64)} | base64 -d > {work}/launch.sh
chmod 700 {work}/launch.sh
systemd-run --unit={unit} --collect --property=Restart=no /bin/sh {work}/launch.sh >/dev/null
STATUS=000
for i in $(seq 1 40); do
 STATUS=$(curl --insecure -s -o /dev/null -w '%{{http_code}}' --max-time 3 https://127.0.0.1:{Q05_PORT}/health || true)
 [ "$STATUS" = 401 ] && break
 sleep 1
done
test "$STATUS" = 401
curl --silent --show-error --fail --insecure --max-time 10 \
 -H {shlex.quote('Authorization: Bearer '+token)} https://127.0.0.1:{Q05_PORT}/health | \
 {py} -c 'import sys,json; x=json.load(sys.stdin); assert x["cloud"]=="{cloud}" and x["query"]=="Q05"'
echo Q05_SERVICE_READY
printf 'Q05_CA_B64='; base64 -w0 {work}/ca.pem; echo
'''
 out=q05_remote(cloud,script,'start-worker')
 if 'Q05_SERVICE_READY' not in out:raise RuntimeError('Q05 TLS startup error on '+cloud)
 ca=q05_marker(out,'Q05_CA_B64=')
 if b'BEGIN CERTIFICATE' not in base64.b64decode(ca,validate=True):raise RuntimeError('Invalid CA on '+cloud)
 return ca

def q05_azure_rule(src,source_ip,key):
 name=f'egressopt-q05-{key}-{src}'
 existing=json.loads(q05_cli(['az','network','nsg','rule','list','--resource-group',AZ_RG,
   '--nsg-name',AZ_NSG,'--output','json']))
 used={int(r['priority']) for r in existing if r.get('direction')=='Inbound'}
 prio=next((i for i in range(1100,2500) if i not in used),None)
 if prio is None:raise RuntimeError('No free Azure ingress priority')
 q05_cli(['az','network','nsg','rule','create','--resource-group',AZ_RG,
   '--nsg-name',AZ_NSG,'--name',name,'--priority',str(prio),'--direction','Inbound',
   '--access','Allow','--protocol','Tcp','--source-address-prefixes',source_ip+'/32',
   '--destination-port-ranges',str(Q05_PORT),'--description','EgressOpt Q05 temporary TLS',
   '--output','none'],timeout=300)
 return name

def q05_aws_rule(source_ip,source,key,group):
 permission=[{'IpProtocol':'tcp','FromPort':Q05_PORT,'ToPort':Q05_PORT,
   'IpRanges':[{'CidrIp':source_ip+'/32','Description':'EgressOpt Q05 '+key+' '+source}]}]
 created=q05_aws('ec2','authorize-security-group-ingress','--group-id',group,
                 '--ip-permissions',json.dumps(permission))
 rules=created.get('SecurityGroupRules',[])
 if len(rules)!=1 or not rules[0].get('SecurityGroupRuleId'):
  raise RuntimeError('AWS Q05 ingress created with missing ID; inspect SG')
 return rules[0]['SecurityGroupRuleId']

def q05_gcp_rule(source_ip,key,network):
 tag='egressopt-q05-'+key
 q05_gcloud('compute','instances','add-tags',GCP_VM,'--zone='+GCP_ZONE,
            '--tags='+tag,'--quiet','--format=json')
 rule=tag
 try:
  q05_gcloud('compute','firewall-rules','create',rule,'--network='+network,
    '--direction=INGRESS','--action=ALLOW','--rules=tcp:8443',
    '--source-ranges='+source_ip+'/32','--target-tags='+tag,
    '--description=EgressOpt Q05 temporary TLS',
    '--quiet','--format=json')
 except Exception:
  q05_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
           '--tags='+tag,'--quiet','--format=json')
  raise
 return rule,tag

def q05_stage(cloud,part,plan,trial,token,key):
 work=f'/opt/egressopt/runtime/q05_{key}_{cloud}'
 payload=json.dumps({'part':part,'plan_id':plan,'trial_id':trial},separators=(',',':'))
 dest=work+'/stage_response.json';py=Q05_CLOUD_VENV[cloud]
 script=(f'curl --fail --silent --show-error --insecure --connect-timeout 5 --max-time 380 '+
  '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
  '-H '+shlex.quote('Content-Type: application/json')+' '+
  '--data-binary '+shlex.quote(payload)+' '+
  'https://127.0.0.1:8443/q05/stage > '+shlex.quote(dest)+'\n'+
  py+' -c '+shlex.quote('import json; x=json.load(open('+repr(dest)+')); print("Q05_RPC="+json.dumps(x,separators=(",",":")))'))
 result=q05_rpc(cloud,script,'stage-'+plan+'-'+part)
 if (result.get('cloud'),result.get('plan_id'),result.get('trial_id'),result.get('part'))!=(cloud,plan,trial,part):
  raise RuntimeError('Q05 wrong staging identity')
 if (int(result['parquet_bytes'])<1 or int(result['rows'])<0 or not re.fullmatch('[0-9a-f]{64}',result['sha256'])):
  raise RuntimeError('Bad Q05 staged Parquet metadata')
 if result['file_path']!=f'{work}/stage/{trial}/{plan}/{part}.parquet':
  raise RuntimeError('Bad Q05 staged file path')
 return result

def q05_upload(source,dest,stage,plan,trial,ips,cas,token,key):
 part=stage['part'];work=f'/opt/egressopt/runtime/q05_{key}_{source}'
 ca=work+'/peer-ca.pem';reply=work+'/upload_response.json'
 url=f'https://{ips[dest]}:8443/q05/upload/{trial}/{plan}/{part}'
 py=Q05_CLOUD_VENV[source]
 script=('printf %s '+shlex.quote(cas[dest])+' | base64 -d > '+shlex.quote(ca)+'\n'+
  'INFO=$(curl --fail --silent --show-error --connect-timeout 25 --max-time 400 '+
  '--cacert '+shlex.quote(ca)+' '+
  '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
  '-H '+shlex.quote('X-Q05-SHA256: '+stage['sha256'])+' '+
  "-H 'Content-Type: application/octet-stream' "+
  '--data-binary @'+shlex.quote(stage['file_path'])+' '+
  '-o '+shlex.quote(reply)+' '+
  "-w '%{time_total} %{size_upload} %{http_code}' "+shlex.quote(url)+')\n'+
  'export INFO\n'+py+' -c '+shlex.quote(
   'import json,os; a,b,c=os.environ["INFO"].split(); r=json.load(open('+repr(reply)+')); '+
   'print("Q05_RPC="+json.dumps({"http_seconds":float(a),"payload_bytes":int(b),'+
   '"http_status":int(c),"response":r},separators=(",",":")))'))
 rpc=q05_rpc(source,script,'upload-'+plan+'-'+part)
 ack=rpc['response']
 if (rpc['http_status']!=200 or ack.get('cloud')!=dest or ack.get('plan_id')!=plan or
  ack.get('trial_id')!=trial or ack.get('part')!=part or
  ack.get('sha256')!=stage['sha256'] or
  int(rpc['payload_bytes'])!=int(stage['parquet_bytes']) or
  int(ack['received_payload_bytes'])!=int(stage['parquet_bytes'])):
  raise RuntimeError('Q05 real TLS transfer validation failed '+plan+'/'+part)
 return {'label':part,'source_cloud':source,'destination_cloud':dest,
  'rows':int(stage['rows']),'parquet_bytes':int(stage['parquet_bytes']),
  'sha256':stage['sha256'],'stage_seconds':float(stage['stage_seconds']),
  'curl_http_total_seconds':float(rpc['http_seconds']),
  'curl_size_upload':int(rpc['payload_bytes']),
  'receiver_file_seconds':float(ack['receive_file_seconds']),
  'execution_mode':'real_multicloud'}

def q05_finish(cloud,plan,trial,token,key):
 work=f'/opt/egressopt/runtime/q05_{key}_{cloud}'
 resp=work+'/finish_response.json';py=Q05_CLOUD_VENV[cloud]
 payload=json.dumps({'plan_id':plan,'trial_id':trial},separators=(',',':'))
 script=('curl --fail --silent --show-error --insecure --max-time 340 '+
  '-H '+shlex.quote('Authorization: Bearer '+token)+' '+
  '-H '+shlex.quote('Content-Type: application/json')+' '+
  '--data-binary '+shlex.quote(payload)+' '+
  'https://127.0.0.1:8443/q05/finish > '+shlex.quote(resp)+'\n'+
  py+' -c '+shlex.quote(
   'import json,gzip,base64; x=json.load(open('+repr(resp)+')); '+
   'print("Q05_RPC_GZIP="+base64.b64encode(gzip.compress(json.dumps(x,separators=(",",":")).encode())).decode())'))
 out=q05_remote(cloud,script,'finish-'+plan)
 data=json.loads(gzip.decompress(base64.b64decode(q05_marker(out,'Q05_RPC_GZIP='),validate=True)))
 if (data.get('cloud'),data.get('plan_id'),data.get('trial_id'))!=(cloud,plan,trial):
  raise RuntimeError('Q05 wrong finish identity')
 explain=gzip.decompress(base64.b64decode(data.pop('explain_gzip_b64'),validate=True)).decode()
 if sha_bytes(explain.encode())!=data.pop('physical_plan_sha256'):
  raise RuntimeError('Q05 EXPLAIN integrity failure')
 data['physical_plan']=explain
 return data

def q05_trial(plan,trial,token,key,ips,cas):
 edges=[]
 for source,dest,part in Q05_PIPE[plan]:
  s=q05_stage(source,part,plan,trial,token,key)
  edges.append(q05_upload(source,dest,s,plan,trial,ips,cas,token,key))
 dest=Q05_DEST[plan]
 result=q05_finish(dest,plan,trial,token,key)
 return edges,result,dest

def q05_optimizer_decision(args):
    """Commit to Q05 plan selection using only trials 1–5 from a fixed, archived run.

    This is a restricted candidate-plan selector, not generalized memo join generation.
    No cloud calls; no dependence on the separate egressopt.py implementation.
    """
    if not math.isfinite(args.optimizer_delta) or args.optimizer_delta < 0 or args.optimizer_delta > 100:
        raise ValueError('Invalid --optimizer-delta; expected finite number 0..100')
    archive=Path(args.optimizer_archive).expanduser()
    tariffs_path=Path(args.optimizer_tariffs).expanduser()
    raw_rates=json.loads(tariffs_path.read_text(encoding='utf-8'))
    if raw_rates.get('classification') not in (
            'PUBLISHED_LIST_RATE_MARGINAL_SCENARIO_NOT_BILLED',
            'ILLUSTRATIVE_SCENARIO_NOT_VERIFIED_RATES') or raw_rates.get('tier_assumption')!='fixed_marginal_rates_no_free_tier':
        raise ValueError('Unsupported/unclassified tariff assumptions')
    rates={}
    for e in raw_rates['directed_rates']:
        key=(e['source'].lower(),e['destination'].lower())
        unit=int(e.get('unit_bytes',raw_rates['unit_bytes']))
        rate=Decimal(str(e['usd_per_unit']))
        if key in rates or key[0]==key[1] or unit<=0 or not rate.is_finite() or rate<0:
            raise ValueError('Invalid/duplicate directed price')
        rates[key]=(rate,unit)
    if set(rates)!={(s,d) for s in ('aws','azure','gcp') for d in ('aws','azure','gcp') if s!=d}:
        raise ValueError('Require exactly six directed inter-cloud tariffs')
    with zipfile.ZipFile(archive) as z:
        if z.testzip() is not None:raise ValueError('Optimizer source ZIP corrupt')
        run='20261015T203105Z_cfdf2d72'
        suffix='/'+run+'/run.json'
        paths=[n[:-len('run.json')] for n in z.namelist() if n.endswith(suffix)]
        if len(paths)!=1:raise ValueError('Expected exactly one locked Q05 SF1/P1 calibration archive')
        prefix=paths[0]
        run_bytes=z.read(prefix+'run.json')
        source=json.loads(run_bytes)
        if (source.get('query_id'),source.get('run_id'),source.get('status'),source.get('execution_mode'))!=(
            'Q05',run,'COMPLETED','real_multicloud'):
            raise ValueError('Q05 locked calibration manifest identity mismatch')
        csv_bytes=z.read(prefix+'trial_metrics.csv')
        if source.get('trial_metrics_sha256') and sha_bytes(csv_bytes)!=source['trial_metrics_sha256']:
            raise ValueError('Q05 calibration CSV hash mismatch')
        rows=list(csv.DictReader(io.StringIO(csv_bytes.decode('utf-8-sig'))))
        if len(rows)!=40:raise ValueError('Q05 calibration archive must contain 40 trial rows')
        byplan={p:[] for p in PLANS}
        for row in rows:
            plan=row['plan_id'];trial=int(row['trial'])
            if plan not in byplan or not 1<=trial<=10 or row.get('correct_vs_reference','').lower()!='true':
                raise ValueError('Q05 calibration archive invalid row')
            if trial>5:continue   # Never use evaluation observations in selection.
            detail=json.loads(z.read(prefix+f'trials/{plan}/trial_{trial:03}.json'))
            edges=detail['transfer_edges']
            if [(e['source_cloud'],e['destination_cloud'],e['label']) for e in edges]!=Q05_PIPE[plan]:
                raise ValueError('Q05 calibration edge topology changed')
            widths=[int(e['curl_size_upload']) for e in edges]
            if sum(widths)!=int(row['http_payload_bytes']):
                raise ValueError('Q05 calibration recorded bytes mismatch')
            duration=float(row['total_seconds']);stage=float(row['stage_seconds'])
            if not math.isfinite(duration) or not math.isfinite(stage) or duration<=0 or stage<0 or duration<stage:
                raise ValueError('Q05 non-finite calibration timing')
            byplan[plan].append({'trial':trial,'time_s':duration,'stage_s':stage,'edge_bytes':widths})
    candidates=[]
    for plan in PLANS:
        tr=sorted(byplan[plan],key=lambda x:x['trial'])
        if [x['trial'] for x in tr]!=[1,2,3,4,5]:
            raise ValueError('Q05 incomplete calibration trial numbers')
        stage=statistics.median([x['stage_s'] for x in tr])
        remainder=statistics.median([x['time_s']-x['stage_s'] for x in tr])
        edges=[]
        cost=Decimal(0)
        for j,(src,dst,label) in enumerate(Q05_PIPE[plan]):
            amount=int(statistics.median([x['edge_bytes'][j] for x in tr]))
            rate,unit=rates[(src,dst)]
            cost+=Decimal(amount)*rate/Decimal(unit)
            edges.append({'source_cloud':src,'destination_cloud':dst,'label':label,'predicted_http_body_bytes':amount})
        candidates.append({'plan':plan,'predicted_latency_ms':(stage+remainder)*1000,
                           'predicted_http_bytes':sum(e['predicted_http_body_bytes'] for e in edges),
                           'predicted_cost_usd_proxy':str(cost),'predicted_edges':edges})
    fastest=min(candidates,key=lambda x:(x['predicted_latency_ms'],x['plan']))
    slo=fastest['predicted_latency_ms']*(1+args.optimizer_delta)
    feasible=[p for p in candidates if p['predicted_latency_ms']<=slo+1e-9]
    if not feasible:raise ValueError('Q05 no latency-feasible candidate')
    selected=min(feasible,key=lambda x:(Decimal(x['predicted_cost_usd_proxy']),x['predicted_latency_ms'],x['plan']))
    byteplan=min(feasible,key=lambda x:(x['predicted_http_bytes'],x['predicted_latency_ms'],x['plan']))
    return {'schema_version':1,'query':'Q05','optimizer_kind':'restricted_prevalidated_plan_selector',
            'selection_without_evaluation_trials':True,'calibration_trial_numbers':[1,2,3,4,5],
            'excluded_evaluation_trial_numbers':[6,7,8,9,10],
            'source_run_id':run,'source_run_json_sha256':sha_bytes(run_bytes),
            'source_trial_csv_sha256':sha_bytes(csv_bytes),
            'source_zip_sha256':sha_file(archive),'source_zip_filename':archive.name,
            'tariff_file_sha256':sha_file(tariffs_path),
            'tariff_scenario_id':raw_rates['scenario_id'],'tariff_classification':raw_rates['classification'],
            'selected_plan':selected['plan'],'bytes_only_plan':byteplan['plan'],
            'fastest_plan':fastest['plan'],'predicted_slo_ms':slo,
            'optimizer_delta':args.optimizer_delta,'candidate_predictions':candidates,
            'model_description':'median of source stage and residual from five calibration trials',
            'cost_description':'directional HTTP request-body bytes with documented marginal-rate proxy, not invoiced charges',
            'prospective_claim':'decision sealed before starting this run’s cloud resources; earlier same-placement calibration remains historical'}


def q05_audit_cloud(dir):
 report=read_json(dir/'run.json')
 if (report.get('status')!='COMPLETED' or report.get('execution_mode')!='real_multicloud' or
     report.get('query_id')!='Q05' or report.get('sf')!=1 or report.get('placement')!='P1'):
  raise ValueError('No completed real-cloud Q05 report')
 reference=result_hash(report['reference_result'])
 if reference!=report['reference_sha256'] or sha_bytes(report['source_sql_text'].encode())!=report['source_sql_sha256']:
  raise ValueError('Q05 source/reference archive mismatch')
 if report.get('expected_edges')!={k:[[a,b] for a,b,c in v] for k,v in Q05_PIPE.items()}:
  # In JSON, tuples are lists; the plan-edge manifest must match the code.
  raise ValueError('Q05 cloud plan-edge manifest mismatch')
 with (dir/'trial_metrics.csv').open(newline='',encoding='utf-8') as f:metrics=list(csv.DictReader(f))
 active=tuple(report.get('executed_plans',PLANS))
 if not active or len(set(active))!=len(active) or any(plan not in PLANS for plan in active):
  raise ValueError('Invalid Q05 executed plan set')
 if report.get('optimizer_decision'):
  decision=report['optimizer_decision']
  if (decision.get('selected_plan') not in active or decision.get('fastest_plan') not in active or
      decision.get('query')!='Q05' or report.get('experiment_subtype')!='prospective_optimizer_vs_predeclared_fastest'):
   raise ValueError('Optimizer plan commitment missing/inconsistent')
  sealed=dir/'optimizer_selection.json'
  if not sealed.is_file() or sha_file(sealed)!=report.get('optimizer_selection_sha256') or read_json(sealed)!=decision:
   raise ValueError('Q05 optimizer preregistered decision mismatch')
 expected=report['trials_per_plan']*len(active)
 if len(metrics)!=expected or expected!=report['validated_trial_count']:
  raise ValueError('Missing Q05 cloud trials')
 seen=set()
 for row in metrics:
  plan=row['plan_id'];n=int(row['trial']);key=(plan,n)
  if plan not in active or not 1<=n<=report['trials_per_plan'] or key in seen:
   raise ValueError('Invalid/duplicate Q05 trial')
  seen.add(key)
  trial=read_json(dir/'trials'/plan/f'trial_{n:03d}.json')
  if (trial['run_id'],trial['plan_id'],int(trial['trial']))!=(report['run_id'],plan,n):
   raise ValueError('Incorrect Q05 trial identity')
  edges=trial['transfer_edges'];expected_edges=Q05_PIPE[plan]
  if [(e['source_cloud'],e['destination_cloud'],e['label']) for e in edges]!=expected_edges:
   raise ValueError('Incorrect Q05 cloud transfer graph')
  for e in edges:
   if not re.fullmatch('[0-9a-f]{64}',e['sha256']):raise ValueError('Malformed Q05 edge SHA')
   if int(e['curl_size_upload'])!=int(e['parquet_bytes']) or int(e['parquet_bytes'])<1:
    raise ValueError('Inconsistent Q05 transfer bytes')
   if float(e['stage_seconds'])<0 or float(e['curl_http_total_seconds'])<0:
    raise ValueError('Bad Q05 transfer time')
  payload=sum(int(e['curl_size_upload']) for e in edges)
  if payload!=int(row['http_payload_bytes']) or payload!=int(trial['http_payload_bytes']):
   raise ValueError('Q05 HTTP byte mismatch')
  if sum(int(e['parquet_bytes']) for e in edges)!=int(row['intermediate_parquet_bytes']):
   raise ValueError('Q05 Parquet byte mismatch')
  computed=sum(float(e['stage_seconds'])+float(e['curl_http_total_seconds']) for e in edges)+float(trial['join_aggregate_seconds'])
  if abs(computed-float(trial['total_seconds']))>1e-9 or abs(computed-float(row['total_seconds']))>1e-9:
   raise ValueError('Q05 measured workflow time mismatch')
  if result_hash(trial['result'])!=reference or trial['result_sha256']!=reference or row['result_sha256']!=reference:
   raise ValueError('Q05 cloud result incorrect')
  if not trial['correct_vs_reference'] or row['correct_vs_reference'] not in ('True','true','1'):
   raise ValueError('Q05 cloud correctness flag false')
  if trial['execution_mode']!='real_multicloud' or row['execution_mode']!='real_multicloud':
   raise ValueError('Q05 cloud evidence mislabeled')
 return report,metrics

def q05_cloud(args):
 if args.sf!=1:raise RuntimeError('Validated Q05 only SF1')
 import duckdb
 sql_path,paths=inputs(args)
 source_sql=sql_path.read_text(encoding='utf-8')
 params=source_parameters(source_sql)
 con=make_connection(duckdb,args.threads)
 ref,raw=source_reference(con,paths,source_sql)
 constraints=verify_sf1_keys(con,paths,params)
 con.close()
 if not ref:raise RuntimeError('Refusing empty Q05 reference')
 runkey=uuid.uuid4().hex[:8]
 run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+runkey
 directory=q05_run_root(args)/run_id;directory.mkdir(parents=True,exist_ok=False)
 manifest={
  'schema_version':2,'query_id':QUERY_ID,'run_id':run_id,'sf':args.sf,'placement':PLACEMENT_ID,
  'execution_mode':'real_multicloud','status':'RUNNING','created_utc':utcnow(),
  'source_sql':str(sql_path.resolve()),'source_sql_text':source_sql,'source_sql_sha256':sha_file(sql_path),
  'reference_result':ref,'reference_raw':raw,'reference_sha256':result_hash(ref),
  'parameters':params,'key_validation':constraints,
  'data_sources':{t:{'cloud':TABLE_LOCATION[t],'sha256':sha_file(p),'bytes':p.stat().st_size}
                  for t,p in paths.items()},
  'code_sha256':sha_file(Path(__file__)),'worker_sha256':sha_bytes(Q05_WORKER_SOURCE.encode()),
  'python_version':platform.python_version(),'duckdb_version':duckdb.__version__,
  'trials_per_plan':args.trials,'warmup_trials_per_plan':args.warmup_trials,
  'expected_edges':{k:[[a,b] for a,b,c in v] for k,v in Q05_PIPE.items()},
  'full_edge_plan':{k:[[a,b,c] for a,b,c in v] for k,v in Q05_PIPE.items()},
  'execution_order':[], 'cloud_datasets':{},'worker_releases':{},'assigned_public_ips':{},
  'gcp_external_ip_added_by_this_run':False,
  'verified_outbound_public_ips':{},'firewall_rules':{},'cleanup':{},
  'network_bytes_definition':'Exact HTTP request-body size, not provider-billed egress',
  'executed_plans':list(getattr(args,'_q05_run_plans',PLANS)),
  'experiment_subtype':'prospective_optimizer_vs_predeclared_fastest' if getattr(args,'_q05_optimizer_decision',None) else 'four_plan_candidate_baseline',

  'timing_definition':'sum(stage Parquet materialization on source + TLS curl POST including receiver processing + destination DuckDB join), excludes cloud management and VM startup',
  'network_time_isolated':False,'egress_usd_estimated':None,
  'limitations':['Actual provider billing was not measured',
                 'The optimizer predicts from prior SF1/P1 plan profiles, not unseen relational plans',
                 'This prospective run is separate from historic calibration and holdout records',
                 'Tokens may remain in authorized cloud-management history'],

 }
 if getattr(args,'_q05_optimizer_decision',None) is not None:
  # Freeze decision, calibration/archive hashes, and tariff assumptions BEFORE any cloud VM starts.
  manifest['optimizer_decision']=args._q05_optimizer_decision
  manifest['optimizer_committed_utc']=utcnow()
  write_json(directory/'optimizer_selection.json',args._q05_optimizer_decision)
  manifest['optimizer_selection_sha256']=sha_file(directory/'optimizer_selection.json')
 write_json(directory/'run.json',manifest)
 state={'started':{c:False for c in ('aws','azure','gcp')},
  'service':{c:False for c in ('aws','azure','gcp')},
  'rules':{'aws':[], 'azure':[], 'gcp':[]},'gcp_tag':None,'gcp_tag_added':False,
  'gcp_external_added':False}
 measured=[];warmups=[]
 try:
  for cloud in ('aws','azure','gcp'):
   print('Checking '+cloud.upper()+' VM...',flush=True)
   state['started'][cloud]=q05_start_vm(cloud)
   print('  '+cloud.upper()+' running',flush=True)
  aws_inst=q05_aws_instance();gcp_inst=q05_gcp_instance()
  if aws_inst['State']['Name']!='running' or q05_azure_state()!='PowerState/running' or gcp_inst['status']!='RUNNING':
   raise RuntimeError('Q05 expected all three cloud VMs running')
  if not any(c.get('natIP') for nic in gcp_inst['networkInterfaces'] for c in nic.get('accessConfigs',[])):
   state['gcp_external_added']=True
   manifest['gcp_external_ip_added_by_this_run']=True
   write_json(directory/'run.json',manifest)
   q05_gcloud('compute','instances','add-access-config',GCP_VM,'--zone='+GCP_ZONE,
              '--network-interface=nic0','--access-config-name=external-nat',
              '--quiet','--format=json',timeout=280)
   gcp_inst=q05_gcp_instance()
  ips={'aws':q05_ip(aws_inst['PublicIpAddress']),
   'azure':q05_ip(json.loads(q05_cli(['az','network','public-ip','show',
              '--resource-group',AZ_RG,'--name',AZ_PUBLIC,'--output','json']))['ipAddress']),
   'gcp':q05_gcp_public(gcp_inst)}
  manifest['assigned_public_ips']=ips
  outbound={c:q05_outbound(c) for c in ('aws','azure','gcp')}
  manifest['verified_outbound_public_ips']=outbound
  aws_sg=[x['GroupId'] for x in aws_inst['SecurityGroups']]
  if len(aws_sg)!=1:raise RuntimeError('Q05 expected one AWS security group')
  manifest['cloud_hardware']={
   'aws':{'region':AWS_REGION,'instance_type':aws_inst['InstanceType'],'instance_id':AWS_INSTANCE},
   'azure':{'resource_group':AZ_RG,'vm_name':AZ_VM},
   'gcp':{'zone':GCP_ZONE,'machine_type':gcp_inst['machineType'].rsplit('/',1)[-1]}}
  for cloud in ('aws','azure','gcp'):
   names=Q05_CLOUD_TABLES[cloud]
   output=q05_remote(cloud,'\n'.join('sha256sum /opt/egressopt/data/tpch_sf1/'+n+'.parquet' for n in names),
     'dataset-hashes')
   found={}
   for line in output.splitlines():
    match=re.search(r'([a-f0-9]{64})\s+(/opt/egressopt/data/tpch_sf1/([a-z]+)\.parquet)',line)
    if match:found[match.group(3)]=match.group(1)
   if set(found)!=set(names):raise RuntimeError('Q05 remote dataset hash list invalid on '+cloud)
   for name in names:
    if found[name]!=manifest['data_sources'][name]['sha256']:
     raise RuntimeError('Q05 remote dataset mismatch on '+cloud+'/'+name)
   manifest['cloud_datasets'][cloud]=found
   manifest['worker_releases'][cloud]=q05_install_worker(cloud,runkey)
   write_json(directory/'run.json',manifest)
  token=secrets.token_urlsafe(48);cas={}
  for cloud in ('aws','azure','gcp'):
   state['service'][cloud]=True
   cas[cloud]=q05_start_service(cloud,runkey,manifest['worker_releases'][cloud],ips[cloud],token,params)
   print('  '+cloud.upper()+' authenticated Q05 TLS worker ready',flush=True)
  # Allow only real, directed source→destination edges; no internet-wide rules.
  for src in ('azure','gcp'):
   rule=q05_aws_rule(outbound[src],src,runkey,aws_sg[0]);state['rules']['aws'].append(rule)
   manifest['firewall_rules']['aws_from_'+src]=rule
  for src in ('aws','gcp'):
   rule=q05_azure_rule(src,outbound[src],runkey);state['rules']['azure'].append(rule)
   manifest['firewall_rules']['azure_from_'+src]=rule
  network=gcp_inst['networkInterfaces'][0]['network'].rsplit('/',1)[-1]
  # If creation fails after the tag is assigned, q05_gcp_rule removes its tag.
  rule,tag=q05_gcp_rule(outbound['azure'],runkey,network)
  state['rules']['gcp'].append(rule);state['gcp_tag']=tag;state['gcp_tag_added']=True
  manifest['firewall_rules']['gcp_from_azure']=rule
  write_json(directory/'run.json',manifest)
  print('  Three-cloud source-restricted TLS ingress active',flush=True)
  for idx in range(args.warmup_trials+args.trials):
   warm=idx<args.warmup_trials
   count=idx+1 if warm else idx-args.warmup_trials+1
   trial_id=('w' if warm else 'm')+f'{count:02d}'
   active=tuple(manifest['executed_plans'])
   rotate=idx%len(active);order=active[rotate:]+active[:rotate]
   manifest['execution_order'].append({'round':idx+1,'trial_id':trial_id,'warmup':warm,'plans':order})
   for plan in order:
    edges,finished,dest=q05_trial(plan,trial_id,token,runkey,ips,cas)
    if finished['result']!=ref or finished['result_sha256']!=manifest['reference_sha256']:
     raise RuntimeError('Q05 cloud SQL mismatch: '+plan+'/'+trial_id)
    stage_sec=sum(float(e['stage_seconds']) for e in edges)
    curl_sec=sum(float(e['curl_http_total_seconds']) for e in edges)
    join_sec=float(finished['join_seconds'])
    payload=sum(int(e['curl_size_upload']) for e in edges)
    rec={'query_id':QUERY_ID,'run_id':run_id,'plan_id':plan,'trial':count,
      'execution_mode':'real_multicloud','scale_factor':1,'placement_id':PLACEMENT_ID,
      'joined_at':dest,'transfer_edges':edges,
      'optimizer_role':('selected_egressopt' if plan==manifest.get('optimizer_decision',{}).get('selected_plan')
                         else 'predeclared_fastest' if manifest.get('optimizer_decision') else 'candidate_baseline'),

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
     write_json(plan_dir/f'warmup_{count:03d}.json',rec)
     write_csv(directory/'warmup_metrics.csv',warmups,tuple(warmups[0]))
    else:
     write_json(plan_dir/f'trial_{count:03d}.json',rec)
     measured.append({**summary_row(run_id,rec,manifest['reference_sha256']),
                     'network_bytes_measured':True, 'http_payload_bytes':payload})
     write_csv(directory/'trial_metrics.csv',measured,METRIC_FIELDS+('http_payload_bytes',))
    typ=f'WARMUP={count}' if warm else f'trial={count}'
    print(f'{plan:<20} {typ} correctness=PASS real_HTTP_bytes={payload:,} '
       f'edges={len(edges)} stage+HTTPS+join={rec["total_seconds"]:.3f}s',flush=True)
  manifest['status']='COMPLETED';manifest['completed_utc']=utcnow()
  manifest['validated_trial_count']=len(measured)
  manifest['warmup_count']=len(warmups)
  manifest['trial_metrics_sha256']=sha_file(directory/'trial_metrics.csv')
  write_json(directory/'run.json',manifest)
  print('Q05_EGRESSOPT_PROSPECTIVE_CLOUD_PASS' if manifest.get('optimizer_decision') else 'Q05_REAL_THREE_CLOUD_PASS')
  print('Q05 cloud run: '+str(directory))
  return 0
 except Exception as exc:
  manifest['status']='FAILED';manifest['failure']=type(exc).__name__+': '+str(exc)
  manifest['completed_utc']=utcnow()
  raise
 finally:
  for cloud in ('aws','azure','gcp'):
   if state['service'][cloud]:
    try:
     q05_remote(cloud,'systemctl stop egressopt-q05-'+runkey+'-'+cloud+' >/dev/null 2>&1 || true; '
       'rm -rf /opt/egressopt/runtime/q05_'+runkey+'_'+cloud+'; echo Q05_RUNTIME_REMOVED',
       'stop-runtime')
     manifest['cleanup'][cloud+'_runtime']='removed'
    except Exception as e:manifest['cleanup'][cloud+'_runtime']='FAILED: '+str(e)
  for rid in state['rules']['aws']:
   try:q05_aws('ec2','revoke-security-group-ingress','--group-id',aws_sg[0],
               '--security-group-rule-ids',rid)
   except Exception as e:manifest['cleanup']['aws_rule_'+rid]='FAILED: '+str(e)
   else:manifest['cleanup']['aws_rule_'+rid]='removed'
  for name in state['rules']['azure']:
   try:q05_cli(['az','network','nsg','rule','delete','--resource-group',AZ_RG,
                 '--nsg-name',AZ_NSG,'--name',name,'--output','none'])
   except Exception as e:manifest['cleanup']['azure_rule_'+name]='FAILED: '+str(e)
   else:manifest['cleanup']['azure_rule_'+name]='removed'
  for name in state['rules']['gcp']:
   try:q05_gcloud('compute','firewall-rules','delete',name,'--quiet')
   except Exception as e:manifest['cleanup']['gcp_rule_'+name]='FAILED: '+str(e)
   else:manifest['cleanup']['gcp_rule_'+name]='removed'
  if state['gcp_tag_added']:
   try:q05_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
              '--tags='+state['gcp_tag'],'--quiet','--format=json')
   except Exception as e:manifest['cleanup']['gcp_tag']='FAILED: '+str(e)
   else:manifest['cleanup']['gcp_tag']='removed'
  if state['gcp_external_added']:
   try:q05_gcloud('compute','instances','delete-access-config',GCP_VM,'--zone='+GCP_ZONE,
          '--network-interface=nic0','--access-config-name=external-nat',
          '--quiet','--format=json')
   except Exception as e:manifest['cleanup']['gcp_external_ip']='FAILED: '+str(e)
   else:manifest['cleanup']['gcp_external_ip']='removed'
  for cloud in ('aws','azure','gcp'):
   if not (args.stop_all_after_run or state['started'][cloud]):continue
   try:
    if cloud=='aws':
     current=q05_aws_instance()['State']['Name']
     if current not in {'stopped','stopping'}:
      q05_aws('ec2','stop-instances','--instance-ids',AWS_INSTANCE)
     q05_cli(['aws','ec2','wait','instance-stopped','--instance-ids',AWS_INSTANCE,
       '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=670)
     if q05_aws_instance()['State']['Name']!='stopped':raise RuntimeError('AWS stop not verified')
     result='stopped_verified'
    elif cloud=='azure':
     if q05_azure_state()!='PowerState/deallocated':
      q05_cli(['az','vm','deallocate','--resource-group',AZ_RG,'--name',AZ_VM,
          '--output','none'],timeout=690)
     if q05_azure_state()!='PowerState/deallocated':raise RuntimeError('Azure deallocation not verified')
     result='deallocated_verified'
    else:
     if q05_gcp_instance()['status']!='TERMINATED':
      q05_gcloud('compute','instances','stop',GCP_VM,'--zone='+GCP_ZONE,
             '--quiet','--format=json',timeout=690)
     if q05_gcp_instance()['status']!='TERMINATED':raise RuntimeError('GCP stop not verified')
     result='stopped_verified'
    manifest['cleanup'][cloud+'_vm']=result
   except Exception as e:manifest['cleanup'][cloud+'_vm']='FAILED: '+str(e)
  manifest['cleanup_incomplete']=any(str(x).startswith('FAILED') for x in manifest['cleanup'].values())
  write_json(directory/'run.json',manifest)
  if manifest['cleanup_incomplete']:
   print('WARNING: Q05 cleanup incomplete; inspect run.json',file=sys.stderr)
  print('Q05 experiment report: '+str(directory),flush=True)


def q05_cleanup_failed_run(args):
 """Retry cleanup from a failed run manifest, using only its recorded IDs.

 This does NOT change status=FAILED or turn partial trials into an experiment.
 It never starts VMs, creates network rules, or guesses an unrecorded IP.
 """
 if not args.run_id or not re.fullmatch(r'\d{8}T\d{6}Z_[0-9a-f]{8}',args.run_id):
  raise ValueError('Supply the exact failed Q05 --run-id YYYYMMDDTHHMMSSZ_hex8')
 directory=q05_run_root(args)/args.run_id
 manifest=read_json(directory/'run.json')
 if (manifest.get('query_id'),manifest.get('run_id'),manifest.get('placement'))!=('Q05',args.run_id,'P1'):
  raise ValueError('Q05 run identity mismatch; refusing cleanup')
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
    'aws':lambda:q05_aws_instance()['State']['Name'],
    'azure':q05_azure_state,
    'gcp':lambda:q05_gcp_instance()['status'],
   }[cloud])()
   if status not in ('running','PowerState/running','RUNNING'):
    return 'vm_not_running; runtime deletion not independently confirmed'
   script=('systemctl stop egressopt-q05-'+key+'-'+cloud+
     ' >/dev/null 2>&1 || true; rm -rf /opt/egressopt/runtime/q05_'+key+'_'+cloud+
     '; echo Q05_RUNTIME_REMOVED')
   output=q05_remote(cloud,script,'recovery-stop-runtime')
   if 'Q05_RUNTIME_REMOVED' not in output:
    raise RuntimeError('Q05 cleanup confirmation missing')
   return 'removed'
  call(cloud+'_runtime',remove_runtime)

 # Delete only resource identifiers present in the archived manifest.
 fw=manifest.get('firewall_rules',{})
 if not isinstance(fw,dict):raise ValueError('Malformed Q05 firewall manifest')
 aws_rules=[v for k,v in fw.items() if k in ('aws_from_azure','aws_from_gcp')]
 for rid in aws_rules:
  if not re.fullmatch(r'sgr-[0-9a-f]+',str(rid)):
   cleanout['aws_rule_'+str(rid)]='FAILED: invalid recorded AWS rule ID';continue
  def remove_aws(rid=rid):
   instance=q05_aws_instance()
   sgs=[r['GroupId'] for r in instance['SecurityGroups']]
   if len(sgs)!=1:raise RuntimeError('Expected one AWS security group')
   try:q05_aws('ec2','revoke-security-group-ingress','--group-id',sgs[0],
        '--security-group-rule-ids',rid)
   except RuntimeError as exc:
    if 'InvalidSecurityGroupRuleId.NotFound' not in str(exc):raise
   return 'removed_or_absent'
  call('aws_rule_'+rid,remove_aws)
 for origin in ('aws','gcp'):
  name=fw.get('azure_from_'+origin)
  if not name:continue
  if name!=f'egressopt-q05-{key}-{origin}':
   cleanout['azure_rule_'+str(name)]='FAILED: foreign rule ID';continue
  def remove_az(name=name):
   existing=json.loads(q05_cli(['az','network','nsg','rule','list',
      '--resource-group',AZ_RG,'--nsg-name',AZ_NSG,'--output','json']))
   if any(r.get('name')==name for r in existing):
    q05_cli(['az','network','nsg','rule','delete','--resource-group',AZ_RG,
        '--nsg-name',AZ_NSG,'--name',name,'--output','none'])
   return 'removed_or_absent'
  call('azure_rule_'+name,remove_az)
 if 'gcp_from_azure' in fw:
  name=fw['gcp_from_azure']
  expected='egressopt-q05-'+key
  if name==expected:
   def remove_gcp():
    try:q05_gcloud('compute','firewall-rules','describe',name,'--format=json')
    except RuntimeError as exc:
     if 'not found' not in str(exc).lower() and 'notfound' not in str(exc).lower():raise
     return 'removed_or_absent'
    q05_gcloud('compute','firewall-rules','delete',name,'--quiet')
    return 'removed_or_absent'
   call('gcp_rule_'+name,remove_gcp)
   def remove_gcp_tag():
    inst=q05_gcp_instance()
    if name in inst.get('tags',{}).get('items',[]):
     q05_gcloud('compute','instances','remove-tags',GCP_VM,'--zone='+GCP_ZONE,
       '--tags='+name,'--quiet','--format=json')
    return 'removed_or_absent'
   call('gcp_tag',remove_gcp_tag)
  else:cleanout['gcp_rule_'+str(name)]='FAILED: foreign rule ID'
 if manifest.get('gcp_external_ip_added_by_this_run') is True:
  def remove_dynamic_gcp_nat():
   inst=q05_gcp_instance()
   configs=[c for nic in inst['networkInterfaces'] for c in nic.get('accessConfigs',[])]
   if any(c.get('name')=='external-nat' for c in configs):
    q05_gcloud('compute','instances','delete-access-config',GCP_VM,
      '--zone='+GCP_ZONE,'--network-interface=nic0',
      '--access-config-name=external-nat','--quiet','--format=json')
   return 'removed_or_absent'
  call('gcp_external_ip',remove_dynamic_gcp_nat)
 elif 'gcp_external_ip_added_by_this_run' not in manifest:
  # The older runner did not record whether it assigned the external IP.
  # Verify absence without deleting a potentially pre-existing shared address.
  def unknown_external_ip():
   inst=q05_gcp_instance()
   configs=[c for nic in inst['networkInterfaces'] for c in nic.get('accessConfigs',[])]
   if not any(c.get('natIP') for c in configs):return 'absent_verified'
   return 'origin_not_recorded_in_old_runner; inspect manually'
  call('gcp_external_ip',unknown_external_ip)

 if args.stop_all_after_run:
  for cloud in ('aws','azure','gcp'):
   def stop(cloud=cloud):
    if cloud=='aws':
     if q05_aws_instance()['State']['Name']!='stopped':
      q05_aws('ec2','stop-instances','--instance-ids',AWS_INSTANCE)
     q05_cli(['aws','ec2','wait','instance-stopped','--instance-ids',AWS_INSTANCE,
         '--profile',AWS_PROFILE,'--region',AWS_REGION],timeout=670)
     if q05_aws_instance()['State']['Name']!='stopped':raise RuntimeError('AWS not stopped')
     return 'stopped_verified'
    if cloud=='azure':
     if q05_azure_state()!='PowerState/deallocated':
      q05_cli(['az','vm','deallocate','--resource-group',AZ_RG,'--name',AZ_VM,
        '--output','none'],timeout=690)
     if q05_azure_state()!='PowerState/deallocated':raise RuntimeError('Azure not deallocated')
     return 'deallocated_verified'
    if q05_gcp_instance()['status']!='TERMINATED':
     q05_gcloud('compute','instances','stop',GCP_VM,'--zone='+GCP_ZONE,
       '--quiet','--format=json',timeout=690)
    if q05_gcp_instance()['status']!='TERMINATED':raise RuntimeError('GCP not stopped')
    return 'stopped_verified'
   call(cloud+'_vm',stop)
 cleanup.update(cleanout)
 manifest['cleanup_repair_utc']=utcnow()
 unresolved=[k for k,v in cleanout.items() if v.startswith('FAILED') or
    'not independently confirmed' in v or v.startswith('origin_not_recorded')]
 manifest['cleanup_incomplete']=bool(unresolved)
 manifest['cleanup_repair_unresolved']=unresolved
 write_json(directory/'run.json',manifest)
 print('Q05_CLEANUP_REPAIR_'+('INCOMPLETE' if unresolved else 'PASS'))
 print('Q05 cleanup manifest: '+str(directory/'run.json'))
 if unresolved:print('Remaining issues: '+', '.join(unresolved))
 print('Existing partial Q05 trials remain FAILED and excluded from summaries.')
 return 1 if unresolved else 0


def q05_reproduce_cloud(dir,args):
 manifest,rows=q05_audit_cloud(dir)
 analysis=dir/'q05_analysis';analysis.mkdir(exist_ok=True)
 summary=[]
 for plan in manifest.get('executed_plans',PLANS):
  chosen=[r for r in rows if r['plan_id']==plan]
  latency=[float(r['total_seconds'])*1000 for r in chosen]
  payload=[int(r['http_payload_bytes']) for r in chosen]
  summary.append({'plan_id':plan,'n':len(chosen),
   'mean_workflow_ms':statistics.mean(latency),'median_workflow_ms':statistics.median(latency),
   'sd_workflow_ms':statistics.stdev(latency) if len(latency)>1 else None,
   'mean_http_payload_bytes':statistics.mean(payload),
   'min_http_payload_bytes':min(payload),'max_http_payload_bytes':max(payload),
   'edges_per_trial':len(Q05_PIPE[plan]),'correct':len(chosen)})
 write_csv(analysis/'plan_summary.csv',summary,tuple(summary[0]))
 write_json(analysis/'paper_summary.json',{'query_id':'Q05','run_id':manifest['run_id'],
  'execution_mode':'real_multicloud','summary':summary,'note':'HTTP payload only, not provider-billed egress; timing is stage+curl+join, not isolated network time'})
 lines=['% Q05 SF1/P1 real-cloud candidate plans, provider-billed cost unmeasured',
  r'\begin{tabular}{lrrr}',r'\hline',
  r'Plan & Trials & Mean workflow (ms) & Mean HTTP bytes \\',r'\hline']
 for rec in summary:
  name=rec['plan_id'].replace('_',r'\_')
  lines.append(f'{name} & {rec["n"]} & {rec["mean_workflow_ms"]:.2f} & {rec["mean_http_payload_bytes"]:.0f} '+r'\\')
 lines.extend([r'\hline',r'\end{tabular}'])
 (analysis/'q05_cloud_table.tex').write_text('\n'.join(lines)+'\n',encoding='utf-8')
 if args.plots:
  import matplotlib
  matplotlib.use('Agg')
  import matplotlib.pyplot as plt
  labels=[x['plan_id'] for x in summary];xs=list(range(len(labels)))
  for kind,key,ylab in [('payload','mean_http_payload_bytes','Mean HTTP payload (bytes)'),
                         ('latency','mean_workflow_ms','Mean workflow duration (ms)')]:
   fig,ax=plt.subplots(figsize=(7.1,3.8))
   ax.bar(xs,[s[key] for s in summary]);ax.set_xticks(xs,labels,rotation=15,ha='right')
   ax.set_ylabel(ylab);ax.set_title('Q05 SF1/P1 real AWS–Azure–GCP comparison')
   fig.tight_layout()
   for fmt in ('png','pdf'):fig.savefig(analysis/f'q05_cloud_{kind}.{fmt}',dpi=240)
   plt.close(fig)
 print('Q05_REPRODUCE_PASS')
 print('Reproduced Q05 real-cloud figures: '+str(analysis))
 return 0


if __name__=="__main__":
    raise SystemExit(main())
