#!/usr/bin/env python3
"""EgressOpt Phase 2 Q05 — non-destructive source gate and candidate-edge enumerator.

Phase 2 includes separate experimental M6d signed SQL and streamed file transport.
Real SF1 multi-cloud pilots and paired repetitions are completed.
SF10 live execution requires explicit M6M dataset/deployment evidence gates.
The original q05.py and recorded Phase 1 results remain untouched.

Usage (from anywhere):
    python experiments/q05-phase2.py --mode inspect
    python experiments/q05-phase2.py --mode enumerate
    python experiments/q05-phase2.py --mode enumerate --write
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

QUERY = 'Q05'
# Public-release source is identical in experiment logic but has no private resource defaults.
# Measured Phase1 historical runner SHA256: bc7ac734062e88e13e4a23b3b6368c7a5623c6a806adfef63c9d913462526d21
ORIGINAL_RUNNER_SHA256 = 'a37a423410cbefbd2d0925741eafc6991a3e26e87e77e734c33d60060b675ed2'
LOCKED_SQL_SHA256 = '495def7debb02ab1def31da07fbc88b4796813b754832ce15d54a35dcb104577'
# The existing placement and SQL join predicates are read-only evidence.
PLACEMENT = {'customer': 'azure', 'orders': 'aws', 'lineitem': 'azure', 'supplier': 'gcp', 'nation': 'azure', 'region': 'gcp'}
PREFIX = {'c': 'customer', 'o': 'orders', 'l': 'lineitem', 's': 'supplier', 'n': 'nation', 'r': 'region'}
EXPECTED_JOINS = [('c_custkey', 'o_custkey'), ('l_orderkey', 'o_orderkey'), ('l_suppkey', 's_suppkey'), ('c_nationkey', 's_nationkey'), ('s_nationkey', 'n_nationkey'), ('n_regionkey', 'r_regionkey')]
# Query-specific single-table predicates that may be pushed into source keys.
# Each clause is checked literally against the locked source SQL before use.
LOCAL_PREDICATES = {'region': ["r_name = 'ASIA'"]}
DELTAS = (0.0, 0.2, 0.5)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_source(project: Path) -> tuple[str, list[dict]]:
    old = project/'experiments'/('q'+QUERY[1:] + '.py')
    sqlpath = project/'queries'/'tpch'/('q'+QUERY[1:] + '.sql')
    for path in (old,sqlpath):
        if not path.is_file():
            raise FileNotFoundError(f'Missing original Phase 1 input: {path}')
    if digest(old) != ORIGINAL_RUNNER_SHA256:
        raise RuntimeError(f'{QUERY}: original runner differs from frozen Phase 1 SHA-256; stop and review before proceeding')
    if digest(sqlpath) != LOCKED_SQL_SHA256:
        raise RuntimeError(f'{QUERY}: SQL differs from frozen Phase 1 SHA-256; stop and review before proceeding')
    sql = sqlpath.read_text(encoding='utf8')
    clean = re.sub(r'/\*.*?\*/', ' ', re.sub(r'--[^\n]*', ' ', sql), flags=re.S)
    join_tokens = re.findall(r'\b([a-z]_[a-z0-9_]+)\s*=\s*([a-z]_[a-z0-9_]+)\b', clean, re.I)
    joins = set()
    for l,r in join_tokens:
        left,right=l[0].lower(),r[0].lower()
        if left not in PREFIX or right not in PREFIX or left==right:
            continue
        items=tuple(sorted((l.lower(),r.lower())))
        joins.add(items)
    if joins != set(tuple(sorted(k)) for k in EXPECTED_JOINS):
        raise RuntimeError(f'{QUERY}: unexpected join graph: {sorted(joins)}')
    for table, conditions in LOCAL_PREDICATES.items():
        for fragment in conditions:
            if re.sub(r'\s+','',fragment.lower()) not in re.sub(r'\s+','',clean.lower()):
                raise RuntimeError(f'{QUERY}: source filter no longer matches locked template ({table})')
    edges=[]
    for a,b in sorted(joins):
        edges.append({'left_table':PREFIX[a[0]],'left_column':a,
                      'right_table':PREFIX[b[0]],'right_column':b,
                      'left_provider':PLACEMENT[PREFIX[a[0]]],
                      'right_provider':PLACEMENT[PREFIX[b[0]]]})
    return sql,edges


def enumerate_filters(edges: list[dict]) -> list[dict]:
    """Enumerate directed semijoin *filters* at inter-provider equijoin edges.

    A filter is a legal logical candidate for INNER equality joins if all original
    joins remain in the final SQL. Filtering alone is not a complete physical
    multi-cloud plan: it still needs a fragment compiler and equivalence testing.
    """
    out=[]
    for edge in edges:
        for side in ('left','right'):
            opposite='right' if side=='left' else 'left'
            source=edge[side+'_table'];destination=edge[opposite+'_table']
            site_a=PLACEMENT[source];site_b=PLACEMENT[destination]
            if site_a==site_b:
                continue
            src_col=edge[side+'_column'];dst_col=edge[opposite+'_column']
            filters=LOCAL_PREDICATES.get(source, [])
            predicates=' AND '.join('s.'+f for f in filters)
            where=' WHERE '+predicates if predicates else ''
            # Each stage returns keys only; duplicate/NULL semantics are preserved
            # when the original INNER JOIN remains in the final plan.
            key_sql=f'SELECT DISTINCT s.{src_col} AS join_key FROM {source} AS s'+where
            receiver_sql=(f'SELECT r.* FROM {destination} AS r SEMI JOIN '
                          f'received_keys AS k ON r.{dst_col} = k.join_key')
            out.append({'source_table':source,'destination_table':destination,
                        'source_provider':site_a,'destination_provider':site_b,
                        'source_join_key':src_col,'receiver_join_key':dst_col,
                        'key_generation_sql':key_sql,
                        'destination_filter_sql':receiver_sql,
                        'full_original_joins_must_be_preserved':True,
                        'execution_compatible':False,
                        'unmeasured':True})
    return out



# Phase 2 M2: executable DuckDB SQL-fragment compiler and portable SQLite
# logical-equivalence smoke tests. NO CLOUD WORKER / NO BILLING / NO RESULTS.
from itertools import combinations
import sqlite3
from decimal import Decimal

COLUMNS = {
 'Q03': ('customer','orders','lineitem'),
 'Q05': ('customer','orders','lineitem','supplier','nation','region')
}

# Column-pruning policy: only fields used by the frozen SQL's joins,
# filters, groupings, or final SELECT are staged/transferred.
# Importantly, the *same* policy applies to the no-semijoin baseline.
# Dropping input predicates from the final SQL is NOT supported.
REQUIRED_SQL_COLUMNS = {
 'Q03': {
   'customer': ('c_custkey','c_mktsegment'),
   'orders': ('o_orderkey','o_custkey','o_orderdate','o_shippriority'),
   'lineitem': ('l_orderkey','l_extendedprice','l_discount','l_shipdate'),
 },
 'Q05': {
   'customer': ('c_custkey','c_nationkey'),
   'orders': ('o_orderkey','o_custkey','o_orderdate'),
   'lineitem': ('l_orderkey','l_suppkey','l_extendedprice','l_discount'),
   'supplier': ('s_suppkey','s_nationkey'),
   'nation': ('n_nationkey','n_regionkey','n_name'),
   'region': ('r_regionkey','r_name'),
 },
}


def projection_sql(table: str, alias: str = 'b') -> str:
    """Explicit, fixed SQL columns; never project entire TPC-H base rows."""
    required = REQUIRED_SQL_COLUMNS[QUERY]
    if table not in required:
        raise ValueError('Unsupported table for frozen SQL projection: ' + table)
    return ', '.join(alias+'.'+column for column in required[table])


def diag_candidates(edges: list[dict], max_filters: int = 2) -> list[dict]:
    """Diagnostic candidates for the known WAN-bound lineitem relation.

    Uses a stated structural rule (orders keys -> lineitem, final AWS).
    NOT a measured optimizer recommendation or a cost-model selection.
    """
    flt, specs = candidate_specs(edges, max_filters=max_filters)
    matches = [i for i,f in enumerate(flt)
               if f['source_table'] == 'orders' and f['destination_table'] == 'lineitem']
    assert len(matches) == 1, 'Frozen Q03/Q05 join graph changed'
    index = matches[0]
    chosen = [i for i,s in enumerate(specs)
              if s['final_site'] == 'aws' and s['filter_indexes'] == [index]]
    baseline = [i for i,s in enumerate(specs)
                if s['final_site'] == 'aws' and not s['filter_indexes']]
    assert len(chosen) == len(baseline) == 1
    return [
      {'role':'projection_only_baseline','candidate_index':baseline[0],
       'final_site':'aws','filter_indexes':[]},
      {'role':'diagnostic_filter_of_transferred_lineitem','candidate_index':chosen[0],
       'final_site':'aws','filter_indexes':[index],
       'filter_direction':'orders (AWS) -> lineitem (Azure)',
       'selection_type':'STRUCTURAL_DIAGNOSTIC_NOT_COST_OPTIMIZATION'},
    ]


def active_filters(sql):
    """Only predicates present in the exact source-hash-locked Qxx.sql.

    The original final query remains unchanged, even after local pushdowns.
    """
    if QUERY == 'Q03':
        segment=re.search(r"c_mktsegment\s*=\s*'([^']+)'",sql,re.I)
        before=re.search(r"o_orderdate\s*<\s*CAST\('([0-9-]+)' AS date\)",sql,re.I)
        after=re.search(r"l_shipdate\s*>\s*CAST\('([0-9-]+)' AS date\)",sql,re.I)
        if not all((segment,before,after)):
            raise ValueError('Unsupported Q03 filter shape')
        return {'customer':("c_mktsegment = '"+segment.group(1)+"'",),
                'orders':("o_orderdate < CAST('"+before.group(1)+"' AS DATE)",),
                'lineitem':("l_shipdate > CAST('"+after.group(1)+"' AS DATE)",)}
    region=re.search(r"r_name\s*=\s*'([^']+)'",sql,re.I)
    after=re.search(r"o_orderdate\s*>=\s*CAST\('([0-9-]+)' AS date\)",sql,re.I)
    before=re.search(r"o_orderdate\s*<\s*CAST\('([0-9-]+)' AS date\)",sql,re.I)
    if not all((region,after,before)):
        raise ValueError('Unsupported Q05 filter shape')
    return {'region':("r_name = '"+region.group(1)+"'",),
            'orders':("o_orderdate >= CAST('"+after.group(1)+"' AS DATE)",
                      "o_orderdate < CAST('"+before.group(1)+"' AS DATE)"),}


def candidate_specs(edges, max_filters=2):
    """All empty/single/pair directed cross-cloud semijoin combinations.

    Direct key extraction uses the original *base* source plus single-table
    predicates, not a potentially circular reduced source.  Each filter is
    therefore independent and preserves inner-join result multiset semantics.
    """
    if not 0 <= max_filters <= 2:
        raise ValueError('For this release max_filters must lie in [0,2]')
    filters=enumerate_filters(edges)
    destinations=sorted(set(PLACEMENT.values()))
    specs=[]
    for final in destinations:
        for count in range(max_filters+1):
            for indexes in combinations(range(len(filters)),count):
                specs.append({'final_site':final,'filter_indexes':list(indexes)})
    return filters,specs


def qlit(value):
    return "'"+str(value).replace("'","''")+"'"


def valid_slug(value):
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,60}',value):
        raise ValueError('Unsafe trial identifier')
    return value


def physical_plan(project, sql, edges, candidate, sf=1, run_id='m2preview',max_filters=2,
                  data_root_override=None,runtime_root_override=None):
    """Compile an independent SQL/Parquet/transfer DAG with provider sites.

    This is a *transport-independent* cloud execution plan: it defines each
    source SQL fragment and every required directed transfer, but does NOT
    start the Phase-1 fixed-plan HTTPS worker or pretend execution occurred.
    """
    if not 1 <= sf <= 100 or sf not in (1,10,100):
        raise ValueError('Only planned SF1/SF10/SF100 source paths are supported')
    valid_slug(run_id)
    filters,specs=candidate_specs(edges,max_filters=max_filters)
    if not 0<=candidate<len(specs):
        raise ValueError('Candidate index out of range')
    spec=specs[candidate]
    final=spec['final_site']
    selected=[(i,filters[i]) for i in spec['filter_indexes']]
    table_pred=active_filters(sql)
    runtime=str(runtime_root_override) if runtime_root_override else f'/opt/egressopt/runtime/phase2_{QUERY.lower()}_{run_id}'
    data_root=str(data_root_override) if data_root_override else f'/opt/egressopt/data/tpch_sf{sf}'
    steps=[]
    key_info={}
    for i,f in selected:
        key_name=f'key_{i:02d}'
        src,dst=f['source_provider'],f['destination_provider']
        table=f['source_table']
        source_path=f'{data_root}/{table}.parquet'
        local=f'{runtime}/stage/{key_name}.parquet'
        received=f'{runtime}/received/{key_name}.parquet'
        cond=table_pred.get(table,())
        where=(' WHERE '+' AND '.join(cond)) if cond else ''
        key_sql=(f'SELECT DISTINCT {f["source_join_key"]} AS join_key '
                 f'FROM read_parquet({qlit(source_path)}){where}')
        steps.append({'id':'build_'+key_name,'type':'duckdb_copy','site':src,
                      'depends_on':[], 'output':local,
                      'sql':f'COPY ({key_sql}) TO {qlit(local)} (FORMAT PARQUET, COMPRESSION ZSTD)'})
        steps.append({'id':'transfer_'+key_name,'type':'transfer_file','from':src,'to':dst,
                      'depends_on':['build_'+key_name],'source':local,'destination':received,
                      'payload_metric':'HTTP request-body file bytes (when actually transferred)'})
        key_info[i]=(received,f)
    for table in COLUMNS[QUERY]:
        site=PLACEMENT[table]
        source_path=f'{data_root}/{table}.parquet'
        local=f'{runtime}/stage/table_{table}.parquet'
        received=f'{runtime}/received/table_{table}.parquet'
        preds=list(table_pred.get(table,()))
        deps=[]
        for idx,(keyfile,f) in key_info.items():
            if f['destination_table']==table:
                col=f['receiver_join_key']
                preds.append('EXISTS (SELECT 1 FROM read_parquet('+qlit(keyfile)
                             +') AS k WHERE b.'+col+' = k.join_key)')
                deps.append('transfer_key_%02d'%idx)
        cond=(' WHERE '+' AND '.join(preds)) if preds else ''
        query=(f'SELECT {projection_sql(table)} FROM read_parquet({qlit(source_path)}) AS b'+cond)
        steps.append({'id':'build_table_'+table,'type':'duckdb_copy','site':site,
                      'depends_on':deps,'output':local,
                      'sql':f'COPY ({query}) TO {qlit(local)} (FORMAT PARQUET, COMPRESSION ZSTD)'})
        if site!=final:
            steps.append({'id':'transfer_table_'+table,'type':'transfer_file','from':site,
                          'to':final,'depends_on':['build_table_'+table],
                          'source':local,'destination':received,
                          'payload_metric':'HTTP request-body file bytes (when actually transferred)'})
    ctes=[]
    finalize_deps=[]
    for table in COLUMNS[QUERY]:
        site=PLACEMENT[table]
        path=(f'{runtime}/stage/table_{table}.parquet' if site==final else
              f'{runtime}/received/table_{table}.parquet')
        ctes.append(f'{table} AS (SELECT * FROM read_parquet({qlit(path)}))')
        finalize_deps.append(('build_table_' if site==final else 'transfer_table_')+table)
    final_sql='WITH '+', '.join(ctes)+'\n'+sql.strip().rstrip(';')
    steps.append({'id':'finish','type':'duckdb_query','site':final,
                  'depends_on':finalize_deps,'sql':final_sql})
    return {'query':QUERY,'candidate_index':candidate,'total_candidates':len(specs),
            'final_site':final,'scale_factor':sf,'selected_directed_filters':selected,
            'steps':steps,'source_sql_sha256':LOCKED_SQL_SHA256,
            'phase1_original_runner_sha256':ORIGINAL_RUNNER_SHA256,
            'expected_new_result_root':str(project/'results'/'phase2'/QUERY.lower()),
            'projection_policy':'FROZEN_SQL_REQUIRED_COLUMNS_BOTH_BASELINE_AND_SEMIJOIN',
            'projected_columns':REQUIRED_SQL_COLUMNS[QUERY],
            'execution_status':'COMPILED_NOT_EXECUTED',
            'constraint_status':'LATENCY_NOT_ESTIMATED',
            'warnings':['No worker/HTTPS integration: JSON specifies the required transfers only.',
                        'No bytes measured or latency prediction from this compiler.',
                        'All final SQL retains the original frozen equijoins and aggregation.']}


def build_fixture():
    """Synthetic adversarial relations for portable SQLite multiset smoke checks."""
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE customer(c_custkey INT,c_mktsegment TEXT,c_nationkey INT)')
    db.execute('CREATE TABLE orders(o_orderkey INT,o_custkey INT,o_orderdate TEXT,o_shippriority INT)')
    db.execute('CREATE TABLE lineitem(l_orderkey INT,l_suppkey INT,l_extendedprice REAL,l_discount REAL,l_shipdate TEXT)')
    if QUERY=='Q05':
        db.execute('CREATE TABLE supplier(s_suppkey INT,s_nationkey INT)')
        db.execute('CREATE TABLE nation(n_nationkey INT,n_regionkey INT,n_name TEXT)')
        db.execute('CREATE TABLE region(r_regionkey INT,r_name TEXT)')
    # Duplicate/NULL keys, variable selectivity, intentional ties, nonmatching rows.
    customers=[(1,'BUILDING',10),(2,'BUILDING',20),(3,'AUTOMOBILE',10),
               (4,'BUILDING',10),(4,'BUILDING',10),(5,'BUILDING',None),
               (6,'BUILDING',30),(7,None,20)]
    orders=[(101,1,'1994-05-01',0),(102,2,'1994-07-01',1),
            (103,3,'1995-03-19',0),(104,4,'1994-08-08',2),
            (105,5,'1994-01-02',0),(106,6,'1993-03-02',1),
            (107,7,'1994-10-01',0),(108,None,'1994-06-01',0),
            (109,1,'1994-01-01',2),(110,1,'1994-12-31',2),
            (111,1,'1995-01-01',0),(112,1,'1995-03-15',0)]
    lines=[(101,1001,50.,.10,'1995-04-01'),(101,1002,50.,.10,'1995-04-01'),
           (101,1001,20.,.01,'1995-03-15'),(102,1002,110.,.00,'1995-05-01'),
           (103,1002,60.,.00,'1995-04-12'),(104,1001,15.,.02,'1995-05-01'),
           (104,1001,15.,.02,'1995-05-01'),(105,1001,20.,.2,'1995-05-01'),
           (106,1003,55.,.3,'1995-05-01'),(107,1002,90.,.1,'1995-05-01'),
           (108,1002,55.,.0,'1995-05-01'),(109,1002,10.,.0,'1995-03-16'),
           (110,None,40.,.2,'1995-03-16'),(111,1001,17.,.0,'1995-03-16'),
           (112,1002,21.,.0,'1995-03-16')]
    db.executemany('INSERT INTO customer VALUES (?,?,?)',customers)
    db.executemany('INSERT INTO orders VALUES (?,?,?,?)',orders)
    db.executemany('INSERT INTO lineitem VALUES (?,?,?,?,?)',lines)
    if QUERY=='Q05':
        db.executemany('INSERT INTO supplier VALUES (?,?)',
                       [(1001,10),(1002,20),(1003,30),(1004,None)])
        db.executemany('INSERT INTO nation VALUES (?,?,?)',
                       [(10,1,'ALPHA'),(20,1,'BETA'),(30,2,'GAMMA'),(40,None,'EMPTY')])
        db.executemany('INSERT INTO region VALUES (?,?)',[(1,'ASIA'),(2,'EUROPE'),(3,'ASIA')])
    return db


def sqlite_dates(sql):
    # SQLite CAST(text AS date) coerces ISO 1995-03-15 -> integer 1995.
    # Replace this for logical smoke testing ONLY; native DuckDB retains original.
    return re.sub(r"CAST\('([0-9]{4}-[0-9]{2}-[0-9]{2})' AS DATE\)",
                  lambda m: qlit(m.group(1)),sql,flags=re.I)


def canonical_rows(rows):
    # Do not use SQLite smoke results for TPC-H/Parquet numeric accuracy claims.
    if QUERY=='Q03':
        return sorted((int(x[0]),round(float(x[1]),4),str(x[2]),int(x[3])) for x in rows)
    return sorted((str(x[0]),round(float(x[1]),4)) for x in rows)




def local_duckdb_execution(project, sql, edges, candidate, sf, max_filters, data_dir=None):
    """Run each compiled site-labelled DuckDB stage locally with COPY handoffs.

    This is NOT an AWS/Azure/GCP result and carries no WAN or billing metrics.
    Remote exchanges are simulated by local file copies; only Parquet sizes
    can be inspected and are explicitly called simulated payload bytes.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError('DuckDB required for local execution. Run: python -m pip install duckdb') from exc
    import shutil
    import tempfile
    import time
    from decimal import Decimal, ROUND_HALF_EVEN
    from collections import Counter
    data_dir=Path(data_dir) if data_dir is not None else project/'data'/'tpch'/f'parquet_sf{sf}'
    data_dir=data_dir.resolve()
    required=[data_dir/(table+'.parquet') for table in COLUMNS[QUERY]]
    missing=[str(x) for x in required if not x.is_file()]
    if missing:
        raise FileNotFoundError('Input Parquet files unavailable (no generated data substituted): '+', '.join(missing))
    def signature(rows):
        # Compare output *multisets*, not row ordering, because Q05's
        # ORDER BY revenue can have ties. Numeric DECIMAL is canonically
        # rounded to the original query runner's output precision.
        prec=Decimal('0.0001') if QUERY=='Q03' else Decimal('0.01')
        out=[]
        for row in rows:
            v=Decimal(str(row[1]))
            canon=format(v.quantize(prec,rounding=ROUND_HALF_EVEN),'f')
            out.append((str(row[0]),canon)+tuple(str(z) for z in row[2:]))
        return sorted(out)
    with tempfile.TemporaryDirectory(prefix='egressopt_phase2_local_') as tmp:
        run_dir=Path(tmp)/'artifacts'; run_dir.mkdir(parents=True,exist_ok=True)
        plan=physical_plan(project,sql,edges,candidate,sf=sf,
                           run_id='localtest',max_filters=max_filters,
                           data_root_override=data_dir,runtime_root_override=run_dir)
        exec_log=[]; payload=0; stage_total=0.; copy_total=0.; finish_seconds=0.
        con=duckdb.connect(database=':memory:')
        try:
            for step in plan['steps']:
                start=time.perf_counter()
                if step['type']=='duckdb_copy':
                    out=Path(step['output']);out.parent.mkdir(parents=True,exist_ok=True)
                    con.execute(step['sql'])
                    category='source_sql_materialization'
                    stage_total+=time.perf_counter()-start
                    size=out.stat().st_size
                elif step['type']=='transfer_file':
                    src=Path(step['source']);dst=Path(step['destination'])
                    dst.parent.mkdir(parents=True,exist_ok=True)
                    shutil.copy2(src,dst)
                    size=src.stat().st_size;payload+=size
                    category='local_file_copy_PROXY_NOT_WAN'
                    copy_total+=time.perf_counter()-start
                elif step['type']=='duckdb_query':
                    output=con.execute(step['sql']).fetchall()
                    size=0;category='destination_query'
                    finish_seconds+=time.perf_counter()-start
                else:raise ValueError('Unknown stage '+step['type'])
                exec_log.append({'id':step['id'],'site':step.get('site',step.get('from')),
                                 'operation':category,'elapsed_seconds':time.perf_counter()-start,
                                 'artifact_bytes':size})
            references=', '.join(table+' AS (SELECT * FROM read_parquet('+qlit(data_dir/(table+'.parquet'))+'))'
                                 for table in COLUMNS[QUERY])
            reference=con.execute('WITH '+references+'\n'+sql.strip().rstrip(';')).fetchall()
            actual_sig=signature(output);reference_sig=signature(reference)
            if actual_sig!=reference_sig:
                raise AssertionError('DuckDB compiled plan differs from frozen SQL reference on local Parquet')
            return {'mode':'LOCAL_DUCKDB_SIMULATION','query':QUERY,'candidate_index':candidate,
                    'source_guard':'PASS','correct_vs_reference':True,
                    'duckdb_version':duckdb.__version__,'reference_rows':len(reference),
                    'plan_rows':len(output),'simulated_intermediate_copy_bytes':payload,
                    'source_stage_seconds':stage_total,'local_file_copy_seconds':copy_total,
                    'destination_query_seconds':finish_seconds,'steps':exec_log,
                    'warnings':['All plan operations ran on one local host, not AWS/Azure/GCP.',
                                'Bytes are Parquet local-file copies, not measured HTTP request bodies.',
                                'Local timings exclude WAN/TLS, cloud cost and distributed orchestration.',
                                'This tests one indexed candidate, not optimizer selection.']}
        finally:con.close()


def smoke_test(sql, edges, project, max_filters):
    """SQLite relation-level equivalence over every plan, incl. duplicate/NULL.

    No external dataset required, and this is NOT a DuckDB/cloud validation.
    """
    db=build_fixture()
    try:
        reference=canonical_rows(db.execute(sqlite_dates(sql)).fetchall())
        if not reference:
            raise AssertionError('Synthetic reference fixture produced no result')
        flt,specs=candidate_specs(edges,max_filters=max_filters)
        cases=[]
        for idx,spec in enumerate(specs):
            # The physical-plan compiler is independently invoked for each
            # combination; the relational smoke executes matching predicates.
            plan=physical_plan(project,sql,edges,idx,max_filters=max_filters)
            # Static SQL stage completeness.
            assert len([x for x in plan['steps'] if x['id'].startswith('build_table_')])==len(COLUMNS[QUERY])
            assert plan['steps'][-1]['type']=='duckdb_query'
            for stage in [step for step in plan['steps'] if step['id'].startswith('build_table_')]:
                name=stage['id'][len('build_table_'):]
                assert 'SELECT '+projection_sql(name) in stage['sql'],(QUERY,idx,name)
                assert 'SELECT b.*' not in stage['sql'],(QUERY,idx,name)

            selected=[flt[i] for i in spec['filter_indexes']]
            srcpred=active_filters(sql)
            ctes=[]
            for table in COLUMNS[QUERY]:
                conditions=list(srcpred.get(table,()))
                for i,f in zip(spec['filter_indexes'],selected):
                    if f['destination_table']==table:
                        predicate=srcpred.get(f['source_table'],())
                        src_filter=(' WHERE '+' AND '.join(predicate)) if predicate else ''
                        cond=('EXISTS (SELECT 1 FROM (SELECT DISTINCT '+f['source_join_key']+
                              ' AS join_key FROM '+f['source_table']+src_filter+') AS k WHERE '+
                              'b.'+f['receiver_join_key']+' = k.join_key)')
                        conditions.append(cond)
                where=' WHERE '+' AND '.join(conditions) if conditions else ''
                ctes.append(table+'_reduced AS (SELECT '+projection_sql(table)+' FROM '+table+' AS b'+where+')')
            sqlcte='WITH '+', '.join(ctes)+'\n'+sql.strip().rstrip(';')
            # Reference uses original names; replace FROM table names with CTEs
            # by wrapping with shadowing CTEs: existing tables stay as *_base.
            # Materialize candidate-filtered copies in a fresh attached SQLite
            # database, then run original SQL on those copies.
            test=build_fixture()
            try:
                # Evaluate intermediate filters on original untouched tables
                # before removing/replacing base tables in this test connection.
                for table in COLUMNS[QUERY]:
                    x=next(c for c in ctes if c.startswith(table+'_reduced AS '))
                    query=x[len(table+'_reduced AS ('):-1]
                    test.execute('CREATE TEMP TABLE phase2_'+table+' AS '+sqlite_dates(query))
                # Rename each unfiltered base table so that unmodified source
                # SQL resolves directly to the new filtered temp relations.
                for table in COLUMNS[QUERY]:
                    test.execute('ALTER TABLE '+table+' RENAME TO base_'+table)
                    test.execute('CREATE TEMP VIEW '+table+' AS SELECT * FROM phase2_'+table)
                outcome=canonical_rows(test.execute(sqlite_dates(sql)).fetchall())
            finally:
                test.close()
            if outcome!=reference:
                raise AssertionError(f'{QUERY} candidate {idx} ({spec}): reference != reduced result')
            cases.append(idx)
        return {'status':'PASS','cases':len(cases),'fixture_reference_rows':len(reference),
                'engine':'sqlite3 standard library','test_type':'synthetic_relational_multiset_semantics',
                'not_tested':['DuckDB SQL execution','Parquet materialization','HTTPS transfer',
                              'cloud latency and byte measurements']}
    finally: db.close()



def select_from_local_calibration(project, query, specs, deltas, calibration_dir=None, sf=1):
    """Provisional, evidence-only constrained selector over available local records.

    Uses one per-candidate local observation as a *calibration proxy*, not a
    statistically validated WAN latency predictor. Does not fabricate scores
    for the unprofiled candidates; does not read postselection evaluation data.
    """
    import math
    from collections import defaultdict
    directory=(Path(calibration_dir) if calibration_dir else
               project/'results'/'phase2'/'m4'/'calibration').resolve()
    if not directory.is_dir():
        raise FileNotFoundError(f'Calibration directory missing: {directory}')
    observations=defaultdict(list)
    inputs=[]
    for file in sorted(directory.glob('*.json')):
        try:
            record=json.loads(file.read_text(encoding='utf8'))
        except (ValueError, OSError) as exc:
            raise ValueError(f'Invalid calibration JSON {file}: {exc}') from exc
        if record.get('query') != query:
            continue
        local=record.get('local_duckdb') or {}
        if local.get('mode')!='LOCAL_DUCKDB_SIMULATION' or local.get('correct_vs_reference') is not True or local.get('source_guard')!='PASS':
            raise ValueError(f'Invalid local-correctness evidence: {file}')
        if record.get('source_sql_sha256') != LOCKED_SQL_SHA256 or record.get('phase1_runner_sha256') != ORIGINAL_RUNNER_SHA256:
            raise ValueError(f'Frozen source hash mismatch: {file}')
        idx=local['candidate_index']
        if not isinstance(idx,int) or not 0 <= idx < len(specs):
            raise ValueError(f'Invalid candidate index {idx}: {file}')
        copies=[s for s in local.get('steps',[]) if s.get('operation')=='local_file_copy_PROXY_NOT_WAN']
        counted=sum(int(s['artifact_bytes']) for s in copies)
        bytes_measured=int(local['simulated_intermediate_copy_bytes'])
        if counted!=bytes_measured:
            raise ValueError(f'Transfer byte reconciliation failed: {file}')
        latency_ms=1000*sum(float(local[f]) for f in ('source_stage_seconds','local_file_copy_seconds','destination_query_seconds'))
        if not (math.isfinite(latency_ms) and latency_ms > 0 and bytes_measured >= 0):
            raise ValueError(f'Invalid sample: {file}')
        observations[idx].append((bytes_measured,latency_ms))
        inputs.append(str(file))
    if len(observations)<2:
        raise ValueError(f'Need at least two calibrated candidate indices for {query}, found {len(observations)}')
    # In the frozen design a demonstrated candidate requires experimental data.
    # Unprofiled specs remain UNRANKED, rather than receiving fabricated costs.
    estimated=[]
    for idx,values in sorted(observations.items()):
        b=sorted(v[0] for v in values)
        t=sorted(v[1] for v in values)
        estimated.append({'candidate_index':idx,'final_site':specs[idx]['final_site'],
                          'filter_indexes':specs[idx]['filter_indexes'],
                          'n_calibration_samples':len(values),
                          'estimated_local_copy_bytes':int((b[(len(b)-1)//2]+b[len(b)//2])/2),
                          'estimated_local_workflow_ms':(t[(len(t)-1)//2]+t[len(t)//2])/2})
    fastest=min(p['estimated_local_workflow_ms'] for p in estimated)
    decisions=[]
    for delta in deltas:
        deadline=(1+delta)*fastest
        feasible=[p for p in estimated if p['estimated_local_workflow_ms']<=deadline+1e-9]
        best=min(feasible,key=lambda p:(p['estimated_local_copy_bytes'],p['estimated_local_workflow_ms'],p['candidate_index']))
        decisions.append({'delta':delta,'modeled_local_deadline_ms':deadline,
                          'feasible_candidate_indices':[p['candidate_index'] for p in feasible],
                          'selected_candidate_index':best['candidate_index'],
                          'selected_estimated_local_copy_bytes':best['estimated_local_copy_bytes'],
                          'selected_estimated_local_workflow_ms':best['estimated_local_workflow_ms']})
    return {'mode':'LOCAL_CALIBRATION_ONLY','query':query,'sf_requested':sf,
            'selection_scope':'ONLY_PROFILED_CANDIDATES','profiled_candidate_count':len(estimated),
            'unprofiled_candidate_count':len(specs)-len(estimated),
            'candidate_estimates':estimated,'decisions':decisions,
            'input_calibration_files':inputs,
            'findings_limitations':[
              'One observed local run per candidate is not a validated predictive model.',
              'The fastest plan and deadlines refer only to profiled candidates.',
              'Bytes represent local Parquet file copies, not HTTPS WAN measurements.',
              'No optimizer-selected plan has been independently executed after selection.',
              'Cross-cloud execution, tail latency, SF10/SF100 and billed cost are untested.',
              'A broader 22/237 candidate optimum cannot be claimed.']}


def repeated_local_evaluation(project, sql, edges, candidates, sf, max_filters,
                              data_dir=None, warmups=2, repeats=10, seed=17):
    """Independent, balanced repeated local Parquet-copy simulation.

    Each candidate gets `warmups` initial executions (unmeasured), then
    a balanced interleaved sequence of `repeats` measured executions.
    Never loads/retrains the M4 calibration files or runs cloud workers.
    """
    import datetime
    import random
    import statistics
    import time
    import uuid
    allowed={0, 4} if QUERY=='Q03' else {0, 6}
    if set(candidates) != allowed or len(candidates)!=2:
        raise ValueError('Evaluation candidate set is locked to Q03 {0,4} or Q05 {0,6}')
    if warmups<0 or repeats<2 or repeats>100:
        raise ValueError('Require warmups>=0 and 2<=repeats<=100')
    flt,specs=candidate_specs(edges,max_filters)
    if max(candidates)>=len(specs):
        raise ValueError('Candidate index unavailable under current max-filters')
    run_id=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8]
    output_dir=project/'results'/'phase2'/'m5'/'local_repeated'/QUERY.lower()/run_id
    if output_dir.exists():raise FileExistsError(output_dir)
    output_dir.mkdir(parents=True,exist_ok=False)
    protocol={'query':QUERY,'sf':sf,'run_id':run_id,'candidates':sorted(candidates),
              'warmups_per_candidate':warmups,'measured_repeats_per_candidate':repeats,
              'seed':seed,'execution_mode':'LOCAL_DUCKDB_SIMULATION',
              'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'scope':'Separate evaluation; M4 single-run calibration untouched',
              'note':'Local file-copy bytes are not WAN or billed egress.'}
    (output_dir/'protocol.json').write_text(json.dumps(protocol,indent=2)+'\n')
    def run_one(candidate,phase,index):
        result=local_duckdb_execution(project,sql,edges,candidate,sf,max_filters,data_dir=data_dir)
        runtime_ms=1000*sum(float(result[k]) for k in ('source_stage_seconds','local_file_copy_seconds','destination_query_seconds'))
        return {'query':QUERY,'sf':sf,'candidate_index':candidate,'phase':phase,
                'trial':index,'correct_vs_reference':result['correct_vs_reference'],
                'simulated_intermediate_copy_bytes':result['simulated_intermediate_copy_bytes'],
                'local_workflow_ms':runtime_ms,'source_stage_ms':1000*result['source_stage_seconds'],
                'copy_ms':1000*result['local_file_copy_seconds'],
                'destination_query_ms':1000*result['destination_query_seconds'],
                'steps':result['steps']}
    rows=[]
    # Balanced order for warmups, then alternate the pair's first candidate.
    for i in range(warmups):
        for candidate in (sorted(candidates) if i%2==0 else sorted(candidates,reverse=True)):
            r=run_one(candidate,'warmup',i+1)
            rows.append(r)
            if not r['correct_vs_reference']:raise AssertionError('Incorrect SQL result')
    rng=random.Random(seed)
    candidates_sorted=sorted(candidates)
    for i in range(repeats):
        order=candidates_sorted.copy()
        if i%2:order.reverse()
        for candidate in order:
            r=run_one(candidate,'measured',i+1)
            rows.append(r)
            if not r['correct_vs_reference']:raise AssertionError('Incorrect SQL result')
    measured=[r for r in rows if r['phase']=='measured']
    summary=[]
    for candidate in candidates_sorted:
        values=[r for r in measured if r['candidate_index']==candidate]
        runtimes=[r['local_workflow_ms'] for r in values]
        payloads=[r['simulated_intermediate_copy_bytes'] for r in values]
        summary.append({'candidate_index':candidate,'n':len(values),
          'correct_trials':sum(bool(r['correct_vs_reference']) for r in values),
          'mean_local_workflow_ms':statistics.mean(runtimes),
          'sd_local_workflow_ms':statistics.stdev(runtimes),
          'median_local_workflow_ms':statistics.median(runtimes),
          'min_local_workflow_ms':min(runtimes),'max_local_workflow_ms':max(runtimes),
          'mean_local_copy_bytes':statistics.mean(payloads),
          'sd_local_copy_bytes':statistics.stdev(payloads)})
    import csv
    columns=['query','sf','candidate_index','phase','trial','correct_vs_reference',
             'simulated_intermediate_copy_bytes','local_workflow_ms','source_stage_ms',
             'copy_ms','destination_query_ms']
    with (output_dir/'trial_metrics.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=columns);w.writeheader()
        w.writerows({k:r[k] for k in columns} for r in rows)
    (output_dir/'summary.json').write_text(json.dumps({'protocol':protocol,'summary':summary,
       'limitations':['Local single-host, not true multi-cloud execution.',
        'No new plan generation or broad-candidate ranking established.',
        'Randomized deployment/session effects not measured.',
        'M4 calibration remains fixed and separate.']},indent=2)+'\n')
    protocol['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    return {'mode':'M5_LOCAL_REPEATED_EVALUATION','query':QUERY,'run_id':run_id,
        'output_dir':str(output_dir),'warmup_executions':2*warmups,
        'measured_executions':len(measured),'summary':summary,
        'correct_measured_trials':sum(bool(r['correct_vs_reference']) for r in measured),
        'experimental_limit':'LOCAL_FILE_COPY_PROXY_NOT_WAN'}


def cloud_dry_run(project, sql, edges, candidate, sf, max_filters, run_id):
    """Static cloud-execution readiness audit. Does NOT invoke the network.

    Distinguishes a transport-independent DAG from actual HTTPS worker support.
    """
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters)
    steps=plan['steps']
    known=set()
    transfers=[]
    errors=[]
    for stage in steps:
        for parent in stage.get('depends_on',[]):
            if parent not in known:
                errors.append(f"{stage['id']}: missing/not-yet-produced dependency {parent}")
        if stage['id'] in known:
            errors.append(f"duplicate stage {stage['id']}")
        known.add(stage['id'])
        if stage['type']=='transfer_file':
            if stage['from']==stage['to']:
                errors.append(f"{stage['id']}: pointless same-provider transfer")
            transfers.append({'step':stage['id'],'from':stage['from'],'to':stage['to'],
                              'source':stage['source'],'destination':stage['destination'],
                              'transport':'REQUIRES_AUTHENTICATED_HTTPS_WORKER'})
    endpoints={site:{'configured':False,'status':'MISSING_CONFIG'} for site in sorted(set(PLACEMENT.values()))}
    blockers=['No authenticated HTTPS transport adapter for generated Parquet stage DAG',
              'No worker endpoint/authentication configuration verified',
              'No remote staging and destination-result integrity verification',
              'No VM lifecycle/egress cost cap/cleanup verification',
              'No prospective per-stage metrics or independent cloud execution']
    return {'mode':'M6A_STATIC_CLOUD_DRY_RUN','query':QUERY,'candidate_index':candidate,
            'scale_factor':sf,'source_guard':'PASS','dag_valid':not errors,
            'stage_count':len(steps),'transfer_count':len(transfers),
            'transfers':transfers,'worker_endpoints':endpoints,'errors':errors,
            'cloud_ready':False,'cloud_executed':False,'provisioned_resources':0,
            'blocked_by':blockers,'experimental_scope':'M6a static readiness only; no WAN observations',
            'original_query_sql_sha256':LOCKED_SQL_SHA256}




def cloud_worker_compatibility(project,sql,edges,candidate,sf,max_filters,run_id):
    """Fail-closed Phase-1 worker compatibility audit (no cloud credentials/network).

    A fixed-plan worker must not be reused for a generated-plan DAG unless it
    accepts arbitrary generated SQL/Parquet stages and their transfer paths.
    """
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters)
    phase1=(project/'experiments'/('q'+QUERY[1:]+'.py')).read_text(encoding='utf-8')
    remote_ops={'duckdb_copy','duckdb_query','transfer_file'}
    operators=sorted(set(stage['type'] for stage in plan['steps']))
    required=['authenticated stage submission','provider-local SQL stage execution',
              'verified provider-to-provider Parquet exchange','remote result verification',
              'isolated trial IDs','resource cleanup and transfer accounting']
    fixed_parts= ('PORTIONS = ' in phase1 or "PLANS = " in phase1)
    has_general_stage=('/phase2/execute-stage' in phase1 and '/phase2/transfer-file' in phase1)
    errors=[]
    if not all(op in remote_ops for op in operators):
        errors.append('Unrecognized generated stage operator')
    if not has_general_stage:
        errors.append('Phase 1 fixed-plan HTTPS worker has no generic Phase 2 generated-stage API')
    # Never claim readiness merely because the Phase 1 HTTPS worker is deployed.
    return {'mode':'M6B_WORKER_COMPATIBILITY_PREFLIGHT','query':QUERY,
            'candidate_index':candidate,'scale_factor':sf,
            'generated_stage_count':len(plan['steps']),
            'provider_transfer_count':sum(st['type']=='transfer_file' for st in plan['steps']),
            'generated_operators':operators,'phase1_fixed_stage_interface':fixed_parts,
            'phase1_generic_phase2_stage_interface':has_general_stage,
            'required_capabilities':required,'errors':errors,
            'compatible':not errors,'cloud_execution_permitted':False,
            'cloud_executed':False,'provisioned_resources':0,
            'interpretation':'Read-only code compatibility check; not a cloud or network experiment.'}

# M6c: authenticated generic remote-stage transport, entirely independent
# of Phase 1 workers. Worker is provisioned externally; no VM lifecycle here.
import base64
import hmac
import os
import ssl
import tempfile
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _transport_cfg(path):
    cfg=json.loads(Path(path).read_text())
    sites=cfg.get('sites',{})
    if not set(PLACEMENT.values()).issubset(sites) or not set(sites).issubset({'aws','azure','gcp'}):
        raise ValueError('Config must include every provider used by this query')
    for site,loc in sites.items():
        if not isinstance(loc,str) or not loc.startswith(('https://','http://127.0.0.1:','http://localhost:')):
            raise ValueError('Non-loopback endpoints MUST be HTTPS: '+site)
        if '/' in loc.split('://',1)[1].strip('/'):
            raise ValueError('Endpoint must contain scheme+host+port only')
    return cfg


def _transport_token():
    secret=os.environ.get('EGRESSOPT_PHASE2_TOKEN','')
    if len(secret)<32:
        raise RuntimeError('Set EGRESSOPT_PHASE2_TOKEN to at least 32 unpredictable characters')
    return secret


def _remote_request(url, method='GET', obj=None, max_bytes=100_000_000, timeout=120):
    from urllib.parse import urlsplit
    host=urlsplit(url).hostname
    if urlsplit(url).scheme!='https' and host not in ('127.0.0.1','localhost'):
        raise ValueError('Only HTTPS remote hosts permitted')
    body=json.dumps(obj).encode('utf8') if obj is not None else None
    headers={'Authorization':'Bearer '+_transport_token()}
    if body is not None: headers['Content-Type']='application/json'
    req=urllib.request.Request(url,data=body,headers=headers,method=method)
    # Authenticated requests must never follow redirects: a redirect could
    # disclose the bearer token to a different host. TLS verification remains
    # enabled through Python's default HTTPS handler and trust store.
    class _ForbidRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, request, fp, code, msg, headers, newurl):
            raise RuntimeError('Redirects prohibited for authenticated worker requests')
    with urllib.request.build_opener(_ForbidRedirect()).open(req,timeout=timeout) as r:
        response=r.read(max_bytes+1)
        if len(response)>max_bytes: raise RuntimeError('Remote response exceeds size cap')
        return response


def phase2_worker_serve(cfg_path, site, bind='127.0.0.1', port=0):
    cfg=_transport_cfg(cfg_path)
    if site not in cfg['sites']: raise ValueError('Unknown site')
    token=_transport_token()
    import duckdb
    root=Path(cfg.get('runtime_root','/opt/egressopt/runtime')).resolve()
    data=Path(cfg.get('data_root','/opt/egressopt/data')).resolve()
    root.mkdir(parents=True,exist_ok=True)
    lock=__import__('threading').Lock()
    max_payload=int(cfg.get('max_artifact_bytes',2_000_000_000))
    if max_payload<=0 or max_payload>10_000_000_000: raise ValueError('Unsafe artifact size cap')
    def within(path,base):
        path=Path(path).resolve()
        if not path.is_relative_to(base):raise PermissionError('Path outside configured root')
        return path
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def fail(self,status,message):
            self.send_response(status);self.end_headers();self.wfile.write(message.encode())
        def send_blob(self,blob,ctype='application/octet-stream'):
            self.send_response(200);self.send_header('Content-Type',ctype)
            self.send_header('Content-Length',str(len(blob)));self.end_headers();self.wfile.write(blob)
        def handle_request(self):
            if not hmac.compare_digest(self.headers.get('Authorization',''), 'Bearer '+token):
                return self.fail(401,'unauthorized')
            if self.path=='/health' and self.command=='GET':
                return self.send_blob(json.dumps({'site':site,'api':'M6c'}).encode(),'application/json')
            if self.command!='POST' or self.path not in ('/execute','/artifact','/fetch'):
                return self.fail(404,'not found')
            n=int(self.headers.get('Content-Length','0'))
            if n<=0 or n>2_000_000:return self.fail(413,'Invalid JSON request size')
            payload=json.loads(self.rfile.read(n))
            if self.path=='/execute':
                if payload.get('site')!=site:return self.fail(403,'stage site mismatch')
                # Runtime and source access confined to configured roots by fixed plan paths.
                sql=payload['sql'];kind=payload['type']
                if kind not in ('duckdb_copy','duckdb_query'):return self.fail(400,'Unsupported stage')
                if ';' in sql.strip().rstrip(';'):return self.fail(400,'Multistatement SQL blocked')
                # Prevent arbitrary SQL submitted via exposed interface; require a
                # SHA256 stage manifest signed by a separately configured HMAC key.
                expected=hmac.new(token.encode(),(site+'\n'+kind+'\n'+sql).encode(), 'sha256').hexdigest()
                if not hmac.compare_digest(payload.get('signature',''),expected):return self.fail(403,'Bad stage signature')
                import re
                # All file paths in the compiled SQL must refer to configured data or runtime roots.
                paths=re.findall(r"read_parquet\('([^']+)'\)|TO\s+'([^']+)'",sql,re.I)
                for a,b in paths: within(a or b,root if (a or b).startswith(str(root)) else data)
                with lock:
                    if kind=='duckdb_copy':
                        output=within(payload['output'],root);output.parent.mkdir(parents=True,exist_ok=True)
                    tic=time.perf_counter();con=duckdb.connect(':memory:')
                    try:
                        cursor=con.execute(sql)
                        result=cursor.fetchall() if kind=='duckdb_query' else None
                    finally:con.close()
                response={'site':site,'stage':payload['stage_id'],'elapsed_seconds':time.perf_counter()-tic,
                          'result_rows':result if result is not None else None,
                          'artifact_bytes':output.stat().st_size if kind=='duckdb_copy' else 0}
                blob=json.dumps(response,default=str).encode()
                if len(blob)>8_000_000:return self.fail(413,'Result too large')
                return self.send_blob(blob,'application/json')
            if self.path=='/artifact':
                path=within(payload['path'],root)
                if not path.is_file():return self.fail(404,'missing artifact')
                if path.stat().st_size>max_payload:return self.fail(413,'Artifact too large')
                return self.send_blob(path.read_bytes())
            if self.path=='/fetch':
                remote_site=payload['from']
                if remote_site==site or remote_site not in cfg['sites']:return self.fail(400,'Invalid source site')
                dst=within(payload['destination'],root)
                src=payload['source']
                # Source path is verified by peer worker before read.
                import hashlib
                blob=_remote_request(cfg['sites'][remote_site]+'/artifact','POST',{'path':src},max_payload)
                digest=hashlib.sha256(blob).hexdigest()
                dst.parent.mkdir(parents=True,exist_ok=True)
                with tempfile.NamedTemporaryFile(dir=dst.parent,delete=False) as f:
                    f.write(blob); temporary=Path(f.name)
                temporary.replace(dst)
                return self.send_blob(json.dumps({'bytes':len(blob),'sha256':digest,'from':remote_site,
                       'to':site}).encode(),'application/json')
        def do_GET(self):
            try:self.handle_request()
            except Exception as e:self.fail(500,type(e).__name__+': '+str(e))
        def do_POST(self):
            try:self.handle_request()
            except Exception as e:self.fail(500,type(e).__name__+': '+str(e))
    server=ThreadingHTTPServer((bind,port),Handler)
    if bind not in ('127.0.0.1','localhost'):
        cert=os.environ.get('EGRESSOPT_PHASE2_TLS_CERT');key=os.environ.get('EGRESSOPT_PHASE2_TLS_KEY')
        if not cert or not key:raise RuntimeError('Public binding requires TLS certificate and key')
        ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(cert,key)
        server.socket=ctx.wrap_socket(server.socket,server_side=True)
    print(json.dumps({'worker_site':site,'bind':bind,'port':server.server_port,'cloud_executed':False}),flush=True)
    server.serve_forever()


def cloud_stage_manifest(project,sql,edges,candidate,sf,max_filters,run_id):
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters)
    output=[]
    for stage in plan['steps']:
        base={k:stage[k] for k in ('id','type','depends_on')}
        if stage['type']=='transfer_file':
            base.update({'from':stage['from'],'to':stage['to'],
                         'source':stage['source'],'destination':stage['destination']})
        else:
            base['site']=stage['site'];base['sql_sha256']=hashlib.sha256(stage['sql'].encode()).hexdigest()
        output.append(base)
    return {'mode':'M6C_AUTHENTICATED_STAGE_MANIFEST','query':QUERY,'candidate_index':candidate,
            'scale_factor':sf,'stages':output,'cloud_executed':False,
            'blockers':['Worker TLS certificates, endpoints and remote data installation must be verified.',
                        'No provider resources are automatically provisioned or terminated.',
                        'This manifest does not independently verify output correctness.']}


def remote_execute_plan(project,sql,edges,candidate,sf,max_filters,run_id,config_path):
    """Execute a *preprovisioned* generic stage DAG via authenticated workers.

    Does not launch/stop VMs. Explicit opt-in required in CLI. The operation
    is not certified until independent reference correctness is verified.
    """
    cfg=_transport_cfg(config_path)
    token=_transport_token()
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters,
                       data_root_override=cfg.get('data_root',f'/opt/egressopt/data/tpch_sf{sf}'),
                       runtime_root_override=cfg.get('runtime_root',f'/opt/egressopt/runtime/phase2_{QUERY.lower()}_{run_id}'))
    traces=[]; finished=set(); total_bytes=0
    for st in plan['steps']:
        if not set(st.get('depends_on',[])).issubset(finished):
            raise RuntimeError('Unsatisfied plan dependency '+st['id'])
        started=time.perf_counter()
        if st['type']=='transfer_file':
            req={'from':st['from'],'source':st['source'],'destination':st['destination']}
            reply=json.loads(_remote_request(cfg['sites'][st['to']]+'/fetch','POST',req).decode())
            if reply['from']!=st['from'] or reply['to']!=st['to'] or len(reply['sha256'])!=64:
                raise RuntimeError('Inconsistent receipt for '+st['id'])
            total_bytes+=int(reply['bytes'])
        else:
            sig=hmac.new(token.encode(),(st['site']+'\n'+st['type']+'\n'+st['sql']).encode(), 'sha256').hexdigest()
            req={'site':st['site'],'type':st['type'],'sql':st['sql'],'stage_id':st['id'],
                 'signature':sig,'output':st.get('output')}
            reply=json.loads(_remote_request(cfg['sites'][st['site']]+'/execute','POST',req).decode())
        traces.append({'stage':st['id'],'type':st['type'],'elapsed_controller_seconds':time.perf_counter()-started,
                       'response':reply if st['type']!='duckdb_query' else {'row_count':len(reply['result_rows']),
                       'result_sha256':hashlib.sha256(json.dumps(reply['result_rows'],sort_keys=True).encode()).hexdigest()}})
        finished.add(st['id'])
    return {'mode':'REMOTE_WORKER_EXECUTION','query':QUERY,'candidate_index':candidate,
            'sf':sf,'stages_completed':len(finished),'payload_bytes':total_bytes,
            'note':'Incoming remote-body bytes, including key exchanges. No provider billing measured.',
            'sql_correctness':'NOT_VERIFIED_AGAINST_INDEPENDENT_REFERENCE',
            'provider_resource_cleanup':'NOT_PERFORMED_BY_THIS_MODE',
            'cloud_executed':True,'traces':traces}

# M6d: mutually authenticated (shared ephemeral bearer key), bounded
# push-mode artifact transfer and real-DuckDB localhost integration test.
# Separate from Phase 1. No provisioning, provider charges or fake cloud claims.
import http.client
import secrets
import socket
import subprocess
import statistics
import datetime as _datetime


def _m6d_allowlist(cfg, stage):
    """Reject SQL stages not frozen in the candidate-specific allowlist."""
    if stage['type'] not in ('duckdb_copy', 'duckdb_query'):
        return False
    sig=hashlib.sha256(stage['sql'].encode('utf8')).hexdigest()
    return sig in cfg.get('allowed_stage_sha256',{}).get(stage['site'],[])


def _m6d_map_runtime(virtual_path, cfg, site):
    """An optional per-site local sandbox; cloud VMs naturally have separate FS."""
    root=Path(cfg['runtime_root']).resolve()
    path=Path(virtual_path).resolve()
    if not path.is_relative_to(root):
        raise PermissionError('Stage file lies outside configured runtime')
    namespace=cfg.get('site_namespace_root')
    if namespace:
        if not all(cfg['sites'][p].startswith('http://127.0.0.1:') for p in cfg['sites']):
            raise ValueError('Site namespaces are permitted only for localhost tests')
        sandbox=(Path(namespace).resolve()/site/'runtime').resolve()
        mapped=(sandbox/path.relative_to(root)).resolve()
        if not mapped.is_relative_to(sandbox):
            raise PermissionError('Sandbox escape')
        return mapped
    return path



def _m6d_translate_sql_paths(sql, cfg, site):
    """Map SQL file references into a per-site runtime sandbox.

    macOS may compile a path under /var/... while Path.resolve() returns
    /private/var/.... Comparing only the resolved runtime prefix with the
    lexical SQL string is incorrect. Map each parsed SQL path individually.
    This helper also rejects any SQL file path outside the approved roots.
    """
    if ';' in sql.strip().rstrip(';'):
        raise ValueError('Multiple SQL statements are prohibited')
    if not (sql.lstrip().upper().startswith('COPY (') or sql.lstrip().upper().startswith('WITH ')):
        raise ValueError('Unsupported SQL operator')

    data = Path(cfg['data_root']).resolve()
    runtime = Path(cfg['runtime_root']).resolve()
    namespace = cfg.get('site_namespace_root')
    # Match only supported DuckDB file read and COPY output slots. Do not
    # replace arbitrary SQL text or values which happen to contain a path.
    pattern = re.compile(r"read_parquet\('([^']+)'\)|\bTO\s+'([^']+)'", re.I)
    matches = [m for m in pattern.finditer(sql)]
    if not matches:
        raise ValueError('Stage has no explicit I/O paths')

    def rewrite(m):
        raw = m.group(1) if m.group(1) is not None else m.group(2)
        real_path = Path(raw).resolve()
        if real_path.is_relative_to(runtime):
            translated = str(_m6d_map_runtime(raw, cfg, site)) if namespace else raw
        elif real_path.is_relative_to(data):
            translated = raw
        else:
            raise PermissionError('SQL data access outside configured roots')
        return m.group(0).replace(raw, translated)

    mapped_sql = pattern.sub(rewrite, sql)
    # Fail closed if any other file-reading function was introduced into an
    # allowlisted stage SQL without explicit rewriting/validation support.
    unsupported = re.search(r"\b(?:read_csv|read_json|sqlite_scan|httpfs)\s*\(", mapped_sql, re.I)
    if unsupported:
        raise ValueError('Unsupported external input operation')
    return mapped_sql


def _m6d_signed_worker(cfg_path, site, bind='127.0.0.1', port=0):
    cfg=_transport_cfg(cfg_path)
    if site not in cfg['sites']: raise ValueError('Site not configured')
    token=_transport_token()
    import duckdb
    max_payload=int(cfg.get('max_artifact_bytes',2_000_000_000))
    if not 0<max_payload<=10_000_000_000:raise ValueError('Unreasonable payload cap')
    runtime=Path(cfg['runtime_root']).resolve()
    data=Path(cfg['data_root']).resolve()
    if not data.is_dir(): raise FileNotFoundError('Configured Parquet data directory does not exist')
    file_lock=__import__('threading').Lock()

    def approved_transfer(source_site,destination_site,source_path,destination_path):
        return any(x.get('from')==source_site and x.get('to')==destination_site
                   and x.get('source')==source_path and x.get('destination')==destination_path
                   for x in cfg.get('allowed_transfers',[]))

    def translate_sql(sql):
        return _m6d_translate_sql_paths(sql, cfg, site)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def reply(self,status,blob,ctype='application/json'):
            self.send_response(status)
            self.send_header('Content-Type',ctype)
            self.send_header('Content-Length',str(len(blob)))
            self.end_headers();self.wfile.write(blob)
        def error(self,code,msg):
            self.reply(code,json.dumps({'error':msg}).encode('utf8'))
        def auth(self):
            if not hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+token):
                self.error(401,'unauthorized');return False
            return True
        def request_json(self):
            try:n=int(self.headers.get('Content-Length','0'))
            except ValueError: raise ValueError('Missing content-length')
            if n<=0 or n>2_000_000:raise ValueError('JSON request outside size bounds')
            req=json.loads(self.rfile.read(n))
            if not isinstance(req,dict):raise ValueError('Expected JSON object')
            return req
        def do_GET(self):
            if not self.auth(): return
            if self.path=='/health':
                self.reply(200,json.dumps({'site':site,'api':'M6d', 'query':QUERY,
                    'duckdb':duckdb.__version__,
                    'config_sha256':_m6f_config_digest(cfg),
                    'source_sql_sha256':LOCKED_SQL_SHA256,
                    'required_local_parquet':{tbl:(data/(tbl+'.parquet')).is_file()
                      for tbl in COLUMNS[QUERY] if PLACEMENT[tbl]==site},
                    'worker_tls_enabled':bind not in ('127.0.0.1','localhost')}).encode())
            else:self.error(404,'unknown route')
        def do_POST(self):
            if not self.auth(): return
            try:
                if self.path=='/execute':
                    p=self.request_json()
                    if p.get('site')!=site:self.error(403,'site mismatch');return
                    sql=p['sql'];kind=p['type']
                    if kind not in ('duckdb_copy','duckdb_query'):
                        self.error(400,'Unsupported stage');return
                    if not _m6d_allowlist(cfg,p):
                        self.error(403,'Stage SQL not allowed by frozen plan');return
                    digest=hmac.new(token.encode(),(site+'\n'+kind+'\n'+sql).encode(),'sha256').hexdigest()
                    if not hmac.compare_digest(digest,p.get('signature','')):
                        self.error(403,'Incorrect stage signature');return
                    if kind=='duckdb_copy':
                        match=re.search(r"\bTO\s+'([^']+)'",sql,re.I)
                        if not match or Path(match.group(1)).resolve()!=Path(p['output']).resolve():
                            self.error(403,'Stage output differs from signed COPY stage');return
                    translated=translate_sql(sql)
                    with file_lock:
                        output=None
                        if kind=='duckdb_copy':
                            output=_m6d_map_runtime(p['output'],cfg,site)
                            output.parent.mkdir(parents=True,exist_ok=True)
                        t=time.perf_counter()
                        con=duckdb.connect(':memory:')
                        try:
                            rows=con.execute(translated).fetchall() if kind=='duckdb_query' else (con.execute(translated) and None)
                        finally:con.close()
                    result={'site':site,'stage':p['stage_id'],'elapsed_seconds':time.perf_counter()-t,
                            'result_rows':rows,'artifact_bytes':output.stat().st_size if output else 0}
                    blob=json.dumps(result,default=str).encode()
                    if len(blob)>8_000_000:self.error(413,'Result set too large');return
                    self.reply(200,blob)
                elif self.path=='/push':
                    p=self.request_json()
                    destination=p.get('to')
                    if destination not in cfg['sites'] or destination==site:
                        self.error(400,'Invalid destination');return
                    if not approved_transfer(site,destination,p['source'],p['destination']):
                        self.error(403,'Transfer is not in approved plan');return
                    src=_m6d_map_runtime(p['source'],cfg,site)
                    if not src.is_file():self.error(404,'Missing stage artifact');return
                    sz=src.stat().st_size
                    if sz>max_payload:self.error(413,'Artifact exceeds cap');return
                    digest=hashlib.sha256()
                    with src.open('rb') as f:
                        for chunk in iter(lambda:f.read(1024*1024),b''):
                            digest.update(chunk)
                    import urllib.parse
                    u=urllib.parse.urlsplit(cfg['sites'][destination])
                    # M6H: trust only the temporary Phase-2 peer certificates.
                    # Local loopback tests have no peer CA bundle and use HTTP.
                    peer_ca=os.environ.get('EGRESSOPT_PHASE2_CA_BUNDLE')
                    if u.scheme=='https' and not peer_ca:
                        raise RuntimeError('Remote HTTPS transfer requires Phase 2 pinned peer CA bundle')
                    peer_ctx=ssl.create_default_context(cafile=peer_ca) if u.scheme=='https' else None
                    conn=(http.client.HTTPSConnection(u.hostname,u.port,timeout=240,context=peer_ctx)
                          if u.scheme=='https' else http.client.HTTPConnection(u.hostname,u.port,timeout=240))
                    transfer_started=time.perf_counter()
                    try:
                        conn.putrequest('POST','/ingest')
                        conn.putheader('Authorization','Bearer '+token)
                        conn.putheader('Content-Type','application/octet-stream')
                        conn.putheader('Content-Length',str(sz))
                        conn.putheader('X-Destination-Path',p['destination'])
                        conn.putheader('X-Source-Path',p['source'])
                        conn.putheader('X-From-Site',site)
                        conn.putheader('X-Sha256',digest.hexdigest())
                        conn.endheaders()
                        with src.open('rb') as f:
                            for chunk in iter(lambda:f.read(1024*1024),b''):
                                conn.send(chunk)
                        resp=conn.getresponse();response=resp.read(50_000)
                        if resp.status!=200:
                            raise RuntimeError(f'Ingest rejected: HTTP {resp.status}: {response[:1500].decode("utf-8",errors="replace")}')
                        receipt=json.loads(response)
                    finally: conn.close()
                    if (int(receipt.get('bytes',-1))!=sz
                        or not hmac.compare_digest(receipt.get('sha256',''),digest.hexdigest())
                        or receipt.get('from')!=site or receipt.get('to')!=destination):
                        raise RuntimeError('Transfer receipt mismatch')
                    receipt['transfer_seconds']=time.perf_counter()-transfer_started
                    self.reply(200,json.dumps(receipt).encode())
                elif self.path=='/ingest':
                    if self.headers.get('Content-Type')!='application/octet-stream':
                        self.error(415,'Must be binary');return
                    source=self.headers.get('X-From-Site')
                    if source not in cfg['sites'] or source==site:
                        self.error(400,'Invalid source site');return
                    size=int(self.headers.get('Content-Length','0'))
                    if not 0<=size<=max_payload:self.error(413,'Artifact outside size cap');return
                    expect=self.headers.get('X-Sha256','')
                    if not re.fullmatch(r'[a-f0-9]{64}',expect):
                        self.error(400,'Missing SHA256');return
                    source_path=self.headers.get('X-Source-Path','')
                    dest_path=self.headers.get('X-Destination-Path','')
                    if not approved_transfer(source,site,source_path,dest_path):
                        self.error(403,'Incoming artifact is not in approved plan');return
                    target=_m6d_map_runtime(dest_path,cfg,site)
                    target.parent.mkdir(parents=True,exist_ok=True)
                    digest=hashlib.sha256();received=0
                    with tempfile.NamedTemporaryFile(dir=target.parent,delete=False) as dst:
                        temp=Path(dst.name)
                        try:
                            while received<size:
                                piece=self.rfile.read(min(size-received,1024*1024))
                                if not piece:raise IOError('Incomplete artifact')
                                dst.write(piece);digest.update(piece);received+=len(piece)
                        except BaseException:
                            temp.unlink(missing_ok=True)
                            raise
                    if not hmac.compare_digest(digest.hexdigest(),expect):
                        temp.unlink(missing_ok=True)
                        self.error(422,'Artifact hash mismatch');return
                    with file_lock:temp.replace(target)
                    receipt={'bytes':received,'sha256':digest.hexdigest(),'from':source,'to':site,
                             'payload_metric':'CROSS_PROVIDER_HTTP_REQUEST_BODY_BYTES'}
                    self.reply(200,json.dumps(receipt).encode())
                else:self.error(404,'Unknown endpoint')
            except (PermissionError,ValueError,KeyError) as e:
                self.error(400,type(e).__name__+': '+str(e))
            except Exception as e:
                self.error(500,type(e).__name__+': '+str(e))

    server=ThreadingHTTPServer((bind,port),Handler)
    if bind not in ('127.0.0.1','localhost'):
        cert=os.environ.get('EGRESSOPT_PHASE2_TLS_CERT');key=os.environ.get('EGRESSOPT_PHASE2_TLS_KEY')
        if not cert or not key: raise RuntimeError('Non-loopback binding requires TLS key and certificate')
        ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(cert,key)
        server.socket=ctx.wrap_socket(server.socket,server_side=True)
    print(json.dumps({'worker_site':site,'bind':bind,'port':server.server_port,
                      'api':'M6d_signed_push','cloud_executed':False}),flush=True)
    try:server.serve_forever()
    finally:server.server_close()


def _m6d_remote_execute(project,sql,edges,candidate,sf,max_filters,run_id,config_path):
    """Run a whitelisted DAG using HTTPS POST request-body artifact pushes.

    The caller is responsible for validating reference SQL outside this method.
    """
    cfg=_transport_cfg(config_path)
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters,
                       data_root_override=cfg['data_root'],
                       runtime_root_override=str(Path(cfg['runtime_root'])/f'{QUERY.lower()}_{run_id}'))
    token=_transport_token();total=0;done=set();traces=[];finish_rows=None

    def request_for_stage(stage, url, method, payload, timeout=240):
        # Retain the worker's JSON diagnostic body rather than urllib's generic
        # 'HTTP Error 500', and identify the exact failing physical operator.
        try:
            return _remote_request(url,method,payload,timeout=timeout)
        except urllib.error.HTTPError as exc:
            body=exc.read(16384).decode('utf-8',errors='replace')
            try:
                detail=json.loads(body).get('error',body)
            except (ValueError,TypeError,AttributeError):
                detail=body
            detail=str(detail)[:4096]  # Bounded, never include bearer credentials.
            raise RuntimeError(
                f"{QUERY} candidate {candidate}: stage={stage['id']} "
                f"op={stage['type']} endpoint_site={stage.get('site', stage.get('from'))} "
                f"HTTP {exc.code}: {detail}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"{QUERY} candidate {candidate}: stage={stage['id']} "
                f"op={stage['type']} endpoint_site={stage.get('site', stage.get('from'))}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    t0=time.perf_counter()
    for stage in plan['steps']:
        if not set(stage['depends_on']).issubset(done):
            raise RuntimeError('Unmet DAG dependency: '+stage['id'])
        ti=time.perf_counter()
        if stage['type']=='transfer_file':
            receipt=json.loads(request_for_stage(stage,cfg['sites'][stage['from']]+'/push','POST',
                {'to':stage['to'],'source':stage['source'],'destination':stage['destination']},
                timeout=240).decode())
            if (receipt.get('payload_metric')!='CROSS_PROVIDER_HTTP_REQUEST_BODY_BYTES'
                or receipt.get('from')!=stage['from'] or receipt.get('to')!=stage['to']
                or not re.fullmatch(r'[a-f0-9]{64}',receipt.get('sha256',''))):
                raise RuntimeError('Invalid signed artifact receipt')
            total+=int(receipt['bytes'])
            evidence={'bytes':int(receipt['bytes']),'sha256':receipt['sha256'],
                      'from':stage['from'],'to':stage['to']}
        else:
            sig=hmac.new(token.encode(),(stage['site']+'\n'+stage['type']+'\n'+stage['sql']).encode(), 'sha256').hexdigest()
            req={'site':stage['site'],'type':stage['type'],'sql':stage['sql'],'stage_id':stage['id'],
                 'signature':sig,'output':stage.get('output')}
            response=json.loads(request_for_stage(stage,cfg['sites'][stage['site']]+'/execute','POST',req,timeout=240).decode())
            if response.get('site')!=stage['site'] or response.get('stage')!=stage['id']:
                raise RuntimeError('Stage receipt mismatch')
            if stage['type']=='duckdb_query':
                finish_rows=response['result_rows']
                evidence={'row_count':len(finish_rows), 'result_sha256':hashlib.sha256(json.dumps(finish_rows,sort_keys=True).encode()).hexdigest()}
            else:evidence={'artifact_bytes':response['artifact_bytes']}
        done.add(stage['id'])
        traces.append({'stage':stage['id'],'type':stage['type'], 'elapsed_seconds':time.perf_counter()-ti,
                       'evidence':evidence})
    return {'query':QUERY,'candidate_index':candidate,'run_id':run_id,
            'total_workflow_seconds':time.perf_counter()-t0,
            'request_body_bytes':total,'result_rows':finish_rows,
            'stage_count':len(traces),'transfer_count':sum(s['type']=='transfer_file' for s in plan['steps']),
            'traces':traces, 'sql_correctness':'UNVERIFIED_UNTIL_INDEPENDENT_REFERENCE',
            'execution_location':'CONFIGURED_WORKER_ENDPOINTS_NOT_AUTOMATICALLY_VERIFIED'}


def _m6d_loopback_validate(project,sql,edges,sf,max_filters,data_dir):
    """Real DuckDB worker/HTTP test in *three isolated local site namespaces*.

    Validates both baseline and semijoin, independent reference and bytes.
    Never provisions VM, contacts public network, or implies cloud egress.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError('DuckDB required: activate conda egressopt and use python (not python3)') from exc
    from decimal import Decimal,ROUND_HALF_EVEN
    data_dir=(Path(data_dir) if data_dir else project/'data'/'tpch'/f'parquet_sf{sf}').resolve()
    missing=[str(data_dir/f'{t}.parquet') for t in COLUMNS[QUERY] if not (data_dir/f'{t}.parquet').is_file()]
    if missing:raise FileNotFoundError('Missing SF Parquet input: '+', '.join(missing))
    pair=[0,4] if QUERY=='Q03' else [0,6]
    def signature(rows):
        prec=Decimal('0.0001') if QUERY=='Q03' else Decimal('0.01')
        return sorted((str(row[0]),format(Decimal(str(row[1])).quantize(prec,rounding=ROUND_HALF_EVEN),'f'))
                      +tuple(str(v) for v in row[2:]) for row in rows)
    with duckdb.connect(':memory:') as con:
        ctes=', '.join(table+' AS (SELECT * FROM read_parquet('+qlit(data_dir/f'{table}.parquet')+'))'
                       for table in COLUMNS[QUERY])
        ref=con.execute('WITH '+ctes+'\n'+sql.strip().rstrip(';')).fetchall()
        ref_signature=signature(ref)
    with tempfile.TemporaryDirectory(prefix='egressopt_phase2_m6d_') as tmp:
        tmp=Path(tmp)
        sites=('aws','azure','gcp')
        ports=[]
        for site in sites:
            sock=socket.socket();sock.bind(('127.0.0.1',0));ports.append(sock.getsockname()[1]);sock.close()
        cfg={'sites':{s:f'http://127.0.0.1:{p}' for s,p in zip(sites,ports)},
             'data_root':str(data_dir),'runtime_root':str(tmp/'runtime'),
             'site_namespace_root':str(tmp/'providers'),
             'max_artifact_bytes':500_000_000,'allowed_stage_sha256':{s:[] for s in sites},
             'allowed_transfers':[]}
        for candidate in pair:
            runid=f'lb_{candidate}'
            compiled=physical_plan(project,sql,edges,candidate,sf=sf,run_id=runid,max_filters=max_filters,
                                   data_root_override=data_dir,
                                   runtime_root_override=str(Path(cfg['runtime_root'])/f'{QUERY.lower()}_{runid}'))
            for stage in compiled['steps']:
                if stage['type']=='transfer_file':
                    cfg['allowed_transfers'].append({k:stage[k] for k in ('from','to','source','destination')})
                if stage['type'] in ('duckdb_copy','duckdb_query'):
                    cfg['allowed_stage_sha256'][stage['site']].append(hashlib.sha256(stage['sql'].encode()).hexdigest())
        config_file=tmp/'worker_config.json';config_file.write_text(json.dumps(cfg))
        token=secrets.token_hex(32)
        env={**os.environ,'EGRESSOPT_PHASE2_TOKEN':token}
        old_token=os.environ.get('EGRESSOPT_PHASE2_TOKEN')
        workers=[]
        try:
            os.environ['EGRESSOPT_PHASE2_TOKEN']=token
            for site,port in zip(sites,ports):
                workers.append(subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--mode','worker-serve',
                    '--worker-site',site,'--worker-port',str(port),'--cloud-config',str(config_file)],
                    stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,env=env,text=True))
            for site,port in zip(sites,ports):
                url=f'http://127.0.0.1:{port}/health'
                for attempt in range(120):
                    if any(p.poll() is not None for p in workers):
                        raise RuntimeError('Worker terminated during startup')
                    try:
                        info=json.loads(_remote_request(url).decode())
                        if info['site']!=site:raise RuntimeError('Wrong health site')
                        break
                    except (urllib.error.URLError,ConnectionError,OSError):
                        if attempt==119:raise RuntimeError('Worker startup timeout')
                        time.sleep(.05)
            # Reject an unauthorized health probe and a forged SQL request.
            try:urllib.request.urlopen('http://127.0.0.1:'+str(ports[0])+'/health',timeout=3)
            except urllib.error.HTTPError as e:
                if e.code!=401:raise
            else:raise AssertionError('Unauthorized health request succeeded')
            test_stage={'site':'aws','stage_id':'fake','type':'duckdb_query','sql':'SELECT 1',
                        'signature':hmac.new(token.encode(),('aws\nduckdb_query\nSELECT 1').encode(),'sha256').hexdigest()}
            try:_remote_request(cfg['sites']['aws']+'/execute','POST',test_stage,timeout=3)
            except urllib.error.HTTPError as e:
                if e.code!=403:raise
            else:raise AssertionError('Unallowlisted SQL stage succeeded')
            observations=[]
            for candidate in pair:
                record=_m6d_remote_execute(project,sql,edges,candidate,sf,max_filters,
                                            f'lb_{candidate}',config_file)
                correct=signature(record.pop('result_rows'))==ref_signature
                record['correct_vs_reference']=correct
                record['reference_rows']=len(ref)
                if not correct: raise AssertionError(f'Remote SQL differs from frozen SQL reference: candidate {candidate}')
                # This reference is separately executed from the immutable SQL on
                # original input Parquet; the earlier inner-run label was misleading.
                record['sql_correctness']='VERIFIED_AGAINST_FROZEN_LOCAL_DUCKDB_REFERENCE'
                record['reference_source_sql_sha256']=LOCKED_SQL_SHA256
                record['reference_signature_sha256']=hashlib.sha256(
                    json.dumps(ref_signature,sort_keys=True).encode()).hexdigest()
                source_bytes=sum(s['evidence']['bytes'] for s in record['traces'] if s['type']=='transfer_file')
                if source_bytes != record['request_body_bytes']:
                    raise AssertionError('HTTP request-body byte reconciliation failed')
                record['transfer_metric']='LOOPBACK_HTTP_REQUEST_BODY_BYTES_NOT_WAN'
                observations.append(record)
            return {'mode':'M6D_REAL_DUCKDB_THREE_WORKER_LOOPBACK','query':QUERY,'sf':sf,
                    'candidate_results':observations,'all_correct':all(r['correct_vs_reference'] for r in observations),
                    'authentication_negative_tests':'PASS',
                    'provider_filesystems':'THREE_ISOLATED_LOCAL_RUNTIME_NAMESPACES_SHARED_SOURCE_DATA',
                    'independent_reference':'FROZEN_SQL_RUN_SEPARATELY_IN_DUCKDB_ON_ORIGINAL_SF_PARQUET',
                    'cloud_executed':False,'provider_billing_measured':False,
                    'limitation':'No provider WAN/TLS latency, IAM, billing or VM lifecycle verification.'}
        finally:
            for p in workers:
                p.terminate()
                try:p.communicate(timeout=5)
                except subprocess.TimeoutExpired:p.kill();p.communicate()
            if old_token is None:os.environ.pop('EGRESSOPT_PHASE2_TOKEN',None)
            else:os.environ['EGRESSOPT_PHASE2_TOKEN']=old_token

def _m6e_build_cloud_bundle(project,sql,edges,sf,max_filters,run_id,data_root,runtime_root,candidate_trial_ids=None):
    """Offline allowlisted baseline/semijoin deployment proposal.

    Does not generate credentials, start VMs or assert that any provider is ready.
    All worker endpoints are deliberately unreachable .invalid placeholders.
    """
    import ipaddress
    from urllib.parse import urlsplit
    if sf not in (1,10):
        raise ValueError('M6K cloud manifest supports SF1/SF10 only; SF100 remains conditional')
    valid_slug(run_id)
    if not Path(data_root).is_absolute() or not Path(runtime_root).is_absolute():
        raise ValueError('Cloud data and runtime paths must be absolute on all worker VMs')
    if Path(data_root).resolve()==Path(runtime_root).resolve():
        raise ValueError('Data and runtime roots must differ')
    sites=sorted(set(PLACEMENT.values()))
    plan_indices=[0,4] if QUERY=='Q03' else [0,6]
    bundle={'sites':{site:f'https://{site}.example.invalid:9443' for site in sites},
            'data_root':str(Path(data_root)), 'runtime_root':str(Path(runtime_root)),
            'max_artifact_bytes':500_000_000,
            'allowed_stage_sha256':{site:[] for site in sites},
            'allowed_transfers':[]}
    # M6J: a repeated trial must have its OWN runtime paths, signed SQL
    # hashes and transfer path records. Do not permit reusing prior artifacts.
    if candidate_trial_ids is not None:
        if set(candidate_trial_ids)!=set(plan_indices):
            raise ValueError('M6J requires IDs for every frozen candidate')
        all_ids=[trial for idx in plan_indices for trial in candidate_trial_ids[idx]]
        if not all_ids or len(all_ids)!=len(set(all_ids)) or len(all_ids)>42:
            raise ValueError('M6J empty, duplicate or excessive trial IDs')
        for trial in all_ids:valid_slug(trial)
    else:
        candidate_trial_ids={idx:[f'{run_id}_{idx}'] for idx in plan_indices}
    summaries=[]
    for index in plan_indices:
        for candidate_id in candidate_trial_ids[index]:
            compiled=physical_plan(project,sql,edges,index,sf=sf,run_id=candidate_id,max_filters=max_filters,
                     data_root_override=bundle['data_root'],
                     runtime_root_override=str(Path(bundle['runtime_root'])/f'{QUERY.lower()}_{candidate_id}'))
            for stage in compiled['steps']:
                if stage['type']=='transfer_file':
                    record={key:stage[key] for key in ('from','to','source','destination')}
                    if record not in bundle['allowed_transfers']:bundle['allowed_transfers'].append(record)
                elif stage['type'] in ('duckdb_copy','duckdb_query'):
                    digest=hashlib.sha256(stage['sql'].encode()).hexdigest()
                    if digest not in bundle['allowed_stage_sha256'][stage['site']]:
                        bundle['allowed_stage_sha256'][stage['site']].append(digest)
                else:raise ValueError('Unrecognized stage type: '+stage['type'])
            summaries.append({'candidate_index':index,'run_id':candidate_id,
                              'final_site':compiled['final_site'],
                              'stage_count':len(compiled['steps']),
                              'transfers':sum(st['type']=='transfer_file' for st in compiled['steps']),
                              'plan_source_guard':'PASS',
                              'stage_digest':'sha256-of-exact-frozen-stage-SQL'})
    # Validate the skeleton passes endpoint syntax and every plan operation is
    # represented; no network checks and no provider file-existence claims.
    for site,url in bundle['sites'].items():
        assert urlsplit(url).scheme=='https'
    _transport_cfg_from_dict = bundle
    assert all(bundle['allowed_stage_sha256'].values())
    return {'status':'CONFIG_TEMPLATE_ONLY_CLOUD_NOT_READY','query':QUERY,'sf':sf,
            'candidate_pair':summaries,'cloud_config_template':bundle,
            'source_sql_sha256':LOCKED_SQL_SHA256,
            'phase1_original_runner_sha256':ORIGINAL_RUNNER_SHA256,
            'must_complete_before_real_run':[
               'Replace every .invalid site URL with a provider HTTPS endpoint using a trusted TLS certificate.',
               'Make this unchanged config JSON available to both controller and appropriate workers.',
               'Distribute the Phase 2 worker code plus its frozen Phase 1 SQL/runner guard files to each VM.',
               'Stage ONLY the locally assigned Parquet relations under data_root on their original cloud providers.',
               'Set an unpredictable EGRESSOPT_PHASE2_TOKEN (>=32 characters) securely and consistently on all workers.',
               'Set EGRESSOPT_PHASE2_TLS_CERT and EGRESSOPT_PHASE2_TLS_KEY for workers bound off localhost.',
               'Verify endpoints reach the intended AWS/Azure/GCP VMs; .invalid placeholders are NOT usable.',
               'Independently verify SQL output against the frozen original after any cloud run.',
               'Establish a per-run VM/transfer cost budget and verified ingress-rule and VM cleanup before running.',
               'Do not use cloud-execute as a completed research experiment without repetitions and precommitment.'
            ],
            'cloud_executed':False,'no_credentials_embedded':True,'ready_for_cloud':False}


# M6f -- Fail-closed cloud readiness and endpoint worker identity checks.
# Uses only standard library. No provisioning, remote SQL, or cloud spending.
def _m6f_config_digest(cfg):
    """Hash a normalized provider config without including credentials."""
    return hashlib.sha256(json.dumps(cfg,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def _m6f_readiness(project, sql, edges, sf, max_filters, run_id, config_path, online=False):
    """Compare external configuration to independently compiled accepted plans.

    Online mode does HTTPS /health only: no SQL, no data transfer, no provisioning.
    A signed response claim alone does not prove physical cloud provider identity;
    independent provider-side VM inventory and cost/cleanup controls are required.
    """
    from urllib.parse import urlsplit
    cfg=_transport_cfg(config_path)
    valid_slug(run_id)
    blocks=[]
    expected_sites=set(PLACEMENT.values())
    sites=cfg.get('sites',{})
    if sf not in (1,10):
        blocks.append('Only SF1/SF10 manifests supported; SF100 remains pending.')
    if set(sites)!=expected_sites:
        blocks.append('Cloud sites must exactly match the query placement: '+str(sorted(expected_sites)))
    root_s=cfg.get('runtime_root','')
    data_s=cfg.get('data_root','')
    for key,path in [('runtime_root',root_s),('data_root',data_s)]:
        if not isinstance(path,str) or not path.startswith('/'):
            blocks.append(key+' must be an absolute POSIX directory')
    if isinstance(root_s,str) and isinstance(data_s,str) and root_s.startswith('/') and data_s.startswith('/'):
        runtime=Path(root_s); data=Path(data_s)
        if runtime==data or runtime in data.parents or data in runtime.parents:
            blocks.append('Runtime and data roots must not overlap')
    if cfg.get('site_namespace_root'):
        blocks.append('Local filesystem test namespaces must not be set in a cloud deployment')
    max_size=cfg.get('max_artifact_bytes')
    if not isinstance(max_size,int) or not 0<max_size<=500_000_000:
        blocks.append('max_artifact_bytes must be an integer between 1 and 500,000,000')
    for site,url in sorted(sites.items()):
        try:
            v=urlsplit(url)
            name=(v.hostname or '').lower().rstrip('.')
            if v.scheme!='https' or not name or not v.port or v.username or v.password or v.path or v.query or v.fragment:
                blocks.append(f'{site}: require HTTPS scheme, hostname and port, without URL path, userinfo or query')
            if (name.endswith('.invalid') or name.endswith('.example') or name in
                ('localhost','127.0.0.1','::1','example.com') or name.endswith('.example.com')):
                blocks.append(f'{site}: template/local endpoint is not a deployable cloud hostname')
        except (ValueError,TypeError,AttributeError):
            blocks.append(f'{site}: invalid endpoint URL')
    expected_stage={s:set() for s in expected_sites}
    expected_transfers=[]
    expected_data={s:set() for s in expected_sites}
    for candidate in ([0,4] if QUERY=='Q03' else [0,6]):
        child=f'{run_id}_{candidate}'
        plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=child,max_filters=max_filters,
                           data_root_override=data_s,
                           runtime_root_override=str(Path(root_s)/f'{QUERY.lower()}_{child}'))
        for stage in plan['steps']:
            if stage['type']=='transfer_file':
                expected_transfers.append({k:stage[k] for k in ('from','to','source','destination')})
            else:
                expected_stage[stage['site']].add(hashlib.sha256(stage['sql'].encode()).hexdigest())
                for raw in re.findall(r"read_parquet\('([^']+)'\)",stage['sql'],re.I):
                    if Path(raw).parent==Path(data_s):
                        expected_data[stage['site']].add(Path(raw).name)
    frozen=cfg.get('allowed_stage_sha256')
    if not isinstance(frozen,dict) or set(frozen)!=expected_sites or any(
        not isinstance(frozen.get(site),list) or
        len(frozen[site])!=len(set(frozen[site])) or
        set(frozen[site])!=expected_stage[site] for site in expected_sites):
        blocks.append('SQL-stage SHA-256 allowlist differs from recompiled frozen candidate pair')
    frozen_transfers=cfg.get('allowed_transfers')
    transfer_tuples=lambda items: [tuple(x.get(k) for k in ('from','to','source','destination')) for x in items]
    if not isinstance(frozen_transfers,list) or any(not isinstance(x,dict) for x in frozen_transfers):
        blocks.append('Transfer allowlist missing or malformed')
    elif sorted(transfer_tuples(frozen_transfers))!=sorted(transfer_tuples(expected_transfers)):
        blocks.append('Directed transfer allowlist differs from recompiled frozen candidate pair')
    if len(expected_transfers)!=(5 if QUERY=='Q03' else 11):
        blocks.append('Unexpected query-specific transfer count')
    remote_health={}
    offline_valid=len(blocks)==0
    if online:
        if not offline_valid:
            blocks.append('HTTPS worker health checks skipped until static configuration passes')
        elif not os.environ.get('EGRESSOPT_PHASE2_TOKEN') or len(os.environ['EGRESSOPT_PHASE2_TOKEN'])<32:
            blocks.append('EGRESSOPT_PHASE2_TOKEN must be set locally (not supplied in JSON)')
        else:
            for site,url in sorted(sites.items()):
                try:
                    info=json.loads(_remote_request(url+'/health',timeout=8).decode())
                    remote_health[site]={
                        'reported_site':info.get('site'),
                        'reported_api':info.get('api'),
                        'reported_query':info.get('query'),
                        'config_match':info.get('config_sha256')==_m6f_config_digest(cfg),
                        'source_sql_match':info.get('source_sql_sha256')==LOCKED_SQL_SHA256,
                        'worker_tls_enabled':info.get('worker_tls_enabled'),
                        'required_parquet_ready':info.get('required_local_parquet')}
                    if (info.get('site')!=site or info.get('api')!='M6d' or
                        info.get('query')!=QUERY or info.get('config_sha256')!=_m6f_config_digest(cfg) or
                        info.get('source_sql_sha256')!=LOCKED_SQL_SHA256 or
                        info.get('worker_tls_enabled') is not True):
                        blocks.append(site+': remote worker identity/API/config/source hash/TLS mismatch')
                    expected_tables={file.split('.parquet')[0] for file in expected_data[site]}
                    checks=info.get('required_local_parquet',{})
                    if (not isinstance(checks,dict) or set(checks)!=expected_tables or
                        any(checks[t] is not True for t in expected_tables)):
                        blocks.append(site+': missing required provider-local Parquet files')
                except Exception as exc:
                    blocks.append(site+': HTTPS worker health failed: '+type(exc).__name__)
    if not online:
        blocks.append('Live trusted-TLS worker health and provider-local data verification not performed')
    blocks.append('Provider-side resource cost budget, VM identity and cleanup controls require operator verification')
    # Cloud eligibility here means technical readiness ONLY; external operator
    # approval and controls cannot be inferred from an HTTP health response.
    return {'mode':'M6F_'+('HTTPS_HEALTHCHECK' if online else 'OFFLINE_CLOUD_READINESS'),
            'query':QUERY,'sf':sf,'verified_candidate_pair':[0,4] if QUERY=='Q03' else [0,6],
            'expected_directed_transfers':len(expected_transfers),
            'expected_stage_digest_counts':{k:len(v) for k,v in sorted(expected_stage.items())},
            'expected_provider_local_input_files':{k:sorted(v) for k,v in sorted(expected_data.items())},
            'static_config_valid':offline_valid,'worker_health':remote_health,
            'ready_for_cloud':False,'cloud_executed':False,'blockers':blocks,
            'note':'No runtime/billing/cleanup guarantee. Production experiment is disabled by this readiness report.'}


# M6G — read-only cloud inventory; emits neither account IDs nor IPs/tokens.
def _m6g_cloud_inventory(aws_profile=None):
    """Read-only check of provider CLI sessions and VM states in locked P1 regions.

    No calls to start/stop/provision; not an endpoint TLS or SQL readiness test.
    Output intentionally redacts VM IDs, names, addresses and identities.
    """
    import subprocess, shutil
    required = {'aws':'us-east-1', 'azure':'eastus', 'gcp':'us-east4-a'}
    report={'mode':'M6G_READ_ONLY_INFRA_INVENTORY', 'query':QUERY,
            'cloud_started':False, 'cloud_resources_modified':False,
            'expected_regions':required,
            'provider_sessions':{},'provider_vm_summaries':{},
            'provider_local_parquet_verified':False,
            'trusted_tls_workers_verified':False,
            'ready_for_cloud':False,
            'warnings':['VM inventory does not prove EgressOpt worker configuration or VM identity.',
                        'Do not upload token, keys, config URLs, public IPs, or cloud credentials.']}
    def cli(cmd, timeout=45):
        if not shutil.which(cmd[0]):
            return None, f'{cmd[0]} CLI not installed'
        try:
            r=subprocess.run(cmd,capture_output=True,text=True,timeout=timeout,check=False)
            if r.returncode:
                # Deliberately omit stderr: SDK errors can include resource/account identifiers.
                return None, f'{cmd[0]} exited {r.returncode}; verify login and permissions locally'
            return r.stdout, None
        except (OSError,subprocess.TimeoutExpired) as exc:
            return None,type(exc).__name__
    def summarize(entries, keep_region):
        counts={};candidates=0
        for entry in entries:
            if entry.get('region')!=keep_region:continue
            state=str(entry.get('state','unknown'))
            counts[state]=counts.get(state,0)+1
            if any(term in str(entry.get('name','')).lower() for term in ('egress','tpch','duckdb')):
                candidates+=1
        return {'states':counts,'matching_name_hint_count':candidates,
                'identity_not_yet_confirmed':True}
    aws=['aws']+(['--profile',aws_profile] if aws_profile else [])
    stdout,error=cli(aws+['sts','get-caller-identity','--output','json'],timeout=20)
    report['provider_sessions']['aws']='ok' if not error else error
    if not error:
        stdout,err=cli(aws+['ec2','describe-instances','--region','us-east-1','--output','json'])
        if err:report['provider_vm_summaries']['aws']={'error':err}
        else:
            try:
                payload=json.loads(stdout); instances=[]
                for res in payload.get('Reservations',[]):
                    for it in res.get('Instances',[]):
                        tags={k['Key']:k.get('Value','') for k in it.get('Tags',[]) if 'Key' in k}
                        instances.append({'name':tags.get('Name',''),'state':it.get('State',{}).get('Name','unknown'),'region':'us-east-1'})
                report['provider_vm_summaries']['aws']=summarize(instances,'us-east-1')
            except (ValueError,TypeError):report['provider_vm_summaries']['aws']={'error':'Malformed AWS JSON'}
    stdout,error=cli(['az','account','show','--output','json'],timeout=20)
    report['provider_sessions']['azure']='ok' if not error else error
    if not error:
        stdout,err=cli(['az','vm','list','--show-details','--output','json'],timeout=70)
        if err:report['provider_vm_summaries']['azure']={'error':err}
        else:
            try:
                entries=[{'name':v.get('name',''),'region':str(v.get('location','')).lower(),
                          'state':v.get('powerState','unknown')} for v in json.loads(stdout)]
                report['provider_vm_summaries']['azure']=summarize(entries,'eastus')
            except (ValueError,TypeError):report['provider_vm_summaries']['azure']={'error':'Malformed Azure JSON'}
    stdout,error=cli(['gcloud','auth','list','--filter=status:ACTIVE','--format=value(account)'],timeout=20)
    if not error and not (stdout or '').strip():error='No active gcloud account'
    report['provider_sessions']['gcp']='ok' if not error else error
    if not error:
        stdout,err=cli(['gcloud','compute','instances','list','--format=json'],timeout=70)
        if err:report['provider_vm_summaries']['gcp']={'error':err}
        else:
            try:
                entries=[{'name':v.get('name',''),'region':str(v.get('zone','')).split('/')[-1],
                          'state':v.get('status','unknown')} for v in json.loads(stdout)]
                report['provider_vm_summaries']['gcp']=summarize(entries,'us-east4-a')
            except (ValueError,TypeError):report['provider_vm_summaries']['gcp']={'error':'Malformed GCP JSON'}
    report['next_gate']='Map inventoried VMs to known Phase 1 deployment; verify trusted TLS endpoints and provider-local SF1 Parquet.'
    return report



# M6M — SF10 live pilot gate, based on the M6L verified direct-upload report.
# No new provider binding, storage service, or Phase 1 modification.
# The hashes/row counts below are locked from the verified M6L/SF10 manifests.
_M6M_SF10_LOCKED_FILES = {'customer': {'site': 'azure', 'bytes': 79830051, 'rows': 1500000, 'sha256': 'f5852350d4079789eff053ea15097d0eecbd9cff41b471b40d444077defbd8f9'}, 'lineitem': {'site': 'azure', 'bytes': 1565341081, 'rows': 59986052, 'sha256': '72e123982814d18fe62c1ad45c014fd54d01bd3f8232517a242d82edc02da96e'}, 'nation': {'site': 'azure', 'bytes': 2009, 'rows': 25, 'sha256': '5279e1c8282a4b6a97258f575536b89cd3eecf09558bd02f61d4b02c141087c4'}, 'orders': {'site': 'aws', 'bytes': 384676403, 'rows': 15000000, 'sha256': 'f601a81423eeaa35766fed911e8d91a3d96181cbff4cb02341db1a9e9a25f3b1'}, 'region': {'site': 'gcp', 'bytes': 997, 'rows': 5, 'sha256': 'a68a52afe4fa7a2db0b791a6f1b43df5bcdaed0631bfcb8e6b8833ae0bd5f43e'}, 'supplier': {'site': 'gcp', 'bytes': 5111453, 'rows': 100000, 'sha256': '3d7140b3ca705ed6b6bebb382d0b7436a0d2ff4c15244d085d096a3f0a3865ec'}}


def _m6m_sf10_evidence_gate(project, check_local_files=True):
    """Fail closed before VM startup unless SF10 files & deployment match.

    The earlier M6L deployment cryptographically verified Parquet contents on
    every provider. Re-check the LOCAL source here; remote file presence is
    checked by the existing worker health/remote config stages on startup.
    A report is an evidence gate, not an ongoing guarantee against disk changes.
    """
    import hashlib
    loc = project/'results'/'dataset_validation'/'parquet_sf10_manifest.json'
    if not loc.is_file():
        raise FileNotFoundError('M6M missing SF10 converter manifest: '+str(loc))
    source = json.loads(loc.read_text())
    if source.get('scale_factor') != 10 or not isinstance(source.get('tables'),dict):
        raise RuntimeError('M6M SF10 source manifest has wrong scale/schema')
    expected = _M6M_SF10_LOCKED_FILES
    for table, spec in expected.items():
        found = source['tables'].get(table)
        if not isinstance(found,dict) or any(found.get(k)!=spec[k] for k in ('bytes','rows','sha256')):
            raise RuntimeError('M6M source manifest drift: '+table)
        required_path = 'data/tpch/parquet_sf10/'+table+'.parquet'
        if found.get('file') != required_path:
            raise RuntimeError('M6M unexpected SF10 data path for '+table)
        if check_local_files:
            local_path = project/required_path
            if not local_path.is_file() or local_path.stat().st_size != spec['bytes']:
                raise RuntimeError('M6M missing/size-mismatched local SF10 Parquet: '+table)
            h=hashlib.sha256()
            with local_path.open('rb') as f:
                for chunk in iter(lambda:f.read(4*1024*1024), b''):
                    h.update(chunk)
            if h.hexdigest()!=spec['sha256']:
                raise RuntimeError('M6M local SF10 Parquet SHA-256 mismatch: '+table)
    records = sorted((project/'results'/'phase2'/'m6l').glob('sf10_deployment_*.json'),
                     key=lambda p:p.stat().st_mtime, reverse=True)
    if not records:
        raise FileNotFoundError('M6M requires successful M6L deployment report under results/phase2/m6l/')
    failures=[]; used=None
    for location in records[:1]:
        try:
            run=json.loads(location.read_text())
            if (run.get('status')!='SF10_CLOUD_FILES_VERIFIED'
                or run.get('sf')!=10
                or run.get('destination_root')!='/opt/egressopt/data/tpch_sf10'
                or run.get('source_manifest_verified_locally') is not True
                or run.get('cleanup_complete') is not True
                or run.get('virtual_machines_restored') is not True
                or run.get('temporary_rules_removed') is not True
                or run.get('s3_used') is not False
                or run.get('protocol')!='M6L_DIRECT_PINNED_TLS_UPLOAD'
                or run.get('phase1_code_changed') is not False
                or any(run.get('cleanup',{}).get(k)!='stopped' for k in ('vm_aws','vm_azure','vm_gcp'))
                or any(run.get('cleanup',{}).get(k)!='removed' for k in ('service_aws','service_azure','service_gcp'))):
                raise RuntimeError('Deployment outcome/cleanup is not verified')
            rows = run.get('verified_files')
            if not isinstance(rows,list) or len(rows)!=len(expected):
                raise RuntimeError('Wrong number of verified cloud files')
            mapped={}
            for entry in rows:
                table=entry.get('table')
                if table in mapped or table not in expected:
                    raise RuntimeError('Duplicate/unexpected cloud dataset table')
                mapped[table]=entry
            if any(any(mapped[t].get(k)!=v for k,v in spec.items()) for t,spec in expected.items()):
                raise RuntimeError('Cloud dataset site/rows/bytes/hash mismatch')
            if run.get('bytes_planned')!=sum(x['bytes'] for x in expected.values()):
                raise RuntimeError('Deployment planned bytes mismatch')
            used=location
            break
        except (ValueError, KeyError, RuntimeError) as exc:
            failures.append(location.name+': '+str(exc))
    if used is None:
        raise RuntimeError('M6M has no fully verified SF10 cloud deployment: '+'; '.join(failures[:3]))
    return {'status':'M6M_SF10_DEPLOYMENT_EVIDENCE_PASS',
            'dataset':'TPC-H DBGEN SF10',
            'source_parquet_sha256_verified':bool(check_local_files),
            'dataset_manifest_file':str(loc.relative_to(project)),
            'deployment_report_file':str(used.relative_to(project)),
            'verified_cloud_files':len(expected),
            'total_verified_bytes':sum(x['bytes'] for x in expected.values()),
            'cloud_sites':sorted(set(x['site'] for x in expected.values())),
            'max_single_stage_transfer_bytes':500_000_000,
            'gate_performs_no_cloud_operations':True,
            'caution':'M6L established cloud SHA-256 and counts at deployment time; live worker health verifies presence. SF10 query timing is unmeasured.'}

# M6H — reuse the locked Phase 1 Q05 multi-provider management/cleanup
# implementation. No cloud management call is made in reuse-plan mode.
# The Phase 1 module remains read-only; the generic Phase 2 DAG remains here.
def _m6h_phase1(project):
    """Import the EXACT original Phase 1 provider-management helper code."""
    import importlib.util
    baseline=project/'experiments'/'q05.py'
    if not baseline.is_file() or digest(baseline)!='bc7ac734062e88e13e4a23b3b6368c7a5623c6a806adfef63c9d913462526d21':
        raise RuntimeError('Frozen Q05 Phase 1 cloud deployment runner missing/modified')
    spec=importlib.util.spec_from_file_location('egressopt_phase1_q05_readonly', baseline)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    expected=('q05_start_vm','q05_remote','q05_aws_instance','q05_azure_state',
              'q05_gcp_instance','q05_outbound','q05_aws_rule','q05_azure_rule','q05_gcp_rule')
    if any(not callable(getattr(module, f, None)) for f in expected):
        raise RuntimeError('Phase 1 provider management API changed')
    return module


def _m6h_reuse_plan(project,sql,edges,sf,max_filters,run_id):
    """Present only actual Phase 1 resource bindings; no discovered lookalikes."""
    import inspect
    infra=_m6h_phase1(project)
    if sf not in (1,10):raise RuntimeError('M6K Phase 1 infrastructure adapter supports SF1/SF10 only')
    if not re.fullmatch(r'[a-z][a-z0-9]{1,11}',run_id):
        raise ValueError('Use a lowercase alphanumeric run ID 2..12 chars, beginning with a letter')
    sites=sorted(set(PLACEMENT.values()))
    required_by_site={s:sorted(t for t in PLACEMENT if PLACEMENT[t]==s) for s in sites}
    bundle=_m6e_build_cloud_bundle(project,sql,edges,sf,max_filters,run_id,
                 f'/opt/egressopt/data/tpch_sf{sf}','/opt/egressopt/runtime/phase2')
    contracts={
        'aws':{'provider_command':'AWS Systems Manager SendCommand',
               'management_function':'q05_remote', 'python':infra.Q05_CLOUD_VENV['aws'],
               'instance_binding':'AWS_INSTANCE', 'profile':infra.AWS_PROFILE,
               'region':infra.AWS_REGION},
        'azure':{'provider_command':'Azure VM Run Command',
                 'management_function':'q05_remote', 'python':infra.Q05_CLOUD_VENV['azure'],
                 'instance_binding':'AZ_RG+AZ_VM', 'region':'eastus'},
        'gcp':{'provider_command':'GCP IAP SSH',
               'management_function':'q05_remote', 'python':infra.Q05_CLOUD_VENV['gcp'],
               'instance_binding':'GCP_PROJECT+GCP_VM+GCP_ZONE', 'zone':infra.GCP_ZONE}}
    transfers=bundle['cloud_config_template']['allowed_transfers']
    directed=sorted(set((t['from'],t['to']) for t in transfers))
    return {'mode':'M6H_PHASE1_CODE_REUSE_PLAN','query':QUERY,'sf':sf,
        'source_lock':'PASS','phase1_provider_runner_sha256':digest(project/'experiments'/'q05.py'),
        'management_channels':{s:contracts[s] for s in sites},
        'existing_vm_bindings_used':True,'no_new_vms_or_storage':True,
        'expected_provider_local_tables':required_by_site,
        'strict_directed_transfer_pairs':[list(pair) for pair in directed],
        'allowed_stage_sha256_counts':{s:len(bundle['cloud_config_template']['allowed_stage_sha256'][s]) for s in sites},
        'allowed_transfer_records':len(transfers),
        'candidates':[0,4] if QUERY=='Q03' else [0,6],
        'required_run_permission':'--ack-cloud-cost for cloud-reuse-run',
        'protocol':'Phase 1 provider management channels for control; provider-to-provider HTTPS for Parquet',
        'cloud_started':False,'cloud_executed':False,
        'limitations':['Provider endpoints and temporary TLS credentials are available only after VMs start.',
                       'Phase 2 remote latency is measured inside workers; management-channel overhead is separately tracked.',
                       'An actual cloud measurement requires a successful cloud-reuse-run.']}


def _m6h_worker_source_bundle(project):
    """Return a gzip tar with just this query's locked Phase 1 + Phase 2 sources."""
    import io,tarfile,gzip
    paths=[project/'experiments'/f'{QUERY.lower()}-phase2.py',
           project/'experiments'/f'{QUERY.lower()}.py',
           project/'queries'/'tpch'/f'{QUERY.lower()}.sql']
    for path in paths:
        if not path.is_file(): raise FileNotFoundError(path)
    raw=io.BytesIO()
    with tarfile.open(fileobj=raw,mode='w') as tf:
        for path in paths:
            if path.parent.name=='experiments': target='experiments/'+path.name
            else:target='queries/tpch/'+path.name
            meta=tarfile.TarInfo(target); b=path.read_bytes()
            meta.size=len(b);meta.mode=0o600;meta.mtime=0
            tf.addfile(meta,io.BytesIO(b))
    return gzip.compress(raw.getvalue(),mtime=0)


def _m6h_remote_upload(infra,site,source_bytes,remote_dir,tag,fast=False):
    """Reuse Phase 1 Q05 remote() rather than adding another cloud SDK stack."""
    import base64,shlex
    from hashlib import sha256
    remote_tmp=remote_dir+'/upload_'+tag+'.b64'
    payload=base64.b64encode(source_bytes).decode('ascii')
    infra.q05_remote(site, 'umask 077; mkdir -p '+shlex.quote(remote_dir)+
                    '; : > '+shlex.quote(remote_tmp), 'p2-m6h-prepare-'+tag)
    chunks=[payload[i:i+2400] for i in range(0,len(payload),2400)]
    # Old safe default = 1 chunk per remote call. Explicit M6I mode batches
    # just four identical append commands per call. Strict order preserved.
    batch_size=4 if fast else 1
    for offset in range(0,len(chunks),batch_size):
        group=chunks[offset:offset+batch_size]
        command='\n'.join('printf %s '+shlex.quote(chunk)+' >> '+shlex.quote(remote_tmp)
                          for chunk in group)
        infra.q05_remote(site,command,
                         ('p2-m6i-upload-' if fast else 'p2-m6h-upload-')+tag)
    return remote_tmp,sha256(source_bytes).hexdigest()


def _m6h_remote_rpc(infra,site,urlpath,payload,token,workdir):
    """Execute control POST locally via the *existing* authenticated management channel.

    Only /health and /execute and /push of the existing generic worker are
    supported. The data plane /push -> /ingest remains true provider-to-provider
    HTTPS, not a copied file tunneled through SSM/IAP/RunCommand.
    """
    import base64,shlex,gzip
    import urllib.parse
    path=urllib.parse.urlsplit(urlpath).path
    if path not in ('/health','/execute','/push'):
        raise ValueError('Illegal Phase 2 RPC path')
    is_get=payload is None
    body='' if is_get else json.dumps(payload,separators=(',',':'))
    # Only requests to the same guest's loopback bypass the guest's public cert.
    script="""set -eu
RESP=$(curl --silent --show-error --fail-with-body --insecure --connect-timeout 4 --max-time 300 \\
  -H {auth} {data} {uri})
export RESP
{py} -c {convert}
""".format(auth=shlex.quote('Authorization: Bearer '+token),
        data='' if is_get else '-H '+shlex.quote('Content-Type: application/json')+' --data-binary '+shlex.quote(body),
        uri=shlex.quote('https://127.0.0.1:8443'+path),
        py=shlex.quote(infra.Q05_CLOUD_VENV[site]),
        convert=shlex.quote('import os,json,gzip,base64; a=json.loads(os.environ["RESP"]); '
        'print("PHASE2_RPC_GZ="+base64.b64encode(gzip.compress(json.dumps(a,separators=(",",":")).encode())).decode())'))
    stdout=infra.q05_remote(site,script,'p2-m6h-'+path.strip('/'))
    markers=[ln.split('PHASE2_RPC_GZ=',1)[1].strip() for ln in stdout.splitlines()
             if ln.startswith('PHASE2_RPC_GZ=')]
    if len(markers)!=1:raise RuntimeError('Expected exactly one authenticated M6H RPC response')
    result=json.loads(gzip.decompress(base64.b64decode(markers[0],validate=True)))
    if not isinstance(result,dict):raise RuntimeError('Bad worker response')
    return result


def _m6h_remote_execute(project,sql,edges,candidate,sf,max_filters,run_id,cfg,infra,token,workdir,progress_cb=None):
    """Execute a signed DAG over the old management plane and actual peer HTTPS.

    Do not conflate the management latency with query/transport latency.
    """
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters,
            data_root_override=cfg['data_root'],
            runtime_root_override=str(Path(cfg['runtime_root'])/f'{QUERY.lower()}_{run_id}'))
    total=0;traces=[];done=set();finished=None
    wall=time.perf_counter()
    for stage in plan['steps']:
        if not set(stage['depends_on']).issubset(done):raise RuntimeError('Unmet DAG dependency '+stage['id'])
        provider=stage.get('site',stage.get('from'))
        if progress_cb:progress_cb('executing_stage',site=provider,
           candidate=candidate,stages=[stage['id']])
        tick=time.perf_counter()
        if stage['type']=='transfer_file':
            receipt=_m6h_remote_rpc(infra,provider,'/push',{
                'to':stage['to'],'source':stage['source'], 'destination':stage['destination']},
                token,workdir)
            if (receipt.get('from')!=stage['from'] or receipt.get('to')!=stage['to'] or
                receipt.get('payload_metric')!='CROSS_PROVIDER_HTTP_REQUEST_BODY_BYTES' or
                not re.fullmatch(r'[0-9a-f]{64}',receipt.get('sha256','')) or
                int(receipt.get('bytes',-1))<0):
                raise RuntimeError('Invalid TLS transfer receipt '+stage['id'])
            total+=int(receipt['bytes'])
            server_seconds=float(receipt.get('transfer_seconds',0))
            evidence={'payload_bytes':int(receipt['bytes']),'sha256':receipt['sha256'],
                      'from':stage['from'],'to':stage['to']}
        else:
            signature=hmac.new(token.encode(),
                     (stage['site']+'\n'+stage['type']+'\n'+stage['sql']).encode(),'sha256').hexdigest()
            response=_m6h_remote_rpc(infra,provider,'/execute',{
                'site':stage['site'],'type':stage['type'],'sql':stage['sql'],
                'stage_id':stage['id'],'signature':signature,'output':stage.get('output')},
                token,workdir)
            if response.get('site')!=stage['site'] or response.get('stage')!=stage['id']:
                raise RuntimeError('Incorrect signed-stage response '+stage['id'])
            server_seconds=float(response['elapsed_seconds'])
            if stage['type']=='duckdb_query':
                finished=response['result_rows'];evidence={'row_count':len(finished)}
            else:evidence={'artifact_bytes':int(response['artifact_bytes'])}
        traces.append({'stage':stage['id'],'type':stage['type'],'site':provider,
                       'worker_or_transfer_seconds':server_seconds,
                       'controller_with_management_seconds':time.perf_counter()-tick,
                       'evidence':evidence})
        done.add(stage['id'])
    if finished is None:raise RuntimeError('No final query rows produced')
    return {'candidate_index':candidate,'run_id':run_id,'query':QUERY,
            'execution_mode':'real_multicloud_management_plane_reused',
            'http_request_body_bytes':total,'result_rows':finished,
            'server_stage_plus_tls_seconds':sum(t['worker_or_transfer_seconds'] for t in traces),
            'controller_including_management_seconds':time.perf_counter()-wall,
            'traces':traces,'stage_count':len(traces),
            'timing_caution':'Non-overlap serial worker timing; does not model parallel execution or exclude warmup/caching.',
            'billing_egress_usd':None}



def _m6i_remote_rpc_batch(infra,site,items,token,workdir):
    """Batch *adjacent same-site* RPCs through one original Phase 1 remote call.

    Each signed /execute or validated /push request still reaches the existing
    TLS worker separately. This optimization only removes redundant Azure
    Run Command / AWS SSM / GCP IAP management round trips. No SQL reordering.
    """
    import base64,gzip,shlex
    if not items or len(items)>8:raise ValueError('M6I batch size 1..8 required')
    py=shlex.quote(infra.Q05_CLOUD_VENV[site])
    shell=['set -eu']
    for i,item in enumerate(items):
        stage=item['stage']
        path=item['path']
        if path not in ('/execute','/push'):
            raise ValueError('M6I only batches /execute and /push')
        if stage.get('site',stage.get('from'))!=site:
            raise ValueError('Cross-site management batching forbidden')
        body=json.dumps(item['payload'],separators=(',',':'))
        shell.append('echo '+shlex.quote('PHASE2_BATCH_STAGE_'+str(i)+'='+stage['id']))
        shell.append('PHASE2_RESP_'+str(i)+'=$(curl --silent --show-error --fail-with-body '
                     '--insecure --connect-timeout 4 --max-time 300 -H '+
                     shlex.quote('Authorization: Bearer '+token)+' -H '+
                     shlex.quote('Content-Type: application/json')+' --data-binary '+
                     shlex.quote(body)+' '+shlex.quote('https://127.0.0.1:8443'+path)+')')
        shell.append('export PHASE2_RESP_'+str(i))
        converter=('import os,json,gzip,base64; a=json.loads(os.environ["PHASE2_RESP_'+str(i)+'"]); '
                   'print("PHASE2_BATCH_RPC_'+str(i)+'="+'
                   'base64.b64encode(gzip.compress(json.dumps(a,separators=(",",":")).encode())).decode())')
        shell.append(py+' -c '+shlex.quote(converter))
    stdout=infra.q05_remote(site,'\n'.join(shell),'p2-m6i-batch-'+site)
    results=[]
    for i in range(len(items)):
        prefix='PHASE2_BATCH_RPC_'+str(i)+'='
        markers=[line[len(prefix):].strip() for line in stdout.splitlines() if line.startswith(prefix)]
        if len(markers)!=1:
            raise RuntimeError('M6I incomplete batch at '+site+' request '+str(i)+' stage '+items[i]['stage']['id'])
        result=json.loads(gzip.decompress(base64.b64decode(markers[0],validate=True)))
        if not isinstance(result,dict):raise RuntimeError('Invalid M6I worker response')
        results.append(result)
    return results


def _m6i_remote_execute(project,sql,edges,candidate,sf,max_filters,run_id,cfg,infra,token,workdir,progress_cb=None):
    """Execute frozen DAG in order; group only contiguous same-site steps."""
    plan=physical_plan(project,sql,edges,candidate,sf=sf,run_id=run_id,max_filters=max_filters,
            data_root_override=cfg['data_root'],
            runtime_root_override=str(Path(cfg['runtime_root'])/f'{QUERY.lower()}_{run_id}'))
    total=0;traces=[];done=set();finished=None; batches=[]
    wall=time.perf_counter()
    steps=plan['steps']
    index=0
    while index<len(steps):
        site=steps[index].get('site',steps[index].get('from'))
        group=[]
        while index<len(steps) and steps[index].get('site',steps[index].get('from'))==site and len(group)<8:
            stage=steps[index]
            # The frozen DAG has no speculative execution or cross-site overlap.
            if not set(stage['depends_on']).issubset(done|{e['id'] for e in group}):
                raise RuntimeError('M6I unmet DAG dependency '+stage['id'])
            group.append(stage);index+=1
        items=[]
        for stage in group:
            if stage['type']=='transfer_file':
                items.append({'stage':stage,'path':'/push','payload':{
                     'to':stage['to'],'source':stage['source'],
                     'destination':stage['destination']}})
            else:
                signature=hmac.new(token.encode(),
                    (stage['site']+'\n'+stage['type']+'\n'+stage['sql']).encode(),'sha256').hexdigest()
                items.append({'stage':stage,'path':'/execute','payload':{
                    'site':stage['site'],'type':stage['type'],'sql':stage['sql'],
                    'stage_id':stage['id'],'signature':signature,'output':stage.get('output')}})
        if progress_cb:progress_cb('executing_management_batch',site=site,
           candidate=candidate,stages=[stage['id'] for stage in group])
        tick=time.perf_counter()
        responses=_m6i_remote_rpc_batch(infra,site,items,token,workdir)
        elapsed=time.perf_counter()-tick
        if len(responses)!=len(group):raise RuntimeError('M6I response count mismatch')
        batch_index=len(batches)
        batches.append({'provider':site,'stages':[e['id'] for e in group],
                        'controller_management_batch_seconds':elapsed})
        for stage,response in zip(group,responses):
            if stage['type']=='transfer_file':
                if (response.get('from')!=stage['from'] or response.get('to')!=stage['to'] or
                    response.get('payload_metric')!='CROSS_PROVIDER_HTTP_REQUEST_BODY_BYTES' or
                    not re.fullmatch(r'[0-9a-f]{64}',response.get('sha256','')) or
                    int(response.get('bytes',-1))<0):
                    raise RuntimeError('M6I invalid TLS receipt '+stage['id'])
                total+=int(response['bytes'])
                seconds=float(response.get('transfer_seconds',0))
                evidence={'payload_bytes':int(response['bytes']),'sha256':response['sha256'],
                          'from':stage['from'],'to':stage['to']}
            else:
                if response.get('site')!=stage['site'] or response.get('stage')!=stage['id']:
                    raise RuntimeError('M6I incorrect signed response '+stage['id'])
                seconds=float(response['elapsed_seconds'])
                if stage['type']=='duckdb_query':
                    finished=response['result_rows'];evidence={'row_count':len(finished)}
                else:evidence={'artifact_bytes':int(response['artifact_bytes'])}
            traces.append({'stage':stage['id'],'type':stage['type'],'site':site,
                 'worker_or_transfer_seconds':seconds,
                 'controller_with_management_seconds':None,
                 'management_batch_index':batch_index,'evidence':evidence})
            done.add(stage['id'])
    if finished is None:raise RuntimeError('M6I no final query rows')
    return {'candidate_index':candidate,'run_id':run_id,'query':QUERY,
      'execution_mode':'real_multicloud_management_plane_batched',
      'http_request_body_bytes':total,'result_rows':finished,
      'server_stage_plus_tls_seconds':sum(t['worker_or_transfer_seconds'] for t in traces),
      'controller_including_management_seconds':time.perf_counter()-wall,
      'management_rpc_calls':len(batches),'management_batches':batches,
      'traces':traces,'stage_count':len(traces),
      'timing_caution':'Serial worker timing; management control batched by consecutive provider. '
                       'Batch timings not attributable to individual stages; no concurrent DAG execution.',
      'billing_egress_usd':None}

def _m6h_signature(rows):
    """Independent SQL equivalence signature, identical to M6D reference gate."""
    from decimal import Decimal, ROUND_HALF_EVEN
    prec=Decimal('0.0001') if QUERY=='Q03' else Decimal('0.01')
    return sorted((str(row[0]), format(Decimal(str(row[1])).quantize(prec,rounding=ROUND_HALF_EVEN),'f'))
                  +tuple(str(v) for v in row[2:]) for row in rows)


def _m6h_local_reference(project,sql,sf):
    import duckdb
    data=project/'data'/'tpch'/f'parquet_sf{sf}'
    tables=sorted(COLUMNS[QUERY])
    missing=[str(data/(t+'.parquet')) for t in tables if not (data/(t+'.parquet')).is_file()]
    if missing:raise FileNotFoundError('Independent reference data missing locally: '+', '.join(missing))
    ctes=', '.join(t+' AS (SELECT * FROM read_parquet('+qlit(data/(t+'.parquet'))+'))' for t in tables)
    with duckdb.connect(':memory:') as con:
        source=con.execute('WITH '+ctes+'\n'+sql.strip().rstrip(';')).fetchall()
    return _m6h_signature(source)


def _m6j_paired_summary(observations,warmups,repeats,candidates):
    """Descriptive and deterministic paired bootstrap evidence, not a runtime claim."""
    import random,statistics
    measured=[o for o in observations if not o['warmup']]
    pairs={}
    for obs in measured:
        rid=obs['round_index']
        if obs['candidate_index'] in pairs.setdefault(rid,{}):
            raise RuntimeError('M6J duplicate candidate in a round')
        pairs[rid][obs['candidate_index']]=obs
    if len(pairs)!=repeats or any(set(d)!=set(candidates) for d in pairs.values()):
        raise RuntimeError('M6J missing paired observations')
    order=sorted(pairs)
    baseline=[pairs[k][candidates[0]] for k in order]
    semijoin=[pairs[k][candidates[1]] for k in order]
    def describe(series):
        vals=list(map(float,series))
        return {'n':len(vals),'mean':statistics.fmean(vals),
                'median':statistics.median(vals),
                'sample_stdev':statistics.stdev(vals) if len(vals)>1 else None,
                'min':min(vals),'max':max(vals)}
    def compare(metric):
        xb=[float(x[metric]) for x in baseline]
        xs=[float(x[metric]) for x in semijoin]
        if not all(x>0 for x in xb):raise RuntimeError('M6J zero baseline for '+metric)
        pct=[100*(1-y/x) for x,y in zip(xb,xs)]
        ratios=[y/x for x,y in zip(xb,xs)]
        result={'baseline':describe(xb),'semijoin':describe(xs),
                'paired_percent_reduction':describe(pct)}
        if len(pct)>=3:
            rng=random.Random(713211)
            boots=[statistics.fmean([pct[rng.randrange(len(pct))]
                    for _ in range(len(pct))]) for _ in range(4000)]
            boots.sort()
            result['paired_mean_reduction_bootstrap_95pct']=[boots[99],boots[3899]]
        return result
    return {'n_measured_pairs':repeats,'n_warmup_pairs':warmups,
            'candidate_0_baseline':candidates[0],'candidate_1_semijoin':candidates[1],
            'plan_order':'alternated by round index; starting plan reverses every round',
            'unit':'Paired sequential cloud runs; serial worker stage+TLS not DAG critical path',
            'cross_cloud_http_request_body_bytes':compare('http_request_body_bytes'),
            'serial_worker_stage_plus_tls_seconds':compare('server_stage_plus_tls_seconds'),
            'management_inclusive_controller_seconds':compare('controller_including_management_seconds'),
            'caution':'Paired percentile bootstrap is descriptive for this limited warm-cache VM run. '
                     'No parallel-DAG latency, monetary bill, randomized cross-day design, or global optimality established.'}


def _m6h_cloud_run(project,sql,edges,sf,max_filters,run_id,warmups,repeats,ack_cloud_cost,batch_management_rpc=False):
    """Reuse Phase 1 VM bindings/SSM/RunCommand/IAP, not its fixed SQL worker.

    Always release temporary worker services and source-restricted ingress.
    Stop only VMs that this run actually started. Cloud changes require an
    explicit CLI flag; this function is NOT called in any offline mode.
    """
    import base64,secrets,shlex,tarfile,io
    if not ack_cloud_cost:raise RuntimeError('Explicit --ack-cloud-cost required before VM startup')
    if not 0<=warmups<=2 or not 1<=repeats<=20 or warmups+repeats>21:
        raise ValueError('M6J permits 0..2 warmup pairs, 1..20 measured pairs; at most 21 rounds')
    sf10_evidence=_m6m_sf10_evidence_gate(project) if sf==10 else None
    reuse=_m6h_reuse_plan(project,sql,edges,sf,max_filters,run_id)
    # Run independently checked locked SQL BEFORE starting billable VMs.
    reference=_m6h_local_reference(project,sql,sf)
    infra=_m6h_phase1(project)
    sites=list(reuse['management_channels']);runkey='m6h'+run_id
    if len(runkey)>28:raise ValueError('Run ID too long for Phase 1 GCP firewall tag')
    # Phase 1 defaults and environment overrides are identical to the previous runs.
    sources=_m6h_worker_source_bundle(project)
    rounds=warmups+repeats
    # Preserve the exact original M6H/M6I pilot identifiers if just one round.
    trial_ids={c:[f'{run_id}_{c}'] if rounds==1 else
                    [f'{run_id}_r{rep:02d}_{c}' for rep in range(rounds)]
               for c in reuse['candidates']}
    # Freeze all trial-specific signed SQL and directed transfers BEFORE
    # starting any cloud resources. Each round uses physically disjoint files.
    bundle=_m6e_build_cloud_bundle(project,sql,edges,sf,max_filters,run_id,
            f'/opt/egressopt/data/tpch_sf{sf}','/opt/egressopt/runtime/phase2',
            candidate_trial_ids=trial_ids)
    cfg=bundle['cloud_config_template']
    edges_allowed=sorted(set((v['from'],v['to']) for v in cfg['allowed_transfers']))
    state={'started':{s:False for s in sites},'service':{},'rules':{'aws':[],'azure':[],'gcp':[]},
           'gcp_tag':None,'gcp_external_added':False,'gcp_tag_added':False}
    outroot=project/'results'/'phase2'/'m6h'/f'{QUERY.lower()}_{run_id}'
    outroot.mkdir(parents=True,exist_ok=False)
    report={'status':'RUNNING','query':QUERY,'sf':sf,'run_id':run_id,
            'candidates':reuse['candidates'],'source_guard':'PASS',
            'phase1_cloud_runner_sha256':reuse['phase1_provider_runner_sha256'],
            'transfer_metric':'cross-cloud TLS HTTP request-body bytes, not billed egress',
            'observations':[],'cleanup':{},'cloud_execution_attempted':True,
            'sf10_deployment_evidence':sf10_evidence,
            'measurement_protocol':{'warmup_pairs':warmups,'measured_pairs':repeats,
                  'plan_order':'alternating AB/BA','independent_trial_paths':True,
                  'source':'M6J extension of M6I, unchanged Phase 1 management'}}
    (outroot/'run.json').write_text(json.dumps(report,indent=2))
    def record_progress(event,site=None,candidate=None,stages=None):
        from datetime import datetime,timezone
        event_record={'event':event,'timestamp_utc':datetime.now(timezone.utc).isoformat(),
           'query':QUERY,'run_id':run_id,'provider':site,'candidate_index':candidate,
           'stage_ids':stages or []}
        (outroot/'progress.json').write_text(json.dumps(event_record,indent=2))
    record_progress('preparing_cloud_resources')
    token=secrets.token_urlsafe(48)
    work='/opt/egressopt/runtime/phase2_m6h_'+QUERY.lower()+'_'+run_id
    release='/opt/egressopt/releases/phase2_'+QUERY.lower()+'_'+hashlib.sha256(sources).hexdigest()[:12]
    ca={};ips={};outbound={};aws_sg=[]
    try:
        for site in sites:
            # Record intended VM ownership BEFORE startup. If a start partially
            # succeeds and its CLI times out, the finally block still stops it.
            previous=({'aws':lambda:infra.q05_aws_instance()['State']['Name'],
                       'azure':infra.q05_azure_state,
                       'gcp':lambda:infra.q05_gcp_instance()['status']}[site])()
            if previous not in ('running','stopped','PowerState/running',
                                'PowerState/deallocated','PowerState/stopped',
                                'RUNNING','TERMINATED','STOPPED'):
                raise RuntimeError('Unrecognized state for original '+site+' VM')
            state['started'][site]=previous in ('stopped','PowerState/deallocated',
                                              'PowerState/stopped','TERMINATED','STOPPED')
            record_progress('starting_vm',site=site)
            infra.q05_start_vm(site)
        aws=infra.q05_aws_instance();ips['aws']=infra.q05_ip(aws['PublicIpAddress'])
        if 'azure' in sites:
            ip_obj=json.loads(infra.q05_cli(['az','network','public-ip','show','--resource-group',
                infra.AZ_RG,'--name',infra.AZ_PUBLIC,'--output','json']))
            ips['azure']=infra.q05_ip(ip_obj['ipAddress'])
        if 'gcp' in sites:
            gcp=infra.q05_gcp_instance()
            # Preserve/restore Phase 1's transient GCP public NAT behavior.
            if not any(x.get('natIP') for nic in gcp['networkInterfaces']
                   for x in nic.get('accessConfigs',[])):
                state['gcp_external_added']=True
                infra.q05_gcloud('compute','instances','add-access-config',infra.GCP_VM,
                    '--zone='+infra.GCP_ZONE,'--network-interface=nic0',
                    '--access-config-name=external-nat','--quiet','--format=json',timeout=280)
                gcp=infra.q05_gcp_instance()
            ips['gcp']=infra.q05_gcp_public(gcp)
        cfg['sites']={s:'https://'+ips[s]+':8443' for s in sites}
        for site in sites:outbound[site]=infra.q05_outbound(site)
        aws_sg=[x['GroupId'] for x in aws['SecurityGroups']]
        if len(aws_sg)!=1:raise RuntimeError('Phase 1 AWS VM should have exactly one security group')
        configbytes=json.dumps(cfg,sort_keys=True,separators=(',',':')).encode()
        # Non-secret config, same exact bytes/hash on controller and all workers.
        cfgfile=outroot/'cloud_config_PRIVATE.json'
        cfgfile.write_bytes(configbytes);cfgfile.chmod(0o600)
        for site in sites:
            record_progress('deploying_worker_code',site=site)
            tmp,digest=_m6h_remote_upload(infra,site,sources,release,runkey,batch_management_rpc)
            python=infra.Q05_CLOUD_VENV[site]
            command=('mkdir -p '+shlex.quote(release)+'; '+
                     'test "$(base64 -d '+shlex.quote(tmp)+' | sha256sum | cut -d\' \' -f1)" = '+shlex.quote(digest)+'; '+
                     'base64 -d '+shlex.quote(tmp)+
                     ' | gzip -d | tar -xf - -C '+shlex.quote(release)+'; '
                     'rm -f '+shlex.quote(tmp)+'; '
                     'test "$(sha256sum '+shlex.quote(release+'/experiments/'+QUERY.lower()+'.py')+
                     ' | cut -d\' \' -f1)" = '+shlex.quote(ORIGINAL_RUNNER_SHA256)+'; '
                     'test "$(sha256sum '+shlex.quote(release+'/queries/tpch/'+QUERY.lower()+'.sql')+
                     ' | cut -d\' \' -f1)" = '+shlex.quote(LOCKED_SQL_SHA256)+'; '
                     'mkdir -p '+shlex.quote(work)+'; '
                     'echo M6H_SOURCE_GUARD_PASS')
            if 'M6H_SOURCE_GUARD_PASS' not in infra.q05_remote(site,command,'p2-verify-source'):
                raise RuntimeError('Remote source verification failed: '+site)
            remote_cfg,remote_cfg_sha=_m6h_remote_upload(infra,site,configbytes,work,runkey+'_cfg',batch_management_rpc)
            cmd='base64 -d '+shlex.quote(remote_cfg)+' > '+shlex.quote(work+'/config.json')+'; '
            cmd+='rm -f '+shlex.quote(remote_cfg)+'; '
            cmd+='test "$(sha256sum '+shlex.quote(work+'/config.json')+\
                 ' | cut -d\' \' -f1)" = '+shlex.quote(remote_cfg_sha)+'; '
            files=[f'/opt/egressopt/data/tpch_sf{sf}/{t}.parquet'
                   for t in reuse['expected_provider_local_tables'][site]]
            cmd+='; '.join('test -f '+shlex.quote(path) for path in files)+'; '
            cmd+=python+' -c '+shlex.quote('import duckdb; assert duckdb.__version__=="1.5.6"')+'; '
            cmd+='echo M6H_REMOTE_CONFIG_PASS'
            if 'M6H_REMOTE_CONFIG_PASS' not in infra.q05_remote(site,cmd,'p2-verify-data-config'):
                raise RuntimeError('Remote config/data validation failed: '+site)
            cert=work+'/server.pem';key=work+'/server.key'
            gen=(f'umask 077; mkdir -p {shlex.quote(work)}; '+
                 f'openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 '+
                 f'-keyout {shlex.quote(key)} -out {shlex.quote(cert)} '+
                 f'-subj /CN=egressopt-m6h -addext basicConstraints=critical,CA:TRUE -addext subjectAltName=IP:{ips[site]} >/dev/null 2>&1; '+
                 'printf "PHASE2_CA="; base64 -w0 '+shlex.quote(cert)+'; echo')
            out=infra.q05_remote(site,gen,'p2-generate-temporary-TLS')
            markers=[x.split('PHASE2_CA=',1)[1].strip() for x in out.splitlines()
                     if x.startswith('PHASE2_CA=')]
            if len(markers)!=1:raise RuntimeError('No temporary CA from '+site)
            ca[site]=base64.b64decode(markers[0],validate=True)
        # Certificates are individually pinned, not a public CA or an insecure
        # cross-provider 'verify=False'. No long-lived credentials are created.
        pem=b'\n'.join(ca.values())
        for site in sites:
            record_progress('installing_peer_tls',site=site)
            ca_tmp,ca_digest=_m6h_remote_upload(infra,site,pem,work,runkey+'_ca',batch_management_rpc)
            cmd='base64 -d '+shlex.quote(ca_tmp)+' > '+shlex.quote(work+'/peers.pem')+\
                '; rm -f '+shlex.quote(ca_tmp)+'; '
            cmd+='test "$(sha256sum '+shlex.quote(work+'/peers.pem')+\
                 ' | cut -d\' \' -f1)" = '+shlex.quote(ca_digest)+'; '
            py=infra.Q05_CLOUD_VENV[site]
            launch='\n'.join(['#!/bin/sh','set -eu',
              'export EGRESSOPT_PHASE2_TOKEN='+shlex.quote(token),
              'export EGRESSOPT_PHASE2_TLS_CERT='+shlex.quote(work+'/server.pem'),
              'export EGRESSOPT_PHASE2_TLS_KEY='+shlex.quote(work+'/server.key'),
              'export EGRESSOPT_PHASE2_CA_BUNDLE='+shlex.quote(work+'/peers.pem'),
              'exec '+shlex.quote(py)+' '+shlex.quote(release+'/experiments/'+QUERY.lower()+'-phase2.py')+
                ' --mode worker-serve --project-root '+shlex.quote(release)+
                ' --cloud-config '+shlex.quote(work+'/config.json')+
                ' --worker-site '+site+' --worker-bind 0.0.0.0 --worker-port 8443'])+'\n'
            cmd+='printf %s '+shlex.quote(base64.b64encode(launch.encode()).decode())+\
                 ' | base64 -d > '+shlex.quote(work+'/launch.sh')+'; '
            cmd+='chmod 700 '+shlex.quote(work+'/launch.sh')+'; '
            unit='egressopt-m6h-'+QUERY.lower()+'-'+run_id+'-'+site
            cmd+='systemd-run --unit='+shlex.quote(unit)+\
                 ' --collect --property=Restart=no /bin/sh '+shlex.quote(work+'/launch.sh')+' >/dev/null; '
            cmd+='for i in $(seq 1 40); do CODE=$(curl --insecure -s -o /dev/null -w "%{http_code}" '+\
                 '--max-time 3 https://127.0.0.1:8443/health || true); '
            cmd+='[ "$CODE" = 401 ] && break; sleep 1; done; '
            cmd+='test "$CODE" = 401; echo M6H_WORKER_READY'
            state['service'][site]=unit
            if 'M6H_WORKER_READY' not in infra.q05_remote(site,cmd,'p2-start-TLS-worker'):
                raise RuntimeError('Phase 2 worker failed to start '+site)
        # Every ingress rule is restricted to real directed plan edges only.
        for src,dst in edges_allowed:
            if dst=='aws':
                rule=infra.q05_aws_rule(outbound[src],src,run_id,aws_sg[0])
                state['rules']['aws'].append(rule)
            elif dst=='azure':
                rule=infra.q05_azure_rule(src,outbound[src],run_id)
                state['rules']['azure'].append(rule)
            elif dst=='gcp':
                net=gcp['networkInterfaces'][0]['network'].rsplit('/',1)[-1]
                if state['gcp_tag_added']:
                    # Different sources to same GCP destination require a
                    # distinct tag/rule name. Phase 1 helper uses one tag.
                    raise RuntimeError('M6H supports at most one distinct GCP ingress source')
                rule,tag=infra.q05_gcp_rule(outbound[src],run_id,net)
                state['rules']['gcp'].append(rule)
                state['gcp_tag']=tag;state['gcp_tag_added']=True
        report['workers_started']=sorted(sites)
        # Health via managed guest loopback means Mac does not need ingress.
        for site in sites:
            record_progress('checking_worker_health',site=site)
            info=_m6h_remote_rpc(infra,site,'/health',None,token,work)
            if (info.get('site')!=site or info.get('query')!=QUERY or info.get('api')!='M6d'
                or info.get('config_sha256')!=_m6f_config_digest(cfg)
                or info.get('source_sql_sha256')!=LOCKED_SQL_SHA256
                or info.get('worker_tls_enabled') is not True
                or any(info.get('required_local_parquet',{}).get(t)!=True
                       for t in reuse['expected_provider_local_tables'][site])):
                raise RuntimeError('Phase 2 worker identity/config/source/data mismatch: '+site)
        for rep in range(rounds):
            warm=rep<warmups
            ordered=(reuse['candidates'] if rep%2==0
                     else list(reversed(reuse['candidates'])))
            for candidate in ordered:
                trial=trial_ids[candidate][rep]
                record_progress('starting_candidate',candidate=candidate,
                                stages=['warmup' if warm else 'measured',f'round_{rep}'])
                execute_fn=_m6i_remote_execute if batch_management_rpc else _m6h_remote_execute
                outcome=execute_fn(project,sql,edges,candidate,sf,max_filters,
                                   trial,cfg,infra,token,work,progress_cb=record_progress)
                outcome['warmup']=warm
                outcome['round_index']=rep
                outcome['pair_order']=list(ordered)
                outcome['correct_vs_independent_reference']=_m6h_signature(outcome['result_rows'])==reference
                if not outcome['correct_vs_independent_reference']:
                    raise RuntimeError('Cloud SQL differs from locked independent reference for candidate '+str(candidate))
                # No raw result rows in shareable summary; retain deterministic hash.
                outcome['result_rows_sha256']=hashlib.sha256(json.dumps(outcome.pop('result_rows'),sort_keys=True).encode()).hexdigest()
                report['observations'].append(outcome)
                (outroot/'run.json').write_text(json.dumps(report,indent=2))
        if repeats>1:
            report[f'repeated_sf{sf}_summary']=_m6j_paired_summary(report['observations'],
                                                      warmups,repeats,reuse['candidates'])
        report['status']='CLOUD_RUN_COMPLETED_REFERENCE_VERIFIED'
    except BaseException as exc:
        report['status']='FAILED';report['failure']=type(exc).__name__+': '+str(exc)[:1000]
        raise
    finally:
        record_progress('cleanup_started')
        for site,unit in state['service'].items():
            try:
                owned_paths=[work]+[
                    str(Path(cfg['runtime_root'])/f'{QUERY.lower()}_{trial}')
                    for c in reuse['candidates'] for trial in trial_ids[c]]
                infra.q05_remote(site,'systemctl stop '+shlex.quote(unit)+
                   ' >/dev/null 2>&1 || true; rm -rf '+
                   ' '.join(shlex.quote(p) for p in owned_paths)+
                   '; echo M6H_WORKER_REMOVED', 'p2-cleanup-worker')
                report['cleanup']['worker_'+site]='removed'
            except Exception as e:report['cleanup']['worker_'+site]='FAILED '+str(e)[:250]
        for rid in state['rules']['aws']:
            try:infra.q05_aws('ec2','revoke-security-group-ingress',
                              '--group-id',aws_sg[0],'--security-group-rule-ids',rid)
            except Exception as e:report['cleanup']['aws_rule_'+rid]='FAILED '+str(e)[:250]
        for name in state['rules']['azure']:
            try:infra.q05_cli(['az','network','nsg','rule','delete','--resource-group',
                         infra.AZ_RG,'--nsg-name',infra.AZ_NSG,'--name',name,'--output','none'])
            except Exception as e:report['cleanup']['azure_rule_'+name]='FAILED '+str(e)[:250]
        for name in state['rules']['gcp']:
            try:infra.q05_gcloud('compute','firewall-rules','delete',name,'--quiet')
            except Exception as e:report['cleanup']['gcp_rule_'+name]='FAILED '+str(e)[:250]
        if state['gcp_tag_added']:
            try:infra.q05_gcloud('compute','instances','remove-tags',infra.GCP_VM,
                       '--zone='+infra.GCP_ZONE,'--tags='+state['gcp_tag'],'--quiet','--format=json')
            except Exception as e:report['cleanup']['gcp_tag']='FAILED '+str(e)[:250]
        if state['gcp_external_added']:
            try:infra.q05_gcloud('compute','instances','delete-access-config',infra.GCP_VM,
                       '--zone='+infra.GCP_ZONE,'--network-interface=nic0',
                       '--access-config-name=external-nat','--quiet','--format=json')
            except Exception as e:report['cleanup']['gcp_external_ip']='FAILED '+str(e)[:250]
        for site,started in state['started'].items():
            if not started:continue
            try:
                if site=='aws':
                    if infra.q05_aws_instance()['State']['Name'] not in ('stopped','stopping'):
                        infra.q05_aws('ec2','stop-instances','--instance-ids',infra.AWS_INSTANCE)
                    infra.q05_cli(['aws','ec2','wait','instance-stopped',
                      '--instance-ids',infra.AWS_INSTANCE,'--profile',infra.AWS_PROFILE,
                      '--region',infra.AWS_REGION],timeout=670)
                    if infra.q05_aws_instance()['State']['Name']!='stopped':
                        raise RuntimeError('AWS VM stop not confirmed')
                elif site=='azure':
                    if infra.q05_azure_state()!='PowerState/deallocated':
                        infra.q05_cli(['az','vm','deallocate','--resource-group',
                          infra.AZ_RG,'--name',infra.AZ_VM,'--output','none'],timeout=690)
                    if infra.q05_azure_state()!='PowerState/deallocated':
                        raise RuntimeError('Azure VM deallocation not confirmed')
                elif site=='gcp':
                    if infra.q05_gcp_instance()['status']!='TERMINATED':
                        infra.q05_gcloud('compute','instances','stop',infra.GCP_VM,
                            '--zone='+infra.GCP_ZONE,'--quiet','--format=json',timeout=690)
                    if infra.q05_gcp_instance()['status']!='TERMINATED':
                        raise RuntimeError('GCP VM stop not confirmed')
                report['cleanup']['vm_'+site]='stopped_verified'
            except Exception as e:report['cleanup']['vm_'+site]='FAILED '+str(e)[:250]
        report['cleanup_incomplete']=any(str(x).startswith('FAILED') for x in report['cleanup'].values())
        (outroot/'run.json').write_text(json.dumps(report,indent=2,sort_keys=True))
        record_progress('cleanup_incomplete' if report['cleanup_incomplete'] else 'cleanup_completed')
    return report

def main():
    parser=argparse.ArgumentParser(description=f'EgressOpt {QUERY} Phase2 single-query runner: offline/loopback or explicit cloud')
    parser.add_argument('--mode',choices=['inspect','enumerate','compile','selftest','local','diagnostic','select','evaluate','cloud-dry-run','cloud-preflight','cloud-manifest','cloud-bundle','cloud-readiness','cloud-healthcheck','cloud-inventory','cloud-reuse-plan','cloud-reuse-run','sf10-preflight','worker-serve','cloud-execute','loopback-validate'],default='inspect')
    parser.add_argument('--project-root',type=Path,default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--aws-profile',default=None,help='Optional AWS SSO profile for read-only cloud-inventory')
    parser.add_argument('--cloud-config',type=Path,help='Shared worker endpoints/runtime config JSON')
    parser.add_argument('--worker-site',choices=('aws','azure','gcp'))
    parser.add_argument('--worker-bind',default='127.0.0.1')
    parser.add_argument('--worker-port',type=int,default=0)
    parser.add_argument('--ack-cloud-cost',action='store_true',help='Explicitly permit use of existing live cloud endpoints')
    parser.add_argument('--batch-management-rpc',action='store_true',help='M6I opt-in: batch consecutive same-provider management RPCs to reduce Azure Run Command overhead')
    parser.add_argument('--candidate-index',type=int,default=0)
    parser.add_argument('--max-filters',type=int,choices=(0,1,2),default=2)
    parser.add_argument('--sf',type=int,choices=(1,10,100),default=1)
    parser.add_argument('--calibration-dir',type=Path,help='Existing locally executed calibration JSONs for --mode select')
    parser.add_argument('--data-dir',type=Path,help='Local Parquet SF directory (only --mode local)')
    parser.add_argument('--warmups',type=int,default=2,help='M6J measured cloud pairs: 0..2 warmup pairs')
    parser.add_argument('--repeats',type=int,default=10,help='M6J measured cloud pairs: 1..20; one pair runs both candidates')
    parser.add_argument('--seed',type=int,default=17)
    parser.add_argument('--run-id',default='m2preview')
    parser.add_argument('--cloud-data-root',default=None,help='Default: /opt/egressopt/data/tpch_sf{sf}')
    parser.add_argument('--cloud-runtime-root',default='/opt/egressopt/runtime/phase2')
    parser.add_argument('--write',action='store_true',help='Write NEW Phase2-only compile or cloud-bundle files, never overwrite')
    args=parser.parse_args()
    if args.mode=='worker-serve':
        if not args.cloud_config or not args.worker_site: parser.error('--worker-serve requires --cloud-config and --worker-site')
        # Workers must carry unmodified locked Phase 1 runner+SQL files too.
        # Prevent launching an apparently healthy VM with different source SQL.
        verify_source(args.project_root.resolve())
        _m6d_signed_worker(args.cloud_config,args.worker_site,args.worker_bind,args.worker_port)
        return 0
    project=args.project_root.resolve()
    sql,edges=verify_source(project)
    flt,specs=candidate_specs(edges,args.max_filters)
    report={'query':QUERY,'phase':'M2','status':'PARTIAL_SQL_DAG_COMPILED_LOCAL_SMOKE',
            'source_guard':'PASS','source_sql_sha256':LOCKED_SQL_SHA256,
            'phase1_runner_sha256':ORIGINAL_RUNNER_SHA256,
            'directed_semijoin_filters':len(flt),'candidate_physical_plan_specs':len(specs),
            'scale_factor_verified_on_cloud':[1],'scale_factors_planned':[1,10,100],
            'delta_grid':[0,0.2,0.5], 'unmeasured':True}
    if args.mode=='enumerate':
        report['directed_filters']=flt
        report['candidates_by_finish_site']={dest:sum(1 for p in specs if p['final_site']==dest)
                                              for dest in sorted(set(PLACEMENT.values()))}
    elif args.mode=='diagnostic':
        report['diagnostic_pair']=diag_candidates(edges,args.max_filters)
        report['interpretation']='Diagnostic pair only: compare equal-site projection-only baseline with orders->lineitem filter. NOT optimizer-selected, no WAN result.'
    elif args.mode=='compile':
        report['compiled_plan']=physical_plan(project,sql,edges,args.candidate_index,
                         sf=args.sf,run_id=args.run_id,max_filters=args.max_filters)
    elif args.mode=='selftest':
        report['local_semantic_smoke']=smoke_test(sql,edges,project,args.max_filters)
    elif args.mode=='evaluate':
        pair=[0,4] if QUERY=='Q03' else [0,6]
        report['evaluation']=repeated_local_evaluation(project,sql,edges,pair,args.sf,args.max_filters,
                       data_dir=args.data_dir,warmups=args.warmups,repeats=args.repeats,seed=args.seed)
        report['status']='M5_LOCAL_REPEATED_EVALUATION'
    elif args.mode=='select':
        report['selection']=select_from_local_calibration(project,QUERY,specs,DELTAS,args.calibration_dir,args.sf)
        report['status']='M4_PROVISIONAL_LOCAL_CALIBRATION_SELECTION'
    elif args.mode=='cloud-inventory':
        report['inventory']=_m6g_cloud_inventory(args.aws_profile)
        report['status']='M6G_READ_ONLY_INFRA_AUDIT_NO_EXECUTION'
        report['unmeasured']=True
    elif args.mode=='cloud-dry-run':
        report['cloud_dry_run']=cloud_dry_run(project,sql,edges,args.candidate_index,args.sf,args.max_filters,args.run_id)
        report['status']='M6A_STATIC_DRY_RUN_CLOUD_NOT_READY'
    elif args.mode=='loopback-validate':
        report['loopback_validation']=_m6d_loopback_validate(project,sql,edges,args.sf,args.max_filters,args.data_dir)
        report['status']='M6D_REAL_DUCKDB_LOOPBACK_NOT_CLOUD'
        report['unmeasured']=True  # No real inter-cloud evidence, even though local HTTP is measured.
    elif args.mode in ('cloud-readiness','cloud-healthcheck'):
        if not args.cloud_config: parser.error('--mode '+args.mode+' requires --cloud-config')
        report['readiness']=_m6f_readiness(project,sql,edges,args.sf,args.max_filters,args.run_id,
                     args.cloud_config,online=args.mode=='cloud-healthcheck')
        report['status']='M6F_CLOUD_READINESS_'+('PASS' if report['readiness']['ready_for_cloud'] else 'BLOCKED')
        report['unmeasured']=True
    elif args.mode=='cloud-execute':
        if not args.ack_cloud_cost or not args.cloud_config:
            parser.error('cloud-execute requires --cloud-config and --ack-cloud-cost')
        ready=_m6f_readiness(project,sql,edges,args.sf,args.max_filters,args.run_id,args.cloud_config,online=True)
        if not ready['ready_for_cloud']:
            raise RuntimeError('Cloud execution refused: '+ '; '.join(ready['blockers']))
        report['readiness']=ready
        report['cloud_execution']=_m6d_remote_execute(project,sql,edges,args.candidate_index,args.sf,args.max_filters,args.run_id,args.cloud_config)
        report['status']='M6D_REMOTE_EXECUTION_REFERENCE_CORRECTNESS_UNVERIFIED'
    elif args.mode=='cloud-reuse-plan':
        report['phase1_reuse']=_m6h_reuse_plan(project,sql,edges,args.sf,args.max_filters,args.run_id)
        report['status']='M6H_FROZEN_PHASE1_REUSE_PLAN_NO_EXECUTION'
    elif args.mode=='sf10-preflight':
        if args.sf!=10:raise RuntimeError('M6M sf10-preflight must use --sf 10')
        report['sf10_evidence']=_m6m_sf10_evidence_gate(project)
        reference=_m6h_local_reference(project,sql,args.sf)
        report['sf10_reference_signature_sha256']=hashlib.sha256(json.dumps(reference,sort_keys=True).encode()).hexdigest()
        report['sf10_reference_rows']=len(reference)
        # Materialize BOTH frozen candidates locally before any paid cloud SF10
        # experiment. This checks actual candidate SQL equivalence and artifact
        # caps, but local file copies are not claimed as provider WAN payloads.
        pilot_candidates=[0,4] if QUERY=='Q03' else [0,6]
        report['sf10_local_plan_gates']=[]
        for idx in pilot_candidates:
            check=local_duckdb_execution(project,sql,edges,idx,args.sf,args.max_filters)
            if check['correct_vs_reference'] is not True:
                raise RuntimeError('M6M SF10 local plan/reference mismatch: '+str(idx))
            oversized=[s['id'] for s in check['steps']
                       if s['artifact_bytes']>500_000_000]
            if oversized:
                raise RuntimeError('M6M SF10 artifact cap 500MB exceeded in '+str(idx)+': '+str(oversized))
            report['sf10_local_plan_gates'].append({
                'candidate_index':idx,'correct_vs_original_sql':True,
                'simulated_local_copy_bytes':check['simulated_intermediate_copy_bytes'],
                'max_artifact_bytes':max(x['artifact_bytes'] for x in check['steps']),
                'stage_count':len(check['steps']),
                'note':'Single-host local staging, not cloud performance evidence'})
        report['status']='M6M_SF10_LOCAL_READY_CLOUD_NOT_EXECUTED'
        report['unmeasured']=True
    elif args.mode=='cloud-reuse-run':
        report['cloud_reuse_run']=_m6h_cloud_run(project,sql,edges,args.sf,args.max_filters,args.run_id,args.warmups,args.repeats,args.ack_cloud_cost,args.batch_management_rpc)
        report['status']='M6H_CLOUD_RUN_RETURNED'
    elif args.mode=='cloud-bundle':
        report['deployment_bundle']=_m6e_build_cloud_bundle(project,sql,edges,args.sf,args.max_filters,
                         args.run_id,args.cloud_data_root or f'/opt/egressopt/data/tpch_sf{args.sf}',args.cloud_runtime_root)
        report['status']='M6E_OFFLINE_DEPLOYMENT_TEMPLATE_NOT_CLOUD_EXECUTED'
    elif args.mode=='cloud-manifest':
        report['stage_manifest']=cloud_stage_manifest(project,sql,edges,args.candidate_index,args.sf,args.max_filters,args.run_id)
        report['status']='M6C_AUTHENTICATED_TRANSPORT_API_NOT_CLOUD_VALIDATED'
    elif args.mode=='cloud-preflight':
        report['cloud_preflight']=cloud_worker_compatibility(project,sql,edges,args.candidate_index,
                                                          args.sf,args.max_filters,args.run_id)
        report['status']='M6B_WORKER_COMPATIBILITY_NOT_CLOUD_EXECUTION'
    elif args.mode=='local':
        report['local_duckdb']=local_duckdb_execution(project,sql,edges,args.candidate_index,
                                                       args.sf,args.max_filters,data_dir=args.data_dir)
    if args.write and args.mode=='cloud-bundle':
        outdir=project/'results'/'phase2'/'m6e'
        outdir.mkdir(parents=True,exist_ok=True)
        config_out=outdir/f'{QUERY.lower()}_{args.run_id}_cloud_config_TEMPLATE.json'
        report_out=outdir/f'{QUERY.lower()}_{args.run_id}_deployment_manifest.json'
        if config_out.exists() or report_out.exists():
            raise FileExistsError('Refusing to overwrite existing Phase2 M6e output; use a new --run-id')
        config_out.write_text(json.dumps(report['deployment_bundle']['cloud_config_template'],indent=2)+'\n')
        report['cloud_config_template_file']=str(config_out)
        report['deployment_manifest_file']=str(report_out)
        report_out.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
    elif args.write:
        if args.mode!='compile':
            raise ValueError('--write currently supported only for --mode compile or cloud-bundle')
        out=project/'results'/'phase2'/QUERY.lower()/('m2_candidate_%03d_sf%d_%s.json'%
                           (args.candidate_index,args.sf,args.run_id))
        if out.exists():raise FileExistsError(f'Refusing to overwrite existing result: {out}')
        out.parent.mkdir(parents=True,exist_ok=True)
        out.write_text(json.dumps(report,indent=2)+'\n',encoding='utf8')
        report['new_file_created']=str(out)
        # limit printout; inspect generated JSON directly
        del report['compiled_plan']
    print(json.dumps(report,indent=2,sort_keys=True))
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except (OSError,ValueError,RuntimeError,AssertionError) as exc:
        print('PHASE2_M2_FAIL:',exc,file=sys.stderr)
        sys.exit(2)
