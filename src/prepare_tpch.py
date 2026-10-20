"""Generate, verify, and snapshot DuckDB TPC-H SF1 research dataset.
Run from EgressOpt project root: python src/prepare_tpch.py
"""
import hashlib
import json
import platform
import time
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data' / 'tpch'
QUERIES = ROOT / 'queries' / 'tpch'
REPORTS = ROOT / 'results' / 'dataset_validation'
for path in (DATA, QUERIES, REPORTS):
    path.mkdir(parents=True, exist_ok=True)

DB = DATA / 'tpch_sf1.duckdb'
if DB.exists():
    raise SystemExit(f'Refusing to overwrite existing database: {DB}. Move it aside to regenerate.')

expected = {
    'region': 5, 'nation': 25, 'supplier': 10_000,
    'customer': 150_000, 'part': 200_000, 'partsupp': 800_000,
    'orders': 1_500_000,
}
print('Generating TPC-H SF1:', DB, flush=True)
start = time.perf_counter()
con = duckdb.connect(str(DB))
try:
    con.execute('INSTALL tpch')
    con.execute('LOAD tpch')
    con.execute('CALL dbgen(sf = 1)')
    timings = {'generation_seconds': round(time.perf_counter() - start, 3)}
    counts = {}
    for table in ['region','nation','supplier','customer','part','partsupp','orders','lineitem']:
        counts[table] = con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        if table in expected and counts[table] != expected[table]:
            raise AssertionError(f'{table} expected {expected[table]} got {counts[table]}')
        print(f'{table:10} {counts[table]:>12,}', flush=True)
    if not (5_000_000 <= counts['lineitem'] <= 7_000_000):
        raise AssertionError('LINEITEM count outside expected approximate SF1 range')
    exported = {}
    rows = con.execute('SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr').fetchall()
    if len(rows) != 22:
        raise AssertionError(f'Expected 22 SQL query templates; found {len(rows)}')
    for number, sql in rows:
        path = QUERIES / f'q{number:02}.sql'
        path.write_text(sql.strip().rstrip(';') + ';\n', encoding='utf-8')
        exported[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    con.execute('CHECKPOINT')
finally:
    con.close()

manifest = {
    'generated_at_utc': datetime.now(timezone.utc).isoformat(),
    'benchmark': 'DuckDB TPC-H extension SF1 (TPC-H-derived; not TPC-certified)',
    'scale_factor': 1,
    'duckdb_version': duckdb.__version__,
    'python_version': platform.python_version(),
    'machine_architecture': platform.machine(),
    'database_path': str(DB.relative_to(ROOT)),
    'database_bytes': DB.stat().st_size,
    'database_sha256': None,
    'row_counts': counts,
    'query_sql_sha256': exported,
    'generation_seconds': timings['generation_seconds'],
    'expected_counts_verified': True,
}
manifest_path = REPORTS / 'tpch_sf1_manifest.json'
manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
print('Saved:', manifest_path)
print('Exported:', len(exported), 'SQL files in', QUERIES)
print('STATUS: SUCCESS')
