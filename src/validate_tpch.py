"""Read-only revalidation of existing SF1 DB and sample reference SQL queries."""
import json
import time
from pathlib import Path
import duckdb
ROOT = Path(__file__).resolve().parents[1]
db = ROOT / 'data/tpch/tpch_sf1.duckdb'
manifest_path = ROOT / 'results/dataset_validation/tpch_sf1_manifest.json'
manifest = json.loads(manifest_path.read_text())
con = duckdb.connect(str(db), read_only=True)
try:
    for table, expected in manifest['row_counts'].items():
        actual = con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        if actual != expected:
            raise AssertionError(f'Count mismatch for {table}: {actual} != {expected}')
    for number in (1, 6, 12):
        sql_path = ROOT / f'queries/tpch/q{number:02}.sql'
        sql = sql_path.read_text()
        t0 = time.perf_counter()
        result = con.execute(sql).fetchall()
        print(f'Q{number:02}: {len(result)} result rows; {time.perf_counter()-t0:.3f}s')
    print('STATUS: VERIFIED')
finally:
    con.close()
