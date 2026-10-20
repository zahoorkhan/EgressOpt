
import duckdb
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/tpch/tpch_sf1.duckdb"
PARQUET_DIR = ROOT / "data/tpch/parquet_sf1"
QUERY_DIR = ROOT / "queries/tpch"

TABLES = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem"
]

QUERIES = [1, 6, 12]

# Reference database
db = duckdb.connect(str(DB_PATH), read_only=True)

# Independent connection reading only Parquet
pq = duckdb.connect(":memory:")

for table in TABLES:
    file = PARQUET_DIR / f"{table}.parquet"
    path = str(file).replace("'", "''")

    pq.execute(
        f"CREATE VIEW {table} AS "
        f"SELECT * FROM read_parquet('{path}')"
    )

def equal_value(a, b):
    if a is None or b is None:
        return a is None and b is None

    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) or isinstance(b, float):
            return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-8)

    return a == b

def results_equal(ref_rows, pq_rows):
    # These queries have either one row or explicit ORDER BY.
    if len(ref_rows) != len(pq_rows):
        return False

    for ref_row, pq_row in zip(ref_rows, pq_rows):
        if len(ref_row) != len(pq_row):
            return False
        for a, b in zip(ref_row, pq_row):
            if not equal_value(a, b):
                return False

    return True

print("\nEGRESSOPT — PARQUET QUERY EQUIVALENCE")
print("-" * 60)

all_pass = True

for query_id in QUERIES:
    name = f"q{query_id:02d}"
    sql = (QUERY_DIR / f"{name}.sql").read_text()

    reference = db.execute(sql).fetchall()
    parquet = pq.execute(sql).fetchall()

    passed = results_equal(reference, parquet)
    all_pass &= passed

    print(
        f"{name.upper():<8} "
        f"Reference rows: {len(reference):<5} "
        f"Parquet rows: {len(parquet):<5} "
        f"{'PASS' if passed else 'FAIL'}"
    )

db.close()
pq.close()

print("-" * 60)
print("FINAL STATUS:", "VERIFIED" if all_pass else "REVIEW REQUIRED")

if not all_pass:
    raise SystemExit(1)