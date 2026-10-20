
import duckdb
import hashlib
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/tpch/tpch_sf1.duckdb"
QUERY_DIR = ROOT / "queries/tpch"
OUTPUT_DIR = ROOT / "results/reference/sf1"
QUERY_IDS = [1, 6, 12]

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if not DB_PATH.exists():
    raise FileNotFoundError(f"Database not found: {DB_PATH}")

con = duckdb.connect(str(DB_PATH), read_only=True)
con.execute("SET threads = 4")

metadata = {
    "dataset": "TPC-H-derived SF1",
    "database": str(DB_PATH),
    "duckdb_version": duckdb.__version__,
    "python_version": sys.version.split()[0],
    "architecture": platform.machine(),
    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "thread_count": 4,
    "queries": {}
}

print("\nEGRESSOPT — LOCAL REFERENCE EXECUTION")
print("=" * 60)

for query_id in QUERY_IDS:
    name = f"q{query_id:02d}"
    path = QUERY_DIR / f"{name}.sql"

    sql = path.read_text(encoding="utf-8")
    sql_hash = hashlib.sha256(sql.encode()).hexdigest()

    print(f"\nExecuting {name.upper()}...")

    start = time.perf_counter()
    cursor = con.execute(sql)
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - start

    columns = [column[0] for column in cursor.description]

    # Deterministic fingerprint for this local reference snapshot.
    # The exact comparison policy for distributed floating-point
    # results will be defined separately.
    serialized = json.dumps(
        {"columns": columns, "rows": rows},
        default=str,
        ensure_ascii=False
    )
    result_hash = hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()

    metadata["queries"][name] = {
        "sql_sha256": sql_hash,
        "result_sha256": result_hash,
        "row_count": len(rows),
        "elapsed_seconds": round(elapsed, 6),
        "columns": columns
    }

    print(f"Rows returned : {len(rows)}")
    print(f"Execution time: {elapsed:.6f} seconds")
    print(f"Result SHA256 : {result_hash}")

    output = OUTPUT_DIR / f"{name}_results.json"
    output.write_text(
        json.dumps(
            {"columns": columns, "rows": rows},
            default=str,
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

manifest = OUTPUT_DIR / "reference_manifest.json"
manifest.write_text(
    json.dumps(metadata, indent=2),
    encoding="utf-8"
)

con.close()

print("\n" + "=" * 60)
print("STATUS: SUCCESS")
print(f"Manifest: {manifest}")