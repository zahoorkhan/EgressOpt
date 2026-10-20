
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import platform
import sys
import duckdb

ROOT = Path(__file__).resolve().parents[1]

DB = ROOT / "data/tpch/tpch_sf1.duckdb"
PARQUET = ROOT / "data/tpch/parquet_sf1"
QUERIES = ROOT / "queries/tpch"
REFERENCE = ROOT / "results/reference/sf1"
OUTPUT = ROOT / "results/dataset_validation"

TABLES = {
    "region": 5,
    "nation": 25,
    "supplier": 10000,
    "customer": 150000,
    "part": 200000,
    "partsupp": 800000,
    "orders": 1500000,
    "lineitem": 6001215,
}

OUTPUT.mkdir(parents=True, exist_ok=True)

def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(8 * 1024 * 1024), b""
        ):
            digest.update(chunk)
    return digest.hexdigest()

def relative_path(path):
    return str(path.relative_to(ROOT))

if not DB.is_file():
    raise SystemExit(f"Missing database: {DB}")

con = duckdb.connect(str(DB), read_only=True)

manifest = {
    "project": "EgressOpt",
    "dataset": "DuckDB-generated TPC-H-derived SF1",
    "scale_factor": 1,
    "created_utc": datetime.now(
        timezone.utc
    ).isoformat(),
    "software": {
        "python": sys.version.split()[0],
        "duckdb": duckdb.__version__,
        "architecture": platform.machine(),
    },
    "database": {
        "file": relative_path(DB),
        "sha256": sha256_file(DB),
    },
    "tables": {},
    "queries": {},
    "reference_results": {},
    "placement": {},
}

print("\nSTEP 5A — RESEARCH MANIFEST")
print("=" * 55)

# 1. Verify and fingerprint all eight tables.
for table, expected in TABLES.items():
    pq = PARQUET / f"{table}.parquet"

    if not pq.is_file():
        raise SystemExit(f"Missing Parquet: {pq}")

    db_count = con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]

    pq_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(pq)]
    ).fetchone()[0]

    db_schema = [
        (r[0], r[1])
        for r in con.execute(
            f"DESCRIBE SELECT * FROM {table}"
        ).fetchall()
    ]

    pq_schema = [
        (r[0], r[1])
        for r in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(pq)]
        ).fetchall()
    ]

    if db_count != expected or pq_count != expected:
        raise SystemExit(
            f"Row-count validation failed: {table}"
        )

    if db_schema != pq_schema:
        raise SystemExit(
            f"Schema validation failed: {table}"
        )

    manifest["tables"][table] = {
        "rows": db_count,
        "parquet_file": relative_path(pq),
        "parquet_bytes": pq.stat().st_size,
        "parquet_sha256": sha256_file(pq),
        "schema": db_schema,
    }

    print(f"{table:<12} {db_count:>12,}  PASS")

# 2. Fingerprint the 22 query files.
for number in range(1, 23):
    qid = f"q{number:02d}"
    file = QUERIES / f"{qid}.sql"

    if not file.is_file():
        raise SystemExit(f"Missing query: {file}")

    manifest["queries"][qid] = {
        "file": relative_path(file),
        "sha256": sha256_file(file),
        "status": (
            "validated locally"
            if number in (1, 6, 12)
            else "not yet validated locally"
        ),
    }

print("\nSQL FILES:", len(manifest["queries"]))

# 3. Preserve existing local reference measurements.
ref_manifest = REFERENCE / "reference_manifest.json"

if not ref_manifest.is_file():
    raise SystemExit(
        "Reference manifest missing. Do not invent runtimes."
    )

reference = json.loads(
    ref_manifest.read_text(encoding="utf-8")
)

for qid in ("q01", "q06", "q12"):
    result_file = REFERENCE / f"{qid}_results.json"

    if not result_file.is_file():
        raise SystemExit(
            f"Missing reference output: {result_file}"
        )

    record = reference["queries"][qid]

    if record["sql_sha256"] != (
        manifest["queries"][qid]["sha256"]
    ):
        raise SystemExit(
            f"SQL fingerprint mismatch: {qid}"
        )

    manifest["reference_results"][qid] = {
        "row_count": record["row_count"],
        "runtime_seconds": record["elapsed_seconds"],
        "result_sha256": record["result_sha256"],
        "output_file": relative_path(result_file),
        "output_file_sha256": sha256_file(result_file),
        "validation": "local database and Parquet PASS",
    }

    print(
        f"{qid.upper()}: "
        f"{record['row_count']} rows, "
        f"{record['elapsed_seconds']:.6f} s"
    )

# 4. Initial whole-dataset placement.
manifest["placement"] = {
    "P1_initial": {
        "aws": ["lineitem", "orders"],
        "azure": ["customer", "nation"],
        "gcp": [
            "supplier", "part", "partsupp", "region"
        ],
    },
    "Q12_scenarios": {
        "P1_colocated": {
            "orders": "aws",
            "lineitem": "aws",
        },
        "P2_split": {
            "orders": "azure",
            "lineitem": "aws",
        },
        "P3_rotated": {
            "orders": "gcp",
            "lineitem": "azure",
        },
        "P4_reverse": {
            "orders": "aws",
            "lineitem": "gcp",
        },
    },
}

con.close()

destination = OUTPUT / "egressopt_sf1_research_manifest.json"

# Do not silently overwrite an earlier research snapshot.
if destination.exists():
    raise SystemExit(
        f"Manifest already exists: {destination}. "
        "Archive it before generating another version."
    )

destination.write_text(
    json.dumps(manifest, indent=2),
    encoding="utf-8"
)

print("\nMANIFEST:", destination)
print("STATUS: SUCCESS")