
import duckdb
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data/tpch/tpch_sf1.duckdb"
OUT = ROOT / "data/tpch/parquet_sf1"
MANIFEST = ROOT / "results/dataset_validation/parquet_sf1_manifest.json"

TABLES = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem"
]

if not DB.is_file():
    raise SystemExit(f"Database not found: {DB}")

OUT.mkdir(parents=True, exist_ok=True)
MANIFEST.parent.mkdir(parents=True, exist_ok=True)

# Refuse partial overwrites. Avoid mixing different exports.
existing = [OUT / f"{table}.parquet" for table in TABLES]
if any(path.exists() for path in existing):
    raise SystemExit(
        "Parquet files already exist. Inspect them before "
        "creating a new dataset export."
    )

con = duckdb.connect(str(DB), read_only=True)
con.execute("SET threads = 4")

manifest = {
    "dataset": "TPC-H-derived",
    "scale_factor": 1,
    "generated_by": "DuckDB TPC-H extension",
    "duckdb_version": duckdb.__version__,
    "architecture": platform.machine(),
    "created_utc": datetime.now(timezone.utc).isoformat(),
    "compression": "zstd",
    "tables": {}
}

print("EGRESSOPT — TPC-H PARQUET EXPORT")

for table in TABLES:
    path = OUT / f"{table}.parquet"

    # Table names come from the fixed list above.
    con.execute(
        f"COPY (SELECT * FROM {table}) TO ? "
        "(FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(path)]
    )

    original_count = con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]

    parquet_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(path)]
    ).fetchone()[0]

    assert original_count == parquet_count, (
        f"Row-count mismatch: {table}"
    )

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    manifest["tables"][table] = {
        "rows": original_count,
        "file": path.name,
        "bytes": path.stat().st_size,
        "sha256": digest.hexdigest()
    }

    print(
        f"{table:<12} "
        f"{original_count:>12,} rows "
        f"{path.stat().st_size / 1048576:>9.2f} MiB PASS"
    )

MANIFEST.write_text(json.dumps(manifest, indent=2))
con.close()

print("\nSTATUS: EXPORT VERIFIED")
print("Manifest:", MANIFEST)