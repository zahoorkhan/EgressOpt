#!/usr/bin/env python3
"""Prepare TPC-H-derived SF1/SF10 data for EgressOpt without touching SQL or cloud.

SF1: DuckDB TPC-H extension -> local .duckdb -> Parquet.
SF10: gregrahn/tpch-kit DBGEN -> .tbl -> existing strict Parquet converter.
All data and generated provenance files are ignored by Git.

    python tools/setup_tpch_data.py --scale 1
    python tools/setup_tpch_data.py --scale 10
    python tools/setup_tpch_data.py --scale 10 --dbgen-dir /path/to/tpch-kit/dbgen
    python tools/setup_tpch_data.py --scale 1 --check
    python tools/setup_tpch_data.py --scale 10 --check
    python tools/setup_tpch_data.py --scale 10 --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "tpch"
REPORTS = ROOT / "results" / "dataset_validation"
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
# Official TPC-H cardinalities (for scale-dependent tables) and fixed dimensions.
ROW_COUNTS = {
    1: {"region": 5, "nation": 25, "supplier": 10_000, "customer": 150_000,
        "part": 200_000, "partsupp": 800_000, "orders": 1_500_000,
        "lineitem": 6_001_215},
    10: {"region": 5, "nation": 25, "supplier": 100_000, "customer": 1_500_000,
         "part": 2_000_000, "partsupp": 8_000_000, "orders": 15_000_000},
}
KIT_URL = "https://github.com/gregrahn/tpch-kit.git"


def execute(args: list[str], *, cwd: Path | None = None, env: dict | None = None) -> None:
    print("$", " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd, env=env, check=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_parquet(scale: int, *, expected_schema: dict | None = None) -> dict:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit("DuckDB is missing: python -m pip install -r requirements.txt") from exc
    out = DATA / f"parquet_sf{scale}"
    results = {}
    with duckdb.connect(":memory:") as con:
        for table in TABLES:
            path = out / f"{table}.parquet"
            if not path.is_file():
                raise RuntimeError(f"Missing Parquet file: {path}. Complete dataset generation first.")
            actual = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
            expected = ROW_COUNTS[scale].get(table)
            if expected is not None and actual != expected:
                raise RuntimeError(f"{table} SF{scale}: row count {actual:,}; expected {expected:,}")
            if table == "lineitem" and scale == 10 and not (55_000_000 <= actual <= 65_000_000):
                raise RuntimeError(f"SF10 lineitem cardinality {actual:,} outside expected bounds")
            schema = [(str(r[0]), str(r[1])) for r in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]
            if expected_schema is not None and schema != expected_schema[table]:
                raise RuntimeError(f"SF{scale} {table}: schema differs from SF1 reference")
            results[table] = {"file": str(path.relative_to(ROOT)), "rows": actual,
                              "bytes": path.stat().st_size, "sha256": sha256_file(path),
                              "schema": [{"name": name, "type": typ} for name, typ in schema]}
            print(f"  SF{scale} {table:<10} {actual:>12,} rows  PASS", flush=True)
    return results


def sf1_generate() -> None:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit("DuckDB is missing: python -m pip install -r requirements.txt") from exc
    DATA.mkdir(parents=True, exist_ok=True)
    db = DATA / "tpch_sf1.duckdb"
    temporary = DATA / ".tpch_sf1_build.duckdb"
    if not db.is_file():
        if temporary.exists():
            raise RuntimeError(f"Incomplete prior SF1 generation found: {temporary}. Inspect/remove it first.")
        print("Generating SF1 with DuckDB's TPC-H extension (first run may download extension)...", flush=True)
        try:
            with duckdb.connect(str(temporary)) as con:
                con.execute("INSTALL tpch")
                con.execute("LOAD tpch")
                con.execute("CALL dbgen(sf = 1)")
                for table, expected in ROW_COUNTS[1].items():
                    actual = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    if actual != expected:
                        raise RuntimeError(f"SF1 {table}: expected {expected:,}; generated {actual:,}")
                con.execute("CHECKPOINT")
            temporary.replace(db)
        except Exception:
            for extra in (temporary, Path(str(temporary) + ".wal")):
                extra.unlink(missing_ok=True)
            raise
    else:
        print("Reusing existing SF1 database (no overwrite):", db, flush=True)
    out = DATA / "parquet_sf1"
    out.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db), read_only=True) as con:
        for table, expected in ROW_COUNTS[1].items():
            actual = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if actual != expected:
                raise RuntimeError(f"SF1 source database {table}: unexpected row count {actual:,}")
        for table in TABLES:
            file = out / f"{table}.parquet"
            if file.exists():
                print("Keeping existing Parquet:", file.name, flush=True)
                continue
            temp = out / f".{table}.parquet.part"
            if temp.exists():
                raise RuntimeError(f"Stale partial export: {temp}. Inspect/remove it first.")
            try:
                # Fixed table identifiers. No input SQL or query-file modification.
                con.execute(f"COPY (SELECT * FROM {table}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                            [str(temp)])
                temp.replace(file)
            except Exception:
                temp.unlink(missing_ok=True)
                raise
    tables = validate_parquet(1)
    REPORTS.mkdir(parents=True, exist_ok=True)
    record = {"dataset": "TPC-H-derived", "scale_factor": 1,
              "generation": "DuckDB tpch extension; CALL dbgen(sf = 1)",
              "duckdb_version": duckdb.__version__,
              "timestamp_utc": datetime.now(timezone.utc).isoformat(),
              "python_version": platform.python_version(), "tables": tables}
    (REPORTS / "tpch_sf1_setup_manifest.json").write_text(json.dumps(record, indent=2) + "\n")
    print("EGRESSOPT_TPCH_SF1_PASS", flush=True)


def resolve_dbgen(specified: Path | None) -> tuple[Path, str | None]:
    if specified is None:
        kit = DATA / "tools" / "tpch-kit"
        if not kit.is_dir():
            kit.parent.mkdir(parents=True, exist_ok=True)
            execute(["git", "clone", "--depth", "1", KIT_URL, str(kit)])
        directory = kit / "dbgen"
    else:
        directory = specified.expanduser().resolve()
        if directory.is_file() and directory.name == "dbgen":
            directory = directory.parent
        if (directory / "dbgen").is_dir():
            directory = directory / "dbgen"
    if not (directory / "Makefile").is_file():
        raise RuntimeError(f"DBGEN directory or Makefile missing: {directory}")
    exe = directory / "dbgen"
    if not exe.is_file():
        system = platform.system()
        machine = {"Darwin": "MACOS", "Linux": "LINUX"}.get(system)
        if not machine:
            raise RuntimeError("DBGEN build supported by this helper only on macOS/Linux")
        execute(["make", f"MACHINE={machine}", "DATABASE=POSTGRESQL"], cwd=directory)
    if not exe.is_file():
        raise RuntimeError(f"DBGEN binary missing after build: {exe}")
    try:
        revision = subprocess.check_output(["git", "-C", str(directory), "rev-parse", "HEAD"],
                                           text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    return directory, revision


def sf10_generate(specified: Path | None) -> None:
    # The existing, validated SF10 converter derives column names and types from SF1.
    sf1 = DATA / "parquet_sf1"
    missing = [name for name in TABLES if not (sf1 / f"{name}.parquet").is_file()]
    if missing:
        raise RuntimeError("SF10 requires SF1 Parquet schemas. First run: "
                           "python tools/setup_tpch_data.py --scale 1")
    raw = DATA / "raw_sf10"
    raw.mkdir(parents=True, exist_ok=True)
    source = [raw / f"{table}.tbl" for table in TABLES]
    present = [p for p in source if p.exists()]
    if 0 < len(present) < len(source):
        raise RuntimeError("Partial SF10 raw DBGEN files detected; do not mix runs or overwrite. "
                           "Inspect data/tpch/raw_sf10/ before retrying.")
    directory, revision = resolve_dbgen(specified)
    if not present:
        env = dict(os.environ)
        env.update({"DSS_CONFIG": str(directory), "DSS_QUERY": str(directory / "queries"),
                    "DSS_PATH": str(raw)})
        print("Generating SF10 .tbl files; allow substantial disk space and time...", flush=True)
        execute([str(directory / "dbgen"), "-v", "-s", "10"], cwd=directory, env=env)
    if not all(p.is_file() and p.stat().st_size > 0 for p in source):
        raise RuntimeError("Incomplete SF10 DBGEN output. Inspect raw_sf10/; refusing conversion.")
    execute([sys.executable, str(ROOT / "src" / "convert_tpch_sf10.py")], cwd=ROOT)
    # Read-only validate exported data with schema matched to generated SF1.
    sf1_schema = {}
    import duckdb
    with duckdb.connect(":memory:") as con:
        for table in TABLES:
            rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)",
                               [str(sf1 / f"{table}.parquet")]).fetchall()
            sf1_schema[table] = [(str(r[0]), str(r[1])) for r in rows]
    tables = validate_parquet(10, expected_schema=sf1_schema)
    REPORTS.mkdir(parents=True, exist_ok=True)
    record = {"dataset": "DBGEN TPC-H-derived SF10", "scale_factor": 10,
              "generator": KIT_URL if specified is None else "user-supplied tpch-kit/dbgen",
              "git_revision": revision, "dbgen_command": "dbgen -v -s 10",
              "conversion_script": "src/convert_tpch_sf10.py",
              "duckdb_version": duckdb.__version__,
              "timestamp_utc": datetime.now(timezone.utc).isoformat(),
              "tables": tables}
    (REPORTS / "tpch_sf10_setup_manifest.json").write_text(json.dumps(record, indent=2) + "\n")
    print("EGRESSOPT_TPCH_SF10_PASS", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate/validate local TPC-H data at SF1 or SF10. No cloud calls.")
    parser.add_argument("--scale", type=int, choices=(1, 10), required=True)
    parser.add_argument("--dbgen-dir", type=Path,
                        help="Optional existing tpch-kit checkout or dbgen directory (SF10 only)")
    parser.add_argument("--dry-run", action="store_true", help="Show prerequisites/output paths; no imports, downloads or writes")
    parser.add_argument("--check", action="store_true", help="Verify existing Parquet data (read-only), without generating it")
    args = parser.parse_args(argv)
    if args.dbgen_dir and args.scale != 10:
        parser.error("--dbgen-dir applies only to --scale 10")
    if args.dry_run and args.check:
        parser.error("--dry-run and --check are mutually exclusive")
    if args.dry_run:
        print(f"TPC-H SF{args.scale} local preparation (DRY RUN; no network, cloud, or writes)")
        if args.scale == 1:
            print("Input: DuckDB TPC-H extension; CALL dbgen(sf = 1)")
            print("Local database:", DATA / "tpch_sf1.duckdb")
        else:
            print("Prerequisite:", DATA / "parquet_sf1" / "*.parquet")
            print("DBGEN source:", args.dbgen_dir if args.dbgen_dir else KIT_URL)
            print("DBGEN flags: -v -s 10; raw destination:", DATA / "raw_sf10")
        print("Output Parquet:", DATA / f"parquet_sf{args.scale}")
        print("Manifest:", REPORTS / f"tpch_sf{args.scale}_setup_manifest.json")
        print("Published SQL files are never changed. No provider VMs are accessed.")
        return 0
    if args.check:
        tables = validate_parquet(args.scale)
        if args.scale == 10:
            import duckdb
            with duckdb.connect(":memory:") as con:
                for table in TABLES:
                    source = DATA / "parquet_sf1" / f"{table}.parquet"
                    if not source.exists():
                        raise RuntimeError(f"SF1 reference schema missing: {source}")
                    rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(source)]).fetchall()
                    types = [(str(r[0]), str(r[1])) for r in rows]
                    exported = [(x["name"], x["type"]) for x in tables[table]["schema"]]
                    if types != exported:
                        raise RuntimeError(f"SF10 {table}: schema differs from SF1")
        print(f"EGRESSOPT_TPCH_SF{args.scale}_CHECK_PASS")
    elif args.scale == 1:
        sf1_generate()
    else:
        sf10_generate(args.dbgen_dir)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"EGRESSOPT_TPCH_SETUP_FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
