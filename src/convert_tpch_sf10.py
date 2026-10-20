#!/usr/bin/env python3
"""Convert genuine DBGEN SF10 .tbl files to EgressOpt Parquet.

Run from EgressOpt project (after placing this file in src/):
    python src/convert_tpch_sf10.py

The existing SF1 Parquet *column names, ordering, and types* are used as
reference. Input CSV dialect: DBGEN pipe-separated, headerless; accepts a present or absent final pipe.
Does not modify original .tbl files, SF1 dataset, or Phase 1 code.
"""

import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data/tpch/raw_sf10'
SF1 = ROOT / 'data/tpch/parquet_sf1'
OUT = ROOT / 'data/tpch/parquet_sf10'
MANIFEST = ROOT / 'results/dataset_validation/parquet_sf10_manifest.json'
TABLES = ('region', 'nation', 'supplier', 'customer', 'part', 'partsupp', 'orders', 'lineitem')
EXACT_ROWS = {
    'region': 5, 'nation': 25, 'supplier': 100_000,
    'customer': 1_500_000, 'part': 2_000_000,
    'partsupp': 8_000_000, 'orders': 15_000_000,
}


def quoted(s):
    return "'" + str(s).replace("'", "''") + "'"


def ident(s):
    return '"' + s.replace('"', '""') + '"'


def schema(connection, parquet_file):
    rows = connection.execute(
        f'DESCRIBE SELECT * FROM read_parquet({quoted(parquet_file)})'
    ).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def check_input(path, expected_fields):
    """Detect both common DBGEN dialects, rejecting mixed or malformed samples.

    A TPC-H table of N columns has N-1 pipes if no trailing pipe is
    present, or N pipes when the last field is terminated by a pipe.
    """
    has_trailing_pipe = None
    with path.open('rb') as stream:
        for i in range(5):
            line = stream.readline()
            if not line:
                raise RuntimeError(f'{path.name}: unexpectedly short input at sample row {i + 1}')
            line = line.rstrip(b'\r\n')
            trailing = line.endswith(b'|')
            found = line.count(b'|')
            expected_pipes = expected_fields if trailing else expected_fields - 1
            if found != expected_pipes:
                raise RuntimeError(
                    f'{path.name}: malformed DBGEN row {i + 1}: '
                    f'{found} delimiters, expected {expected_pipes} '
                    f'for {expected_fields} data columns '
                    f'(trailing_pipe={trailing})'
                )
            if has_trailing_pipe is not None and trailing != has_trailing_pipe:
                raise RuntimeError(f'{path.name}: inconsistent trailing-pipe style in first five rows')
            has_trailing_pipe = trailing
    return has_trailing_pipe


def main():
    for table in TABLES:
        for path in (RAW / f'{table}.tbl', SF1 / f'{table}.parquet'):
            if not path.is_file():
                raise SystemExit(f'Missing required source: {path}')
    OUT.mkdir(parents=True, exist_ok=True)
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(':memory:')
    con.execute('SET threads = 4')
    report = {
        'dataset': 'DBGEN TPC-H-derived SF10',
        'scale_factor': 10,
        'generator': 'tpch-kit/dbgen -v -s 10',
        'conversion': 'DuckDB strict CSV read; DBGEN dialect autodetection; match SF1 Parquet names/types; ZSTD',
        'duckdb_version': duckdb.__version__,
        'platform': platform.platform(),
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'tables': {},
    }
    print('EGRESSOPT SF10: DBGEN .TBL -> PARQUET', flush=True)
    try:
        for table in TABLES:
            raw = RAW / f'{table}.tbl'
            sf1 = SF1 / f'{table}.parquet'
            target = OUT / f'{table}.parquet'
            temporary = OUT / f'.{table}.parquet.part'
            columns = schema(con, sf1)
            if not columns or any(name == '__eoq_trailer' for name, _ in columns):
                raise RuntimeError(f'Invalid reference schema for {table}')
            # Different DBGEN builds emit either N-1 or N delimiters for N fields.
            trailing_pipe = check_input(raw, len(columns))
            if not target.exists():
                if temporary.exists():
                    raise RuntimeError(f'Stale temporary file: {temporary}. Inspect/remove it before retry.')
                csv_columns = ', '.join(
                    f'{quoted(name)}: {quoted("VARCHAR")}' for name, _ in columns
                )
                if trailing_pipe:
                    csv_columns += f', {quoted("__eoq_trailer")}: {quoted("VARCHAR")}'
                casts = ', '.join(
                    f'CAST({ident(name)} AS {datatype}) AS {ident(name)}'
                    for name, datatype in columns
                )
                # Reject a populated extra field for trailing-pipe files.
                trailer_check = (
                    " WHERE CAST(CASE WHEN __eoq_trailer IS NULL OR __eoq_trailer = '' "
                    "THEN '0' ELSE 'INVALID_DBGEN_TRAILING_FIELD' END AS INTEGER) = 0"
                    if trailing_pipe else ''
                )
                sql = (
                    f'COPY (SELECT {casts} FROM read_csv({quoted(raw)}, '
                    "delim='|', header=false, auto_detect=false, "
                    "quote='', escape='', strict_mode=true, "
                    f'columns={{{csv_columns}}}){trailer_check}) '
                    f'TO {quoted(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD)'
                )
                print(f'Converting {table} ...', flush=True)
                try:
                    con.execute(sql)
                    if schema(con, temporary) != columns:
                        raise RuntimeError(f'{table}: Parquet schema differs from SF1')
                    count = con.execute(
                        f'SELECT COUNT(*) FROM read_parquet({quoted(temporary)})'
                    ).fetchone()[0]
                    if table in EXACT_ROWS and count != EXACT_ROWS[table]:
                        raise RuntimeError(
                            f'{table}: expected {EXACT_ROWS[table]:,} rows, got {count:,}'
                        )
                    if table == 'lineitem' and not 55_000_000 <= count <= 65_000_000:
                        raise RuntimeError(f'lineitem: unexpected SF10 count {count:,}')
                    temporary.rename(target)
                except Exception:
                    if temporary.is_file():
                        temporary.unlink()
                    raise
            else:
                print(f'Checking existing {table}.parquet (no overwrite) ...', flush=True)
                if schema(con, target) != columns:
                    raise RuntimeError(f'{table}: existing SF10 Parquet schema mismatch')
                count = con.execute(
                    f'SELECT COUNT(*) FROM read_parquet({quoted(target)})'
                ).fetchone()[0]
                if table in EXACT_ROWS and count != EXACT_ROWS[table]:
                    raise RuntimeError(f'{table}: existing SF10 Parquet row count mismatch')
                if table == 'lineitem' and not 55_000_000 <= count <= 65_000_000:
                    raise RuntimeError(f'lineitem: unexpected SF10 count {count:,}')
            report['tables'][table] = {
                'rows': count,
                'dbgen_trailing_pipe': trailing_pipe,
                'file': str(target.relative_to(ROOT)),
                'bytes': target.stat().st_size,
                'sha256': sha256(target),
                'schema': [{'name': name, 'type': typ} for name, typ in columns],
            }
            print(f'  {table:<12} {count:>12,} rows  {target.stat().st_size / 1048576:>9.1f} MiB  PASS', flush=True)
        tmp_manifest = MANIFEST.with_suffix('.json.tmp')
        tmp_manifest.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        tmp_manifest.replace(MANIFEST)
        print('\nSTATUS: SF10_PARQUET_VALIDATED', flush=True)
        print('Manifest:', MANIFEST, flush=True)
    finally:
        con.close()


if __name__ == '__main__':
    main()
