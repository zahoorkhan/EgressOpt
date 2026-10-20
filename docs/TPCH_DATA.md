# Generate and validate TPC-H data for EgressOpt (SF1 and SF10)

**Run these steps locally after installing `requirements.txt`.** This data is not downloaded as a ready-made benchmark archive. The repository generates its own **TPC-H-derived** tables and converts them to the Parquet layout expected by the project. This is not a TPC-certified benchmark result. The generated data and tool checkout live in `data/` and are excluded from Git.

## 1. Prerequisites

- Python and `duckdb` from `requirements.txt`.
- Internet access for the **first** DuckDB TPC-H extension installation.
- For SF10: `git`, `make`, a C compiler, and network access to clone the DBGEN toolkit, unless you already have its source and compiled binary. On macOS, use `xcode-select --install` for the command-line developer tools.
- Sufficient **local** disk capacity. SF10 entails tens of millions of rows and can require tens of gigabytes of temporary `.tbl`, Parquet, and conversion space. Check local free space before running; data generation can take time.

The scripts do **not** log in to AWS, Azure or GCP, start VMs, or incur cloud-provider compute or transfer charges.

## 2. Generate SF1 (first)

From the root of the cloned `EgressOpt` repository, with the Python environment activated:

```bash
python tools/setup_tpch_data.py --scale 1
```

This calls `INSTALL tpch; LOAD tpch; CALL dbgen(sf = 1)` through DuckDB, verifies table row counts, and exports eight ZSTD Parquet files. It creates:

```text
data/tpch/tpch_sf1.duckdb

data/tpch/parquet_sf1/
  region.parquet
  nation.parquet
  supplier.parquet
  customer.parquet
  part.parquet
  partsupp.parquet
  orders.parquet
  lineitem.parquet

results/dataset_validation/tpch_sf1_setup_manifest.json
```

**The script never regenerates or overwrites `queries/tpch/*.sql`.** These SQL files are part of the frozen experiment inputs and must not change. Existing valid dataset files are checked and reused without overwriting them.

## 3. Generate SF10

After SF1 has completed, run:

```bash
python tools/setup_tpch_data.py --scale 10
```

The helper clones [gregrahn/tpch-kit](https://github.com/gregrahn/tpch-kit) into the gitignored `data/tpch/tools/tpch-kit/` directory, builds `dbgen` for your operating system, runs `dbgen -v -s 10` with output in `data/tpch/raw_sf10/`, then invokes the original project converter `src/convert_tpch_sf10.py`. The converter reads the SF1 Parquet schema so SF10 column names/types match it exactly. The command creates:

```text
data/tpch/raw_sf10/*.tbl

data/tpch/parquet_sf10/
  region.parquet
  nation.parquet
  supplier.parquet
  customer.parquet
  part.parquet
  partsupp.parquet
  orders.parquet
  lineitem.parquet

results/dataset_validation/parquet_sf10_manifest.json
results/dataset_validation/tpch_sf10_setup_manifest.json
```

If you already have a trusted local DBGEN checkout or a built `dbgen`, use it instead of cloning:

```bash
python tools/setup_tpch_data.py --scale 10 --dbgen-dir /path/to/tpch-kit/dbgen
```

The generated provenance manifest records the generator's Git revision when available and SHA-256 hashes and row counts for all eight Parquet files. The generator's current upstream checkout is *not* guaranteed to produce byte-identical output to the version used in the historical paper. Exact reproduction of those archived byte-level measurements requires the original generator revision, data files, and recorded checksums; **do not substitute newly generated files and claim historical hash equivalence without checking**.

## 4. Local read-only verification

```bash
python tools/setup_tpch_data.py --scale 1 --check
python tools/setup_tpch_data.py --scale 10 --check
```

These check row counts and Parquet files. SF10 additionally verifies schema equality against SF1. They do not change the datasets or call any cloud APIs.

To preview the procedures without generating gigabytes of data or accessing the internet:

```bash
python tools/setup_tpch_data.py --scale 1 --dry-run
python tools/setup_tpch_data.py --scale 10 --dry-run
```

## 5. Expected cardinalities

| Table | SF1 rows | SF10 rows |
|---|---:|---:|
| region | 5 | 5 |
| nation | 25 | 25 |
| supplier | 10,000 | 100,000 |
| customer | 150,000 | 1,500,000 |
| part | 200,000 | 2,000,000 |
| partsupp | 800,000 | 8,000,000 |
| orders | 1,500,000 | 15,000,000 |
| lineitem | 6,001,215 | approximately 60 million (DBGEN-dependent) |

For `lineitem` at SF10, the original converter checks that its count lies between 55 and 65 million. Each converted table is checked against its SF1 schema, with individual SHA-256 and byte sizes recorded. SF1 results and SF10 historical results used *different generator workflows*; the public preparation commands intentionally preserve that distinction.

## 6. Git and security notes

Never commit `data/`, `results/`, `.env`, private TLS certificates, credentials, or provider runtime logs. Confirm with `git status --short` and `python tools/scan_public_release.py .` before publishing. Generated data is local only until you choose to perform a separate cloud placement experiment. **Dataset preparation does not perform deployment.**

## References

- [DuckDB TPC-H extension documentation](https://duckdb.org/docs/current/core_extensions/tpch)
- [TPC-H DBGEN toolkit with macOS/Linux build instructions](https://github.com/gregrahn/tpch-kit)
