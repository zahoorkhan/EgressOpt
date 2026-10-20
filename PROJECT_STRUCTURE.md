# EgressOpt — Repository Directory Structure

This map describes the **tracked public research files** in the repository before installing Python dependencies. All paths below are relative to the repository root (`EgressOpt/`). The structure does not include a virtual environment, cloud credentials, private data, or the large TPC-H dataset.

```text
EgressOpt/
├── README.md                       # Project overview and local quick start
├── PROJECT_STRUCTURE.md            # This directory map
├── SECURITY.md                     # Security notes and release caveats
├── requirements.txt                # Python dependencies
├── .gitignore                      # Excludes local secrets, environments and outputs
├── .gitattributes
├── .env.example                    # Configuration template; no live credentials
├── egressopt.py                    # Offline selector and self-check entry point
│
├── config/
│   └── tariffs_public_list_marginal.json  # Published-rate transfer-cost model
├── experiments/                    # Historical SF1 and selected Phase2 runners
│   ├── q03.py
│   ├── q03-phase2.py
│   ├── q05.py
│   ├── q05-phase2.py
│   ├── q06.py
│   ├── q09.py
│   ├── q12.py
│   └── q14.py
├── queries/
│   └── tpch/
│       ├── q01.sql ... q22.sql     # TPC-H query files
│       └── query_manifest.json
├── src/                            # Data preparation, validation and worker utilities
│   ├── convert_tpch_sf10.py
│   ├── create_research_manifest.py
│   ├── export_tpch_parquet.py
│   ├── prepare_tpch.py
│   ├── provision_tpch_sf10.py
│   ├── run_reference_queries.py
│   ├── test_duckdb.py
│   ├── transfer_server.py
│   ├── validate_parquet_queries.py
│   ├── validate_reference.py
│   ├── validate_tpch.py
│   └── worker_api.py
├── analysis/
│   ├── phase2/
│   │   ├── report_sf1_sf10.py      # Regenerates tables/figures from evidence
│   │   └── evidence/
│   │       ├── q03_sf1_run.json
│   │       ├── q03_sf10_run.json
│   │       ├── q05_sf1_run.json
│   │       └── q05_sf10_run.json
│   └── sf1_offline_evaluation/
│       ├── audit_manifest.json
│       ├── heldout_decisions.csv
│       ├── plan_selection_details.csv
│       └── prediction_errors.csv
├── tests/
│   ├── test_m6l_direct_offline.py   # Local TLS and mocked cloud lifecycle
│   └── test_tpch_setup_cli.py      # No-network dataset command-line checks
├── tools/
│   ├── run_local_checks.py         # No-cloud test suite
│   ├── setup_tpch_data.py         # Generate SF1/SF10 data locally (not in Git)
│   └── scan_public_release.py      # Static credential-pattern scan
├── docs/
│   ├── TPCH_DATA.md                # SF1/SF10 generation and provenance guide
│   ├── CLOUD_SETUP.md              # AWS/Azure/GCP setup for authorized users
│   ├── LOCAL_TESTING.md            # Local, no-cloud validation
│   ├── LOCAL_TEST_REPORT.md
│   ├── PUBLIC_RELEASE_REVIEW.md
│   └── SANITIZATION_AUDIT.json
└── paper/
    ├── README.md
    ├── OVERLEAF_README.txt
    ├── REFERENCE_VERIFICATION.md
    ├── main.tex                    # IEEEtran LaTeX manuscript
    └── figures/
        ├── 04_latency_payload_tradeoff.pdf
        ├── 09_prospective_latency.pdf
        ├── fig01_cross_cloud_payload.pdf
        └── fig03_worker_vs_controller.pdf
```

## Before installing requirements

Either clone your own repository (once published) or extract the release ZIP. **Change into the `EgressOpt` folder before running Python commands.**

```bash
# If using GitHub:
git clone https://github.com/ahmadzahoor/EgressOpt.git
cd EgressOpt

# OR, after extracting the downloaded ZIP:
# cd /path/to/extracted/EgressOpt

# Verify you are in the right directory:
pwd
ls -la
ls requirements.txt tools/run_local_checks.py
```

Then create a Python environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Then prepare TPC-H SF1 and SF10 as needed for data-dependent execution:
python tools/setup_tpch_data.py --scale 1
python tools/setup_tpch_data.py --scale 10

# Offline evidence checks do not require either dataset:
python tools/run_local_checks.py
python tools/run_local_checks.py --with-loopback   # Optional
```

This installation may connect to the Python package index to download packages; the validation commands do **not** run cloud experiments.

## What must NOT be committed

- `.venv/`, `venv/`, `__pycache__/`, installed Python packages or local machine caches.
- `.env`, cloud CLI credential directories, tokens, private TLS keys/certificates, or service-account secrets.
- `data/`, `results/`, raw TPC-H `.tbl`/`.parquet` files, cloud deployment output, generated figures or execution logs.

The `.gitignore` already excludes these paths. `data/` and `results/` are created or populated by particular workflows as needed; they are **not** missing directories that must be uploaded. GitHub tracks files, not empty folders. The four intentionally included, sanitized evidence JSON files are under `analysis/phase2/evidence/`.

For cloud setup instructions, see [docs/CLOUD_SETUP.md](docs/CLOUD_SETUP.md). For no-cloud local tests, see [docs/LOCAL_TESTING.md](docs/LOCAL_TESTING.md). This release is a publication draft derived from archived scripts; it is not yet confirmed identical to any newer private local source tree.
