# EgressOpt — Public research code (publication draft)

*Egress-Optimal Query Optimization for Multi-Cloud Disaggregation*

This repository draft assembles the archived code and SQL from `Research.zip`, the most recent available Phase2 scripts (M6M), SF10 tools, and the latest paper from `EggOPT_7.zip`. The manuscript ZIP was **paper-only**, and the project may have newer local scripts. Before publication, compare this copy to your current `~/Research/EgressOpt` checkout. This is **not yet an independently end-to-end validated cloud release**.

## Included

- `experiments/q03.py`, `q05.py`, `q06.py`, `q09.py`, `q12.py`, `q14.py`: historical query experiments, with cloud identifier fallbacks sanitized for a new public release.
- `experiments/q03-phase2.py` and `q05-phase2.py`: restricted physical-plan evaluation and multicloud Phase2 runners.
- `queries/tpch/`: original query SQL and manifest; the q03/q05 SQL inputs are byte-identical to the frozen study.
- `src/`: local data preparation, validation, worker routines, and SF10 data transfer tools.
- `analysis/sf1_offline_evaluation/`: published aggregate/offline evaluator outputs.
- `analysis/phase2/evidence/`: four measured paired-run JSON reports at SF1 and SF10; intentionally contains no keys or live resource information.
- `analysis/phase2/report_sf1_sf10.py`: local, no-cloud figure and table regeneration.
- `paper/`: latest paper uploaded as `EggOPT_7.zip`.

## Cloud setup and strictly local testing

- **[docs/CLOUD_SETUP.md](docs/CLOUD_SETUP.md)** — how to configure AWS, Azure, and GCP using local CLI authentication and private environment variables; includes security and source-integrity caveats.
- **[docs/LOCAL_TESTING.md](docs/LOCAL_TESTING.md)** — safe, no-cloud test procedures, evidence regeneration, expected results, and limits.

Run this from the repository root after installing the Python requirements:

```bash
python tools/run_local_checks.py
python tools/run_local_checks.py --with-loopback   # optional local HTTPS + mock VM tests
```

Neither command performs real cloud operations. No `.env`, account login, or Parquet dataset is required to run the offline checks.

## Directory structure (before installing requirements)

See **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** for the full list of folders, scripts, SQL files, paper assets and evidence included in this repository. Do not upload `.venv/`, `data/`, `results/`, or cloud-credential directories.

```bash
# After cloning the repository or extracting the ZIP, enter the project root:
cd EgressOpt
# Confirm the requirements file and test runner are here:
ls requirements.txt tools/run_local_checks.py
```

Then follow **Local setup** below. If you have extracted the ZIP elsewhere, replace `cd EgressOpt` with the actual path to the extracted `EgressOpt` folder.

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Prepare TPC-H datasets (SF1 and SF10)

After the requirements have been installed, generate the TPC-H-derived research datasets **locally**. Run SF1 first because the original SF10 converter uses its Parquet schema as a reference.

```bash
# Generate all eight TPC-H tables at SF1 (DuckDB TPC-H extension)
python tools/setup_tpch_data.py --scale 1

# Generate all eight tables at SF10 (TPC-H DBGEN + Parquet conversion)
python tools/setup_tpch_data.py --scale 10

# Verify both datasets without modifying them
python tools/setup_tpch_data.py --scale 1 --check
python tools/setup_tpch_data.py --scale 10 --check
```

See **[docs/TPCH_DATA.md](docs/TPCH_DATA.md)** for prerequisites, SF10 disk requirements, DBGEN setup, data formats, provenance manifests, and safe dry-run options. The setup does not overwrite frozen query SQL, use cloud credentials, or deploy data to AWS/Azure/GCP. **Generating these datasets is optional for the offline evidence tests below.** Raw `.tbl`, Parquet and generated database files are excluded from Git.

## Offline evidence and security checks

```bash
python tools/scan_public_release.py .
python analysis/phase2/report_sf1_sf10.py \
  --q03-sf1 analysis/phase2/evidence/q03_sf1_run.json \
  --q05-sf1 analysis/phase2/evidence/q05_sf1_run.json \
  --q03-sf10 analysis/phase2/evidence/q03_sf10_run.json \
  --q05-sf10 analysis/phase2/evidence/q05_sf10_run.json
```

The reporting script consumes the four tracked sanitized evidence JSON files. It writes figures/tables in `results/phase2/sf1_sf10_report/`, which is gitignored.

## Multicloud configuration (not required for offline reporting)

```bash
cp .env.example .env
# Edit .env locally with your own non-public resource IDs.
set -a; source .env; set +a
aws sso login --profile "$EGRESSOPT_AWS_PROFILE"
az login
gcloud auth login
```

**Cloud execution may incur costs and make infrastructure changes.** Cloud-runnable code has not been exercised after removing historical resource-ID defaults. Perform offline preflight and compare against your private frozen source before invoking any cloud action. Do not push `.env` or generated runtime outputs.

## Evidence boundaries

Payload represents observed cross-cloud HTTPS request-body bytes, **not provider-billed egress**. Serial worker-stage timings are **not DAG critical-path or parallel makespan**; controller-inclusive timings include cloud-management overhead. Offline cost proxy/latency choices do not imply demonstrated savings on billed invoices or global optimality. The SF10 study compares two fixed candidate plans for Q03 and Q05, not optimizer-wide plan scalability.

## Repository/version provenance

The Phase1 cloud code in this public draft is a publication configuration fork (no private cloud identifier fallbacks). SHA-256 for the experimental Phase1 runners is preserved in the archived execution evidence; `SECURITY.md` documents the original/public distinction. This release has not been pushed to GitHub on the user's behalf.
