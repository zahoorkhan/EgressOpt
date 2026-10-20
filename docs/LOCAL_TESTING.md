# Testing the EgressOpt GitHub copy on your laptop — offline only

This is a **local reproducibility check**, not a new AWS/Azure/GCP benchmark. No accounts, cloud credentials, cloud CLIs, or VMs are needed. The default test runner uses only local files and Python; the optional test uses a loopback HTTPS fixture.

## 1. Setup (macOS or Linux)

```bash
# From the extracted directory (or Git clone), enter the repository root first.
cd EgressOpt        # Skip this line if already inside EgressOpt/
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Initial package installation may contact Python package servers; it does **not** contact AWS, Azure, or GCP or run cloud experiments. For fully disconnected testing, install dependencies from a locally cached wheelhouse. The Python modules used by the optional mock test are part of the standard library; it additionally requires the locally installed `openssl` command. The publication evidence fits in the repository; no SF1/SF10 Parquet dataset is required.

## 2. Single-command offline validation

```bash
python tools/run_local_checks.py
```

Runs these checks in sequence:

1. Parses Python sources for syntax errors without importing the experiments.
2. Scans publication text for recognizable secrets and infrastructure identifiers (a screening tool, not a complete secret audit).
3. Runs the direction-aware cost/SLO/Pareto selector selfcheck.
4. Checks Q03 and Q05 SQL/runner SHA-256 and physical-candidate source guards using **inspection mode only**.
5. Recalculates the four archived SF1/SF10 paired-run reports, audits each observation, validates the twenty measured pairs, verifies the paper's payload percentages, and regenerates the chart PDFs and LaTeX snippet in an automatically deleted temporary directory.

**Expected terminal marker:** `EGRESSOPT_LOCAL_CHECKS_PASS`. A source-hash warning about live provisioning is a documented publication limitation, not a failure of offline replay.

## 3. Additional local TLS and mock VM tests

```bash
python tools/run_local_checks.py --with-loopback
```

Also checks SHA-256 tamper rejection, TLS authentication, transfer idempotency, and three-provider **mocked** VM startup/cleanup. Uses synthetic files, a fake DuckDB record counter inside the fixture, and a temporary HTTPS server bound only to `127.0.0.1:18443`. No real cloud CLI calls are made. The test may fail if that local port is occupied or local socket creation is prohibited by the environment; this does not mean AWS, Azure, or GCP connectivity failed.

## 4. Regenerate figures and inspect the outputs

```bash
python analysis/phase2/report_sf1_sf10.py \
  --q03-sf1 analysis/phase2/evidence/q03_sf1_run.json \
  --q05-sf1 analysis/phase2/evidence/q05_sf1_run.json \
  --q03-sf10 analysis/phase2/evidence/q03_sf10_run.json \
  --q05-sf10 analysis/phase2/evidence/q05_sf10_run.json
```

Generated results are written under `results/phase2/sf1_sf10_report/` (ignored by Git). The reporting script should print:

```
EGRESSOPT_SF1_SF10_REPEATED_AUDIT_PASS
Reports: 4, measured pairs: 20
```

Inspect `sf1_sf10_summary.csv`, `RESULTS_AND_LIMITATIONS.md`, and the generated figures. The plotted bytes measure cross-cloud HTTP request bodies, **not billed network egress**; worker timings represent the sum of stages, not critical-path makespan.

## 5. What these tests do NOT demonstrate

- They **do not** rerun the original 200 SF1 executions or any real-cloud Q03/Q05 experiment.
- They do not revalidate the prospective Q05/Q09 experiments using live provider services.
- They do not reproduce all historical SF1 plan measurements because the full raw dataset and complete archived runs are not included in this lean public draft.
- They do not verify AWS/Azure/GCP credentials, authorization, live VM readiness, or the integrity of an environment-specific deployment.
- They do not prove full equivalence of the sanitized runner to the private, frozen research source.

**Do not run** cloud execution modes such as `--mode cloud-execute`, `--mode cloud-reuse-run`, `src/provision_tpch_sf10.py --mode apply`, or any `--ack-cloud-cost` command for an offline test.

## 6. Git pre-publication inspection

```bash
python tools/scan_public_release.py .
git status --short
git diff --cached --check
```

The scanner cannot guarantee that all secrets or sensitive data have been removed; manually inspect configuration, figures, history, and evidence before making the repository public.

## 7. TPC-H dataset preparation commands (separate from offline tests)

After installing `requirements.txt`, `python tools/setup_tpch_data.py --scale 1` creates the DuckDB SF1 data and Parquet exports; `python tools/setup_tpch_data.py --scale 10` generates SF10 using DBGEN and the schema-verified converter. **Neither command is part of `run_local_checks.py`:** they generate potentially large local datasets and may download DuckDB's extension or clone the DBGEN toolkit. See [TPCH_DATA.md](TPCH_DATA.md).

Safe dataset-setup CLI dry-runs are checked by the no-cloud suite. The dry-runs do not install extensions or generate any data.
