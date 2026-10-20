#!/usr/bin/env python3
"""EgressOpt Q12: independent per-query reproducibility entry point.

From EgressOpt project root (SF1; use EGRESSOPT_SF=1 or --sf 1):

    python experiments/q12.py --mode cloud --trials 10
    python experiments/q12.py --mode reproduce
    python experiments/q12.py --mode audit

Cloud mode (CURRENTLY SF1/P1 ONLY) runs both directions with two retained
warm-ups per plan, ten measured trials per plan, alternating plan order,
correctness checks, TLS workers and temporary /32 ingress. It automatically
requests VM stop/deallocation and cleans up temporary ingress afterward.
Cloud execution incurs provider charges and requires the existing configured
AWS/Azure instances, existing SF1 data, predeployed verified worker release,
archive, working provider credentials and permissions. It is NOT a turnkey
fresh-account provisioning tool.

Reproduce and audit are completely OFFLINE. With no --run-id, select the most
recent suitable saved run under results/experiments/q12/. Reproduce refuses
uncontrolled or incomplete evidence, recalculates tables from the original
full-precision CSV and regenerates figures in a versioned analysis directory.
Audit independently checks cross-file consistency of raw trials and correctness.
Neither command can prove historical file immutability or identical VM timing.

To use a particular saved run:
    python experiments/q12.py --mode reproduce --run-id 20261015T052016Z_ee6c6e83
    python experiments/q12.py --mode audit --run-id 20261015T052016Z_ee6c6e83

The SCALE_FACTOR setting below is shared as a convention for each independently
maintained query runner. SF10/SF100 are NOT enabled until their actual datasets,
reference answers, deployed workers and expected intermediate shapes are
validated. A non-SF1 request fails closed instead of mislabeling SF1 evidence.
"""

from __future__ import annotations

import argparse
import base64
import subprocess
import tarfile
import csv
import hashlib
import json
import os
import platform
import statistics
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

QUERY_ID = "Q12"
PLACEMENT_ID = "P1"
# Configure scale once here, via EGRESSOPT_SF, or with --sf.
# Future independently versioned qXX.py files should expose the same names.
try:
    DEFAULT_SF = int(os.environ.get("EGRESSOPT_SF", "1"))
except ValueError as exc:
    raise SystemExit("EGRESSOPT_SF must be a positive integer") from exc
if DEFAULT_SF < 1:
    raise SystemExit("EGRESSOPT_SF must be a positive integer")
SCALE_FACTOR = f"SF{DEFAULT_SF}"
VALIDATED_SCALE_FACTORS = (1,)  # More scales require genuinely validated data/worker versions.
SF1_REFERENCE_RESULT_SHA256 = "a45c41482df91de5d95c5f59620945f5f7a0baae6aeb0fea6e04d460fca5e323"
EXECUTION_MODE = "local_emulation"  # Original local SQL-plan mode only.

# Fixed cross-query column schema. Blank network/cost cells mean NOT MEASURED.
METRIC_COLUMNS = [
    "schema_version", "run_id", "timestamp_utc", "query_id", "scale_factor",
    "execution_mode", "placement_id", "plan_id", "trial", "join_cloud",
    "logical_source_cloud", "logical_destination_cloud", "intermediate_rows",
    "intermediate_parquet_bytes", "stage_seconds", "join_aggregate_seconds",
    "total_seconds", "result_row_count", "result_sha256", "correct_vs_local_sql",
    "network_bytes_measured", "network_seconds_measured", "egress_usd_estimated",
    "duckdb_version", "python_version", "threads",
]

# Exactly the Q12 predicates from queries/tpch/q12.sql.
LINEITEM_PREDICATES = """
    l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS date)
    AND l_receiptdate < CAST('1995-01-01' AS date)
"""

AGGREGATES = """
    SUM(CASE WHEN o_orderpriority = '1-URGENT'
                  OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT'
                 AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
"""


def sql_literal(value: Path) -> str:
    return "'" + str(value.resolve()).replace("'", "''") + "'"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def save_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def canonical_q12(rows: list[tuple]) -> list[dict]:
    normalized = [
        {"l_shipmode": str(r[0]), "high_line_count": int(r[1]),
         "low_line_count": int(r[2])}
        for r in rows
    ]
    return sorted(normalized, key=lambda row: row["l_shipmode"])



def load_archived_q12(path: Path) -> list[dict]:
    """Normalize the independently archived SF1 Q12 result to canonical rows.

    Archive format: {"columns": [names...], "rows": [[values...], ...]}.
    Fail closed on absent or unexpectedly structured reference files.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Archived reference missing: {path}")
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Archived reference must be an object: {path}")
    expected = {"l_shipmode", "high_line_count", "low_line_count"}
    columns = data.get("columns")
    rows = data.get("rows")
    if (not isinstance(columns, list) or len(columns) != 3
            or set(columns) != expected or not isinstance(rows, list)):
        raise ValueError(f"Unexpected archived Q12 schema: {path}")
    tuples = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 3:
            raise ValueError(f"Unexpected archived Q12 row: {row!r}")
        mapped = dict(zip(columns, row))
        tuples.append((mapped["l_shipmode"], mapped["high_line_count"],
                       mapped["low_line_count"]))
    canonical = canonical_q12(tuples)
    if SCALE_FACTOR == 'SF1' and hash_result(canonical) != SF1_REFERENCE_RESULT_SHA256:
        raise ValueError('Q12 SF1 reference does not match the independently validated expected result checksum')
    return canonical


def hash_result(rows: list[dict]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def write_csv(path: Path, records: list[dict], append: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    need_header = not (append and path.exists() and path.stat().st_size > 0)
    with path.open("a" if append else "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=METRIC_COLUMNS, extrasaction="ignore")
        if need_header:
            writer.writeheader()
        writer.writerows(records)


def run_local_reference(duckdb, orders: Path, lineitem: Path, sql_path: Path,
                        threads: int) -> tuple[list[dict], float]:
    """Run the unmodified q12.sql against local Parquet-backed views."""
    sql = sql_path.read_text()
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"SET threads = {threads}")
        con.execute(f"CREATE VIEW orders AS SELECT * FROM read_parquet({sql_literal(orders)})")
        con.execute(f"CREATE VIEW lineitem AS SELECT * FROM read_parquet({sql_literal(lineitem)})")
        started = time.perf_counter()
        rows = con.execute(sql).fetchall()
        elapsed = time.perf_counter() - started
        return canonical_q12(rows), elapsed
    finally:
        con.close()


def run_local_plan(duckdb, plan: str, orders: Path, lineitem: Path,
                   threads: int) -> dict:
    """Materialize a transfer-shaped Parquet intermediate, then join locally.

    Stage bytes are real Parquet file bytes; they are NOT network bytes.
    No data crosses a cloud boundary in this implementation.
    """
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"SET threads = {threads}")
        with tempfile.TemporaryDirectory(prefix="egressopt_q12_") as temp:
            intermediate = Path(temp) / "intermediate.parquet"
            temp_sql = sql_literal(intermediate)
            start_total = time.perf_counter()

            if plan == "aws_join":
                # Azure pre-filters/project lineitem; logically Azure -> AWS.
                stage_query = f"""
                    SELECT l_orderkey, l_shipmode
                    FROM read_parquet({sql_literal(lineitem)})
                    WHERE {LINEITEM_PREDICATES}
                """
                join_query = f"""
                    SELECT l_shipmode, {AGGREGATES}
                    FROM read_parquet({sql_literal(orders)}) AS o
                    JOIN read_parquet({temp_sql}) AS l
                      ON o.o_orderkey = l.l_orderkey
                    GROUP BY l_shipmode ORDER BY l_shipmode
                """
                join_cloud, source_cloud, dest_cloud = "AWS", "Azure", "AWS"
            elif plan == "azure_join":
                # AWS projects orders; logically AWS -> Azure.
                stage_query = f"""
                    SELECT o_orderkey, o_orderpriority
                    FROM read_parquet({sql_literal(orders)})
                """
                join_query = f"""
                    SELECT l_shipmode, {AGGREGATES}
                    FROM read_parquet({sql_literal(lineitem)}) AS l
                    JOIN read_parquet({temp_sql}) AS o
                      ON o.o_orderkey = l.l_orderkey
                    WHERE {LINEITEM_PREDICATES}
                    GROUP BY l_shipmode ORDER BY l_shipmode
                """
                join_cloud, source_cloud, dest_cloud = "Azure", "AWS", "Azure"
            else:
                raise ValueError(f"Unknown plan: {plan}")

            stage_started = time.perf_counter()
            con.execute(
                f"COPY ({stage_query}) TO {temp_sql} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            stage_seconds = time.perf_counter() - stage_started
            intermediate_bytes = intermediate.stat().st_size

            join_started = time.perf_counter()
            result = canonical_q12(con.execute(join_query).fetchall())
            join_seconds = time.perf_counter() - join_started
            total_seconds = time.perf_counter() - start_total

            # Count outside the measured plan region to avoid warming the join input.
            intermediate_rows = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(intermediate)]
            ).fetchone()[0]

            return {
                "plan_id": plan,
                "join_cloud": join_cloud,
                "logical_source_cloud": source_cloud,
                "logical_destination_cloud": dest_cloud,
                "intermediate_rows": intermediate_rows,
                "intermediate_parquet_bytes": intermediate_bytes,
                "stage_seconds": round(stage_seconds, 9),
                "join_aggregate_seconds": round(join_seconds, 9),
                "total_seconds": round(total_seconds, 9),
                "result": result,
            }
    finally:
        con.close()



def make_local_preview_plots(records: list[dict], run_dir: Path) -> None:
    """Optional diagnostic charts. Not evidence of multi-cloud speed/egress."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("Preview plots skipped: matplotlib is not installed", file=sys.stderr)
        return

    plans = list(dict.fromkeys(row["plan_id"] for row in records))
    for metric, filename, ylabel in [
        ("total_seconds", "q12_local_time", "Local execution time (seconds)"),
        ("intermediate_parquet_bytes", "q12_intermediate_bytes",
         "Local serialized intermediate (bytes)"),
    ]:
        groups = [
            [float(row[metric]) for row in records if row["plan_id"] == plan]
            for plan in plans
        ]
        means = [statistics.mean(values) for values in groups]
        stds = [statistics.stdev(values) if len(values) > 1 else 0 for values in groups]
        fig, ax = plt.subplots(figsize=(5.7, 3.3))
        ax.bar(plans, means, yerr=stds if len(records) > len(plans) else None,
               capsize=4)
        ax.set_ylabel(ylabel)
        ax.set_title("Q12 LOCAL EMULATION — not cloud measurements")
        fig.tight_layout()
        fig.savefig(run_dir / f"{filename}.png", dpi=180)
        fig.savefig(run_dir / f"{filename}.pdf")
        plt.close(fig)



def make_worker_preview_plots(records: list[dict], run_dir: Path) -> None:
    """Diagnostics from loopback HTTP orchestration; NOT inter-cloud measurements."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("Preview plots skipped: matplotlib not installed", file=sys.stderr)
        return
    plans = list(dict.fromkeys(row["plan_id"] for row in records))
    for metric, filename, ylabel in [
        ("total_seconds", "q12_worker_local_end_to_end", "Local HTTP workflow wall time (s)"),
        ("intermediate_parquet_bytes", "q12_worker_local_intermediate_bytes", "Parquet payload (bytes)"),
    ]:
        groups = [[float(r[metric]) for r in records if r["plan_id"] == plan] for plan in plans]
        means = [statistics.mean(g) for g in groups]
        deviations = [statistics.stdev(g) if len(g) > 1 else 0.0 for g in groups]
        fig, ax = plt.subplots(figsize=(5.8, 3.4))
        ax.bar(plans, means, yerr=deviations if len(records) > len(plans) else None,
               capsize=4)
        ax.set_ylabel(ylabel)
        ax.set_title("Q12 LOOPBACK HTTP TEST — not cloud performance")
        fig.tight_layout()
        fig.savefig(run_dir / (filename + ".png"), dpi=180)
        fig.savefig(run_dir / (filename + ".pdf"))
        plt.close(fig)


def run_worker_local(args: argparse.Namespace, duckdb) -> int:
    """Orchestrate pre-existing FastAPI Q12 endpoints over localhost only.

    The Python coordinator calls stage, downloads intermediate, uploads to join,
    checks archived reference, and writes durable CSV + JSON. A single worker
    serves both sides: NO provider traffic, so monetary/network metrics are blank.
    """
    from urllib.parse import urlsplit
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError("worker_local requires requests in the active environment") from exc

    parsed = urlsplit(args.worker_url)
    if (parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost")
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in ("", "/")):
        raise ValueError("worker_local accepts only http://127.0.0.1:<port> or localhost")
    base_url = args.worker_url.rstrip("/")
    token_path = args.token_file.expanduser()
    token = token_path.read_text().strip()
    if len(token) < 32:
        raise ValueError("Worker token file is empty or too short")

    orders = args.data_dir / "orders.parquet"
    lineitem = args.data_dir / "lineitem.parquet"
    for path in (orders, lineitem, args.query_file, args.reference_file):
        if not path.is_file():
            raise FileNotFoundError(f"Required local input missing: {path}")
    if args.threads != 4:
        raise ValueError("Current worker API uses 4 hardcoded DuckDB threads; pass --threads 4")

    # Independent SQL archive is the authority for final correctness.
    baseline, baseline_seconds = run_local_reference(
        duckdb, orders, lineitem, args.query_file, args.threads
    )
    archived = load_archived_q12(args.reference_file)
    if baseline != archived:
        raise RuntimeError("Archived Q12 SQL reference differs from freshly executed SQL")
    reference_sha = hash_result(archived)
    print("Archived SF1 Q12 reference: PASS", flush=True)

    plans = ["aws_join", "azure_join"] if args.plan == "both" else [args.plan]
    session = requests.Session()
    session.headers["Authorization"] = "Bearer " + token
    response = session.get(base_url + "/health", timeout=(10, 30))
    response.raise_for_status()
    health = response.json()
    if health.get("status") != "ok" or health.get("provider") != "local":
        raise RuntimeError(f"Expected local worker health, got {health.get('provider')!r}")
    if health.get("duckdb_version") != duckdb.__version__:
        raise RuntimeError("Worker and coordinator DuckDB versions differ")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_dir = args.output_dir / "q12" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    save_json(run_dir / "q12_local_reference.json", archived)
    metadata = {
        "schema_version": 1, "run_id": run_id, "created_utc": stamp(),
        "query_id": QUERY_ID, "scale_factor": SCALE_FACTOR,
        "execution_mode": "worker_local_http", "run_status": "running",
        "measurement_scope": "ONE Mac with loopback HTTP; no cross-cloud network transfer",
        "worker_url": base_url, "worker_health": health,
        "placement_assumption": {"AWS": ["orders"], "Azure": ["lineitem"]},
        "plans": plans, "trials": args.trials, "threads": args.threads,
        "inputs": {
            "orders_sha256": sha256_file(orders),
            "lineitem_sha256": sha256_file(lineitem),
            "sql_sha256": sha256_file(args.query_file),
            "archived_reference_sha256": sha256_file(args.reference_file),
        },
        "reference_result_sha256": reference_sha,
        "reference_sql_execution_seconds": round(baseline_seconds, 9),
        "network_bytes_measured": None,
        "network_seconds_measured": None,
        "egress_usd_estimated": None,
        "notes": "download/upload payload bytes are loopback HTTP application bodies, NOT measured cloud egress",
    }
    save_json(run_dir / "run.json", metadata)

    records: list[dict] = []
    transport_records: list[dict] = []
    transport_columns = [
        "run_id", "timestamp_utc", "trial", "plan_id", "artifact_id", "receipt_id",
        "stage_http_wall_seconds", "download_http_wall_seconds",
        "post_join_http_wall_seconds", "worker_receive_seconds",
        "worker_join_aggregate_seconds", "payload_bytes_downloaded",
        "payload_bytes_uploaded", "correct_vs_archived_reference",
    ]
    try:
        for trial in range(1, args.trials + 1):
            for plan in plans:
                total_start = time.perf_counter()
                # The API operates on fixed query fragments, not arbitrary SQL.
                t = time.perf_counter()
                stage_resp = session.post(base_url + "/q12/stage", json={"plan_id": plan},
                                          timeout=(10, 300))
                stage_resp.raise_for_status()
                stage_http_wall = time.perf_counter() - t
                stage = stage_resp.json()
                if stage.get("plan_id") != plan or stage.get("actual_worker_cloud") != "local":
                    raise RuntimeError(f"Unexpected source worker for {plan}")
                artifact_id = stage.get("artifact_id", "")
                if not isinstance(artifact_id, str) or len(artifact_id) != 32:
                    raise RuntimeError("Invalid stage artifact identifier")
                expected_hash = stage.get("intermediate_sha256")
                if not isinstance(expected_hash, str) or len(expected_hash) != 64:
                    raise RuntimeError("Invalid stage checksum")
                save_json(run_dir / f"trial_{trial:02d}_{plan}_stage.json", stage)

                # Download actual artifact and verify size/hash before upload.
                intermediate = run_dir / f"trial_{trial:02d}_{plan}_intermediate.parquet"
                t = time.perf_counter()
                with session.get(base_url + "/q12/artifacts/" + artifact_id,
                                 stream=True, timeout=(10, 300)) as artifact_response:
                    artifact_response.raise_for_status()
                    digest = hashlib.sha256()
                    received = 0
                    with intermediate.open("wb") as out:
                        for chunk in artifact_response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                out.write(chunk)
                                digest.update(chunk)
                                received += len(chunk)
                download_wall = time.perf_counter() - t
                if received != int(stage["intermediate_parquet_bytes"]):
                    raise RuntimeError(f"Intermediate size mismatch for {plan}")
                if digest.hexdigest() != expected_hash:
                    raise RuntimeError(f"Intermediate SHA-256 mismatch for {plan}")

                # Upload same bytes to the receiving endpoint; time includes join/response.
                t = time.perf_counter()
                with intermediate.open("rb") as src:
                    join_resp = session.post(
                        base_url + "/q12/join/" + plan,
                        headers={"Content-Type": "application/octet-stream",
                                 "X-EgressOpt-SHA256": expected_hash},
                        data=src, timeout=(10, 300),
                    )
                join_resp.raise_for_status()
                post_join_wall = time.perf_counter() - t
                join = join_resp.json()
                total_wall = time.perf_counter() - total_start
                save_json(run_dir / f"trial_{trial:02d}_{plan}_join.json", join)

                if join.get("plan_id") != plan or join.get("actual_worker_cloud") != "local":
                    raise RuntimeError(f"Unexpected destination worker for {plan}")
                if (join.get("intermediate_sha256") != expected_hash
                        or int(join.get("received_payload_bytes", -1)) != received):
                    raise RuntimeError(f"Destination checksum/size mismatch for {plan}")
                result = sorted(join["result"], key=lambda x: x["l_shipmode"])
                correct = result == archived and join.get("correct_vs_archived_reference") is True
                save_json(run_dir / f"trial_{trial:02d}_{plan}_result.json", result)
                if not correct or hash_result(result) != reference_sha:
                    raise RuntimeError(f"Q12 final result does not match archived reference for {plan}")

                join_cloud, source_cloud, dest_cloud = (
                    ("AWS", "Azure", "AWS") if plan == "aws_join"
                    else ("Azure", "AWS", "Azure")
                )
                record = {
                    "schema_version": "1", "run_id": run_id, "timestamp_utc": stamp(),
                    "query_id": QUERY_ID, "scale_factor": SCALE_FACTOR,
                    "execution_mode": "worker_local_http", "placement_id": PLACEMENT_ID,
                    "plan_id": plan, "trial": trial, "join_cloud": join_cloud,
                    "logical_source_cloud": source_cloud,
                    "logical_destination_cloud": dest_cloud,
                    "intermediate_rows": int(stage["intermediate_rows"]),
                    "intermediate_parquet_bytes": received,
                    "stage_seconds": float(stage["stage_seconds"]),
                    "join_aggregate_seconds": float(join["join_aggregate_seconds"]),
                    "total_seconds": round(total_wall, 9),
                    "result_row_count": len(result), "result_sha256": reference_sha,
                    "correct_vs_local_sql": True,
                    "network_bytes_measured": "", "network_seconds_measured": "",
                    "egress_usd_estimated": "", "duckdb_version": duckdb.__version__,
                    "python_version": platform.python_version(), "threads": 4,
                }
                records.append(record)
                transport_records.append({
                    "run_id": run_id, "timestamp_utc": stamp(),
                    "trial": trial, "plan_id": plan, "artifact_id": artifact_id,
                    "receipt_id": join.get("receipt_id"),
                    "stage_http_wall_seconds": round(stage_http_wall, 9),
                    "download_http_wall_seconds": round(download_wall, 9),
                    "post_join_http_wall_seconds": round(post_join_wall, 9),
                    "worker_receive_seconds": join.get("receive_seconds"),
                    "worker_join_aggregate_seconds": join.get("join_aggregate_seconds"),
                    "payload_bytes_downloaded": received,
                    "payload_bytes_uploaded": int(join["received_payload_bytes"]),
                    "correct_vs_archived_reference": True,
                })
                # Save incrementally so interrupted runs retain their measurements.
                write_csv(run_dir / "trial_metrics.csv", records)
                with (run_dir / "worker_transport_metrics.csv").open("w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=transport_columns)
                    writer.writeheader()
                    writer.writerows(transport_records)
                print(f"{plan:10} trial={trial} correctness=PASS "
                      f"intermediate={received:,} bytes "
                      f"loopback_http_workflow={total_wall:.3f}s", flush=True)

        if args.plots:
            make_worker_preview_plots(records, run_dir)
        write_csv(args.output_dir / "all_query_metrics.csv", records, append=True)
        metadata["run_status"] = "success"
        metadata["completed_utc"] = stamp()
        metadata["all_plans_correct_vs_archived_reference"] = True
        metadata["results_csv"] = "trial_metrics.csv"
        metadata["transport_csv"] = "worker_transport_metrics.csv"
        save_json(run_dir / "run.json", metadata)
        print(f"\nRun artifacts: {run_dir}")
        print(f"Aggregate metrics: {args.output_dir / 'all_query_metrics.csv'}")
        print("IMPORTANT: worker_local_http uses localhost ONLY; NO cloud egress measured.")
        return 0
    except Exception as exc:
        metadata["run_status"] = "failed"
        metadata["completed_utc"] = stamp()
        metadata["error_type"] = type(exc).__name__
        metadata["error_message"] = str(exc)
        save_json(run_dir / "run.json", metadata)
        print(f"FAILED run preserved at: {run_dir}", file=sys.stderr)
        raise
    finally:
        session.close()



# Infrastructure configuration for the Q12 P1 Azure source worker. Keeping
# this in the query runner means one command per query, rather than users
# copying dozens of az CLI snippets. These are not embedded credentials.
AZURE_RESOURCE_GROUP = os.environ.get("EGRESSOPT_AZURE_RESOURCE_GROUP", '')
AZURE_VM = os.environ.get("EGRESSOPT_AZURE_VM", '')
DEPLOYMENT_BUNDLE = Path(os.environ.get("EGRESSOPT_Q12_WORKER_BUNDLE", "deployment/egressopt_q12_worker_v1.tar.gz"))
EXPECTED_BUNDLE_FILES = {
    "src/worker_api.py",
    "queries/tpch/q12.sql",
    "results/reference/sf1/q12_results.json",
}


def run_cli(arguments: list[str], *, timeout: int = 300) -> str:
    """Run cloud CLI without a shell and fail on any nonzero exit code."""
    try:
        result = subprocess.run(arguments, text=True, capture_output=True,
                                check=False, timeout=timeout)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Required CLI is not installed: {arguments[0]}") from exc
    if result.returncode:
        error = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"{arguments[0]} command failed: {error[-2500:]}")
    return result.stdout.strip()


def azure_command(script: str, *, label: str) -> str:
    """Execute a short shell program on Azure via existing VM Run Command.

    This is an administrative setup channel. It must NEVER be reported as
    a direct cloud-to-cloud query transfer or included in query egress data.
    """
    out = run_cli([
        "az", "vm", "run-command", "invoke",
        "--resource-group", AZURE_RESOURCE_GROUP,
        "--name", AZURE_VM, "--command-id", "RunShellScript",
        "--scripts", "set -eu\n" + script,
        "--query", "value[0].message", "--output", "tsv",
    ], timeout=420)
    if "Enable succeeded" not in out:
        raise RuntimeError(f"Azure Run Command did not report success for {label}: {out[-2000:]}")
    if "[stderr]" in out:
        stderr = out.split("[stderr]", 1)[-1].strip()
        if stderr:
            # Run Command may return a successful wrapper with script stderr.
            # For commands that must succeed, look for explicit success marker.
            print(f"Azure diagnostic ({label}): {stderr[-500:]}", file=sys.stderr)
    return out


def azure_power_state() -> str:
    output = run_cli([
        "az", "vm", "get-instance-view",
        "--resource-group", AZURE_RESOURCE_GROUP, "--name", AZURE_VM,
        "--query", "instanceView.statuses[].code", "--output", "json",
    ])
    values = json.loads(output)
    matches = [x for x in values if isinstance(x, str) and x.startswith("PowerState/")]
    if len(matches) != 1:
        raise RuntimeError(f"Cannot determine Azure VM state: {values!r}")
    return matches[0]


def validate_deployment_bundle(bundle: Path) -> tuple[str, str]:
    if not bundle.is_file():
        raise FileNotFoundError(
            f"Missing {bundle}. Create it from the verified worker, Q12 SQL, and reference JSON."
        )
    with tarfile.open(bundle, "r:gz") as tf:
        members = tf.getmembers()
        paths = [m.name.removeprefix("./") for m in members]
        if set(paths) != EXPECTED_BUNDLE_FILES or len(paths) != 3 or not all(m.isfile() for m in members):
            raise ValueError(f"Unexpected archive entries: {paths!r}")
    return sha256_file(bundle), base64.b64encode(bundle.read_bytes()).decode("ascii")


def azure_release_ready(release: str) -> bool:
    """Skip redundant upload when this immutable content-addressed release exists."""
    output = azure_command(
        f"if test -f '{release}/src/worker_api.py' && "
        f"test -f '{release}/results/reference/sf1/q12_results.json'; "
        "then echo Q12_RELEASE_READY; else echo Q12_RELEASE_MISSING; fi",
        label="release-check",
    )
    return "Q12_RELEASE_READY" in output


def deploy_azure_q12(bundle_hash: str, b64: str, release: str) -> None:
    import shlex
    staging = f"/opt/egressopt/deployment/q12_{uuid.uuid4().hex[:12]}"
    chunks = [b64[i:i+1500] for i in range(0, len(b64), 1500)]
    print(f"Azure deployment: {len(chunks)} small Run Command chunks", flush=True)
    azure_command(f"umask 077; mkdir -p '{staging}'; : > '{staging}/archive.b64'",
                  label="initialize")
    for index, chunk in enumerate(chunks, 1):
        azure_command(f"printf '%s' {shlex.quote(chunk)} >> '{staging}/archive.b64'",
                      label=f"chunk {index}/{len(chunks)}")
        print(f"  Azure upload {index}/{len(chunks)}", flush=True)
    script = f"""
base64 --decode '{staging}/archive.b64' > '{staging}/worker.tar.gz'
printf '%s  %s\\n' '{bundle_hash}' '{staging}/worker.tar.gz' | sha256sum -c -
mkdir -p '{release}'
tar -xzf '{staging}/worker.tar.gz' -C '{release}' \\
  src/worker_api.py queries/tpch/q12.sql results/reference/sf1/q12_results.json
/opt/egressopt/venv/bin/python -m py_compile '{release}/src/worker_api.py'
rm -rf '{staging}'
echo Q12_AZURE_DEPLOYMENT_PASS
"""
    # The literal backslash/continuation above is processed by bash on Azure.
    result = azure_command(script, label="verify-and-deploy")
    if "Q12_AZURE_DEPLOYMENT_PASS" not in result:
        raise RuntimeError(f"Azure deployment did not pass SHA and syntax checks: {result[-1500:]}")
    print("Azure deployment SHA-256 and Python syntax: PASS", flush=True)


def azure_smoke_test(release: str) -> dict:
    """Run localhost-only health and Q12 stage on Azure; no network experiment."""
    script = f"""
set -eu
cd '{release}'
PY=/opt/egressopt/venv/bin/python
$PY -c 'import duckdb; print("DuckDB:", duckdb.__version__)'
if ! $PY -c 'import fastapi, uvicorn' >/dev/null 2>&1; then
  $PY -m pip install 'fastapi==0.143.0' 'uvicorn==0.54.0'
fi
export EGRESSOPT_WORKER_CLOUD=azure
export EGRESSOPT_DATA_DIR=/opt/egressopt/data/tpch_sf1
export EGRESSOPT_STAGING_DIR=/opt/egressopt/results/worker_staging/q12
export EGRESSOPT_WORKER_TOKEN=$($PY -c 'import secrets; print(secrets.token_urlsafe(48))')
mkdir -p "$EGRESSOPT_STAGING_DIR"
test -f "$EGRESSOPT_DATA_DIR/lineitem.parquet"
test -f "$EGRESSOPT_DATA_DIR/customer.parquet"
test -f "$EGRESSOPT_DATA_DIR/nation.parquet"
$PY -m uvicorn src.worker_api:app --host 127.0.0.1 --port 18767 --log-level warning \\
   > /tmp/egressopt_q12_azure_smoke.log 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT
STATUS=000
for i in $(seq 1 25); do
  STATUS=$(curl -s -o /dev/null -w '%{{http_code}}' http://127.0.0.1:18767/health || true)
  [ "$STATUS" = 401 ] && break
  sleep 1
done
test "$STATUS" = 401
HEALTH=$(curl -fsS -H "Authorization: Bearer $EGRESSOPT_WORKER_TOKEN" \\
   http://127.0.0.1:18767/health)
RESPONSE=$(curl -fsS -X POST -H "Authorization: Bearer $EGRESSOPT_WORKER_TOKEN" \\
  -H 'Content-Type: application/json' -d '{{"plan_id":"aws_join"}}' \\
  http://127.0.0.1:18767/q12/stage)
export EGRESSOPT_SMOKE_HEALTH="$HEALTH"
export EGRESSOPT_SMOKE_RESPONSE="$RESPONSE"
$PY -c '
import os, json
h=json.loads(os.environ["EGRESSOPT_SMOKE_HEALTH"])
d=json.loads(os.environ["EGRESSOPT_SMOKE_RESPONSE"])
assert h["provider"] == "azure" and h["status"] == "ok", h
assert d["actual_worker_cloud"] == "azure", d
assert d["plan_id"] == "aws_join", d
assert d["intermediate_rows"] == 30988, d
assert d["intermediate_parquet_bytes"] > 0, d
print("Q12_AZURE_STAGE_JSON=" + json.dumps(d, separators=(",", ":")))
print("Q12_AZURE_LOCAL_SMOKE_PASS")
'
"""
    output = azure_command(script, label="localhost-health-and-Q12-stage")
    if "Q12_AZURE_LOCAL_SMOKE_PASS" not in output:
        raise RuntimeError(f"Azure health/stage test did not pass: {output[-1800:]}")
    marker = "Q12_AZURE_STAGE_JSON="
    lines = [line.split(marker, 1)[1] for line in output.splitlines() if marker in line]
    if len(lines) != 1:
        raise RuntimeError("Cannot locate the Azure stage metrics in Run Command output")
    return json.loads(lines[0])


def run_cloud_setup(args: argparse.Namespace) -> int:
    """Automate Azure's code deployment and localhost Q12 staging smoke test.

    Does not do any direct AWS/Azure exchange. Cloud VMs remain isolated by
    inbound rules; this mode makes NO monetary/transfer measurements.
    """
    bundle_hash, encoded = validate_deployment_bundle(args.deployment_bundle)
    release = f"/opt/egressopt/releases/q12_{bundle_hash[:12]}"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    output_dir = args.output_dir / "q12" / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": 1, "query_id": "Q12", "run_id": run_id,
        "created_utc": stamp(), "execution_mode": "cloud_worker_setup_only",
        "run_status": "running", "target_cloud": "azure",
        "azure_vm": AZURE_VM, "azure_resource_group": AZURE_RESOURCE_GROUP,
        "deployment_bundle_sha256": bundle_hash,
        "deployment_release": release,
        "dataset_placement_id": "P1",
        "measured_intercloud_bytes": None, "measured_intercloud_seconds": None,
        "egress_usd_estimated": None,
        "warning": "This command validates an Azure worker in isolation; NOT a distributed query run.",
    }
    save_json(output_dir / "run.json", report)
    original_state = None
    started_here = False
    try:
        original_state = azure_power_state()
        print(f"Azure current state: {original_state}", flush=True)
        if original_state != "PowerState/running":
            if original_state not in ("PowerState/deallocated", "PowerState/stopped"):
                raise RuntimeError(f"Unexpected Azure power state: {original_state}")
            print("Starting existing Azure VM for Q12 worker setup...", flush=True)
            run_cli(["az", "vm", "start", "--resource-group", AZURE_RESOURCE_GROUP,
                     "--name", AZURE_VM], timeout=600)
            started_here = True
        if azure_release_ready(release):
            print(f"Azure Q12 release already exists: {release} (skipping upload)", flush=True)
        else:
            deploy_azure_q12(bundle_hash, encoded, release)
        result = azure_smoke_test(release)
        save_json(output_dir / "azure_q12_stage.json", result)
        report.update({
            "run_status": "success", "completed_utc": stamp(),
            "azure_intermediate_rows": result["intermediate_rows"],
            "azure_intermediate_parquet_bytes": result["intermediate_parquet_bytes"],
            "azure_stage_seconds": result["stage_seconds"],
            "azure_intermediate_sha256": result["intermediate_sha256"],
        })
        print(f"Azure Q12 stage rows: {result['intermediate_rows']:,}", flush=True)
        print(f"Azure intermediate bytes: {result['intermediate_parquet_bytes']:,}", flush=True)
        print(f"Azure staging seconds: {result['stage_seconds']:.6f}", flush=True)
        print("AZURE_Q12_WORKER_SETUP_PASS", flush=True)
        return 0
    except Exception as exc:
        report.update({"run_status": "failed", "completed_utc": stamp(),
                       "error_type": type(exc).__name__, "error_message": str(exc)})
        raise
    finally:
        if started_here and not args.keep_azure_running:
            print("Deallocating Azure VM started by this command to limit compute charges...", flush=True)
            try:
                run_cli(["az", "vm", "deallocate", "--resource-group", AZURE_RESOURCE_GROUP,
                         "--name", AZURE_VM], timeout=600)
                report["azure_deallocated_after_setup"] = True
            except Exception as stop_error:
                report["azure_deallocated_after_setup"] = False
                report["azure_deallocation_error"] = str(stop_error)
                print(f"WARNING: Could not deallocate Azure automatically: {stop_error}", file=sys.stderr)
        save_json(output_dir / "run.json", report)
        print(f"Saved Azure setup report: {output_dir / 'run.json'}", flush=True)


# Step 9V: one-command, read-only network readiness audit. This does NOT
# grant ingress, open ports, start/stop VMs, transport data, or spend compute.
AWS_INSTANCE = os.environ.get("EGRESSOPT_AWS_INSTANCE_ID", '')
AWS_PROFILE = os.environ.get("EGRESSOPT_AWS_PROFILE", '')
AWS_REGION = os.environ.get("EGRESSOPT_AWS_REGION", '')
AZURE_NSG = os.environ.get("EGRESSOPT_AZURE_NSG", '')
AZURE_PUBLIC_IP_RESOURCE = os.environ.get("EGRESSOPT_AZURE_PUBLIC_IP_RESOURCE", '')
Q12_PORT = 8443


def cli_json(arguments: list[str], *, timeout: int = 180):
    # Arguments must end in --output json; no shell invocation.
    return json.loads(run_cli(arguments, timeout=timeout))


def aws_json(*arguments: str):
    return cli_json(["aws", *arguments, "--region", AWS_REGION,
                     "--profile", AWS_PROFILE, "--output", "json"])


def port_covers(value, port: int) -> bool:
    if value is None:
        return False
    if isinstance(value, list):
        return any(port_covers(v, port) for v in value)
    if isinstance(value, int):
        return value == port
    if isinstance(value, str):
        if value == "*":
            return True
        if value.isdigit():
            return int(value) == port
        if "-" in value:
            a, b = value.split("-", 1)
            return a.isdigit() and b.isdigit() and int(a) <= port <= int(b)
    return False


def azure_nsg_allows(rules, source_ip: str | None, port: int) -> dict:
    """Only establish a definite result for explicit single-IP rules.

    Default VNet/service-tag rules or ASGs can alter effective policy; when
    unclear, report inconclusive rather than guessing effective reachability.
    """
    from ipaddress import ip_address, ip_network
    if not source_ip:
        return {"assessment": "unknown_source_ip", "matched_rule": None}
    candidates = []
    for r in rules:
        if r.get("direction", "").lower() != "inbound":
            continue
        if r.get("protocol", "*").lower() not in ("tcp", "*"):
            continue
        ports = r.get("destinationPortRanges") or [r.get("destinationPortRange")]
        if not port_covers(ports, port):
            continue
        sources = r.get("sourceAddressPrefixes") or [r.get("sourceAddressPrefix")]
        if r.get("sourceApplicationSecurityGroups"):
            continue
        in_scope = False
        for source in sources:
            if not source:
                continue
            if source == "*" or source == "Internet":
                in_scope = True
            else:
                try:
                    in_scope = in_scope or ip_address(source_ip) in ip_network(source, strict=False)
                except ValueError:
                    pass
        if in_scope:
            candidates.append(r)
    if not candidates:
        return {"assessment": "no_explicit_matching_rule", "matched_rule": None}
    winner = min(candidates, key=lambda r: int(r.get("priority", 65000)))
    access = winner.get("access", "").lower()
    return {"assessment": "allow" if access == "allow" else "deny",
            "matched_rule": {"name": winner.get("name"), "priority": winner.get("priority"),
                             "access": winner.get("access"),
                             "source": winner.get("sourceAddressPrefix")}}


def aws_sg_allows(sg: dict, source_ip: str | None, port: int) -> dict:
    from ipaddress import ip_address, ip_network
    if not source_ip:
        return {"assessment": "unknown_source_ip", "matched_rule": None}
    for perm in sg.get("IpPermissions", []):
        proto = str(perm.get("IpProtocol", ""))
        if proto not in ("-1", "tcp", "6"):
            continue
        if proto != "-1" and not (perm.get("FromPort", 0) <= port <= perm.get("ToPort", -1)):
            continue
        for rng in perm.get("IpRanges", []):
            try:
                if ip_address(source_ip) in ip_network(rng["CidrIp"], strict=False):
                    return {"assessment": "allow", "matched_rule": rng["CidrIp"]}
            except (ValueError, KeyError):
                pass
    return {"assessment": "no_matching_ipv4_ingress_rule", "matched_rule": None}


def run_cloud_preflight(args: argparse.Namespace) -> int:
    """Record AWS/Azure VM addresses and proposed 8443 ingress paths.

    Intentionally READ-ONLY: provider outbound addresses and actual TCP/TLS
    reachability are NOT established until running workers are tested later.
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    outdir = args.output_dir / "q12" / run_id
    outdir.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "run_id": run_id, "query_id": "Q12",
              "execution_mode": "cloud_network_preflight_only",
              "run_status": "running", "created_utc": stamp(),
              "changes_performed": False, "cloud_data_transfer_performed": False,
              "network_bytes_measured": None, "network_seconds_measured": None,
              "egress_usd_estimated": None, "intended_worker_tcp_port": Q12_PORT}
    save_json(outdir / "run.json", report)
    try:
        desc = aws_json("ec2", "describe-instances", "--instance-ids", AWS_INSTANCE)
        instances = [i for res in desc.get("Reservations", []) for i in res.get("Instances", [])]
        if len(instances) != 1:
            raise RuntimeError("Expected exactly one AWS research instance")
        ins = instances[0]
        sg_ids = [x["GroupId"] for x in ins.get("SecurityGroups", [])]
        sg_data = aws_json("ec2", "describe-security-groups", "--group-ids", *sg_ids)
        public_azure = cli_json([
            "az", "network", "public-ip", "show", "--resource-group", AZURE_RESOURCE_GROUP,
            "--name", AZURE_PUBLIC_IP_RESOURCE, "--output", "json",
        ])
        nsg = cli_json([
            "az", "network", "nsg", "show", "--resource-group", AZURE_RESOURCE_GROUP,
            "--name", AZURE_NSG, "--output", "json",
        ])
        # Explicitly include default rules, since NSG show custom rules alone omits them.
        rules = list(nsg.get("securityRules", [])) + list(nsg.get("defaultSecurityRules", []))
        azure_ip = public_azure.get("ipAddress")
        aws_ip = ins.get("PublicIpAddress")
        aws_checks = [aws_sg_allows(sg, azure_ip, Q12_PORT)
                      for sg in sg_data.get("SecurityGroups", [])]
        azure_check = azure_nsg_allows(rules, aws_ip, Q12_PORT)
        report.update({
            "aws": {"instance_id": AWS_INSTANCE, "region": AWS_REGION,
                    "power_state": ins["State"]["Name"],
                    "public_ip": aws_ip, "private_ip": ins.get("PrivateIpAddress"),
                    "security_groups": sg_ids,
                    "ingress_8443_from_azure_public_ip": aws_checks},
            "azure": {"vm": AZURE_VM, "region": "eastus",
                      "power_state": azure_power_state(),
                      "public_ip_resource": AZURE_PUBLIC_IP_RESOURCE,
                      "public_ip": azure_ip,
                      "allocation": public_azure.get("publicIPAllocationMethod"),
                      "nsg": AZURE_NSG,
                      "ingress_8443_from_aws_public_ip": azure_check},
            "notes": [
                "Read-only configuration audit; no VM startup, no IAM/firewall modification.",
                "Addresses are resources' assigned public IPs, not verified outbound SNAT source IPs.",
                "Azure NSG decisions are simplified; effective rules may involve subnet or service tags.",
                "TLS certificate, worker authentication, and direct network connectivity remain untested.",
                "Do not treat this report as cloud query benchmark data.",
            ],
            "run_status": "success", "completed_utc": stamp(),
        })
        print("Q12 cloud network preflight (read-only)", flush=True)
        print(f"AWS VM: {ins['State']['Name']}; assigned public IP: {aws_ip or 'none'}", flush=True)
        print(f"Azure VM: {report['azure']['power_state']}; static public IP: {azure_ip or 'none'}", flush=True)
        print(f"AWS TCP {Q12_PORT} from Azure public IP: {[c['assessment'] for c in aws_checks]}", flush=True)
        print(f"Azure TCP {Q12_PORT} from AWS public IP: {azure_check['assessment']}", flush=True)
        print("Source NAT IP, TLS and real reachability NOT yet verified.", flush=True)
        print(f"Preflight report: {outdir / 'run.json'}", flush=True)
        return 0
    except Exception as exc:
        report.update({"run_status": "failed", "completed_utc": stamp(),
                       "error_type": type(exc).__name__, "error_message": str(exc)})
        raise
    finally:
        save_json(outdir / "run.json", report)


# Q12 cloud implementation extension. Appended to runner before main().
import ipaddress
import shlex


def aws_ssm_startup(script: str, purpose: str) -> str:
    # Unlike aws_json() this output is intentionally a plain UUID.
    command_id = run_cli([
        'aws', 'ssm', 'send-command', '--instance-ids', AWS_INSTANCE,
        '--document-name', 'AWS-RunShellScript', '--parameters', json.dumps({'commands': ['set -eu', script]}),
        '--comment', f'EgressOpt Q12: {purpose}'[:100],
        '--query', 'Command.CommandId', '--output', 'text',
        '--region', AWS_REGION, '--profile', AWS_PROFILE,
    ], timeout=90)
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline:
        proc = subprocess.run([
            'aws','ssm','get-command-invocation','--command-id',command_id,
            '--instance-id',AWS_INSTANCE,'--region',AWS_REGION,'--profile',AWS_PROFILE,
            '--output','json'],capture_output=True,text=True,timeout=40)
        if proc.returncode:
            if 'InvocationDoesNotExist' in proc.stderr:
                time.sleep(2); continue
            raise RuntimeError(f'SSM command query failed: {proc.stderr[-800:]}')
        info = json.loads(proc.stdout)
        if info['Status'] == 'Success':
            return info.get('StandardOutputContent','')
        if info['Status'] in ('Failed','TimedOut','Cancelled','Cancelling'):
            raise RuntimeError(f"SSM {purpose} {info['Status']}: {info.get('StandardErrorContent','')[-900:]} "
                               f"{info.get('StandardOutputContent','')[-900:]}")
        time.sleep(2)
    raise RuntimeError(f'SSM command {command_id} timeout')


def vm_remote(cloud: str, script: str, label: str) -> str:
    if cloud == 'aws':
        return aws_ssm_startup(script, label)
    if cloud == 'azure':
        return azure_command(script, label=label)
    raise ValueError(cloud)


def valid_public_ipv4(value: str) -> str:
    ip = ipaddress.ip_address(value.strip())
    if ip.version != 4 or not ip.is_global:
        raise ValueError(f'Not a publicly routable IPv4 address: {value!r}')
    return str(ip)


def cloud_vm_state():
    reservations = aws_json('ec2','describe-instances','--instance-ids',AWS_INSTANCE)['Reservations']
    if len(reservations) != 1 or len(reservations[0]['Instances']) != 1:
        raise RuntimeError('Cannot uniquely identify the AWS research EC2 instance')
    inst = reservations[0]['Instances'][0]
    return inst, azure_power_state()


def start_cloud_if_needed(provider: str) -> bool:
    if provider == 'aws':
        inst, _ = cloud_vm_state()
        current = inst['State']['Name']
        if current == 'running': return False
        if current not in ('stopped','pending'):
            raise RuntimeError(f'AWS VM state {current} is not safe to start')
        if current == 'stopped':
            run_cli(['aws','ec2','start-instances','--instance-ids',AWS_INSTANCE,
                     '--region',AWS_REGION,'--profile',AWS_PROFILE,'--output','json'])
        run_cli(['aws','ec2','wait','instance-running','--instance-ids',AWS_INSTANCE,
                 '--region',AWS_REGION,'--profile',AWS_PROFILE],timeout=600)
        return current == 'stopped'
    status = azure_power_state()
    if status == 'PowerState/running': return False
    if status not in ('PowerState/deallocated','PowerState/stopped'):
        raise RuntimeError(f'Azure VM state {status} is not safe to start')
    run_cli(['az','vm','start','--resource-group',AZURE_RESOURCE_GROUP,
             '--name',AZURE_VM],timeout=600)
    return True


def wait_aws_ssm_online(max_seconds: int = 240) -> None:
    deadline = time.monotonic() + max_seconds
    while time.monotonic() < deadline:
        data = aws_json('ssm','describe-instance-information',
                        '--filters',json.dumps([{'Key':'InstanceIds','Values':[AWS_INSTANCE]}]))
        matches = data.get('InstanceInformationList', [])
        if any(x.get('InstanceId')==AWS_INSTANCE and x.get('PingStatus')=='Online'
               for x in matches):
            return
        time.sleep(5)
    raise RuntimeError('AWS SSM agent did not become Online after EC2 startup')


def source_egress_ip(cloud: str) -> str:
    # Queries each VM's actual outbound internet source IP; SNAT can differ
    # from its assigned public interface address.
    output = vm_remote(cloud,
        'curl --fail --silent --show-error --connect-timeout 10 --max-time 25 https://checkip.amazonaws.com',
        'outbound-IP-verification')
    if cloud == 'azure' and '[stdout]' in output:
        output = output.split('[stdout]',1)[1].split('[stderr]',1)[0]
    return valid_public_ipv4(output.strip())


def certificate_server_setup(*, cloud: str, ip: str, token: str,
                             release: str, run_key: str) -> str:
    """Create one-day CA and IP-SAN server cert; start temporary systemd unit.

    Returns a base64-encoded *public* CA certificate. No private key is returned.
    """
    work = f'/opt/egressopt/runtime/q12_{run_key}_{cloud}'
    venv = '/opt/egressopt/venv311/bin/python' if cloud == 'aws' else '/opt/egressopt/venv/bin/python'
    unit = f'egressopt-q12-{run_key}-{cloud}'
    data_dir = '/opt/egressopt/data/tpch_sf1'
    # Literal interpolated values are validated IPs / random identifiers, and
    # shlex.quote prevents shell interpolation of bearer-token contents.
    startup = f'''#!/bin/sh
set -eu
cd {shlex.quote(release)}
export EGRESSOPT_WORKER_TOKEN={shlex.quote(token)}
export EGRESSOPT_WORKER_CLOUD={shlex.quote(cloud)}
export EGRESSOPT_DATA_DIR={shlex.quote(data_dir)}
export EGRESSOPT_STAGING_DIR={shlex.quote('/opt/egressopt/results/worker_staging/q12')}
exec {venv} -m uvicorn src.worker_api:app --host 0.0.0.0 --port 8443 \\
  --ssl-certfile {work}/server.pem --ssl-keyfile {work}/server.key --log-level warning
'''
    # Encode startup to avoid multi-level quoting of scripts; script still passes
    # through cloud management history, so tokens MUST be ephemeral.
    launch_b64 = base64.b64encode(startup.encode()).decode()
    script = f'''
umask 077
mkdir -p {work} /opt/egressopt/results/worker_staging/q12
test -f {release}/src/worker_api.py
test -f {data_dir}/{'orders' if cloud == 'aws' else 'lineitem'}.parquet
{venv} -c 'import duckdb,fastapi,uvicorn; assert duckdb.__version__ == "1.5.6"'
command -v openssl >/dev/null
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 \\
  -keyout {work}/ca.key -out {work}/ca.pem \\
  -subj '/CN=EgressOpt temporary experiment CA' \\
  -addext 'basicConstraints=critical,CA:TRUE' >/dev/null 2>&1
openssl req -nodes -newkey rsa:2048 -sha256 \\
  -keyout {work}/server.key -out {work}/server.csr \\
  -subj '/CN=EgressOpt temporary worker' >/dev/null 2>&1
printf 'basicConstraints=CA:FALSE\\nsubjectAltName=IP:{ip}\\nextendedKeyUsage=serverAuth\\n' > {work}/server.ext
openssl x509 -req -days 1 -sha256 -in {work}/server.csr \\
  -CA {work}/ca.pem -CAkey {work}/ca.key -CAcreateserial \\
  -extfile {work}/server.ext -out {work}/server.pem >/dev/null 2>&1
printf '%s' '{launch_b64}' | base64 -d > {work}/launch.sh
chmod 700 {work}/launch.sh
systemd-run --unit={unit} --collect --property=Restart=no \\
  /bin/sh {work}/launch.sh >/dev/null
STATUS=000
for i in $(seq 1 30); do
  STATUS=$(curl --insecure --silent --output /dev/null --write-out '%{{http_code}}' \\
    --connect-timeout 1 --max-time 3 https://127.0.0.1:8443/health || true)
  [ "$STATUS" = 401 ] && break
  sleep 1
done
test "$STATUS" = 401 || (systemctl status {unit} --no-pager >&2 || true; exit 1)
echo EGRESSOPT_TLS_WORKER_READY
printf 'EGRESSOPT_CA_B64='; base64 -w0 {work}/ca.pem; echo
'''
    out = vm_remote(cloud, script, f'{cloud}-TLS-start')
    if 'EGRESSOPT_TLS_WORKER_READY' not in out:
        raise RuntimeError(f'{cloud} worker failed TLS startup: {out[-1000:]}')
    matched = [s.split('EGRESSOPT_CA_B64=',1)[-1] for s in out.splitlines()
               if s.startswith('EGRESSOPT_CA_B64=')]
    if len(matched) != 1:
        raise RuntimeError(f'Unable to retrieve temporary {cloud} CA certificate')
    ca = base64.b64decode(matched[0],validate=True)
    if b'BEGIN CERTIFICATE' not in ca:
        raise RuntimeError(f'Unexpected {cloud} CA certificate')
    return matched[0]


def open_aws_ingress(sg_id: str, source_ip: str, run_key: str) -> str:
    permission = [{'IpProtocol':'tcp','FromPort':8443,'ToPort':8443,
                   'IpRanges':[{'CidrIp':f'{source_ip}/32',
                                'Description':f'EgressOpt Q12 {run_key} temporary TLS'}]}]
    data = aws_json('ec2','authorize-security-group-ingress','--group-id',sg_id,
                    '--ip-permissions',json.dumps(permission))
    rules = data.get('SecurityGroupRules',[])
    if len(rules) != 1 or not rules[0].get('SecurityGroupRuleId'):
        raise RuntimeError('AWS ingress rule created but returned no rule ID; inspect immediately')
    return rules[0]['SecurityGroupRuleId']


def close_aws_ingress(sg_id: str, rule_id: str):
    aws_json('ec2','revoke-security-group-ingress','--group-id',sg_id,
             '--security-group-rule-ids',rule_id)


def open_azure_ingress(source_ip: str, run_key: str) -> str:
    name = f'egressopt-q12-{run_key}'
    current = cli_json(['az','network','nsg','rule','list','--resource-group',AZURE_RESOURCE_GROUP,
                        '--nsg-name',AZURE_NSG,'--output','json'])
    used = {int(r['priority']) for r in current if r.get('direction')=='Inbound'}
    priority = next((p for p in range(1100, 2100) if p not in used),None)
    if priority is None: raise RuntimeError('No available Azure NSG rule priority')
    run_cli(['az','network','nsg','rule','create','--resource-group',AZURE_RESOURCE_GROUP,
             '--nsg-name',AZURE_NSG,'--name',name,'--priority',str(priority),
             '--direction','Inbound','--access','Allow','--protocol','Tcp',
             '--source-address-prefixes',f'{source_ip}/32',
             '--destination-port-ranges','8443',
             '--description',f'EgressOpt Q12 temporary TLS {run_key}',
             '--output','none'],timeout=240)
    return name


def close_azure_ingress(rule: str):
    run_cli(['az','network','nsg','rule','delete','--resource-group',AZURE_RESOURCE_GROUP,
             '--nsg-name',AZURE_NSG,'--name',rule,'--output','none'],timeout=240)


def perform_cloud_trials(source: str, plan: str, count: int, dst_ip: str,
                          ca_b64: str, token: str, run_key: str):
    assert source in ('aws','azure')
    assert plan in ('aws_join','azure_join')
    work = f'/opt/egressopt/runtime/q12_{run_key}_{source}'
    venv = '/opt/egressopt/venv311/bin/python' if source == 'aws' else '/opt/egressopt/venv/bin/python'
    script = f'''
set -eu
umask 077
printf '%s' {shlex.quote(ca_b64)} | base64 -d > {work}/peer-ca.pem
for TRIAL in $(seq 1 {count}); do
  STAGE=$(curl --insecure --fail --silent --show-error \\
    -H 'Authorization: Bearer {token}' -H 'Content-Type: application/json' \\
    -X POST -d '{{"plan_id":"{plan}"}}' \\
    https://127.0.0.1:8443/q12/stage)
  export STAGE
  ARTIFACT_ID=$({venv} -c 'import os,json; print(json.loads(os.environ["STAGE"])["artifact_id"])')
  CHECKSUM=$({venv} -c 'import os,json; print(json.loads(os.environ["STAGE"])["intermediate_sha256"])')
  OUT={work}/response.json
  INFO=$(curl --fail --silent --show-error --connect-timeout 20 --max-time 300 \\
    --cacert {work}/peer-ca.pem \\
    -H 'Authorization: Bearer {token}' \\
    -H 'Content-Type: application/octet-stream' \\
    -H "X-EgressOpt-SHA256: $CHECKSUM" \\
    -X POST --data-binary @/opt/egressopt/results/worker_staging/q12/$ARTIFACT_ID/intermediate.parquet \\
    --output "$OUT" --write-out '%{{time_total}} %{{size_upload}} %{{http_code}}' \\
    https://{dst_ip}:8443/q12/join/{plan})
  export INFO OUT TRIAL
  {venv} -c '
import os,json
s=json.loads(os.environ["STAGE"]); j=json.load(open(os.environ["OUT"]))
t,uploaded,status = os.environ["INFO"].split()
assert status == "200", status
assert s["intermediate_sha256"] == j["intermediate_sha256"]
assert int(s["intermediate_parquet_bytes"]) == int(uploaded) == int(j["received_payload_bytes"])
assert j["correct_vs_archived_reference"] is True, j
assert j["result_row_count"] == 2, j
assert s["intermediate_rows"] == j["intermediate_rows"]
print("EGRESSOPT_TRIAL_JSON=" + json.dumps({{"trial":int(os.environ["TRIAL"]),"stage":s,
 "join":j,"curl_http_total_seconds":float(t),"curl_size_upload":int(uploaded)}},separators=(",",":")))
'
done
printf 'EGRESSOPT_CLOUD_PLAN_PASS\n'
'''
    output = vm_remote(source, script, f'Q12-{plan}-{count}-trials')
    if 'EGRESSOPT_CLOUD_PLAN_PASS' not in output:
        raise RuntimeError(f'Cloud plan {plan} did not complete: {output[-1100:]}')
    results = []
    for line in output.splitlines():
        if line.startswith('EGRESSOPT_TRIAL_JSON='):
            results.append(json.loads(line.split('=',1)[1]))
    if len(results) != count:
        raise RuntimeError(f'Expected {count} cloud trials; found {len(results)}. Azure Run Command may truncate stdout.')
    return results


def make_cloud_plots(records, run_dir):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print('Cloud plots skipped: matplotlib not installed',file=sys.stderr);return
    for metric, suffix, ylabel in [
        ('total_seconds','q12_cloud_latency','Stage + TLS request wall time (s)'),
        ('network_bytes_measured','q12_cloud_payload','Transferred HTTP-body bytes'),
        ('join_aggregate_seconds','q12_cloud_join','Receiver join/aggregation time (s)')]:
        plans = list(dict.fromkeys(x['plan_id'] for x in records))
        vals = [[float(x[metric]) for x in records if x['plan_id']==p] for p in plans]
        means = [statistics.mean(v) for v in vals]
        errs = [statistics.stdev(v) if len(v)>1 else 0 for v in vals]
        fig,ax=plt.subplots(figsize=(5.8,3.5))
        ax.bar(plans,means,yerr=errs if all(len(v)>1 for v in vals) else None,capsize=4)
        ax.set_title('Q12 direct AWS–Azure TLS transfer (measured)')
        ax.set_ylabel(ylabel)
        fig.tight_layout()
        fig.savefig(run_dir/f'{suffix}.png',dpi=180)
        fig.savefig(run_dir/f'{suffix}.pdf')
        plt.close(fig)


def run_real_multicloud(args):
    if args.keep_cloud_vms_running and args.stop_all_after_run:
        raise ValueError('--keep-cloud-vms-running conflicts with --stop-all-after-run')
    if not args.allow_temporary_ingress:
        raise RuntimeError('Cloud mode changes AWS/Azure firewalls temporarily. '
                           'Rerun with --allow-temporary-ingress to approve these scoped changes.')
    if args.trials > 100:
        raise ValueError('Limit a single cloud experiment to 100 trials per plan')
    bundle_hash, _ = validate_deployment_bundle(args.deployment_bundle)
    release = f'/opt/egressopt/releases/q12_{bundle_hash[:12]}'
    archive = load_archived_q12(args.reference_file)
    expected_result_hash = hash_result(archive)
    plans = ['aws_join','azure_join'] if args.plan == 'both' else [args.plan]
    run_key = uuid.uuid4().hex[:8]
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+run_key
    outdir = args.output_dir/'q12'/run_id
    outdir.mkdir(parents=True,exist_ok=False)
    report = {'schema_version':1,'query_id':'Q12','run_id':run_id,
              'execution_mode':'real_multicloud','run_status':'running',
              'placement_id':'P1','scale_factor':SCALE_FACTOR,'created_utc':stamp(),'plans':plans,'trials':args.trials,
              'warmup_trials_per_plan':getattr(args,'warmup_trials',0),
              'alternate_plan_order_by_round':getattr(args,'alternate_plan_order',False),
              'cloud_cost_status':'NOT_PRICED','egress_usd_estimated':None,
              'measurement_definition':{
                  'network_bytes_measured':'curl size_upload, application HTTP request body bytes, not provider billed bytes',
                  'network_seconds_measured':'NOT isolated; null (receiver_body_seconds in join JSON, curl_http_total_seconds in transport JSON include other work)',
                  'total_seconds':'stage_seconds + source VM curl total time: includes TLS, network and destination join; excludes control plane orchestration',
                  'curl_http_total_seconds':'TLS connection + HTTP upload + destination processing and response'},
              'worker_release':release,'worker_bundle_sha256':bundle_hash,
              'cloud_source_ips':{},'temporary_network_rule_ids':{},'cleanup':{},
              'security_note':'Per-run bearer tokens pass through authorized management command history. Tokens are revoked by stopping services and deleting runtime directory; they are not retained in run.json.'}
    report['reproducibility'] = capture_q12_local_provenance(args, bundle_hash)
    save_json(outdir/'run.json',report)
    changed = {'aws_started':False,'azure_started':False,'aws_rule':None,
               'azure_rule':None,'aws_service':False,'azure_service':False,
               'aws_sg':None}
    records=[]
    try:
        for cloud in ('aws','azure'):
            print(f'Checking {cloud.upper()} VM...',flush=True)
            changed[f'{cloud}_started'] = start_cloud_if_needed(cloud)
            print(f'  {cloud.upper()} running',flush=True)
        wait_aws_ssm_online()
        inst, state = cloud_vm_state()
        if inst['State']['Name'] != 'running' or state != 'PowerState/running':
            raise RuntimeError('Both cloud VMs must be running')
        aws_public = valid_public_ipv4(inst.get('PublicIpAddress',''))
        az_public_data = cli_json(['az','network','public-ip','show',
            '--resource-group',AZURE_RESOURCE_GROUP,'--name',AZURE_PUBLIC_IP_RESOURCE,'--output','json'])
        az_public = valid_public_ipv4(az_public_data['ipAddress'])
        sg_ids = [g['GroupId'] for g in inst['SecurityGroups']]
        if len(sg_ids)!=1: raise RuntimeError(f'Expected one managed AWS SG, found {sg_ids!r}')
        changed['aws_sg'] = sg_ids[0]
        report['assigned_public_ips']={'aws':aws_public,'azure':az_public}
        aws_snat = source_egress_ip('aws')
        az_snat = source_egress_ip('azure')
        report['cloud_source_ips']={'aws':aws_snat,'azure':az_snat}
        save_json(outdir/'run.json',report)
        print(f'Outbound source IPs verified (AWS {aws_snat}, Azure {az_snat})',flush=True)
        for cloud,ip in (('aws',aws_public),('azure',az_public)):
            verify = vm_remote(cloud, f'test -f {release}/src/worker_api.py && '
                               f'test -f {release}/results/reference/sf1/q12_results.json && '
                               'echo RELEASE_VERIFIED',f'{cloud}-release-verification')
            if 'RELEASE_VERIFIED' not in verify:
                raise RuntimeError(f'Missing verified Q12 release on {cloud}. Run --mode cloud_setup for Azure; '
                                   'AWS should use your already completed SSM deployment.')
        # Capture dataset and worker fingerprints on both actual VMs.
        # These commands are outside measured query timing.
        report['reproducibility']['cloud_workers'] = {
            cloud: capture_q12_remote_provenance(cloud, release)
            for cloud in ('aws', 'azure')
        }
        # Check the remote files against the exact published deployment archive,
        # not merely against a release directory name.
        with tarfile.open(args.deployment_bundle, 'r:gz') as archive_tar:
            member_hashes = {
                member.name.removeprefix('./'): hashlib.sha256(
                    archive_tar.extractfile(member).read()).hexdigest()
                for member in archive_tar.getmembers()
            }
        for cloud in ('aws', 'azure'):
            remote = report['reproducibility']['cloud_workers'][cloud]
            for remote_key, tar_path in (
                    ('worker_api_sha256', 'src/worker_api.py'),
                    ('reference_sha256', 'results/reference/sf1/q12_results.json')):
                if remote[remote_key] != member_hashes[tar_path]:
                    raise RuntimeError(f'{cloud} deployed {tar_path} does not match immutable Q12 bundle')
        report['reproducibility']['deployment_members_sha256'] = member_hashes
        az_vm_description = cli_json([
            'az', 'vm', 'show', '--resource-group', AZURE_RESOURCE_GROUP,
            '--name', AZURE_VM,
            '--query', '{location:location,vm_size:hardwareProfile.vmSize,os_type:storageProfile.osDisk.osType}',
            '--output', 'json'])
        report['reproducibility']['cloud_instances'] = {
            'aws': {'instance_id': AWS_INSTANCE, 'region': AWS_REGION,
                    'instance_type': inst.get('InstanceType'),
                    'availability_zone': inst.get('Placement', {}).get('AvailabilityZone')},
            'azure': {'vm_name': AZURE_VM, 'resource_group': AZURE_RESOURCE_GROUP,
                      'region': az_vm_description['location'],
                      'vm_size': az_vm_description['vm_size'],
                      'os_type': az_vm_description.get('os_type')}
        }
        save_json(outdir/'run.json',report)
        # Tokens are one-time per-run, never recorded in results.
        import secrets
        token = secrets.token_urlsafe(48)
        ca = {}
        for cloud,ip in (('aws',aws_public),('azure',az_public)):
            # Set service flag before setup: even a half-started systemd unit must be cleaned.
            changed[f'{cloud}_service']=True
            ca[cloud]=certificate_server_setup(cloud=cloud,ip=ip,token=token,
                                               release=release,run_key=run_key)
            print(f'  {cloud.upper()} authenticated TLS worker running',flush=True)
        if 'aws_join' in plans:
            changed['aws_rule']=open_aws_ingress(changed['aws_sg'],az_snat,run_key)
            report['temporary_network_rule_ids']['aws']=changed['aws_rule']
            print('  AWS temporary 8443 ingress from Azure verified outbound /32 created',flush=True)
        if 'azure_join' in plans:
            changed['azure_rule']=open_azure_ingress(aws_snat,run_key)
            report['temporary_network_rule_ids']['azure']=changed['azure_rule']
            print('  Azure temporary 8443 ingress from AWS verified outbound /32 created',flush=True)
        save_json(outdir/'run.json',report)
        warmup_records = []
        warmup_count = getattr(args, 'warmup_trials', 0)
        alternate = getattr(args, 'alternate_plan_order', False)
        for round_index in range(warmup_count + args.trials):
            current_plans = list(plans)
            if alternate and round_index % 2:
                current_plans.reverse()
            is_warmup = round_index < warmup_count
            for plan in current_plans:
                source,dest,ip=(('azure','aws',aws_public) if plan=='aws_join'
                                else ('aws','azure',az_public))
                # Issue one VM management command per trial internally. This avoids
                # Azure Run Command's short stdout window while preserving one
                # user-facing command for any requested trial count.
                trial_index = round_index - warmup_count + 1
                trials = perform_cloud_trials(source,plan,1,ip,ca[dest],token,run_key)
                for t in trials:
                    t['trial'] = trial_index
                    stage, join, trial = t['stage'], t['join'], int(t['trial'])
                    result = sorted(join['result'], key=lambda x: x['l_shipmode'])
                    if result != archive or join['result_sha256'] != expected_result_hash:
                        raise RuntimeError(f'Q12 actual cloud result mismatch {plan} trial {trial}')
                    if stage['actual_worker_cloud'] != source or join['actual_worker_cloud'] != dest:
                        raise RuntimeError('Cross-cloud source/dest identity mismatch')
                    transport = {
                        'curl_http_total_seconds': float(t['curl_http_total_seconds']),
                        'curl_size_upload': int(t['curl_size_upload']),
                        'receiver_body_seconds': float(join['receive_seconds']),
                        'note': 'HTTP wall time includes network, TLS, join and response; not isolated network time.',
                    }
                    if is_warmup:
                        item = {
                            'round': round_index + 1, 'plan_id': plan,
                            'execution_mode': 'real_multicloud_warmup',
                            'timestamp_utc': stamp(),
                            'stage_seconds': float(stage['stage_seconds']),
                            'curl_http_total_seconds': transport['curl_http_total_seconds'],
                            'total_seconds': float(stage['stage_seconds']) + transport['curl_http_total_seconds'],
                            'intermediate_parquet_bytes': int(stage['intermediate_parquet_bytes']),
                            'curl_size_upload': transport['curl_size_upload'],
                            'result_row_count': len(result),
                            'correct_vs_archived_reference': True,
                        }
                        warmup_records.append(item)
                        with (outdir / 'warmup_metrics.csv').open('w', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=list(item))
                            writer.writeheader()
                            writer.writerows(warmup_records)
                        save_json(outdir / f'warmup_{round_index+1:02d}_{plan}_stage.json', stage)
                        save_json(outdir / f'warmup_{round_index+1:02d}_{plan}_join.json', join)
                        save_json(outdir / f'warmup_{round_index+1:02d}_{plan}_transport.json', transport)
                        print(f"{plan} WARMUP={round_index+1}: "
                              f"{item['intermediate_parquet_bytes']:,} bytes; "
                              f"stage+HTTPS+join={item['total_seconds']:.3f}s; correctness=PASS", flush=True)
                        continue
                    record = {
                        'schema_version': 1, 'run_id': run_id, 'timestamp_utc': stamp(),
                        'query_id': 'Q12', 'scale_factor': SCALE_FACTOR, 'execution_mode': 'real_multicloud',
                        'placement_id': 'P1', 'plan_id': plan, 'trial': trial, 'join_cloud': dest.upper(),
                        'logical_source_cloud': source.upper(), 'logical_destination_cloud': dest.upper(),
                        'intermediate_rows': int(stage['intermediate_rows']),
                        'intermediate_parquet_bytes': int(stage['intermediate_parquet_bytes']),
                        'stage_seconds': float(stage['stage_seconds']),
                        'join_aggregate_seconds': float(join['join_aggregate_seconds']),
                        'total_seconds': float(stage['stage_seconds']) + transport['curl_http_total_seconds'],
                        'result_row_count': len(result), 'result_sha256': expected_result_hash,
                        'correct_vs_local_sql': True,
                        'network_bytes_measured': transport['curl_size_upload'],
                        'network_seconds_measured': '', 'egress_usd_estimated': '',
                        'duckdb_version': '1.5.6',
                        'python_version': 'source/destination recorded in stage/join', 'threads': 4,
                    }
                    records.append(record)
                    key = f'trial_{trial:02d}_{plan}'
                    save_json(outdir / f'{key}_stage.json', stage)
                    save_json(outdir / f'{key}_join.json', join)
                    save_json(outdir / f'{key}_transport.json', transport)
                    save_json(outdir / f'{key}_result.json', result)
                    write_csv(outdir / 'trial_metrics.csv', records)
                    print(f"{plan} trial={trial}: {record['network_bytes_measured']:,} bytes across "
                          f"{source.upper()}→{dest.upper()}, Q12 correctness=PASS, "
                          f"stage+HTTPS+join={record['total_seconds']:.3f}s", flush=True)
        summary_rows=[]
        for plan in plans:
            subset=[r for r in records if r['plan_id']==plan]
            durations=[float(r['total_seconds']) for r in subset]
            assert len(durations)==args.trials
            summary_rows.append({
                'query_id':'Q12','run_id':run_id,'execution_mode':'real_multicloud',
                'plan_id':plan,'trials':len(durations),
                'warmup_trials_per_plan':warmup_count,
                'alternate_plan_order_by_round':alternate,
                'mean_seconds':statistics.mean(durations),
                'median_seconds':statistics.median(durations),
                'sample_stdev_seconds':statistics.stdev(durations) if len(durations)>1 else None,
                'min_seconds':min(durations),'max_seconds':max(durations),
                'http_payload_bytes':subset[0]['network_bytes_measured'],
                'all_correct':all(r['correct_vs_local_sql'] for r in subset),
                'cost_estimated_usd':None,
                'timing_definition':'stage + HTTPS curl request wall time incl. receiving join',
            })
        save_json(outdir/'paper_summary.json',{
            'schema_version':1,'source_trial_metrics_csv':'trial_metrics.csv',
            'warmup_metrics_csv':'warmup_metrics.csv' if warmup_records else None,
            'include_warmups_in_summary':False,
            'separate_cloud_network_time_measured':False,
            'provider_billed_bytes_measured':False,
            'plans':summary_rows,
        })
        print('Cloud plan statistics saved: '+str(outdir/'paper_summary.json'),flush=True)
        if args.plots:make_cloud_plots(records,outdir)
        write_csv(args.output_dir/'all_query_metrics.csv',records,append=True)
        report.update({'run_status':'success','completed_utc':stamp(),
                       'all_plans_correct':True,'trial_metrics_csv':'trial_metrics.csv',
                       'network_bytes_measured_total':sum(r['network_bytes_measured'] for r in records),
                       'warmup_trials_recorded':len(warmup_records),
                       'egress_usd_estimated':None})
        print('Q12_REAL_MULTICLOUD_PASS',flush=True)
        return 0
    except Exception as exc:
        report.update({'run_status':'failed','completed_utc':stamp(),
                       'error_type':type(exc).__name__,'error_message':str(exc)})
        print(f'Q12 cloud trial failed: {type(exc).__name__}: {exc}',file=sys.stderr)
        raise
    finally:
        # Fail closed: stop worker services FIRST, then remove inbound rules.
        for cloud in ('aws','azure'):
            if changed[f'{cloud}_service']:
                work=f'/opt/egressopt/runtime/q12_{run_key}_{cloud}'
                unit=f'egressopt-q12-{run_key}-{cloud}'
                try:
                    vm_remote(cloud,f'systemctl stop {unit} >/dev/null 2>&1 || true; '
                                    f'rm -rf {work}; echo EGRESSOPT_RUNTIME_CLEANED',
                              f'{cloud}-runtime-cleanup')
                    report['cleanup'][f'{cloud}_runtime']='removed'
                except Exception as exc:
                    report['cleanup'][f'{cloud}_runtime']=f'FAILED: {exc}'
                    print(f'WARNING: {cloud} runtime cleanup failed: {exc}',file=sys.stderr)
        if changed['aws_rule']:
            try:
                close_aws_ingress(changed['aws_sg'],changed['aws_rule'])
                report['cleanup']['aws_ingress']='removed'
            except Exception as exc:
                report['cleanup']['aws_ingress']=f'FAILED: {exc}'
                print(f'WARNING AWS SG RULE REMOVAL FAILED: {exc}',file=sys.stderr)
        if changed['azure_rule']:
            try:
                close_azure_ingress(changed['azure_rule'])
                report['cleanup']['azure_ingress']='removed'
            except Exception as exc:
                report['cleanup']['azure_ingress']=f'FAILED: {exc}'
                print(f'WARNING AZURE NSG RULE REMOVAL FAILED: {exc}',file=sys.stderr)
        # Only power down VMs this command started, leave user-owned running VMs alone.
        if args.stop_all_after_run or not args.keep_cloud_vms_running:
            if args.stop_all_after_run or changed['aws_started']:
                try:
                    run_cli(['aws','ec2','stop-instances','--instance-ids',AWS_INSTANCE,
                             '--region',AWS_REGION,'--profile',AWS_PROFILE,'--output','json'])
                    report['cleanup']['aws_started_here']='stop_requested'
                except Exception as exc:
                    report['cleanup']['aws_started_here']=f'FAILED: {exc}'
            if args.stop_all_after_run or changed['azure_started']:
                try:
                    run_cli(['az','vm','deallocate','--resource-group',AZURE_RESOURCE_GROUP,
                             '--name',AZURE_VM,'--output','none'],timeout=600)
                    report['cleanup']['azure_started_here']='deallocated'
                except Exception as exc:
                    report['cleanup']['azure_started_here']=f'FAILED: {exc}'
        if any(str(x).startswith('FAILED') for x in report['cleanup'].values()):
            report['cleanup_incomplete']=True
            print('WARNING: Manual cleanup required; inspect run.json',file=sys.stderr)
        save_json(outdir/'run.json',report)
        print(f'Cloud experiment report: {outdir}',flush=True)




def run_price_model(args) -> int:
    """Offline scenario pricing from immutable Q12 cloud trial measurements.

    IMPORTANT: only prices reported curl HTTP request-body bytes. The result
    is a hypothetical paid-tier marginal *model*, NOT the actual invoice, and
    it deliberately does not modify original real_multicloud metrics.
    """
    if not args.cloud_run_id or any(x in args.cloud_run_id for x in ('/', '\\', '..')):
        raise ValueError('--mode price_model requires an exact --cloud-run-id (no path separators)')
    folder = args.output_dir / 'q12' / args.cloud_run_id
    run_file = folder / 'run.json'
    csv_file = folder / 'trial_metrics.csv'
    if not run_file.is_file() or not csv_file.is_file():
        raise FileNotFoundError(f'Cloud run data not found under {folder}')
    run = json.loads(run_file.read_text())
    if run.get('run_id') != args.cloud_run_id or run.get('execution_mode') != 'real_multicloud':
        raise ValueError('The selected run is not a real_multicloud Q12 run')
    if run.get('run_status') != 'success' or run.get('all_plans_correct') is not True:
        raise ValueError('Selected cloud run did not complete successfully with all results correct')
    with csv_file.open(newline='') as fh:
        trials = list(csv.DictReader(fh))
    if not trials:
        raise ValueError('Cloud trial_metrics.csv contains no measurements')
    rates = {'AWS': args.aws_egress_rate, 'AZURE': args.azure_egress_rate}
    if any(x < 0 for x in rates.values()):
        raise ValueError('Egress rates must be non-negative')
    priced_trials = []
    by_plan = {}
    expected_sources = {'aws_join': ('AZURE','AWS'), 'azure_join': ('AWS','AZURE')}
    for trial in trials:
        plan = trial['plan_id']
        if plan not in expected_sources:
            raise ValueError(f'Unexpected Q12 plan {plan}')
        source, destination = expected_sources[plan]
        if trial['execution_mode'] != 'real_multicloud' or trial['run_id'] != args.cloud_run_id:
            raise ValueError('Trial belongs to a different run or mode')
        if (trial['logical_source_cloud'],trial['logical_destination_cloud']) != (source,destination):
            raise ValueError(f'Cloud direction mismatch for {plan}')
        if trial['correct_vs_local_sql'].strip().lower() not in ('true','1'):
            raise ValueError(f'An incorrect Q12 trial is present: {plan}')
        size = int(trial['network_bytes_measured'])
        if size <= 0 or size != int(trial['intermediate_parquet_bytes']):
            raise ValueError(f'Invalid/mismatched HTTP payload size for {plan}')
        duration = float(trial['total_seconds'])
        if duration < 0:
            raise ValueError(f'Invalid elapsed time for {plan}')
        amount = (size/1_000_000_000)*rates[source]
        item = {'plan_id':plan,'trial':int(trial['trial']),
                'source_cloud':source,'destination_cloud':destination,
                'http_body_bytes':size,'hypothetical_usd_per_GB':rates[source],
                'payload_only_paid_tier_usd':amount,
                'stage_plus_http_plus_join_seconds':duration,
                'pricing_status':'modeled_not_billed'}
        priced_trials.append(item)
        by_plan.setdefault(plan,[]).append(item)
    summary=[]
    for plan in ('aws_join','azure_join'):
        items=by_plan.get(plan,[])
        if not items:
            continue
        if len(set(x['http_body_bytes'] for x in items)) != 1:
            raise ValueError('Per-trial HTTP payload sizes differ; requires per-trial analysis')
        amounts=[x['payload_only_paid_tier_usd'] for x in items]
        times=[x['stage_plus_http_plus_join_seconds'] for x in items]
        summary.append({'plan_id':plan,'source_cloud':items[0]['source_cloud'],
                        'destination_cloud':items[0]['destination_cloud'],
                        'measured_trials':len(items),'http_body_bytes_per_trial':items[0]['http_body_bytes'],
                        'hypothetical_paid_tier_usd_per_GB':items[0]['hypothetical_usd_per_GB'],
                        'payload_only_paid_tier_usd_per_trial':statistics.mean(amounts),
                        'payload_only_paid_tier_usd_for_measured_trials':sum(amounts),
                        'mean_stage_plus_http_plus_join_seconds':statistics.mean(times),
                        'median_stage_plus_http_plus_join_seconds':statistics.median(times),
                        'sample_stdev_seconds':statistics.stdev(times) if len(times)>1 else None})
    output={
        'schema_version':1,'query_id':'Q12','source_run_id':args.cloud_run_id,
        'source_trial_metrics_csv':'trial_metrics.csv',
        'execution_mode':'offline_price_model_not_a_cloud_trial',
        'unit_definition':'1 GB = 1,000,000,000 bytes',
        'price_lookup_date_utc':'2026-10-15',
        'assumed_internet_egress_routing':'Azure Premium Global Network; AWS standard Internet DTO',
        'rates_usd_per_decimal_GB':rates,
        'first_100GB_month_note':'AWS and Azure both publish first 100GB/month free with conditions; billing depends on existing monthly usage.',
        'prices_may_vary_note':'Rates are editable scenarios, may depend on region, routing preference, contract, tier, and date.',
        'model_limitations':['HTTP POST request-body bytes only; not provider-billed bytes',
                             'Excludes TLS/IP headers, retransmissions and other traffic',
                             'Excludes compute, IPv4 address fees, fixed setup costs and taxes',
                             'Does not claim actual incremental invoice costs',
                             'Not an optimizer comparison'],
        'prices_sources':{
            'aws':'https://aws.amazon.com/ec2/pricing/on-demand/',
            'azure':'https://azure.microsoft.com/en-us/pricing/details/bandwidth/'},
        'plan_summary':summary,
    }
    outfile=folder/'q12_pricing_model.json'
    save_json(outfile,output)
    output_csv=folder/'q12_pricing_model_trials.csv'
    with output_csv.open('w',newline='') as fh:
        writer=csv.DictWriter(fh,fieldnames=list(priced_trials[0])); writer.writeheader(); writer.writerows(priced_trials)
    if args.plots:
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print('Pricing figure skipped: matplotlib is not installed',file=sys.stderr)
        else:
            fig,ax=plt.subplots(figsize=(6.4,3.6))
            ax.bar([s['plan_id'] for s in summary], [s['payload_only_paid_tier_usd_per_trial']*1e6 for s in summary])
            ax.set_title('Q12 conditional paid-tier marginal egress (payload-only)')
            ax.set_ylabel('USD per trial (microdollars; modeled, not billed)')
            ax.text(.5,-.28,'Free monthly allowances and actual billed bytes not applied',
                    transform=ax.transAxes,ha='center',fontsize=8)
            fig.tight_layout()
            fig.savefig(folder/'q12_pricing_model.png',dpi=200,bbox_inches='tight')
            plt.close(fig)
    print('Q12 offline pricing scenario; ZERO additional cloud executions',flush=True)
    for s in summary:
        print(f"{s['plan_id']}: HTTP body={s['http_body_bytes_per_trial']:,} B, "
              f"modeled paid-tier payload USD/trial=${s['payload_only_paid_tier_usd_per_trial']:.9f}, "
              f"mean duration={s['mean_stage_plus_http_plus_join_seconds']*1000:.2f} ms",flush=True)
    print('ACTUAL_BILLING_NOT_ESTABLISHED; 100GB/month free tiers may make invoice transfer charges zero',flush=True)
    print(f'Price scenario saved: {outfile}',flush=True)
    return 0

# -------------------------------------------------------------
# Reproducibility: capture immutable code/data identifiers, and audit
# original trial-level files offline. This does not access cloud VMs.
# -------------------------------------------------------------
def _safe_git_info() -> dict:
    """Capture code revision without recording source contents or credentials."""
    def git(*parts):
        try:
            p = subprocess.run(['git', *parts], capture_output=True, text=True,
                               timeout=10, check=False)
            return p.stdout.strip() if p.returncode == 0 else None
        except (OSError, subprocess.TimeoutExpired):
            return None
    commit = git('rev-parse', 'HEAD')
    dirty = git('status', '--porcelain', '--untracked-files=no') if commit else None
    return {'commit': commit, 'tracked_files_modified': bool(dirty) if dirty is not None else None,
            'git_revision_available': commit is not None}


def capture_q12_local_provenance(args, bundle_sha: str) -> dict:
    """The local environment and hashes as of the actual execution."""
    def digest_optional(path):
        path = Path(path)
        return {'path': str(path), 'sha256': sha256_file(path) if path.is_file() else None,
                'exists': path.is_file()}
    try:
        import duckdb as _duckdb
        duckdb_version = _duckdb.__version__
    except ImportError:
        duckdb_version = None
    return {
        'captured_utc': stamp(), 'capture_stage': 'before_cloud_trials',
        'git': _safe_git_info(),
        'files': {
            'executing_runner': digest_optional(Path(__file__)),
            'q12_sql': digest_optional(args.query_file),
            'archived_reference': digest_optional(args.reference_file),
            'worker_archive': {'path': str(args.deployment_bundle), 'sha256': bundle_sha},
            'dataset_export_script': digest_optional(Path('src/export_tpch_parquet.py')),
            'dataset_generation_script': digest_optional(Path('src/prepare_tpch.py')),
            'dataset_research_manifest_script': digest_optional(Path('src/create_research_manifest.py')),
        },
        'controller': {'python_version': platform.python_version(),
                       'duckdb_version': duckdb_version,
                       'os': platform.platform()},
        'experiment_config': {
            'mode': args.mode, 'query': 'Q12', 'placement': 'P1',
            'scale_factor': 'SF1', 'plans_requested': args.plan,
            'warmup_trials_per_plan': args.warmup_trials,
            'measured_trials_per_plan': args.trials,
            'alternate_plan_order': args.alternate_plan_order,
            'worker_threads': 4,
            'stop_all_after_run_requested': args.stop_all_after_run,
        },
        'dataset_family': 'DuckDB-generated TPC-H-derived SF1; not an audited TPC-H result',
        'notes': ['Cloud-side dataset hashes captured separately on the actual VMs',
                  'Git commit alone is insufficient if tracked files were modified',
                  'HTTPS HTTP-body byte counts are not provider-billed bytes'],
    }


def capture_q12_remote_provenance(cloud: str, release: str) -> dict:
    """Read exact dataset/worker digests, binary versions, and input row counts.

    This runs before trials, outside the benchmark stopwatch. Hashing data
    can influence disk cache state; recorded warm-up trials remain mandatory.
    """
    if cloud == 'aws':
        python_bin = '/opt/egressopt/venv311/bin/python'
        dataset = '/opt/egressopt/data/tpch_sf1/orders.parquet'
        expected_rows = 1500000
    else:
        python_bin = '/opt/egressopt/venv/bin/python'
        dataset = '/opt/egressopt/data/tpch_sf1/lineitem.parquet'
        expected_rows = 6001215
    payload = f"""import json,hashlib,platform,pathlib,duckdb
p = pathlib.Path({dataset!r})
w = pathlib.Path({(release + '/src/worker_api.py')!r})
ref = pathlib.Path({(release + '/results/reference/sf1/q12_results.json')!r})
def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as fh:
        for block in iter(lambda: fh.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()
c=duckdb.connect(database=':memory:')
try:
    row_count=c.execute('SELECT COUNT(*) FROM read_parquet(?)', [str(p)]).fetchone()[0]
finally:
    c.close()
assert row_count == {expected_rows}, (row_count, {expected_rows})
print('EGRESSOPT_PROVENANCE=' + json.dumps({{'provider': {cloud!r}, 'dataset_path':str(p),
  'dataset_sha256':digest(p), 'dataset_bytes':p.stat().st_size,'dataset_rows':row_count,
  'worker_api_sha256':digest(w),'reference_sha256':digest(ref),
  'python_version':platform.python_version(),'duckdb_version':duckdb.__version__,
  'os':platform.platform()}},sort_keys=True))"""
    remote_script = "set -eu\n" + python_bin + " - <<'EGRESSOPT_REPRO_PY'\n" + payload + "\nEGRESSOPT_REPRO_PY\n"
    output = vm_remote(cloud, remote_script, f'{cloud}-reproducibility-provenance')
    matching = [line.split('EGRESSOPT_PROVENANCE=', 1)[1].strip()
                for line in output.splitlines() if 'EGRESSOPT_PROVENANCE=' in line]
    if len(matching) != 1:
        raise RuntimeError(f'Expected one cloud provenance record from {cloud}, got {len(matching)}')
    return json.loads(matching[0])


def audit_q12_saved_run(args) -> int:
    """Offline, fail-closed verification of raw results and their summary.

    This tests internal consistency of archived evidence. It cannot
    retroactively prove a past runner version or dataset hash if omitted.
    """
    name = args.cloud_run_id
    if not name or not name.replace('_', '').isalnum() or '/' in name or '\\' in name:
        raise ValueError('--mode audit_run needs the exact --cloud-run-id')
    folder = args.output_dir / 'q12' / name
    p_run = folder / 'run.json'
    p_csv = folder / 'trial_metrics.csv'
    p_summary = folder / 'paper_summary.json'
    if not all(f.is_file() for f in (p_run, p_csv, p_summary)):
        raise FileNotFoundError(f'Incomplete Q12 evidence in {folder}')
    run, summary = json.loads(p_run.read_text()), json.loads(p_summary.read_text())
    reference = load_archived_q12(args.reference_file)
    ref_hash = hash_result(reference)
    with p_csv.open(newline='') as stream:
        trials = list(csv.DictReader(stream))
    plans = run.get('plans')
    count = run.get('trials')
    if not isinstance(plans, list) or not plans or type(count) is not int or count <= 0:
        raise ValueError('Invalid run plan or trial count')
    if len(trials) != len(plans)*count:
        raise ValueError(f'Expected {len(plans)*count} measured trials; got {len(trials)}')
    if run.get('run_id') != name or run.get('execution_mode') != 'real_multicloud':
        raise ValueError('Run identity or execution mode mismatch')
    if run.get('run_status') != 'success' or run.get('all_plans_correct') is not True:
        raise ValueError('Selected cloud run is not recorded as fully correct')
    paths = [p_run, p_csv, p_summary]
    observed = set()
    mapping = {'aws_join': ('azure', 'aws', 30988, 55745),
               'azure_join': ('aws', 'azure', 1500000, 1759321)}
    for row in trials:
        plan = row.get('plan_id')
        if plan not in plans or plan not in mapping:
            raise ValueError(f'Unexpected plan in trial CSV: {plan}')
        idx = int(row['trial'])
        if idx not in range(1, count+1) or (plan, idx) in observed:
            raise ValueError('Duplicate or invalid plan/trial index')
        observed.add((plan, idx))
        source, target, expected_rows, expected_bytes = mapping[plan]
        prefix = f'trial_{idx:02d}_{plan}'
        files = [folder / f'{prefix}_{suffix}.json' for suffix in ('stage','join','transport','result')]
        if not all(f.is_file() for f in files):
            raise FileNotFoundError(f'Missing raw trial evidence for {prefix}')
        paths.extend(files)
        stage, join, transport, result = [json.loads(f.read_text()) for f in files]
        if result != reference or hash_result(result) != ref_hash:
            raise ValueError(f'Archived SQL-result mismatch: {prefix}')
        if (stage['actual_worker_cloud'], join['actual_worker_cloud']) != (source, target):
            raise ValueError(f'Cloud direction mismatch: {prefix}')
        if (row['logical_source_cloud'].lower(), row['logical_destination_cloud'].lower()) != (source, target):
            raise ValueError(f'CSV direction mismatch: {prefix}')
        if (int(stage['intermediate_rows']), int(stage['intermediate_parquet_bytes'])) != (expected_rows, expected_bytes):
            raise ValueError(f'Unexpected SF1 intermediate shape: {prefix}')
        if (int(row['intermediate_rows']), int(row['intermediate_parquet_bytes']),
            int(row['network_bytes_measured']), int(transport['curl_size_upload'])) != \
           (expected_rows, expected_bytes, expected_bytes, expected_bytes):
            raise ValueError(f'Payload or row count inconsistent: {prefix}')
        if stage['intermediate_sha256'] != join['intermediate_sha256']:
            raise ValueError(f'Intermediate hashes disagree: {prefix}')
        if join['result_sha256'] != ref_hash or join['correct_vs_archived_reference'] is not True:
            raise ValueError(f'Join correctness evidence mismatch: {prefix}')
        if row['result_sha256'] != ref_hash or row['correct_vs_local_sql'].lower() != 'true':
            raise ValueError(f'CSV correctness evidence mismatch: {prefix}')
        duration = float(stage['stage_seconds']) + float(transport['curl_http_total_seconds'])
        if abs(duration - float(row['total_seconds'])) > 1e-8:
            raise ValueError(f'Timing arithmetic inconsistent: {prefix}')
    warmup_count = run.get('warmup_trials_per_plan', 0)
    warmup_file = folder/'warmup_metrics.csv'
    if warmup_count:
        if not warmup_file.is_file():
            raise FileNotFoundError('Missing warmup_metrics.csv')
        paths.append(warmup_file)
        with warmup_file.open(newline='') as stream:
            warmups = list(csv.DictReader(stream))
        if len(warmups) != len(plans)*warmup_count:
            raise ValueError('Warm-up trial count mismatch')
        for w in warmups:
            w_idx, plan = int(w['round']), w['plan_id']
            if w_idx < 1 or w_idx > warmup_count or plan not in plans:
                raise ValueError('Unexpected warm-up trial')
            for suffix in ('stage','join','transport'):
                f = folder / f'warmup_{w_idx:02d}_{plan}_{suffix}.json'
                if not f.is_file():
                    raise FileNotFoundError(f'Missing warm-up evidence: {f}')
                paths.append(f)
    if summary.get('include_warmups_in_summary') is not False:
        raise ValueError('Warm-up exclusion is not recorded in paper_summary.json')
    for plan_summary in summary['plans']:
        plan = plan_summary['plan_id']
        values = [float(x['total_seconds']) for x in trials if x['plan_id'] == plan]
        if not values or len(values) != count:
            raise ValueError('Summary plan missing or count inconsistent')
        for key, val in [('mean_seconds', statistics.mean(values)),
                         ('median_seconds', statistics.median(values)),
                         ('min_seconds', min(values)), ('max_seconds', max(values))]:
            if abs(float(plan_summary[key]) - val) > 1e-8:
                raise ValueError(f'Statistic {key} inconsistent for {plan}')
    snapshot = {'schema_version': 1, 'audited_utc': stamp(),
                'original_run_id': name, 'verification': 'PASS',
                'validation_scope': 'Local archived output integrity and numerical consistency only',
                'reference_sql_result_sha256': ref_hash,
                'recorded_measured_trials': len(trials),
                'recorded_warmups': warmup_count*len(plans),
                'evidence_hashes_sha256_at_audit_time': {
                    str(p.relative_to(folder)): sha256_file(p) for p in sorted(set(paths))},
                'past_run_provenance': run.get('reproducibility'),
                'reproduction_limitations': []}
    if not run.get('reproducibility'):
        snapshot['reproduction_limitations'].append(
            'Exact runner version and cloud dataset SHA256 were not captured in the original run; cannot retroactively verify them.')
    cleanup = run.get('cleanup', {})
    if any(v == 'stop_requested' for v in cleanup.values()):
        snapshot['reproduction_limitations'].append(
            'AWS stop_requested records an API request, not confirmed instance-stopped state.')
    snapshot['reproduction_limitations'].extend([
        'An evidence hash generated today cannot prove the file was never modified beforehand.',
        'This audit does not rerun the experiment, access provider billing or validate cloud power state.',
        'Results apply to one configuration and SF1 dataset; independent run-to-run replication remains required.'])
    report_path = folder/'q12_reproducibility_audit.json'
    save_json(report_path, snapshot)
    print(f'Q12_OFFLINE_EVIDENCE_AUDIT_PASS ({len(trials)} measured trials; {warmup_count*len(plans)} warm-ups)')
    print('Original run code/data provenance: ' + ('CAPTURED' if run.get('reproducibility') else 'NOT CAPTURED AT RUN TIME'))
    print('This is an evidence-consistency check, NOT proof of independent reproduction.')
    print(f'Reproducibility audit: {report_path}')
    return 0

# ---------------------------------------------------------------------------
# Q12 finalization: entirely offline, reproducible paper artifacts and a
# *prototype* two-candidate, latency-constrained egress selector. This is not
# a novel multi-cloud query optimizer or a provider invoice reconciliation.
# ---------------------------------------------------------------------------

def _q12_quantile_linear(values: list[float], fraction: float) -> float:
    if not values or not 0 <= fraction <= 1:
        raise ValueError('Invalid percentile sample or quantile')
    ordered = sorted(float(x) for x in values)
    position = (len(ordered)-1) * fraction
    lower = int(position)
    upper = min(lower+1, len(ordered)-1)
    return ordered[lower] + (position-lower) * (ordered[upper]-ordered[lower])


def _q12_paired_bootstrap_ci(a: dict[int, float], b: dict[int, float],
                             repeats: int = 10000, seed: int = 121215) -> dict:
    """Bootstrap differences in workflow time, by within-round trial number.

    Interleaved trials from the same run are matched by trial index, but true
    independent-session uncertainty cannot be learned from a single run.
    """
    import random
    if sorted(a) != sorted(b) or not a:
        raise ValueError('Both plans must have the same unique measured trial IDs')
    diffs = [b[i]-a[i] for i in sorted(a)]
    observed = statistics.mean(diffs)
    rng = random.Random(seed)
    sims = [statistics.mean(rng.choices(diffs, k=len(diffs))) for _ in range(repeats)]
    return {
        'aws_join_minus_azure_join_seconds': -observed,
        'azure_join_minus_aws_join_seconds': observed,
        'illustrative_bootstrap_95pct_CI_azure_minus_aws_seconds': [
            _q12_quantile_linear(sims, .025), _q12_quantile_linear(sims, .975)],
        'bootstrap_resamples': repeats,
        'bootstrap_seed': seed,
        'interpretation': 'Within-run paired block bootstrap, descriptive only; '
                          'not a confidence interval for independent cloud sessions',
    }


def _q12_latex_escape(s: str) -> str:
    # LaTeX output contains only known, validated plan IDs.
    return s.replace('_', r'\_')


def _q12_figures(analysis: dict, out_dir: Path, *, plots: bool) -> list[str]:
    if not plots:
        return []
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError('Matplotlib is required when --plots is specified') from exc
    figfiles = []
    plans = ['aws_join', 'azure_join']
    trial_map = analysis['measured_trial_durations_ms']
    fig, ax = plt.subplots(figsize=(6.8, 3.55))
    for p in plans:
        if p not in trial_map:
            continue
        series = trial_map[p]
        ax.plot([x['trial'] for x in series], [x['duration_ms'] for x in series],
                marker='o', linewidth=1.5, markersize=4,
                label=f'{p} (join on {"AWS" if p == "aws_join" else "Azure"})')
    ax.set(xlabel='Measured trial (warm-ups excluded)',
           ylabel='Stage + HTTPS request + join (ms)',
           title='Q12 | Real AWS–Azure distributed execution')
    ax.grid(alpha=.22)
    ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        name = f'fig_q12_trial_duration.{ext}'
        fig.savefig(out_dir/name, dpi=220)
        figfiles.append(name)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6.5, 3.5))
    present = [p for p in plans if p in analysis['plan_summaries']]
    heights = [analysis['plan_summaries'][p]['http_payload_bytes_per_trial'] for p in present]
    bars = ax.bar([_q12_latex_escape(p).replace('\\_', '_') for p in present], heights)
    ax.bar_label(bars, labels=[f'{x:,} B' for x in heights], padding=3, fontsize=9)
    ax.set(ylabel='Transferred Parquet HTTP-body bytes (not billed bytes)',
           title='Q12 | Directional intermediate transfer volume')
    ax.set_ylim(0, max(heights)*1.18)
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        name = f'fig_q12_transfer_bytes.{ext}'
        fig.savefig(out_dir/name, dpi=220)
        figfiles.append(name)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6.5, 3.5))
    for p in present:
        s = analysis['plan_summaries'][p]
        ax.scatter(s['empirical_p95_workflow_ms_linear'], s['modeled_marginal_egress_micro_usd_per_query'], s=75,
                   label=p)
        ax.annotate(p, (s['empirical_p95_workflow_ms_linear'], s['modeled_marginal_egress_micro_usd_per_query']),
                    xytext=(6, 5), textcoords='offset points', fontsize=9)
    ax.axvline(analysis['latency_slo_ms'], linestyle='--', linewidth=1.2,
               label='Illustrative p95 workflow SLO')
    ax.set(xlabel='Empirical p95 measured workflow (ms)',
           ylabel='Modeled marginal payload-only egress (µUSD/query)',
           title='Q12 | Two-candidate latency and cost trade-off')
    ax.legend(frameon=False, fontsize=8)
    ax.grid(alpha=.2)
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        name = f'fig_q12_cost_latency.{ext}'
        fig.savefig(out_dir/name, dpi=220)
        figfiles.append(name)
    plt.close(fig)
    return figfiles


def finalize_q12_offline(args) -> int:
    """Fail-closed evidence audit, cost model, plan selection and paper figures.

    Does NOT access any cloud API and never modifies original raw trial files.
    Every numerical figure and table is regenerated from saved full-precision CSV.
    """
    if not args.cloud_run_id or not args.cloud_run_id.replace('_', '').isalnum():
        raise ValueError('--mode finalize requires the exact --cloud-run-id')
    if not 0 < args.latency_slo_ms < 1e7:
        raise ValueError('--latency-slo-ms must be positive and finite')
    if (not all(__import__('math').isfinite(x) and x >= 0 for x in
                (args.aws_egress_rate, args.azure_egress_rate))):
        raise ValueError('Non-negative finite directional egress rates are required')
    run_dir = args.output_dir/'q12'/args.cloud_run_id
    # First establish saved evidence consistency; refuse finalization otherwise.
    audit_q12_saved_run(args)
    run = json.loads((run_dir/'run.json').read_text())
    with (run_dir/'trial_metrics.csv').open(newline='') as stream:
        entries = list(csv.DictReader(stream))
    if set(run['plans']) != {'aws_join', 'azure_join'}:
        raise ValueError('Q12 finalizer requires BOTH measured join plans')
    if not run.get('alternate_plan_order_by_round'):
        raise ValueError('Controlled Q12 finalizer requires an alternating plan-order run')
    if run.get('warmup_trials_per_plan', 0) < 1:
        raise ValueError('Controlled Q12 finalizer requires recorded warm-up trials')
    by_plan: dict[str, list[dict]] = {}
    for plan in ('aws_join', 'azure_join'):
        subset = sorted([r for r in entries if r['plan_id'] == plan],
                        key=lambda r: int(r['trial']))
        if len(subset) != run['trials']:
            raise ValueError(f'Missing measured trials for {plan}')
        by_plan[plan] = subset
    rates = {'aws': args.aws_egress_rate, 'azure': args.azure_egress_rate}
    plan_summaries = {}
    durations_by_id = {}
    trial_table = []
    for plan, source in [('aws_join', 'azure'), ('azure_join', 'aws')]:
        subset = by_plan[plan]
        values = [float(x['total_seconds']) for x in subset]
        by_id = {int(x['trial']): float(x['total_seconds']) for x in subset}
        durations_by_id[plan] = by_id
        byte_counts = {int(x['network_bytes_measured']) for x in subset}
        if len(byte_counts) != 1:
            raise ValueError('Bytes vary by trial; requires a per-trial cost model')
        bytes_ = next(iter(byte_counts))
        p95 = _q12_quantile_linear(values, .95)
        marginal_usd = bytes_ / 1_000_000_000 * rates[source]
        plan_summaries[plan] = {
            'plan_id': plan, 'source_cloud': source, 'join_cloud': 'aws' if plan=='aws_join' else 'azure',
            'measured_trials': len(values),
            'http_payload_bytes_per_trial': bytes_,
            'mean_workflow_ms': statistics.mean(values)*1000,
            'median_workflow_ms': statistics.median(values)*1000,
            'sample_stddev_workflow_ms': statistics.stdev(values)*1000 if len(values)>1 else None,
            'empirical_p95_workflow_ms_linear': p95*1000,
            'modeled_source_rate_USD_decimal_GB': rates[source],
            'modeled_marginal_egress_USD_per_query': marginal_usd,
            'modeled_marginal_egress_micro_usd_per_query': marginal_usd*1_000_000,
            'p95_meets_latency_SLO': p95*1000 <= args.latency_slo_ms,
            'all_correct': all(x['correct_vs_local_sql'].strip().lower() in ('true','1') for x in subset),
        }
        for r in subset:
            trial_table.append({'plan':plan, 'trial':int(r['trial']),
                                'workflow_ms':float(r['total_seconds'])*1000,
                                'http_body_bytes':int(r['network_bytes_measured']),
                                'correct':r['correct_vs_local_sql']})
    eligible = [s for s in plan_summaries.values() if s['p95_meets_latency_SLO']]
    eligible.sort(key=lambda s:(s['modeled_marginal_egress_USD_per_query'],
                                s['mean_workflow_ms'],s['plan_id']))
    selected = eligible[0]['plan_id'] if eligible else None
    latency_fastest = min(plan_summaries.values(), key=lambda s:s['mean_workflow_ms'])['plan_id']
    egress_cheapest = min(plan_summaries.values(),
                          key=lambda s:s['modeled_marginal_egress_USD_per_query'])['plan_id']
    a = plan_summaries['aws_join']
    b = plan_summaries['azure_join']
    diff = _q12_paired_bootstrap_ci(durations_by_id['aws_join'], durations_by_id['azure_join'])
    csv_hash = sha256_file(run_dir/'trial_metrics.csv')
    ref_hash = sha256_file(args.reference_file)
    params = {'source_run_id':args.cloud_run_id,
              'source_csv_sha256':csv_hash,
              'analysis_runner_sha256':sha256_file(Path(__file__)),
              'reference_file_sha256':ref_hash,
              'latency_slo_ms':args.latency_slo_ms,
              'aws_egress_rate_USD_GB':args.aws_egress_rate,
              'azure_egress_rate_USD_GB':args.azure_egress_rate}
    digest12 = hashlib.sha256(json.dumps(params,sort_keys=True).encode()).hexdigest()[:12]
    out = run_dir/'q12_analysis'/digest12
    out.mkdir(parents=True,exist_ok=True)
    analysis = {
        'schema_version':1,
        'execution_mode':'offline_finalize_from_real_multicloud_evidence',
        'original_cloud_run_id':args.cloud_run_id,
        'analysis_parameters':params,
        'analysis_id':digest12,
        'latency_slo_ms':args.latency_slo_ms,
        'SLO_definition':'Empirical 95th percentile of measured stage+curl+join time, '
                         'linear interpolation across 10 trials. Excludes warm-ups, VM start and management-plane overhead.',
        'pricing_definition':'Hypothetical marginal paid-tier Internet DTO using HTTP body bytes, '
                             'decimal GB; NOT actual billing or exact wire-byte charge.',
        'plan_summaries':plan_summaries,
        'measured_trial_durations_ms':{
            p:[{'trial':i, 'duration_ms':v*1000} for i,v in sorted(d.items())]
            for p,d in durations_by_id.items()},
        'relative_transfer_byte_reduction_aws_vs_azure':1 - a['http_payload_bytes_per_trial']/b['http_payload_bytes_per_trial'],
        'relative_mean_workflow_reduction_aws_vs_azure':1-a['mean_workflow_ms']/b['mean_workflow_ms'],
        'relative_modeled_egress_reduction_aws_vs_azure':
            1-a['modeled_marginal_egress_USD_per_query']/b['modeled_marginal_egress_USD_per_query']
            if b['modeled_marginal_egress_USD_per_query'] else None,
        'within_run_paired_difference':diff,
        'policy':{
            'name':'two-plan_min_modeled_egress_subject_to_empirical_p95_latency',
            'candidate_set':['aws_join', 'azure_join'],
            'eligible_plans_by_p95_SLO':[s['plan_id'] for s in eligible],
            'selected_plan':selected,
            'status':'SELECTED' if selected else 'INFEASIBLE',
            'fastest_mean_baseline_plan':latency_fastest,
            'lowest_modeled_egress_plan':egress_cheapest,
            'is_new_optimizer_novelty_claim':False,
            'note':'Offline two-candidate policy sanity check, not yet a general query optimizer. '
                   'No performance claim against unseen workloads or independent sessions.'},
        'reproducibility':{
            'original_run_provenance_captured':bool(run.get('reproducibility')),
            'original_trial_level_consistency':'validated by audit_run',
            'controlled_warmups_per_plan':run['warmup_trials_per_plan'],
            'measured_trials_per_plan':run['trials'],
            'alternate_plan_order_by_round':True,
            'analysis_uses_full_precision_original_csv':True,
            'raw_cloud_results_not_changed':True,
            'recorded_cleanup':run.get('cleanup', {}),
            'cleanup_is_not_independently_reverified':True,
        },
    }
    save_json(out/'q12_final_analysis.json',analysis)
    with (out/'q12_measured_trials.csv').open('w',newline='') as fh:
        cols=['plan','trial','workflow_ms','http_body_bytes','correct']
        writer=csv.DictWriter(fh,fieldnames=cols)
        writer.writeheader()
        writer.writerows(sorted(trial_table,key=lambda r:(r['trial'],r['plan'])))
    tex = r'''% Autogenerated from full-precision Q12 cloud measurements. Do not hand-edit.
% Experiment: __RUN__; price scenario only (not provider invoice).
\begin{table}[t]
\caption{Q12 two-cloud plan comparison at SF1 (warm-ups excluded).}
\label{tab:q12_multicloud}
\centering
\begin{tabular}{lrr}
\hline
Metric & Join on AWS & Join on Azure \\
\hline
Measured runs & __NA__ & __NB__ \\
HTTP-body bytes & __BA__ & __BB__ \\
Mean workflow (ms) & __MA__ & __MB__ \\
Median workflow (ms) & __MDA__ & __MDB__ \\
Std. dev. (ms) & __SDA__ & __SDB__ \\
Empirical p95 (ms) & __P95A__ & __P95B__ \\
Correct results & __CA__ & __CB__ \\
\hline
\end{tabular}
\vspace{1mm}
\footnotesize{Workflow = source stage + HTTPS request including receiver join;
not isolated network latency. Bytes are HTTP request-body payloads, not billed bytes.}
\end{table}
'''
    substitutions={
        '__RUN__':args.cloud_run_id,
        '__NA__':str(a['measured_trials']), '__NB__':str(b['measured_trials']),
        '__BA__':format(a['http_payload_bytes_per_trial'],','),
        '__BB__':format(b['http_payload_bytes_per_trial'],','),
        '__MA__':f"{a['mean_workflow_ms']:.2f}", '__MB__':f"{b['mean_workflow_ms']:.2f}",
        '__MDA__':f"{a['median_workflow_ms']:.2f}", '__MDB__':f"{b['median_workflow_ms']:.2f}",
        '__SDA__':f"{a['sample_stddev_workflow_ms']:.2f}",
        '__SDB__':f"{b['sample_stddev_workflow_ms']:.2f}",
        '__P95A__':f"{a['empirical_p95_workflow_ms_linear']:.2f}",
        '__P95B__':f"{b['empirical_p95_workflow_ms_linear']:.2f}",
        '__CA__':f"{a['measured_trials']}/{a['measured_trials']}",
        '__CB__':f"{b['measured_trials']}/{b['measured_trials']}",
    }
    for key,value in substitutions.items():
        tex=tex.replace(key,value)
    (out/'q12_ieee_table.tex').write_text(tex,encoding='utf-8')
    figure_paths=_q12_figures(analysis,out,plots=args.plots)
    readme = f'''# EgressOpt Q12 -- reproducible offline paper analysis

Source experiment: {args.cloud_run_id}
Raw trial CSV SHA256: {csv_hash}
Reference JSON SHA256: {ref_hash}
Scale factor: SF1; two-cloud placement P1 only.

Run this from the EgressOpt project root (requires original experiment files):

    python experiments/q12.py --mode reproduce --run-id {args.cloud_run_id} --latency-slo-ms {args.latency_slo_ms:g} --aws-egress-rate {args.aws_egress_rate:g} --azure-egress-rate {args.azure_egress_rate:g}

Outputs: q12_final_analysis.json, q12_ieee_table.tex, q12_measured_trials.csv,
optional PNG/PDF figures, q12_source_checksums.json.

The optimizer selection is a two-plan sanity check under a user-supplied
illustrative p95 latency SLO, not a novel general-purpose query optimizer.
Cloud billed bytes/charges were not measured. Raw evidence audit is offline
and cannot recover missing original-run code/data provenance.
'''
    (out/'README.md').write_text(readme,encoding='utf-8')
    audited = json.loads((run_dir/'q12_reproducibility_audit.json').read_text())
    checksum_manifest = {
        'source_run_id':args.cloud_run_id,
        'analysis_id':digest12,
        'evidence_sha256_at_analysis_time':audited['evidence_hashes_sha256_at_audit_time'],
        'reference_sha256':ref_hash,
        'figures_generated':figure_paths,
        'source_had_execution_provenance':bool(run.get('reproducibility')),
        'analysis_outputs_sha256':{
            p.name:sha256_file(p) for p in sorted(out.iterdir()) if p.is_file() and p.name != 'q12_source_checksums.json'},
    }
    save_json(out/'q12_source_checksums.json', checksum_manifest)
    print(f'Q12_FINALIZATION_PASS; measured={len(entries)}; correctness=PASS; offline only')
    for plan in ('aws_join','azure_join'):
        s=plan_summaries[plan]
        print(f"{plan}: mean={s['mean_workflow_ms']:.3f} ms, "
              f"p95={s['empirical_p95_workflow_ms_linear']:.3f} ms, "
              f"bytes={s['http_payload_bytes_per_trial']:,}, "
              f"hypothetical_marginal_egress_USD=${s['modeled_marginal_egress_USD_per_query']:.9f}")
    print(f'Latency-constrained two-plan policy (p95 SLO {args.latency_slo_ms:g} ms): '
          f'{selected or "NO_FEASIBLE_PLAN"}')
    print('Optimizer contribution is NOT established; this is a two-plan offline selection check.')
    print(f'Q12 paper/reproduction artifacts: {out}')
    return 0



def q12_run_root(args: argparse.Namespace) -> Path:
    """Versioned run root. SF1 deliberately preserves the historical location."""
    root = args.output_dir / 'q12'
    return root if args.sf == 1 else root / f'sf{args.sf}'


def select_q12_saved_run(args: argparse.Namespace, *, controlled: bool) -> str:
    """Choose the latest *completed* suitable archived run, never an active run."""
    if args.cloud_run_id:
        return args.cloud_run_id
    base = q12_run_root(args)
    if not base.is_dir():
        raise FileNotFoundError(f'No Q12 evidence folder: {base}')
    matches = []
    for folder in base.iterdir():
        if not folder.is_dir() or not (folder/'run.json').is_file():
            continue
        try:
            details = json.loads((folder/'run.json').read_text())
        except (ValueError, OSError):
            continue
        if (details.get('run_id') != folder.name
                or details.get('execution_mode') != 'real_multicloud'
                or details.get('run_status') != 'success'
                or details.get('all_plans_correct') is not True
                or not (folder/'trial_metrics.csv').is_file()
                or not (folder/'paper_summary.json').is_file()):
            continue
        if controlled and not (
                set(details.get('plans', [])) == {'aws_join', 'azure_join'}
                and details.get('warmup_trials_per_plan', 0) >= 1
                and details.get('alternate_plan_order_by_round') is True
                and (folder/'warmup_metrics.csv').is_file()):
            continue
        matches.append(folder.name)
    if not matches:
        kind = 'controlled, successful' if controlled else 'successful'
        raise FileNotFoundError(
            f'No {kind} Q12 cloud runs found under {base}. '
            'Provide --run-id to select a specific archived run.')
    selected = sorted(matches)[-1]
    print(f'Auto-selected {"controlled " if controlled else ""}Q12 run: {selected}', flush=True)
    return selected


def run_q12_audit(args: argparse.Namespace) -> int:
    args.cloud_run_id = select_q12_saved_run(args, controlled=False)
    return audit_q12_saved_run(args)


def run_q12_reproduce(args: argparse.Namespace) -> int:
    args.cloud_run_id = select_q12_saved_run(args, controlled=True)
    # These three graphical output pairs are part of the reproduce contract.
    args.plots = True
    return finalize_q12_offline(args)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["cloud", "reproduce", "audit", "local_emulation", "worker_local", "cloud_setup", "cloud_preflight", "price_model", "audit_run", "finalize"],
                        default="reproduce",
                        help="worker_local=localhost; cloud_setup=Azure staging; cloud_preflight=read-only audit; cloud=real TLS AWS/Azure experiment")
    parser.add_argument("--cloud-run-id", "--run-id", dest="cloud_run_id",
                        default=os.getenv("EGRESSOPT_Q12_RUN_ID"),
                        help="Select archived run; default: latest successful suitable run")
    parser.add_argument("--sf", "--scale-factor", dest="sf", type=int, default=DEFAULT_SF,
                        help="Global TPC-H-derived scale factor (or EGRESSOPT_SF); SF1 validated")
    parser.add_argument("--latency-slo-ms", type=float, default=250.0,
                        help="User scenario p95 latency constraint (ms) for offline two-plan selection")
    parser.add_argument("--aws-egress-rate", type=float, default=0.090,
                        help="Assumed paid-tier AWS Internet egress USD/decimal GB; default 0.090")
    parser.add_argument("--azure-egress-rate", type=float, default=0.087,
                        help="Assumed paid-tier Azure Premium Internet egress USD/decimal GB; default 0.087")
    parser.add_argument("--allow-temporary-ingress", action="store_true", default=None,
                        help="Approve temporary AWS/Azure TCP 8443 /32 firewall rules (removed after trials)")
    parser.add_argument("--deny-temporary-ingress", action="store_true",
                        help="Deny managed temporary /32 ingress and abort cloud mode")
    parser.add_argument("--keep-cloud-vms-running", action="store_true",
                        help="Do not power down VMs which cloud mode started")
    parser.add_argument("--stop-all-after-run", action="store_true", default=None,
                        help="Stop AWS and deallocate Azure after a cloud run, even if already running")
    parser.add_argument("--worker-url", default="http://127.0.0.1:8766")
    parser.add_argument("--deployment-bundle", type=Path, default=DEPLOYMENT_BUNDLE)
    parser.add_argument("--keep-azure-running", action="store_true",
                        help="Do not deallocate the Azure VM if cloud_setup starts it")
    parser.add_argument("--token-file", type=Path,
                        default=Path("~/.egressopt_worker_token"))
    parser.add_argument("--plan", choices=["both", "aws_join", "azure_join"], default="both")
    parser.add_argument("--trials", type=int, default=10)
    parser.add_argument("--warmup-trials", type=int, default=None,
                        help="Discard from performance summary but retain in warmup_metrics.csv (per plan)")
    parser.add_argument("--alternate-plan-order", action="store_true", default=None,
                        help="Alternate which plan runs first each round to mitigate plan-order bias")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--plots", action="store_true", default=None,
                        help="Produce plots labeled by execution mode")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--query-file", type=Path, default=Path("queries/tpch/q12.sql"))
    parser.add_argument("--reference-file", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("results/experiments"))
    args = parser.parse_args()
    global SCALE_FACTOR
    if args.sf < 1:
        parser.error('--sf must be a positive integer')
    SCALE_FACTOR = f'SF{args.sf}'
    if args.data_dir is None:
        args.data_dir = Path(f'data/tpch/parquet_sf{args.sf}')
    if args.reference_file is None:
        args.reference_file = Path(f'results/reference/sf{args.sf}/q12_results.json')
    if args.sf not in VALIDATED_SCALE_FACTORS:
        parser.error(f'{SCALE_FACTOR} is not validated. SF1 is the only supported scale '
                     'until SF10/SF100 data, reference answers, cloud workers and '
                     'intermediate-shape expectations have been verified.')
    if args.warmup_trials is None:
        args.warmup_trials = 2 if args.mode == 'cloud' else 0
    if args.alternate_plan_order is None:
        args.alternate_plan_order = (args.mode == 'cloud')
    if args.plots is None:
        args.plots = args.mode in ('cloud', 'reproduce')
    if args.mode == 'cloud':
        # --mode cloud is an explicit request to run managed temporary TLS and
        # source-restricted inbound rules. Deny explicitly with the new flag.
        if args.deny_temporary_ingress:
            args.allow_temporary_ingress = False
        elif args.allow_temporary_ingress is None:
            args.allow_temporary_ingress = True
        if args.stop_all_after_run is None:
            args.stop_all_after_run = not args.keep_cloud_vms_running
    elif args.stop_all_after_run is None:
        args.stop_all_after_run = False
    if args.warmup_trials < 0:
        parser.error("--warmup-trials must not be negative")
    if args.trials < 1 or args.threads < 1:
        parser.error("--trials and --threads must be positive")
    if args.mode == 'reproduce':
        return run_q12_reproduce(args)
    if args.mode == 'audit':
        return run_q12_audit(args)
    if args.mode == "finalize":
        args.cloud_run_id = select_q12_saved_run(args, controlled=True)
        return finalize_q12_offline(args)
    if args.mode == "audit_run":
        return run_q12_audit(args)
    if args.mode == "price_model":
        args.cloud_run_id = select_q12_saved_run(args, controlled=False)
        return run_price_model(args)
    if args.mode == "cloud":
        return run_real_multicloud(args)
    if args.mode in ("worker_local", "local_emulation"):
        try:
            import duckdb
        except ImportError:
            parser.error("DuckDB is not installed in the active Python environment")
    if args.mode == "worker_local":
        return run_worker_local(args, duckdb)
    if args.mode == "cloud_setup":
        return run_cloud_setup(args)
    if args.mode == "cloud_preflight":
        return run_cloud_preflight(args)

    orders = args.data_dir / "orders.parquet"
    lineitem = args.data_dir / "lineitem.parquet"
    for p in (orders, lineitem, args.query_file):
        if not p.is_file():
            parser.error(f"Required input missing: {p}")

    baseline, baseline_seconds = run_local_reference(
        duckdb, orders, lineitem, args.query_file, args.threads
    )
    archived = load_archived_q12(args.reference_file)
    if archived != baseline:
        raise RuntimeError(
            "FAIL: Q12 fresh SQL result does not match the archived SF1 reference. "
            "No experiment results have been written."
        )
    print("Archived SF1 Q12 reference: PASS", flush=True)
    baseline_sha = hash_result(baseline)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_dir = args.output_dir / "q12" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    save_json(run_dir / "q12_local_reference.json", baseline)
    plans = ["aws_join", "azure_join"] if args.plan == "both" else [args.plan]
    entries: list[dict] = []
    overall_ok = True

    for trial in range(1, args.trials + 1):
        for plan in plans:
            measurement = run_local_plan(duckdb, plan, orders, lineitem, args.threads)
            correct = (measurement["result"] == baseline)
            overall_ok &= correct
            result_sha = hash_result(measurement["result"])
            save_json(run_dir / f"trial_{trial:02d}_{plan}_result.json", measurement["result"])
            record = {
                "schema_version": "1", "run_id": run_id, "timestamp_utc": stamp(),
                "query_id": QUERY_ID, "scale_factor": SCALE_FACTOR,
                "execution_mode": EXECUTION_MODE, "placement_id": PLACEMENT_ID,
                "plan_id": plan, "trial": trial,
                "join_cloud": measurement["join_cloud"],
                "logical_source_cloud": measurement["logical_source_cloud"],
                "logical_destination_cloud": measurement["logical_destination_cloud"],
                "intermediate_rows": measurement["intermediate_rows"],
                "intermediate_parquet_bytes": measurement["intermediate_parquet_bytes"],
                "stage_seconds": measurement["stage_seconds"],
                "join_aggregate_seconds": measurement["join_aggregate_seconds"],
                "total_seconds": measurement["total_seconds"],
                "result_row_count": len(measurement["result"]),
                "result_sha256": result_sha,
                "correct_vs_local_sql": correct,
                # Deliberately blank: this is local emulation, not network measurement.
                "network_bytes_measured": "", "network_seconds_measured": "",
                "egress_usd_estimated": "", "duckdb_version": duckdb.__version__,
                "python_version": platform.python_version(), "threads": args.threads,
            }
            entries.append(record)
            status = "PASS" if correct else "FAIL"
            print(f"{plan:10} trial={trial}  correctness={status}  "
                  f"intermediate={measurement['intermediate_parquet_bytes']:,} bytes  "
                  f"local_time={measurement['total_seconds']:.3f}s", flush=True)

    write_csv(run_dir / "trial_metrics.csv", entries)
    if args.plots:
        make_local_preview_plots(entries, run_dir)
    write_csv(args.output_dir / "all_query_metrics.csv", entries, append=True)
    metadata = {
        "schema_version": "1", "run_id": run_id, "created_utc": stamp(),
        "query_id": QUERY_ID, "scale_factor": SCALE_FACTOR,
        "execution_mode": EXECUTION_MODE,
        "measurement_scope": "One-machine local emulation; NO cross-cloud network transfer",
        "placement_assumption": {"AWS": ["orders"], "Azure": ["lineitem"]},
        "plans": plans, "trials": args.trials, "threads": args.threads,
        "engine": {"duckdb": duckdb.__version__, "python": platform.python_version(),
                   "host": platform.platform(), "logical_cpu_count": os.cpu_count()},
        "inputs": {
            "orders": {"path": str(orders.resolve()), "sha256": sha256_file(orders)},
            "lineitem": {"path": str(lineitem.resolve()), "sha256": sha256_file(lineitem)},
            "sql": {"path": str(args.query_file.resolve()),
                    "sha256": sha256_file(args.query_file)},
        },
        "archived_reference": {
            "path": str(args.reference_file.resolve()),
            "sha256": sha256_file(args.reference_file) if args.reference_file.is_file() else None,
            "comparison_status": "passed_exact_rows_after_normalization",
            "canonical_result_sha256": hash_result(archived),
        },
        "fresh_sql_baseline": {"result_sha256": baseline_sha,
                               "execution_seconds": round(baseline_seconds, 9)},
        "all_plans_correct_vs_fresh_sql_baseline": overall_ok,
        "metric_definitions": {
            "intermediate_parquet_bytes": "bytes of locally materialized ZSTD Parquet intermediate",
            "total_seconds": "local COPY + join/aggregate; no network transfer",
            "network_bytes_measured": "blank because not measured",
            "egress_usd_estimated": "blank because not calculated or measured",
        },
        "results_csv": "trial_metrics.csv",
    }
    save_json(run_dir / "run.json", metadata)
    print(f"\nRun artifacts: {run_dir}")
    print(f"Aggregate metrics: {args.output_dir / 'all_query_metrics.csv'}")
    print("IMPORTANT: execution_mode=local_emulation; DO NOT report as cloud benchmark.")
    return 0 if overall_ok else 2


if __name__ == "__main__":
    sys.exit(main())
