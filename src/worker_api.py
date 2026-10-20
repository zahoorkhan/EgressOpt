"""EgressOpt worker API: authenticated health, Q12 stage, download, receive/join.

All endpoints are RESEARCH PROTOTYPES for LOCALHOST development. Do NOT expose
this service on a public network without TLS and firewall restrictions.

Local test:
    EGRESSOPT_WORKER_TOKEN="$(cat ~/.egressopt_worker_token)" \
        python -m uvicorn src.worker_api:app --host 127.0.0.1 --port 8766

Fixed query fragments only; no arbitrary SQL or caller-specified filesystem paths.
The upload endpoint accepts a single Parquet payload, not a URL, to avoid SSRF.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("EGRESSOPT_DATA_DIR", str(PROJECT_ROOT / "data/tpch/parquet_sf1")))
STAGING_DIR = Path(os.environ.get("EGRESSOPT_STAGING_DIR", str(PROJECT_ROOT / "results/worker_staging/q12")))
CLOUD = os.environ.get("EGRESSOPT_WORKER_CLOUD", "local").lower()
TOKEN = os.environ.get("EGRESSOPT_WORKER_TOKEN", "")
MAX_UPLOAD_BYTES = 64 * 1024 * 1024  # Q12 SF1 intermediate maximum, not an arbitrary file service
if len(TOKEN) < 32:
    raise RuntimeError("Set EGRESSOPT_WORKER_TOKEN to a secret of at least 32 characters")

app = FastAPI(title="EgressOpt Worker", docs_url=None, redoc_url=None, openapi_url=None)
_bearer = HTTPBearer(auto_error=False)


class StageQ12Request(BaseModel):
    plan_id: Literal["aws_join", "azure_join"]


def require_token(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> None:
    supplied = "" if credentials is None else credentials.credentials
    if not hmac.compare_digest(supplied, TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Bearer"})


def sql_literal(value: Path) -> str:
    return "'" + str(value.resolve()).replace("'", "''") + "'"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_q12(rows: list[tuple]) -> list[dict]:
    return sorted(
        [
            {"l_shipmode": str(mode), "high_line_count": int(high), "low_line_count": int(low)}
            for mode, high, low in rows
        ],
        key=lambda record: record["l_shipmode"],
    )


def result_sha256(rows: list[dict]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def archived_q12_match(rows: list[dict]) -> bool | None:
    """Return None when the independent archive is not deployed on the worker."""
    archive = PROJECT_ROOT / "results/reference/sf1/q12_results.json"
    if not archive.is_file():
        return None
    data = json.loads(archive.read_text())
    columns = data["columns"]
    required = {"l_shipmode", "high_line_count", "low_line_count"}
    if len(columns) != 3 or set(columns) != required:
        raise ValueError("Archived Q12 reference has an unexpected schema")
    expected = []
    for values in data["rows"]:
        mapped = dict(zip(columns, values, strict=True))
        expected.append((mapped["l_shipmode"], mapped["high_line_count"], mapped["low_line_count"]))
    return rows == canonical_q12(expected)


def persist_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


@app.get("/health", dependencies=[Depends(require_token)])
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "worker": "egressopt",
        "provider": CLOUD,
        "duckdb_version": duckdb.__version__,
    }


@app.post("/q12/stage", dependencies=[Depends(require_token)])
def stage_q12(request: StageQ12Request) -> dict:
    """Create a fixed, pushdown-aware Q12 intermediate on its source worker."""
    specs = {
        "aws_join": ("lineitem", "Azure"),
        "azure_join": ("orders", "AWS"),
    }
    table, expected_source = specs[request.plan_id]
    if CLOUD not in ("local", expected_source.lower()):
        raise HTTPException(status_code=409, detail="Wrong source worker for requested Q12 plan")
    source = DATA_DIR / f"{table}.parquet"
    if not source.is_file():
        raise HTTPException(status_code=409, detail=f"Missing local SF1 table: {table}")

    artifact_id = uuid.uuid4().hex
    artifact_dir = STAGING_DIR / artifact_id
    artifact_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    artifact_dir.chmod(0o700)
    output = artifact_dir / "intermediate.parquet"

    if request.plan_id == "aws_join":
        fragment = f"""
            SELECT l_orderkey, l_shipmode
            FROM read_parquet({sql_literal(source)})
            WHERE l_shipmode IN ('MAIL', 'SHIP')
              AND l_commitdate < l_receiptdate
              AND l_shipdate < l_commitdate
              AND l_receiptdate >= CAST('1994-01-01' AS DATE)
              AND l_receiptdate < CAST('1995-01-01' AS DATE)
        """
    else:
        fragment = f"""
            SELECT o_orderkey, o_orderpriority
            FROM read_parquet({sql_literal(source)})
        """

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("SET threads = 4")
        started = time.perf_counter()
        con.execute(f"COPY ({fragment}) TO {sql_literal(output)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        stage_seconds = time.perf_counter() - started
        rows = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(output)]).fetchone()[0]
    finally:
        con.close()

    output.chmod(0o600)
    record = {
        "schema_version": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "execution_mode": "worker_local_stage_only" if CLOUD == "local" else "cloud_worker_stage_only",
        "query_id": "Q12",
        "scale_factor": "SF1",
        "plan_id": request.plan_id,
        "source_cloud_expected": expected_source,
        "actual_worker_cloud": CLOUD,
        "artifact_id": artifact_id,
        "intermediate_rows": int(rows),
        "intermediate_parquet_bytes": output.stat().st_size,
        "intermediate_sha256": sha256_file(output),
        "stage_seconds": round(stage_seconds, 9),
        "duckdb_version": duckdb.__version__,
        "download_path": f"/q12/artifacts/{artifact_id}",
        "network_bytes_measured": None,
        "network_seconds_measured": None,
        "egress_usd_estimated": None,
    }
    persist_json(artifact_dir / "stage.json", record)
    return record


@app.get("/q12/artifacts/{artifact_id}", dependencies=[Depends(require_token)])
def download_q12_artifact(artifact_id: uuid.UUID):
    output = STAGING_DIR / artifact_id.hex / "intermediate.parquet"
    if not output.is_file():
        raise HTTPException(status_code=404, detail="Unknown Q12 artifact")
    return FileResponse(
        output,
        media_type="application/octet-stream",
        filename=f"q12_{artifact_id.hex}.parquet",
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


@app.post("/q12/join/{plan_id}", dependencies=[Depends(require_token)])
async def join_q12(plan_id: Literal["aws_join", "azure_join"], request: Request) -> dict:
    """Receive a Parquet intermediate and run the predefined Q12 join/aggregate.

    The incoming HTTP body is NOT itself trusted or a caller-specified SQL query.
    SHA-256 detects accidental corruption; the bearer token authenticates the caller.
    A future cloud test must additionally use TLS and restricted source IPs.
    """
    join_cloud = {"aws_join": "AWS", "azure_join": "Azure"}[plan_id]
    if CLOUD not in ("local", join_cloud.lower()):
        raise HTTPException(status_code=409, detail="Wrong destination worker for requested Q12 plan")
    base_table = "orders" if plan_id == "aws_join" else "lineitem"
    base = DATA_DIR / f"{base_table}.parquet"
    if not base.is_file():
        raise HTTPException(status_code=409, detail=f"Missing local SF1 table: {base_table}")

    provided_hash = request.headers.get("x-egressopt-sha256", "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", provided_hash):
        raise HTTPException(status_code=400, detail="Provide X-EgressOpt-SHA256 (64 hex digits)")
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != "application/octet-stream":
        raise HTTPException(status_code=415, detail="Expected application/octet-stream")
    length_header = request.headers.get("content-length")
    if length_header:
        try:
            declared_length = int(length_header)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from None
        if declared_length > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="Q12 artifact is too large")

    receipt_id = uuid.uuid4().hex
    receipt_dir = STAGING_DIR / f"received_{receipt_id}"
    receipt_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    receipt_dir.chmod(0o700)
    intermediate = receipt_dir / "received.parquet"

    total_received = 0
    digest = hashlib.sha256()
    started_receive = time.perf_counter()
    try:
        with intermediate.open("wb") as dest:
            async for chunk in request.stream():
                total_received += len(chunk)
                if total_received > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Q12 artifact is too large")
                dest.write(chunk)
                digest.update(chunk)
        receive_seconds = time.perf_counter() - started_receive
        intermediate.chmod(0o600)
        if total_received == 0:
            raise HTTPException(status_code=400, detail="Empty Q12 artifact")
        if not hmac.compare_digest(digest.hexdigest(), provided_hash):
            raise HTTPException(status_code=422, detail="Q12 artifact checksum mismatch")

        aggregates = """
            SUM(CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH') THEN 1 ELSE 0 END) AS high_line_count,
            SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT','2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
        """
        if plan_id == "aws_join":
            query = f"""
                SELECT l_shipmode, {aggregates}
                FROM read_parquet({sql_literal(base)}) AS o
                JOIN read_parquet({sql_literal(intermediate)}) AS l
                  ON o.o_orderkey = l.l_orderkey
                GROUP BY l_shipmode ORDER BY l_shipmode
            """
        else:
            query = f"""
                SELECT l_shipmode, {aggregates}
                FROM read_parquet({sql_literal(base)}) AS l
                JOIN read_parquet({sql_literal(intermediate)}) AS o
                  ON o.o_orderkey = l.l_orderkey
                WHERE l_shipmode IN ('MAIL', 'SHIP')
                  AND l_commitdate < l_receiptdate
                  AND l_shipdate < l_commitdate
                  AND l_receiptdate >= CAST('1994-01-01' AS DATE)
                  AND l_receiptdate < CAST('1995-01-01' AS DATE)
                GROUP BY l_shipmode ORDER BY l_shipmode
            """

        con = duckdb.connect(database=":memory:")
        try:
            con.execute("SET threads = 4")
            started_join = time.perf_counter()
            rows = canonical_q12(con.execute(query).fetchall())
            join_seconds = time.perf_counter() - started_join
            intermediate_rows = int(con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(intermediate)]
            ).fetchone()[0])
        finally:
            con.close()

        record = {
            "schema_version": 1,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "execution_mode": "worker_local_receive_join_only" if CLOUD == "local" else "cloud_worker_receive_join_only",
            "query_id": "Q12", "scale_factor": "SF1", "plan_id": plan_id,
            "expected_join_cloud": join_cloud,
            "actual_worker_cloud": CLOUD,
            "receipt_id": receipt_id,
            "received_payload_bytes": total_received,
            "intermediate_rows": intermediate_rows,
            "intermediate_sha256": digest.hexdigest(),
            "receive_seconds": round(receive_seconds, 9),
            "join_aggregate_seconds": round(join_seconds, 9),
            "result_row_count": len(rows),
            "result_sha256": result_sha256(rows),
            "result": rows,
            "correct_vs_archived_reference": archived_q12_match(rows),
            "network_bytes_measured": None,
            "network_seconds_measured": None,
            "egress_usd_estimated": None,
            "duckdb_version": duckdb.__version__,
        }
        persist_json(receipt_dir / "join_result.json", record)
        return record
    except Exception:
        # If transfer is invalid or query fails, avoid leaving unverified payloads.
        shutil.rmtree(receipt_dir, ignore_errors=True)
        raise
