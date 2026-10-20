
import json
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "results/reference/sf1"

manifest = json.loads(
    (BASE / "reference_manifest.json").read_text()
)

for name in ["q01", "q06", "q12"]:
    path = BASE / f"{name}_results.json"
    result = json.loads(path.read_text())

    canonical = json.dumps(
        result,
        default=str,
        ensure_ascii=False
    )

    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()

    expected = manifest["queries"][name]["result_sha256"]
    actual_rows = len(result["rows"])
    expected_rows = manifest["queries"][name]["row_count"]

    assert digest == expected, f"{name}: hash mismatch"
    assert actual_rows == expected_rows, f"{name}: row count mismatch"

    print(f"{name.upper()}: PASS ({actual_rows} rows)")

print("REFERENCE FILE VALIDATION: SUCCESS")