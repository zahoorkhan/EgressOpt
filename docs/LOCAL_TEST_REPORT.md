# Local test report — 2026-10-20

**Target:** GitHub publication copy derived from `EgressOpt_GitHub_Public_Draft_20261020.zip`.

**Execution:** Python 3.13.5, local Linux test environment. No AWS/Azure/GCP CLI commands invoked, no cloud authentication used, no VMs started, no real-cloud experiment rerun. The optional TLS fixture was restricted to the `127.0.0.1` loopback interface.

| Check | Result | Evidence |
|---|---|---|
| Parse all publication Python source files | PASS | 25 `.py` files |
| Static credential-pattern screening | PASS | 68 scanned text files, zero pattern findings (not a comprehensive secrets audit) |
| Direction-aware cost, SLO, Pareto selfcheck | PASS | `EGRESSOPT_SELFCHECK_PASS` |
| Q03 Phase2 source/SQL guard | PASS | `source_guard=PASS`, 22 candidate specifications |
| Q05 Phase2 source/SQL guard | PASS | `source_guard=PASS`, 237 candidate specifications |
| Four repeated-run evidence reports and plot generation | PASS | `EGRESSOPT_SF1_SF10_REPEATED_AUDIT_PASS`: 4 reports, 20 measured pairs |
| Synthetic TLS data-transfer integrity/authorization and idempotency | PASS | `M6L_OFFLINE_TEST_PASS`, test-bound to `127.0.0.1` |
| Simulated AWS/Azure/GCP VM startup and cleanup | PASS | `M6L_MOCK_VM_LIFECYCLE_PASS`, mock objects only |

Test commands executed:

```bash
python3 tools/run_local_checks.py
python3 tools/run_local_checks.py --with-loopback
```

Both terminated with `EGRESSOPT_LOCAL_CHECKS_PASS` (6 and 7 check groups, respectively).

## Open limitation — live cloud source guard

`src/provision_tpch_sf10.py` expects original private Phase1 Q05 `sha256=bc7ac734062e88e13e4a23b3b6368c7a5623c6a806adfef63c9d913462526d21`. The sanitized public Q05 file has `sha256=a37a423410cbefbd2d0925741eafc6991a3e26e87e77e734c33d60060b675ed2`. Consequently, its real-cloud `load_phase1()` guard would reject the public Q05 runner. Other historical-source checks in `experiments/q03-phase2.py` and `experiments/q05-phase2.py` also contain original private hash literals for a cloud reuse path. These were **not changed** by the local documentation/testing update, to avoid silently modifying experimental provenance or cloud execution semantics.

**Conclusion:** The publication copy passes its specifically defined offline reproducibility tests. It is **not** certified as executable on live clouds; compare with latest private checkout and reconcile source guards before any real-cloud validation. The fully measured original campaign remains historical evidence, not newly repeated measurements.

## Addendum — TPC-H SF1/SF10 preparation CLI (2026-10-20)

The additional `tools/setup_tpch_data.py` helper and `docs/TPCH_DATA.md` provide two local data-generation routes. The generator **was not run at SF1 or SF10** in this packaging environment; the large datasets were intentionally not downloaded or generated. The following checks **were** executed successfully:

- `python tests/test_tpch_setup_cli.py` — 6 test cases PASS, including SF1/SF10 dry-run behavior, accepted scale-factor enforcement, mutually exclusive flags, and DBGEN option applicability.
- `python tools/setup_tpch_data.py --scale 1 --dry-run` — PASS; no filesystem writes or network activity.
- `python tools/setup_tpch_data.py --scale 10 --dry-run` — PASS; no filesystem writes or network activity.
- Python compilation of the new CLI and test files — PASS.
- `python tools/scan_public_release.py .` — PASS; 73 text files scanned, no pattern findings.

The top-level `tools/run_local_checks.py` now includes these six CLI tests as an additional offline check group. **An actual SF10 DBGEN generation/conversion and a full cloud end-to-end benchmark are not covered by this addendum.** The previous successful full offline evidence test report remains applicable to the otherwise unchanged experiment/evidence files.
