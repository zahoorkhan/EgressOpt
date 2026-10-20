# Prepublication checklist

- [ ] Compare public copy to the *latest actual Mac source*. `EggOPT_7.zip` includes only the manuscript.
- [ ] Confirm cloud resource IDs / token values are absent, including test fixtures and generated output.
- [ ] Confirm `.env` is ignored (`git check-ignore -v .env`).
- [ ] Run `python tools/scan_public_release.py .` and manual sensitive-data review.
- [ ] Ensure dependencies are pinned and the Python version is documented for intended reproducibility.
- [ ] Run local tests, data generation, and Phase2 preflight against your current private datasets.
- [ ] Review licensing/copyright; do not publish any third-party code without checking license.
- [ ] Verify the four included experimental evidence JSON files correspond to the published paper.
- [ ] Do not advertise official TPC certification; benchmark is TPC-H-derived.
- [ ] If code or secrets have ever been pushed elsewhere, rotate leaked keys and review git history.
- [ ] Create an empty GitHub repository, verify `git status`, commit then push only this public draft.

## Updated offline-only test status (2026-10-20)

- [x] Added provider setup steps in `docs/CLOUD_SETUP.md` (cloud login is optional and outside the offline tests).
- [x] Added reproducibility commands in `docs/LOCAL_TESTING.md`.
- [x] Added `tools/run_local_checks.py` to validate syntax, static secret patterns, optimizer selfcheck, Q03/Q05 guarded inspection, and four verified paired-run reports without cloud contact.
- [x] Restricted the synthetic TLS test server in the test fixture to the loopback address `127.0.0.1`; the cloud receiver source is unchanged.
- [x] Local test run and mock VM lifecycle PASS. Details in `docs/LOCAL_TEST_REPORT.md`.
- [ ] **Real-cloud deploy blocker:** historical source-hash checks in `src/provision_tpch_sf10.py` and parts of Phase2 still reference private original runner content, not the sanitized public fork. Do not claim live deployment verified.
