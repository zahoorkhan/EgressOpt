# Secure cloud configuration

This publication copy contains no intentional static AWS/Azure/GCP credentials or live resource-ID defaults. It uses your local cloud CLIs and environment variables.

## Before use

1. Create a private `./.env` from `.env.example`, edit the values locally, then execute `set -a; source .env; set +a` in a trusted shell. Do not commit `.env`.
2. Run `aws sso login --profile "$EGRESSOPT_AWS_PROFILE"` for AWS SSO; `az login` for Azure; `gcloud auth login` for Google Cloud. Use provider IAM least privilege.
3. Never copy `~/.aws`, `~/.azure`, `~/.config/gcloud`, private TLS keys, service-account credentials, cloud run logs, private inventory, or complete cloud deployment reports into this repository.
4. Check `git status --short` and run `python tools/scan_public_release.py .` before pushing.
5. If any credentials were ever published, **revoke/rotate them**, purge the history where possible, and review cloud audit logs. Git removal alone is not sufficient.

Cloud instance IDs, resource group names, and project identifiers are not authentication secrets by themselves, but this release avoids embedding them to minimize disclosure of private infrastructure.

## Frozen study source vs publication copy

The original historical Q03 and Q05 Phase1 runners were **not modified**. In this separate publication copy, their *default cloud identifier strings* were replaced by empty environment fallbacks. Thus, their public-release SHA-256 digests differ from the frozen historical experimental source. Phase2 source guards were updated to recognize the public release's sanitized runner SHA, and historical run.json evidence retains the original source SHA. This distinction must remain explicit. The SQL files are byte-identical to the archived sources. No cloud reruns or correctness-equivalence tests of this publication fork have been performed.

Do not present this publication copy as byte-identical to the original real-cloud experiment runner. Archive the frozen originals privately.

## Cloud setup and offline verification

See [docs/CLOUD_SETUP.md](docs/CLOUD_SETUP.md), [docs/LOCAL_TESTING.md](docs/LOCAL_TESTING.md), and [docs/LOCAL_TEST_REPORT.md](docs/LOCAL_TEST_REPORT.md). The new local checks do **not** authorize real-cloud execution; historical/private source hash differences remain an explicit blocker for some cloud execution paths.
