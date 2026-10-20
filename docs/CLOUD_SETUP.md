# Configuring AWS, Microsoft Azure, and Google Cloud for EgressOpt

This guide describes how a researcher **would configure their own cloud accounts** for the public EgressOpt code. **Cloud accounts and CLI sign-in are not required for the offline tests.** Nothing in `tools/run_local_checks.py` authenticates with a provider, contacts public-cloud endpoints, starts VMs, or incurs cloud charges.

## Prerequisites

- Python 3.10+ and the packages in `requirements.txt` for analysis and full data-dependent execution.
- To use cloud experiments later: AWS CLI v2, Azure CLI (`az`), and Google Cloud CLI (`gcloud`), installed from their official instructions.
- Existing, authorized research VMs, correct dataset placement, associated network/security rules, and the provider permissions necessary for VM management and remote commands. **This repository does not provision an account or deploy an entire environment automatically.**

Useful official references:
- [AWS CLI v2 installation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and [IAM Identity Center / CLI sign-in](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html)
- [Azure CLI installation](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) and [Azure CLI sign-in](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-interactively)
- [Google Cloud CLI installation](https://cloud.google.com/sdk/docs/install) and [initialization](https://cloud.google.com/sdk/docs/initializing)

### 1. AWS configuration (only if cloud execution is later authorized)

```bash
aws --version
aws configure sso --profile research       # first-time setup, interactive
aws sso login --profile research           # online authentication
```

Use an AWS profile you own; `research` is only an example. Locate your permitted EC2 instance ID in the AWS EC2 console and confirm it is in the region you intend to use. The experiment implementation uses AWS Systems Manager (SSM) for remote commands, so an EC2 instance being visible is **not** sufficient: it must be managed by SSM and authorized for the commands the user requests. Prefer IAM Identity Center/SSO to static access keys. **Do not place `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or `AWS_SESSION_TOKEN` in project files.**

Relevant environment variables:

| Variable | Meaning |
|---|---|
| `EGRESSOPT_AWS_PROFILE` | Local AWS CLI profile, such as `research` |
| `EGRESSOPT_AWS_REGION` | AWS region of the research VM |
| `EGRESSOPT_AWS_INSTANCE_ID` | EC2 ID used by Q03 Phase1 runner |
| `EGRESSOPT_AWS_INSTANCE` | Same EC2 ID; other query runners use this variable |

### 2. Azure configuration (only if cloud execution is later authorized)

```bash
az --version
az login                                 # online authentication
az account set --subscription "YOUR_SUBSCRIPTION_ID"
```

Find your VM name, resource group, network security group (NSG), and public-IP resource name in the Azure portal. Some experiments use **Azure Run Command**; you need permission to invoke it on your VM. The selected Azure subscription is a local CLI account setting; do not commit subscription identifiers, credentials, or cached tokens.

| Variable | Meaning |
|---|---|
| `EGRESSOPT_AZURE_RESOURCE_GROUP` | Resource group containing your VM |
| `EGRESSOPT_AZURE_VM` | Azure VM name |
| `EGRESSOPT_AZURE_NSG` | Relevant network security group |
| `EGRESSOPT_AZURE_PUBLIC_IP_RESOURCE` | Public-IP resource name, if present |

### 3. Google Cloud configuration (only if cloud execution is later authorized)

```bash
gcloud --version
gcloud init                              # first-time interactive configuration
gcloud auth login                        # online authentication, if needed
gcloud config set project "YOUR_PROJECT_ID"
```

Identify the Compute Engine VM and zone in the Google Cloud console. The existing cloud runner uses `gcloud compute ssh` via **IAP** in its remote-management path; appropriate IAP tunnel access, instance access, and network rules are required. Do not add service-account JSON keys to the repository.

| Variable | Meaning |
|---|---|
| `EGRESSOPT_GCP_PROJECT` | Google Cloud project ID |
| `EGRESSOPT_GCP_ZONE` | Zone containing the VM |
| `EGRESSOPT_GCP_VM` | Compute Engine VM name |

### 4. Private local configuration

From the repository root:

```bash
cp .env.example .env
# Edit .env with values belonging to your own cloud accounts.
# DO NOT share or commit the .env file.
set -a
source .env
set +a
```

The `.env` file is ignored by Git. Copy only the **template** (`.env.example`) into your public repository. The template contains no actual cloud credentials. If you work with a different region/zone, update it locally; the template's locations are examples, not required cloud infrastructure.

The Phase2 worker may also require short-lived authentication tokens and TLS material at actual runtime (`EGRESSOPT_PHASE2_TOKEN`, `EGRESSOPT_PHASE2_TLS_CERT`, `EGRESSOPT_PHASE2_TLS_KEY`, and `EGRESSOPT_PHASE2_CA_BUNDLE`). These must remain private and **are not required for offline tests**. Never commit access tokens, server keys, or local CLI credential directories.

### 5. Confirm local tests BEFORE any cloud usage

```bash
python tools/run_local_checks.py
python tools/run_local_checks.py --with-loopback
```

The second command uses a synthetic, locally generated TLS certificate and a loopback-only (`127.0.0.1`) receiver, plus mocked AWS/Azure/GCP VM transitions. It **does not log in to cloud providers** or transfer real data. The test needs local OpenSSL and free TCP port 18443. See [LOCAL_TESTING.md](LOCAL_TESTING.md).

## Current publication-draft limitation

The public repository copy was assembled from earlier implementation archives, whereas `EggOPT_7.zip` was manuscript-only. Some source-integrity guards for **historical real-cloud execution** still refer to checksums of the unmodified, private Phase1 runner. Consequently, passing offline tests **does not authorize or certify cloud execution from the public fork**. Keep the historical source and evidence private and reconcile these guards with the original experimental revision before performing any new cloud runs. Do not claim the public fork has already been revalidated live.
