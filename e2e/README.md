# Nightly E2E Testing Setup Guide

This document describes how to set up and configure the nightly end-to-end testing infrastructure for Nurion on Volcengine Kubernetes.

## Architecture Overview

```
GitHub Actions (Nightly Schedule @ 02:00 UTC)
       │
       ▼
Self-hosted Runner (K8s Pod, nurion-sh context)
       │
       ├──► Deploy Aether Service (Pulumi Python)
       ├──► Register test data (Lance/Iceberg) to Aether
       ├──► Register K8s cluster to Aether
       ├──► Submit Solstice workflows via Aether API
       ├──► Execute E2E Tests (2 workflows)
       └──► Cleanup (preserve debug artifacts)
```

## Prerequisites

- Kubernetes cluster on Volcengine (context: `nurion-sh`)
- Volcengine Container Registry (CR)
- Volcengine TOS (S3-compatible storage)
- GitHub repository with Actions enabled
- Pulumi account for state management

## Required GitHub Secrets

Configure these secrets in your GitHub repository settings:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `KUBECONFIG_NURION_SH` | Base64-encoded kubeconfig for K8s cluster | `cat ~/.kube/config \| base64` |
| `VOLCENGINE_ACCESS_KEY` | Volcengine IAM Access Key | `AKLT...` |
| `VOLCENGINE_SECRET_KEY` | Volcengine IAM Secret Key | `...` |
| `CR_URL` | Container Registry URL | `your-registry/namespace` |
| `CR_USERNAME` | Registry username | `your-username` |
| `CR_PASSWORD` | Registry password/token | `...` |
| `RUNNER_TOKEN` | GitHub PAT for runner registration | `ghp_...` |
| `PULUMI_ACCESS_TOKEN` | Pulumi Cloud access token | `pul-...` |
| `PULUMI_PASSPHRASE` | Passphrase for Pulumi secrets | `your-passphrase` |
| `POSTGRES_PASSWORD` | PostgreSQL password for Aether | `secure-password` |

### How to Set Secrets

```bash
# Using GitHub CLI
gh secret set KUBECONFIG_NURION_SH --body "$(cat ~/.kube/config | base64)"
gh secret set VOLCENGINE_ACCESS_KEY --body "YOUR_ACCESS_KEY"
gh secret set VOLCENGINE_SECRET_KEY --body "YOUR_SECRET_KEY"
# ... etc
```

## Volcengine Setup

### 1. Container Registry (CR)

Create a namespace in Volcengine CR:

```bash
# Login to Volcengine CR (use your registry URL from E2E_CR_URL)
docker login $CR_REGISTRY -u YOUR_USERNAME

# The images will be pushed to:
# $E2E_CR_URL/aether:nightly
```

### 2. TOS (Object Storage)

Create a bucket for test data:

```bash
# Bucket: nurion
# Region: configured via AWS_DEFAULT_REGION environment variable
# Endpoint: configured via AWS_ENDPOINT_URL environment variable

# Structure:
s3://nurion/
├── raw/videos/          # 1000 test videos
├── raw/images/          # 10000 test images  
├── lance/
│   ├── videos_lance/    # Video metadata (S3 paths)
│   └── images_lance/    # Images as binary blobs
├── iceberg/
│   ├── videos_iceberg/
│   └── images_iceberg/
└── test_outputs/        # E2E test outputs
```

### 3. Kubernetes Cluster

Ensure the cluster has:
- Sufficient resources (recommend: 4+ nodes, 8GB+ RAM each)
- Default StorageClass for PVCs
- Network access to Volcengine services

## Test Data Preparation

### Download and Upload Test Data

```bash
cd scripts

# Run the data preparation script
python prepare_test_data.py \
  # --s3-endpoint uses AWS_ENDPOINT_URL env var by default
  --s3-bucket nurion \
  --video-count 1000 \
  --image-count 10000
```

### Data Sources

- **Videos**: HuggingFace `HuggingFaceFV/finevideo` dataset
  - Filter: 8-12 minute duration
  - ~1000 videos, ~150GB total
  
- **Images**: HuggingFace `laion/laion-high-resolution`
  - Filter: 800KB-1.2MB, 1024x1024+
  - ~10000 images, ~10GB total

## Pulumi Setup

### Initialize Pulumi Stack

```bash
cd infra

# Install dependencies
uv sync

# Login to Pulumi Cloud
pulumi login

# Initialize stack
pulumi stack init nightly

# Configure secrets
pulumi config set --secret postgres_password "YOUR_PASSWORD"
pulumi config set --secret github_token "YOUR_GITHUB_TOKEN"
pulumi config set github_repo "your-org/nurion"
```

### Deploy Manually (for testing)

```bash
cd infra
pulumi up -s nightly
```

### Destroy

```bash
cd infra
pulumi destroy -s nightly
```

## Running Tests Locally

```bash
cd e2e

# Install dependencies
uv sync

# Set environment variables
export AETHER_URL="http://localhost:8000"
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_ENDPOINT_URL="$E2E_S3_ENDPOINT"  # From GitHub secrets

# Run tests
uv run pytest -v -m "e2e" test_aether_setup.py
```

## Workflow Details

### Workflow 1: Video Processing

```
Lance Table (videos_lance)
  → LanceTableSource
  → FFmpegSceneDetectOperator
  → FFmpegSliceOperator
  → LanceSink (output slices)
```

### Workflow 2: Image Processing

```
Iceberg Table (images_iceberg) / Lance Table (images_lance)
  → SparkV2Source / LanceTableSource
  → ImageResizeOperator
  → ImageFilterOperator
  → ImageMetadataOperator
  → JsonFileSink
```

## Debugging Failed Tests

### View Artifacts

After each run, debug artifacts are uploaded to GitHub:
1. Go to Actions → Workflow Run → Artifacts
2. Download `e2e-debug-{run_id}`

### Manual Debug Mode

```bash
# Trigger workflow with skip_cleanup
gh workflow run nightly-e2e.yml -f skip_cleanup=true

# Connect to the cluster
kubectl -n nurion-nightly get pods
kubectl -n nurion-nightly logs -l app=aether
```

### Collect Logs Manually

```bash
./scripts/collect-debug-logs.sh nurion-nightly debug-artifacts
```

## China Network Optimizations

The setup uses China mirrors for faster dependency installation:

- **pip**: `mirrors.aliyun.com`
- **Maven**: `maven.aliyun.com`
- **npm**: `registry.npmmirror.com`
- **Docker**: Volcengine CR

Run `source scripts/china-mirrors.sh` to configure all mirrors.

## Troubleshooting

### Runner Not Picking Up Jobs

1. Check runner registration:
   ```bash
   kubectl -n nurion-nightly get pods -l app=github-runner
   ```

2. Verify GitHub token is valid:
   - Go to Settings → Actions → Runners
   - Check runner status

### Pulumi State Issues

```bash
# Force refresh state
cd infra
pulumi refresh -s nightly

# Import existing resources if needed
pulumi import kubernetes:core/v1:Namespace nurion-nightly nurion-nightly
```

### Image Pull Errors

1. Verify registry secret:
   ```bash
   kubectl -n nurion-nightly get secret registry-secret
   ```

2. Test pull manually:
   ```bash
   docker pull $E2E_CR_URL/aether:nightly
   ```
