# ARC (Actions Runner Controller) Deployment Guide

This directory configures the `smg-project/smg` runner scale sets. The shared ARC
controller and listener pods run in `org-actions-runner`; runner scale-set objects,
credentials, and runner pods run in `actions-runner-system`.

The controller and runner scale-set charts are pinned to `0.14.2` so their CRDs and
controller behavior stay compatible.

## Prerequisites

- Kubernetes cluster v1.28 or newer
- `kubectl` configured with cluster access
- Helm v3
- Organization-owner access to install and authorize the GitHub App

## 1. Create and install a GitHub App

Create a GitHub App under the `smg-project` organization with webhooks disabled.
Grant only the permissions required by ARC:

- Repository permissions:
  - **Administration**: Read and write
  - **Metadata**: Read-only
- Organization permissions:
  - **Self-hosted runners**: Read and write

Install the App on `smg-project` and grant it access to the `smg` repository. Record
the App ID and the installation ID, then generate and securely store a private key.
The installation ID is the final number in the installation settings URL:

```text
https://github.com/organizations/smg-project/settings/installations/<installation-id>
```

See GitHub's [ARC authentication documentation](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/authenticate-to-the-api)
for the authoritative permission list.

## 2. Create or update the Kubernetes secret

Never paste credentials into `archived/arc-secret-template.yaml` or commit a private key.
Create the secret directly from the values and PEM file instead:

```bash
kubectl create namespace actions-runner-system --dry-run=client -o yaml \
  | kubectl apply -f -

kubectl create secret generic github-arc-secret \
  --namespace actions-runner-system \
  --from-literal=github_app_id='<app-id>' \
  --from-literal=github_app_installation_id='<installation-id>' \
  --from-file=github_app_private_key='<path-to-private-key.pem>' \
  --dry-run=client -o yaml \
  | kubectl apply -f -
```

The archived `arc-secret-template.yaml` contains placeholders only and is retained
for schema/reference purposes.

## 3. Install or upgrade the shared controller

```bash
helm upgrade --install arc \
  --namespace org-actions-runner \
  --create-namespace \
  --version 0.14.2 \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set-controller
```

Verify the controller:

```bash
kubectl get pods \
  --namespace org-actions-runner \
  -l app.kubernetes.io/part-of=gha-rs-controller
```

## 4. Install or upgrade runner scale sets

Each values file explicitly references the shared controller service account in
`org-actions-runner`. Install the required scale sets into `actions-runner-system`:

```bash
helm upgrade --install k8s-runner-cpu \
  --namespace actions-runner-system \
  --create-namespace \
  --version 0.14.2 \
  -f scripts/k8s-runner-resources/runner-values-cpu.yaml \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set

helm upgrade --install 1-gpu-h100 \
  --namespace actions-runner-system \
  --create-namespace \
  --version 0.14.2 \
  -f scripts/k8s-runner-resources/runner-values-1-gpu-h100.yaml \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set

helm upgrade --install 2-gpu-h100 \
  --namespace actions-runner-system \
  --create-namespace \
  --version 0.14.2 \
  -f scripts/k8s-runner-resources/runner-values-2-gpu-h100.yaml \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set

helm upgrade --install 4-gpu-h100 \
  --namespace actions-runner-system \
  --create-namespace \
  --version 0.14.2 \
  -f scripts/k8s-runner-resources/runner-values-4-gpu-h100.yaml \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set
```

## 5. Verify

```bash
# Scale-set objects and runner pods
kubectl get autoscalingrunnersets,pods --namespace actions-runner-system

# Listener pods managed by the shared controller
kubectl get pods \
  --namespace org-actions-runner \
  -l actions.github.com/scale-set-name
```

Each scale set should have a listener pod in `Running` state. Runner pods are created
on demand when a workflow uses the corresponding `runnerScaleSetName` as its
`runs-on` label.

## Uninstalling

Remove scale sets before removing the shared controller:

```bash
helm uninstall <runner-set-name> --namespace actions-runner-system
helm uninstall arc --namespace org-actions-runner
```

Do not uninstall the shared controller until every runner scale set that uses it has
been removed.

## Archived manifests

Inactive scale-set values and deprecated `actions.summerwind.dev` resources are in
[`archived/`](archived/README.md). They are retained only for historical recovery
and must not be applied alongside the active runner scale sets.

## CI node health monitor

`.github/workflows/ci-node-health.yml` runs `scripts/ci_node_health.py` every hour on the
`k8s-runner-cpu` scale set. It reads Prometheus (node-problem-detector, DCGM, node-exporter,
kube-state-metrics) over the ClusterIP and the GitHub Actions API, and keeps **one GitHub
issue per active problem** under the `ci-node-health` label. No kubeconfig and no secret
beyond `GITHUB_TOKEN`.

- Slack: `/github subscribe smg-project/smg issues label:"ci-node-health"` in the channel.
  Only "opened" and "closed" reach Slack; body updates while a problem persists do not.
- Acknowledge a known problem (for example a deliberate cordon) by assigning the issue to
  yourself and leaving it open. It closes on its own two runs after the condition clears.
- `GPU XID error` issues are never auto-closed. Close them after looking at the GPU.
- Every run writes a fleet table (per-node ready, GPUs, NPD, disk, runner pods, 24 h XID and
  OOM counts) and queue-wait percentiles to the run's job summary.
- Dry run from a laptop:
  `kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 19090:9090 &`
  then `GITHUB_TOKEN=$(gh auth token) python3 scripts/ci_node_health.py --dry-run --repo smg-project/smg --prom-url http://127.0.0.1:19090`.

### Checks

One issue per check, never per node; the issue body lists the affected nodes.

| Sev | Check | Fires when |
|---|---|---|
| CRIT | H100 node not Ready | `Ready` condition is not `True` |
| CRIT | NPD hardware condition | any of `GpuEcc`, `GpuRowRemap`, `GpuBus`, `GpuCount`, `RdmaLink`, `RdmaLinkFlapping`, `RdmaWpaAuth`, `RdmaRttcc`, `KernelDeadlock`, `ReadonlyFilesystem` is `True` (node-problem-detector never cordons on its own) |
| CRIT | GPU XID error | `DCGM_FI_DEV_XID_ERRORS` changed in the last hour (closed by a human) |
| CRIT | GPU row-remap failure | `DCGM_FI_DEV_ROW_REMAP_FAILURE > 0` |
| CRIT | Node disk over 85% | root (runner emptyDirs, dind) or `/raid` (model cache) |
| CRIT | GPU runner label starved | an H100 job queued over 60 min in any unfinished run |
| CRIT | Monitor cannot reach Prometheus | the node checks were skipped this run |
| WARN | H100 node cordoned | unschedulable for over 2 h |
| WARN | GPU jobs waiting for runners | median H100 queue wait over 30 min in the last 2 h |
| WARN | NPD condition Unknown | an NPD condition Unknown for over half of the last 6 h (plugin timeouts) |
| WARN | GPU memory held by no pod | over 2 GiB allocated with no pod attached for 30 min |
| WARN | GPU over 85C | for 15 min |
| WARN | Workflow runs queued over 24h | runs that will never be picked up |

### Noise rules

- An issue opens on the first sighting and is edited in place while the problem persists,
  which the Slack app does not relay.
- A state finding closes only after it has been absent for two consecutive runs. Checks that
  were not evaluated (Prometheus down) are never closed.
- Event findings (XID) never auto-close and get at most one comment per 24 h. After a human
  closes an XID issue, the same nodes are not re-reported for the hour the lookback still sees
  the event; an XID on another node opens a new issue immediately.
- Kernel OOM kills appear in the job summary only: container-limit OOMs match too.
- The GitHub side reads one page of recent runs (capped at 40 for wait statistics) plus one
  page each of queued and in-progress runs for starvation, with one jobs page per run: normally
  well under 80 API requests per run, about 245 in the worst case.
