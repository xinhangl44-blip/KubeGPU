# KubeGPU Scheduler

A Kubernetes Scheduler Framework plugin for GPU workloads. Extends the default scheduler with VRAM-aware bin-packing, gang scheduling, deadlock detection, and KV cache-aware inference routing.

Built as a learning project during Y1 at NUS CEG, deployed on k3s + WSL2 with RTX 5060 Ti.

---

## Features

| Feature | Description |
|---|---|
| **VRAM-aware Filter** | Reads per-GPU VRAM from node annotation, rejects nodes where no single card has enough free VRAM |
| **Best-fit GPU selection** | Reserve picks the GPU with least post-allocation free VRAM, reducing fragmentation |
| **Gang scheduling** | All-or-nothing: all Pods in a GPUJob must be placed before any can start |
| **Deadlock detection** | Background worker evicts the lowest-priority waiting gang when multiple jobs are mutually stuck |
| **Priority + FIFO ordering** | Higher priority scheduled first; ties broken by submission time |
| **KV cache-aware Score** | Routes inference Pods to nodes with higher vLLM prefix cache hit rate |
| **GPU usage exporter** | DaemonSet scrapes `nvidia-smi` every 5s and patches `UsedVRAM` into node annotation |
| **Dry-run Simulator** | CLI that predicts scheduling outcomes by reading real cluster state, without modifying anything |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Control Plane                      │   │
│  │  GPUJob CRD → Controller → Pods                 │   │
│  │  (labels: gpu-job-name, gpu-job-min-member)     │   │
│  └──────────────────┬──────────────────────────────┘   │
│                     │                                   │
│  ┌──────────────────▼──────────────────────────────┐   │
│  │         my-gpu-scheduler (this project)         │   │
│  │                                                 │   │
│  │  PreFilter → Filter      → Reserve              │   │
│  │  (gang)      (VRAMFit)    (per-GPU ledger)      │   │
│  │                                                 │   │
│  │  Permit    → Score       → Bind                 │   │
│  │  (gang)      (NVLink +    (default)             │   │
│  │              KV cache)                          │   │
│  └──────────────────┬──────────────────────────────┘   │
│                     │                                   │
│  ┌──────────────────▼──────────────────────────────┐   │
│  │              GPU Worker Node                    │   │
│  │  annotation: custom.com/gpu-layout              │   │
│  │  [{id:0, uuid:"...", totalVRAM:...,             │   │
│  │    usedVRAM:...}]          ▲                    │   │
│  │                            │                    │   │
│  │         gpu-usage-exporter (DaemonSet)          │   │
│  │         nvidia-smi → patch node annotation      │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Data flow**:
1. User creates a `GPUJob` CR
2. Controller generates Pods with `gpu-job-name` / `gpu-job-min-member` labels and `custom.com/vram` resource limits
3. Scheduler plugin chain: gang PreFilter validates quorum → VRAMFit Filter checks per-GPU free VRAM → Reserve locks VRAM in the in-memory ledger → gang Permit blocks until all members arrive → Score ranks nodes by NVLink topology + KV cache hit rate
4. `gpu-usage-exporter` DaemonSet keeps `UsedVRAM` fresh in node annotations every 5 seconds

---

## Quick Start

### Prerequisites

- k3s or kind cluster
- `kubectl` configured
- Go 1.21+
- `nvidia-smi` available on GPU nodes

### 1. Install CRDs and RBAC

```bash
kubectl apply -f config/crd/
kubectl apply -f config/rbac/
kubectl apply -f priority_class.yaml
```

### 2. Annotate GPU nodes with hardware topology

```bash
# Get your GPU UUID first
nvidia-smi --query-gpu=uuid --format=csv,noheader

# Annotate the node (replace values with your actual hardware)
kubectl annotate node <node-name> \
  custom.com/gpu-layout='[{"id":0,"uuid":"GPU-xxxx","totalVRAM":21474836480,"usedVRAM":0}]'
```

### 3. Build and run the scheduler

```bash
go build -o bin/my-kube-scheduler ./cmd/gpu-scheduler

./bin/my-kube-scheduler \
  --config=gpu-scheduler-config.yaml \
  --secure-port=10261 \
  -v=4
```

### 4. Deploy the GPU usage exporter

```bash
kubectl apply -f GPU_deam.yaml
```

### 5. Run the controller

```bash
go run main.go
```

### 6. Submit a GPUJob

```bash
kubectl apply -f good-job.yaml
kubectl get pods -w
```

Example GPUJob:

```yaml
apiVersion: scheduler.lawson.com/v1alpha1
kind: GPUJob
metadata:
  name: resnet-training
  namespace: default
spec:
  gangSize: 2
  gpuCount: 2
  priority: 100
  vramPerGPU: "2Gi"
```

---

## Design Decisions

### Per-GPU VRAM tracking instead of node-level totals

The default scheduler only understands node-level resources. Two Pods scheduled to the same node might both pass based on total VRAM, but if each needs a card with 16 GiB and the node has two 8 GiB cards, neither actually fits. VRAMFit Filter checks per-card free VRAM independently.

### In-memory Reserve ledger instead of relying on `nodeInfo.Requested()`

`nodeInfo.Requested()` only reflects Pods that have completed Bind. Between Reserve and Bind there is a window where concurrent scheduling passes can double-book the same GPU. The `pendingVRAM` map closes this window: Reserve adds a hold immediately, Unreserve rolls it back if any later stage fails.

### Gang scheduling deadlock detection is heuristic, not graph-based

The detector uses a timer-based heuristic: if 2+ gangs have been waiting longer than 10 seconds simultaneously, the lowest-priority one is evicted. This is not a strict wait-for-graph cycle detector. False positives are acceptable at this cluster scale given the implementation simplicity.

### KV cache score weight

KV cache hit rate contributes 30% of the Score (`KVCacheWeight = 0.3`), NVLink topology contributes 70%. Co-location for NVLink bandwidth matters more than cache locality for most training workloads, but cache routing is still meaningful for inference.

---

## Benchmark

**Environment**: single-node k3s on WSL2, RTX 5060 Ti (20 GiB VRAM)

**Workload**: 24 concurrent GPUJobs

| Type | Count | gangSize | vramPerGPU | Priority |
|------|-------|----------|------------|----------|
| small | 12 | 1 | 512 MiB | 110 |
| medium | 9 | 2 | 1024 MiB | 50 |
| large | 3 | 2 | 2048 MiB | 10 |

### Results

| Metric | KubeGPU | Default Scheduler |
|--------|---------|-------------------|
| Overall success rate | **79.2%** | 79.2% |
| medium P50 latency | **11.1s** | 24.1s |
| medium P95 latency | 127.1s | 91.4s |
| small success rate | 100% | 100% |
| large success rate | 0% | 0% |

**Medium-type gang jobs are scheduled 2.2× faster at P50** (11.1s vs 24.1s). These are gangSize=2 workloads — exactly the scenario this scheduler is designed for. VRAM-aware best-fit selection finds the right card faster than the default scheduler's approach.

Large jobs failed on both schedulers due to insufficient remaining VRAM after small and medium jobs were placed — a capacity constraint, not a scheduler bug.

The default scheduler does not understand `custom.com/vram`. It succeeds on some jobs by coincidence rather than design. In a multi-node cluster with heterogeneous GPU sizes, this would produce OOM kills rather than scheduling failures.

---

## Dry-run Simulator

Predict scheduling outcomes without submitting anything:

```bash
go run cmd/simulator/main.go --namespace default --out simulation.json
```

Sample output:

```json
{
  "generatedAt": "2026-06-24T10:00:00Z",
  "clusterSummary": {
    "nodeCount": 1,
    "totalGPUs": 2,
    "totalVRAMMiB": 40960,
    "freeVRAMMiB": 38912
  },
  "results": [
    {
      "jobName": "resnet-training",
      "outcome": "Scheduled",
      "gangSize": 2,
      "assignments": [
        {"podIndex": 0, "nodeName": "laptop-6ha1jio1", "gpuUUID": "gpu-0"},
        {"podIndex": 1, "nodeName": "laptop-6ha1jio1", "gpuUUID": "gpu-1"}
      ]
    }
  ]
}
```

---

## Known Limitations

- **Single-node tested only**: validated on a single-node k3s setup. Multi-node NVLink topology scoring is implemented but untested against real NVLink hardware.
- **`UsedVRAM` eventual consistency**: GPU usage exporter patches annotations every 5 seconds. Rapid Pod churn can cause a stale window. The `pendingVRAM` ledger covers new reservations but evicted Pods' released VRAM may take up to 5 seconds to reflect.
- **No running-pod preemption**: deadlock detection only evicts Pods still in the Permit waiting stage. There is no implementation that deletes a running low-priority Pod to free VRAM for a higher-priority one.
- **Hardcoded priority tiers**: three fixed PriorityClass objects (10/50/100). Adding new tiers requires code changes.
- **Sidecar containers**: init containers with `restartPolicy=Always` (K8s 1.29+ sidecar semantics) are treated as regular init containers, slightly under-estimating their VRAM contribution.

---

## Repository Structure

```
.
├── api/v1alpha1/          # GPUJob CRD types
├── cmd/
│   └── gpu-scheduler/     # Scheduler binary entry point
├── config/
│   ├── crd/               # CRD manifests
│   ├── rbac/              # ServiceAccount, ClusterRole, ClusterRoleBinding
│   └── samples/           # Example GPUJob YAMLs
├── internal/
│   └── controller/        # GPUJob reconcile loop
├── pkg/
│   └── plugins/
│       ├── gang/          # Gang scheduling: PreFilter, Permit, PostFilter
│       └── vramfit/       # VRAM filter, Reserve ledger, Score
├── benchmark.py           # Benchmark tool
├── generate_jobs.py       # GPUJob batch generator
├── GPU_deam.yaml          # gpu-usage-exporter DaemonSet
├── gpu-scheduler-config.yaml
└── priority_class.yaml
```

---

## References

- [Kubernetes Scheduler Framework](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/)
- [Volcano](https://github.com/volcano-sh/volcano) — gang scheduling reference
- [koordinator](https://github.com/koordinator-sh/koordinator) — GPU bin-packing reference
- [vLLM metrics](https://docs.vllm.ai/en/latest/serving/metrics.html) — KV cache hit rate source
