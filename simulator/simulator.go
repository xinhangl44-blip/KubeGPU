package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	schedulerv1alpha1 "lawson.com/gpu-crd/api/v1alpha1"
)

const (
	GPULayoutAnnotation = "custom.com/gpu-layout"
	instrumentationName = "gpu-scheduler-simulator"
)

type gpuState struct {
	ID        int    `json:"id"`
	UUID      string `json:"uuid"`
	TotalVRAM int64  `json:"totalVRAM"`
	UsedVRAM  int64  `json:"usedVRAM"`
}

type nodeState struct {
	Name string
	GPUs []gpuState
}

type jobState struct {
	Name        string
	GangSize    int
	VRAMPerGPU  int64
	Priority    int32
	SubmittedAt time.Time
}

type OutcomeKind string

const (
	OutcomeScheduled     OutcomeKind = "Scheduled"
	OutcomeWaiting       OutcomeKind = "Waiting"
	OutcomeUnschedulable OutcomeKind = "Unschedulable"
	OutcomeDeadlock      OutcomeKind = "DeadlockRisk"
)

type Assignment struct {
	PodIndex int    `json:"podIndex"`
	NodeName string `json:"nodeName"`
	GPUUUID  string `json:"gpuUUID"`
}

type JobResult struct {
	JobName       string       `json:"jobName"`
	Outcome       OutcomeKind  `json:"outcome"`
	Reason        string       `json:"reason"`
	Priority      int32        `json:"priority"`
	GangSize      int          `json:"gangSize"`
	VRAMPerGPUMiB int64        `json:"vramPerGPUMiB"`
	Assignments   []Assignment `json:"assignments,omitempty"`
	EstimatedWait string       `json:"estimatedWait,omitempty"`
}

type ClusterInfo struct {
	NodeCount    int   `json:"nodeCount"`
	TotalGPUs    int   `json:"totalGPUs"`
	TotalVRAMMiB int64 `json:"totalVRAMMiB"`
	UsedVRAMMiB  int64 `json:"usedVRAMMiB"`
	FreeVRAMMiB  int64 `json:"freeVRAMMiB"`
}

type SimulatorReport struct {
	GeneratedAt    string      `json:"generatedAt"`
	ClusterSummary ClusterInfo `json:"clusterSummary"`
	Results        []JobResult `json:"results"`
}

type Simulator struct {
	nodes       []nodeState
	pendingVRAM map[string]map[string]int64
}

func Run(ctx context.Context, namespace string) (*SimulatorReport, error) {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	ctx, span := tracer.Start(ctx, "Simulator.Run", trace.WithAttributes(attribute.String("k8s.namespace", namespace)))
	defer span.End()
	nodes, err := fetchNodes(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("fetch nodes: %w", err)
	}
	jobs, err := fetchGPUJobs(ctx, namespace)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("fetch GPUJobs: %w", err)
	}
	span.SetAttributes(attribute.Int("cluster.node_count", len(nodes)), attribute.Int("cluster.job_count", len(jobs)))
	sim := &Simulator{
		nodes:       nodes,
		pendingVRAM: make(map[string]map[string]int64),
	}
	results := sim.schedule(ctx, jobs)
	return &SimulatorReport{
		GeneratedAt:    time.Now().Format(time.RFC3339),
		ClusterSummary: sim.clusterInfo(),
		Results:        results,
	}, nil
}

func newK8sClient() (*kubernetes.Clientset, error) {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func newCRDClient() (client.Client, error) {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := schedulerv1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

func fetchNodes(ctx context.Context) ([]nodeState, error) {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	_, span := tracer.Start(ctx, "fetchNodes")
	defer span.End()
	cs, err := newK8sClient()
	if err != nil {
		return nil, err
	}
	nodeList, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var result []nodeState
	for _, n := range nodeList.Items {
		layoutJSON, ok := n.Annotations[GPULayoutAnnotation]
		if !ok || layoutJSON == "" {
			continue
		}
		var gpus []gpuState
		if err := json.Unmarshal([]byte(layoutJSON), &gpus); err != nil {
			return nil, fmt.Errorf("node %s malformed gpu-layout: %w", n.Name, err)
		}
		result = append(result, nodeState{Name: n.Name, GPUs: gpus})
	}
	return result, nil
}

func fetchGPUJobs(ctx context.Context, namespace string) ([]jobState, error) {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	_, span := tracer.Start(ctx, "fetchGPUJobs")
	defer span.End()
	c, err := newCRDClient()
	if err != nil {
		return nil, err
	}
	jobList := &schedulerv1alpha1.GPUJobList{}
	if err := c.List(ctx, jobList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var result []jobState
	for _, j := range jobList.Items {
		vramQty, err := resource.ParseQuantity(j.Spec.VRAMPerGPU)
		if err != nil {
			return nil, fmt.Errorf("job %s invalid vramPerGPU: %w", j.Name, err)
		}
		gangSize := int(j.Spec.GangSize)
		if gangSize <= 0 {
			gangSize = int(j.Spec.GPUCount)
		}
		result = append(result, jobState{
			Name:        j.Name,
			GangSize:    gangSize,
			VRAMPerGPU:  vramQty.Value(),
			Priority:    j.Spec.Priority,
			SubmittedAt: j.CreationTimestamp.Time,
		})
	}
	return result, nil
}

func (s *Simulator) schedule(ctx context.Context, jobs []jobState) []JobResult {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	ctx, span := tracer.Start(ctx, "Simulator.schedule")
	defer span.End()
	_, sortSpan := tracer.Start(ctx, "schedule.sortJobs")
	sorted := make([]jobState, len(jobs))
	copy(sorted, jobs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Priority != sorted[j].Priority {
			return sorted[i].Priority > sorted[j].Priority
		}
		return sorted[i].SubmittedAt.Before(sorted[j].SubmittedAt)
	})
	sortSpan.End()
	results := make([]JobResult, 0, len(sorted))
	for _, job := range sorted {
		results = append(results, s.scheduleJob(ctx, job))
	}
	s.detectDeadlock(ctx, results)
	return results
}

func (s *Simulator) scheduleJob(ctx context.Context, job jobState) JobResult {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	ctx, span := tracer.Start(ctx, "Simulator.scheduleJob", trace.WithAttributes(attribute.String("job.name", job.Name), attribute.Int("job.gang_size", job.GangSize), attribute.Int64("job.vram_demand_mib", toMiB(job.VRAMPerGPU))))
	defer span.End()
	assignments := make([]Assignment, 0, job.GangSize)
	for i := 0; i < job.GangSize; i++ {
		a, ok := s.tryAssign(ctx, job, i)
		if !ok {
			for _, prev := range assignments {
				s.releaseVRAM(prev.NodeName, prev.GPUUUID, job.VRAMPerGPU)
			}
			if i == 0 {
				span.SetAttributes(attribute.String("job.outcome", string(OutcomeUnschedulable)))
				return JobResult{
					JobName:       job.Name,
					Outcome:       OutcomeUnschedulable,
					Priority:      job.Priority,
					GangSize:      job.GangSize,
					VRAMPerGPUMiB: toMiB(job.VRAMPerGPU),
					Reason:        fmt.Sprintf("no single GPU has %d MiB free", toMiB(job.VRAMPerGPU)),
				}
			}
			span.SetAttributes(attribute.String("job.outcome", string(OutcomeWaiting)))
			return JobResult{
				JobName:       job.Name,
				Outcome:       OutcomeWaiting,
				Priority:      job.Priority,
				GangSize:      job.GangSize,
				VRAMPerGPUMiB: toMiB(job.VRAMPerGPU),
				Reason:        fmt.Sprintf("gang partially placed (%d/%d), waiting for capacity", i, job.GangSize),
				EstimatedWait: estimateWait(job.Priority),
			}
		}
		assignments = append(assignments, a)
	}
	span.SetAttributes(attribute.String("job.outcome", string(OutcomeScheduled)))
	return JobResult{
		JobName:       job.Name,
		Outcome:       OutcomeScheduled,
		Priority:      job.Priority,
		GangSize:      job.GangSize,
		VRAMPerGPUMiB: toMiB(job.VRAMPerGPU),
		Reason:        fmt.Sprintf("all %d gang members placed", job.GangSize),
		Assignments:   assignments,
	}
}

func (s *Simulator) tryAssign(ctx context.Context, job jobState, podIdx int) (Assignment, bool) {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	_, span := tracer.Start(ctx, "Simulator.tryAssign", trace.WithAttributes(attribute.Int("pod.index", podIdx)))
	defer span.End()
	type candidate struct {
		nodeName     string
		gpuUUID      string
		postFreeVRAM int64
	}
	var best *candidate
	for _, node := range s.nodes {
		pending := s.pendingVRAM[node.Name]
		for _, gpu := range node.GPUs {
			var pendingUsed int64
			if pending != nil {
				pendingUsed = pending[gpu.UUID]
			}
			used := gpu.UsedVRAM + pendingUsed
			free := gpu.TotalVRAM - used
			if free < 0 {
				free = 0
			}
			if free < job.VRAMPerGPU {
				continue
			}
			postFree := free - job.VRAMPerGPU
			if best == nil || postFree < best.postFreeVRAM {
				best = &candidate{
					nodeName:     node.Name,
					gpuUUID:      gpu.UUID,
					postFreeVRAM: postFree,
				}
			}
		}
	}
	if best == nil {
		span.SetAttributes(attribute.Bool("assign.success", false))
		return Assignment{}, false
	}
	s.reserveVRAM(best.nodeName, best.gpuUUID, job.VRAMPerGPU)
	span.SetAttributes(attribute.Bool("assign.success", true), attribute.String("assign.target_node", best.nodeName), attribute.String("assign.target_gpu", best.gpuUUID))
	return Assignment{PodIndex: podIdx, NodeName: best.nodeName, GPUUUID: best.gpuUUID}, true
}

func (s *Simulator) reserveVRAM(nodeName, uuid string, bytes int64) {
	if s.pendingVRAM[nodeName] == nil {
		s.pendingVRAM[nodeName] = make(map[string]int64)
	}
	s.pendingVRAM[nodeName][uuid] += bytes
}

func (s *Simulator) releaseVRAM(nodeName, uuid string, bytes int64) {
	if m := s.pendingVRAM[nodeName]; m != nil {
		m[uuid] -= bytes
		if m[uuid] <= 0 {
			delete(m, uuid)
		}
		if len(m) == 0 {
			delete(s.pendingVRAM, nodeName)
		}
	}
}

func (s *Simulator) detectDeadlock(ctx context.Context, results []JobResult) {
	tracer := otel.GetTracerProvider().Tracer(instrumentationName)
	_, span := tracer.Start(ctx, "Simulator.detectDeadlock")
	defer span.End()
	var waitingIdx []int
	for i, r := range results {
		if r.Outcome == OutcomeWaiting {
			waitingIdx = append(waitingIdx, i)
		}
	}
	if len(waitingIdx) < 2 {
		return
	}
	clusterFree := s.totalFreeVRAM()
	var totalDemand int64
	for _, idx := range waitingIdx {
		r := results[idx]
		totalDemand += int64(r.GangSize) * (r.VRAMPerGPUMiB * 1024 * 1024)
	}
	if totalDemand > clusterFree {
		for _, idx := range waitingIdx {
			results[idx].Outcome = OutcomeDeadlock
			results[idx].Reason = fmt.Sprintf("%s — combined waiting demand %d MiB exceeds cluster free %d MiB", results[idx].Reason, toMiB(totalDemand), toMiB(clusterFree))
		}
	}
}

func (s *Simulator) totalFreeVRAM() int64 {
	var total int64
	for _, node := range s.nodes {
		pending := s.pendingVRAM[node.Name]
		for _, gpu := range node.GPUs {
			var p int64
			if pending != nil {
				p = pending[gpu.UUID]
			}
			free := gpu.TotalVRAM - gpu.UsedVRAM - p
			if free > 0 {
				total += free
			}
		}
	}
	return total
}

func (s *Simulator) clusterInfo() ClusterInfo {
	var totalGPUs int
	var totalVRAM, usedVRAM int64
	for _, node := range s.nodes {
		totalGPUs += len(node.GPUs)
		for _, gpu := range node.GPUs {
			totalVRAM += gpu.TotalVRAM
			usedVRAM += gpu.UsedVRAM
		}
	}
	return ClusterInfo{
		NodeCount:    len(s.nodes),
		TotalGPUs:    totalGPUs,
		TotalVRAMMiB: toMiB(totalVRAM),
		UsedVRAMMiB:  toMiB(usedVRAM),
		FreeVRAMMiB:  toMiB(totalVRAM - usedVRAM),
	}
}

func estimateWait(priority int32) string {
	switch {
	case priority >= 100:
		return "~5s"
	case priority >= 50:
		return "~15s"
	default:
		return "~30s"
	}
}

func toMiB(bytes int64) int64 {
	return bytes / (1024 * 1024)
}
