package vramfit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name                      = "VRAMFitPlugin"
	VRAMResourceName          = "custom.com/vram"
	GPULayoutAnnotation       = "custom.com/gpu-layout"
	PodVRAMReqAnnotation      = "custom.com/required-vram"
	AssignedGPUIDAnnotation   = "custom.com/assigned-gpu-id"
	AssignedGPUUUIDAnnotation = "custom.com/assigned-gpu-uuid"
	KVCacheHitRateAnnotation  = "scheduling.x-k8s.io/kv-cache-hit-rate"
	KVCacheWeight             = 0.3
	MaxNodeScore              = 100
)

type GPUState struct {
	ID        int    `json:"id"`
	UUID      string `json:"uuid"`
	TotalVRAM int64  `json:"totalVRAM"`
	UsedVRAM  int64  `json:"usedVRAM"`
}
type NodeGPULayout []GPUState
type PodAllocationInfo struct {
	NodeName string
	GPUUUID  string
	VRAMReq  int64
}
type Plugin struct {
	handle         framework.Handle
	mu             sync.RWMutex
	pendingVRAM    map[string]map[string]int64
	podAllocations map[string]PodAllocationInfo
}
type nvlinkScoreCacheState struct {
	totalSiblings      int64
	siblingCountByNode map[string]int64
	siblingCountByRack map[string]int64
}

var _ framework.PreFilterPlugin = &Plugin{}
var _ framework.FilterPlugin = &Plugin{}
var _ framework.ReservePlugin = &Plugin{}
var _ framework.ScorePlugin = &Plugin{}

func (pl *Plugin) Name() string {
	return Name
}
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &Plugin{
		handle:         h,
		pendingVRAM:    make(map[string]map[string]int64),
		podAllocations: make(map[string]PodAllocationInfo),
	}, nil
}
func (pl *Plugin) PreFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) (*framework.PreFilterResult, *framework.Status) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for podKey, allocInfo := range pl.podAllocations {
		info, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(allocInfo.NodeName)
		if err != nil || info.Node() == nil {
			continue
		}
		layoutJSON, exists := info.Node().Annotations[GPULayoutAnnotation]
		if !exists || layoutJSON == "" {
			continue
		}
		var currentGPUs NodeGPULayout
		if err := json.Unmarshal([]byte(layoutJSON), &currentGPUs); err != nil {
			continue
		}
		for _, gpu := range currentGPUs {
			if gpu.UUID == allocInfo.GPUUUID {
				if gpu.UsedVRAM >= allocInfo.VRAMReq {
					if pending := pl.pendingVRAM[allocInfo.NodeName]; pending != nil {
						pending[allocInfo.GPUUUID] -= allocInfo.VRAMReq
						if pending[allocInfo.GPUUUID] <= 0 {
							delete(pending, allocInfo.GPUUUID)
						}
						if len(pending) == 0 {
							delete(pl.pendingVRAM, allocInfo.NodeName)
						}
					}
					delete(pl.podAllocations, podKey)
				}
				break
			}
		}
	}
	return nil, framework.NewStatus(framework.Success, "")
}
func (pl *Plugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}
func (pl *Plugin) parseNodeLayout(node *corev1.Node) (NodeGPULayout, error) {
	layoutJSON, exists := node.Annotations[GPULayoutAnnotation]
	if !exists || layoutJSON == "" {
		return nil, fmt.Errorf("node %s is missing GPU hardware topology annotation (%s)", node.Name, GPULayoutAnnotation)
	}
	var gpus NodeGPULayout
	if err := json.Unmarshal([]byte(layoutJSON), &gpus); err != nil {
		return nil, fmt.Errorf("node %s has malformed GPU topology annotation: %v", node.Name, err)
	}
	return gpus, nil
}
func (pl *Plugin) Filter(ctx context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Success, "")
	}
	node := nodeInfo.Node()
	podNeededVRAM := calculatePodVRAMRequest(pod)
	if podNeededVRAM == 0 {
		return framework.NewStatus(framework.Success, "")
	}
	gpus, err := pl.parseNodeLayout(node)
	if err != nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	if len(gpus) == 0 {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("node %s GPU topology annotation lists 0 available GPUs", node.Name))
	}
	pl.mu.RLock()
	pending := pl.pendingVRAM[node.Name]
	var maxFreeVRAM int64
	foundFitGPU := false
	for _, gpu := range gpus {
		if ctx.Err() != nil {
			pl.mu.RUnlock()
			return framework.NewStatus(framework.Error, fmt.Sprintf("filter cancelled for node %s during processing: %v", node.Name, ctx.Err()))
		}
		var pendingUsed int64
		if pending != nil {
			pendingUsed = pending[gpu.UUID]
		}
		used := gpu.UsedVRAM + pendingUsed
		freeVRAM := gpu.TotalVRAM - used
		if freeVRAM < 0 {
			freeVRAM = 0
		}
		if freeVRAM > maxFreeVRAM {
			maxFreeVRAM = freeVRAM
		}
		if freeVRAM >= podNeededVRAM {
			foundFitGPU = true
		}
	}
	pl.mu.RUnlock()
	if !foundFitGPU {
		var nodeTotalAllocatedVRAM int64
		if req := nodeInfo.Requested; req != nil && req.ScalarResources != nil {
			nodeTotalAllocatedVRAM = req.ScalarResources[corev1.ResourceName(VRAMResourceName)]
		}
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("insufficient single-GPU VRAM on node %s: pod needs %d MiB, best available card has %d MiB free (including unconfirmed bindings) (node total allocated: %d MiB across %d GPUs)", node.Name, toMiB(podNeededVRAM), toMiB(maxFreeVRAM), toMiB(nodeTotalAllocatedVRAM), len(gpus)))
	}
	return framework.NewStatus(framework.Success, "")
}
func (pl *Plugin) Reserve(ctx context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	podNeededVRAM := calculatePodVRAMRequest(pod)
	if podNeededVRAM == 0 {
		return framework.NewStatus(framework.Success, "")
	}
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil || nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get node %s info: %v", nodeName, err))
	}
	node := nodeInfo.Node()
	gpus, err := pl.parseNodeLayout(node)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if ctx.Err() != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("reserve cancelled for node %s: %v", nodeName, ctx.Err()))
	}
	if pl.pendingVRAM[nodeName] == nil {
		pl.pendingVRAM[nodeName] = make(map[string]int64)
	}
	pending := pl.pendingVRAM[nodeName]
	targetIdx := -1
	var minPostFreeVRAM int64 = int64(^uint64(0) >> 1)
	for i, gpu := range gpus {
		used := gpu.UsedVRAM + pending[gpu.UUID]
		freeVRAM := gpu.TotalVRAM - used
		if freeVRAM >= podNeededVRAM {
			postFree := freeVRAM - podNeededVRAM
			if postFree < minPostFreeVRAM {
				minPostFreeVRAM = postFree
				targetIdx = i
			}
		}
	}
	if targetIdx == -1 {
		return framework.NewStatus(framework.Unschedulable, "race condition: no single GPU has enough VRAM remaining during reserve")
	}
	pending[gpus[targetIdx].UUID] += podNeededVRAM
	podKey := getPodKey(pod)
	pl.podAllocations[podKey] = PodAllocationInfo{
		NodeName: nodeName,
		GPUUUID:  gpus[targetIdx].UUID,
		VRAMReq:  podNeededVRAM,
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[AssignedGPUIDAnnotation] = strconv.Itoa(gpus[targetIdx].ID)
	pod.Annotations[AssignedGPUUUIDAnnotation] = gpus[targetIdx].UUID
	return framework.NewStatus(framework.Success, "")
}
func (pl *Plugin) Unreserve(ctx context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) {
	podNeededVRAM := calculatePodVRAMRequest(pod)
	if podNeededVRAM == 0 {
		return
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	podKey := getPodKey(pod)
	allocInfo, exists := pl.podAllocations[podKey]
	if !exists {
		return
	}
	if pending := pl.pendingVRAM[nodeName]; pending != nil {
		pending[allocInfo.GPUUUID] -= podNeededVRAM
		if pending[allocInfo.GPUUUID] <= 0 {
			delete(pending, allocInfo.GPUUUID)
		}
		if len(pending) == 0 {
			delete(pl.pendingVRAM, nodeName)
		}
	}
	delete(pl.podAllocations, podKey)
}
func (pl *Plugin) Score(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil || nodeInfo.Node() == nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("failed to get node %s info: %v", nodeName, err))
	}
	node := nodeInfo.Node()
	var kvCacheScore int64 = 0
	if hitRateStr, exists := node.Annotations[KVCacheHitRateAnnotation]; exists && hitRateStr != "" {
		if hitRate, err := strconv.ParseFloat(hitRateStr, 64); err == nil {
			baseKvScore := int64(hitRate * float64(MaxNodeScore))
			kvCacheScore = int64(float64(baseKvScore) * KVCacheWeight)
		}
	} else {
		kvCacheScore = int64(10 * KVCacheWeight)
	}
	jobName := pod.Labels["gpu-job-name"]
	if jobName == "" {
		finalScore := kvCacheScore
		if finalScore > MaxNodeScore {
			finalScore = MaxNodeScore
		}
		return finalScore, framework.NewStatus(framework.Success, "")
	}
	const cacheKey = "vramfit-nvlink-score-lazy-cache"
	var cache *nvlinkScoreCacheState
	if data, err := state.Read(cacheKey); err == nil {
		cache = data.(*nvlinkScoreCacheState)
	} else {
		allNodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
		if err != nil {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("failed to list nodes: %v", err))
		}
		nodeCounts := make(map[string]int64)
		rackCounts := make(map[string]int64)
		var totalSiblings int64
		for _, nInfo := range allNodes {
			if nInfo == nil || nInfo.Node() == nil {
				continue
			}
			currNode := nInfo.Node()
			rack := currNode.Labels["topology.kubernetes.io/rack"]
			for _, existingPod := range nInfo.Pods {
				if existingPod == nil || existingPod.Pod == nil {
					continue
				}
				if existingPod.Pod.Labels["gpu-job-name"] == jobName &&
					(existingPod.Pod.Name != pod.Name || existingPod.Pod.Namespace != pod.Namespace) {
					nodeCounts[currNode.Name]++
					if rack != "" {
						rackCounts[rack]++
					}
					totalSiblings++
				}
			}
		}
		cache = &nvlinkScoreCacheState{
			totalSiblings:      totalSiblings,
			siblingCountByNode: nodeCounts,
			siblingCountByRack: rackCounts,
		}
		state.Write(cacheKey, cache)
	}
	if cache.totalSiblings == 0 {
		finalScore := kvCacheScore
		if finalScore > MaxNodeScore {
			finalScore = MaxNodeScore
		}
		return finalScore, framework.NewStatus(framework.Success, "")
	}
	targetRack := node.Labels["topology.kubernetes.io/rack"]
	sameNodeCount := cache.siblingCountByNode[nodeName]
	var sameRackCount int64
	if targetRack != "" {
		sameRackCount = cache.siblingCountByRack[targetRack] - sameNodeCount
	}
	crossCount := cache.totalSiblings - sameNodeCount - sameRackCount
	const (
		sameNodeBase  = 70
		sameRackBase  = 30
		crossNodeBase = 0
		countBonusCap = 20
	)
	localMin := func(a, b int64) int64 {
		if a < b {
			return a
		}
		return b
	}
	var topologyScore int64 = 0
	switch {
	case sameNodeCount > 0:
		topologyScore = sameNodeBase + localMin(sameNodeCount*5, countBonusCap)
	case sameRackCount > 0:
		topologyScore = sameRackBase + localMin(sameRackCount*2, countBonusCap)
	case crossCount > 0:
		topologyScore = crossNodeBase
	default:
		topologyScore = 0
	}
	totalScore := topologyScore + kvCacheScore
	if totalScore > MaxNodeScore {
		totalScore = MaxNodeScore
	}
	return totalScore, framework.NewStatus(framework.Success, "")
}
func getPodKey(pod *corev1.Pod) string {
	if string(pod.UID) != "" {
		return string(pod.UID)
	}
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}
func calculatePodVRAMRequest(pod *corev1.Pod) int64 {
	var maxInitVRAM int64
	for _, c := range pod.Spec.InitContainers {
		if v := containerVRAM(c); v > maxInitVRAM {
			maxInitVRAM = v
		}
	}
	var sumAppVRAM int64
	for _, c := range pod.Spec.Containers {
		sumAppVRAM += containerVRAM(c)
	}
	if maxInitVRAM > sumAppVRAM {
		return maxInitVRAM
	}
	return sumAppVRAM
}
func containerVRAM(c corev1.Container) int64 {
	if q, ok := c.Resources.Limits[corev1.ResourceName(VRAMResourceName)]; ok {
		return q.Value()
	}
	if q, ok := c.Resources.Requests[corev1.ResourceName(VRAMResourceName)]; ok {
		return q.Value()
	}
	return 0
}
func toMiB(bytes int64) int64 {
	return bytes / (1024 * 1024)
}
func (s *nvlinkScoreCacheState) Clone() framework.StateData {
	return s
}

func (pl *Plugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}
