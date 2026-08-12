package vramfit

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type mockNodeInfoLister struct {
	framework.NodeInfoLister
	nodeInfo *framework.NodeInfo
}

func (m *mockNodeInfoLister) Get(nodeName string) (*framework.NodeInfo, error) {
	return m.nodeInfo, nil
}

type mockSharedLister struct {
	framework.SharedLister
	lister *mockNodeInfoLister
}

func (m *mockSharedLister) NodeInfos() framework.NodeInfoLister {
	return m.lister
}

func makeTestNode(name string, layout NodeGPULayout) *corev1.Node {
	bytes, _ := json.Marshal(layout)
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{GPULayoutAnnotation: string(bytes)},
		},
	}
}

type mockHandle struct {
	framework.Handle
	sharedLister framework.SharedLister
}

func (mh *mockHandle) SnapshotSharedLister() framework.SharedLister {
	return mh.sharedLister
}

// makeTestPod builds a Pod whose VRAM demand goes through Resources.Limits —
// the ONLY data source calculatePodVRAMRequest reads (see vram_request.go).
// Do NOT use a PodVRAMReqAnnotation here; that field was intentionally
// removed so there is a single source of truth for VRAM demand. A test that
// sets only the annotation will silently produce podNeededVRAM == 0 and the
// Reserve/Filter logic under test will never actually run — see the failure
// mode this caused before this fix.
func makeTestPod(name string, uid types.UID, vramBytes int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
			UID:       uid,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "test-container",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName(VRAMResourceName): *resource.NewQuantity(vramBytes, resource.BinarySI),
						},
					},
				},
			},
		},
	}
}

// 1. 测试 Best-Fit 选卡策略
func TestReserve_PicksBestFitGPU(t *testing.T) {
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 16000, UsedVRAM: 6000},  // 剩 10000
		{ID: 1, UUID: "gpu-1", TotalVRAM: 16000, UsedVRAM: 11000}, // 剩 5000  -> best fit
		{ID: 2, UUID: "gpu-2", TotalVRAM: 16000, UsedVRAM: 8000},  // 剩 8000
	}
	node := makeTestNode("node-test", layout)
	ni := framework.NewNodeInfo()
	ni.SetNode(node)

	mockLister := &mockNodeInfoLister{nodeInfo: ni}
	sharedLister := &mockSharedLister{lister: mockLister}
	mh := &mockHandle{sharedLister: sharedLister}

	pl := &Plugin{
		handle:         mh,
		pendingVRAM:    make(map[string]map[string]int64),
		podAllocations: make(map[string]PodAllocationInfo),
	}

	// 需求 4000，通过 Resources.Limits 声明，不是 annotation
	pod := makeTestPod("infer-pod", "uid-123", 4000)

	status := pl.Reserve(context.TODO(), nil, pod, "node-test")
	if !status.IsSuccess() {
		t.Fatalf("Reserve failed: %v", status.Message())
	}

	alloc, exists := pl.podAllocations[string(pod.UID)]
	if !exists {
		t.Fatalf("allocation record missing — check calculatePodVRAMRequest is reading Resources.Limits correctly")
	}
	if alloc.GPUUUID != "gpu-1" {
		t.Errorf("best-fit strategy failure: expected gpu-1, got %s", alloc.GPUUUID)
	}
}

// 2. 测试无卡可选时安全拦截
func TestReserve_NoGPUFits_ReturnsUnschedulable(t *testing.T) {
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 16000, UsedVRAM: 14000}, // 剩 2000
		{ID: 1, UUID: "gpu-1", TotalVRAM: 16000, UsedVRAM: 15000}, // 剩 1000
	}
	node := makeTestNode("node-test", layout)
	ni := framework.NewNodeInfo()
	ni.SetNode(node)

	mockLister := &mockNodeInfoLister{nodeInfo: ni}
	sharedLister := &mockSharedLister{lister: mockLister}
	mh := &mockHandle{sharedLister: sharedLister}

	pl := &Plugin{
		handle:         mh,
		pendingVRAM:    make(map[string]map[string]int64),
		podAllocations: make(map[string]PodAllocationInfo),
	}

	// 需求 5000，超过两张卡各自的剩余空间
	pod := makeTestPod("heavy-pod", "uid-456", 5000)

	status := pl.Reserve(context.TODO(), nil, pod, "node-test")
	if status.Code() != framework.Unschedulable {
		t.Errorf("expected Unschedulable, got code=%v msg=%s", status.Code(), status.Message())
	}
}

// 3. 验证 Unreserve 内存泄漏防御
func TestUnreserve_RollsBackPendingCorrectly(t *testing.T) {
	pl := &Plugin{
		pendingVRAM:    make(map[string]map[string]int64),
		podAllocations: make(map[string]PodAllocationInfo),
	}
	podKey := "uid-999"
	pl.podAllocations[podKey] = PodAllocationInfo{
		NodeName: "node-test", GPUUUID: "gpu-0", VRAMReq: 1000,
	}
	pl.pendingVRAM["node-test"] = map[string]int64{"gpu-0": 1000}

	// VRAM 需求同样通过 Resources.Limits 声明，和 Reserve 阶段保持一致，
	// 否则 calculatePodVRAMRequest 在 Unreserve 里算出 0，
	// 会在 podNeededVRAM == 0 分支提前 return，根本测不到回滚逻辑。
	pod := makeTestPod("leaked-pod", "uid-999", 1000)

	pl.Unreserve(context.TODO(), nil, pod, "node-test")

	if _, exists := pl.podAllocations[podKey]; exists {
		t.Error("leak detected: podAllocations record still remains")
	}
	if _, exists := pl.pendingVRAM["node-test"]; exists {
		t.Error("leak detected: pendingVRAM map for node was not pruned")
	}
}
