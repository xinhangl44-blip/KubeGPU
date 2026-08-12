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

// ── helpers ───────────────────────────────────────────────────────────────────

func makeContainer(name string, limitBytes int64) corev1.Container {
	qty := resource.NewQuantity(limitBytes, resource.BinarySI)
	return corev1.Container{
		Name: name,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceName(VRAMResourceName): *qty,
			},
		},
	}
}

func makeContainerRequestOnly(name string, requestBytes int64) corev1.Container {
	qty := resource.NewQuantity(requestBytes, resource.BinarySI)
	return corev1.Container{
		Name: name,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceName(VRAMResourceName): *qty,
			},
		},
	}
}

func makeNodeWithLayout(name string, layout NodeGPULayout) *corev1.Node {
	b, _ := json.Marshal(layout)
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{GPULayoutAnnotation: string(b)},
		},
	}
}

func makeNodeInfo(node *corev1.Node) *framework.NodeInfo {
	ni := framework.NewNodeInfo()
	ni.SetNode(node)
	return ni
}

func makePodWithVRAM(uid types.UID, limitBytes int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       uid,
			Namespace: "default",
			Name:      "test-pod",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				makeContainer("main", limitBytes),
			},
		},
	}
}

// ── containerVRAM ─────────────────────────────────────────────────────────────

func TestContainerVRAM_Limits(t *testing.T) {
	c := makeContainer("c", 4096)
	if got := containerVRAM(c); got != 4096 {
		t.Errorf("got %d, want 4096", got)
	}
}

func TestContainerVRAM_RequestsWhenNoLimits(t *testing.T) {
	c := makeContainerRequestOnly("c", 2048)
	if got := containerVRAM(c); got != 2048 {
		t.Errorf("got %d, want 2048", got)
	}
}

func TestContainerVRAM_LimitsTakePrecedenceOverRequests(t *testing.T) {
	qty := resource.NewQuantity(8192, resource.BinarySI)
	reqQty := resource.NewQuantity(4096, resource.BinarySI)
	c := corev1.Container{
		Name: "c",
		Resources: corev1.ResourceRequirements{
			Limits:   corev1.ResourceList{corev1.ResourceName(VRAMResourceName): *qty},
			Requests: corev1.ResourceList{corev1.ResourceName(VRAMResourceName): *reqQty},
		},
	}
	if got := containerVRAM(c); got != 8192 {
		t.Errorf("expected Limits to win: got %d, want 8192", got)
	}
}

func TestContainerVRAM_NoVRAMReturnsZero(t *testing.T) {
	c := corev1.Container{Name: "c"}
	if got := containerVRAM(c); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

// ── calculatePodVRAMRequest ───────────────────────────────────────────────────

func TestCalculatePodVRAMRequest_AppContainersSum(t *testing.T) {
	// 两个主容器，VRAM 求和
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				makeContainer("a", 1000),
				makeContainer("b", 2000),
			},
		},
	}
	if got := calculatePodVRAMRequest(pod); got != 3000 {
		t.Errorf("got %d, want 3000", got)
	}
}

func TestCalculatePodVRAMRequest_InitContainersTakeMax(t *testing.T) {
	// Init 容器串行执行，取单个最大值
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				makeContainer("init-a", 5000),
				makeContainer("init-b", 3000),
			},
			// 没有主容器
		},
	}
	if got := calculatePodVRAMRequest(pod); got != 5000 {
		t.Errorf("got %d, want 5000 (max of init containers)", got)
	}
}

func TestCalculatePodVRAMRequest_InitVsApp_InitWins(t *testing.T) {
	// init 最大值 > app 总和，返回 init 的值
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				makeContainer("init", 9000),
			},
			Containers: []corev1.Container{
				makeContainer("app", 4000),
			},
		},
	}
	if got := calculatePodVRAMRequest(pod); got != 9000 {
		t.Errorf("got %d, want 9000 (init wins)", got)
	}
}

func TestCalculatePodVRAMRequest_InitVsApp_AppWins(t *testing.T) {
	// app 总和 > init 最大值，返回 app 总和
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				makeContainer("init", 3000),
			},
			Containers: []corev1.Container{
				makeContainer("app-a", 5000),
				makeContainer("app-b", 5000),
			},
		},
	}
	if got := calculatePodVRAMRequest(pod); got != 10000 {
		t.Errorf("got %d, want 10000 (app sum wins)", got)
	}
}

func TestCalculatePodVRAMRequest_NoVRAMReturnsZero(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "cpu-only"},
			},
		},
	}
	if got := calculatePodVRAMRequest(pod); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

// ── getPodKey ─────────────────────────────────────────────────────────────────

func TestGetPodKey_UsesUIDWhenPresent(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "abc-123",
			Namespace: "default",
			Name:      "my-pod",
		},
	}
	if got := getPodKey(pod); got != "abc-123" {
		t.Errorf("got %q, want %q", got, "abc-123")
	}
}

func TestGetPodKey_FallsBackToNamespaceName(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			// UID 为空
			Namespace: "default",
			Name:      "my-pod",
		},
	}
	if got := getPodKey(pod); got != "default/my-pod" {
		t.Errorf("got %q, want %q", got, "default/my-pod")
	}
}

// ── Filter ────────────────────────────────────────────────────────────────────
// 测 Filter 的核心判断逻辑（foundFitGPU），不依赖 framework.Handle，
// 直接构造 Plugin 和 NodeInfo。

func makePlugin() *Plugin {
	return &Plugin{
		pendingVRAM:    make(map[string]map[string]int64),
		podAllocations: make(map[string]PodAllocationInfo),
	}
}

func TestFilter_PassesWhenVRAMFits(t *testing.T) {
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 0},
	}
	node := makeNodeWithLayout("node-a", layout)
	ni := makeNodeInfo(node)
	pl := makePlugin()
	pod := makePodWithVRAM("uid-1", 10000)

	status := pl.Filter(context.Background(), nil, pod, ni)
	if !status.IsSuccess() {
		t.Errorf("expected success, got: %s", status.Message())
	}
}

func TestFilter_RejectsWhenVRAMInsufficient(t *testing.T) {
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 5000, UsedVRAM: 0},
	}
	node := makeNodeWithLayout("node-a", layout)
	ni := makeNodeInfo(node)
	pl := makePlugin()
	pod := makePodWithVRAM("uid-1", 10000) // 需要 10000，只有 5000

	status := pl.Filter(context.Background(), nil, pod, ni)
	if status.IsSuccess() {
		t.Error("expected Unschedulable, got Success")
	}
	if status.Code() != framework.Unschedulable {
		t.Errorf("expected Unschedulable code, got %v", status.Code())
	}
}

func TestFilter_BypassesNonGPUPod(t *testing.T) {
	// Pod 没有 VRAM 需求，无论节点有没有 GPU 都应该直接通过
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 5000, UsedVRAM: 5000}, // 满了
	}
	node := makeNodeWithLayout("node-a", layout)
	ni := makeNodeInfo(node)
	pl := makePlugin()
	pod := &corev1.Pod{ // 没有 VRAM resource
		ObjectMeta: metav1.ObjectMeta{UID: "uid-cpu", Namespace: "default", Name: "cpu-pod"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	}

	status := pl.Filter(context.Background(), nil, pod, ni)
	if !status.IsSuccess() {
		t.Errorf("non-GPU pod should bypass filter, got: %s", status.Message())
	}
}

func TestFilter_RejectsWhenAnnotationMissing(t *testing.T) {
	// 节点没有 gpu-layout annotation，返回 UnschedulableAndUnresolvable
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-no-layout"},
	}
	ni := makeNodeInfo(node)
	pl := makePlugin()
	pod := makePodWithVRAM("uid-1", 4096)

	status := pl.Filter(context.Background(), nil, pod, ni)
	if status.Code() != framework.UnschedulableAndUnresolvable {
		t.Errorf("missing annotation should return UnschedulableAndUnresolvable, got %v", status.Code())
	}
}

func TestFilter_AccountsForPendingVRAM(t *testing.T) {
	// GPU 有 20000 总量，pendingVRAM 已记录 15000，
	// Pod 需要 8000，剩余 5000 不够 → Unschedulable
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 0},
	}
	node := makeNodeWithLayout("node-a", layout)
	ni := makeNodeInfo(node)
	pl := makePlugin()

	// 模拟已经有其他 Pod 预留了 15000
	pl.pendingVRAM["node-a"] = map[string]int64{"gpu-0": 15000}

	pod := makePodWithVRAM("uid-1", 8000)

	status := pl.Filter(context.Background(), nil, pod, ni)
	if status.IsSuccess() {
		t.Error("should be Unschedulable: 20000 - 15000 = 5000 < 8000")
	}
}

func TestFilter_MultiGPU_FindsBestCard(t *testing.T) {
	// 两张卡，只有第二张够用
	layout := NodeGPULayout{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 5000, UsedVRAM: 0},  // 不够
		{ID: 1, UUID: "gpu-1", TotalVRAM: 20000, UsedVRAM: 0}, // 够
	}
	node := makeNodeWithLayout("node-a", layout)
	ni := makeNodeInfo(node)
	pl := makePlugin()
	pod := makePodWithVRAM("uid-1", 10000)

	status := pl.Filter(context.Background(), nil, pod, ni)
	if !status.IsSuccess() {
		t.Errorf("second GPU should be enough: %s", status.Message())
	}
}

func TestFilter_NilNodeInfo(t *testing.T) {
	pl := makePlugin()
	pod := makePodWithVRAM("uid-1", 4096)

	// nodeInfo 为 nil 时应该直接 Success，不 panic
	status := pl.Filter(context.Background(), nil, pod, nil)
	if !status.IsSuccess() {
		t.Errorf("nil nodeInfo should succeed (fail-safe), got: %s", status.Message())
	}
}
