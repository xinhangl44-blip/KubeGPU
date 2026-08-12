package gang

import (
	"context"
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ==================== Helper ====================

func makePod(name, jobName string, minMember, priority int32) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name),
		},
		Spec: v1.PodSpec{
			Priority: &priority,
		},
	}
	if jobName != "" {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[JobNameLabel] = jobName
		pod.Labels[MinMemberLabel] = strconv.Itoa(int(minMember))
	}
	return pod
}

// ==================== Mocks ====================

type mockWaitingPod struct {
	pod          *v1.Pod
	allowed      bool
	rejected     bool
	rejectMsg    string
	pendingPlugins []string // 满足接口要求
}

func (m *mockWaitingPod) GetPod() *v1.Pod {
	return m.pod
}

func (m *mockWaitingPod) Allow(plugin string) {
	m.allowed = true
}

func (m *mockWaitingPod) Reject(plugin string, msg string) {
	m.rejected = true
	m.rejectMsg = msg
}

// 实现 WaitingPod 接口所需的其他方法（v1.30+ 需要）
func (m *mockWaitingPod) GetPendingPlugins() []string {
	return m.pendingPlugins
}

type mockHandle struct {
	framework.Handle
	waiting map[types.UID]*mockWaitingPod
}

func (m *mockHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	if p, ok := m.waiting[uid]; ok {
		return p
	}
	return nil
}

// ==================== Tests ====================

func TestLess_PriorityFirst(t *testing.T) {
	gs := &GangScheduling{}

	p1 := makePod("p1", "job", 2, 100)
	p2 := makePod("p2", "job", 2, 50)

	pi1, err := framework.NewPodInfo(p1)
	if err != nil {
		t.Fatalf("NewPodInfo failed: %v", err)
	}
	pi2, err := framework.NewPodInfo(p2)
	if err != nil {
		t.Fatalf("NewPodInfo failed: %v", err)
	}

	q1 := &framework.QueuedPodInfo{PodInfo: pi1}
	q2 := &framework.QueuedPodInfo{PodInfo: pi2}

	if !gs.Less(q1, q2) {
		t.Fatal("higher priority should come first")
	}
	if gs.Less(q2, q1) {
		t.Fatal("lower priority should not come first")
	}
}
func TestPreFilter_NonGangPod(t *testing.T) {
	gs := &GangScheduling{state: make(map[string]*gangState)}
	pod := &v1.Pod{}

	_, status := gs.PreFilter(context.Background(), nil, pod)
	if !status.IsSuccess() {
		t.Fatalf("non-gang pod should success, got: %v", status)
	}
}

func TestPreFilter_CreateGangState(t *testing.T) {
	gs := &GangScheduling{state: make(map[string]*gangState)}
	pod := makePod("p1", "job-a", 3, 0)

	_, status := gs.PreFilter(context.Background(), nil, pod)
	if !status.IsSuccess() {
		t.Fatalf("PreFilter failed: %v", status)
	}

	g, ok := gs.state["job-a"]
	if !ok || g.minMember != 3 {
		t.Fatal("gang state not created correctly")
	}
}

func TestPermit_Wait(t *testing.T) {
	h := &mockHandle{waiting: make(map[types.UID]*mockWaitingPod)}
	gs := &GangScheduling{
		handle:        h,
		state:         make(map[string]*gangState),
		deadlockQueue: workqueue.NewDelayingQueue(),
	}

	gs.state["job-a"] = &gangState{
		minMember: 3,
		pods:      make(map[types.UID]struct{}),
	}

	pod := makePod("pod1", "job-a", 3, 0)

	status, timeout := gs.Permit(context.Background(), nil, pod, "node1")

	if status.Code() != framework.Wait {
		t.Fatalf("expected Wait, got %v", status.Code())
	}
	if timeout != 30*time.Second {
		t.Fatal("wrong timeout")
	}
}

func TestPermit_GangReady(t *testing.T) {
	h := &mockHandle{waiting: make(map[types.UID]*mockWaitingPod)}

	wp1 := &mockWaitingPod{pod: makePod("pod1", "job-a", 3, 0)}
	wp2 := &mockWaitingPod{pod: makePod("pod2", "job-a", 3, 0)}
	h.waiting["pod1"] = wp1
	h.waiting["pod2"] = wp2

	gs := &GangScheduling{
		handle:        h,
		deadlockQueue: workqueue.NewDelayingQueue(),
		state: map[string]*gangState{
			"job-a": {
				minMember: 3,
				pods: map[types.UID]struct{}{
					"pod1": {},
					"pod2": {},
				},
				waiting: []types.UID{"pod1", "pod2"},
			},
		},
	}

	pod3 := makePod("pod3", "job-a", 3, 0)
	status, _ := gs.Permit(context.Background(), nil, pod3, "node1")

	if status.Code() != framework.Success {
		t.Fatalf("expected Success, got %v", status.Code())
	}
	if !wp1.allowed || !wp2.allowed {
		t.Fatal("waiting pods not released")
	}
}

func TestPostFilter_RejectWaitingPods(t *testing.T) {
	h := &mockHandle{waiting: make(map[types.UID]*mockWaitingPod)}
	wp := &mockWaitingPod{pod: makePod("pod1", "job-b", 2, 0)}
	h.waiting["pod1"] = wp

	gs := &GangScheduling{
		handle: h,
		state: map[string]*gangState{
			"job-b": {
				minMember: 2,
				waiting:   []types.UID{"pod1"},
				pods:      map[types.UID]struct{}{"pod1": {}},
			},
		},
	}

	pod2 := makePod("pod2", "job-b", 2, 0)
	_, status := gs.PostFilter(context.Background(), nil, pod2, nil)

	if status.Code() != framework.Unschedulable {
		t.Fatalf("expected Unschedulable, got %v", status.Code())
	}
	if !wp.rejected {
		t.Fatal("waiting pod should be rejected")
	}
}
