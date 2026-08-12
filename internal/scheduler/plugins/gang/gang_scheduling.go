package gang

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	PluginName     = "GangScheduling"
	JobNameLabel   = "gpu-job-name"
	MinMemberLabel = "gpu-job-min-member"
)

var (
	queueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_gang_queue_depth",
			Help: "Current number of gang jobs waiting in permit stage.",
		},
		[]string{"priority"},
	)
	preemptionAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_gang_preemption_attempts_total",
			Help: "Total number of gang preemption or deadlock eviction attempts.",
		},
		[]string{"reason", "result"},
	)
	schedulingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scheduler_gang_scheduling_duration_seconds",
			Help:    "Scheduling latency in seconds for gang jobs.",
			Buckets: []float64{0.1, 0.5, 1.0, 5.0, 10.0, 30.0},
		},
		[]string{"result"},
	)
)

type gangState struct {
	minMember     int
	pods          map[types.UID]struct{}
	waiting       []types.UID
	epoch         int64
	ready         bool
	resolved      bool
	firstWaitTime time.Time
}

type GangScheduling struct {
	handle        framework.Handle
	mu            sync.Mutex
	state         map[string]*gangState
	// 顺应 v1.32.3 规范：移除方括号，使用标准的 DelayingInterface
	deadlockQueue workqueue.DelayingInterface
}

var _ framework.PreFilterPlugin = &GangScheduling{}
var _ framework.PermitPlugin = &GangScheduling{}
var _ framework.ReservePlugin = &GangScheduling{}
var _ framework.PostFilterPlugin = &GangScheduling{}
var _ framework.QueueSortPlugin = &GangScheduling{}

func init() {
	prometheus.MustRegister(queueDepth)
	prometheus.MustRegister(preemptionAttempts)
	prometheus.MustRegister(schedulingLatency)
}

func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	gs := &GangScheduling{
		handle:        h,
		state:         make(map[string]*gangState),
		// 顺应 v1.32.3 规范：使用原生的初始化函数
		deadlockQueue: workqueue.NewDelayingQueue(),
	}
	gs.StartDeadlockWorker(ctx)
	return gs, nil
}

func (gs *GangScheduling) Name() string {
	return PluginName
}

func getJob(pod *v1.Pod) string {
	return pod.Labels[JobNameLabel]
}

func getMinMember(pod *v1.Pod) (int, error) {
	return strconv.Atoi(pod.Labels[MinMemberLabel])
}

func (gs *GangScheduling) Less(podInfo1, podInfo2 *framework.QueuedPodInfo) bool {
	p1 := podInfo1.Pod
	p2 := podInfo2.Pod
	p1Priority := int32(0)
	p2Priority := int32(0)
	if p1.Spec.Priority != nil {
		p1Priority = *p1.Spec.Priority
	}
	if p2.Spec.Priority != nil {
		p2Priority = *p2.Spec.Priority
	}
	if p1Priority != p2Priority {
		return p1Priority > p2Priority
	}
	return p1.CreationTimestamp.Before(&p2.CreationTimestamp)
}

func (gs *GangScheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	job := getJob(pod)
	if job == "" {
		return nil, framework.NewStatus(framework.Success, "")
	}
	minMember, err := getMinMember(pod)
	if err != nil || minMember <= 0 {
		return nil, framework.NewStatus(framework.Error, "invalid min-member")
	}
	gs.mu.Lock()
	g, ok := gs.state[job]
	if !ok {
		gs.state[job] = &gangState{
			minMember: minMember,
			pods:      make(map[types.UID]struct{}),
			waiting:   make([]types.UID, 0),
			epoch:     time.Now().UnixNano(),
		}
	} else if g.resolved && !g.ready {
		g.resolved = false
		g.waiting = make([]types.UID, 0)
		g.pods = make(map[types.UID]struct{})
		g.minMember = minMember
		g.firstWaitTime = time.Time{}
	}
	gs.mu.Unlock()
	return nil, framework.NewStatus(framework.Success, "")
}

func (gs *GangScheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (gs *GangScheduling) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (gs *GangScheduling) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	job := getJob(pod)
	if job == "" {
		return
	}
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if g, ok := gs.state[job]; ok {
		delete(g.pods, pod.UID)
	}
}

func (gs *GangScheduling) Permit(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	job := getJob(pod)
	if job == "" {
		return framework.NewStatus(framework.Success, ""), 0
	}
	gs.mu.Lock()
	g, ok := gs.state[job]
	if !ok {
		gs.mu.Unlock()
		return framework.NewStatus(framework.Error, "gang not found"), 0
	}
	if g.ready {
		gs.mu.Unlock()
		return framework.NewStatus(framework.Success, ""), 0
	}
	g.pods[pod.UID] = struct{}{}
	n := len(g.pods)
	klog.Infof("[%s] job=%s current progress: %d/%d", PluginName, job, n, g.minMember)
	priorityStr := "0"
	if pod.Spec.Priority != nil {
		priorityStr = strconv.Itoa(int(*pod.Spec.Priority))
	}
	if n >= g.minMember {
		g.ready = true
		waiting := append([]types.UID(nil), g.waiting...)
		g.waiting = make([]types.UID, 0)
		queueDepth.WithLabelValues(priorityStr).Dec()
		duration := time.Since(g.firstWaitTime).Seconds()
		schedulingLatency.WithLabelValues("success").Observe(duration)
		gs.mu.Unlock()
		for _, uid := range waiting {
			if wp := gs.handle.GetWaitingPod(uid); wp != nil {
				klog.Infof("[%s] Allowing backlogged gang member: %s", PluginName, uid)
				wp.Allow(gs.Name())
			}
		}
		return framework.NewStatus(framework.Success, ""), 0
	}
	if len(g.waiting) == 0 {
		g.firstWaitTime = time.Now()
		queueDepth.WithLabelValues(priorityStr).Inc()
		gs.deadlockQueue.AddAfter(job, 10*time.Second)
	}
	g.waiting = append(g.waiting, pod.UID)
	epoch := g.epoch
	gs.mu.Unlock()
	go gs.timeoutGang(job, epoch)
	return framework.NewStatus(framework.Wait, "waiting gang to assemble"), 30 * time.Second
}

func (gs *GangScheduling) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	job := getJob(pod)
	if job == "" {
		return nil, framework.NewStatus(framework.Unschedulable, "not a gang-scheduled job")
	}
	gs.mu.Lock()
	g, ok := gs.state[job]
	if !ok || g.ready || g.resolved {
		gs.mu.Unlock()
		return nil, framework.NewStatus(framework.Unschedulable, "gang job status stable")
	}
	g.resolved = true
	waiting := append([]types.UID(nil), g.waiting...)
	g.waiting = nil
	g.pods = make(map[types.UID]struct{})
	g.epoch = time.Now().UnixNano()
	priorityStr := "0"
	if pod.Spec.Priority != nil {
		priorityStr = strconv.Itoa(int(*pod.Spec.Priority))
	}
	queueDepth.WithLabelValues(priorityStr).Dec()
	latency := time.Since(g.firstWaitTime).Seconds()
	gs.mu.Unlock()
	schedulingLatency.WithLabelValues("unschedulable").Observe(latency)
	klog.Infof("[%s] job=%s pod=%s failed, clearing %d members", PluginName, job, pod.Name, len(waiting))
	for _, uid := range waiting {
		if uid == pod.UID {
			continue
		}
		if wp := gs.handle.GetWaitingPod(uid); wp != nil {
			wp.Reject(gs.Name(), fmt.Sprintf("gang member %s failed at postfilter", pod.Name))
		}
	}
	return &framework.PostFilterResult{}, framework.NewStatus(framework.Unschedulable, "gang fragmented")
}

func (gs *GangScheduling) timeoutGang(job string, epoch int64) {
	time.Sleep(30 * time.Second)
	gs.mu.Lock()
	g, ok := gs.state[job]
	if !ok || g.ready || g.resolved || g.epoch != epoch || len(g.waiting) == 0 {
		gs.mu.Unlock()
		return
	}
	g.resolved = true
	waiting := append([]types.UID(nil), g.waiting...)
	g.waiting = nil
	g.pods = make(map[types.UID]struct{})
	g.epoch = time.Now().UnixNano()
	priorityStr := "0"
	if wp := gs.handle.GetWaitingPod(waiting[0]); wp != nil && wp.GetPod() != nil && wp.GetPod().Spec.Priority != nil {
		priorityStr = strconv.Itoa(int(*wp.GetPod().Spec.Priority))
	}
	queueDepth.WithLabelValues(priorityStr).Dec()
	latency := time.Since(g.firstWaitTime).Seconds()
	gs.mu.Unlock()
	preemptionAttempts.WithLabelValues("timeout", "success").Inc()
	schedulingLatency.WithLabelValues("timeout").Observe(latency)
	klog.Warningf("[%s] Timeout fired for job %s! Cleared %d pod(s) out.", PluginName, job, len(waiting))
	for _, uid := range waiting {
		if wp := gs.handle.GetWaitingPod(uid); wp != nil {
			wp.Reject(gs.Name(), "gang scheduling timeout assembly failed")
		}
	}
}

func (gs *GangScheduling) StartDeadlockWorker(ctx context.Context) {
	go func() {
		for {
			item, shutdown := gs.deadlockQueue.Get()
			if shutdown {
				return
			}
			// 因为底层返回的是 interface{}，我们通过类型断言转回 string 即可
			if job, ok := item.(string); ok {
				gs.reconcileDeadlock(job)
			}
			gs.deadlockQueue.Done(item)
		}
	}()
	go func() {
		<-ctx.Done()
		gs.deadlockQueue.ShutDown()
	}()
}

func (gs *GangScheduling) reconcileDeadlock(job string) {
	gs.mu.Lock()
	if _, exists := gs.state[job]; !exists {
		gs.mu.Unlock()
		return
	}
	var victimJob string
	var lowestPriority int32 = int32(^uint32(0) >> 1)
	var maxWaitDuration time.Duration
	waitingJobsCount := 0
	for name, state := range gs.state {
		if state.ready || state.resolved || len(state.waiting) == 0 {
			continue
		}
		waitingJobsCount++
		waitDuration := time.Since(state.firstWaitTime)
		var currentJobPriority int32 = 0
		if wp := gs.handle.GetWaitingPod(state.waiting[0]); wp != nil && wp.GetPod() != nil && wp.GetPod().Spec.Priority != nil {
			currentJobPriority = *wp.GetPod().Spec.Priority
		}
		if currentJobPriority < lowestPriority {
			lowestPriority = currentJobPriority
			maxWaitDuration = waitDuration
			victimJob = name
		} else if currentJobPriority == lowestPriority && waitDuration > maxWaitDuration {
			maxWaitDuration = waitDuration
			victimJob = name
		}
	}
	if waitingJobsCount > 1 && maxWaitDuration > 10*time.Second {
		klog.Warningf("[%s] Deadlock detected! %d jobs are interlocking. Executing global priority-based eviction on victim: %s", PluginName, waitingJobsCount, victimJob)
		victimG := gs.state[victimJob]
		victimG.resolved = true
		victims := append([]types.UID(nil), victimG.waiting...)
		victimG.waiting = nil
		victimG.pods = make(map[types.UID]struct{})
		victimG.epoch = time.Now().UnixNano()
		priorityStr := strconv.Itoa(int(lowestPriority))
		queueDepth.WithLabelValues(priorityStr).Dec()
		preemptionAttempts.WithLabelValues("deadlock_eviction", "success").Inc()
		latency := time.Since(victimG.firstWaitTime).Seconds()
		gs.mu.Unlock()
		schedulingLatency.WithLabelValues("deadlock").Observe(latency)
		for _, uid := range victims {
			if wp := gs.handle.GetWaitingPod(uid); wp != nil {
				klog.Infof("[%s] Deadlock Detector: Rejecting pod %s", PluginName, uid)
				wp.Reject(gs.Name(), "evicted to break gang scheduling deadlock interleaving")
			}
		}
		gs.mu.Lock()
		if job != victimJob {
			if currentG, exists := gs.state[job]; exists && !currentG.ready && !currentG.resolved && len(currentG.waiting) > 0 {
				gs.deadlockQueue.AddAfter(job, 5*time.Second)
			}
		}
		gs.mu.Unlock()
		return
	}
	if currentG, exists := gs.state[job]; exists && !currentG.ready && !currentG.resolved && len(currentG.waiting) > 0 {
		gs.deadlockQueue.AddAfter(job, 5*time.Second)
	}
	gs.mu.Unlock()
}
