package main
import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)
const (
	GPULayoutAnnotation = "custom.com/gpu-layout"
	scrapeInterval      = 5 * time.Second
	minDeltaMiB         = 64
)
type gpuState struct {
	ID        int    `json:"id"`
	UUID      string `json:"uuid"`
	TotalVRAM int64  `json:"totalVRAM"`
	UsedVRAM  int64  `json:"usedVRAM"`
}
type jsonPatchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}
type Exporter struct {
	clientset   *kubernetes.Clientset
	nodeName    string
	lastUsedMiB map[string]int64
}
func main() {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		klog.Fatalf("NODE_NAME environment variable must be set (inject via Downward API: spec.nodeName)")
	}
	var config *rest.Config
	var err error
	config, err = rest.InClusterConfig()
	if err != nil {
		homeDir, _ := os.UserHomeDir()
		kubeconfigPath := filepath.Join(homeDir, ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			klog.Fatalf("failed to load kubeconfig: %v", err)
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("failed to create clientset: %v", err)
	}
	exp := &Exporter{
		clientset:   clientset,
		nodeName:    nodeName,
		lastUsedMiB: make(map[string]int64),
	}
	klog.Infof("starting GPU usage exporter for node %s, scrape interval %s", nodeName, scrapeInterval)
	ticker := time.NewTicker(scrapeInterval)
	defer ticker.Stop()
	exp.scrapeAndPatch(context.Background())
	for range ticker.C {
		exp.scrapeAndPatch(context.Background())
	}
}
func (e *Exporter) scrapeAndPatch(ctx context.Context) {
	usage, err := scrapeNvidiaSMI()
	if err != nil {
		klog.Errorf("failed to scrape nvidia-smi: %v", err)
		return
	}
	if len(usage) == 0 {
		klog.Warningf("nvidia-smi returned no GPUs on node %s", e.nodeName)
		return
	}
	node, err := e.clientset.CoreV1().Nodes().Get(ctx, e.nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("failed to get node %s: %v", e.nodeName, err)
		return
	}
	layoutJSON, exists := node.Annotations[GPULayoutAnnotation]
	if !exists || layoutJSON == "" {
		klog.Warningf("node %s has no %s annotation yet; skipping until static layout exists", e.nodeName, GPULayoutAnnotation)
		return
	}
	var gpus []gpuState
	if err := json.Unmarshal([]byte(layoutJSON), &gpus); err != nil {
		klog.Errorf("node %s has malformed %s annotation: %v", e.nodeName, GPULayoutAnnotation, err)
		return
	}
	changed := false
	for i := range gpus {
		usedMiB, ok := usage[gpus[i].UUID]
		if !ok {
			continue
		}
		usedBytes := usedMiB * 1024 * 1024
		last, hasLast := e.lastUsedMiB[gpus[i].UUID]
		if hasLast && abs64(usedMiB-last) < minDeltaMiB {
			continue
		}
		gpus[i].UsedVRAM = usedBytes
		e.lastUsedMiB[gpus[i].UUID] = usedMiB
		changed = true
	}
	if !changed {
		return
	}
	if err := e.patchNode(ctx, gpus); err != nil {
		klog.Errorf("failed to patch node %s: %v", e.nodeName, err)
		return
	}
	klog.Infof("patched node %s GPU usage for %d GPU(s)", e.nodeName, len(gpus))
}
func (e *Exporter) patchNode(ctx context.Context, gpus []gpuState) error {
	newLayout, err := json.Marshal(gpus)
	if err != nil {
		return fmt.Errorf("failed to marshal updated layout: %w", err)
	}
	quotedLayout, err := json.Marshal(string(newLayout))
	if err != nil {
		return fmt.Errorf("failed to quote layout value: %w", err)
	}
	patch := []jsonPatchOp{
		{
			Op:    "replace",
			Path:  "/metadata/annotations/" + jsonPointerEscape(GPULayoutAnnotation),
			Value: quotedLayout,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}
	_, err = e.clientset.CoreV1().Nodes().Patch(
		ctx, e.nodeName, types.JSONPatchType, patchBytes, metav1.PatchOptions{},
	)
	return err
}
func jsonPointerEscape(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}
func scrapeNvidiaSMI() (map[string]int64, error) {
	out, err := exec.Command(
		"nvidia-smi",
		"--query-gpu=uuid,memory.used",
		"--format=csv,noheader,nounits",
	).Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi exec failed: %w", err)
	}
	result := make(map[string]int64)
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			klog.Warningf("unexpected nvidia-smi output line, skipping: %q", line)
			continue
		}
		uuid := strings.TrimSpace(parts[0])
		usedMiB, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			klog.Warningf("failed to parse memory.used for %s: %v", uuid, err)
			continue
		}
		result[uuid] = usedMiB
	}
	return result, nil
}
func abs64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}
