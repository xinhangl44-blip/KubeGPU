package main

import (
	"encoding/json"
	"testing"
)

// ── jsonPointerEscape ────────────────────────────────────────────────────────

func TestJsonPointerEscape_Slash(t *testing.T) {
	got := jsonPointerEscape("custom.com/gpu-layout")
	want := "custom.com~1gpu-layout"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJsonPointerEscape_Tilde(t *testing.T) {
	got := jsonPointerEscape("a~b")
	want := "a~0b"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJsonPointerEscape_TildeAndSlash_OrderMatters(t *testing.T) {
	// "a/b~c" -> 先 ~ 变 ~0，再 / 变 ~1 -> "a~1b~0c"
	got := jsonPointerEscape("a/b~c")
	want := "a~1b~0c"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJsonPointerEscape_NoSpecialChars(t *testing.T) {
	got := jsonPointerEscape("scheduling.x-k8s.io")
	want := "scheduling.x-k8s.io"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ── abs64 ────────────────────────────────────────────────────────────────────

func TestAbs64_Positive(t *testing.T) {
	if got := abs64(42); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestAbs64_Negative(t *testing.T) {
	if got := abs64(-42); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestAbs64_Zero(t *testing.T) {
	if got := abs64(0); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

// ── delta 抑制逻辑 ─────────────────────────────────────────────────────────
// 你的 scrapeAndPatch 里这段逻辑：
//
//   last, hasLast := e.lastUsedMiB[gpus[i].UUID]
//   if hasLast && abs64(usedMiB-last) < minDeltaMiB {
//       continue
//   }
//   gpus[i].UsedVRAM = usedBytes
//   e.lastUsedMiB[gpus[i].UUID] = usedMiB
//   changed = true
//
// 这段逻辑和 K8s API、nvidia-smi 都没有关系，
// 直接复现到这个辅助函数里测，不改你的 main.go。

func applyUsageToLayout(gpus []gpuState, usage map[string]int64, lastUsedMiB map[string]int64) ([]gpuState, bool) {
	changed := false
	for i := range gpus {
		usedMiB, ok := usage[gpus[i].UUID]
		if !ok {
			continue
		}
		usedBytes := usedMiB * 1024 * 1024
		last, hasLast := lastUsedMiB[gpus[i].UUID]
		if hasLast && abs64(usedMiB-last) < minDeltaMiB {
			continue
		}
		gpus[i].UsedVRAM = usedBytes
		lastUsedMiB[gpus[i].UUID] = usedMiB
		changed = true
	}
	return gpus, changed
}

func TestDeltaSuppression_FirstTime_AlwaysUpdates(t *testing.T) {
	// hasLast = false，无论变化多小都要更新
	gpus := []gpuState{{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 0}}
	usage := map[string]int64{"gpu-0": 10}
	last := map[string]int64{}

	result, changed := applyUsageToLayout(gpus, usage, last)

	if !changed {
		t.Error("第一次见到 UUID，应该无条件更新，但 changed=false")
	}
	want := int64(10) * 1024 * 1024
	if result[0].UsedVRAM != want {
		t.Errorf("UsedVRAM = %d, want %d", result[0].UsedVRAM, want)
	}
}

func TestDeltaSuppression_BelowThreshold_Skipped(t *testing.T) {
	// 变化量 10 MiB < minDeltaMiB(64)，跳过
	gpus := []gpuState{{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 0}}
	usage := map[string]int64{"gpu-0": 1010}
	last := map[string]int64{"gpu-0": 1000}

	_, changed := applyUsageToLayout(gpus, usage, last)

	if changed {
		t.Errorf("变化量 10 MiB < minDeltaMiB %d，应该跳过，但 changed=true", minDeltaMiB)
	}
}

func TestDeltaSuppression_AboveThreshold_Updates(t *testing.T) {
	// 变化量 100 MiB >= minDeltaMiB(64)，更新
	gpus := []gpuState{{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 0}}
	usage := map[string]int64{"gpu-0": 1100}
	last := map[string]int64{"gpu-0": 1000}

	result, changed := applyUsageToLayout(gpus, usage, last)

	if !changed {
		t.Error("变化量 100 MiB >= minDeltaMiB，应该更新，但 changed=false")
	}
	want := int64(1100) * 1024 * 1024
	if result[0].UsedVRAM != want {
		t.Errorf("UsedVRAM = %d, want %d", result[0].UsedVRAM, want)
	}
}

func TestDeltaSuppression_MissingUUID_Unchanged(t *testing.T) {
	// nvidia-smi 这轮没报告这个 UUID，原值保持不变，不能清零
	gpus := []gpuState{{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 9999}}
	usage := map[string]int64{}
	last := map[string]int64{}

	result, changed := applyUsageToLayout(gpus, usage, last)

	if changed {
		t.Error("UUID 不在本轮 usage 里，不应有变化")
	}
	if result[0].UsedVRAM != 9999 {
		t.Errorf("UsedVRAM 应保持 9999，但得到 %d", result[0].UsedVRAM)
	}
}

func TestDeltaSuppression_ExactThreshold_Skipped(t *testing.T) {
	// 变化量恰好等于 minDeltaMiB(64)，你的条件是 < minDeltaMiB，所以 64 不跳过
	gpus := []gpuState{{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 0}}
	usage := map[string]int64{"gpu-0": 1064}
	last := map[string]int64{"gpu-0": 1000}

	_, changed := applyUsageToLayout(gpus, usage, last)

	// abs64(1064-1000) = 64，不满足 < 64，所以应该更新
	if !changed {
		t.Error("变化量 64 MiB 不满足 < minDeltaMiB，应该更新，但 changed=false")
	}
}

// ── patchNode 的 JSON 序列化 ─────────────────────────────────────────────────

func TestPatchNodeJSON_CorrectFormat(t *testing.T) {
	gpus := []gpuState{
		{ID: 0, UUID: "gpu-0", TotalVRAM: 20000, UsedVRAM: 5000},
	}

	// 复现 patchNode 里的序列化逻辑（不调 K8s API）
	newLayout, err := json.Marshal(gpus)
	if err != nil {
		t.Fatalf("marshal gpus: %v", err)
	}
	quotedLayout, err := json.Marshal(string(newLayout))
	if err != nil {
		t.Fatalf("quote layout: %v", err)
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
		t.Fatalf("marshal patch: %v", err)
	}

	var ops []map[string]interface{}
	if err := json.Unmarshal(patchBytes, &ops); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0]["op"] != "replace" {
		t.Errorf("op = %v, want replace", ops[0]["op"])
	}
	wantPath := "/metadata/annotations/custom.com~1gpu-layout"
	if ops[0]["path"] != wantPath {
		t.Errorf("path = %v, want %v", ops[0]["path"], wantPath)
	}
	// annotation 的 value 必须是 JSON string，不能是裸 JSON 对象
	if _, ok := ops[0]["value"].(string); !ok {
		t.Errorf("value 应该是 string 类型，但得到 %T", ops[0]["value"])
	}
}

func TestGPUStateJSON_RoundTrip(t *testing.T) {
	// gpuState 的 JSON tag 和 vramfit.GPUState 必须一致（两者各自定义，tag 是唯一契约）
	original := gpuState{ID: 1, UUID: "gpu-1", TotalVRAM: 21474836480, UsedVRAM: 1073741824}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(data)
	for _, key := range []string{`"totalVRAM"`, `"usedVRAM"`, `"uuid"`, `"id"`} {
		found := false
		for i := 0; i <= len(s)-len(key); i++ {
			if s[i:i+len(key)] == key {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("JSON 里缺少 key %s，实际: %s", key, s)
		}
	}
	var roundtripped gpuState
	if err := json.Unmarshal(data, &roundtripped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundtripped != original {
		t.Errorf("round-trip 失败: got %+v, want %+v", roundtripped, original)
	}
}
