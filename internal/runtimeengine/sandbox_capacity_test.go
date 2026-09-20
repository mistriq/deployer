package runtimeengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

type sandboxCapacity struct {
	CPU    *int64 `json:"cpu_millis"`
	Memory *int64 `json:"memory_mb"`
	Disk   *int64 `json:"disk_mb"`
}

// sandboxCapacityPreflight is deliberately conservative: all six candidates must
// fit even if failed and historical releases retain their complete allocation.
// A successful snapshot is not a reservation; admission still handles races.
func sandboxCapacityPreflight(raw []byte, resources Resources, candidates int) (string, error) {
	var node struct {
		Driver     string          `json:"driver"`
		Proxy      string          `json:"proxy"`
		Total      sandboxCapacity `json:"total"`
		Allocated  sandboxCapacity `json:"allocated"`
		Reserve    sandboxCapacity `json:"reserve"`
		Healthy    *bool           `json:"healthy"`
		Draining   *bool           `json:"draining"`
		GlobalStop *bool           `json:"global_stop"`
	}
	if json.Unmarshal(raw, &node) != nil {
		return "", errors.New("invalid node capacity response")
	}
	if node.Driver != "mock" || node.Proxy != "mock" {
		return "", errors.New("test requires confirmed mock driver and mock proxy")
	}
	if node.Healthy == nil || node.Draining == nil || node.GlobalStop == nil || !*node.Healthy || *node.Draining || *node.GlobalStop {
		return "", errors.New("node is unavailable or readiness fields are missing")
	}
	if candidates <= 0 {
		return "", errors.New("invalid candidate count")
	}
	remaining := []string{}
	for _, dimension := range []struct {
		name                      string
		total, allocated, reserve *int64
		perCandidate              int
	}{
		{"cpu_millis", node.Total.CPU, node.Allocated.CPU, node.Reserve.CPU, resources.CPUMillis},
		{"memory_mb", node.Total.Memory, node.Allocated.Memory, node.Reserve.Memory, resources.MemoryMB},
		{"disk_mb", node.Total.Disk, node.Allocated.Disk, node.Reserve.Disk, resources.DiskMB},
	} {
		if dimension.total == nil || dimension.allocated == nil || dimension.reserve == nil {
			return "", fmt.Errorf("missing %s capacity fields", dimension.name)
		}
		total, allocated, reserve := *dimension.total, *dimension.allocated, *dimension.reserve
		if total <= 0 || allocated < 0 || reserve < 0 || allocated > total || reserve > total-allocated || dimension.perCandidate <= 0 || int64(dimension.perCandidate) > math.MaxInt64/int64(candidates) {
			return "", fmt.Errorf("invalid or exhausted %s capacity", dimension.name)
		}
		available := total - allocated - reserve
		required := int64(dimension.perCandidate) * int64(candidates)
		if available < required {
			return "", fmt.Errorf("insufficient %s: available=%d required=%d for %d candidates (total=%d allocated=%d reserve=%d)", dimension.name, available, required, candidates, total, allocated, reserve)
		}
		remaining = append(remaining, fmt.Sprintf("%s available=%d required=%d", dimension.name, available, required))
	}
	return strings.Join(remaining, ", "), nil
}

func TestSandboxCapacityPreflight(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			"driver": "mock", "proxy": "mock", "healthy": true, "draining": false, "global_stop": false,
			"total":     map[string]any{"cpu_millis": 8000, "memory_mb": 16384, "disk_mb": 204800},
			"allocated": map[string]any{"cpu_millis": 1000, "memory_mb": 1024, "disk_mb": 2048},
			"reserve":   map[string]any{"cpu_millis": 500, "memory_mb": 1024, "disk_mb": 10240},
		}
	}
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
		want bool
	}{
		{"sufficient", func(map[string]any) {}, true},
		{"exact-budget", func(n map[string]any) { n["allocated"].(map[string]any)["cpu_millis"] = 4500 }, true},
		{"observed-exhaustion", func(n map[string]any) { n["allocated"].(map[string]any)["cpu_millis"] = 7500 }, false},
		{"whole-run-not-one-candidate", func(n map[string]any) { n["allocated"].(map[string]any)["cpu_millis"] = 6500 }, false},
		{"memory-exhausted", func(n map[string]any) { n["allocated"].(map[string]any)["memory_mb"] = 15000 }, false},
		{"disk-exhausted", func(n map[string]any) { n["allocated"].(map[string]any)["disk_mb"] = 190000 }, false},
		{"missing-total", func(n map[string]any) { delete(n, "total") }, false},
		{"missing-reserve-field", func(n map[string]any) { delete(n["reserve"].(map[string]any), "disk_mb") }, false},
		{"negative-allocation", func(n map[string]any) { n["allocated"].(map[string]any)["cpu_millis"] = -1 }, false},
		{"noninteger", func(n map[string]any) { n["total"].(map[string]any)["cpu_millis"] = 1.5 }, false},
		{"null-allocation", func(n map[string]any) { n["allocated"] = nil }, false},
		{"draining", func(n map[string]any) { n["draining"] = true }, false},
		{"missing-health", func(n map[string]any) { delete(n, "healthy") }, false},
		{"real-driver", func(n map[string]any) { n["driver"] = "docker" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := base()
			tc.edit(n)
			raw, _ := json.Marshal(n)
			_, err := sandboxCapacityPreflight(raw, manifest().Resources, 6)
			if (err == nil) != tc.want {
				t.Fatalf("want accepted=%v, error=%v", tc.want, err)
			}
		})
	}
	if _, err := sandboxCapacityPreflight([]byte(`{broken`), manifest().Resources, 6); err == nil {
		t.Fatal("malformed response accepted")
	}
}
