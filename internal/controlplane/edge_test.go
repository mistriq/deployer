package controlplane

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mistriq/deployer/internal/runtimeengine"
)

type edgeRuntime struct {
	fakeRuntime
	asked []string
	edge  runtimeengine.Edge
	err   error
}

func (r *edgeRuntime) Edge(_ context.Context, id string) (runtimeengine.Edge, error) {
	r.asked = append(r.asked, id)
	return r.edge, r.err
}

func TestProjectEdge(t *testing.T) {
	s := testService(t)
	rt := &edgeRuntime{edge: runtimeengine.Edge{RouteStatus: "active", Hosts: []runtimeengine.EdgeHost{{Hostname: "site.yo.example.com", Primary: true, CertStatus: "issuing"}}}}
	s.Runtime = rt
	requireStatus(t, request(t, s, "GET", "/internal/v1/projects/demo/edge", "token-a", nil), 404)
	setupProject(t, s)
	requireStatus(t, request(t, s, "GET", "/internal/v1/projects/demo/edge", "token-b", nil), 404)
	w := request(t, s, "GET", "/internal/v1/projects/demo/edge", "token-a", nil)
	var body struct {
		Supported bool               `json:"supported"`
		Edge      runtimeengine.Edge `json:"edge"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || !body.Supported || body.Edge.Hosts[0].CertStatus != "issuing" || rt.asked[0] != runtimeProject("tenant-a", "demo") {
		t.Fatalf("edge: %d %s %v", w.Code, w.Body.String(), rt.asked)
	}
	rt.err = &runtimeengine.Error{Code: "EDGE_UNAVAILABLE", HTTPStatus: 502}
	if w := request(t, s, "GET", "/internal/v1/projects/demo/edge", "token-a", nil); w.Code != 502 || !strings.Contains(w.Body.String(), "EDGE_UNAVAILABLE") {
		t.Fatalf("runtime error: %d %s", w.Code, w.Body.String())
	}
	s.Runtime = &fakeRuntime{}
	if w := request(t, s, "GET", "/internal/v1/projects/demo/edge", "token-a", nil); w.Code != 200 || !strings.Contains(w.Body.String(), `"supported":false`) {
		t.Fatalf("unsupported runtime: %d %s", w.Code, w.Body.String())
	}
}
