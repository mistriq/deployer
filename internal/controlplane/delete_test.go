package controlplane

import (
	"context"
	"testing"

	"github.com/mistriq/deployer/internal/runtimeengine"
)

type removingRuntime struct {
	fakeRuntime
	deleted []string
	err     error
}

func (r *removingRuntime) DeleteProject(_ context.Context, id string) error {
	r.deleted = append(r.deleted, id)
	return r.err
}

func TestDeleteProject(t *testing.T) {
	s := testService(t)
	rt := &removingRuntime{}
	s.Runtime = rt
	setupProject(t, s)
	job := enqueue(t, s, "deploy-1")
	requireStatus(t, request(t, s, "DELETE", "/internal/v1/projects/demo", "token-b", nil), 404)
	requireStatus(t, request(t, s, "DELETE", "/internal/v1/projects/demo", "token-a", nil), 409)
	if err := s.change(job, func(j *Job) { j.State = "online" }); err != nil {
		t.Fatal(err)
	}
	rt.err = &runtimeengine.Error{Code: "EDGE_UNAVAILABLE", HTTPStatus: 502, Retryable: true}
	if w := request(t, s, "DELETE", "/internal/v1/projects/demo", "token-a", nil); w.Code != 502 || s.Store.snapshot().Projects["demo"] == nil {
		t.Fatalf("a Runtime failure must keep the project: %d %s", w.Code, w.Body.String())
	}
	rt.err = nil
	requireStatus(t, request(t, s, "DELETE", "/internal/v1/projects/demo", "token-a", nil), 200)
	d := s.Store.snapshot()
	if d.Projects["demo"] != nil || d.Jobs[job] != nil || rt.deleted[len(rt.deleted)-1] != runtimeProject("tenant-a", "demo") {
		t.Fatalf("project not removed: %+v %v", d.Projects, rt.deleted)
	}
	requireStatus(t, request(t, s, "DELETE", "/internal/v1/projects/demo", "token-a", nil), 200)
	requireStatus(t, request(t, s, "GET", "/internal/v1/deployments/"+job, "token-a", nil), 404)
}
