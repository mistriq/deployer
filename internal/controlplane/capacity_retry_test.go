package controlplane

import (
	"bytes"
	"context"
	"testing"

	"github.com/mistriq/deployer/internal/runtimeengine"
)

// Admission rejection creates no Runtime operation. The control plane must keep
// the published artifact and retry the same operation identity when capacity returns.
type capacityAdmissionRuntime struct {
	fakeRuntime
	rejections int
	accepted   int
}

func (r *capacityAdmissionRuntime) Deploy(_ context.Context, req runtimeengine.DeploymentRequest, key string) (runtimeengine.Deployment, error) {
	r.requests = append(r.requests, req)
	r.keys = append(r.keys, key)
	if r.rejections > 0 {
		r.rejections--
		return runtimeengine.Deployment{}, &runtimeengine.Error{Code: "NODE_CAPACITY_EXHAUSTED", HTTPStatus: 503, Retryable: true}
	}
	r.accepted++
	return runtimeengine.Deployment{ID: "runtime-admitted", ProjectID: req.ProjectID}, nil
}

func TestCapacityAdmissionRetriesKeepArtifactAndIdentityAcrossRestart(t *testing.T) {
	s, originalBuilder, _, id := workerFixture(t)
	runtime := &capacityAdmissionRuntime{fakeRuntime: fakeRuntime{active: true}, rejections: 4}
	s.Runtime = runtime
	step(t, s, 3)
	built := s.Store.snapshot().Jobs[id]
	if built.Artifact == nil || built.Stage != "submitting" {
		t.Fatal("fixture did not publish an artifact ready for admission")
	}
	artifact := *built.Artifact
	var restartedBuilder *fakeBuilder
	for attempt := 0; attempt < 4; attempt++ {
		step(t, s, 1)
		job := s.Store.snapshot().Jobs[id]
		if job.ErrorCode != "NODE_CAPACITY_EXHAUSTED" || job.State != "deploying" || job.Stage != "submitting" || job.RuntimeID != "" || job.SiteURL != "" {
			t.Fatalf("admission rejection did not remain safely pending: %+v", job)
		}
		if job.Artifact == nil || *job.Artifact != artifact || job.Commit != built.Commit || job.BuildID != built.BuildID {
			t.Fatal("capacity rejection changed the published artifact or build identity")
		}
		if runtime.accepted != 0 {
			t.Fatal("rejected admission created an operation")
		}
		if attempt == 1 {
			path, config := s.Store.path, s.Config
			if err := s.Store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenStore(path, bytes.Repeat([]byte{7}, 32))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { reopened.Close() })
			restartedBuilder = &fakeBuilder{}
			s = &Service{Store: reopened, Runtime: runtime, Builder: restartedBuilder, Config: config}
			if replay := enqueue(t, s, "deployment-worker"); replay != id {
				t.Fatal("restart lost the original deployment receipt")
			}
		}
	}
	step(t, s, 1)
	accepted := s.Store.snapshot().Jobs[id]
	if accepted.RuntimeID != "runtime-admitted" || accepted.ErrorCode != "" || accepted.State == "online" {
		t.Fatal("accepted submission must clear capacity error and await verification")
	}
	step(t, s, 4)
	job := s.Store.snapshot().Jobs[id]
	if job.State != "online" || job.ReleaseID != "release-1" || job.SiteURL != "https://app.example.com" || job.ErrorCode != "" {
		t.Fatalf("accepted operation did not become verified online: %+v", job)
	}
	if originalBuilder.calls != 1 || restartedBuilder.calls != 0 {
		t.Fatalf("capacity retry rebuilt artifact: before restart=%d after=%d", originalBuilder.calls, restartedBuilder.calls)
	}
	if len(runtime.requests) != 5 || runtime.accepted != 1 || len(s.Store.snapshot().Jobs) != 1 {
		t.Fatalf("expected four rejections and one accepted operation: attempts=%d accepted=%d", len(runtime.requests), runtime.accepted)
	}
	for i, req := range runtime.requests {
		if req != runtime.requests[0] || req.ExternalDeploymentID != id || runtime.keys[i] != id {
			t.Fatalf("admission attempt %d changed payload or idempotency identity", i+1)
		}
		if req.Artifact.Digest != artifact.Digest || req.Artifact.Image+"@"+req.Artifact.Digest != artifact.ImageRef {
			t.Fatalf("admission attempt %d changed immutable artifact", i+1)
		}
	}
}
