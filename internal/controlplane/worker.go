package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/mistriq/deployer/internal/ocibuild"
	"github.com/mistriq/deployer/internal/runtimeengine"
)

func (s *Service) Run(ctx context.Context) error {
	delay := s.Config.PollInterval
	if delay <= 0 {
		delay = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := s.Step(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// Step advances one durable stage. Every external mutation can safely be replayed
// after a crash. Submission always uses the local immutable job ID as its key.
func (s *Service) Step(ctx context.Context) error {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	d := s.Store.snapshot()
	// Keep FIFO ordering within a project, but don't let one unhealthy Runtime
	// candidate starve all other projects. UpdatedAt provides durable round-robin.
	oldest := map[string]*Job{}
	for _, j := range d.Jobs {
		if terminal(j.State) {
			continue
		}
		key := j.Tenant + "|" + j.ProjectID
		prev := oldest[key]
		if prev == nil || j.CreatedAt.Before(prev.CreatedAt) || (j.CreatedAt.Equal(prev.CreatedAt) && j.ID < prev.ID) {
			oldest[key] = j
		}
	}
	jobs := []*Job{}
	for _, j := range oldest {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].UpdatedAt.Equal(jobs[j].UpdatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].UpdatedAt.Before(jobs[j].UpdatedAt)
	})
	if len(jobs) == 0 {
		return nil
	}
	j := jobs[0]
	rt, err := s.runtimeFor(j.Snapshot.Spec.RuntimeTargetID)
	if err != nil {
		return s.change(j.ID, func(v *Job) {
			v.ErrorCode = "RUNTIME_TARGET_UNAVAILABLE"
			v.Message = "Cílový Runtime není nakonfigurován; původní cíl zůstává zachován."
		})
	}
	projectID := runtimeProject(j.Tenant, j.ProjectID)
	switch {
	case j.Commit == "" && j.Artifact == nil:
		if err := s.change(j.ID, func(v *Job) { v.State = "building"; v.Stage = "resolving"; v.Message = "Sestavuje se." }); err != nil {
			return err
		}
		sha, err := s.Builder.Resolve(ctx, j.Snapshot.Spec.Repository, j.Snapshot.Spec.Ref)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return s.fail(j.ID, "build_failed", "SOURCE_FAILED")
		}
		return s.change(j.ID, func(v *Job) { v.Commit = sha; v.Stage = "building" })
	case j.Artifact == nil:
		if err := s.change(j.ID, func(v *Job) { v.State = "building"; v.Stage = "building"; v.Message = "Sestavuje se." }); err != nil {
			return err
		}
		// A persistence failure aborts advancement even if the external build completes.
		var storageErr error
		a, err := s.Builder.Build(ctx, ocibuild.Request{Repository: j.Snapshot.Spec.Repository, Commit: j.Commit, ImageRepository: strings.TrimSuffix(s.Config.ImagePrefix, "/") + "/" + projectID, JobID: j.ID, Spec: j.Snapshot.Spec.Build, Credentials: s.Config.Credentials, OnPhase: func(phase string) {
			if phase == "publishing" && storageErr == nil {
				storageErr = s.change(j.ID, func(v *Job) { v.State = "publishing"; v.Stage = "publishing"; v.Message = "Zveřejňuje se." })
			}
		}}, func(line string) {
			if storageErr == nil {
				storageErr = s.appendLog(j.ID, "build", "stdout", line)
			}
		})
		if storageErr != nil {
			return storageErr
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			code := "BUILD_FAILED"
			if s.Store.snapshot().Jobs[j.ID].State == "publishing" {
				code = "REGISTRY_PUBLISH_FAILED"
			}
			var pipeline *ocibuild.PipelineError
			if errors.As(err, &pipeline) {
				code = "BUILD_PUBLISH_FAILED"
			}
			return s.fail(j.ID, "build_failed", code)
		}
		if a.CommitSHA != j.Commit || (runtimeengine.Artifact{Image: strings.Split(a.ImageRef, "@")[0], Digest: a.Digest}).Validate() != nil {
			return s.fail(j.ID, "build_failed", "ARTIFACT_INVALID")
		}
		return s.change(j.ID, func(v *Job) {
			v.Artifact = &a
			v.State = "deploying"
			v.Stage = "runtime_sync"
			v.Message = "Zveřejňuje se."
		})
	case j.Stage == "queued" || j.Stage == "runtime_sync":
		caps, err := rt.Capabilities(ctx)
		if err != nil {
			return s.runtimeError(j, err)
		}
		if err = j.Snapshot.Spec.Manifest.ValidateCapabilities(caps); err != nil {
			return s.fail(j.ID, "deployment_failed", "MANIFEST_INVALID")
		}
		_, err = rt.UpsertProject(ctx, runtimeengine.ProjectRequest{ProjectID: projectID, Slug: strings.TrimPrefix(projectID, "prj_"), Manifest: j.Snapshot.Spec.Manifest})
		if err != nil {
			return s.runtimeError(j, err)
		}
		if err = rt.ReplaceEnvironment(ctx, projectID, j.Snapshot.Env); err != nil {
			return s.runtimeError(j, err)
		}
		return s.change(j.ID, func(v *Job) { v.State = "deploying"; v.Stage = "submitting"; v.Message = "Zveřejňuje se." })
	case j.RuntimeID == "":
		dep, err := rt.Deploy(ctx, runtimeengine.DeploymentRequest{ProjectID: projectID, ExternalDeploymentID: j.ID, Artifact: runtimeengine.Artifact{Image: strings.Split(j.Artifact.ImageRef, "@")[0], Digest: j.Artifact.Digest}}, j.ID)
		if err != nil {
			return s.runtimeError(j, err)
		}
		if dep.ID == "" {
			return s.fail(j.ID, "deployment_failed", "INVALID_RESPONSE")
		}
		return s.change(j.ID, func(v *Job) {
			v.RuntimeID = dep.ID
			v.State = "deploying"
			v.Stage = "verifying"
			v.VerificationStarted = time.Now().UTC()
			v.ErrorCode = ""
		})
	default:
		v, err := rt.VerifyActive(ctx, projectID, j.RuntimeID)
		if err != nil {
			return s.runtimeError(j, err)
		}
		if err = s.collectLogs(ctx, j); err != nil {
			if ctx.Err() != nil {
				return nil
			} /* Logs are independently retryable; never assert activation from logs. */
		}
		if !v.Active {
			return s.change(j.ID, func(v *Job) {
				v.ErrorCode = ""
				v.Message = "Čeká na ověření zdraví a routy."
				if !v.VerificationStarted.IsZero() && time.Since(v.VerificationStarted) > 15*time.Minute {
					v.ErrorCode = "ACTIVATION_UNCONFIRMED"
					v.Message = "Aktivaci se nedaří ověřit; je nutná kontrola Runtime. Operace dál ověřuje původní ID."
				}
			})
		}
		hostname := v.Route.Hostname
		site := ""
		if u, e := url.Parse("https://" + hostname); e == nil && u.Hostname() != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == "" {
			site = u.String()
		}
		return s.change(j.ID, func(job *Job) {
			job.State = "online"
			job.Stage = "active"
			job.ReleaseID = v.ReleaseID
			job.SiteURL = site
			job.Message = "Online"
			job.ErrorCode = ""
		})
	}
}
func (s *Service) change(id string, fn func(*Job)) error {
	return s.Store.update(func(d *database) error {
		j := d.Jobs[id]
		if j == nil {
			return fmt.Errorf("job missing")
		}
		fn(j)
		j.UpdatedAt = time.Now().UTC()
		return nil
	})
}
func (s *Service) fail(id, state, code string) error {
	return s.change(id, func(j *Job) {
		j.State = state
		j.Stage = "failed"
		j.ErrorCode = code
		j.Message = "Nasazení se nezdařilo."
		j.SiteURL = ""
	})
}
func (s *Service) runtimeError(j *Job, err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	var remote *runtimeengine.Error
	if errors.As(err, &remote) {
		if j.RuntimeID == "" && j.Stage == "submitting" && remote.Code == "INVALID_RESPONSE" {
			return s.change(j.ID, func(v *Job) {
				v.ErrorCode = "SUBMISSION_UNCERTAIN"
				v.Message = "Ověřuje se přijetí požadavku; opakování používá stejné ID."
			})
		}
		if !remote.Retryable {
			if remote.Code == "DEPLOY_CANCELLED" {
				return s.fail(j.ID, "cancelled", remote.Code)
			}
			return s.fail(j.ID, "deployment_failed", remote.Code)
		}
		return s.change(j.ID, func(v *Job) {
			v.ErrorCode = remote.Code
			v.Message = "Runtime je dočasně nedostupný; požadavek bude zopakován."
		})
	}
	return s.change(j.ID, func(v *Job) {
		v.ErrorCode = "RUNTIME_UNAVAILABLE"
		v.Message = "Runtime je dočasně nedostupný; požadavek bude zopakován."
	})
}
