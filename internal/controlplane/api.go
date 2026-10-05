package controlplane

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var errConflict = errors.New("REQUEST_CONFLICT")
var errMissing = errors.New("NOT_FOUND")
var errInvalid = errors.New("INVALID_REQUEST")

func id() string { b := make([]byte, 16); _, _ = rand.Read(b); return "dep_" + hex.EncodeToString(b) }
func runtimeProject(tenant, project string) string {
	b := sha256.Sum256([]byte(tenant + "\x00" + project))
	return "prj_" + hex.EncodeToString(b[:16])
}
func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func apiError(w http.ResponseWriter, status int, code string) {
	reply(w, status, map[string]string{"error_code": code, "message": "Požadavek nelze dokončit."})
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	tenant := ""
	if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		for k, t := range s.Config.Tokens {
			if subtle.ConstantTimeCompare([]byte(token), []byte(k)) == 1 {
				tenant = t
			}
		}
	}
	if tenant == "" {
		apiError(w, 401, "UNAUTHORIZED")
		return
	}
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(p) < 4 || p[0] != "internal" || p[1] != "v1" || !identifier.MatchString(p[3]) {
		apiError(w, 404, "NOT_FOUND")
		return
	}
	if p[2] == "deployments" && r.Method == http.MethodGet {
		d := s.Store.snapshot()
		j := d.Jobs[p[3]]
		if j == nil || j.Tenant != tenant {
			apiError(w, 404, "NOT_FOUND")
			return
		}
		if len(p) == 4 {
			reply(w, 200, j.public())
			return
		}
		if len(p) == 5 && p[4] == "logs" {
			s.logs(w, r, j)
			return
		}
	}
	if p[2] != "projects" || len(p) > 5 {
		apiError(w, 404, "NOT_FOUND")
		return
	}
	if r.Method != "PUT" && r.Method != "POST" {
		apiError(w, 405, "METHOD_NOT_ALLOWED")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256<<10))
	if err != nil {
		apiError(w, 413, "REQUEST_TOO_LARGE")
		return
	}
	var payload struct {
		RequestID string `json:"request_id"`
		ProjectSpec
		Env       map[string]string `json:"env"`
		ReleaseID string            `json:"release_id"`
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&payload); err != nil || !identifier.MatchString(payload.RequestID) {
		apiError(w, 400, "INVALID_REQUEST")
		return
	}
	if dec.Decode(new(any)) != io.EOF {
		apiError(w, 400, "INVALID_REQUEST")
		return
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	key := tenant + "|" + r.Method + "|" + r.URL.Path + "|" + payload.RequestID
	var response json.RawMessage
	status := 200
	err = s.Store.update(func(d *database) error {
		if old, ok := d.Requests[key]; ok {
			if old.Hash != hash {
				return errConflict
			}
			response = old.Response
			if r.Method == "POST" {
				status = 202
			}
			return nil
		}
		project := d.Projects[p[3]]
		if project != nil && project.Tenant != tenant {
			return errMissing
		}
		switch {
		case len(p) == 4 && r.Method == "PUT":
			u, e := url.Parse(payload.Repository)
			remote := e == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
			if !(remote || localSource(s.Config.LocalSourceRoot, payload.Repository)) || payload.Ref == "" || strings.HasPrefix(payload.Ref, "-") {
				return errInvalid
			}
			if payload.Manifest.Validate() != nil || payload.Build.Kind != payload.Manifest.Kind {
				return errInvalid
			}
			if payload.Build.Dockerfile == "" {
				expectedPort := payload.Build.Port
				if payload.Build.Kind == "static" {
					expectedPort = 8080
				} else if expectedPort == 0 {
					expectedPort = 3000
				}
				if payload.Manifest.Runtime.Port != expectedPort {
					return errInvalid
				}
			}
			target := payload.RuntimeTargetID
			if target == "" {
				if project != nil {
					target = stableTarget(project.Spec.RuntimeTargetID)
				} else {
					target = stableTarget(s.Config.DefaultRuntimeTarget)
				}
			}
			if !s.knownTarget(target) {
				return errTargetUnknown
			}
			if project != nil && target != stableTarget(project.Spec.RuntimeTargetID) {
				for _, job := range d.Jobs {
					if job.ProjectID == project.ID && job.Tenant == tenant {
						return errTargetLocked
					}
				}
			}
			payload.RuntimeTargetID = target
			if project == nil {
				project = &Project{ID: p[3], Tenant: tenant, Env: map[string]string{}}
				d.Projects[p[3]] = project
			}
			project.Spec = payload.ProjectSpec
			project.Revision++
			response = raw(map[string]any{"project_id": project.ID, "revision": project.Revision, "runtime_target_id": target, "request_id": payload.RequestID})
		case len(p) == 5 && p[4] == "env" && r.Method == "PUT":
			if project == nil {
				return errMissing
			}
			if payload.Env == nil || len(payload.Env) > 100 {
				return errInvalid
			}
			names := []string{}
			for k, v := range payload.Env {
				if !envName.MatchString(k) || len(v) > 65536 || strings.ContainsRune(v, 0) {
					return errInvalid
				}
				names = append(names, k)
			}
			sort.Strings(names)
			project.Env = payload.Env
			project.EnvRevision++
			response = raw(map[string]any{"project_id": project.ID, "revision": project.EnvRevision, "names": names, "request_id": payload.RequestID})
		case len(p) == 5 && (p[4] == "deployments" || p[4] == "rollback") && r.Method == "POST":
			if project == nil {
				return errMissing
			}
			if p[4] == "deployments" && !envMatches(*project) {
				return errInvalid
			}
			now := time.Now().UTC()
			jobID := id()
			j := &Job{ID: jobID, Tenant: tenant, ProjectID: project.ID, RequestID: payload.RequestID, State: "queued", Stage: "queued", Message: "Čeká na sestavení.", CreatedAt: now, UpdatedAt: now, BuildID: jobID, Snapshot: *project}
			if p[4] == "rollback" {
				if payload.ReleaseID == "" {
					return errInvalid
				}
				var target *Job
				for _, candidate := range d.Jobs {
					if candidate.Tenant == tenant && candidate.ProjectID == project.ID && candidate.ReleaseID == payload.ReleaseID && stableTarget(candidate.Snapshot.Spec.RuntimeTargetID) == stableTarget(project.Spec.RuntimeTargetID) && candidate.State == "online" && candidate.Artifact != nil {
						target = candidate
						break
					}
				}
				if target == nil {
					return errMissing
				}
				j.Snapshot = target.Snapshot
				j.Artifact = target.Artifact
				j.Commit = target.Commit
				j.RollbackOf = target.ID
				j.Stage = "runtime_sync"
				j.Message = "Čeká na obnovení ověřeného release."
			}
			j.Snapshot.Spec.RuntimeTargetID = stableTarget(j.Snapshot.Spec.RuntimeTargetID)
			if !s.knownTarget(j.Snapshot.Spec.RuntimeTargetID) {
				return errTargetUnknown
			}
			d.Jobs[jobID] = j
			response = raw(map[string]any{"deployment_id": jobID, "runtime_target_id": j.Snapshot.Spec.RuntimeTargetID, "request_id": payload.RequestID})
			status = 202
		default:
			return errMissing
		}
		d.Requests[key] = Receipt{hash, response}
		return nil
	})
	if err != nil {
		switch err {
		case errMissing:
			apiError(w, 404, "NOT_FOUND")
		case errConflict:
			apiError(w, 409, "REQUEST_CONFLICT")
		case errTargetUnknown:
			apiError(w, 400, "RUNTIME_TARGET_UNKNOWN")
		case errTargetLocked:
			apiError(w, 409, "RUNTIME_TARGET_LOCKED")
		case errInvalid:
			apiError(w, 400, "INVALID_REQUEST")
		default:
			apiError(w, 503, "STORE_UNAVAILABLE")
		}
		return
	}
	reply(w, status, response)
}
func envMatches(p Project) bool {
	if len(p.Env) != len(p.Spec.Manifest.Runtime.EnvNames) {
		return false
	}
	for _, n := range p.Spec.Manifest.Runtime.EnvNames {
		if _, ok := p.Env[n]; !ok {
			return false
		}
	}
	return true
}
func (s *Service) logs(w http.ResponseWriter, r *http.Request, j *Job) {
	source := r.URL.Query().Get("source")
	if source != "build" && source != "runtime" {
		apiError(w, 400, "INVALID_SOURCE")
		return
	}
	cursor := int64(0)
	if v := r.URL.Query().Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			apiError(w, 400, "INVALID_CURSOR")
			return
		}
		cursor = n
	}
	if source == "runtime" && j.RuntimeID != "" {
		if err := s.collectLogs(r.Context(), j); err != nil {
			apiError(w, 502, "RUNTIME_LOGS_UNAVAILABLE")
			return
		}
	}
	lines := s.Store.snapshot().Logs[j.ID+"|"+source]
	out := []LogLine{}
	next := cursor
	gap := len(lines) > 0 && cursor < lines[0].Cursor-1
	for _, l := range lines {
		if l.Cursor > cursor {
			l.Text = s.redact(l.Text)
			out = append(out, l)
			next = l.Cursor
			if len(out) >= 200 {
				break
			}
		}
	}
	reply(w, 200, map[string]any{"items": out, "next_cursor": strconv.FormatInt(next, 10), "truncated": gap, "source": source})
}

// localSource accepts only a clean absolute path to a direct child of root, e.g. root/prj_x.git.
func localSource(root, repository string) bool {
	if root == "" || !filepath.IsAbs(repository) || filepath.Clean(repository) != repository {
		return false
	}
	return filepath.Dir(repository) == root && strings.HasSuffix(repository, ".git") && !strings.HasPrefix(filepath.Base(repository), ".")
}
