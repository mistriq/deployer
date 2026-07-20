package app

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func validHostingManifest(kind string) HostingProjectManifest {
	manifest := HostingProjectManifest{
		SchemaVersion: hostingManifestVersion,
		Repository: HostingRepositoryManifest{
			InstallationID: 1001,
			RepositoryID:   2002,
			FullName:       "socials-century/customer-app",
		},
		Runtime: HostingRuntimeManifest{
			Kind:           kind,
			NodeVersion:    "22",
			PackageManager: "npm",
			BuildScript:    "build",
		},
		ResourceProfile: "starter",
	}
	if kind == "static" {
		manifest.Runtime.OutputDirectory = "dist"
	} else {
		manifest.Runtime.StartScript = "start"
		manifest.Runtime.Port = 3000
		manifest.Runtime.HealthPath = "/healthz"
	}
	return manifest
}

func withHostingConfig(t *testing.T) {
	t.Helper()
	old := appConfig
	oldStorage := artifactStorage
	t.Cleanup(func() {
		appConfig = old
		artifactStorage = oldStorage
	})
	root := t.TempDir()
	appConfig.HostingRepoRoot = filepath.Join(root, "repos")
	appConfig.HostingDeployRoot = filepath.Join(root, "apps")
	if artifactStorage == nil {
		appConfig.ArtifactDir = filepath.Join(root, "artifacts")
		appConfig.SnapshotDir = filepath.Join(root, "snapshots")
		configureArtifactStorage(appConfig)
	}
}

func signedManifestRequest(t *testing.T, token, externalProjectID string, manifest HostingProjectManifest, signedAt time.Time) *http.Request {
	t.Helper()
	body, err := json.Marshal(hostingProjectUpsertRequest{Manifest: manifest})
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	timestamp := signedAt.Unix()
	timestampText := itoa(timestamp)
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(timestampText + "."))
	mac.Write(body)
	req := httptest.NewRequest(http.MethodPut, "/api/internal/v1/projects/"+externalProjectID, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(manifestTimestampHeader, timestampText)
	req.Header.Set(manifestSignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return req
}

func TestInternalProjectProvisionCreatesReplaysAndUpdates(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	token, err := createServiceToken("project-writer", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	manifest := validHostingManifest("static")

	req := signedManifestRequest(t, token.Token, "project_01JHOSTING", manifest, time.Now())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create project = %d body=%s", rec.Code, rec.Body.String())
	}
	var created internalProjectResponse
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if !created.Created || created.Replayed || created.ExternalProjectID != "project_01JHOSTING" || !strings.HasPrefix(created.ManifestDigest, "sha256:") {
		t.Fatalf("unexpected create response: %+v", created)
	}

	req = signedManifestRequest(t, token.Token, "project_01JHOSTING", manifest, time.Now())
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("replay project = %d body=%s", rec.Code, rec.Body.String())
	}
	var replay internalProjectResponse
	if err := json.NewDecoder(rec.Body).Decode(&replay); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replay.Created || !replay.Replayed || replay.ExternalProjectID != created.ExternalProjectID || replay.ManifestDigest != created.ManifestDigest {
		t.Fatalf("unexpected replay response: %+v", replay)
	}

	manifest.ResourceProfile = "standard"
	req = signedManifestRequest(t, token.Token, "project_01JHOSTING", manifest, time.Now())
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("update project = %d body=%s", rec.Code, rec.Body.String())
	}
	var updated internalProjectResponse
	if err := json.NewDecoder(rec.Body).Decode(&updated); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	if updated.Created || updated.Replayed || updated.ExternalProjectID != created.ExternalProjectID || updated.ManifestDigest == created.ManifestDigest {
		t.Fatalf("unexpected update response: %+v", updated)
	}

	stored, err := getHostingProjectByExternalID(created.ExternalProjectID)
	if err != nil {
		t.Fatalf("get provisioned project: %v", err)
	}
	if stored.ExternalProjectID != "project_01JHOSTING" || stored.RuntimeKind != "static" || stored.RunnerID != 0 {
		t.Fatalf("unexpected stored project: %+v", stored)
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_audit_events WHERE hosting_project_id=? AND event_type IN ('hosting_project_created','hosting_project_updated')`, stored.ID).Scan(&audits); err != nil || audits != 2 {
		t.Fatalf("project audit count=%d err=%v", audits, err)
	}
	var legacyProjects int
	if err := db.QueryRow(`SELECT COUNT(*) FROM projects`).Scan(&legacyProjects); err != nil {
		t.Fatalf("count legacy projects: %v", err)
	}
	if legacyProjects != 0 {
		t.Fatalf("hosting provision must not create a trusted admin project, got %d", legacyProjects)
	}
}

func TestInternalProjectProvisionEnforcesScopeAndSignature(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	readToken, err := createServiceToken("deployment-reader", []string{serviceScopeDeploymentsRead})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))

	req := signedManifestRequest(t, readToken.Token, "project_01JHOSTING", validHostingManifest("static"), time.Now())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusForbidden, errCodeInsufficientScope)

	writeToken, err := createServiceToken("project-writer", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatalf("create write token: %v", err)
	}
	req = signedManifestRequest(t, writeToken.Token, "project_01JHOSTING", validHostingManifest("static"), time.Now())
	req.Header.Set(manifestSignatureHeader, "sha256="+strings.Repeat("0", 64))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusUnauthorized, errCodeInvalidManifestSignature)

	req = signedManifestRequest(t, writeToken.Token, "project_01JHOSTING", validHostingManifest("static"), time.Now())
	req.Header.Del(manifestSignatureHeader)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusUnauthorized, errCodeManifestSignatureRequired)

	req = signedManifestRequest(t, writeToken.Token, "project_01JHOSTING", validHostingManifest("static"), time.Now().Add(-10*time.Minute))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusUnauthorized, errCodeManifestSignatureExpired)

	req = signedManifestRequest(t, writeToken.Token, "project_01JHOSTING", validHostingManifest("static"), time.Now())
	req.Header.Del("Content-Type")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusUnsupportedMediaType, errCodeUnsupportedMediaType)
}

func TestInternalProjectProvisionRejectsArbitraryShellAndUnknownFields(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	token, err := createServiceToken("project-writer", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatalf("create service token: %v", err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))

	manifest := validHostingManifest("node")
	manifest.Runtime.StartScript = "start; curl attacker.invalid"
	req := signedManifestRequest(t, token.Token, "project_01JHOSTING", manifest, time.Now())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusBadRequest, errCodeInvalidManifest)

	body := []byte(`{"manifest":{"schema_version":"v1","repository":{"installation_id":1,"repository_id":2,"full_name":"owner/repo"},"runtime":{"kind":"node","node_version":"22","package_manager":"npm","start_script":"start","port":3000,"health_path":"/health"},"resource_profile":"starter","post_deploy":"curl attacker.invalid"}}`)
	timestamp := itoa(time.Now().Unix())
	mac := hmac.New(sha256.New, []byte(token.Token))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	req = httptest.NewRequest(http.MethodPut, "/api/internal/v1/projects/project_01JHOSTING", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(manifestTimestampHeader, timestamp)
	req.Header.Set(manifestSignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertAPIErrorCode(t, rec, http.StatusBadRequest, errCodeInvalidManifest)
}

func TestHostingManifestValidationRejectsUnsupportedOrUnsafeFields(t *testing.T) {
	tests := map[string]func(*hostingProjectUpsertRequest){
		"unsupported runtime":  func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.Kind = "compose" },
		"unsupported node":     func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.NodeVersion = "latest" },
		"unsafe output":        func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.OutputDirectory = "../secrets" },
		"repository as output": func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.OutputDirectory = "." },
		"unsafe script":        func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.BuildScript = "build && env" },
		"unsafe health": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest = validHostingManifest("node")
			payload.Manifest.Runtime.HealthPath = "//metadata.internal"
		},
		"invalid repository": func(payload *hostingProjectUpsertRequest) { payload.Manifest.Repository.FullName = "missing-owner" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			payload := hostingProjectUpsertRequest{ExternalProjectID: "project_01JHOSTING", Manifest: validHostingManifest("static")}
			mutate(&payload)
			if err := validateHostingProjectRequest(&payload); err == nil {
				t.Fatal("expected manifest to be rejected")
			}
		})
	}
}

func TestDeriveHostingPathRequiresSafeAbsoluteRoot(t *testing.T) {
	if _, err := deriveHostingPath("relative/root", "project_01JHOSTING"); err == nil {
		t.Fatal("expected relative root to be rejected")
	}
	if _, err := deriveHostingPath("/", "project_01JHOSTING"); err == nil {
		t.Fatal("expected filesystem root to be rejected")
	}
	got, err := deriveHostingPath("/srv/deployer/hosting/repos", "project_01JHOSTING")
	if err != nil || got != "/srv/deployer/hosting/repos/project_01JHOSTING" {
		t.Fatalf("unexpected derived path %q err=%v", got, err)
	}
}

func TestHostingProjectUpsertIsConcurrentAndUnique(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	manifest := validHostingManifest("node")
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	ids := make(chan int64, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			project, _, _, err := upsertHostingProject(t.Context(), "project_01JCONCURRENT", manifest)
			if err != nil {
				errs <- err
				return
			}
			ids <- project.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		t.Errorf("concurrent upsert: %v", err)
	}
	var first int64
	for id := range ids {
		if first == 0 {
			first = id
		}
		if id != first {
			t.Errorf("expected one project ID, got %d and %d", first, id)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hosting_projects WHERE external_project_id=?`, "project_01JCONCURRENT").Scan(&count); err != nil {
		t.Fatalf("count projects: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one project, got %d", count)
	}
}

func TestExternalIdentityConstraintsAndDeploymentReplay(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JHOSTING", validHostingManifest("static"))
	if err != nil {
		t.Fatalf("provision project: %v", err)
	}
	now := formatSQLiteTime(time.Now())
	if _, err := db.Exec(`INSERT INTO hosting_projects (external_project_id, manifest_version, manifest_json, manifest_digest, repository_installation_id, repository_id, repository_full_name, runtime_kind, resource_profile, repo_path, deploy_path, runner_id, created_at, updated_at) VALUES (?, 'v1', '{}', 'sha256:duplicate', 1, 2, 'owner/repo', 'static', 'starter', '/srv/repos/duplicate', '/srv/apps/duplicate', 0, ?, ?)`, project.ExternalProjectID, now, now); err == nil {
		t.Fatal("expected external_project_id unique constraint")
	}
	if _, err := db.Exec(`INSERT INTO hosting_projects (external_project_id, manifest_version, manifest_json, manifest_digest, repository_installation_id, repository_id, repository_full_name, runtime_kind, resource_profile, repo_path, deploy_path, runner_id, created_at, updated_at) VALUES ('', 'v1', '{}', 'sha256:empty', 1, 2, 'owner/repo', 'static', 'starter', '/srv/repos/empty', '/srv/apps/empty', 0, ?, ?)`, now, now); err == nil {
		t.Fatal("expected empty external_project_id check constraint")
	}

	insertDeployment := func(externalID string) error {
		_, err := db.Exec(`INSERT INTO hosting_deployments (hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)`, project.ID, externalID, strings.Repeat("a", 40), project.ManifestDigest, "sha256:"+strings.Repeat("b", 64), now, now)
		return err
	}
	if err := insertDeployment("deployment_01JHOSTING"); err != nil {
		t.Fatalf("insert external deployment: %v", err)
	}
	if err := insertDeployment("deployment_01JHOSTING"); err == nil {
		t.Fatal("expected external_deployment_id unique constraint")
	}
	if err := insertDeployment(""); err == nil {
		t.Fatal("expected empty external_deployment_id check constraint")
	}
	if _, err := db.Exec(`INSERT INTO hosting_deployments (hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest, status, created_at, updated_at) VALUES (999999, 'deployment_01JFOREIGN', ?, ?, ?, 'queued', ?, ?)`, strings.Repeat("a", 40), project.ManifestDigest, "sha256:"+strings.Repeat("b", 64), now, now); err == nil {
		t.Fatal("expected hosting deployment foreign-key constraint")
	}
}

func TestGetProjectByExternalIDReturnsNoRows(t *testing.T) {
	withTempDB(t)
	if _, err := getHostingProjectByExternalID("project_missing"); err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func assertAPIErrorCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, status, rec.Body.String())
	}
	var response apiErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Code != code {
		t.Fatalf("error code = %q, want %q", response.Code, code)
	}
}
