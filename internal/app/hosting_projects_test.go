package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	appConfig.HostingDeployRoot = filepath.Join(root, "apps")
	if artifactStorage == nil {
		appConfig.ArtifactDir = filepath.Join(root, "artifacts")
		appConfig.SnapshotDir = filepath.Join(root, "snapshots")
		configureArtifactStorage(appConfig)
	}
}

func signedManifestRequest(t *testing.T, token, externalProjectID string, manifest HostingProjectManifest, signedAt time.Time) *http.Request {
	t.Helper()
	body, err := json.Marshal(hostingProjectUpsertRequest{
		DefaultHostname: "customer-app.apps.example.test",
		Manifest:        manifest,
	})
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return signedRawManifestRequest(t, token, externalProjectID, body, signedAt)
}

func signedManifestRequestWithPublicationMode(t *testing.T, token, externalProjectID, publicationMode string, manifest HostingProjectManifest, signedAt time.Time) *http.Request {
	t.Helper()
	body, err := json.Marshal(hostingProjectUpsertRequest{
		DefaultHostname: "customer-app.apps.example.test",
		PublicationMode: publicationMode,
		Manifest:        manifest,
	})
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return signedRawManifestRequest(t, token, externalProjectID, body, signedAt)
}

func TestInternalProjectPublicationModeDefaultsPersistsAndIsImmutable(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	token, err := createServiceToken("publication-mode-writer", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	manifest := validHostingManifest("static")

	legacyRequest := signedManifestRequest(t, token.Token, "project_01JMODELEG", manifest, time.Now())
	legacyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(legacyRecorder, legacyRequest)
	if legacyRecorder.Code != http.StatusCreated {
		t.Fatalf("legacy create=%d body=%s", legacyRecorder.Code, legacyRecorder.Body.String())
	}
	var legacy internalProjectResponse
	if err := json.NewDecoder(legacyRecorder.Body).Decode(&legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.PublicationMode != hostingPublicationProxyV1 {
		t.Fatalf("legacy mode=%q", legacy.PublicationMode)
	}

	runtimeRequest := signedManifestRequestWithPublicationMode(t, token.Token, "project_01JMODERUN", hostingPublicationRuntimeOnlyV1, manifest, time.Now())
	runtimeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(runtimeRecorder, runtimeRequest)
	if runtimeRecorder.Code != http.StatusCreated {
		t.Fatalf("runtime create=%d body=%s", runtimeRecorder.Code, runtimeRecorder.Body.String())
	}
	runtimeRequest = signedManifestRequestWithPublicationMode(t, token.Token, "project_01JMODERUN", hostingPublicationRuntimeOnlyV1, manifest, time.Now())
	runtimeRecorder = httptest.NewRecorder()
	handler.ServeHTTP(runtimeRecorder, runtimeRequest)
	if runtimeRecorder.Code != http.StatusOK {
		t.Fatalf("runtime replay=%d body=%s", runtimeRecorder.Code, runtimeRecorder.Body.String())
	}
	var replay internalProjectResponse
	if err := json.NewDecoder(runtimeRecorder.Body).Decode(&replay); err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.PublicationMode != hostingPublicationRuntimeOnlyV1 {
		t.Fatalf("runtime replay=%+v", replay)
	}

	conflict := signedManifestRequest(t, token.Token, "project_01JMODERUN", manifest, time.Now())
	conflictRecorder := httptest.NewRecorder()
	handler.ServeHTTP(conflictRecorder, conflict)
	if conflictRecorder.Code != http.StatusConflict || !strings.Contains(conflictRecorder.Body.String(), "conflict") {
		t.Fatalf("mode conflict=%d body=%s", conflictRecorder.Code, conflictRecorder.Body.String())
	}
	stored, err := getHostingProjectByExternalID("project_01JMODERUN")
	if err != nil || stored.PublicationMode != hostingPublicationRuntimeOnlyV1 {
		t.Fatalf("stored runtime mode=%+v err=%v", stored, err)
	}
}

func signedRawManifestRequest(t *testing.T, token, externalProjectID string, body []byte, signedAt time.Time) *http.Request {
	t.Helper()
	timestampText := itoa(signedAt.Unix())
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

	baseBody, err := json.Marshal(hostingProjectUpsertRequest{DefaultHostname: "customer-app.apps.example.test", Manifest: validHostingManifest("node")})
	if err != nil {
		t.Fatal(err)
	}
	for field, value := range map[string]any{
		"dockerfile":   "Dockerfile.customer",
		"compose_file": "compose.yaml",
		"host_path":    "/etc",
		"host_mounts":  []string{"/:/host"},
		"host_port":    443,
		"privileged":   true,
		"capabilities": []string{"SYS_ADMIN"},
		"build_args":   map[string]string{"TOKEN": "plaintext"},
		"environment":  map[string]string{"TOKEN": "plaintext"},
		"command":      "curl attacker.invalid",
	} {
		t.Run(field, func(t *testing.T) {
			var document map[string]any
			if err := json.Unmarshal(baseBody, &document); err != nil {
				t.Fatal(err)
			}
			manifestObject := document["manifest"].(map[string]any)
			runtimeObject := manifestObject["runtime"].(map[string]any)
			runtimeObject[field] = value
			body, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			request := signedRawManifestRequest(t, token.Token, "project_01JHOSTING", body, time.Now())
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			assertAPIErrorCode(t, recorder, http.StatusBadRequest, errCodeInvalidManifest)
		})
	}
}

func TestInternalProjectProvisionRejectsBadRequestEnvelopes(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	token, err := createServiceToken("project-request-boundaries", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	handler := serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI))
	validBody, err := json.Marshal(hostingProjectUpsertRequest{DefaultHostname: "customer-app.apps.example.test", Manifest: validHostingManifest("static")})
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]struct {
		body   []byte
		status int
		code   string
	}{
		"oversized":       {body: bytes.Repeat([]byte(" "), maxManifestBodyBytes+1), status: http.StatusRequestEntityTooLarge, code: errCodePayloadTooLarge},
		"malformed":       {body: []byte(`{"manifest":`), status: http.StatusBadRequest, code: errCodeInvalidManifest},
		"multiple values": {body: append(append([]byte(nil), validBody...), []byte(` {}`)...), status: http.StatusBadRequest, code: errCodeInvalidManifest},
	}
	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			request := signedRawManifestRequest(t, token.Token, "project_01JREQBOUND", testCase.body, time.Now())
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			assertAPIErrorCode(t, recorder, testCase.status, testCase.code)
			if recorder.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("error content type=%q", recorder.Header().Get("Content-Type"))
			}
		})
	}
}

func TestHostingManifestValidationRejectsUnsupportedOrUnsafeFields(t *testing.T) {
	tests := map[string]func(*hostingProjectUpsertRequest){
		"unsupported schema":  func(payload *hostingProjectUpsertRequest) { payload.Manifest.SchemaVersion = "v2" },
		"unsupported runtime": func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.Kind = "compose" },
		"unsupported node":    func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.NodeVersion = "latest" },
		"missing repository identity": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Repository.InstallationID = 0
		},
		"missing output directory": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Runtime.OutputDirectory = ""
		},
		"unsafe output": func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.OutputDirectory = "../secrets" },
		"output instruction injection": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Runtime.OutputDirectory = "dist\nRUN curl attacker.invalid"
		},
		"output comment injection": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Runtime.OutputDirectory = "dist # ignored"
		},
		"output backslash": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Runtime.OutputDirectory = `dist\escape`
		},
		"output noncanonical": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Runtime.OutputDirectory = "dist/../public"
		},
		"reserved output": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest.Runtime.OutputDirectory = ".deployer/site"
		},
		"unsafe script": func(payload *hostingProjectUpsertRequest) { payload.Manifest.Runtime.BuildScript = "build && env" },
		"unsafe health": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest = validHostingManifest("node")
			payload.Manifest.Runtime.HealthPath = "//metadata.internal"
		},
		"health path backslash": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest = validHostingManifest("node")
			payload.Manifest.Runtime.HealthPath = `/health\escape`
		},
		"health path unicode": func(payload *hostingProjectUpsertRequest) {
			payload.Manifest = validHostingManifest("node")
			payload.Manifest.Runtime.HealthPath = "/héalth"
		},
		"invalid repository": func(payload *hostingProjectUpsertRequest) { payload.Manifest.Repository.FullName = "missing-owner" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			payload := hostingProjectUpsertRequest{ExternalProjectID: "project_01JHOSTING", DefaultHostname: "customer-app.apps.example.test", Manifest: validHostingManifest("static")}
			mutate(&payload)
			if err := validateHostingProjectRequest(&payload); err == nil {
				t.Fatal("expected manifest to be rejected")
			}
		})
	}
}

func TestHostingManifestValidationAcceptsNoBuildStatic(t *testing.T) {
	for _, runtime := range []HostingRuntimeManifest{
		{Kind: "static", OutputDirectory: "."},
		{Kind: "static", OutputDirectory: "public/assets"},
		{Kind: "static", NodeVersion: "20", OutputDirectory: "site"},
	} {
		t.Run(runtime.OutputDirectory+runtime.NodeVersion, func(t *testing.T) {
			manifest := validHostingManifest("static")
			manifest.Runtime = runtime
			payload := hostingProjectUpsertRequest{ExternalProjectID: "project_01JNOBUILD", DefaultHostname: "plain-site.apps.example.test", Manifest: manifest}
			if err := validateHostingProjectRequest(&payload); err != nil {
				t.Fatalf("valid no-build static manifest rejected: %v", err)
			}
		})
	}
}

func TestHostingManifestValidationRejectsMalformedNoBuildStatic(t *testing.T) {
	tests := map[string]HostingRuntimeManifest{
		"package without build": {Kind: "static", NodeVersion: "22", PackageManager: "npm", OutputDirectory: "."},
		"build without package": {Kind: "static", NodeVersion: "22", BuildScript: "build", OutputDirectory: "."},
		"build without node":    {Kind: "static", PackageManager: "npm", BuildScript: "build", OutputDirectory: "."},
		"invalid package":       {Kind: "static", NodeVersion: "22", PackageManager: "bun", BuildScript: "build", OutputDirectory: "."},
		"invalid optional node": {Kind: "static", NodeVersion: "latest", OutputDirectory: "."},
		"start script":          {Kind: "static", OutputDirectory: ".", StartScript: "start"},
		"port":                  {Kind: "static", OutputDirectory: ".", Port: 8080},
		"health path":           {Kind: "static", OutputDirectory: ".", HealthPath: "/health"},
	}
	for name, runtime := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := validHostingManifest("static")
			manifest.Runtime = runtime
			payload := hostingProjectUpsertRequest{ExternalProjectID: "project_01JNOBUILD", DefaultHostname: "plain-site.apps.example.test", Manifest: manifest}
			if err := validateHostingProjectRequest(&payload); err == nil {
				t.Fatal("malformed no-build static manifest was accepted")
			}
		})
	}
}

func TestInternalProjectProvisionAcceptsExactNoBuildStaticWireShape(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	token, err := createServiceToken("no-build-project-writer", []string{serviceScopeProjectsWrite})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"default_hostname":"plain-site.apps.example.test","manifest":{"schema_version":"v1","repository":{"installation_id":1001,"repository_id":2002,"full_name":"socials-century/plain-site"},"runtime":{"kind":"static","output_directory":"."},"resource_profile":"starter"}}`)
	request := signedRawManifestRequest(t, token.Token, "project_01JNOBUILD", body, time.Now())
	recorder := httptest.NewRecorder()
	serviceTokenAuthMiddleware(http.HandlerFunc(handleInternalAPI)).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("provision no-build static = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response internalProjectResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Manifest.Runtime != (HostingRuntimeManifest{Kind: "static", OutputDirectory: "."}) {
		t.Fatalf("unexpected canonical runtime: %+v", response.Manifest.Runtime)
	}
}

func TestHostingExternalIdentifiersRejectSecretShapedValues(t *testing.T) {
	for _, identifier := range []string{
		"dpl_1234567890abcdefghij",
		"htr_1234567890abcdefghij",
		"wli_1234567890abcdefghij",
		"ghp_1234567890abcdefghij",
		"gho_1234567890abcdefghij",
		"ghu_1234567890abcdefghij",
		"ghs_1234567890abcdefghij",
		"github_pat_1234567890abcdefghij",
		"sk_live_1234567890abcdefghij",
		"xoxb-1234567890abcdefghij",
		"xoxa-1234567890abcdefghij",
		"xoxp-1234567890abcdefghij",
		"xoxr-1234567890abcdefghij",
		"xoxs-1234567890abcdefghij",
		"AKIA1234567890ABCDEF",
	} {
		for _, candidate := range []string{identifier, "project-" + identifier} {
			projectRequest := hostingProjectUpsertRequest{ExternalProjectID: candidate, DefaultHostname: "candidate.apps.example.test",
				Manifest: validHostingManifest("static")}
			if err := validateHostingProjectRequest(&projectRequest); err == nil {
				t.Fatalf("secret-shaped external project ID %q was accepted", candidate)
			}
			deploymentRequest := validHostingDeploymentRequest(candidate)
			if err := validateHostingDeploymentRequestShape(&deploymentRequest); err == nil {
				t.Fatalf("secret-shaped external deployment ID %q was accepted", candidate)
			}
			if !validHostingExternalIDSyntax(candidate) {
				t.Fatalf("legacy-compatible syntax unexpectedly rejected %q", candidate)
			}
		}
	}
}

func TestHostingManifestDigestUsesValidatedCanonicalValues(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	canonical := validHostingManifest("static")
	padded := canonical
	padded.Repository.FullName = "  " + padded.Repository.FullName + "  "
	padded.Runtime.Kind = " static "
	padded.Runtime.NodeVersion = " 22 "
	padded.Runtime.PackageManager = " npm "
	padded.Runtime.BuildScript = " build "
	padded.Runtime.OutputDirectory = " dist "
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JMANDIGEST", padded)
	if err != nil {
		t.Fatal(err)
	}
	_, expectedDigest, err := manifestJSONAndDigest(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if project.ManifestDigest != expectedDigest || project.Manifest != canonical {
		t.Fatalf("canonical manifest mismatch: digest=%q want=%q manifest=%+v", project.ManifestDigest, expectedDigest, project.Manifest)
	}
}

func TestNoBuildStaticManifestDigestOmitsBuildFields(t *testing.T) {
	manifest := validHostingManifest("static")
	manifest.Runtime = HostingRuntimeManifest{Kind: "static", OutputDirectory: "."}
	encoded, firstDigest, err := manifestJSONAndDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{"node_version", "package_manager", "build_script"} {
		if strings.Contains(encoded, `"`+absent+`"`) {
			t.Fatalf("canonical no-build manifest contains absent field %q: %s", absent, encoded)
		}
	}
	_, secondDigest, err := manifestJSONAndDigest(manifest)
	if err != nil || secondDigest != firstDigest {
		t.Fatalf("no-build manifest digest is not deterministic: first=%q second=%q err=%v", firstDigest, secondDigest, err)
	}
	built := manifest
	built.Runtime = HostingRuntimeManifest{Kind: "static", NodeVersion: "22", PackageManager: "npm", BuildScript: "build", OutputDirectory: "."}
	_, builtDigest, err := manifestJSONAndDigest(built)
	if err != nil {
		t.Fatal(err)
	}
	if builtDigest == firstDigest {
		t.Fatal("built and no-build static manifests have the same digest")
	}
}

func TestDeriveHostingPathRequiresSafeAbsoluteRoot(t *testing.T) {
	if _, err := deriveHostingPath("relative/root", "project_01JHOSTING"); err == nil {
		t.Fatal("expected relative root to be rejected")
	}
	if _, err := deriveHostingPath("/", "project_01JHOSTING"); err == nil {
		t.Fatal("expected filesystem root to be rejected")
	}
	got, err := deriveHostingPath("/srv/deployer/hosting/apps", "project_01JHOSTING")
	if err != nil || got != "/srv/deployer/hosting/apps/project_01JHOSTING" {
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

func TestHostingProjectConcurrentDifferentManifestsReturnTransactionSnapshot(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	manifests := []HostingProjectManifest{validHostingManifest("node"), validHostingManifest("node")}
	manifests[1].ResourceProfile = "standard"
	const workers = 32
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for index := 0; index < workers; index++ {
		manifest := manifests[index%len(manifests)]
		_, expectedDigest, err := manifestJSONAndDigest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			project, _, _, err := upsertHostingProject(context.Background(), "project_01JMANIRACE", manifest)
			if err != nil {
				errs <- err
				return
			}
			_, returnedDigest, err := manifestJSONAndDigest(project.Manifest)
			if err != nil {
				errs <- err
				return
			}
			if project.ManifestDigest != expectedDigest || returnedDigest != expectedDigest {
				errs <- fmt.Errorf("mixed manifest response: expected=%s field=%s body=%s", expectedDigest, project.ManifestDigest, returnedDigest)
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExternalIdentityConstraintsAndDeploymentReplay(t *testing.T) {
	withTempDB(t)
	withHostingConfig(t)
	project, _, _, err := upsertHostingProject(t.Context(), "project_01JHOSTING", validHostingManifest("static"))
	if err != nil {
		t.Fatalf("provision project: %v", err)
	}
	secondProject, _, _, err := upsertHostingProject(t.Context(), "project_01JHOSTING2", validHostingManifest("static"))
	if err != nil {
		t.Fatalf("provision second project: %v", err)
	}
	now := formatSQLiteTime(time.Now())
	if _, err := db.Exec(`INSERT INTO hosting_projects (external_project_id, manifest_version, manifest_json, manifest_digest, repository_installation_id, repository_id, repository_full_name, runtime_kind, resource_profile, repo_path, deploy_path, runner_id, created_at, updated_at) VALUES (?, 'v1', '{}', 'sha256:duplicate', 1, 2, 'owner/repo', 'static', 'starter', '/srv/repos/duplicate', '/srv/apps/duplicate', 0, ?, ?)`, project.ExternalProjectID, now, now); err == nil {
		t.Fatal("expected external_project_id unique constraint")
	}
	if _, err := db.Exec(`INSERT INTO hosting_projects (external_project_id, manifest_version, manifest_json, manifest_digest, repository_installation_id, repository_id, repository_full_name, runtime_kind, resource_profile, repo_path, deploy_path, runner_id, created_at, updated_at) VALUES ('', 'v1', '{}', 'sha256:empty', 1, 2, 'owner/repo', 'static', 'starter', '/srv/repos/empty', '/srv/apps/empty', 0, ?, ?)`, now, now); err == nil {
		t.Fatal("expected empty external_project_id check constraint")
	}

	insertDeployment := func(projectID int64, externalID string) error {
		_, err := db.Exec(`INSERT INTO hosting_deployments (hosting_project_id, external_deployment_id, commit_sha, manifest_digest, artifact_digest, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'failed', ?, ?)`, projectID, externalID, strings.Repeat("a", 40), project.ManifestDigest, "sha256:"+strings.Repeat("b", 64), now, now)
		return err
	}
	if err := insertDeployment(project.ID, "deployment_01JHOSTING"); err != nil {
		t.Fatalf("insert external deployment: %v", err)
	}
	if err := insertDeployment(secondProject.ID, "deployment_01JHOSTING"); err == nil {
		t.Fatal("expected external_deployment_id unique constraint")
	}
	if err := insertDeployment(secondProject.ID, ""); err == nil {
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
