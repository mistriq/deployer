package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostingWorkloadIdentityIsLeaseBoundAndContainsNoReference(t *testing.T) {
	withTempDB(t)
	_, _, runnerID, job := createAndClaimHostingJob(t, "project_01JWLIJOB", "deployment_01JWLIJOB")
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("workload-signing-key-", 2)
	secretReference := "opaque-workload-reference"
	refs := []HostingSecretReference{{Provider: "control-plane", Reference: secretReference, Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute)}}
	encoded, _ := json.Marshal(refs)
	if _, err := db.Exec(`UPDATE hosting_jobs SET secret_refs_json=? WHERE id=?`, string(encoded), job.JobID); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/workload-identity", job.JobID), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	recorder := httptest.NewRecorder()
	handleHostingJobWorkloadIdentity(recorder, request, &HostingRunner{ID: runnerID}, job.JobID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("identity response=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("identity response is cacheable: %q", recorder.Header().Get("Cache-Control"))
	}
	var identity hostingWorkloadIdentityResponse
	if err := json.NewDecoder(recorder.Body).Decode(&identity); err != nil {
		t.Fatal(err)
	}
	claims, err := parseHostingWorkloadIdentityForTest(identity.Token, appConfig.HostingWorkloadIdentitySecret)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, _ := hostingSecretReferencesDigest(refs)
	if claims.Issuer != hostingWorkloadIdentityIssuer || claims.Audience != hostingWorkloadIdentityAudience || claims.Operation != "build" || claims.RunnerID != runnerID ||
		claims.WorkID != job.JobID || claims.LeaseGeneration != job.LeaseGeneration || claims.SecretRefsDigest != expectedDigest ||
		claims.ExpiresAt <= claims.IssuedAt || strings.Contains(identity.Token, secretReference) {
		t.Fatalf("unexpected workload claims: %+v", claims)
	}
	if strings.Contains(redactSecrets("Authorization: Bearer "+identity.Token), identity.Token) {
		t.Fatal("workload identity was not redacted")
	}
	if _, err := parseHostingWorkloadIdentityForTest(strings.Replace(identity.Token, "wli_", "wli_A", 1), appConfig.HostingWorkloadIdentitySecret); err == nil {
		t.Fatal("tampered workload identity signature accepted")
	}
	second, err := mintHostingWorkloadIdentity(runnerID, job.JobID, job.LeaseGeneration, "build",
		claims.ExternalProjectID, claims.ExternalDeploymentID, refs, time.Now(), time.Now().Add(time.Minute))
	if err != nil || second.Token == identity.Token {
		t.Fatalf("workload token IDs are not unique: equal=%t err=%v", second != nil && second.Token == identity.Token, err)
	}

	request = httptest.NewRequest(http.MethodPost, request.URL.String(), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration+1))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	recorder = httptest.NewRecorder()
	handleHostingJobWorkloadIdentity(recorder, request, &HostingRunner{ID: runnerID}, job.JobID)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("stale lease identity response=%d", recorder.Code)
	}
	if _, err := db.Exec(`UPDATE hosting_jobs SET cancel_requested_at=? WHERE id=?`, formatSQLiteTime(time.Now().UTC()), job.JobID); err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/hosting-agent/v1/jobs/%d/workload-identity", job.JobID), nil)
	request.Header.Set("X-Deployer-Lease-Generation", fmt.Sprint(job.LeaseGeneration))
	request.Header.Set("X-Deployer-Lease-Token", job.LeaseToken)
	recorder = httptest.NewRecorder()
	handleHostingJobWorkloadIdentity(recorder, request, &HostingRunner{ID: runnerID}, job.JobID)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("cancelled lease identity response=%d", recorder.Code)
	}
}

func TestHostingWorkloadIdentityCannotOutliveLease(t *testing.T) {
	appConfig.HostingWorkloadIdentitySecret = strings.Repeat("workload-signing-key-", 2)
	now := time.Now().UTC()
	leaseExpires := now.Add(12 * time.Second)
	refs := []HostingSecretReference{{Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: now.Add(time.Minute)}}
	identity, err := mintHostingWorkloadIdentity(1, 2, 3, "build", "project_01JLEASE", "deployment_01JLEASE", refs, now, leaseExpires)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := parseHostingWorkloadIdentityForTest(identity.Token, appConfig.HostingWorkloadIdentitySecret)
	if err != nil {
		t.Fatal(err)
	}
	if claims.ExpiresAt != leaseExpires.Unix() || !identity.ExpiresAt.Equal(time.Unix(leaseExpires.Unix(), 0).UTC()) {
		t.Fatalf("identity outlives lease: claims=%d response=%s lease=%s", claims.ExpiresAt, identity.ExpiresAt, leaseExpires)
	}
}

func TestHostingSecretReferenceDigestCanonicalizesTimestampZone(t *testing.T) {
	expires := time.Date(2026, time.July, 20, 14, 15, 16, 123456789, time.FixedZone("plus-two", 2*60*60))
	base := HostingSecretReference{Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: expires}
	first, err := hostingSecretReferencesDigest([]HostingSecretReference{base})
	if err != nil {
		t.Fatal(err)
	}
	base.ExpiresAt = expires.UTC()
	second, err := hostingSecretReferencesDigest([]HostingSecretReference{base})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("equivalent timestamps produced different digests: %q != %q", first, second)
	}
}

func TestHostingAgentRedeemsSecretsIntoTmpfsFilesOnly(t *testing.T) {
	identity := "wli_opaque.short-signature"
	secretValue := "postgres://user:runtime-password@db.internal/app"
	refs := []HostingSecretReference{
		{Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute)},
		{Provider: "control-plane", Reference: "reference-api-token", Name: "API_TOKEN", ExpiresAt: time.Now().Add(time.Minute)},
	}
	deployer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/hosting-agent/v1/jobs/71/workload-identity" || r.Header.Get("X-Deployer-Lease-Token") != "lease-token" {
			t.Errorf("unexpected identity request path=%q", r.URL.Path)
		}
		jsonResponse(w, hostingWorkloadIdentityResponse{Token: identity, ExpiresAt: time.Now().Add(time.Minute)})
	}))
	defer deployer.Close()
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal/v1/workload-secrets/redeem" || r.Header.Get("Authorization") != "Bearer "+identity {
			t.Errorf("unexpected secret redemption")
		}
		var request hostingSecretBrokerRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.SecretReferences) != 2 {
			t.Errorf("decode references: %+v err=%v", request, err)
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, hostingSecretBrokerResponse{Values: []hostingSecretValue{{Name: "API_TOKEN", Value: "runtime-token-value"}, {Name: "DATABASE_URL", Value: secretValue}}})
	}))
	defer broker.Close()
	root := filepath.Join("/dev/shm", "deployer-secret-test-"+safeFileName(t.Name()))
	t.Cleanup(func() {
		_ = removeHostingSecretDirectory(filepath.Join(root, "build-71-3"))
		_ = os.RemoveAll(root)
	})
	job := &hostingClaimedJob{JobID: 71, LeaseGeneration: 3, LeaseToken: "lease-token", Recipe: hostingJobRecipe{Operation: "build", Runtime: HostingRuntimeManifest{Kind: "node"}}, SecretRefs: refs}
	directory, err := prepareHostingRuntimeSecrets(t.Context(), hostingAgentConfig{
		ServerURL: deployer.URL, Token: "runner", SecretBrokerURL: broker.URL,
		SecretBrokerTimeout: time.Second, SecretMemoryRoot: root,
	}, job, "build-71-3")
	if err != nil {
		t.Fatal(err)
	}
	databaseURL, err := os.ReadFile(filepath.Join(directory, "DATABASE_URL"))
	if err != nil || string(databaseURL) != secretValue {
		t.Fatalf("secret file value=%q err=%v", databaseURL, err)
	}
	info, err := os.Stat(filepath.Join(directory, "DATABASE_URL"))
	if err != nil || info.Mode().Perm() != 0444 {
		t.Fatalf("secret file permissions=%v err=%v", info.Mode().Perm(), err)
	}
	if strings.Contains(directory, secretValue) || strings.Contains(directory, refs[0].Reference) {
		t.Fatal("secret material leaked into mount path")
	}
	if err := removeHostingSecretDirectory(directory); err != nil {
		t.Fatalf("remove read-only secret directory: %v", err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secret directory remains after cleanup: %v", err)
	}
}

func TestValidateHostingSecretValuesRejectsMismatch(t *testing.T) {
	refs := []HostingSecretReference{{Name: "DATABASE_URL"}}
	for _, values := range [][]hostingSecretValue{
		{},
		{{Name: "OTHER", Value: "value"}},
		{{Name: "DATABASE_URL", Value: ""}},
	} {
		if err := validateHostingSecretValues(refs, values); err == nil {
			t.Fatalf("invalid secret values accepted: %#v", values)
		}
	}
}

func TestHostingAgentRejectsMalformedSecretBrokerResponses(t *testing.T) {
	refs := []HostingSecretReference{{Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute)}}
	deployer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		jsonResponse(w, hostingWorkloadIdentityResponse{Token: "wli_opaque.signature", ExpiresAt: time.Now().Add(time.Minute)})
	}))
	defer deployer.Close()
	root := filepath.Join("/dev/shm", "deployer-secret-test-"+safeFileName(t.Name()))
	t.Cleanup(func() {
		_ = removeHostingSecretDirectory(filepath.Join(root, "build-72-1"))
		_ = os.RemoveAll(root)
	})
	job := &hostingClaimedJob{JobID: 72, LeaseGeneration: 1, LeaseToken: "lease-token", Recipe: hostingJobRecipe{Operation: "build", Runtime: HostingRuntimeManifest{Kind: "node"}}, SecretRefs: refs}

	for name, response := range map[string]func(http.ResponseWriter){
		"cacheable response": func(w http.ResponseWriter) {
			w.Header().Set("Cache-Control", "private, x-no-storeable, no-store=garbage")
			jsonResponse(w, hostingSecretBrokerResponse{Values: []hostingSecretValue{{Name: "DATABASE_URL", Value: "secret"}}})
		},
		"wrong content type": func(w http.ResponseWriter) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(`{"values":[{"name":"DATABASE_URL","value":"secret"}]}`))
		},
		"trailing JSON": func(w http.ResponseWriter) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"name":"DATABASE_URL","value":"secret"}]} {}`))
		},
		"oversize body": func(w http.ResponseWriter) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(strings.Repeat("x", int(maxHostingSecretBrokerResponseBytes+1))))
		},
		"redirect": func(w http.ResponseWriter) {
			w.Header().Set("Location", "/other")
			w.WriteHeader(http.StatusFound)
		},
	} {
		t.Run(name, func(t *testing.T) {
			broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { response(w) }))
			defer broker.Close()
			directory, err := prepareHostingRuntimeSecrets(t.Context(), hostingAgentConfig{
				ServerURL: deployer.URL, Token: "runner", SecretBrokerURL: broker.URL,
				SecretBrokerTimeout: time.Second, SecretMemoryRoot: root,
			}, job, "build-72-1")
			if err == nil || directory != "" {
				t.Fatalf("malformed broker response accepted: directory=%q err=%v", directory, err)
			}
		})
	}
}

func TestHostingAgentStartupRemovesOrphanedSecretDirectories(t *testing.T) {
	root := filepath.Join("/dev/shm", "deployer-secret-test-"+safeFileName(t.Name()))
	orphan := filepath.Join(root, "build-99-2")
	if err := os.MkdirAll(orphan, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "DATABASE_URL"), []byte("orphaned-plaintext"), 0444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(orphan, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = removeHostingSecretDirectory(orphan)
		_ = os.RemoveAll(root)
	})
	oldFind := execHostingSecretContainers
	execHostingSecretContainers = func(string) (string, error) {
		return "", nil
	}
	t.Cleanup(func() { execHostingSecretContainers = oldFind })
	if err := reconcileHostingSecretDirectories(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphaned secret directory remains: %v", err)
	}
}

func TestHostingAgentStartupFailsWhenManagedContainerLostTmpfsSecrets(t *testing.T) {
	root := filepath.Join("/dev/shm", "deployer-secret-test-"+safeFileName(t.Name()))
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	oldList := execHostingSecretContainers
	execHostingSecretContainers = func(namespace string) (string, error) {
		return "container-id\tbuild-88-1\t" + namespace + "\n", nil
	}
	t.Cleanup(func() { execHostingSecretContainers = oldList })
	if err := reconcileHostingSecretDirectories(root, false); err == nil || !strings.Contains(err.Error(), "lost its tmpfs material") {
		t.Fatalf("missing secret material was not detected: %v", err)
	}
}

func TestHostingAgentStartupValidatesSecretNamespaceAndBindSource(t *testing.T) {
	root := filepath.Join("/dev/shm", "deployer-secret-test-"+safeFileName(t.Name()))
	directory := filepath.Join(root, "build-87-1")
	if err := removeHostingSecretDirectory(root); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = removeHostingSecretDirectory(directory)
		_ = os.RemoveAll(root)
	})
	if err := os.Mkdir(directory, 0555); err != nil {
		t.Fatal(err)
	}
	expectedNamespace := hostingAgentSecretNamespace(root)
	oldList := execHostingSecretContainers
	oldSource := hostingContainerSecretSource
	execHostingSecretContainers = func(namespace string) (string, error) {
		if namespace != expectedNamespace {
			t.Fatalf("container discovery namespace=%q want=%q", namespace, expectedNamespace)
		}
		return "container-id\tbuild-87-1\t" + namespace + "\n", nil
	}
	hostingContainerSecretSource = func(string) (string, error) {
		return filepath.Join(root, "wrong-instance"), nil
	}
	t.Cleanup(func() {
		execHostingSecretContainers = oldList
		hostingContainerSecretSource = oldSource
	})
	if err := reconcileHostingSecretDirectories(root, true); err == nil || !strings.Contains(err.Error(), "unexpected bind source") {
		t.Fatalf("mismatched secret bind source was accepted: %v", err)
	}
}

func TestHostingAgentSecretRootsAreNamespacedByWorkRoot(t *testing.T) {
	first := hostingAgentSecretMemoryRoot("/var/lib/deployer-agent-a")
	second := hostingAgentSecretMemoryRoot("/var/lib/deployer-agent-b")
	if first == second || filepath.Dir(first) != "/dev/shm/deployer-hosting-secrets" || filepath.Dir(second) != filepath.Dir(first) {
		t.Fatalf("secret roots are not isolated: first=%q second=%q", first, second)
	}
}

func TestHostingAgentAdvertisesSecretCapabilityOnlyWithBroker(t *testing.T) {
	without := hostingAgentOperations(hostingAgentConfig{})
	with := hostingAgentOperations(hostingAgentConfig{SecretBrokerURL: "https://broker.internal"})
	if hostingStringListContains(without, hostingRunnerSecretOperation) || !hostingStringListContains(with, hostingRunnerSecretOperation) ||
		!hostingStringListContains(without, hostingRunnerInventoryOperation) || !hostingStringListContains(with, hostingRunnerInventoryOperation) {
		t.Fatalf("unexpected secret capabilities: without=%v with=%v", without, with)
	}
}

func TestHostingAgentRemintsIdentityAfterTransientBrokerFailure(t *testing.T) {
	refs := []HostingSecretReference{{Provider: "control-plane", Reference: "reference-database-url", Name: "DATABASE_URL", ExpiresAt: time.Now().Add(time.Minute)}}
	identityCalls := 0
	deployer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		identityCalls++
		jsonResponse(w, hostingWorkloadIdentityResponse{Token: fmt.Sprintf("wli_identity-%d.signature", identityCalls), ExpiresAt: time.Now().Add(time.Minute)})
	}))
	defer deployer.Close()
	var brokerTokens []string
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		brokerTokens = append(brokerTokens, r.Header.Get("Authorization"))
		if len(brokerTokens) == 1 {
			jsonErrorCode(w, "unavailable", "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, hostingSecretBrokerResponse{Values: []hostingSecretValue{{Name: "DATABASE_URL", Value: "runtime-value"}}})
	}))
	defer broker.Close()
	root := filepath.Join("/dev/shm", "deployer-secret-test-"+safeFileName(t.Name()))
	t.Cleanup(func() {
		_ = removeHostingSecretDirectory(filepath.Join(root, "build-73-1"))
		_ = os.RemoveAll(root)
	})
	job := &hostingClaimedJob{JobID: 73, LeaseGeneration: 1, LeaseToken: "lease-token", Recipe: hostingJobRecipe{Operation: "build", Runtime: HostingRuntimeManifest{Kind: "node"}}, SecretRefs: refs}
	directory, err := prepareHostingRuntimeSecrets(t.Context(), hostingAgentConfig{
		ServerURL: deployer.URL, Token: "runner", SecretBrokerURL: broker.URL,
		SecretBrokerTimeout: time.Second, SecretMemoryRoot: root,
	}, job, "build-73-1")
	if err != nil {
		t.Fatal(err)
	}
	if identityCalls != 2 || len(brokerTokens) != 2 || brokerTokens[0] == brokerTokens[1] {
		t.Fatalf("transient retry did not remint identity: identity_calls=%d broker_tokens=%v", identityCalls, brokerTokens)
	}
	if err := removeHostingSecretDirectory(directory); err != nil {
		t.Fatal(err)
	}
}
