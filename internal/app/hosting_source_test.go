package app

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func sourceBrokerTestProject() *HostingProject {
	return &HostingProject{
		RepositoryInstallationID: 1001,
		RepositoryID:             2002,
		RepositoryFullName:       "socials-century/customer-app",
	}
}

func sourceBrokerTestReference() HostingSourceReference {
	return HostingSourceReference{
		Provider:  "control-plane",
		Reference: "source_01JBROKERTEST",
		ExpiresAt: time.Now().UTC().Add(30 * time.Minute),
	}
}

func sourceBrokerTestDigest(body []byte) string {
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sourceBrokerTestArchive(t *testing.T, content string) []byte {
	t.Helper()
	var body bytes.Buffer
	writer := tar.NewWriter(&body)
	data := []byte(content)
	if err := writer.WriteHeader(&tar.Header{Name: "index.js", Mode: 0644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes()
}

func configureSourceBrokerTestStorage(t *testing.T) {
	t.Helper()
	oldConfig := appConfig
	oldStorage := artifactStorage
	t.Cleanup(func() {
		appConfig = oldConfig
		artifactStorage = oldStorage
	})
	root := t.TempDir()
	appConfig.ArtifactDir = root + "/artifacts"
	appConfig.SnapshotDir = root + "/snapshots"
	configureArtifactStorage(appConfig)
}

func writeSourceBrokerResponse(w http.ResponseWriter, r *http.Request, body []byte) {
	w.Header().Set("Content-Type", "application/x-tar")
	for _, header := range []string{
		sourceRepositoryInstallationHeader,
		sourceRepositoryIDHeader,
		sourceRepositoryNameHeader,
		sourceCommitSHAHeader,
	} {
		w.Header().Set(header, r.Header.Get(header))
	}
	w.Header().Set(sourceArtifactDigestHeader, sourceBrokerTestDigest(body))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func TestHostingSourceBrokerFetchBindsIdentityAndStoresImmutableArtifact(t *testing.T) {
	configureSourceBrokerTestStorage(t)
	body := sourceBrokerTestArchive(t, "exact immutable source")
	commitSHA := strings.Repeat("a", 40)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/api/internal/v1/source-artifacts/redeem" {
			t.Errorf("unexpected broker request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer broker-token" || r.Header.Get("Accept") != "application/x-tar" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("missing source broker authentication/content negotiation")
		}
		var redemption hostingSourceRedeemRequest
		if err := json.NewDecoder(r.Body).Decode(&redemption); err != nil || redemption.Reference != sourceBrokerTestReference().Reference {
			t.Errorf("unexpected source redemption: %+v err=%v", redemption, err)
		}
		if r.Header.Get(sourceRepositoryInstallationHeader) != "1001" ||
			r.Header.Get(sourceRepositoryIDHeader) != "2002" ||
			r.Header.Get(sourceRepositoryNameHeader) != "socials-century/customer-app" ||
			r.Header.Get(sourceCommitSHAHeader) != commitSHA ||
			r.Header.Get(sourceArtifactDigestHeader) != sourceBrokerTestDigest(body) {
			t.Errorf("source identity headers do not match the provisioned project")
		}
		writeSourceBrokerResponse(w, r, body)
	}))
	defer server.Close()

	client, err := newHostingSourceBrokerClient(AppConfig{
		HostingSourceBrokerURL:     server.URL,
		HostingSourceBrokerToken:   "broker-token",
		HostingSourceBrokerTimeout: time.Second,
		HostingSourceMaxBytes:      1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	path, digest, err := client.fetch(context.Background(), sourceBrokerTestProject(), sourceBrokerTestReference(), commitSHA, sourceBrokerTestDigest(body))
	if err != nil {
		t.Fatal(err)
	}
	if digest != sourceBrokerTestDigest(body) || !isManagedArtifactPath(path) {
		t.Fatalf("path=%q digest=%q", path, digest)
	}
	stored, err := os.ReadFile(path)
	if err != nil || string(stored) != string(body) {
		t.Fatalf("stored source body=%q err=%v", stored, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("source artifact permissions=%v err=%v", info.Mode().Perm(), err)
	}
	replayedPath, replayedDigest, err := client.fetch(context.Background(), sourceBrokerTestProject(), sourceBrokerTestReference(), commitSHA, sourceBrokerTestDigest(body))
	if err != nil || replayedPath != path || replayedDigest != digest || calls != 2 {
		t.Fatalf("content-addressed replay path=%q digest=%q calls=%d err=%v", replayedPath, replayedDigest, calls, err)
	}
}

func TestHostingSourceBrokerRejectsMismatchedIdentityAndBody(t *testing.T) {
	configureSourceBrokerTestStorage(t)
	commitSHA := strings.Repeat("b", 40)
	tests := map[string]func(http.ResponseWriter, *http.Request){
		"repository identity": func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set(sourceRepositoryIDHeader, "9999")
			writeSourceBrokerResponse(w, r, []byte("archive"))
		},
		"commit identity": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-tar")
			w.Header().Set(sourceRepositoryInstallationHeader, r.Header.Get(sourceRepositoryInstallationHeader))
			w.Header().Set(sourceRepositoryIDHeader, r.Header.Get(sourceRepositoryIDHeader))
			w.Header().Set(sourceRepositoryNameHeader, r.Header.Get(sourceRepositoryNameHeader))
			w.Header().Set(sourceCommitSHAHeader, strings.Repeat("c", 40))
			w.Header().Set(sourceArtifactDigestHeader, sourceBrokerTestDigest([]byte("archive")))
			_, _ = w.Write([]byte("archive"))
		},
		"artifact body": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-tar")
			for _, header := range []string{sourceRepositoryInstallationHeader, sourceRepositoryIDHeader, sourceRepositoryNameHeader, sourceCommitSHAHeader} {
				w.Header().Set(header, r.Header.Get(header))
			}
			w.Header().Set(sourceArtifactDigestHeader, sourceBrokerTestDigest([]byte("expected")))
			_, _ = w.Write([]byte("different"))
		},
	}
	for name, handler := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(handler))
			defer server.Close()
			client, err := newHostingSourceBrokerClient(AppConfig{
				HostingSourceBrokerURL: server.URL, HostingSourceBrokerToken: "token",
				HostingSourceMaxBytes: 1 << 20, HostingSourceBrokerTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			path, digest, err := client.fetch(t.Context(), sourceBrokerTestProject(), sourceBrokerTestReference(), commitSHA, sourceBrokerTestDigest([]byte("expected")))
			if name == "artifact body" {
				if !errors.Is(err, errHostingSourceArtifactMismatch) || path != "" || digest != sourceBrokerTestDigest([]byte("different")) {
					t.Fatalf("body mismatch path=%q digest=%q err=%v", path, digest, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("mismatched %s was accepted", name)
			}
		})
	}
}

func TestHostingSourceBrokerRejectsMalformedOrUnsafeTar(t *testing.T) {
	configureSourceBrokerTestStorage(t)
	commitSHA := strings.Repeat("e", 40)
	invalidBodies := map[string][]byte{
		"plain bytes": []byte("not a tar archive"),
		"unsafe path": func() []byte {
			var body bytes.Buffer
			writer := tar.NewWriter(&body)
			_ = writer.WriteHeader(&tar.Header{Name: "../escape", Mode: 0644, Size: 1, Typeflag: tar.TypeReg})
			_, _ = writer.Write([]byte("x"))
			_ = writer.Close()
			return body.Bytes()
		}(),
	}
	for name, body := range invalidBodies {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeSourceBrokerResponse(w, r, body)
			}))
			defer server.Close()
			client, err := newHostingSourceBrokerClient(AppConfig{
				HostingSourceBrokerURL: server.URL, HostingSourceBrokerToken: "token",
				HostingSourceMaxBytes: 1 << 20, HostingSourceBrokerTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			if path, _, err := client.fetch(t.Context(), sourceBrokerTestProject(), sourceBrokerTestReference(), commitSHA, sourceBrokerTestDigest(body)); err == nil || path != "" {
				t.Fatalf("invalid archive path=%q err=%v", path, err)
			}
		})
	}
}

func TestHostingSourceBrokerRejectsOversizeRedirectAndUnsafeConfiguration(t *testing.T) {
	configureSourceBrokerTestStorage(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSourceBrokerResponse(w, r, []byte("body exceeds limit"))
	}))
	defer server.Close()
	client, err := newHostingSourceBrokerClient(AppConfig{
		HostingSourceBrokerURL: server.URL, HostingSourceBrokerToken: "token",
		HostingSourceMaxBytes: 4, HostingSourceBrokerTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.fetch(t.Context(), sourceBrokerTestProject(), sourceBrokerTestReference(), strings.Repeat("d", 40), sourceBrokerTestDigest([]byte("body exceeds limit"))); err == nil {
		t.Fatal("oversize source artifact was accepted")
	}

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err = newHostingSourceBrokerClient(AppConfig{
		HostingSourceBrokerURL: redirect.URL, HostingSourceBrokerToken: "token",
		HostingSourceMaxBytes: 1 << 20, HostingSourceBrokerTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.fetch(t.Context(), sourceBrokerTestProject(), sourceBrokerTestReference(), strings.Repeat("d", 40), sourceBrokerTestDigest([]byte("body exceeds limit"))); err == nil {
		t.Fatal("source broker redirect was followed")
	}

	for _, rawURL := range []string{"", "http://10.0.0.5", "ftp://localhost/source", "https://user:pass@example.test"} {
		if _, err := newHostingSourceBrokerClient(AppConfig{
			HostingSourceBrokerURL: rawURL, HostingSourceBrokerToken: "token", HostingSourceMaxBytes: 1024,
		}); err == nil {
			t.Fatalf("unsafe source broker URL %q was accepted", rawURL)
		}
	}
	if _, err := newHostingSourceBrokerClient(AppConfig{
		HostingSourceBrokerURL: server.URL, HostingSourceBrokerToken: "token", HostingSourceMaxBytes: 1024,
		HostingSourceBrokerTimeout: 5 * time.Minute, ServerWriteTimeout: 5 * time.Minute,
	}); err == nil {
		t.Fatal("source broker timeout without server response margin was accepted")
	}
}

func TestHostingSourceBrokerRejectsSymlinkAtContentAddress(t *testing.T) {
	configureSourceBrokerTestStorage(t)
	if err := currentArtifactStorage().Ensure(); err != nil {
		t.Fatal(err)
	}
	body := sourceBrokerTestArchive(t, "symlink defense")
	digest := sourceBrokerTestDigest(body)
	finalPath := managedArtifactPath("hosting-source-" + strings.TrimPrefix(digest, "sha256:") + ".tar")
	outside := t.TempDir() + "/outside"
	if err := os.WriteFile(outside, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, finalPath); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSourceBrokerResponse(w, r, body)
	}))
	defer server.Close()
	client, err := newHostingSourceBrokerClient(AppConfig{
		HostingSourceBrokerURL: server.URL, HostingSourceBrokerToken: "token",
		HostingSourceMaxBytes: 1 << 20, HostingSourceBrokerTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.fetch(t.Context(), sourceBrokerTestProject(), sourceBrokerTestReference(), strings.Repeat("f", 40), digest); err == nil {
		t.Fatal("symlink content-address target was accepted")
	}
}

func TestHostingDeploymentSourceReferenceValidation(t *testing.T) {
	request := validHostingDeploymentRequest("deployment_01JSOURCE")
	request.SourceReference.Provider = "caller-url"
	if err := validateHostingDeploymentRequest(&request); err == nil {
		t.Fatal("caller-selected source provider was accepted")
	}
	request = validHostingDeploymentRequest("deployment_01JSOURCE")
	request.SourceReference.Reference = "https://attacker.invalid/source"
	if err := validateHostingDeploymentRequest(&request); err == nil {
		t.Fatal("unsafe source reference was accepted")
	}
	request = validHostingDeploymentRequest("deployment_01JSOURCE")
	request.SourceReference.ExpiresAt = time.Now().Add(-time.Second)
	if err := validateHostingDeploymentRequest(&request); err == nil {
		t.Fatal("expired source reference was accepted")
	}
}
