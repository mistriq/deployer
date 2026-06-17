package app

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagedArtifactPathRejectsOutsidePath(t *testing.T) {
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })

	base := t.TempDir()
	appConfig = AppConfig{
		ArtifactDir: filepath.Join(base, "artifacts"),
		SnapshotDir: filepath.Join(base, "snapshots"),
	}
	if err := ensureRuntimeDirs(appConfig); err != nil {
		t.Fatalf("ensure runtime dirs: %v", err)
	}

	if !isManagedArtifactPath(managedArtifactPath("build.tar")) {
		t.Fatal("expected managed artifact path to be accepted")
	}
	if isManagedArtifactPath(filepath.Join(base, "other", "build.tar")) {
		t.Fatal("expected outside path to be rejected")
	}
}

func TestManagedSnapshotPathSanitizesName(t *testing.T) {
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })

	base := t.TempDir()
	appConfig = AppConfig{
		ArtifactDir: filepath.Join(base, "artifacts"),
		SnapshotDir: filepath.Join(base, "snapshots"),
	}
	if err := ensureRuntimeDirs(appConfig); err != nil {
		t.Fatalf("ensure runtime dirs: %v", err)
	}

	got := managedSnapshotPath("../Dashboard API.tar.gz")
	want := filepath.Join(appConfig.SnapshotDir, "Dashboard-API.tar.gz")
	if got != want {
		t.Fatalf("managed snapshot path = %q, want %q", got, want)
	}
	if !isManagedArtifactPath(got) {
		t.Fatal("expected managed snapshot path to be accepted")
	}
}

func TestRemoveManagedArtifactOnlyRemovesManagedFiles(t *testing.T) {
	oldConfig := appConfig
	t.Cleanup(func() { appConfig = oldConfig })

	base := t.TempDir()
	appConfig = AppConfig{
		ArtifactDir: filepath.Join(base, "artifacts"),
		SnapshotDir: filepath.Join(base, "snapshots"),
	}
	if err := ensureRuntimeDirs(appConfig); err != nil {
		t.Fatalf("ensure runtime dirs: %v", err)
	}

	managed := managedArtifactPath("old.tar")
	if err := os.WriteFile(managed, []byte("old"), 0644); err != nil {
		t.Fatalf("write managed artifact: %v", err)
	}
	unmanaged := filepath.Join(base, "unmanaged.tar")
	if err := os.WriteFile(unmanaged, []byte("keep"), 0644); err != nil {
		t.Fatalf("write unmanaged artifact: %v", err)
	}

	removeManagedArtifact(managed)
	if _, err := os.Stat(managed); !os.IsNotExist(err) {
		t.Fatalf("expected managed artifact removed, got %v", err)
	}
	removeManagedArtifact(unmanaged)
	if _, err := os.Stat(unmanaged); err != nil {
		t.Fatalf("expected unmanaged artifact to remain: %v", err)
	}
}

func TestRemoveFilesOlderThan(t *testing.T) {
	dir := t.TempDir()
	oldFile := filepath.Join(dir, "old.tar")
	newFile := filepath.Join(dir, "new.tar")
	if err := os.WriteFile(oldFile, []byte("old"), 0644); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	if err := os.WriteFile(newFile, []byte("new"), 0644); err != nil {
		t.Fatalf("write new file: %v", err)
	}
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatalf("set old file time: %v", err)
	}

	removed, err := removeFilesOlderThan(dir, 24*time.Hour)
	if err != nil {
		t.Fatalf("remove old files: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed file, got %d", removed)
	}
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("expected old file to be removed, got %v", err)
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Fatalf("expected new file to remain: %v", err)
	}
}

func TestCleanupStaleArtifactsRemovesOldManagedFiles(t *testing.T) {
	base := t.TempDir()
	cfg := AppConfig{
		ArtifactDir:            filepath.Join(base, "artifacts"),
		SnapshotDir:            filepath.Join(base, "snapshots"),
		ArtifactRetentionHours: 24,
	}
	if err := ensureRuntimeDirs(cfg); err != nil {
		t.Fatalf("ensure runtime dirs: %v", err)
	}

	oldArtifact := filepath.Join(cfg.ArtifactDir, "old.tar")
	oldSnapshot := filepath.Join(cfg.SnapshotDir, "old.tar.gz")
	newArtifact := filepath.Join(cfg.ArtifactDir, "new.tar")
	for _, path := range []string{oldArtifact, oldSnapshot, newArtifact} {
		if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	oldTime := time.Now().Add(-48 * time.Hour)
	for _, path := range []string{oldArtifact, oldSnapshot} {
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatalf("age %s: %v", path, err)
		}
	}

	cleanupStaleArtifacts(cfg)
	for _, path := range []string{oldArtifact, oldSnapshot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected old file %s removed, got %v", path, err)
		}
	}
	if _, err := os.Stat(newArtifact); err != nil {
		t.Fatalf("expected new artifact to remain: %v", err)
	}
}

func TestArtifactStorageCanBeInjected(t *testing.T) {
	oldStorage := artifactStorage
	artifactStorage = &fakeArtifactStorage{
		artifactPrefix: "mem-artifact/",
		snapshotPrefix: "mem-snapshot/",
		content:        []byte("artifact-data"),
	}
	store := artifactStorage.(*fakeArtifactStorage)
	t.Cleanup(func() {
		artifactStorage = oldStorage
	})

	if err := ensureRuntimeDirs(AppConfig{}); err != nil {
		t.Fatalf("ensure runtime dirs through fake storage: %v", err)
	}
	if !store.ensured {
		t.Fatal("expected fake storage Ensure to be called")
	}

	if got := managedArtifactPath("build.tar"); got != "mem-artifact/build.tar" {
		t.Fatalf("managed artifact path = %q", got)
	}
	if got := managedSnapshotPath("snapshot.tar.gz"); got != "mem-snapshot/snapshot.tar.gz" {
		t.Fatalf("managed snapshot path = %q", got)
	}
	if !isManagedArtifactPath("mem-artifact/build.tar") || isManagedArtifactPath("/tmp/build.tar") {
		t.Fatal("expected fake managed path checks to be used")
	}

	tmpPath, out, err := prepareManagedArtifactUpload("mem-snapshot/snapshot.tar.gz")
	if err != nil {
		t.Fatalf("prepare upload: %v", err)
	}
	if _, err := out.Write([]byte("upload")); err != nil {
		t.Fatalf("write fake upload: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("close fake upload: %v", err)
	}
	if err := commitManagedArtifactUpload(tmpPath, "mem-snapshot/snapshot.tar.gz"); err != nil {
		t.Fatalf("commit upload: %v", err)
	}
	if store.committedTmp != tmpPath || store.committedPath != "mem-snapshot/snapshot.tar.gz" {
		t.Fatalf("unexpected commit paths tmp=%q path=%q", store.committedTmp, store.committedPath)
	}
	abortManagedArtifactUpload(tmpPath)
	if store.abortedTmp != tmpPath {
		t.Fatalf("expected abort tmp %q, got %q", tmpPath, store.abortedTmp)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/artifact", nil)
	if err := serveManagedArtifact(rr, req, "mem-artifact/build.tar"); err != nil {
		t.Fatalf("serve artifact: %v", err)
	}
	if rr.Body.String() != "artifact-data" {
		t.Fatalf("unexpected served body %q", rr.Body.String())
	}

	removeManagedArtifact("mem-artifact/build.tar")
	if store.removedPath != "mem-artifact/build.tar" {
		t.Fatalf("expected remove through fake storage, got %q", store.removedPath)
	}
	cleanupStaleArtifacts(AppConfig{ArtifactRetentionHours: 2})
	if store.cleanupMaxAge != 2*time.Hour {
		t.Fatalf("expected cleanup max age 2h, got %s", store.cleanupMaxAge)
	}
}

func TestFileSHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact")
	if err := os.WriteFile(path, []byte("hello"), 0644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	got, err := fileSHA256(path)
	if err != nil {
		t.Fatalf("hash artifact: %v", err)
	}
	const want = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got != want {
		t.Fatalf("sha256 = %q, want %q", got, want)
	}
}

type fakeArtifactStorage struct {
	artifactPrefix string
	snapshotPrefix string
	content        []byte

	ensured       bool
	removedPath   string
	cleanupMaxAge time.Duration
	committedTmp  string
	committedPath string
	abortedTmp    string
}

func (s *fakeArtifactStorage) Ensure() error {
	s.ensured = true
	return nil
}

func (s *fakeArtifactStorage) ArtifactPath(filename string) string {
	return s.artifactPrefix + filename
}

func (s *fakeArtifactStorage) SnapshotPath(filename string) string {
	return s.snapshotPrefix + filename
}

func (s *fakeArtifactStorage) IsManagedPath(path string) bool {
	return strings.HasPrefix(path, s.artifactPrefix) || strings.HasPrefix(path, s.snapshotPrefix)
}

func (s *fakeArtifactStorage) Remove(path string) error {
	s.removedPath = path
	return nil
}

func (s *fakeArtifactStorage) Cleanup(maxAge time.Duration) (int, error) {
	s.cleanupMaxAge = maxAge
	return 1, nil
}

func (s *fakeArtifactStorage) Stat(path string) (os.FileInfo, error) {
	return fakeArtifactFileInfo{size: int64(len(s.content)), modTime: time.Unix(1700000000, 0)}, nil
}

func (s *fakeArtifactStorage) Open(path string) (readSeekCloser, error) {
	return nopReadSeekCloser{Reader: bytes.NewReader(s.content)}, nil
}

func (s *fakeArtifactStorage) PrepareUpload(path string) (string, io.WriteCloser, error) {
	return path + ".uploading", &bufferWriteCloser{}, nil
}

func (s *fakeArtifactStorage) CommitUpload(tmpPath, path string) error {
	s.committedTmp = tmpPath
	s.committedPath = path
	return nil
}

func (s *fakeArtifactStorage) AbortUpload(tmpPath string) {
	s.abortedTmp = tmpPath
}

type nopReadSeekCloser struct {
	*bytes.Reader
}

func (r nopReadSeekCloser) Close() error {
	return nil
}

type bufferWriteCloser struct {
	bytes.Buffer
}

func (w *bufferWriteCloser) Close() error {
	return nil
}

type fakeArtifactFileInfo struct {
	size    int64
	modTime time.Time
}

func (i fakeArtifactFileInfo) Name() string       { return "artifact" }
func (i fakeArtifactFileInfo) Size() int64        { return i.size }
func (i fakeArtifactFileInfo) Mode() os.FileMode  { return 0644 }
func (i fakeArtifactFileInfo) ModTime() time.Time { return i.modTime }
func (i fakeArtifactFileInfo) IsDir() bool        { return false }
func (i fakeArtifactFileInfo) Sys() interface{}   { return nil }
