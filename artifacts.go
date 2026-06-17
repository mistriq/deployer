package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type readSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type ArtifactStorage interface {
	Ensure() error
	ArtifactPath(filename string) string
	SnapshotPath(filename string) string
	IsManagedPath(path string) bool
	Remove(path string) error
	Cleanup(maxAge time.Duration) (int, error)
	Stat(path string) (os.FileInfo, error)
	Open(path string) (readSeekCloser, error)
	PrepareUpload(path string) (string, io.WriteCloser, error)
	CommitUpload(tmpPath, path string) error
	AbortUpload(tmpPath string)
}

type localArtifactStorage struct {
	artifactDir string
	snapshotDir string
}

var artifactStorage ArtifactStorage

func configureArtifactStorage(cfg AppConfig) {
	artifactStorage = newLocalArtifactStorage(cfg)
}

func currentArtifactStorage() ArtifactStorage {
	if artifactStorage != nil {
		return artifactStorage
	}
	return newLocalArtifactStorage(appConfig)
}

func artifactStorageForConfig(cfg AppConfig) ArtifactStorage {
	if artifactStorage != nil {
		return artifactStorage
	}
	return newLocalArtifactStorage(cfg)
}

func newLocalArtifactStorage(cfg AppConfig) localArtifactStorage {
	return localArtifactStorage{
		artifactDir: cfg.ArtifactDir,
		snapshotDir: cfg.SnapshotDir,
	}
}

func ensureRuntimeDirs(cfg AppConfig) error {
	return artifactStorageForConfig(cfg).Ensure()
}

func (s localArtifactStorage) Ensure() error {
	for _, dir := range []string{s.artifactDir, s.snapshotDir} {
		if strings.TrimSpace(dir) == "" {
			return fmt.Errorf("runtime directory is empty")
		}
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("create runtime directory %s: %w", dir, err)
		}
	}
	return nil
}

func managedArtifactPath(filename string) string {
	return currentArtifactStorage().ArtifactPath(filename)
}

func managedSnapshotPath(filename string) string {
	return currentArtifactStorage().SnapshotPath(filename)
}

func (s localArtifactStorage) ArtifactPath(filename string) string {
	return filepath.Join(s.artifactDir, safeFileName(filename))
}

func (s localArtifactStorage) SnapshotPath(filename string) string {
	return filepath.Join(s.snapshotDir, safeFileName(filename))
}

func safeFileName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "artifact"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	name := strings.Trim(b.String(), ".-")
	if name == "" {
		return "artifact"
	}
	return name
}

func isManagedArtifactPath(path string) bool {
	return currentArtifactStorage().IsManagedPath(path)
}

func (s localArtifactStorage) IsManagedPath(path string) bool {
	if path == "" {
		return false
	}
	return pathWithinBase(s.artifactDir, path) || pathWithinBase(s.snapshotDir, path)
}

func pathWithinBase(base, path string) bool {
	if strings.TrimSpace(base) == "" || strings.TrimSpace(path) == "" {
		return false
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absBase, absPath)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func removeManagedArtifact(path string) {
	logOperationalError("remove artifact", currentArtifactStorage().Remove(path))
}

func (s localArtifactStorage) Remove(path string) error {
	if path == "" {
		return nil
	}
	if !s.IsManagedPath(path) {
		return fmt.Errorf("refusing to remove unmanaged artifact path %s", path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func cleanupStaleArtifacts(cfg AppConfig) {
	maxAge := retentionDuration(cfg.ArtifactRetentionHours)
	if maxAge == 0 {
		return
	}
	removed, err := artifactStorageForConfig(cfg).Cleanup(maxAge)
	if err != nil {
		logOperationalError("cleanup stale artifacts", err)
		return
	}
	if removed > 0 {
		logOperationalInfo("Removed %d stale artifact files", removed)
	}
}

func (s localArtifactStorage) Cleanup(maxAge time.Duration) (int, error) {
	total := 0
	for _, dir := range []string{s.artifactDir, s.snapshotDir} {
		removed, err := removeFilesOlderThan(dir, maxAge)
		if err != nil {
			return total, err
		}
		total += removed
	}
	return total, nil
}

func removeFilesOlderThan(dir string, maxAge time.Duration) (int, error) {
	if strings.TrimSpace(dir) == "" {
		return 0, nil
	}
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			removed++
		}
		return nil
	})
	return removed, err
}

func managedArtifactInfo(path string) (os.FileInfo, error) {
	if !isManagedArtifactPath(path) {
		return nil, fmt.Errorf("artifact path is not managed")
	}
	return currentArtifactStorage().Stat(path)
}

func (s localArtifactStorage) Stat(path string) (os.FileInfo, error) {
	if !s.IsManagedPath(path) {
		return nil, fmt.Errorf("artifact path is not managed")
	}
	return os.Stat(path)
}

func serveManagedArtifact(w http.ResponseWriter, r *http.Request, path string) error {
	if !isManagedArtifactPath(path) {
		return fmt.Errorf("artifact path is not managed")
	}
	f, err := currentArtifactStorage().Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := currentArtifactStorage().Stat(path)
	if err != nil {
		return err
	}
	http.ServeContent(w, r, filepath.Base(path), info.ModTime(), f)
	return nil
}

func (s localArtifactStorage) Open(path string) (readSeekCloser, error) {
	if !s.IsManagedPath(path) {
		return nil, fmt.Errorf("artifact path is not managed")
	}
	return os.Open(path)
}

func prepareManagedArtifactUpload(path string) (string, io.WriteCloser, error) {
	if !isManagedArtifactPath(path) {
		return "", nil, fmt.Errorf("artifact path is not managed")
	}
	return currentArtifactStorage().PrepareUpload(path)
}

func (s localArtifactStorage) PrepareUpload(path string) (string, io.WriteCloser, error) {
	if !s.IsManagedPath(path) {
		return "", nil, fmt.Errorf("artifact path is not managed")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", nil, err
	}
	tmpPath := filepath.Join(filepath.Dir(path), filepath.Base(path)+".uploading")
	out, err := os.Create(tmpPath)
	if err != nil {
		return "", nil, err
	}
	return tmpPath, out, nil
}

func commitManagedArtifactUpload(tmpPath, path string) error {
	if !isManagedArtifactPath(path) {
		return fmt.Errorf("artifact path is not managed")
	}
	return currentArtifactStorage().CommitUpload(tmpPath, path)
}

func (s localArtifactStorage) CommitUpload(tmpPath, path string) error {
	if !s.IsManagedPath(path) || !s.IsManagedPath(tmpPath) {
		return fmt.Errorf("artifact path is not managed")
	}
	return os.Rename(tmpPath, path)
}

func abortManagedArtifactUpload(tmpPath string) {
	currentArtifactStorage().AbortUpload(tmpPath)
}

func (s localArtifactStorage) AbortUpload(tmpPath string) {
	if tmpPath == "" || !s.IsManagedPath(tmpPath) {
		return
	}
	_ = os.Remove(tmpPath)
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
