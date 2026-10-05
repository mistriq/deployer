package app

import (
	"archive/tar"
	"os"
	"path/filepath"
	"testing"
)

func releaseArchive(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".tar.gz")
	if err := writeTestTarGz(path, func(tw *tar.Writer) error {
		body := []byte(content)
		if err := tw.WriteHeader(&tar.Header{Name: "index.txt", Mode: 0644, Size: int64(len(body))}); err != nil {
			return err
		}
		_, err := tw.Write(body)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFilesReleaseActivateAndRollbackKeepsLatestMutableData(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "app")
	if err := os.MkdirAll(filepath.Join(deploy, "uploads"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deploy, "uploads", "customer.txt"), []byte("initial"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 1, releaseArchive(t, "a", "release-a"), []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 1, []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deploy, "uploads", "customer.txt"), []byte("latest"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 2, releaseArchive(t, "b", "release-b"), []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 2, []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "index.txt")); string(got) != "release-b" {
		t.Fatalf("active=%q", got)
	}
	if err := activateFilesRelease(deploy, 1, []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "index.txt")); string(got) != "release-a" {
		t.Fatalf("rollback=%q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "uploads", "customer.txt")); string(got) != "latest" {
		t.Fatalf("mutable=%q", got)
	}
}

func TestFilesReleaseMalformedArchiveLeavesActiveUnchanged(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "app")
	if err := prepareFilesRelease(deploy, 1, releaseArchive(t, "a", "release-a"), nil); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 1, nil); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(t.TempDir(), "bad.tar.gz")
	if err := writeTestTarGz(bad, func(tw *tar.Writer) error { return tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0644, Size: 0}) }); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 2, bad, nil); err == nil {
		t.Fatal("expected unsafe archive rejection")
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "index.txt")); string(got) != "release-a" {
		t.Fatalf("active changed=%q", got)
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(deploy), "escape")); !os.IsNotExist(err) {
		t.Fatalf("archive escaped: %v", err)
	}
	symlinkArchive := filepath.Join(t.TempDir(), "symlink.tar.gz")
	if err := writeTestTarGz(symlinkArchive, func(tw *tar.Writer) error {
		return tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd", Mode: 0777})
	}); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 3, symlinkArchive, nil); err == nil {
		t.Fatal("expected symlink archive rejection")
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "index.txt")); string(got) != "release-a" {
		t.Fatalf("active changed after symlink=%q", got)
	}
}

func TestFilesReleaseMigratesExistingDeployment(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "legacy")
	if err := os.MkdirAll(filepath.Join(deploy, "uploads"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deploy, "uploads", "customer.txt"), []byte("legacy-data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 7, releaseArchive(t, "new", "new-code"), []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 7, []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(deploy); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("live path not activated symlink: %v %v", info, err)
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "uploads", "customer.txt")); string(got) != "legacy-data" {
		t.Fatalf("legacy mutable=%q", got)
	}
	layout, _ := newFilesReleaseLayout(deploy)
	if got, _ := os.ReadFile(filepath.Join(layout.root, "pre-release-deployment", "uploads", "customer.txt")); string(got) != "legacy-data" {
		t.Fatalf("legacy backup=%q", got)
	}
}

func TestFilesReleaseRejectsOverlappingMutablePaths(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "app")
	if err := prepareFilesRelease(deploy, 1, releaseArchive(t, "release", "code"), []string{"uploads", "uploads/images"}); err == nil {
		t.Fatal("expected overlapping paths rejection")
	}
	if _, err := os.Lstat(deploy); !os.IsNotExist(err) {
		t.Fatalf("deployment changed: %v", err)
	}
}

func TestFailedInitialPreparationDoesNotSnapshotStaleMutableData(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "app")
	if err := os.MkdirAll(filepath.Join(deploy, "uploads"), 0755); err != nil {
		t.Fatal(err)
	}
	customer := filepath.Join(deploy, "uploads", "customer.txt")
	if err := os.WriteFile(customer, []byte("before"), 0644); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(t.TempDir(), "bad.tar.gz")
	if err := writeTestTarGz(bad, func(tw *tar.Writer) error { return tw.WriteHeader(&tar.Header{Name: "../bad", Mode: 0644}) }); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 1, bad, []string{"uploads"}); err == nil {
		t.Fatal("expected invalid archive")
	}
	if err := os.WriteFile(customer, []byte("after-failure"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 2, releaseArchive(t, "valid", "code"), []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(customer, []byte("after-prepare"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 2, []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "uploads", "customer.txt")); string(got) != "after-prepare" {
		t.Fatalf("stale mutable data=%q", got)
	}
}

func TestMissingOptionalMutableFileRemainsCreatableAsFile(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "app")
	if err := prepareFilesRelease(deploy, 1, releaseArchive(t, "a", "code"), []string{"data.json"}); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 1, []string{"data.json"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(deploy, "data.json")
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("optional path fabricated: %v", err)
	}
	if err := os.WriteFile(path, []byte("{}"), 0644); err != nil {
		t.Fatalf("create optional file: %v", err)
	}
	if err := prepareFilesRelease(deploy, 2, releaseArchive(t, "b", "code2"), []string{"data.json"}); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 2, []string{"data.json"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(deploy, "data.json")); string(got) != "{}" {
		t.Fatalf("optional data=%q", got)
	}
}

func TestFilesReleaseRejectsUnmanagedDeploymentSymlink(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(base, "outside")
	if err := os.Mkdir(outside, 0755); err != nil {
		t.Fatal(err)
	}
	deploy := filepath.Join(base, "app")
	if err := os.Symlink(outside, deploy); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 1, releaseArchive(t, "a", "code"), []string{"uploads"}); err != nil {
		t.Fatal(err)
	}
	if err := activateFilesRelease(deploy, 1, []string{"uploads"}); err == nil {
		t.Fatal("expected unmanaged symlink rejection")
	}
	target, _ := os.Readlink(deploy)
	if target != outside {
		t.Fatalf("symlink changed=%q", target)
	}
}

func TestPrepareFilesReleaseIsIdempotentForSameArtifact(t *testing.T) {
	deploy := filepath.Join(t.TempDir(), "app")
	archive := releaseArchive(t, "a", "code")
	if err := prepareFilesRelease(deploy, 1, archive, nil); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 1, archive, nil); err != nil {
		t.Fatal(err)
	}
	if err := prepareFilesRelease(deploy, 1, releaseArchive(t, "different", "other"), nil); err == nil {
		t.Fatal("expected changed artifact rejection")
	}
}
