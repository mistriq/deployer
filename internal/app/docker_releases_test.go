package app

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDockerActivationRestoresExactConfigAndImageWithoutTouchingVolume(t *testing.T) {
	deploy := t.TempDir()
	compose := filepath.Join(deploy, "compose.yml")
	env := filepath.Join(deploy, "deploy.env")
	volume := filepath.Join(deploy, "named-volume", "customer.db")
	if err := os.MkdirAll(filepath.Dir(volume), 0755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(volume, []byte("customer-data"), 0644)
	imageA := filepath.Join(t.TempDir(), "a.tar")
	_ = os.WriteFile(imageA, []byte("image-a"), 0600)
	shaA, _ := fileSHA256(imageA)
	_ = os.WriteFile(compose, []byte("services:\n  app:\n    image: app:a\n"), 0600)
	_ = os.WriteFile(env, []byte("VERSION=A\n"), 0600)
	snapshotA, err := captureDockerActivation(deploy, 1, imageA, shaA, "compose.yml", []string{"deploy.env"})
	if err != nil {
		t.Fatal(err)
	}
	imageB := filepath.Join(t.TempDir(), "b.tar")
	_ = os.WriteFile(imageB, []byte("image-b"), 0600)
	shaB, _ := fileSHA256(imageB)
	_ = os.WriteFile(compose, []byte("services:\n  app:\n    image: app:b\n"), 0600)
	_ = os.WriteFile(env, []byte("VERSION=B\n"), 0600)
	if _, err := captureDockerActivation(deploy, 2, imageB, shaB, "compose.yml", []string{"deploy.env"}); err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	run := func(ctx context.Context, dir string, args ...string) error {
		calls = append(calls, append([]string{dir}, args...))
		return nil
	}
	if err := activateDockerSnapshot(context.Background(), deploy, snapshotA, run); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(compose); string(got) != "services:\n  app:\n    image: app:a\n" {
		t.Fatalf("compose=%q", got)
	}
	if got, _ := os.ReadFile(env); string(got) != "VERSION=A\n" {
		t.Fatalf("env=%q", got)
	}
	if got, _ := os.ReadFile(volume); string(got) != "customer-data" {
		t.Fatalf("volume=%q", got)
	}
	want := [][]string{{"", "docker", "load", "-i", imageA}, {deploy, "docker", "compose", "-f", "compose.yml", "up", "-d"}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%#v", calls)
	}
}

func TestDockerActivationRejectsChangedImageBeforeConfigMutation(t *testing.T) {
	deploy := t.TempDir()
	compose := filepath.Join(deploy, "compose.yml")
	image := filepath.Join(t.TempDir(), "image.tar")
	_ = os.WriteFile(compose, []byte("version-a"), 0600)
	_ = os.WriteFile(image, []byte("image-a"), 0600)
	sha, _ := fileSHA256(image)
	snapshot, err := captureDockerActivation(deploy, 1, image, sha, "compose.yml", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(compose, []byte("live-b"), 0600)
	_ = os.WriteFile(image, []byte("tampered"), 0600)
	called := false
	err = activateDockerSnapshot(context.Background(), deploy, snapshot, func(context.Context, string, ...string) error { called = true; return nil })
	if err == nil {
		t.Fatal("expected checksum error")
	}
	if called {
		t.Fatal("command ran")
	}
	if got, _ := os.ReadFile(compose); string(got) != "live-b" {
		t.Fatalf("config changed=%q", got)
	}
}

func TestDockerActivationSnapshotIsPrivate(t *testing.T) {
	deploy := t.TempDir()
	_ = os.WriteFile(filepath.Join(deploy, "compose.yml"), []byte("secret"), 0600)
	image := filepath.Join(t.TempDir(), "image.tar")
	_ = os.WriteFile(image, []byte("image"), 0600)
	sha, _ := fileSHA256(image)
	path, err := captureDockerActivation(deploy, 1, image, sha, "compose.yml", nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
}
