package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeployerInstanceLockEnforcesSingleDatabaseOwner(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "deployer.db")
	first, err := acquireDeployerInstanceLock(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireDeployerInstanceLock(dbPath); err == nil {
		first.Close()
		t.Fatal("second Deployer instance acquired the same database lock")
	}
	symlinkPath := filepath.Join(filepath.Dir(dbPath), "deployer-alias.db")
	if err := os.Symlink(dbPath, symlinkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireDeployerInstanceLock(symlinkPath); err == nil {
		first.Close()
		t.Fatal("database symlink alias bypassed the instance lock")
	}
	hardlinkPath := filepath.Join(filepath.Dir(dbPath), "deployer-hardlink.db")
	if err := os.Link(dbPath, hardlinkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireDeployerInstanceLock(hardlinkPath); err == nil {
		first.Close()
		t.Fatal("database hard-link alias bypassed the instance lock")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := acquireDeployerInstanceLock(symlinkPath)
	if err != nil {
		t.Fatalf("database lock was not released: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireDeployerInstanceLock(":memory:"); err == nil {
		t.Fatal("in-memory database was accepted without singleton ownership")
	}
}
