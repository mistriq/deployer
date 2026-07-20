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

func TestHostingAgentInstanceLockSerializesWorkRootAliases(t *testing.T) {
	root := t.TempDir()
	first, err := acquireHostingAgentInstanceLock(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := acquireHostingAgentInstanceLock(root); err == nil {
		t.Fatal("second hosting agent acquired the same work root")
	}
	symlink := filepath.Join(t.TempDir(), "agent-root")
	if err := os.Symlink(root, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireHostingAgentInstanceLock(symlink); err == nil {
		t.Fatal("work-root symlink bypassed the hosting-agent instance lock")
	}
}

func TestCanonicalHostingAgentWorkRootPreservesSecretNamespaceAcrossSymlinkAlias(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(t.TempDir(), "agent-root")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	canonicalRoot, err := canonicalHostingAgentWorkRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	canonicalAlias, err := canonicalHostingAgentWorkRoot(alias)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalRoot != canonicalAlias {
		t.Fatalf("work-root alias resolved to a different identity: %q != %q", canonicalRoot, canonicalAlias)
	}
	if hostingAgentSecretMemoryRoot(canonicalRoot) != hostingAgentSecretMemoryRoot(canonicalAlias) {
		t.Fatal("work-root alias selected a different secret namespace")
	}
}
