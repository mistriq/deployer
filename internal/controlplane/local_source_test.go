package controlplane

import "testing"

func TestLocalSourceOnlyAcceptsDirectGitChildrenOfRoot(t *testing.T) {
	root := "/srv/portal/sources"
	for repository, want := range map[string]bool{
		"/srv/portal/sources/prj_1.git":     true,
		"/srv/portal/sources/prj_1":         false,
		"/srv/portal/sources/../etc/x.git":  false,
		"/srv/portal/sources/a/prj_1.git":   false,
		"/srv/portal/sources/.hidden.git":   false,
		"/srv/portal/sourcesevil/prj_1.git": false,
		"relative/prj_1.git":                false,
		"https://github.com/acme/site":      false,
		"/srv/portal/sources/prj_1.git/":    false,
	} {
		if got := localSource(root, repository); got != want {
			t.Errorf("localSource(%q) = %v, want %v", repository, got, want)
		}
	}
	if localSource("", "/srv/portal/sources/prj_1.git") {
		t.Fatal("local sources must be disabled when no root is configured")
	}
}
