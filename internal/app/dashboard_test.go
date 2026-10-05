package app

import (
	"strings"
	"testing"
)

func TestDashboardBuildMetadata(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) {
		withTempDB(t)
		checkDashboardBuildMetadata(t)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		u := isolatedPostgres(t, postgresTestURL(t))
		oldDB := db
		t.Cleanup(func() { db.Close(); db = oldDB })
		if err := initPostgres(u); err != nil {
			t.Fatal(err)
		}
		checkDashboardBuildMetadata(t)
	})
}

func checkDashboardBuildMetadata(t *testing.T) {
	t.Helper()
	busy := createHistoryProject(t, "Alpha")
	empty := createHistoryProject(t, "Empty")
	other := createHistoryProject(t, "Other")
	for i := 0; i < 27; i++ {
		build := createHistoryBuild(t, busy.ID, "success", i+1)
		if _, err := db.Exec(`UPDATE builds SET log=? WHERE id=?`, strings.Repeat("unused build log", 10000), build.ID); err != nil {
			t.Fatal(err)
		}
	}
	failed := createHistoryBuild(t, other.ID, "failed", 11)
	latest := createHistoryBuild(t, busy.ID, "failed", 38)
	projects, err := listProjectsWithLastBuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 3 || projects[0].LastBuild.ID != latest.ID || projects[1].ID != empty.ID ||
		projects[1].LastBuild != nil || projects[2].LastBuild.ID != failed.ID {
		t.Fatalf("wrong project ordering or latest build: %+v", projects)
	}
	for _, project := range projects {
		if project.LastBuild != nil && project.LastBuild.Log != "" {
			t.Fatal("project listing included a build log")
		}
	}
	view, err := newDashboardView(projects)
	if err != nil {
		t.Fatal(err)
	}
	if view.Idle != 1 || view.Failing != 2 || view.Deploying != 0 {
		t.Fatalf("wrong dashboard counts: %+v", view)
	}
	if history := view.Rows[0].History; history.Counts.Total != 24 || history.Counts.Failed != 1 || history.MaxDurationSeconds != 38 {
		t.Fatalf("wrong bounded history: %+v", history)
	}
	if len(view.Rows[1].History.Points) != 0 || len(view.Rows[2].History.Points) != 1 {
		t.Fatal("build histories crossed project boundaries")
	}
	if got := view.Rows[0].LastBuild; got.Log != "" || got.FinishedAt == nil || got.DurationSeconds == nil || *got.DurationSeconds != 38 {
		t.Fatalf("missing metadata or unexpected log: %+v", got)
	}
}
