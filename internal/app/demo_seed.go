package app

import (
	"fmt"
	"time"
)

func seedDemoDataIfEnabled(cfg AppConfig) error {
	if !cfg.DemoMode {
		return nil
	}
	empty, err := databaseHasNoUserData()
	if err != nil {
		return err
	}
	if !empty {
		logStructured("info", "demo_seed_skipped", map[string]interface{}{
			"reason": "database already contains user data",
		})
		return nil
	}
	if err := seedDemoData(); err != nil {
		return err
	}
	logStructured("info", "demo_seeded", nil)
	return nil
}

func databaseHasNoUserData() (bool, error) {
	var projects, runners, builds int
	if err := db.QueryRow(`SELECT COUNT(*) FROM projects`).Scan(&projects); err != nil {
		return false, err
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM runners`).Scan(&runners); err != nil {
		return false, err
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM builds`).Scan(&builds); err != nil {
		return false, err
	}
	return projects == 0 && runners == 0 && builds == 0, nil
}

func seedDemoData() error {
	runner := &Runner{
		Name:   "demo-runner-1",
		Token:  "demo-runner-token",
		Labels: "linux,docker,staging",
	}
	if err := createRunner(runner); err != nil {
		return fmt.Errorf("create demo runner: %w", err)
	}
	if err := updateRunnerHeartbeat(runner.ID); err != nil {
		return fmt.Errorf("mark demo runner online: %w", err)
	}

	offlineRunner := &Runner{
		Name:   "demo-runner-2",
		Token:  "demo-runner-token-2",
		Labels: "linux,files,production",
	}
	if err := createRunner(offlineRunner); err != nil {
		return fmt.Errorf("create offline demo runner: %w", err)
	}

	api := &Project{
		Name:               "Demo API",
		RepoPath:           "/srv/demo/api",
		DockerfilePath:     "Dockerfile",
		ComposeFile:        "compose.yaml",
		ImageName:          "registry.example.com/demo/api",
		DeployDir:          "/srv/demo/api",
		HealthURL:          "https://api.demo.example.com/health",
		HealthContainer:    "demo-api-web-1",
		BuildArgs:          map[string]string{"APP_ENV": "demo", "GIT_SHA": "${GIT_SHA}"},
		ComposeServices:    "web worker",
		GitPullBeforeBuild: true,
		RunnerID:           runner.ID,
		DeployMode:         "docker",
		Preserve:           ".env\nuploads",
	}
	if err := createProject(api); err != nil {
		return fmt.Errorf("create demo API project: %w", err)
	}
	if err := seedDemoBuild(api.ID, "success", "9f3a7c1", "demo", 96, "Deploy completed successfully"); err != nil {
		return err
	}
	if err := seedDemoBuild(api.ID, "failed", "1c4e2b8", "webhook", 41, "Health check failed: demo-api-web-1 not healthy"); err != nil {
		return err
	}

	site := &Project{
		Name:               "Demo Static Site",
		RepoPath:           "/srv/demo/site",
		DockerfilePath:     "Dockerfile",
		ComposeFile:        "compose.yaml",
		DeployDir:          "/var/www/demo-site",
		HealthURL:          "https://demo.example.com/health",
		BuildArgs:          map[string]string{},
		ComposeServices:    "web",
		GitPullBeforeBuild: true,
		RunnerID:           offlineRunner.ID,
		DeployMode:         "files",
		PostDeploy:         "systemctl reload demo-site",
		Permissions:        `{"files":{"*.html":"0644"},"dirs":{"assets":"0755"}}`,
		Preserve:           ".env\nuploads",
	}
	if err := createProject(site); err != nil {
		return fmt.Errorf("create demo static site project: %w", err)
	}
	if err := seedDemoBuild(site.ID, "success", "6b7d901", "manual", 24, "Files deployed successfully"); err != nil {
		return err
	}
	return nil
}

func seedDemoBuild(projectID int64, status, commitSHA, triggeredBy string, durationSeconds int, message string) error {
	build, err := createBuild(projectID, triggeredBy)
	if err != nil {
		return fmt.Errorf("create demo build: %w", err)
	}
	finished := time.Now().UTC()
	build.Status = status
	build.CommitSHA = commitSHA
	build.FinishedAt = &finished
	build.DurationSeconds = &durationSeconds
	build.Log = fmt.Sprintf("##[group]Demo deploy\n%s\n##[endgroup:%d]\n", message, durationSeconds)
	if status == "failed" {
		build.ErrorMessage = message
	}
	if err := updateBuild(build); err != nil {
		return fmt.Errorf("update demo build: %w", err)
	}
	if _, err := createBuildAnnotation(build.ID, "Demo data for public screenshots"); err != nil {
		return fmt.Errorf("annotate demo build: %w", err)
	}
	return nil
}
