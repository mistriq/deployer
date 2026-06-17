package app

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

//go:embed web/*
var webFS embed.FS

// Build version — set at compile time via -ldflags
var buildVersion = "dev"

var (
	appConfig AppConfig
	broker    *SSEBroker
	builder   *Builder
	tmpl      *template.Template
)

func Run() {
	// Agent subcommand
	if len(os.Args) >= 2 && os.Args[1] == "agent" {
		runAgent()
		return
	}

	appConfig = loadConfig()
	configureArtifactStorage(appConfig)
	if err := ensureRuntimeDirs(appConfig); err != nil {
		logFatal("startup_error", "failed to prepare runtime directories", err, nil)
	}

	// Init database
	if err := initDB(appConfig.DBPath); err != nil {
		logFatal("startup_error", "failed to init database", err, map[string]interface{}{
			"db_path": appConfig.DBPath,
		})
	}
	defer db.Close()
	cleanupRuntimeState(appConfig)
	if err := seedDemoDataIfEnabled(appConfig); err != nil {
		logFatal("startup_error", "failed to seed demo data", err, nil)
	}

	// Init SSE broker and builder
	broker = NewSSEBroker()
	builder = NewBuilder(broker)

	// Background: mark stale runners as offline
	go func() {
		for {
			time.Sleep(30 * time.Second)
			logOperationalError("mark stale runners", markStaleRunners())
		}
	}()
	go func() {
		for {
			time.Sleep(6 * time.Hour)
			cleanupRuntimeState(appConfig)
		}
	}()

	// Parse templates
	var err error
	tmpl, err = template.New("").Funcs(template.FuncMap{
		"json": func(v interface{}) template.JS {
			b, _ := json.Marshal(v)
			return template.JS(b)
		},
		"statusColor": func(status string) string {
			switch status {
			case "success":
				return "#22c55e"
			case "failed":
				return "#ef4444"
			case "running":
				return "#3b82f6"
			case "cancelled":
				return "#f59e0b"
			default:
				return "#6b7280"
			}
		},
		"statusIcon": func(status string) string {
			switch status {
			case "success":
				return "check"
			case "failed":
				return "x"
			case "running":
				return "running"
			case "cancelled":
				return "cancelled"
			default:
				return "pending"
			}
		},
		"formatDuration": func(seconds *int) string {
			if seconds == nil {
				return "-"
			}
			s := *seconds
			if s < 60 {
				return fmt.Sprintf("%ds", s)
			}
			return fmt.Sprintf("%dm %ds", s/60, s%60)
		},
		"shortSHA": func(sha string) string {
			if len(sha) > 7 {
				return sha[:7]
			}
			return sha
		},
		"b64": func(s string) template.JS {
			return template.JS(`"` + base64.StdEncoding.EncodeToString([]byte(s)) + `"`)
		},
		"b64str": func(s string) string {
			return base64.StdEncoding.EncodeToString([]byte(s))
		},
		"securityStatus": func() SecurityStatus {
			return securityStatus(appConfig)
		},
		"compactPath": compactPath,
	}).ParseFS(webFS, "web/*.html")
	if err != nil {
		logFatal("startup_error", "failed to parse templates", err, nil)
	}

	// Static files — no-cache headers for development
	staticFS, _ := fs.Sub(webFS, "web/static")
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.FS(staticFS)))
	mux := http.NewServeMux()
	mux.Handle("/static/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		staticHandler.ServeHTTP(w, r)
	}))

	// Download endpoint — serve the deployer binary itself
	mux.HandleFunc("/download/deployer", handleDownloadBinary)
	mux.HandleFunc("/api/capabilities", handleAPICapabilities)
	mux.HandleFunc("/api/version", func(w http.ResponseWriter, r *http.Request) {
		response := map[string]string{"version": buildVersion}
		if checksum, err := currentBinaryChecksum(); err == nil {
			response["checksum_sha256"] = checksum
		} else {
			logOperationalError("calculate binary checksum", err)
		}
		jsonResponse(w, response)
	})

	// HTML routes
	mux.HandleFunc("/", handleDashboard)
	mux.HandleFunc("/projects/", handleProjectPage)
	mux.HandleFunc("/builds/", handleBuildPage)
	mux.HandleFunc("/runners", handleRunnersPage)
	mux.HandleFunc("/runners/", handleRunnersPage)

	// API routes
	mux.HandleFunc("/api/projects", handleAPIProjects)
	mux.HandleFunc("/api/projects/", handleAPIProject)
	mux.HandleFunc("/api/builds/", handleAPIBuild)
	mux.HandleFunc("/api/runners", handleAPIRunners)
	mux.HandleFunc("/api/runners/", handleAPIRunner)

	// Agent API routes
	mux.HandleFunc("/api/agent/poll", handleAgentPoll)
	mux.HandleFunc("/api/agent/artifact/", handleAgentArtifact)
	mux.HandleFunc("/api/agent/snapshot/", handleAgentSnapshotUpload)
	mux.HandleFunc("/api/agent/log/", handleAgentLog)
	mux.HandleFunc("/api/agent/complete/", handleAgentComplete)
	mux.HandleFunc("/api/agent/heartbeat", handleAgentHeartbeat)

	server := &http.Server{
		Addr:              appConfig.Addr,
		Handler:           wrapHTTPHandler(appConfig, mux),
		ReadTimeout:       appConfig.ServerReadTimeout,
		WriteTimeout:      appConfig.ServerWriteTimeout,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	if appConfig.AdminPassword == "" {
		logStructured("warn", "admin_auth_disabled", map[string]interface{}{
			"addr": appConfig.Addr,
		})
	}
	logStructured("info", "server_started", map[string]interface{}{
		"addr": appConfig.Addr,
	})
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logFatal("server_error", "server stopped unexpectedly", err, map[string]interface{}{
				"addr": appConfig.Addr,
			})
		}
	case sig := <-stop:
		logStructured("info", "server_shutdown_started", map[string]interface{}{
			"signal": sig.String(),
		})
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if builder != nil {
			builder.Shutdown(shutdownCtx)
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			logOperationalError("shutdown server", err)
		}
	}
}

func cleanupRuntimeState(cfg AppConfig) {
	cleanupStaleArtifacts(cfg)
	if cfg.LogRetentionDays > 0 {
		affected, err := cleanupOldBuildLogs(cfg.LogRetentionDays)
		if err != nil {
			logOperationalError("cleanup old build logs", err)
		} else if affected > 0 {
			logOperationalInfo("Cleared logs from %d old builds", affected)
		}
	}
}

func currentBinaryChecksum() (string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return "", err
	}
	return fileSHA256(exePath)
}

func compactPath(value string) string {
	value = strings.TrimSpace(strings.TrimRight(value, `/\`))
	if value == "" {
		return ""
	}
	if i := strings.LastIndexAny(value, `/\`); i >= 0 && i < len(value)-1 {
		return value[i+1:]
	}
	return value
}

// HTML Handlers

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	projects, err := listProjectsWithLastBuild()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl.ExecuteTemplate(w, "index.html", projects)
}

func handleProjectPage(w http.ResponseWriter, r *http.Request) {
	// /projects/new or /projects/123
	runners, _ := listRunners()
	if runners == nil {
		runners = []Runner{}
	}

	if r.URL.Path == "/projects/new" {
		tmpl.ExecuteTemplate(w, "project.html", map[string]interface{}{
			"Project": nil,
			"Builds":  nil,
			"IsNew":   true,
			"Runners": runners,
		})
		return
	}

	id, suffix, ok := parseIDPath(r.URL.Path, "/projects/")
	if !ok || suffix != "" {
		http.NotFound(w, r)
		return
	}

	project, err := getProject(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	builds, _ := listBuilds(id, 20)
	history, _ := newProjectBuildHistory(id, 20)

	tmpl.ExecuteTemplate(w, "project.html", map[string]interface{}{
		"Project": project,
		"Builds":  builds,
		"History": history,
		"IsNew":   false,
		"Runners": runners,
	})
}

func handleBuildPage(w http.ResponseWriter, r *http.Request) {
	id, suffix, ok := parseIDPath(r.URL.Path, "/builds/")
	if !ok || suffix != "" {
		http.NotFound(w, r)
		return
	}

	build, err := getBuild(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	tmpl.ExecuteTemplate(w, "build.html", build)
}

// API Handlers

type projectRequest struct {
	Name               string          `json:"name"`
	RepoPath           string          `json:"repo_path"`
	DockerfilePath     string          `json:"dockerfile_path"`
	ComposeFile        string          `json:"compose_file"`
	ImageName          string          `json:"image_name"`
	SSHHost            string          `json:"ssh_host"`
	DeployDir          string          `json:"deploy_dir"`
	HealthURL          string          `json:"health_url"`
	HealthContainer    string          `json:"health_container"`
	BuildArgs          json.RawMessage `json:"build_args"`
	ComposeServices    string          `json:"compose_services"`
	GitPullBeforeBuild bool            `json:"git_pull_before_build"`
	RunnerID           int64           `json:"runner_id"`
	DeployMode         string          `json:"deploy_mode"`
	PostDeploy         string          `json:"post_deploy"`
	Permissions        json.RawMessage `json:"permissions"`
	Preserve           string          `json:"preserve"`
}

func decodeProjectRequest(r *http.Request) (*Project, error) {
	var payload projectRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}

	buildArgs, err := parseBuildArgsField(payload.BuildArgs)
	if err != nil {
		return nil, err
	}
	permissions, err := parseJSONStringField(payload.Permissions, "permissions")
	if err != nil {
		return nil, err
	}

	return &Project{
		Name:               payload.Name,
		RepoPath:           payload.RepoPath,
		DockerfilePath:     payload.DockerfilePath,
		ComposeFile:        payload.ComposeFile,
		ImageName:          payload.ImageName,
		SSHHost:            payload.SSHHost,
		DeployDir:          payload.DeployDir,
		HealthURL:          payload.HealthURL,
		HealthContainer:    payload.HealthContainer,
		BuildArgs:          buildArgs,
		ComposeServices:    payload.ComposeServices,
		GitPullBeforeBuild: payload.GitPullBeforeBuild,
		RunnerID:           payload.RunnerID,
		DeployMode:         payload.DeployMode,
		PostDeploy:         payload.PostDeploy,
		Permissions:        permissions,
		Preserve:           payload.Preserve,
	}, nil
}

func parseBuildArgsField(raw json.RawMessage) (map[string]string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return map[string]string{}, nil
	}

	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("build_args must be a JSON object with string values")
	}

	buildArgs := make(map[string]string, len(values))
	for key, rawValue := range values {
		var value string
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("build_args.%s must be a string value", key)
		}
		buildArgs[key] = value
	}
	return buildArgs, nil
}

func parseJSONStringField(raw json.RawMessage, field string) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return "", nil
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a JSON string containing the %s object", field, field)
	}
	return value, nil
}

func handleAPIProjects(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		projects, err := listProjectsWithLastBuild()
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse(w, projects)

	case "POST":
		p, err := decodeProjectRequest(r)
		if err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		if err := createProject(p); err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		jsonResponse(w, p)

	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func handleAPIProject(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/projects/import" {
		handleAPIProjectImport(w, r)
		return
	}

	// Parse path: /api/projects/123 or /api/projects/123/deploy
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/projects/")
	if !ok {
		http.NotFound(w, r)
		return
	}

	// /api/projects/:id/summary
	if suffix == "summary" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIProjectSummary(w, r, id)
		return
	}

	// /api/projects/:id/preview
	if suffix == "preview" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIProjectPreview(w, r, id)
		return
	}

	// /api/projects/:id/runbook
	if suffix == "runbook" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIProjectRunbook(w, r, id)
		return
	}

	// /api/projects/:id/autodetect
	if suffix == "autodetect" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIProjectAutodetect(w, r, id)
		return
	}

	// /api/projects/:id/history
	if suffix == "history" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIProjectHistory(w, r, id)
		return
	}

	// /api/projects/:id/export
	if suffix == "export" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIProjectExport(w, r, id)
		return
	}

	// /api/projects/:id/clone
	if suffix == "clone" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		handleAPIProjectClone(w, r, id)
		return
	}

	// /api/projects/:id/deploy
	if suffix == "deploy" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		project, err := getProject(id)
		if err != nil {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		buildID, err := builder.Deploy(project, "manual")
		if err != nil {
			jsonErrorCode(w, deployConflictErrorCode(err), err.Error(), http.StatusConflict)
			return
		}
		jsonResponse(w, map[string]int64{"build_id": buildID})
		return
	}

	// /api/projects/:id/snapshot
	if suffix == "snapshot" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		project, err := getProject(id)
		if err != nil {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		buildID, err := builder.FetchRemoteSnapshot(project, "snapshot")
		if err != nil {
			jsonErrorCode(w, deployConflictErrorCode(err), err.Error(), http.StatusConflict)
			return
		}
		jsonResponse(w, map[string]int64{"build_id": buildID})
		return
	}
	if suffix != "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case "GET":
		project, err := getProject(id)
		if err != nil {
			jsonErrorCode(w, errCodeProjectNotFound, "project not found", http.StatusNotFound)
			return
		}
		jsonResponse(w, project)

	case "PUT":
		p, err := decodeProjectRequest(r)
		if err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		p.ID = id
		if err := updateProject(p); err != nil {
			jsonErrorCode(w, errCodeValidation, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, p)

	case "DELETE":
		if err := deleteProject(id); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		jsonMethodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func handleAPIBuild(w http.ResponseWriter, r *http.Request) {
	id, suffix, ok := parseIDPath(r.URL.Path, "/api/builds/")
	if !ok {
		http.NotFound(w, r)
		return
	}

	// /api/builds/:id/stream
	if suffix == "stream" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleSSEStream(broker, w, r, id)
		return
	}

	// /api/builds/:id/annotations and /api/builds/:id/annotations/:annotation_id
	if suffix == "annotations" || strings.HasPrefix(suffix, "annotations/") {
		handleAPIBuildAnnotations(w, r, id, suffix)
		return
	}

	// /api/builds/:id/events
	if suffix == "events" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIBuildEvents(w, r, id)
		return
	}

	// /api/builds/:id/failure-summary
	if suffix == "failure-summary" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIBuildFailureSummary(w, r, id)
		return
	}

	// /api/builds/:id/release-notes
	if suffix == "release-notes" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleAPIBuildReleaseNotes(w, r, id)
		return
	}

	// /api/builds/:id/cancel
	if suffix == "cancel" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		if err := builder.Cancel(id); err != nil {
			ok, forceErr := cancelRunningBuild(id, "cancelled by user")
			if forceErr != nil {
				jsonErrorCode(w, errCodeBuildCancelFailed, forceErr.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				jsonErrorCode(w, errCodeBuildNotRunning, err.Error(), http.StatusBadRequest)
				return
			}
		}
		jsonResponse(w, map[string]string{"status": "cancelled"})
		return
	}

	// /api/builds/:id/artifact
	if suffix == "artifact" {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		handleBuildArtifact(w, r, id)
		return
	}
	if suffix != "" {
		http.NotFound(w, r)
		return
	}

	// /api/builds/:id
	if r.Method == "GET" {
		build, err := getBuild(id)
		if err != nil {
			jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
			return
		}
		jsonResponse(w, build)
		return
	}

	jsonMethodNotAllowed(w, http.MethodGet)
}

func handleBuildArtifact(w http.ResponseWriter, r *http.Request, buildID int64) {
	build, err := getBuild(buildID)
	if err != nil {
		jsonErrorCode(w, errCodeBuildNotFound, "build not found", http.StatusNotFound)
		return
	}
	if build.Status != "success" || build.TriggeredBy != "snapshot" {
		jsonErrorCode(w, errCodeArtifactUnavailable, "artifact not available for this build", http.StatusNotFound)
		return
	}

	job, err := getJobByBuildID(buildID)
	if err != nil || job.Mode != "snapshot" || job.ArtifactPath == "" {
		jsonErrorCode(w, errCodeArtifactNotFound, "artifact not found", http.StatusNotFound)
		return
	}
	if !isManagedArtifactPath(job.ArtifactPath) {
		jsonErrorCode(w, errCodeArtifactUnmanaged, "artifact path is not managed", http.StatusInternalServerError)
		return
	}
	if _, err := managedArtifactInfo(job.ArtifactPath); err != nil {
		jsonErrorCode(w, errCodeArtifactNotFound, "artifact not found", http.StatusNotFound)
		return
	}

	safeName := strings.ReplaceAll(build.ProjectName, " ", "-")
	safeName = strings.ReplaceAll(safeName, "/", "-")
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-snapshot-%d.tar.gz", safeName, buildID))
	if err := serveManagedArtifact(w, r, job.ArtifactPath); err != nil {
		logOperationalError("serve snapshot artifact", err)
	}
}

func handleDownloadBinary(w http.ResponseWriter, r *http.Request) {
	exePath, err := os.Executable()
	if err != nil {
		http.Error(w, "cannot find binary", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=deployer")
	http.ServeFile(w, r, exePath)
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	logOperationalError("encode JSON response", json.NewEncoder(w).Encode(data))
}
