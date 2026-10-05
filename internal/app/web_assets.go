package app

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func publicAssetURL(path string) string {
	return strings.TrimRight(appConfig.PublicURL, "/") + path
}

// Embedded assets have no modification time. Content ETags let browsers reuse
// their private copies while checking every navigation for a new binary's UI.
func serveWebAsset(w http.ResponseWriter, r *http.Request, path, contentType string) {
	content, err := webFS.ReadFile("web/static/" + path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, sha256.Sum256(content)))
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(w, r, path, time.Time{}, bytes.NewReader(content))
}

func registerWebAssets(mux *http.ServeMux) {
	mux.HandleFunc("/static/", func(w http.ResponseWriter, r *http.Request) {
		serveWebAsset(w, r, strings.TrimPrefix(r.URL.Path, "/static/"), "")
	})
	mux.HandleFunc("/manifest.webmanifest", func(w http.ResponseWriter, r *http.Request) {
		serveWebAsset(w, r, "manifest.webmanifest", "application/manifest+json")
	})
	mux.HandleFunc("/service-worker.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Service-Worker-Allowed", "/")
		serveWebAsset(w, r, "service-worker.js", "text/javascript; charset=utf-8")
	})
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		serveWebAsset(w, r, "brand/favicon.ico", "image/vnd.microsoft.icon")
	})
}
