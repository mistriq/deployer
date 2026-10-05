package app

import (
	"encoding/json"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebAssetsAndInstallManifest(t *testing.T) {
	mux := http.NewServeMux()
	registerWebAssets(mux)
	request := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s returned %d", path, response.Code)
		}
		return response
	}
	manifestResponse := request("/manifest.webmanifest")
	if manifestResponse.Header().Get("Content-Type") != "application/manifest+json" {
		t.Fatal("manifest has wrong MIME type")
	}
	var manifest struct {
		ID, Name, Scope, Display string
		StartURL                 string `json:"start_url"`
		Icons                    []struct{ Src, Sizes, Purpose string }
	}
	if err := json.Unmarshal(manifestResponse.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.ID != "/" || manifest.Scope != "/" || manifest.Display != "standalone" || manifest.Name != "Deployer" || manifest.StartURL != "/" {
		t.Fatalf("invalid install metadata: %+v", manifest)
	}
	for _, icon := range manifest.Icons {
		response := request(icon.Src)
		image, err := png.DecodeConfig(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if image.Width != image.Height || (image.Width != 192 && image.Width != 512) {
			t.Fatalf("invalid install icon dimensions: %+v", image)
		}
	}
	worker := request("/service-worker.js")
	if worker.Header().Get("Service-Worker-Allowed") != "/" || !strings.Contains(worker.Header().Get("Content-Type"), "javascript") {
		t.Fatal("service worker has wrong scope or MIME type")
	}
	request("/static/offline.html")
	request("/favicon.ico")
	asset := request("/static/style.css")
	if strings.Contains(asset.Header().Get("Cache-Control"), "no-store") || asset.Header().Get("ETag") == "" {
		t.Fatal("static assets cannot be reused")
	}
	revalidate := httptest.NewRequest(http.MethodGet, "/static/style.css", nil)
	revalidate.Header.Set("If-None-Match", asset.Header().Get("ETag"))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, revalidate)
	if response.Code != http.StatusNotModified || response.Body.Len() != 0 {
		t.Fatalf("expected empty 304, got %d", response.Code)
	}
}
