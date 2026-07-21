package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHostingProxyClientUsesVersionedAuthenticatedAdapterContract(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal/v1/projects/project_01JPROXY/activate" || r.Method != http.MethodPost {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer adapter-token" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("headers = %#v", r.Header)
		}
		var request proxyActivationRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.OperationID == "" || request.ReleaseDigest != digest || request.RuntimeEndpoint != "http://10.1.2.3:3000" || request.RouteGeneration != 7 {
			t.Fatalf("activation = %+v", request)
		}
		jsonResponse(w, proxyActivationResponse{RouteRevision: "revision-1", ActiveReleaseDigest: digest})
	}))
	defer server.Close()
	client, err := newHostingProxyClient(AppConfig{ProxyAdapterURL: server.URL, ProxyAdapterToken: "adapter-token"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Activate(context.Background(), proxyActivationRequest{
		OperationID: "activate-operation", ExternalProjectID: "project_01JPROXY", ReleaseDigest: digest,
		RuntimeEndpoint: "http://10.1.2.3:3000", RouteGeneration: 7,
	})
	if err != nil || response.RouteRevision != "revision-1" {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func TestHostingProxySuspensionCarriesRouteGeneration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request proxySuspendRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.OperationID != "suspend-operation" || request.ExternalProjectID != "project_01JSUSPEND" ||
			!request.Suspended || request.RouteGeneration != 9 {
			t.Fatalf("suspension request=%+v", request)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := newHostingProxyClient(AppConfig{ProxyAdapterURL: server.URL, ProxyAdapterToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetSuspended(t.Context(), proxySuspendRequest{OperationID: "suspend-operation",
		ExternalProjectID: "project_01JSUSPEND", Suspended: true, RouteGeneration: 9}); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateServiceURLsRequireHTTPSOutsideLoopback(t *testing.T) {
	for _, value := range []string{"http://10.1.2.3:8080", "http://control-plane.internal", "ftp://localhost/service", "https://user:pass@example.com"} {
		if _, err := newHostingProxyClient(AppConfig{ProxyAdapterURL: value, ProxyAdapterToken: "token"}); err == nil {
			t.Fatalf("unsafe proxy URL %q was accepted", value)
		}
		if _, err := validateCallbackTarget(value); err == nil {
			t.Fatalf("unsafe callback URL %q was accepted", value)
		}
	}
	for _, value := range []string{"https://control-plane.internal/callback", "http://127.0.0.1:8080", "http://localhost:8080"} {
		if _, err := validateCallbackTarget(value); err != nil {
			t.Fatalf("safe callback URL %q rejected: %v", value, err)
		}
	}
}

func TestHostingProxyTreatsMismatchedActivationIdentityAsAmbiguous(t *testing.T) {
	requested := "sha256:" + strings.Repeat("b", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		jsonResponse(w, proxyActivationResponse{RouteRevision: "revision-2", ActiveReleaseDigest: "sha256:" + strings.Repeat("c", 64)})
	}))
	defer server.Close()
	client, err := newHostingProxyClient(AppConfig{ProxyAdapterURL: server.URL, ProxyAdapterToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Activate(context.Background(), proxyActivationRequest{OperationID: "op", ExternalProjectID: "project_01JPROXY", ReleaseDigest: requested, RuntimeEndpoint: "http://10.1.2.3:3000", RouteGeneration: 1})
	apiErr, ok := err.(*hostingAPIError)
	if !ok || apiErr.Code != errCodeProxyUnavailable {
		t.Fatalf("error = %v", err)
	}
}

func TestHostingProxyClassifiesAdapterServerErrorAsAmbiguous(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		jsonErrorCode(w, errCodeInternal, "adapter failed after dispatch", http.StatusBadGateway)
	}))
	defer server.Close()
	client, err := newHostingProxyClient(AppConfig{ProxyAdapterURL: server.URL, ProxyAdapterToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Activate(t.Context(), proxyActivationRequest{
		OperationID: "op-ambiguous-5xx", ExternalProjectID: "project_01JPROXY",
		ReleaseDigest: "sha256:" + strings.Repeat("d", 64), RuntimeEndpoint: "http://10.1.2.3:3000",
		RouteGeneration: 1,
	})
	apiErr, ok := err.(*hostingAPIError)
	if !ok || apiErr.Code != errCodeProxyUnavailable || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %#v, want proxy_unavailable 503", err)
	}
}

func TestHostingProxyClassifiesMalformedSuccessAsAmbiguous(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"route_revision":`))
	}))
	defer server.Close()
	client, err := newHostingProxyClient(AppConfig{ProxyAdapterURL: server.URL, ProxyAdapterToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Activate(t.Context(), proxyActivationRequest{
		OperationID: "op-malformed-2xx", ExternalProjectID: "project_01JPROXY",
		ReleaseDigest: "sha256:" + strings.Repeat("e", 64), RuntimeEndpoint: "http://10.1.2.3:3000",
		RouteGeneration: 1,
	})
	apiErr, ok := err.(*hostingAPIError)
	if !ok || apiErr.Code != errCodeProxyUnavailable || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %#v, want proxy_unavailable 503", err)
	}
}
