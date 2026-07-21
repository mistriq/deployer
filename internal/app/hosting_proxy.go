package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type proxyActivationRequest struct {
	OperationID                   string `json:"operation_id"`
	ExternalProjectID             string `json:"external_project_id"`
	ReleaseDigest                 string `json:"release_digest"`
	RuntimeEndpoint               string `json:"runtime_endpoint"`
	ExpectedPreviousReleaseDigest string `json:"expected_previous_release_digest,omitempty"`
	RouteGeneration               int64  `json:"route_generation"`
}

type proxyActivationResponse struct {
	RouteRevision       string `json:"route_revision"`
	ActiveReleaseDigest string `json:"active_release_digest"`
}

type proxySuspendRequest struct {
	OperationID       string `json:"operation_id"`
	ExternalProjectID string `json:"external_project_id"`
	Suspended         bool   `json:"suspended"`
	RouteGeneration   int64  `json:"route_generation"`
}

type hostingProxyClient interface {
	Activate(context.Context, proxyActivationRequest) (*proxyActivationResponse, error)
	SetSuspended(context.Context, proxySuspendRequest) error
}

type httpHostingProxyClient struct {
	baseURL string
	token   string
	client  *http.Client
}

var hostingProxyClientFactory = newHostingProxyClient

func newHostingProxyClient(cfg AppConfig) (hostingProxyClient, error) {
	if strings.TrimSpace(cfg.ProxyAdapterURL) == "" || strings.TrimSpace(cfg.ProxyAdapterToken) == "" {
		return nil, &hostingAPIError{Code: errCodeProxyUnavailable, Message: "reverse-proxy adapter is not configured", StatusCode: http.StatusServiceUnavailable}
	}
	parsed, err := url.Parse(cfg.ProxyAdapterURL)
	if err != nil || !validPrivateServiceURL(parsed) {
		return nil, fmt.Errorf("DEPLOYER_PROXY_ADAPTER_URL must use HTTPS (or loopback HTTP) without credentials, query, or fragment")
	}
	timeout := cfg.ProxyAdapterTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &httpHostingProxyClient{baseURL: strings.TrimRight(cfg.ProxyAdapterURL, "/"), token: cfg.ProxyAdapterToken, client: &http.Client{Timeout: timeout}}, nil
}

func (client *httpHostingProxyClient) Activate(ctx context.Context, request proxyActivationRequest) (*proxyActivationResponse, error) {
	if request.RouteGeneration <= 0 {
		return nil, &hostingAPIError{Code: errCodeProxyRejected, Message: "reverse-proxy activation requires a positive route generation", StatusCode: http.StatusBadGateway}
	}
	var response proxyActivationResponse
	path := "/api/internal/v1/projects/" + url.PathEscape(request.ExternalProjectID) + "/activate"
	if err := client.doJSON(ctx, http.MethodPost, path, request, &response); err != nil {
		return nil, err
	}
	if !validSHA256Digest(response.ActiveReleaseDigest) || response.ActiveReleaseDigest != request.ReleaseDigest || strings.TrimSpace(response.RouteRevision) == "" {
		return nil, &hostingAPIError{Code: errCodeProxyUnavailable,
			Message:    "reverse-proxy adapter returned an invalid activation identity",
			StatusCode: http.StatusServiceUnavailable}
	}
	return &response, nil
}

func (client *httpHostingProxyClient) SetSuspended(ctx context.Context, request proxySuspendRequest) error {
	if request.RouteGeneration <= 0 {
		return &hostingAPIError{Code: errCodeProxyRejected, Message: "reverse-proxy suspension requires a positive route generation", StatusCode: http.StatusBadGateway}
	}
	path := "/api/internal/v1/projects/" + url.PathEscape(request.ExternalProjectID) + "/suspension"
	return client.doJSON(ctx, http.MethodPut, path, request, nil)
}

func (client *httpHostingProxyClient) doJSON(ctx context.Context, method, path string, requestBody any, responseBody any) error {
	encoded, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.client.Do(request)
	if err != nil {
		return &hostingAPIError{Code: errCodeProxyUnavailable, Message: "reverse-proxy adapter is unavailable", StatusCode: http.StatusServiceUnavailable, Err: err}
	}
	defer response.Body.Close()
	limited, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return &hostingAPIError{Code: errCodeProxyUnavailable,
			Message:    "reverse-proxy adapter response was ambiguous",
			StatusCode: http.StatusServiceUnavailable, Err: err}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode >= http.StatusInternalServerError ||
			response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooEarly ||
			response.StatusCode == http.StatusTooManyRequests {
			return &hostingAPIError{Code: errCodeProxyUnavailable,
				Message:    "reverse-proxy adapter returned a transient or ambiguous response",
				StatusCode: http.StatusServiceUnavailable}
		}
		return &hostingAPIError{Code: errCodeProxyRejected, Message: "reverse-proxy adapter rejected the operation", StatusCode: http.StatusBadGateway}
	}
	if responseBody == nil || response.StatusCode == http.StatusNoContent || len(bytes.TrimSpace(limited)) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(limited))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(responseBody); err != nil {
		return &hostingAPIError{Code: errCodeProxyUnavailable,
			Message:    "reverse-proxy adapter returned an ambiguous response",
			StatusCode: http.StatusServiceUnavailable, Err: err}
	}
	return nil
}

func validPrivateServiceURL(parsed *url.URL) bool {
	if parsed == nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	if parsed.Scheme == "https" {
		return true
	}
	if parsed.Scheme != "http" {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
