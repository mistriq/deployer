package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunHealthCheckExpectedStatusAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"state":"ready"}`))
	}))
	defer server.Close()
	result, err := runHealthCheck(context.Background(), HealthCheckSpec{URL: server.URL, ExpectedStatus: http.StatusAccepted, BodyContains: `"state":"ready"`, RequestTimeout: time.Second, TotalTimeout: time.Second}, server.Client(), nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 1 || result.StatusCode != http.StatusAccepted {
		t.Fatalf("result=%+v", result)
	}
}

func TestRunHealthCheckReportsStatusAndBodyMismatch(t *testing.T) {
	status := http.StatusServiceUnavailable
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status); _, _ = w.Write([]byte("warming")) }))
	defer server.Close()
	_, err := runHealthCheck(context.Background(), HealthCheckSpec{URL: server.URL, ExpectedStatus: http.StatusOK, BodyContains: "ready", RequestTimeout: 20 * time.Millisecond, TotalTimeout: 35 * time.Millisecond}, server.Client(), nil, 5*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("error=%v", err)
	}
}

func TestRunHealthCheckEnforcesRequestAndTotalTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	start := time.Now()
	result, err := runHealthCheck(context.Background(), HealthCheckSpec{URL: server.URL, RequestTimeout: 15 * time.Millisecond, TotalTimeout: 45 * time.Millisecond}, server.Client(), nil, 2*time.Millisecond)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("health deadline took %s", elapsed)
	}
}

func TestRunHealthCheckHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runHealthCheck(ctx, HealthCheckSpec{URL: "http://127.0.0.1", RequestTimeout: time.Second, TotalTimeout: time.Second}, http.DefaultClient, nil, time.Millisecond)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestDockerContainerHealthUsesFixedArguments(t *testing.T) {
	var name string
	var args []string
	check := dockerContainerHealth(func(ctx context.Context, gotName string, gotArgs ...string) ([]byte, error) {
		name = gotName
		args = gotArgs
		return []byte("healthy\n"), nil
	})
	state, err := check(context.Background(), "app-1")
	if err != nil {
		t.Fatal(err)
	}
	if state != "healthy" || name != "docker" || !reflect.DeepEqual(args, []string{"inspect", "--format={{.State.Health.Status}}", "app-1"}) {
		t.Fatalf("state=%q command=%s %#v", state, name, args)
	}
}

func TestCheckHTTPHealthBoundsResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxHealthResponseBytes+1)))
	}))
	defer server.Close()
	_, err := checkHTTPHealth(context.Background(), server.Client(), HealthCheckSpec{URL: server.URL, ExpectedStatus: 200, RequestTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error=%v", err)
	}
}
