package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxHealthResponseBytes = 64 << 10

type HealthCheckSpec struct {
	URL            string
	Container      string
	ExpectedStatus int
	BodyContains   string
	RequestTimeout time.Duration
	TotalTimeout   time.Duration
}

type HealthCheckResult struct {
	Attempts       int
	StatusCode     int
	ContainerState string
}

type containerHealthFunc func(context.Context, string) (string, error)

func (s HealthCheckSpec) normalized() (HealthCheckSpec, error) {
	if s.ExpectedStatus == 0 {
		s.ExpectedStatus = http.StatusOK
	}
	if s.RequestTimeout == 0 {
		s.RequestTimeout = 5 * time.Second
	}
	if s.TotalTimeout == 0 {
		s.TotalTimeout = 60 * time.Second
	}
	if s.ExpectedStatus < 100 || s.ExpectedStatus > 599 {
		return s, fmt.Errorf("expected health status must be between 100 and 599")
	}
	if s.RequestTimeout <= 0 || s.TotalTimeout <= 0 {
		return s, fmt.Errorf("health timeouts must be positive")
	}
	if s.RequestTimeout > s.TotalTimeout {
		s.RequestTimeout = s.TotalTimeout
	}
	return s, nil
}

func runHealthCheck(ctx context.Context, spec HealthCheckSpec, client *http.Client, containerCheck containerHealthFunc, retryInterval time.Duration) (HealthCheckResult, error) {
	spec, err := spec.normalized()
	if err != nil {
		return HealthCheckResult{}, err
	}
	if spec.URL == "" && spec.Container == "" {
		return HealthCheckResult{}, nil
	}
	if client == nil {
		client = http.DefaultClient
	}
	if retryInterval <= 0 {
		retryInterval = 2 * time.Second
	}
	checkCtx, cancel := context.WithTimeout(ctx, spec.TotalTimeout)
	defer cancel()
	result := HealthCheckResult{}
	var lastErr error
	for {
		result.Attempts++
		if spec.Container != "" {
			if containerCheck == nil {
				return result, fmt.Errorf("container health check is unavailable")
			}
			state, checkErr := containerCheck(checkCtx, spec.Container)
			result.ContainerState = state
			if checkErr == nil && state == "healthy" {
				return result, nil
			}
			if checkErr != nil {
				lastErr = fmt.Errorf("container %s health: %w", spec.Container, checkErr)
			} else {
				lastErr = fmt.Errorf("container %s state is %q", spec.Container, state)
			}
		} else {
			status, checkErr := checkHTTPHealth(checkCtx, client, spec)
			result.StatusCode = status
			if checkErr == nil {
				return result, nil
			}
			lastErr = checkErr
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-checkCtx.Done():
			timer.Stop()
			return result, fmt.Errorf("health check ended after %d attempt(s): %w: %v", result.Attempts, checkCtx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func checkHTTPHealth(ctx context.Context, client *http.Client, spec HealthCheckSpec) (int, error) {
	requestCtx, cancel := context.WithTimeout(ctx, spec.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, spec.URL, nil)
	if err != nil {
		return 0, fmt.Errorf("create health request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request health URL: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHealthResponseBytes+1))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("read health response: %w", err)
	}
	if len(body) > maxHealthResponseBytes {
		return resp.StatusCode, fmt.Errorf("health response exceeds %d bytes", maxHealthResponseBytes)
	}
	if resp.StatusCode != spec.ExpectedStatus {
		return resp.StatusCode, fmt.Errorf("health URL returned status %d, expected %d", resp.StatusCode, spec.ExpectedStatus)
	}
	if spec.BodyContains != "" && !strings.Contains(string(body), spec.BodyContains) {
		return resp.StatusCode, fmt.Errorf("health response does not contain expected text")
	}
	return resp.StatusCode, nil
}

type healthCommandOutput func(context.Context, string, ...string) ([]byte, error)

func dockerContainerHealth(check healthCommandOutput) containerHealthFunc {
	return func(ctx context.Context, container string) (string, error) {
		if strings.TrimSpace(container) == "" {
			return "", fmt.Errorf("container name is required")
		}
		out, err := check(ctx, "docker", "inspect", "--format={{.State.Health.Status}}", container)
		return strings.TrimSpace(string(out)), err
	}
}
