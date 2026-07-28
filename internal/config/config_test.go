package config_test

import (
	"testing"
	"time"

	"github.com/ZerGo0/nginx-retry-proxy/internal/config"
)

func TestLoadUsesSafeDefaultsAndExplicitBackendEndpoints(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"BACKEND_URL":   "http://backend:3000",
		"READINESS_URL": "http://backend:3000/public/api/v1/health",
	}
	got, err := config.Load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if got.ProxyListenAddr != ":8080" || got.AdminListenAddr != ":9090" {
		t.Fatalf("unexpected listeners: proxy=%q admin=%q", got.ProxyListenAddr, got.AdminListenAddr)
	}
	if got.HoldProxy.BackendURL != values["BACKEND_URL"] || got.HoldProxy.ReadinessURL != values["READINESS_URL"] {
		t.Fatalf("unexpected endpoints: backend=%q readiness=%q", got.HoldProxy.BackendURL, got.HoldProxy.ReadinessURL)
	}
	if got.HoldProxy.HoldTimeout != 30*time.Second || got.HoldProxy.RequestTimeout != 60*time.Second {
		t.Fatalf("unexpected deadlines: hold=%v request=%v", got.HoldProxy.HoldTimeout, got.HoldProxy.RequestTimeout)
	}
	if got.ShutdownTimeout != 70*time.Second {
		t.Fatalf("unexpected shutdown timeout: %v", got.ShutdownTimeout)
	}
	if got.HoldProxy.HealthInterval != 250*time.Millisecond || got.HoldProxy.HealthTimeout != time.Second {
		t.Fatalf("unexpected health settings: interval=%v timeout=%v", got.HoldProxy.HealthInterval, got.HoldProxy.HealthTimeout)
	}
	if got.HoldProxy.MaxHeldRequests != 100 || got.HoldProxy.ForwardConcurrent != 25 {
		t.Fatalf("unexpected capacities: held=%d forward=%d", got.HoldProxy.MaxHeldRequests, got.HoldProxy.ForwardConcurrent)
	}
}

func TestLoadRejectsMissingOrMalformedValues(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		values map[string]string
	}{
		{name: "missing backend URL", values: map[string]string{"READINESS_URL": "http://backend:3000/ready"}},
		{name: "malformed backend URL", values: map[string]string{"BACKEND_URL": "://backend", "READINESS_URL": "http://backend:3000/ready"}},
		{name: "malformed readiness URL", values: map[string]string{"BACKEND_URL": "http://backend:3000", "READINESS_URL": "backend/ready"}},
		{name: "invalid hold timeout", values: map[string]string{"BACKEND_URL": "http://backend:3000", "READINESS_URL": "http://backend:3000/ready", "HOLD_TIMEOUT": "eventually"}},
		{name: "shutdown shorter than request timeout", values: map[string]string{"BACKEND_URL": "http://backend:3000", "READINESS_URL": "http://backend:3000/ready", "REQUEST_TIMEOUT": "30s", "SHUTDOWN_TIMEOUT": "20s"}},
		{name: "zero held capacity", values: map[string]string{"BACKEND_URL": "http://backend:3000", "READINESS_URL": "http://backend:3000/ready", "MAX_HELD_REQUESTS": "0"}},
		{name: "matching listen addresses", values: map[string]string{"BACKEND_URL": "http://backend:3000", "READINESS_URL": "http://backend:3000/ready", "PROXY_LISTEN_ADDR": ":8080", "ADMIN_LISTEN_ADDR": ":8080"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := config.Load(func(key string) (string, bool) {
				value, ok := test.values[key]
				return value, ok
			}); err == nil {
				t.Fatalf("Load returned nil error for %v", test.values)
			}
		})
	}
}

func TestLoadPreservesCustomListenAddresses(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"BACKEND_URL":       "http://backend:3000",
		"READINESS_URL":     "http://backend:3000/ready",
		"PROXY_LISTEN_ADDR": "127.0.0.1:8081",
		"ADMIN_LISTEN_ADDR": "127.0.0.1:9091",
		"SHUTDOWN_TIMEOUT":  "90s",
	}
	got, err := config.Load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if got.ProxyListenAddr != values["PROXY_LISTEN_ADDR"] || got.AdminListenAddr != values["ADMIN_LISTEN_ADDR"] {
		t.Fatalf("unexpected listeners: proxy=%q admin=%q", got.ProxyListenAddr, got.AdminListenAddr)
	}
	if got.ShutdownTimeout != 90*time.Second {
		t.Fatalf("unexpected shutdown timeout: %v", got.ShutdownTimeout)
	}
}
