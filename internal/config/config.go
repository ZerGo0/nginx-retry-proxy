package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ZerGo0/nginx-retry-proxy/internal/holdproxy"
)

type LookupEnv func(string) (string, bool)

type Config struct {
	ProxyListenAddr string
	AdminListenAddr string
	ShutdownTimeout time.Duration
	HoldProxy       holdproxy.Config
}

func Load(lookup LookupEnv) (Config, error) {
	backendURL, err := required(lookup, "BACKEND_URL")
	if err != nil {
		return Config{}, err
	}
	readinessURL, err := required(lookup, "READINESS_URL")
	if err != nil {
		return Config{}, err
	}
	holdTimeout, err := duration(lookup, "HOLD_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	requestTimeout, err := duration(lookup, "REQUEST_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	shutdownTimeout, err := duration(lookup, "SHUTDOWN_TIMEOUT", requestTimeout+10*time.Second)
	if err != nil {
		return Config{}, err
	}
	if shutdownTimeout < requestTimeout {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be at least REQUEST_TIMEOUT")
	}
	healthInterval, err := duration(lookup, "HEALTH_INTERVAL", 250*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	healthTimeout, err := duration(lookup, "HEALTH_TIMEOUT", time.Second)
	if err != nil {
		return Config{}, err
	}
	maxHeld, err := positiveInt(lookup, "MAX_HELD_REQUESTS", 100)
	if err != nil {
		return Config{}, err
	}
	forwardConcurrent, err := positiveInt(lookup, "FORWARD_CONCURRENT", 25)
	if err != nil {
		return Config{}, err
	}

	proxyListenAddr := value(lookup, "PROXY_LISTEN_ADDR", ":8080")
	adminListenAddr := value(lookup, "ADMIN_LISTEN_ADDR", ":9090")
	if proxyListenAddr == adminListenAddr {
		return Config{}, fmt.Errorf("PROXY_LISTEN_ADDR and ADMIN_LISTEN_ADDR must be different")
	}
	cfg := Config{
		ProxyListenAddr: proxyListenAddr,
		AdminListenAddr: adminListenAddr,
		ShutdownTimeout: shutdownTimeout,
		HoldProxy: holdproxy.Config{
			BackendURL:        backendURL,
			ReadinessURL:      readinessURL,
			HoldTimeout:       holdTimeout,
			RequestTimeout:    requestTimeout,
			HealthInterval:    healthInterval,
			HealthTimeout:     healthTimeout,
			MaxHeldRequests:   maxHeld,
			ForwardConcurrent: forwardConcurrent,
		},
	}
	if err := cfg.HoldProxy.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func required(lookup LookupEnv, key string) (string, error) {
	value := value(lookup, key, "")
	if value == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return value, nil
}

func value(lookup LookupEnv, key, fallback string) string {
	if raw, ok := lookup(key); ok {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

func duration(lookup LookupEnv, key string, fallback time.Duration) (time.Duration, error) {
	raw := value(lookup, key, fallback.String())
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return parsed, nil
}

func positiveInt(lookup LookupEnv, key string, fallback int) (int, error) {
	raw := value(lookup, key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}
