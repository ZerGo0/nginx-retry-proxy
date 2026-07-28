package holdproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	BackendURL        string
	ReadinessURL      string
	HoldTimeout       time.Duration
	RequestTimeout    time.Duration
	HealthInterval    time.Duration
	HealthTimeout     time.Duration
	MaxHeldRequests   int
	ForwardConcurrent int
}

type Service struct {
	proxyHandler http.Handler
	config       Config
	gate         *readinessGate
	metrics      metrics
}

type metrics struct {
	held            atomic.Int64
	released        atomic.Uint64
	expired         atomic.Uint64
	overloaded      atomic.Uint64
	canceled        atomic.Uint64
	forwardErrors   atomic.Uint64
	requestTimeouts atomic.Uint64
	upgradeReject   atomic.Uint64
	waitCount       atomic.Uint64
	waitSumNanos    atomic.Uint64
	waitBuckets     [7]atomic.Uint64
}

var waitBucketUpperBounds = [...]time.Duration{
	100 * time.Millisecond,
	time.Second,
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
	90 * time.Second,
}

func New(cfg Config) (*Service, error) {
	validatedConfig, backendURL, readinessURL, err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}
	cfg = validatedConfig

	maxIdleConnections := cfg.ForwardConcurrent + 1
	transport := &http.Transport{
		MaxIdleConns:        maxIdleConnections,
		MaxIdleConnsPerHost: maxIdleConnections,
		IdleConnTimeout:     10 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).DialContext,
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(backendURL)
	reverseProxy.Transport = transport
	service := &Service{config: cfg}
	reverseProxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		switch {
		case errors.Is(r.Context().Err(), context.Canceled) || errors.Is(err, context.Canceled):
			service.metrics.canceled.Add(1)
			slog.Info("backend forwarding canceled", "method", r.Method, "path", r.URL.Path, "request_id", r.Header.Get("X-Request-ID"), "error", err)
		case errors.Is(r.Context().Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
			service.metrics.requestTimeouts.Add(1)
			slog.Warn("backend forwarding timed out", "method", r.Method, "path", r.URL.Path, "request_id", r.Header.Get("X-Request-ID"), "error", err)
		default:
			service.metrics.forwardErrors.Add(1)
			slog.Error("backend forwarding failed", "method", r.Method, "path", r.URL.Path, "request_id", r.Header.Get("X-Request-ID"), "error", err)
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	heldSlots := make(chan struct{}, cfg.MaxHeldRequests)
	forwardSlots := make(chan struct{}, cfg.ForwardConcurrent)
	gate := newReadinessGate(transport, readinessURL, cfg.HealthInterval, cfg.HealthTimeout)
	service.gate = gate

	service.proxyHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		if r.Header.Get("Upgrade") != "" || headerContainsToken(r.Header.Get("Connection"), "upgrade") {
			service.metrics.upgradeReject.Add(1)
			service.logRequest(r, "upgrade_rejected", time.Since(started), false)
			http.Error(w, "protocol upgrade is not supported", http.StatusUpgradeRequired)
			return
		}
		select {
		case heldSlots <- struct{}{}:
			service.metrics.held.Add(1)
			holdingSlot := true
			releaseHeldSlot := func() {
				if !holdingSlot {
					return
				}
				holdingSlot = false
				service.metrics.held.Add(-1)
				<-heldSlots
			}
			defer releaseHeldSlot()

			outcome := "released"
			forwarded := false
			var waitDuration time.Duration
			defer func() {
				service.metrics.recordWait(waitDuration)
				service.logRequest(r, outcome, waitDuration, forwarded)
			}()

			requestCtx, cancelRequest := context.WithTimeout(r.Context(), cfg.RequestTimeout)
			defer cancelRequest()
			holdCtx, cancelHold := context.WithTimeout(requestCtx, cfg.HoldTimeout)

			if !gate.wait(holdCtx) {
				cancelHold()
				waitDuration = time.Since(started)
				releaseHeldSlot()
				if r.Context().Err() != nil {
					service.metrics.canceled.Add(1)
					outcome = "canceled"
				} else {
					service.metrics.expired.Add(1)
					outcome = "expired"
				}
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			cancelHold()
			waitDuration = time.Since(started)
			releaseHeldSlot()
			select {
			case forwardSlots <- struct{}{}:
				defer func() { <-forwardSlots }()
				service.metrics.released.Add(1)
				forwarded = true
				reverseProxy.ServeHTTP(w, r.WithContext(requestCtx))
				switch {
				case errors.Is(requestCtx.Err(), context.DeadlineExceeded):
					outcome = "forward_timeout"
				case r.Context().Err() != nil:
					outcome = "canceled"
				}
			case <-requestCtx.Done():
				if r.Context().Err() != nil {
					service.metrics.canceled.Add(1)
					outcome = "canceled"
				} else {
					service.metrics.requestTimeouts.Add(1)
					outcome = "release_timeout"
				}
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			}
			return
		default:
			service.metrics.overloaded.Add(1)
			service.logRequest(r, "overloaded", time.Since(started), false)
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
	})
	return service, nil
}

func (cfg Config) Validate() error {
	_, _, _, err := validateConfig(cfg)
	return err
}

func validateConfig(cfg Config) (Config, *url.URL, *url.URL, error) {
	if cfg.HoldTimeout <= 0 {
		return Config{}, nil, nil, fmt.Errorf("hold timeout must be positive")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = cfg.HoldTimeout + 30*time.Second
	}
	if cfg.RequestTimeout <= cfg.HoldTimeout {
		return Config{}, nil, nil, fmt.Errorf("request timeout must be greater than hold timeout")
	}
	if cfg.HealthInterval <= 0 {
		return Config{}, nil, nil, fmt.Errorf("health interval must be positive")
	}
	if cfg.HealthTimeout <= 0 {
		return Config{}, nil, nil, fmt.Errorf("health timeout must be positive")
	}
	if cfg.MaxHeldRequests <= 0 {
		return Config{}, nil, nil, fmt.Errorf("max held requests must be positive")
	}
	if cfg.ForwardConcurrent <= 0 {
		return Config{}, nil, nil, fmt.Errorf("forward concurrency must be positive")
	}
	backendURL, err := parseEndpoint("backend", cfg.BackendURL)
	if err != nil {
		return Config{}, nil, nil, err
	}
	readinessURL, err := parseEndpoint("readiness", cfg.ReadinessURL)
	if err != nil {
		return Config{}, nil, nil, err
	}
	return cfg, backendURL, readinessURL, nil
}

func parseEndpoint(name, value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("parse %s URL: %w", name, err)
	}
	if parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("%s URL must be an absolute HTTP URL", name)
	}
	return parsed, nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.proxyHandler.ServeHTTP(w, r)
}

func (s *Service) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		backendReady, lastReadyAt := s.gate.status()
		status := map[string]any{
			"backend_ready":      backendReady,
			"held_requests":      s.metrics.held.Load(),
			"max_held_requests":  s.config.MaxHeldRequests,
			"forward_concurrent": s.config.ForwardConcurrent,
		}
		if !lastReadyAt.IsZero() {
			status["backend_last_ready_at"] = lastReadyAt.UTC().Format(time.RFC3339Nano)
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, `# HELP hold_proxy_held_requests Requests currently waiting for backend readiness.
# TYPE hold_proxy_held_requests gauge
# HELP hold_proxy_released_requests_total Requests released for forwarding after backend readiness.
# TYPE hold_proxy_released_requests_total counter
# HELP hold_proxy_expired_requests_total Requests whose hold deadline expired before backend readiness.
# TYPE hold_proxy_expired_requests_total counter
# HELP hold_proxy_overloaded_requests_total Requests rejected because the waiting capacity was full.
# TYPE hold_proxy_overloaded_requests_total counter
# HELP hold_proxy_canceled_requests_total Requests canceled by their client before completion.
# TYPE hold_proxy_canceled_requests_total counter
# HELP hold_proxy_forward_errors_total Requests whose single forwarding attempt failed.
# TYPE hold_proxy_forward_errors_total counter
# HELP hold_proxy_request_timeouts_total Requests that exhausted their total deadline after backend readiness.
# TYPE hold_proxy_request_timeouts_total counter
# HELP hold_proxy_upgrade_rejections_total Protocol upgrade requests rejected by the hold proxy.
# TYPE hold_proxy_upgrade_rejections_total counter
# HELP hold_proxy_wait_duration_seconds Time spent waiting for backend readiness.
# TYPE hold_proxy_wait_duration_seconds histogram
`)
		_, _ = fmt.Fprintf(w, "hold_proxy_held_requests %d\nhold_proxy_released_requests_total %d\nhold_proxy_expired_requests_total %d\nhold_proxy_overloaded_requests_total %d\nhold_proxy_canceled_requests_total %d\nhold_proxy_forward_errors_total %d\nhold_proxy_request_timeouts_total %d\nhold_proxy_upgrade_rejections_total %d\n",
			s.metrics.held.Load(),
			s.metrics.released.Load(),
			s.metrics.expired.Load(),
			s.metrics.overloaded.Load(),
			s.metrics.canceled.Load(),
			s.metrics.forwardErrors.Load(),
			s.metrics.requestTimeouts.Load(),
			s.metrics.upgradeReject.Load(),
		)
		for index, upperBound := range waitBucketUpperBounds {
			_, _ = fmt.Fprintf(w, "hold_proxy_wait_duration_seconds_bucket{le=%q} %d\n", formatPrometheusDuration(upperBound), s.metrics.waitBuckets[index].Load())
		}
		count := s.metrics.waitCount.Load()
		_, _ = fmt.Fprintf(w, "hold_proxy_wait_duration_seconds_bucket{le=\"+Inf\"} %d\nhold_proxy_wait_duration_seconds_sum %f\nhold_proxy_wait_duration_seconds_count %d\n",
			count,
			float64(s.metrics.waitSumNanos.Load())/float64(time.Second),
			count,
		)
	})
	return mux
}

func (s *Service) logRequest(r *http.Request, outcome string, heldDuration time.Duration, forwarded bool) {
	slog.Info("held request completed",
		"method", r.Method,
		"path", r.URL.Path,
		"request_id", r.Header.Get("X-Request-ID"),
		"outcome", outcome,
		"held_duration", heldDuration.String(),
		"forwarded", forwarded,
	)
}

func (m *metrics) recordWait(duration time.Duration) {
	m.waitCount.Add(1)
	m.waitSumNanos.Add(uint64(duration))
	for index, upperBound := range waitBucketUpperBounds {
		if duration <= upperBound {
			m.waitBuckets[index].Add(1)
		}
	}
}

func formatPrometheusDuration(duration time.Duration) string {
	return fmt.Sprintf("%g", duration.Seconds())
}

func headerContainsToken(value, target string) bool {
	for token := range strings.SplitSeq(value, ",") {
		if strings.EqualFold(strings.TrimSpace(token), target) {
			return true
		}
	}
	return false
}

type readinessGate struct {
	mu          sync.Mutex
	check       func(context.Context) bool
	interval    time.Duration
	waiters     int
	readyUntil  time.Time
	lastReadyAt time.Time
	readyCh     chan struct{}
	stopCh      chan struct{}
	polling     bool
	failedCheck bool
}

func newReadinessGate(transport http.RoundTripper, readinessURL *url.URL, interval, timeout time.Duration) *readinessGate {
	return &readinessGate{
		check: func(parent context.Context) bool {
			return backendReady(parent, transport, readinessURL, timeout)
		},
		interval: interval,
	}
}

func (g *readinessGate) wait(ctx context.Context) bool {
	g.mu.Lock()
	g.waiters++
	if g.readyAt(time.Now()) {
		g.mu.Unlock()
		g.doneWaiting()
		return true
	}
	if !g.polling {
		g.polling = true
		g.failedCheck = false
		g.readyCh = make(chan struct{})
		g.stopCh = make(chan struct{})
		go g.poll(g.readyCh, g.stopCh)
	}
	readyCh := g.readyCh
	g.mu.Unlock()

	ready := false
	select {
	case <-readyCh:
		ready = true
	case <-ctx.Done():
	}
	g.doneWaiting()
	return ready
}

func (g *readinessGate) doneWaiting() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.waiters--
	if g.waiters != 0 {
		return
	}
	if g.polling {
		close(g.stopCh)
		g.polling = false
		g.readyCh = nil
		g.stopCh = nil
	}
}

func (g *readinessGate) status() (*bool, time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ready := g.readyAt(time.Now())
	if ready || (g.polling && g.failedCheck) {
		return &ready, g.lastReadyAt
	}
	return nil, g.lastReadyAt
}

func (g *readinessGate) readyAt(now time.Time) bool {
	return now.Before(g.readyUntil)
}

func (g *readinessGate) poll(readyCh, stopCh chan struct{}) {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		if g.check(context.Background()) {
			g.mu.Lock()
			if g.polling && g.readyCh == readyCh && g.stopCh == stopCh {
				g.lastReadyAt = time.Now()
				g.readyUntil = g.lastReadyAt.Add(g.interval)
				g.polling = false
				close(readyCh)
			}
			g.mu.Unlock()
			return
		}
		g.mu.Lock()
		if g.polling && g.readyCh == readyCh && g.stopCh == stopCh {
			g.failedCheck = true
		}
		g.mu.Unlock()
		select {
		case <-stopCh:
			return
		case <-ticker.C:
		}
	}
}

func backendReady(parent context.Context, transport http.RoundTripper, readinessURL *url.URL, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, readinessURL.String(), nil)
	if err != nil {
		return false
	}
	response, err := transport.RoundTrip(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return response.StatusCode >= 200 && response.StatusCode < 300
}
