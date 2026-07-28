package holdproxy_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ZerGo0/nginx-retry-proxy/internal/holdproxy"
)

func TestPOSTWaitsForReadinessAndIsForwardedOnce(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	var backendRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			if !ready.Load() {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		backendRequests.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read backend request body: %v", err)
			return
		}
		if r.Method != http.MethodPost || r.URL.RequestURI() != "/orders?source=test" || r.Header.Get("X-Request-ID") != "request-1" || string(body) != `{"total":42}` {
			t.Errorf("backend request was not preserved: method=%s uri=%s request_id=%s body=%s", r.Method, r.URL.RequestURI(), r.Header.Get("X-Request-ID"), body)
		}
		w.Header().Set("X-Backend", "recovered")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"order-1"}`))
	}))
	t.Cleanup(backend.Close)

	handler, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     50 * time.Millisecond,
		MaxHeldRequests:   10,
		ForwardConcurrent: 2,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	type result struct {
		response *http.Response
		body     []byte
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		req, requestErr := http.NewRequestWithContext(context.Background(), http.MethodPost, proxy.URL+"/orders?source=test", bytes.NewBufferString(`{"total":42}`))
		if requestErr != nil {
			resultCh <- result{err: requestErr}
			return
		}
		req.Header.Set("X-Request-ID", "request-1")
		response, requestErr := http.DefaultClient.Do(req)
		if requestErr != nil {
			resultCh <- result{err: requestErr}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		resultCh <- result{response: response, body: body, err: readErr}
	}()

	select {
	case early := <-resultCh:
		t.Fatalf("request completed before readiness: status=%v error=%v", early.response, early.err)
	case <-time.After(50 * time.Millisecond):
	}

	ready.Store(true)
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("held request failed: %v", got.err)
	}
	if got.response.StatusCode != http.StatusCreated || got.response.Header.Get("X-Backend") != "recovered" || string(got.body) != `{"id":"order-1"}` {
		t.Fatalf("unexpected response: status=%d x-backend=%q body=%s", got.response.StatusCode, got.response.Header.Get("X-Backend"), got.body)
	}
	if got := backendRequests.Load(); got != 1 {
		t.Fatalf("backend received %d requests, want 1", got)
	}
}

func TestAdministrationListenerReportsLivenessStatusAndMetrics(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(backend.Close)
	service, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL,
		HoldTimeout:       100 * time.Millisecond,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     10 * time.Millisecond,
		MaxHeldRequests:   7,
		ForwardConcurrent: 3,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	admin := httptest.NewServer(service.AdminHandler())
	t.Cleanup(admin.Close)

	for _, test := range []struct {
		path        string
		contentType string
		body        string
	}{
		{path: "/livez", contentType: "text/plain", body: "ok\n"},
		{path: "/status", contentType: "application/json", body: `"max_held_requests":7`},
		{path: "/metrics", contentType: "text/plain", body: "hold_proxy_held_requests"},
	} {
		response, requestErr := http.Get(admin.URL + test.path)
		if requestErr != nil {
			t.Fatalf("GET %s: %v", test.path, requestErr)
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			t.Fatalf("read %s: %v", test.path, readErr)
		}
		if response.StatusCode != http.StatusOK || !bytes.Contains([]byte(response.Header.Get("Content-Type")), []byte(test.contentType)) || !bytes.Contains(body, []byte(test.body)) {
			t.Fatalf("GET %s: status=%d content-type=%q body=%s", test.path, response.StatusCode, response.Header.Get("Content-Type"), body)
		}
	}
}

func TestUpgradeRequestsAreRejectedWithoutWaitingOrForwarding(t *testing.T) {
	t.Parallel()

	var backendRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		backendRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(backend.Close)
	service, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     10 * time.Millisecond,
		MaxHeldRequests:   2,
		ForwardConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(service)
	t.Cleanup(proxy.Close)

	request, err := http.NewRequest(http.MethodGet, proxy.URL+"/socket", nil)
	if err != nil {
		t.Fatalf("create upgrade request: %v", err)
	}
	request.Header.Set("Connection", "upgrade")
	request.Header.Set("Upgrade", "websocket")
	started := time.Now()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upgrade request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("upgrade status=%d, want 426", response.StatusCode)
	}
	if time.Since(started) > 200*time.Millisecond {
		t.Fatal("upgrade request was held instead of rejected")
	}
	if got := backendRequests.Load(); got != 0 {
		t.Fatalf("backend received %d requests, want none", got)
	}
}

func TestInvalidConfigurationIsRejected(t *testing.T) {
	t.Parallel()

	valid := holdproxy.Config{
		BackendURL:        "http://backend:3000",
		ReadinessURL:      "http://backend:3000/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     10 * time.Millisecond,
		MaxHeldRequests:   2,
		ForwardConcurrent: 1,
	}
	if _, err := holdproxy.New(valid); err != nil {
		t.Fatalf("New rejected valid configuration: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*holdproxy.Config)
	}{
		{name: "missing backend URL", mutate: func(cfg *holdproxy.Config) { cfg.BackendURL = "" }},
		{name: "relative readiness URL", mutate: func(cfg *holdproxy.Config) { cfg.ReadinessURL = "/ready" }},
		{name: "zero hold timeout", mutate: func(cfg *holdproxy.Config) { cfg.HoldTimeout = 0 }},
		{name: "request timeout equal to hold timeout", mutate: func(cfg *holdproxy.Config) { cfg.RequestTimeout = cfg.HoldTimeout }},
		{name: "zero health interval", mutate: func(cfg *holdproxy.Config) { cfg.HealthInterval = 0 }},
		{name: "zero health timeout", mutate: func(cfg *holdproxy.Config) { cfg.HealthTimeout = 0 }},
		{name: "zero held capacity", mutate: func(cfg *holdproxy.Config) { cfg.MaxHeldRequests = 0 }},
		{name: "zero forward capacity", mutate: func(cfg *holdproxy.Config) { cfg.ForwardConcurrent = 0 }},
		{name: "non-HTTP backend scheme", mutate: func(cfg *holdproxy.Config) { cfg.BackendURL = "ftp://backend:3000" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if _, err := holdproxy.New(cfg); err == nil {
				t.Fatal("New returned nil error")
			}
		})
	}
}

func TestForwardingIsBoundedByAbsoluteRequestTimeout(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(backend.Close)
	service, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       80 * time.Millisecond,
		RequestTimeout:    120 * time.Millisecond,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     10 * time.Millisecond,
		MaxHeldRequests:   2,
		ForwardConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(service)
	t.Cleanup(proxy.Close)

	started := time.Now()
	response, err := http.Get(proxy.URL + "/hang")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want 502", response.StatusCode)
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("request completed after %v, want absolute timeout near 120ms", elapsed)
	}
	admin := httptest.NewServer(service.AdminHandler())
	t.Cleanup(admin.Close)
	metricsResponse, err := http.Get(admin.URL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	metricsBody, readErr := io.ReadAll(metricsResponse.Body)
	_ = metricsResponse.Body.Close()
	if readErr != nil {
		t.Fatalf("read metrics: %v", readErr)
	}
	if !bytes.Contains(metricsBody, []byte("hold_proxy_request_timeouts_total 1\n")) || !bytes.Contains(metricsBody, []byte("hold_proxy_forward_errors_total 0\n")) {
		t.Fatalf("unexpected timeout metrics:\n%s", metricsBody)
	}
}

func TestRequestBeyondHoldCapacityFailsImmediately(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(backend.Close)

	handler, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL,
		HoldTimeout:       time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     20 * time.Millisecond,
		MaxHeldRequests:   1,
		ForwardConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone := make(chan struct{})
	firstRequest, err := http.NewRequestWithContext(firstCtx, http.MethodGet, proxy.URL+"/first", nil)
	if err != nil {
		t.Fatalf("create first request: %v", err)
	}
	go func() {
		response, requestErr := http.DefaultClient.Do(firstRequest)
		if requestErr == nil {
			_ = response.Body.Close()
		}
		close(firstDone)
	}()
	time.Sleep(50 * time.Millisecond)
	started := time.Now()
	response, err := http.Get(proxy.URL + "/second")
	if err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second status=%d, want 503", response.StatusCode)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("capacity rejection took %v, want immediate response", elapsed)
	}
	cancelFirst()
	<-firstDone
}

func TestBackendFiveHundredIsPassedThroughWithoutRetry(t *testing.T) {
	t.Parallel()

	var backendRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		backendRequests.Add(1)
		w.Header().Set("X-Application-Response", "true")
		http.Error(w, "application failure", http.StatusInternalServerError)
	}))
	t.Cleanup(backend.Close)
	service := newTestService(t, backend.URL, backend.URL+"/ready", 500*time.Millisecond)
	proxy := httptest.NewServer(service)
	t.Cleanup(proxy.Close)

	response, err := http.Post(proxy.URL+"/commands", "application/json", bytes.NewBufferString(`{"command":"run"}`))
	if err != nil {
		t.Fatalf("POST command: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError || response.Header.Get("X-Application-Response") != "true" {
		t.Fatalf("response was not passed through: status=%d header=%q", response.StatusCode, response.Header.Get("X-Application-Response"))
	}
	if got := backendRequests.Load(); got != 1 {
		t.Fatalf("backend received %d requests, want exactly 1", got)
	}
}

func TestForwardFailureAfterBackendReadsPOSTIsNotRetried(t *testing.T) {
	t.Parallel()

	var backendRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		backendRequests.Add(1)
		if _, err := io.ReadAll(r.Body); err != nil {
			t.Errorf("read backend request: %v", err)
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("backend response writer does not support hijacking")
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack backend connection: %v", err)
			return
		}
		_ = connection.Close()
	}))
	t.Cleanup(backend.Close)
	service := newTestService(t, backend.URL, backend.URL+"/ready", 500*time.Millisecond)
	proxy := httptest.NewServer(service)
	t.Cleanup(proxy.Close)

	response, err := http.Post(proxy.URL+"/payments", "application/json", bytes.NewBufferString(`{"amount":42}`))
	if err != nil {
		t.Fatalf("POST payment: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status=%d, want 502", response.StatusCode)
	}
	if got := backendRequests.Load(); got != 1 {
		t.Fatalf("backend received %d requests after ambiguous failure, want exactly 1", got)
	}
}

func TestCanceledHeldRequestIsNeverForwarded(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	var backendRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			if !ready.Load() {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		backendRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	service := newTestService(t, backend.URL, backend.URL+"/ready", time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "http://proxy.local/commands", bytes.NewBufferString("command")).WithContext(ctx)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		service.ServeHTTP(response, request)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	ready.Store(true)
	time.Sleep(50 * time.Millisecond)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", response.Code)
	}
	if got := backendRequests.Load(); got != 0 {
		t.Fatalf("backend received %d canceled requests, want 0", got)
	}
}

func TestHoldTimeoutReturnsServiceUnavailableWithoutForwarding(t *testing.T) {
	t.Parallel()

	var backendRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		backendRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	service := newTestService(t, backend.URL, backend.URL+"/ready", 80*time.Millisecond)
	proxy := httptest.NewServer(service)
	t.Cleanup(proxy.Close)

	response, err := http.Post(proxy.URL+"/commands", "text/plain", bytes.NewBufferString("command"))
	if err != nil {
		t.Fatalf("POST held request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", response.StatusCode)
	}
	if got := backendRequests.Load(); got != 0 {
		t.Fatalf("backend received %d expired requests, want 0", got)
	}
}

func TestRequestBodyIsNotReadUntilBackendIsReady(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			if !ready.Load() {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "payload" {
			t.Errorf("backend body=%q error=%v", body, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	service := newTestService(t, backend.URL, backend.URL+"/ready", time.Second)

	readStarted := make(chan struct{}, 1)
	body := &notifyingReader{Reader: bytes.NewBufferString("payload"), started: readStarted}
	request := httptest.NewRequest(http.MethodPost, "http://proxy.local/commands", body)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		service.ServeHTTP(response, request)
		close(done)
	}()

	select {
	case <-readStarted:
		t.Fatal("hold proxy read the request body while waiting")
	case <-time.After(50 * time.Millisecond):
	}
	ready.Store(true)
	<-done
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", response.Code)
	}
	select {
	case <-readStarted:
	default:
		t.Fatal("request body was not read during forwarding")
	}
}

type notifyingReader struct {
	io.Reader
	started chan<- struct{}
	once    atomic.Bool
}

func (r *notifyingReader) Read(buffer []byte) (int, error) {
	if r.once.CompareAndSwap(false, true) {
		r.started <- struct{}{}
	}
	return r.Reader.Read(buffer)
}

func newTestService(t *testing.T, backendURL, readinessURL string, holdTimeout time.Duration) *holdproxy.Service {
	t.Helper()
	service, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backendURL,
		ReadinessURL:      readinessURL,
		HoldTimeout:       holdTimeout,
		RequestTimeout:    holdTimeout + time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     20 * time.Millisecond,
		MaxHeldRequests:   10,
		ForwardConcurrent: 2,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	return service
}

func TestRecoveredRequestsAreReleasedWithinForwardConcurrency(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maxActive.Load()
			if current <= observed || maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		if r.URL.Path == "/first" {
			close(firstStarted)
			<-releaseFirst
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	handler, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     20 * time.Millisecond,
		MaxHeldRequests:   4,
		ForwardConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	firstDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get(proxy.URL + "/first")
		if requestErr == nil {
			_ = response.Body.Close()
		}
		firstDone <- requestErr
	}()
	<-firstStarted
	metrics := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "http://admin.local/metrics", nil))
	if !bytes.Contains(metrics.Body.Bytes(), []byte("hold_proxy_held_requests 0\n")) {
		t.Fatalf("forwarding request was still counted as held:\n%s", metrics.Body.String())
	}

	secondDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get(proxy.URL + "/second")
		if requestErr == nil {
			_ = response.Body.Close()
		}
		secondDone <- requestErr
	}()

	var secondEarly error
	secondCompletedEarly := false
	select {
	case secondEarly = <-secondDone:
		secondCompletedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	if secondCompletedEarly {
		t.Fatalf("second request completed while first occupied release slot: %v", secondEarly)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("backend saw %d concurrent requests, want 1", got)
	}
}

func TestHeldRequestsShareReadinessPolling(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	var healthChecks atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			healthChecks.Add(1)
			if !ready.Load() {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	handler, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    10 * time.Millisecond,
		HealthTimeout:     20 * time.Millisecond,
		MaxHeldRequests:   20,
		ForwardConcurrent: 20,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	const requestCount = 10
	done := make(chan error, requestCount)
	for index := 0; index < requestCount; index++ {
		go func() {
			response, requestErr := http.Get(proxy.URL + "/held")
			if requestErr == nil {
				_ = response.Body.Close()
			}
			done <- requestErr
		}()
	}
	time.Sleep(100 * time.Millisecond)
	checksBeforeReady := healthChecks.Load()
	ready.Store(true)
	for index := 0; index < requestCount; index++ {
		if err := <-done; err != nil {
			t.Fatalf("held request failed: %v", err)
		}
	}
	if checksBeforeReady > 15 {
		t.Fatalf("readiness endpoint received %d checks for %d held requests; polling was not shared", checksBeforeReady, requestCount)
	}
}

func TestSequentialRequestsReuseRecentReadiness(t *testing.T) {
	t.Parallel()

	var healthChecks atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			healthChecks.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	handler, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    5 * time.Second,
		HealthTimeout:     20 * time.Millisecond,
		MaxHeldRequests:   2,
		ForwardConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	for index := 0; index < 5; index++ {
		response, requestErr := http.Get(proxy.URL + "/sequential")
		if requestErr != nil {
			t.Fatalf("request %d failed: %v", index, requestErr)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("request %d status=%d, want 204", index, response.StatusCode)
		}
	}
	if got := healthChecks.Load(); got != 1 {
		t.Fatalf("readiness endpoint received %d checks for sequential requests, want 1", got)
	}
}

func TestStatusReportsStaleReadinessAsUnknown(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	handler, err := holdproxy.New(holdproxy.Config{
		BackendURL:        backend.URL,
		ReadinessURL:      backend.URL + "/ready",
		HoldTimeout:       time.Second,
		HealthInterval:    20 * time.Millisecond,
		HealthTimeout:     20 * time.Millisecond,
		MaxHeldRequests:   2,
		ForwardConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("create hold proxy: %v", err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	response, err := http.Get(proxy.URL + "/request")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = response.Body.Close()

	deadline := time.Now().Add(time.Second)
	for {
		status := httptest.NewRecorder()
		handler.AdminHandler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "http://admin.local/status", nil))
		body := status.Body.Bytes()
		if bytes.Contains(body, []byte(`"backend_ready":null`)) && bytes.Contains(body, []byte(`"backend_last_ready_at"`)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("readiness did not become unknown after the observation expired: %s", body)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
