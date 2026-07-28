package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ZerGo0/nginx-retry-proxy/internal/config"
	"github.com/ZerGo0/nginx-retry-proxy/internal/holdproxy"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	service, err := holdproxy.New(cfg.HoldProxy)
	if err != nil {
		logger.Error("create hold proxy", "error", err)
		os.Exit(1)
	}

	proxyServer := &http.Server{
		Addr:              cfg.ProxyListenAddr,
		Handler:           service,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	adminServer := &http.Server{
		Addr:              cfg.AdminListenAddr,
		Handler:           service.AdminHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsCh := make(chan error, 2)
	go serve(logger, "proxy", proxyServer, errorsCh)
	go serve(logger, "admin", adminServer, errorsCh)

	serverFailed := false
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errorsCh:
		logger.Error("server failed", "error", err)
		serverFailed = true
	}
	stop()

	var shutdowns sync.WaitGroup
	shutdowns.Add(2)
	go shutdown(logger, "proxy", proxyServer, cfg.ShutdownTimeout, &shutdowns)
	go shutdown(logger, "admin", adminServer, cfg.ShutdownTimeout, &shutdowns)
	shutdowns.Wait()
	if serverFailed {
		os.Exit(1)
	}
}

func shutdown(logger *slog.Logger, name string, server *http.Server, timeout time.Duration, shutdowns *sync.WaitGroup) {
	defer shutdowns.Done()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error(name+" server shutdown failed", "error", err)
	}
}

func serve(logger *slog.Logger, name string, server *http.Server, errorsCh chan<- error) {
	logger.Info("server listening", "server", name, "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errorsCh <- errors.New(name + " server: " + err.Error())
	}
}
