// Command yukon-collector is the ingest/decode layer for the yukon agent's
// OTLP-style push export. It has no storage of its own; a real backend
// implements ingest.Sink and is wired in here in place of LogSink.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	defaultAddr     = ":4319"
	shutdownTimeout = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("YUKON_COLLECTOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	authToken, err := resolveAuthToken(os.Getenv("YUKON_COLLECTOR_AUTH_TOKEN"), os.Getenv("YUKON_COLLECTOR_INSECURE_NO_AUTH"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	mux := http.NewServeMux()
	registerRoutes(mux, logger, authToken)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("yukon-collector listening", "addr", addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		stop()
		logger.Info("shutting down")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			os.Exit(1)
		}
	}
}

// resolveAuthToken decides the token (if any) the ingest routes require.
// It fails closed: a missing token is an error unless insecureNoAuthRaw
// explicitly opts out, so forgetting to set the token stops the server
// at startup instead of running it unauthenticated.
func resolveAuthToken(token, insecureNoAuthRaw string) (string, error) {
	if token != "" {
		return token, nil
	}
	insecureNoAuth, _ := strconv.ParseBool(insecureNoAuthRaw)
	if insecureNoAuth {
		return "", nil
	}
	return "", errors.New("YUKON_COLLECTOR_AUTH_TOKEN not set; refusing to start without auth " +
		"(set YUKON_COLLECTOR_INSECURE_NO_AUTH=1 to run unauthenticated)")
}
