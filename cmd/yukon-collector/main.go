// Command yukon-collector is the ingest/decode layer for the yukon agent's
// OTLP-style push export. It has no storage of its own: decoded payloads
// go to an ingest.Sink, either a forward.ForwardingSink that relays them
// to a backend or, with no backend configured, a LogSink that only logs
// them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
)

const (
	defaultAddr     = ":4319"
	shutdownTimeout = 10 * time.Second

	// defaultRateLimitRPS and defaultRateLimitBurst size the per-client-IP
	// token bucket around the agent's flush interval (every 30-60s): a
	// handful of instances sharing one NAT'd IP should never trip it, but
	// a request storm still gets capped.
	defaultRateLimitRPS   = 5
	defaultRateLimitBurst = 20
)

func main() {
	level := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	logLevel, err := resolveLogLevel(os.Getenv("YUKON_COLLECTOR_LOG_LEVEL"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	level.Set(logLevel)

	addr := os.Getenv("YUKON_COLLECTOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	authToken, err := resolveAuthToken(os.Getenv("YUKON_COLLECTOR_AUTH_TOKEN"), os.Getenv("YUKON_COLLECTOR_INSECURE_NO_AUTH"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	rps, burst, err := resolveRateLimit(os.Getenv("YUKON_COLLECTOR_RATE_LIMIT_RPS"), os.Getenv("YUKON_COLLECTOR_RATE_LIMIT_BURST"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	var limiter *ratelimit.Limiter
	if rps > 0 {
		var opts []ratelimit.Option
		if header := os.Getenv("YUKON_COLLECTOR_CLIENT_IP_HEADER"); header != "" {
			opts = append(opts, ratelimit.WithClientIPHeader(header))
		}
		limiter = ratelimit.New(rps, burst, opts...)
		defer limiter.Stop()
	}

	forwardURL := os.Getenv("YUKON_COLLECTOR_FORWARD_URL")
	forwardAuthToken := os.Getenv("YUKON_COLLECTOR_FORWARD_AUTH_TOKEN")

	mux := http.NewServeMux()
	fwd, err := registerRoutes(mux, logger, authToken, limiter, forwardURL, forwardAuthToken)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

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
		srvErr := srv.Shutdown(shutdownCtx)
		if fwd != nil {
			fwd.Shutdown(shutdownCtx)
		}
		if srvErr != nil {
			logger.Error("graceful shutdown failed", "error", srvErr)
			os.Exit(1)
		}
	}
}

// resolveLogLevel parses YUKON_COLLECTOR_LOG_LEVEL (debug, info, warn, or
// error, in any case). Unset means info.
func resolveLogLevel(raw string) (slog.Level, error) {
	if raw == "" {
		return slog.LevelInfo, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("YUKON_COLLECTOR_LOG_LEVEL: %w", err)
	}
	return level, nil
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

// resolveRateLimit decides the per-client-IP request rate and burst size
// for the ingest routes, applying defaults for unset env vars. A rate of
// 0 disables rate limiting entirely (rpsRaw = "0"). Unlike auth, this
// doesn't fail closed: a misconfigured limit falls back to blocking
// startup only when the value is present but unparsable, not when it's
// merely absent.
func resolveRateLimit(rpsRaw, burstRaw string) (rate.Limit, int, error) {
	rps := float64(defaultRateLimitRPS)
	if rpsRaw != "" {
		parsed, err := strconv.ParseFloat(rpsRaw, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("YUKON_COLLECTOR_RATE_LIMIT_RPS: %w", err)
		}
		rps = parsed
	}

	burst := defaultRateLimitBurst
	if burstRaw != "" {
		parsed, err := strconv.Atoi(burstRaw)
		if err != nil {
			return 0, 0, fmt.Errorf("YUKON_COLLECTOR_RATE_LIMIT_BURST: %w", err)
		}
		burst = parsed
	}

	if math.IsNaN(rps) || math.IsInf(rps, 0) {
		return 0, 0, errors.New("YUKON_COLLECTOR_RATE_LIMIT_RPS must be a finite number")
	}
	if rps < 0 {
		return 0, 0, errors.New("YUKON_COLLECTOR_RATE_LIMIT_RPS must not be negative")
	}
	if rps > 0 && burst <= 0 {
		return 0, 0, errors.New("YUKON_COLLECTOR_RATE_LIMIT_BURST must be positive when rate limiting is enabled")
	}

	return rate.Limit(rps), burst, nil
}
