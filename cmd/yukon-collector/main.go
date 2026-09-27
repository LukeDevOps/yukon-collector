// Command yukon-collector is the ingest/decode layer for the yukon agent's
// OTLP-style push export. It has no storage of its own: decoded payloads
// go to an ingest.Sink, either a forward.ForwardingSink that relays them
// to a backend or, with no backend configured, a LogSink that only logs
// them. With no arguments it serves; "healthcheck" probes the collector's
// own /healthz and exits 0 or 1, for use as the container image's
// HEALTHCHECK. Any other argument list exits 2 with a usage line.
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
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/LukeDevOps/yukon-collector/internal/auth"
	"github.com/LukeDevOps/yukon-collector/internal/forward"
	"github.com/LukeDevOps/yukon-collector/internal/processor"
	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
	"github.com/LukeDevOps/yukon-collector/internal/tokenfile"
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

	// authFileLabel and forwardFileLabel name the two token files in logs
	// and in the reload failure counter.
	authFileLabel    = "auth"
	forwardFileLabel = "forward"
)

func main() {
	if len(os.Args) > 1 {
		os.Exit(runSubcommand(context.Background(), os.Args[1:], os.Getenv, os.Stderr))
	}

	level := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	logLevel, err := resolveLogLevel(os.Getenv("YUKON_COLLECTOR_LOG_LEVEL"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	level.Set(logLevel)

	addr := resolveAddr(os.Getenv("YUKON_COLLECTOR_ADDR"))

	authTokens, authFile, err := resolveAuthTokens(os.Getenv)
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

	forwardCfg, forwardFile, err := resolveForwardConfig(os.Getenv)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	envCfg, err := resolveEnvironment(os.Getenv("YUKON_COLLECTOR_ENVIRONMENT"), os.Getenv("YUKON_COLLECTOR_ENVIRONMENT_ACTION"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	nsCfg, err := resolveNamespace(os.Getenv("YUKON_COLLECTOR_SERVICE_NAMESPACE"), os.Getenv("YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	redactCfg, err := resolveRedaction(os.Getenv("YUKON_COLLECTOR_REDACT_BLOCKED_VALUES"), os.Getenv("YUKON_COLLECTOR_REDACT_ALL_LITERALS"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	mux := http.NewServeMux()
	fwd, err := registerRoutes(mux, logger, authTokens, limiter, forwardCfg, envCfg, nsCfg, redactCfg)
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

	if authFile != nil {
		go authFile.Watch(ctx, tokenfile.ReloadInterval, logger)
	}
	if forwardFile != nil && fwd != nil {
		go forwardFile.Watch(ctx, tokenfile.ReloadInterval, logger)
	}

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

// resolveAuthTokens decides the tokens (if any) the ingest routes
// require. YUKON_COLLECTOR_AUTH_TOKEN holds one token or a comma-separated
// list. YUKON_COLLECTOR_AUTH_TOKEN_FILE names a file with one token per
// line, which the caller re-reads through the returned tokenfile.File.
// Setting both is an error.
//
// It fails closed. With neither variable set, it is an error unless
// YUKON_COLLECTOR_INSECURE_NO_AUTH explicitly opts out, and then both
// results are nil. A token file that cannot be read or holds no token is
// an error even with the opt-out, since the operator asked for that file.
func resolveAuthTokens(getenv func(string) string) (*auth.TokenSet, *tokenfile.File, error) {
	raw := getenv("YUKON_COLLECTOR_AUTH_TOKEN")
	path := getenv("YUKON_COLLECTOR_AUTH_TOKEN_FILE")
	if raw != "" && path != "" {
		return nil, nil, errors.New("YUKON_COLLECTOR_AUTH_TOKEN and YUKON_COLLECTOR_AUTH_TOKEN_FILE are both set; set one")
	}

	if path != "" {
		tokens := new(auth.TokenSet)
		file, err := tokenfile.Open(path, authFileLabel, tokenfile.AtLeastOne, tokens.Store)
		if err != nil {
			return nil, nil, fmt.Errorf("YUKON_COLLECTOR_AUTH_TOKEN_FILE: %w", err)
		}
		return tokens, file, nil
	}

	if raw != "" {
		list, err := parseTokenList(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("YUKON_COLLECTOR_AUTH_TOKEN: %w", err)
		}
		return auth.NewTokenSet(list), nil, nil
	}

	insecureNoAuth, _ := strconv.ParseBool(getenv("YUKON_COLLECTOR_INSECURE_NO_AUTH"))
	if insecureNoAuth {
		return nil, nil, nil
	}
	return nil, nil, errors.New("neither YUKON_COLLECTOR_AUTH_TOKEN nor YUKON_COLLECTOR_AUTH_TOKEN_FILE is set; " +
		"refusing to start without auth (set YUKON_COLLECTOR_INSECURE_NO_AUTH=1 to run unauthenticated)")
}

// parseTokenList splits a comma-separated token list and trims spaces
// around each token. An empty entry is an error that gives its position.
func parseTokenList(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	tokens := make([]string, 0, len(parts))
	for i, part := range parts {
		token := strings.TrimSpace(part)
		if token == "" {
			return nil, fmt.Errorf("entry %d of %d is empty", i+1, len(parts))
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

// forwardKey is a forward.TokenSource whose key a token file replaces.
type forwardKey struct {
	key atomic.Pointer[string]
}

// Token returns the last key stored, or "" before the first store.
func (k *forwardKey) Token() string {
	if p := k.key.Load(); p != nil {
		return *p
	}
	return ""
}

// store keeps the first of tokens. The file check allows only one.
func (k *forwardKey) store(tokens []string) {
	k.key.Store(&tokens[0])
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

// resolveForwardConfig reads the forwarding settings from the environment
// via getenv. Only YUKON_COLLECTOR_FORWARD_URL decides whether forwarding
// is on; the rest tune it and fall back to the forward package's defaults
// when unset. A value that is present but not a positive number or
// duration is an error.
//
// The backend key comes from YUKON_COLLECTOR_FORWARD_AUTH_TOKEN, with
// spaces around it trimmed, or from the file that
// YUKON_COLLECTOR_FORWARD_AUTH_TOKEN_FILE names, which must hold exactly
// one token. Setting both is an error, and so is a variable that holds
// only spaces. For a file, the caller re-reads it through the returned
// tokenfile.File.
func resolveForwardConfig(getenv func(string) string) (forward.Config, *tokenfile.File, error) {
	cfg := forward.Config{URL: getenv("YUKON_COLLECTOR_FORWARD_URL")}

	raw := getenv("YUKON_COLLECTOR_FORWARD_AUTH_TOKEN")
	token := strings.TrimSpace(raw)
	path := getenv("YUKON_COLLECTOR_FORWARD_AUTH_TOKEN_FILE")
	var file *tokenfile.File
	switch {
	case raw != "" && path != "":
		return forward.Config{}, nil, errors.New(
			"YUKON_COLLECTOR_FORWARD_AUTH_TOKEN and YUKON_COLLECTOR_FORWARD_AUTH_TOKEN_FILE are both set; set one")
	case raw != "" && token == "":
		return forward.Config{}, nil, errors.New("YUKON_COLLECTOR_FORWARD_AUTH_TOKEN holds only spaces")
	case path != "":
		key := new(forwardKey)
		var err error
		if file, err = tokenfile.Open(path, forwardFileLabel, tokenfile.ExactlyOne, key.store); err != nil {
			return forward.Config{}, nil, fmt.Errorf("YUKON_COLLECTOR_FORWARD_AUTH_TOKEN_FILE: %w", err)
		}
		cfg.AuthToken = key
	case token != "":
		cfg.AuthToken = forward.StaticToken(token)
	}

	var err error
	if cfg.Shards, err = positiveIntEnv(getenv, "YUKON_COLLECTOR_FORWARD_SHARDS"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.QueueSize, err = positiveIntEnv(getenv, "YUKON_COLLECTOR_FORWARD_QUEUE_SIZE"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RequestTimeout, err = positiveDurationEnv(getenv, "YUKON_COLLECTOR_FORWARD_REQUEST_TIMEOUT"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RetryInitialInterval, err = positiveDurationEnv(getenv, "YUKON_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RetryMaxInterval, err = positiveDurationEnv(getenv, "YUKON_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RetryMaxElapsedTime, err = positiveDurationEnv(getenv, "YUKON_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME"); err != nil {
		return forward.Config{}, nil, err
	}
	return cfg, file, nil
}

// positiveIntEnv returns the named variable as an int greater than zero,
// or zero when it is unset.
func positiveIntEnv(getenv func(string) string, name string) (int, error) {
	raw := getenv(name)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return n, nil
}

// positiveDurationEnv returns the named variable as a duration greater
// than zero (Go syntax such as "30s" or "5m"), or zero when it is unset.
func positiveDurationEnv(getenv func(string) string, name string) (time.Duration, error) {
	raw := getenv(name)
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return d, nil
}

// resolveEnvironment builds the processor.EnvironmentConfig from
// YUKON_COLLECTOR_ENVIRONMENT and YUKON_COLLECTOR_ENVIRONMENT_ACTION. A
// value that is blank after trimming space turns the processor off. An
// action with no value is an error: the operator meant to label
// payloads, and starting without the label would hide that mistake.
func resolveEnvironment(valueRaw, actionRaw string) (processor.EnvironmentConfig, error) {
	value := strings.TrimSpace(valueRaw)

	action, err := processor.ParseAction(actionRaw)
	if err != nil {
		return processor.EnvironmentConfig{}, fmt.Errorf("YUKON_COLLECTOR_ENVIRONMENT_ACTION: %w", err)
	}

	if value == "" {
		if actionRaw != "" {
			return processor.EnvironmentConfig{}, errors.New(
				"YUKON_COLLECTOR_ENVIRONMENT_ACTION is set but YUKON_COLLECTOR_ENVIRONMENT is empty; " +
					"set YUKON_COLLECTOR_ENVIRONMENT or unset YUKON_COLLECTOR_ENVIRONMENT_ACTION")
		}
		return processor.EnvironmentConfig{}, nil
	}

	return processor.EnvironmentConfig{Value: value, Action: action}, nil
}

// resolveNamespace builds the processor.NamespaceConfig from
// YUKON_COLLECTOR_SERVICE_NAMESPACE and
// YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION. A value that is blank after
// trimming space turns the processor off, so each agent's namespace
// passes through unchanged. An action with no value is an error: the
// operator meant to set a namespace, and starting without it would hide
// that mistake. A value of "." or ".." is an error, since the ingest
// handler rejects that namespace from an agent too.
func resolveNamespace(valueRaw, actionRaw string) (processor.NamespaceConfig, error) {
	value := strings.TrimSpace(valueRaw)

	action, err := processor.ParseAction(actionRaw)
	if err != nil {
		return processor.NamespaceConfig{}, fmt.Errorf("YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION: %w", err)
	}

	if value == "" {
		if actionRaw != "" {
			return processor.NamespaceConfig{}, errors.New(
				"YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION is set but YUKON_COLLECTOR_SERVICE_NAMESPACE is empty; " +
					"set YUKON_COLLECTOR_SERVICE_NAMESPACE or unset YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION")
		}
		return processor.NamespaceConfig{}, nil
	}
	if value == "." || value == ".." {
		return processor.NamespaceConfig{}, fmt.Errorf("YUKON_COLLECTOR_SERVICE_NAMESPACE is %q, which no URL path can name", value)
	}

	return processor.NamespaceConfig{Value: value, Action: action}, nil
}

// resolveRedaction builds the processor.RedactionConfig from
// YUKON_COLLECTOR_REDACT_BLOCKED_VALUES and
// YUKON_COLLECTOR_REDACT_ALL_LITERALS. The first holds one regular
// expression per line, since a comma can appear inside a pattern. A line
// that is blank after trimming space is skipped. Other lines are used as
// written, apart from a trailing carriage return. A pattern that does not
// compile is an error that names its line. The second is a boolean in
// strconv.ParseBool syntax. An unparsable value is an error, so a typo
// cannot leave redaction off.
func resolveRedaction(blockedRaw, allLiteralsRaw string) (processor.RedactionConfig, error) {
	var cfg processor.RedactionConfig
	for i, line := range strings.Split(blockedRaw, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		re, err := regexp.Compile(line)
		if err != nil {
			return processor.RedactionConfig{}, fmt.Errorf("YUKON_COLLECTOR_REDACT_BLOCKED_VALUES line %d: %w", i+1, err)
		}
		cfg.BlockedValues = append(cfg.BlockedValues, re)
	}

	if allLiteralsRaw != "" {
		all, err := strconv.ParseBool(allLiteralsRaw)
		if err != nil {
			return processor.RedactionConfig{}, fmt.Errorf("YUKON_COLLECTOR_REDACT_ALL_LITERALS: %w", err)
		}
		cfg.AllLiterals = all
	}
	return cfg, nil
}
