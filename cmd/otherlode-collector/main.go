// Command otherlode-collector is the ingest/decode layer for the
// Otherlode agent's OTLP-style push export. It has no storage of its own:
// decoded payloads go to an ingest.Sink, either a forward.ForwardingSink
// that relays them to a backend or, with no backend configured, a LogSink
// that only logs them. With no arguments it serves; "healthcheck" probes
// the collector's own /healthz and exits 0 or 1, for use as the container
// image's HEALTHCHECK. Any other argument list exits 2 with a usage line.
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
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/time/rate"

	"github.com/otherlodehq/otherlode-collector/internal/auth"
	"github.com/otherlodehq/otherlode-collector/internal/forward"
	"github.com/otherlodehq/otherlode-collector/internal/processor"
	"github.com/otherlodehq/otherlode-collector/internal/ratelimit"
	"github.com/otherlodehq/otherlode-collector/internal/tokenfile"
)

const (
	defaultAddr     = ":4319"
	shutdownTimeout = 10 * time.Second

	// defaultRateLimitRPS and defaultRateLimitBurst set the per-client-IP
	// token bucket. An agent flushes every 30 to 60 seconds, so several
	// instances behind one shared address stay far below 5 requests per
	// second. A request storm from one address is still capped.
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

	logLevel, err := resolveLogLevel(os.Getenv("OTHERLODE_COLLECTOR_LOG_LEVEL"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	level.Set(logLevel)

	addr := resolveAddr(os.Getenv("OTHERLODE_COLLECTOR_ADDR"))

	authTokens, authFile, err := resolveAuthTokens(os.Getenv)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	rps, burst, err := resolveRateLimit(os.Getenv("OTHERLODE_COLLECTOR_RATE_LIMIT_RPS"), os.Getenv("OTHERLODE_COLLECTOR_RATE_LIMIT_BURST"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	ipv6Prefix, err := resolveIPv6Prefix(os.Getenv("OTHERLODE_COLLECTOR_RATE_LIMIT_IPV6_PREFIX"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	var limiter *ratelimit.Limiter
	if rps > 0 {
		opts := []ratelimit.Option{ratelimit.WithIPv6Prefix(ipv6Prefix)}
		if header := os.Getenv("OTHERLODE_COLLECTOR_CLIENT_IP_HEADER"); header != "" {
			opts = append(opts, ratelimit.WithClientIPHeader(header))
		}
		limiter = ratelimit.New(rps, burst, opts...)
		defer limiter.Stop()
	}

	maxDecodes, err := resolveMaxConcurrentDecodes(os.Getenv)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	forwardCfg, forwardFile, err := resolveForwardConfig(os.Getenv)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	envCfg, err := resolveEnvironment(os.Getenv("OTHERLODE_COLLECTOR_ENVIRONMENT"), os.Getenv("OTHERLODE_COLLECTOR_ENVIRONMENT_ACTION"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	nsCfg, err := resolveNamespace(os.Getenv("OTHERLODE_COLLECTOR_SERVICE_NAMESPACE"), os.Getenv("OTHERLODE_COLLECTOR_SERVICE_NAMESPACE_ACTION"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	redactCfg, err := resolveRedaction(os.Getenv("OTHERLODE_COLLECTOR_REDACT_BLOCKED_VALUES"), os.Getenv("OTHERLODE_COLLECTOR_REDACT_ALL_LITERALS"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	mux := http.NewServeMux()
	fwd, err := registerRoutes(mux, routeConfig{
		Logger:      logger,
		AuthTokens:  authTokens,
		Limiter:     limiter,
		MaxDecodes:  maxDecodes,
		Forward:     forwardCfg,
		Environment: envCfg,
		Namespace:   nsCfg,
		Redaction:   redactCfg,
	})
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
		logger.Info("otherlode-collector listening", "addr", addr)
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

// resolveLogLevel parses OTHERLODE_COLLECTOR_LOG_LEVEL (debug, info, warn,
// or error, in any case). Unset means info.
func resolveLogLevel(raw string) (slog.Level, error) {
	if raw == "" {
		return slog.LevelInfo, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("OTHERLODE_COLLECTOR_LOG_LEVEL: %w", err)
	}
	return level, nil
}

// resolveAuthTokens decides the tokens (if any) the ingest routes require.
// OTHERLODE_COLLECTOR_AUTH_TOKEN holds one token or a comma-separated list.
// OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE names a file with one token per line,
// which the caller re-reads through the returned tokenfile.File. Setting
// both is an error.
//
// It fails closed. With neither variable set, it is an error unless
// OTHERLODE_COLLECTOR_INSECURE_NO_AUTH explicitly opts out, and then both
// results are nil. A token file that cannot be read or holds no token is
// an error even with the opt-out, since the operator asked for that file.
func resolveAuthTokens(getenv func(string) string) (*auth.TokenSet, *tokenfile.File, error) {
	raw := getenv("OTHERLODE_COLLECTOR_AUTH_TOKEN")
	path := getenv("OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE")
	if raw != "" && path != "" {
		return nil, nil, errors.New("OTHERLODE_COLLECTOR_AUTH_TOKEN and OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE are both set; set one")
	}

	if path != "" {
		tokens := new(auth.TokenSet)
		file, err := tokenfile.Open(path, authFileLabel, tokenfile.AtLeastOne, tokens.Store)
		if err != nil {
			return nil, nil, fmt.Errorf("OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE: %w", err)
		}
		return tokens, file, nil
	}

	if raw != "" {
		list, err := parseTokenList(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("OTHERLODE_COLLECTOR_AUTH_TOKEN: %w", err)
		}
		return auth.NewTokenSet(list), nil, nil
	}

	insecureNoAuth, _ := strconv.ParseBool(getenv("OTHERLODE_COLLECTOR_INSECURE_NO_AUTH"))
	if insecureNoAuth {
		return nil, nil, nil
	}
	return nil, nil, errors.New("neither OTHERLODE_COLLECTOR_AUTH_TOKEN nor OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE is set; " +
		"refusing to start without auth (set OTHERLODE_COLLECTOR_INSECURE_NO_AUTH=1 to run unauthenticated)")
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

// checkHeaderValue returns an error when key holds a control character,
// which an HTTP header value cannot carry. A request with such a key fails
// in the HTTP client, and the forwarder would retry it until it gave up.
func checkHeaderValue(key string) error {
	for i := 0; i < len(key); i++ {
		if c := key[i]; c < 0x20 || c == 0x7f {
			return fmt.Errorf("the key holds a control character at byte %d", i)
		}
	}
	return nil
}

// checkForwardKeys accepts a key file only when it holds exactly one key
// that is usable as a header value.
func checkForwardKeys(tokens []string) error {
	if err := tokenfile.ExactlyOne(tokens); err != nil {
		return err
	}
	return checkHeaderValue(tokens[0])
}

// resolveRateLimit returns the per-client-IP rate and burst for the ingest
// routes. An unset variable takes its default. A rate of 0 turns limiting
// off. A value that does not parse, a negative or non-finite rate, or a
// burst below 1 with limiting on is an error.
func resolveRateLimit(rpsRaw, burstRaw string) (rate.Limit, int, error) {
	rps := float64(defaultRateLimitRPS)
	if rpsRaw != "" {
		parsed, err := strconv.ParseFloat(rpsRaw, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("OTHERLODE_COLLECTOR_RATE_LIMIT_RPS: %w", err)
		}
		rps = parsed
	}

	burst := defaultRateLimitBurst
	if burstRaw != "" {
		parsed, err := strconv.Atoi(burstRaw)
		if err != nil {
			return 0, 0, fmt.Errorf("OTHERLODE_COLLECTOR_RATE_LIMIT_BURST: %w", err)
		}
		burst = parsed
	}

	if math.IsNaN(rps) || math.IsInf(rps, 0) {
		return 0, 0, errors.New("OTHERLODE_COLLECTOR_RATE_LIMIT_RPS must be a finite number")
	}
	if rps < 0 {
		return 0, 0, errors.New("OTHERLODE_COLLECTOR_RATE_LIMIT_RPS must not be negative")
	}
	if rps > 0 && burst <= 0 {
		return 0, 0, errors.New("OTHERLODE_COLLECTOR_RATE_LIMIT_BURST must be positive when rate limiting is enabled")
	}

	return rate.Limit(rps), burst, nil
}

// resolveIPv6Prefix returns the IPv6 prefix length, in bits, that the rate
// limiter keys on. An unset variable gives 128, one bucket per address.
// A value that is not an integer from 1 to 128 is an error.
func resolveIPv6Prefix(raw string) (int, error) {
	if raw == "" {
		return ratelimit.DefaultIPv6Prefix, nil
	}
	bits, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("OTHERLODE_COLLECTOR_RATE_LIMIT_IPV6_PREFIX: %w", err)
	}
	if bits < 1 || bits > 128 {
		return 0, errors.New("OTHERLODE_COLLECTOR_RATE_LIMIT_IPV6_PREFIX must be from 1 to 128")
	}
	return bits, nil
}

// resolveForwardConfig reads the forwarding settings from the environment
// via getenv. Only OTHERLODE_COLLECTOR_FORWARD_URL decides whether
// forwarding is on; the rest tune it and fall back to the forward package's
// defaults when unset. A value that is present but not a positive number or
// duration is an error.
//
// The backend key comes from OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN, with
// spaces around it trimmed, or from the file that
// OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE names, which must hold
// exactly one token. Setting both is an error, and so is a variable that
// holds only spaces. A key with a control character is an error, for a
// variable and for each read of a file. For a file, the caller re-reads it
// through the returned tokenfile.File.
//
// Setting either key variable without OTHERLODE_COLLECTOR_FORWARD_URL is an
// error, since the operator meant to forward.
func resolveForwardConfig(getenv func(string) string) (forward.Config, *tokenfile.File, error) {
	cfg := forward.Config{URL: getenv("OTHERLODE_COLLECTOR_FORWARD_URL")}

	raw := getenv("OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN")
	token := strings.TrimSpace(raw)
	path := getenv("OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE")
	if cfg.URL == "" && (raw != "" || path != "") {
		return forward.Config{}, nil, errors.New(
			"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN or OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE is set but OTHERLODE_COLLECTOR_FORWARD_URL is empty; " +
				"set OTHERLODE_COLLECTOR_FORWARD_URL or unset the key variable")
	}
	var file *tokenfile.File
	switch {
	case raw != "" && path != "":
		return forward.Config{}, nil, errors.New(
			"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN and OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE are both set; set one")
	case raw != "" && token == "":
		return forward.Config{}, nil, errors.New("OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN holds only spaces")
	case path != "":
		key := new(forwardKey)
		var err error
		if file, err = tokenfile.Open(path, forwardFileLabel, checkForwardKeys, key.store); err != nil {
			return forward.Config{}, nil, fmt.Errorf("OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE: %w", err)
		}
		cfg.AuthToken = key
	case token != "":
		if err := checkHeaderValue(token); err != nil {
			return forward.Config{}, nil, fmt.Errorf("OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN: %w", err)
		}
		cfg.AuthToken = forward.StaticToken(token)
	}

	var err error
	if cfg.Shards, err = positiveIntEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_SHARDS"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.QueueSize, err = positiveIntEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_QUEUE_SIZE"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.QueueBytes, err = positiveIntEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_QUEUE_BYTES"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RequestTimeout, err = positiveDurationEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_REQUEST_TIMEOUT"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RetryInitialInterval, err = positiveDurationEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RetryMaxInterval, err = positiveDurationEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL"); err != nil {
		return forward.Config{}, nil, err
	}
	if cfg.RetryMaxElapsedTime, err = positiveDurationEnv(getenv, "OTHERLODE_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME"); err != nil {
		return forward.Config{}, nil, err
	}
	return cfg, file, nil
}

// resolveMaxConcurrentDecodes returns the cap on concurrent request
// decodes from OTHERLODE_COLLECTOR_MAX_CONCURRENT_DECODES. It defaults to
// runtime.GOMAXPROCS(0), because decoding is CPU-bound and more parallel
// decodes than CPUs add memory but no throughput.
func resolveMaxConcurrentDecodes(getenv func(string) string) (int, error) {
	n, err := positiveIntEnv(getenv, "OTHERLODE_COLLECTOR_MAX_CONCURRENT_DECODES")
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return runtime.GOMAXPROCS(0), nil
	}
	return n, nil
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
// OTHERLODE_COLLECTOR_ENVIRONMENT and
// OTHERLODE_COLLECTOR_ENVIRONMENT_ACTION. A value that is blank after
// trimming space turns the processor off. An action with no value is an
// error: the operator meant to label payloads, and starting without the
// label would hide that mistake. A value that is not valid UTF-8 is an
// error, since a payload that carries it cannot be encoded for forwarding.
func resolveEnvironment(valueRaw, actionRaw string) (processor.EnvironmentConfig, error) {
	value := strings.TrimSpace(valueRaw)
	if !utf8.ValidString(value) {
		return processor.EnvironmentConfig{}, errors.New("OTHERLODE_COLLECTOR_ENVIRONMENT is not valid UTF-8")
	}

	action, err := processor.ParseAction(actionRaw)
	if err != nil {
		return processor.EnvironmentConfig{}, fmt.Errorf("OTHERLODE_COLLECTOR_ENVIRONMENT_ACTION: %w", err)
	}

	if value == "" {
		if actionRaw != "" {
			return processor.EnvironmentConfig{}, errors.New(
				"OTHERLODE_COLLECTOR_ENVIRONMENT_ACTION is set but OTHERLODE_COLLECTOR_ENVIRONMENT is empty; " +
					"set OTHERLODE_COLLECTOR_ENVIRONMENT or unset OTHERLODE_COLLECTOR_ENVIRONMENT_ACTION")
		}
		return processor.EnvironmentConfig{}, nil
	}

	return processor.EnvironmentConfig{Value: value, Action: action}, nil
}

// resolveNamespace builds the processor.NamespaceConfig from
// OTHERLODE_COLLECTOR_SERVICE_NAMESPACE and
// OTHERLODE_COLLECTOR_SERVICE_NAMESPACE_ACTION. A value that is blank after
// trimming space turns the processor off, so each agent's namespace
// passes through unchanged. An action with no value is an error: the
// operator meant to set a namespace, and starting without it would hide
// that mistake. A value of "." or ".." is an error, since the ingest
// handler rejects that namespace from an agent too. So is a value that is
// not valid UTF-8, since a payload that carries it cannot be encoded for
// forwarding.
func resolveNamespace(valueRaw, actionRaw string) (processor.NamespaceConfig, error) {
	value := strings.TrimSpace(valueRaw)
	if !utf8.ValidString(value) {
		return processor.NamespaceConfig{}, errors.New("OTHERLODE_COLLECTOR_SERVICE_NAMESPACE is not valid UTF-8")
	}

	action, err := processor.ParseAction(actionRaw)
	if err != nil {
		return processor.NamespaceConfig{}, fmt.Errorf("OTHERLODE_COLLECTOR_SERVICE_NAMESPACE_ACTION: %w", err)
	}

	if value == "" {
		if actionRaw != "" {
			return processor.NamespaceConfig{}, errors.New(
				"OTHERLODE_COLLECTOR_SERVICE_NAMESPACE_ACTION is set but OTHERLODE_COLLECTOR_SERVICE_NAMESPACE is empty; " +
					"set OTHERLODE_COLLECTOR_SERVICE_NAMESPACE or unset OTHERLODE_COLLECTOR_SERVICE_NAMESPACE_ACTION")
		}
		return processor.NamespaceConfig{}, nil
	}
	if value == "." || value == ".." {
		return processor.NamespaceConfig{}, fmt.Errorf("OTHERLODE_COLLECTOR_SERVICE_NAMESPACE is %q, which no URL path can name", value)
	}

	return processor.NamespaceConfig{Value: value, Action: action}, nil
}

// resolveRedaction builds the processor.RedactionConfig from
// OTHERLODE_COLLECTOR_REDACT_BLOCKED_VALUES and
// OTHERLODE_COLLECTOR_REDACT_ALL_LITERALS. The first holds one regular
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
			return processor.RedactionConfig{}, fmt.Errorf("OTHERLODE_COLLECTOR_REDACT_BLOCKED_VALUES line %d: %w", i+1, err)
		}
		cfg.BlockedValues = append(cfg.BlockedValues, re)
	}

	if allLiteralsRaw != "" {
		all, err := strconv.ParseBool(allLiteralsRaw)
		if err != nil {
			return processor.RedactionConfig{}, fmt.Errorf("OTHERLODE_COLLECTOR_REDACT_ALL_LITERALS: %w", err)
		}
		cfg.AllLiterals = all
	}
	return cfg, nil
}
