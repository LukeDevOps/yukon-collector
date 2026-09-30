package main

import (
	"log/slog"
	"net/http"

	"github.com/otherlodehq/otherlode-collector/ingest"
	"github.com/otherlodehq/otherlode-collector/internal/auth"
	"github.com/otherlodehq/otherlode-collector/internal/forward"
	"github.com/otherlodehq/otherlode-collector/internal/processor"
	"github.com/otherlodehq/otherlode-collector/internal/ratelimit"
	"github.com/otherlodehq/otherlode-collector/metrics"
)

// registerRoutes wires the ingest handler and health check onto mux. When
// authTokens is non-nil, the ingest routes require an
// "Authorization: Bearer <token>" header whose token is in authTokens.
// Each request reads the current set, so a token file re-read applies
// without a restart. When limiter is non-nil, the
// ingest routes are throttled per client IP, checked before auth so a
// flood is capped regardless of whether it carries a valid token.
// /healthz and /metrics stay open, unauthenticated and unthrottled: the
// first for liveness/readiness probes, the second for a Prometheus
// scraper, which exposes only counts.
//
// When forwardCfg.URL is non-empty, ingested payloads are relayed to that
// backend via a forward.ForwardingSink built from forwardCfg, which
// registerRoutes returns so the caller can Shutdown it on graceful
// shutdown. An empty URL falls back to logging payloads instead of
// forwarding them, so local/dev/CI runs keep working with no backend at
// all; registerRoutes then returns a nil *forward.ForwardingSink. A URL
// that cannot be used is returned as an error.
//
// When envCfg.Value is non-empty, a processor.Environment wraps the
// chosen sink. It writes that environment onto delta batches, manifests
// and static baselines before the sink sees them. A zero envCfg leaves
// payloads untouched.
//
// When nsCfg.Value is non-empty, a processor.Namespace wraps the sink
// outside the environment processor. It writes that service namespace
// onto the same three payloads. A zero nsCfg leaves each agent's
// namespace as sent.
//
// When redactCfg is enabled, a processor.Redaction wraps the sink outside
// the other processors. It hides string literals and drops unknown
// fields before any payload is forwarded. A zero redactCfg leaves it out
// of the chain.
func registerRoutes(mux *http.ServeMux, logger *slog.Logger, authTokens *auth.TokenSet, limiter *ratelimit.Limiter, forwardCfg forward.Config, envCfg processor.EnvironmentConfig, nsCfg processor.NamespaceConfig, redactCfg processor.RedactionConfig) (*forward.ForwardingSink, error) {
	if logger == nil {
		logger = slog.Default()
	}

	var sink ingest.Sink
	var fwd *forward.ForwardingSink
	if forwardCfg.URL != "" {
		forwardCfg.Logger = logger
		var err error
		fwd, err = forward.NewForwardingSink(forwardCfg)
		if err != nil {
			return nil, err
		}
		sink = fwd
	} else {
		logger.Warn("OTHERLODE_COLLECTOR_FORWARD_URL not set; ingest payloads are only logged, not forwarded")
		sink = ingest.NewLogSink(logger)
	}
	if envCfg.Value != "" {
		logger.Info("stamping environment on ingested payloads", "environment", envCfg.Value, "action", envCfg.Action)
		sink = processor.NewEnvironment(sink, envCfg, logger)
	}
	if nsCfg.Value != "" {
		logger.Info("stamping service namespace on ingested payloads", "namespace", nsCfg.Value, "action", nsCfg.Action)
		sink = processor.NewNamespace(sink, nsCfg, logger)
	}
	if redactCfg.Enabled() {
		logger.Info("redacting string literals in ingested payloads",
			"blocked_values", len(redactCfg.BlockedValues), "all_literals", redactCfg.AllLiterals)
		sink = processor.NewRedaction(sink, redactCfg, logger)
	}
	handler := ingest.NewHandler(sink, logger)

	ingestMux := http.NewServeMux()
	handler.Register(ingestMux)

	var ingestHandler http.Handler = ingestMux
	if authTokens != nil {
		ingestHandler = auth.RequireBearerToken(authTokens, ingestMux)
	} else {
		logger.Warn("OTHERLODE_COLLECTOR_AUTH_TOKEN and OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE not set; ingest endpoints are unauthenticated")
	}
	if limiter != nil {
		ingestHandler = limiter.Middleware(ingestHandler)
	} else {
		logger.Warn("rate limiting disabled; ingest endpoints accept requests unthrottled")
	}
	mux.Handle("/v1/otherlode/", ingestHandler)

	mux.HandleFunc("GET "+healthzPath, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", metrics.Handler())

	return fwd, nil
}
